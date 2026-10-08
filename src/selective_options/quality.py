"""Observable data quality. Missing, stale and incomplete feeds never imply readiness."""

from collections import Counter
from dataclasses import dataclass

from .events import MarketEvent


@dataclass(frozen=True)
class FeedRequirement:
    source: str
    asset: str
    kind: str
    max_age_ms: int
    status: str = "CONFIGURED"


class FreshnessMonitor:
    def __init__(self, max_skew_ms: int) -> None:
        self.latest: dict[tuple[str, str, str], MarketEvent] = {}
        self.latest_conflicts: set[tuple[str, str, str]] = set()
        self.counts: Counter[str] = Counter()
        self.max_skew_ms = max_skew_ms
        self.by_instrument: dict[tuple[str, str, str], MarketEvent] = {}
        self.expected_options: set[tuple[str, str]] = set()
        self.stream_state = "UNKNOWN"
        self.stream_control_at_ms = 0
        self.stream_start_at_ms = 0

    def observe(self, event: MarketEvent) -> None:
        instrument_key = (event.source, event.symbol, event.kind)
        previous = self.by_instrument.get(instrument_key)
        if previous is None or (event.event_time_ms, event.observed_at_ms) > (
            previous.event_time_ms,
            previous.observed_at_ms,
        ):
            self.by_instrument[instrument_key] = event
        payload = event.payload()
        if event.source == "deribit" and event.kind == "status":
            reason = payload.get("reason")
            if (
                isinstance(reason, str)
                and reason
                in {
                    "STREAM_RESYNC",
                    "STREAM_DISCONNECTED",
                    "STREAM_REFRESH_DUE",
                    "STREAM_SUBSCRIBED",
                }
                and event.observed_at_ms >= self.stream_control_at_ms
            ):
                self.stream_control_at_ms = event.observed_at_ms
                self.stream_state = str(reason)
                if reason == "STREAM_RESYNC":
                    self.stream_start_at_ms = event.observed_at_ms
                    self.by_instrument = {
                        key: row
                        for key, row in self.by_instrument.items()
                        if key[0] != "deribit" or key[2] not in {"ticker", "book"}
                    }
        if (
            event.source == "deribit"
            and event.kind == "status"
            and payload.get("reason") == "STREAM_SUBSCRIBED"
            and event.observed_at_ms == self.stream_control_at_ms
        ):
            instruments = payload.get("instruments")
            if isinstance(instruments, list):
                self.expected_options = set()
                for item in instruments:
                    if isinstance(item, dict):
                        asset, symbol = item.get("asset"), item.get("symbol")
                        if isinstance(asset, str) and isinstance(symbol, str):
                            self.expected_options.add((asset, symbol))
        key = (event.source, event.asset, event.kind)
        old = self.latest.get(key)
        # A delayed old packet must not reset freshness to the receipt time.
        if old is None or (event.event_time_ms, event.observed_at_ms) > (
            old.event_time_ms,
            old.observed_at_ms,
        ):
            self.latest[key] = event
            self.latest_conflicts.discard(key)
        elif (event.event_time_ms, event.observed_at_ms) == (
            old.event_time_ms,
            old.observed_at_ms,
        ) and event != old:
            self.latest_conflicts.add(key)
        self.counts[f"{event.source}:{event.asset}:{event.kind}"] += 1

    def option_freshness(self, now_ms: int, max_age_ms: int) -> list[dict[str, object]]:
        """A fresh busy option must never hide a stale or missing selected leg."""
        result: list[dict[str, object]] = []
        for asset, symbol in sorted(self.expected_options):
            for kind in ("ticker", "book"):
                event = self.by_instrument.get(("deribit", symbol, kind))
                age = None if event is None else now_ms - event.event_time_ms
                state = (
                    "STREAM_INACTIVE"
                    if self.stream_state != "STREAM_SUBSCRIBED"
                    else "MISSING"
                    if event is None or event.observed_at_ms < self.stream_start_at_ms
                    else "CLOCK_SKEW"
                    if age is not None and age < -self.max_skew_ms
                    else "UNAVAILABLE_AT_AS_OF"
                    if event is not None and max(event.event_time_ms, event.observed_at_ms) > now_ms
                    else "STALE"
                    if age is not None and age > max_age_ms
                    else "FRESH"
                )
                quote_state = "UNKNOWN"
                if event is not None and kind == "book":
                    payload = event.payload()
                    bids, asks = payload.get("bids"), payload.get("asks")
                    quote_state = "TWO_SIDED" if bids and asks else "ONE_SIDED_OR_EMPTY"
                result.append(
                    {
                        "asset": asset,
                        "symbol": symbol,
                        "kind": kind,
                        "state": state,
                        "age_ms": age,
                        "quote_state": quote_state,
                        "stream_state": self.stream_state,
                    }
                )
        return result

    def check(self, requirements: list[FeedRequirement], now_ms: int) -> list[dict[str, object]]:
        results: list[dict[str, object]] = []
        for requirement in requirements:
            event = self.latest.get((requirement.source, requirement.asset, requirement.kind))
            age = None if event is None else now_ms - event.event_time_ms
            state = requirement.status
            if state == "CONFIGURED":
                state = (
                    "MISSING"
                    if event is None
                    else "CLOCK_SKEW"
                    if age is not None and age < -self.max_skew_ms
                    else "UNAVAILABLE_AT_AS_OF"
                    if event is not None and max(event.event_time_ms, event.observed_at_ms) > now_ms
                    else "AMBIGUOUS_LATEST"
                    if (requirement.source, requirement.asset, requirement.kind)
                    in self.latest_conflicts
                    else "STALE"
                    if age is not None and age > requirement.max_age_ms
                    else "FRESH"
                )
                if (
                    state == "FRESH"
                    and event is not None
                    and event.kind == "feed_health"
                    and event.payload().get("ok") is not True
                ):
                    state = "UNAVAILABLE_OR_DEGRADED"
            results.append(
                {
                    "source": requirement.source,
                    "asset": requirement.asset,
                    "kind": requirement.kind,
                    "state": state,
                    "age_ms": age,
                    "timestamp_kind": None if event is None else event.timestamp_kind,
                }
            )
        return results

    def divergence(
        self, asset: str, now_ms: int, max_age_ms: int, threshold_bps: float
    ) -> dict[str, object]:
        prices: list[float] = []
        sources: list[str] = []
        for (source, event_asset, kind), event in self.latest.items():
            if event_asset != asset or kind != "spot" or event.environment != "public_reference":
                continue
            if event.observed_at_ms > now_ms or not 0 <= now_ms - event.event_time_ms <= max_age_ms:
                continue
            if (source, event_asset, kind) in self.latest_conflicts:
                continue
            price = event.payload().get("price")
            if isinstance(price, (int, float)) and price > 0:
                prices.append(float(price))
                sources.append(source)
        if len(set(sources)) < 2:
            return {"asset": asset, "state": "INSUFFICIENT_SOURCES", "sources": sources}
        bps = (max(prices) - min(prices)) / (sum(prices) / len(prices)) * 10_000
        return {
            "asset": asset,
            "state": "PRICE_DIVERGENCE" if bps > threshold_bps else "AGREE",
            "divergence_bps": bps,
            "sources": sources,
        }
