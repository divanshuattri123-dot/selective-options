"""Bounded as-of membership and leg-observation coverage; no continuity or trading approval."""

from collections.abc import Iterable
from typing import Literal

from pydantic import Field, StrictInt, StrictStr, ValidationError

from .collectors import ChannelRegistry, Notification
from .errors import IntegrityError
from .events import MarketEvent, make_event
from .models import ExternalModel, Instrument


class Member(ExternalModel):
    asset: StrictStr
    symbol: StrictStr


class Subscription(ExternalModel):
    reason: Literal["STREAM_SUBSCRIBED"]
    connection: StrictInt = Field(gt=0)
    channels: StrictInt = Field(gt=0)
    instruments: tuple[Member, ...] = Field(max_length=10000)
    universe_event_ids: tuple[StrictStr, ...] = Field(max_length=16)


class Universe(ExternalModel):
    reason: Literal["UNIVERSE_DISCOVERED"]
    asset: StrictStr
    available: StrictInt = Field(ge=0, le=10000)
    selected: StrictInt = Field(ge=0, le=10000)
    coverage: Literal["FULL", "PARTIAL_OR_ABSENT"]
    instruments: tuple[StrictStr, ...] = Field(max_length=10000)
    selected_instruments: tuple[StrictStr, ...] = Field(max_length=10000)
    metadata_event_ids: tuple[StrictStr, ...] = Field(max_length=10000)


def status_canonical(row: MarketEvent) -> None:
    expected = make_event(
        source="deribit",
        environment="testnet",
        asset="SYSTEM",
        symbol="SYSTEM",
        kind="status",
        event_time_ms=row.observed_at_ms,
        observed_at_ms=row.observed_at_ms,
        timestamp_kind="receipt",
        payload=row.payload(),
    )
    if row != expected:
        raise IntegrityError("OPTION_STATUS_CANONICAL")


def coverage_at(
    events: Iterable[MarketEvent],
    asset: str,
    as_of_ms: int,
    max_leg_age_ms: int = 300000,
    max_universe_age_ms: int = 3600000,
    max_rows: int = 100000,
) -> dict[str, object]:
    if (
        asset not in {"BTC", "ETH", "SOL", "PAXG"}
        or any(
            type(v) is not int or v <= 0
            for v in (as_of_ms, max_leg_age_ms, max_universe_age_ms, max_rows)
        )
        or max_rows > 200000
    ):
        raise ValueError("invalid bounded option-coverage policy")
    result: dict[str, object] = {
        "asset": asset,
        "as_of_ms": as_of_ms,
        "state": "BLOCKED",
        "blocker": None,
        "trading_approval": False,
        "trade_history_continuity_verified": False,
        "all_options_native_fields_available": False,
    }
    try:
        rows = []
        for row in events:
            if (
                row.source != "deribit"
                or row.environment != "testnet"
                or row.event_time_ms > as_of_ms
                or row.observed_at_ms > as_of_ms
            ):
                continue
            if row.asset == asset and row.kind in {"instrument", "ticker", "book"}:
                rows.append(row)
            elif row.asset == "SYSTEM" and row.kind == "status":
                body = row.payload()
                if (
                    body.get("reason")
                    in (
                        "STREAM_RESYNC",
                        "STREAM_SUBSCRIBED",
                        "STREAM_DISCONNECTED",
                        "STREAM_REFRESH_DUE",
                    )
                    or body.get("reason") == "UNIVERSE_DISCOVERED"
                    and body.get("asset") == asset
                ):
                    rows.append(row)
            if len(rows) > max_rows:
                raise IntegrityError("OPTION_COVERAGE_OVER_BUDGET")
        controls = []
        for row in rows:
            if row.kind == "status":
                status_canonical(row)
                if row.payload().get("reason") != "UNIVERSE_DISCOVERED":
                    controls.append(row)
        if not controls:
            raise IntegrityError("OPTION_STREAM_CONTROL_MISSING")
        latest = max(row.observed_at_ms for row in controls)
        choices = {row for row in controls if row.observed_at_ms == latest}
        if len(choices) != 1:
            raise IntegrityError("OPTION_STREAM_CONTROL_AMBIGUOUS")
        ack = choices.pop()
        result["stream_control_event_id"] = ack.event_id
        if ack.payload().get("reason") != "STREAM_SUBSCRIBED":
            raise IntegrityError("OPTION_STREAM_INACTIVE")
        if "universe_event_ids" not in ack.payload():
            raise IntegrityError("OPTION_LEGACY_MANIFEST_BINDING_MISSING")
        subscribed = Subscription.model_validate(ack.payload())
        if subscribed.model_dump(mode="json") != ack.payload():
            raise IntegrityError("OPTION_SUBSCRIPTION_CANONICAL")
        if len(set(subscribed.universe_event_ids)) != len(subscribed.universe_event_ids) or len(
            set(subscribed.instruments)
        ) != len(subscribed.instruments):
            raise IntegrityError("OPTION_SUBSCRIPTION_DUPLICATES")
        if subscribed.instruments and not 3 * len(
            subscribed.instruments
        ) + 1 <= subscribed.channels <= 4 * len(subscribed.instruments):
            raise IntegrityError("OPTION_CHANNEL_COUNT")
        starts = {
            row
            for row in controls
            if row.payload().get("reason") == "STREAM_RESYNC"
            and type(row.payload().get("connection")) is int
            and row.payload().get("connection") == subscribed.connection
            and row.observed_at_ms <= ack.observed_at_ms
        }
        if len(starts) != 1:
            raise IntegrityError("OPTION_RESYNC_MISSING_OR_AMBIGUOUS")
        start = starts.pop().observed_at_ms
        manifests = {
            row
            for row in rows
            if row.kind == "status"
            and row.event_id in subscribed.universe_event_ids
            and row.payload().get("reason") == "UNIVERSE_DISCOVERED"
        }
        if len(manifests) != 1:
            raise IntegrityError("OPTION_SCOPED_MANIFEST_MISSING_OR_AMBIGUOUS")
        manifest = manifests.pop()
        universe = Universe.model_validate(manifest.payload())
        if universe.model_dump(mode="json") != manifest.payload():
            raise IntegrityError("OPTION_MANIFEST_CANONICAL")
        members = {member.symbol for member in subscribed.instruments if member.asset == asset}
        all_symbols, selected = set(universe.instruments), set(universe.selected_instruments)
        if (
            len(all_symbols) != universe.available
            or len(universe.instruments) != universe.available
            or len(selected) != universe.selected
            or len(universe.selected_instruments) != universe.selected
            or len(universe.metadata_event_ids) != universe.available
            or len(set(universe.metadata_event_ids)) != universe.available
            or not selected <= all_symbols
            or selected != members
            or manifest.observed_at_ms > start
            or universe.asset != asset
            or universe.coverage
            != ("FULL" if selected == all_symbols and all_symbols else "PARTIAL_OR_ABSENT")
        ):
            raise IntegrityError("OPTION_MANIFEST_SCOPE_OR_COUNTS")
        if as_of_ms - manifest.observed_at_ms > max_universe_age_ms:
            raise IntegrityError("OPTION_UNIVERSE_STALE")
        registry = ChannelRegistry()
        metadata_by_id: dict[str, set[MarketEvent]] = {}
        legs_by_key: dict[tuple[str, str], list[MarketEvent]] = {}
        for row in rows:
            if row.kind == "instrument":
                metadata_by_id.setdefault(row.event_id, set()).add(row)
            elif row.kind in {"ticker", "book"} and row.observed_at_ms >= start:
                legs_by_key.setdefault((row.symbol, row.kind), []).append(row)
        for symbol, identity in zip(universe.instruments, universe.metadata_event_ids, strict=True):
            matches = metadata_by_id.get(identity, set())
            if len(matches) != 1:
                raise IntegrityError("OPTION_METADATA_MISSING_OR_CONFLICTING")
            metadata = next(iter(matches))
            native = Instrument.model_validate(metadata.payload())
            expected = make_event(
                source="deribit",
                environment="testnet",
                asset=asset,
                symbol=symbol,
                kind="instrument",
                timestamp_kind="receipt",
                event_time_ms=metadata.observed_at_ms,
                observed_at_ms=metadata.observed_at_ms,
                payload=native.model_dump(mode="json"),
            )
            if (
                metadata != expected
                or native.instrument_name != symbol
                or native.base_currency != asset
                or metadata.observed_at_ms > manifest.observed_at_ms
                or not native.is_active
            ):
                raise IntegrityError("OPTION_METADATA_CANONICAL_OR_SCOPE")
            if native.expiration_timestamp <= as_of_ms:
                raise IntegrityError("OPTION_UNIVERSE_EXPIRED_REDISCOVERY_REQUIRED")
            if symbol in selected:
                registry.add_instrument(asset, symbol, native.price_index)
        if not selected:
            raise IntegrityError("OPTION_SELECTED_UNIVERSE_EMPTY")
        if all(
            member.asset == asset for member in subscribed.instruments
        ) and subscribed.channels != len(registry.channels):
            raise IntegrityError("OPTION_CHANNEL_COUNT")
        diagnostics = []
        for symbol in sorted(selected):
            for kind in ("ticker", "book"):
                candidates = legs_by_key.get((symbol, kind), [])
                state = "MISSING"
                chosen = None
                if candidates:
                    key = max((row.event_time_ms, row.observed_at_ms) for row in candidates)
                    choices = {
                        row for row in candidates if (row.event_time_ms, row.observed_at_ms) == key
                    }
                    if len(choices) != 1:
                        raise IntegrityError("OPTION_LEG_AMBIGUOUS")
                    chosen = choices.pop()
                    channel = (
                        f"ticker.{symbol}.100ms"
                        if kind == "ticker"
                        else f"book.{symbol}.none.20.100ms"
                    )
                    canonical = ChannelRegistry()
                    canonical.channels = registry.channels
                    rebuilt = canonical.normalize(
                        Notification.model_validate(
                            {
                                "jsonrpc": "2.0",
                                "method": "subscription",
                                "params": {"channel": channel, "data": chosen.payload()},
                            }
                        ),
                        chosen.observed_at_ms,
                    )
                    if rebuilt != [chosen]:
                        raise IntegrityError("OPTION_LEG_CANONICAL")
                    state = (
                        "STALE"
                        if as_of_ms - chosen.event_time_ms > max_leg_age_ms
                        or as_of_ms - chosen.observed_at_ms > max_leg_age_ms
                        else "OBSERVED"
                    )
                diagnostics.append(
                    {
                        "symbol": symbol,
                        "kind": kind,
                        "state": state,
                        "event_id": None if chosen is None else chosen.event_id,
                    }
                )
        observed = sum(row["state"] == "OBSERVED" for row in diagnostics)
        result.update(
            state="SELECTED_FEEDS_OBSERVED"
            if observed == len(diagnostics)
            else "INCOMPLETE_LEG_OBSERVATIONS",
            manifest_event_id=manifest.event_id,
            available_options=universe.available,
            selected_options=universe.selected,
            full_discovered_membership=selected == all_symbols,
            expected_leg_observations=len(diagnostics),
            observed_leg_observations=observed,
            legs=diagnostics,
        )
    except (IntegrityError, ValidationError, ValueError, TypeError, KeyError) as exc:
        result["blocker"] = (
            str(exc) if isinstance(exc, IntegrityError) else "OPTION_INVALID_EVIDENCE"
        )
    return result
