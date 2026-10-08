"""Public global account ratios; retain reported field semantics and period-end clocks."""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StrictInt, StrictStr, TypeAdapter

from .binance import Endpoint, Instrument
from .binance_taker import (
    MAX_AGE_MS,
    MAX_LATEST_END_AGE_MS,
    PERIOD_MS,
    BinanceTakerCollector,
    DecimalText,
)
from .errors import IntegrityError
from .events import MarketEvent, make_event


class AccountSample(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")
    symbol: StrictStr
    longShortRatio: DecimalText
    longAccount: DecimalText
    shortAccount: DecimalText
    timestamp: StrictInt = Field(gt=PERIOD_MS)


def account_events(
    instrument: Instrument,
    raw: JsonValue,
    received: int,
    discovered: int,
    *,
    check_latest: bool = True,
) -> list[MarketEvent]:
    if not (
        instrument.contractType == "PERPETUAL"
        and instrument.status == "TRADING"
        and instrument.quoteAsset == instrument.marginAsset == "USDT"
        and instrument.symbol == instrument.baseAsset + "USDT"
    ):
        raise IntegrityError("BINANCE_ACCOUNT_INSTRUMENT_SCOPE")
    if not 0 < discovered <= received:
        raise IntegrityError("BINANCE_ACCOUNT_RECEIPT_ORDER")
    samples = TypeAdapter(list[AccountSample]).validate_python(raw)
    if not 1 <= len(samples) <= 3:
        raise IntegrityError("BINANCE_ACCOUNT_SAMPLE_COUNT")
    unique: dict[int, AccountSample] = {}
    for row in samples:
        if row.symbol != instrument.symbol:
            raise IntegrityError("BINANCE_ACCOUNT_SYMBOL_MISMATCH")
        if max(Decimal(row.longAccount), Decimal(row.shortAccount)) > 1:
            raise IntegrityError("BINANCE_ACCOUNT_FRACTION_RANGE")
        if not received - MAX_AGE_MS <= row.timestamp <= received:
            raise IntegrityError("BINANCE_ACCOUNT_STALE_OR_FUTURE")
        if row.timestamp in unique and unique[row.timestamp] != row:
            raise IntegrityError("BINANCE_ACCOUNT_CONFLICTING_PERIOD")
        unique[row.timestamp] = row
    if check_latest and received - max(unique) > MAX_LATEST_END_AGE_MS:
        raise IntegrityError("BINANCE_ACCOUNT_LATEST_PERIOD_STALE")
    return [
        make_event(
            source="binance_accounts",
            environment="public_reference",
            asset=instrument.baseAsset,
            symbol=instrument.symbol,
            kind="account_ratio",
            event_time_ms=ts,
            observed_at_ms=received,
            native_id=str(ts),
            payload={
                "observation_received_at_ms": received,
                "reported_longShortRatio": row.longShortRatio,
                "reported_longAccount": row.longAccount,
                "reported_shortAccount": row.shortAccount,
                "period_end_ms": ts,
                "period_start_ms": ts - PERIOD_MS,
                "period_ms": PERIOD_MS,
                "scope": "venue global account-ratio endpoint; not position size or top-trader subset",
                "semantics_status": "REPORTED_FIELDS_PRESERVED_DOCUMENTATION_INCONSISTENCIES",
                "ratio_basis": "venue reported; not recomputed or inferred from example values",
                "instrument": instrument.model_dump(mode="json"),
                "timestamp_basis": "documented period end; derived start; receipt also required",
                "publication_time_available": False,
                "revision_policy": "changed values retained as separate observations; no finality assumed",
                "coverage": "sampled latest five-minute global account ratios; no continuous history claim",
            },
        )
        for ts, row in sorted(unique.items())
    ]


class BinanceAccountCollector(BinanceTakerCollector):
    source = "binance_accounts"
    endpoint: Endpoint = "globalLongShortAccountRatio"

    def parse_samples(
        self, instrument: Instrument, raw: JsonValue, received: int, discovered: int
    ) -> list[MarketEvent]:
        return account_events(instrument, raw, received, discovered)

    def poll_interval(self) -> float:
        return self.config.ingestion.binance_account_ratio_poll_interval_s
