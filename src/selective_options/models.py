"""Validate required external fields while allowing documented venue schema evolution."""

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, JsonValue, SecretStr, model_validator


class ExternalModel(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True, allow_inf_nan=False)


class ClosedBarPayload(ExternalModel):
    open_time_ms: int = Field(ge=0)
    close_time_ms: int = Field(gt=0)
    timeframe_s: int = Field(gt=0)
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: float = Field(ge=0)
    trade_count: int = Field(ge=1)
    closed: Literal[True]
    complete: bool

    @model_validator(mode="after")
    def valid_bar(self) -> Self:
        interval = self.timeframe_s * 1000
        if self.close_time_ms - self.open_time_ms != interval or self.open_time_ms % interval:
            raise ValueError("invalid bar interval or alignment")
        if not self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high:
            raise ValueError("invalid bar OHLC")
        return self


class RpcErrorPayload(ExternalModel):
    code: int
    message: str
    data: JsonValue = None


class RpcEnvelope(ExternalModel):
    jsonrpc: Literal["2.0"]
    id: int
    result: JsonValue = None
    error: RpcErrorPayload | None = None

    @model_validator(mode="after")
    def one_payload(self) -> Self:
        if ("result" in self.model_fields_set) == (self.error is not None):
            raise ValueError("RPC must contain exactly one of result or error")
        return self


class TickStep(ExternalModel):
    tick_size: float = Field(gt=0)
    above_price: float = Field(ge=0)


class Instrument(ExternalModel):
    instrument_name: str
    base_currency: str
    quote_currency: str
    settlement_currency: str
    kind: Literal["option"]
    option_type: Literal["call", "put"]
    strike: float = Field(gt=0)
    expiration_timestamp: int = Field(gt=0)
    is_active: bool
    contract_size: float = Field(gt=0)
    min_trade_amount: float = Field(gt=0)
    tick_size: float = Field(gt=0)
    tick_size_steps: tuple[TickStep, ...] = ()
    price_index: str
    instrument_type: Literal["linear", "reversed"]
    maker_commission: float
    taker_commission: float


class BookSummary(ExternalModel):
    instrument_name: str
    bid_price: float | None = Field(default=None, ge=0)
    ask_price: float | None = Field(default=None, ge=0)
    mark_price: float | None = Field(default=None, ge=0)
    mark_iv: float | None = Field(default=None, ge=0)
    open_interest: float = Field(ge=0)
    volume: float = Field(ge=0)
    creation_timestamp: int = Field(gt=0)


class Greeks(ExternalModel):
    delta: float = Field(ge=-1, le=1)
    gamma: float
    vega: float
    theta: float
    rho: float


class Ticker(ExternalModel):
    instrument_name: str
    timestamp: int = Field(gt=0)
    best_bid_price: float | None = Field(default=None, ge=0)
    best_ask_price: float | None = Field(default=None, ge=0)
    best_bid_amount: float = Field(ge=0)
    best_ask_amount: float = Field(ge=0)
    mark_iv: float = Field(ge=0)
    bid_iv: float | None = Field(default=None, ge=0)
    ask_iv: float | None = Field(default=None, ge=0)
    underlying_price: float = Field(gt=0)
    open_interest: float = Field(ge=0)
    greeks: Greeks


class DiscoveryResult(ExternalModel):
    asset: str
    query_currency: str
    fallback_used: bool
    instruments: tuple[Instrument, ...]


class ChainRow(ExternalModel):
    instrument: Instrument
    summary: BookSummary | None


class ChainSnapshot(ExternalModel):
    asset: str
    observed_at_ms: int
    discovery: DiscoveryResult
    rows: tuple[ChainRow, ...]


class AuthToken(ExternalModel):
    access_token: SecretStr
    refresh_token: SecretStr
    expires_in: int = Field(gt=0)
    scope: str
    token_type: Literal["bearer"]


class ApiKeyMetadata(ExternalModel):
    client_id: SecretStr
    max_scope: str
    enabled: bool


class CancelOnDisconnect(ExternalModel):
    enabled: bool
    scope: Literal["connection", "account"]


class AssetProbe(ExternalModel):
    asset: str
    instrument_count: int
    quoted_count: int
    two_sided_count: int
    missing_summary_count: int
    stale_summary_count: int
    ticker_samples: tuple[Ticker, ...]
    quote_currencies: tuple[str, ...]
    settlement_currencies: tuple[str, ...]
    capability: str
    tradable: Literal[False] = False
    reason: str


class ProbeReport(ExternalModel):
    created_at: str
    endpoint: str
    server_time_ms: int
    clock_offset_ms: float
    round_trip_ms: float
    authenticated_checks: str
    assets: tuple[AssetProbe, ...]
    milestone: Literal[1] = 1
    trading_enabled: Literal[False] = False
    validation_status: Literal["NOT_VALIDATED"] = "NOT_VALIDATED"
