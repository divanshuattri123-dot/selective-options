import asyncio
import logging
import time
from datetime import UTC, datetime
from pathlib import Path

from pydantic import TypeAdapter

from .auth import AuthSession
from .config import AppConfig
from .errors import IntegrityError, TradingUnavailable, TransportError
from .models import AssetProbe, ProbeReport, Ticker
from .rate_limit import EndpointLimiter
from .rpc import RpcClient, WebSocketRpc
from .security import load_credentials
from .venue import DeribitAdapter

logger = logging.getLogger(__name__)


async def inspect_connection(
    rpc: RpcClient, config: AppConfig, authenticated: bool = False, chain_dir: Path | None = None
) -> ProbeReport:
    start = time.time()
    server_ms = TypeAdapter(int).validate_python(await rpc.call("public/get_time", {}), strict=True)
    end = time.time()
    offset_ms = server_ms - (start + end) * 500
    round_trip_ms = (end - start) * 1000
    if abs(offset_ms) + round_trip_ms / 2 > config.data.max_clock_skew_s * 1000:
        raise IntegrityError("CLOCK_SKEW: server offset/uncertainty exceeds tolerance")
    auth: AuthSession | None = None
    if authenticated:
        auth = AuthSession(
            rpc, load_credentials(config.venue.key_profile), config.venue.auth_refresh_margin_s
        )
        await auth.authenticate()
    adapter = DeribitAdapter(rpc)
    assets: list[AssetProbe] = []
    for asset in config.system.assets:
        if auth is not None:
            await auth.ensure_fresh()
        chain = await adapter.option_chain(asset)
        if chain_dir is not None:
            await asyncio.to_thread(chain_dir.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(
                (chain_dir / f"{asset}-chain.json").write_text,
                chain.model_dump_json(indent=2),
                encoding="utf-8",
            )
        summaries = [row.summary for row in chain.rows if row.summary is not None]
        stale = sum(
            not -config.data.max_clock_skew_s * 1000
            <= chain.observed_at_ms - row.creation_timestamp
            <= config.data.max_staleness_s * 1000
            for row in summaries
        )
        two_sided = [
            row
            for row in summaries
            if row.bid_price is not None
            and row.ask_price is not None
            and 0 < row.bid_price <= row.ask_price
        ]
        samples: list[Ticker] = []
        for row in chain.rows[: config.venue.ticker_samples_per_asset]:
            ticker = await adapter.ticker(row.instrument.instrument_name)
            age_s = time.time() - ticker.timestamp / 1000
            if not -config.data.max_clock_skew_s <= age_s <= config.data.max_staleness_s:
                raise IntegrityError("STALE_TICKER: outside permitted age/skew")
            samples.append(ticker)
        capability = "OPTIONS_DISCOVERED" if chain.rows else "NO_OPTION_CHAIN"
        reason = "Discovery only; liquidity, feeds, risk and validation are not established."
        if asset == "PAXG":
            capability = "GOLD_OPTIONS_UNVERIFIED" if chain.rows else "GOLD_UNTRADEABLE"
            reason = "No verified liquid option chain or configured, validated alternative route."
        if stale:
            reason += " STALE_SUMMARIES present."
        result = AssetProbe(
            asset=asset,
            instrument_count=len(chain.rows),
            quoted_count=len(summaries),
            two_sided_count=len(two_sided),
            missing_summary_count=len(chain.rows) - len(summaries),
            stale_summary_count=stale,
            ticker_samples=tuple(samples),
            quote_currencies=tuple(sorted({row.instrument.quote_currency for row in chain.rows})),
            settlement_currencies=tuple(
                sorted({row.instrument.settlement_currency for row in chain.rows})
            ),
            capability=capability,
            reason=reason,
        )
        assets.append(result)
        logger.info(
            "%s asset=%s instruments=%d two_sided=%d reason=%s",
            capability,
            asset,
            len(chain.rows),
            len(two_sided),
            reason,
        )
    return ProbeReport(
        created_at=datetime.now(UTC).isoformat(),
        endpoint=config.venue.ws_url,
        server_time_ms=server_ms,
        clock_offset_ms=offset_ms,
        round_trip_ms=round_trip_ms,
        authenticated_checks="PASSED_ON_THIS_CONNECTION"
        if authenticated
        else "NOT_RUN_NO_CREDENTIALS_REQUESTED",
        assets=tuple(assets),
    )


async def probe(
    config: AppConfig, authenticated: bool = False, chain_dir: Path | None = None
) -> ProbeReport:
    if config.system.mode == "live" or config.venue.key_profile != "testnet":
        raise TradingUnavailable("public and authenticated probes are testnet-only in milestone 1")
    # Persist limiter across reconnects; reconnecting must not replenish credits.
    limiter = EndpointLimiter(config.rate_limits)
    for attempt in range(config.venue.retry_attempts):
        try:
            async with WebSocketRpc(config.venue, limiter) as rpc:
                return await inspect_connection(rpc, config, authenticated, chain_dir)
        except TransportError:
            if authenticated or attempt + 1 == config.venue.retry_attempts:
                raise
            delay = min(config.venue.retry_base_s * 2**attempt, config.venue.retry_max_s)
            logger.warning(
                "TRANSPORT_RECONNECT attempt=%d delay_s=%.1f full_discovery_required=true",
                attempt + 1,
                delay,
            )
            await asyncio.sleep(delay)
    raise AssertionError("validated positive attempt count is unreachable")
