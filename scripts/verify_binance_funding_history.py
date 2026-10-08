"""Finite public Binance funding-history integrity check; no credentials or orders."""

import argparse
import asyncio
import json
from pathlib import Path

import aiohttp
import duckdb
from pydantic import JsonValue

from selective_options import __version__
from selective_options.archive import seal_archive
from selective_options.binance import BinanceCollector, Endpoint
from selective_options.binance_funding_history import collect_binance_funding_history
from selective_options.binance_funding_view import funding_history_at, funding_view_summary
from selective_options.bybit import FeedUnavailable
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent
from selective_options.funding_checkpoint import load_funding_seed
from selective_options.store import DuckStore


class RecordingCollector(BinanceCollector):
    def __init__(self, config: AppConfig) -> None:
        self.responses: list[dict[str, object]] = []
        self.requests_attempted = 0
        super().__init__(config, self.unused_sink)

    async def unused_sink(self, event: MarketEvent) -> None:
        raise RuntimeError("history collection must stage rows before publication")

    async def request(
        self, session: aiohttp.ClientSession, endpoint: Endpoint, params: dict[str, str]
    ) -> tuple[JsonValue, int]:
        self.requests_attempted += 1
        raw, received = await super().request(session, endpoint, params)
        self.responses.append(
            {"endpoint": endpoint, "params": params, "observed_at_ms": received, "response": raw}
        )
        return raw, received


async def verify(
    output: Path,
    asset: str = "BTC",
    hours: int = 48,
    page_size: int = 200,
    max_pages: int = 10,
    *,
    resume_from: Path | None = None,
) -> None:
    if type(hours) is not int or not 1 <= hours <= 720:
        raise ValueError("hours must be 1..720")
    config = AppConfig.model_validate({"system": {"assets": [asset]}})
    end = now_ms()
    start = end - hours * 3_600_000
    seed = (
        await load_funding_seed(
            resume_from,
            asset=asset,
            symbol=asset + "USDT",
            start_ms=start,
            end_ms=end,
            as_of_ms=end,
        )
        if resume_from is not None
        else ()
    )
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    store = await DuckStore.open(output / "events.duckdb")
    collector = RecordingCollector(config)
    failure: BaseException | None = None
    observations: tuple[MarketEvent, ...] = ()
    phase = "FETCH_AND_VALIDATE"
    report: dict[str, object] = {
        "version": __version__,
        "asset": asset,
        "start_ms": start,
        "end_ms": end,
        "page_size": page_size,
        "max_pages": max_pages,
        "pit_checks": 0,
        "seed_rows_validated": len(seed),
        "seed_rows_copied": 0,
        "seed_committed_reload_verified": False,
        "seed_scope": "All valid observed revisions within the requested window; rows outside it remain in the original archive",
        "repeat_deduplication_verified": False,
        "historical_schedule_completeness": "NOT_PROVEN",
        "historical_interval_available": False,
        "publication_time_available": False,
        "finality_verified": False,
        "observation_identity": "Includes actual poll receipt; exact poll replay deduplicates; repeated downloads are observations, not extra settlements",
        "orders_submitted": 0,
        "trading_enabled": False,
        "paper_trading_started": False,
    }
    try:
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                plan = await collect_binance_funding_history(
                    collector,
                    session,
                    asset,
                    start_ms=start,
                    end_ms=end,
                    page_size=page_size,
                    max_pages=max_pages,
                )
            observations = plan.events
            combined = (*seed, *observations)
            # Use the raising projection; blocked evidence must never publish as success.
            funding_history_at(
                combined,
                asset=asset,
                symbol=asset + "USDT",
                start_ms=start,
                end_ms=end,
                as_of_ms=now_ms(),
            )
            phase = "PUBLICATION_AND_PIT_CHECKS"
            await store.append(list(combined))
            await store.append(list(combined))
            for row in observations:
                before = (
                    await store.snapshot(asset, row.observed_at_ms - 1, "public_reference")
                ).rows()
                at = (await store.snapshot(asset, row.observed_at_ms, "public_reference")).rows()
                assert row.event_id not in {e.event_id for e in before}
                assert row in at
            report.update(
                status="PUBLIC_SETTLEMENT_ARCHIVE_VERIFIED"
                if observations
                else "NO_SETTLEMENTS_RETURNED",
                pages=plan.pages,
                termination=plan.termination,
                pit_checks=len(observations),
                repeat_deduplication_verified=bool(combined),
                seed_rows_copied=len(seed),
            )
        except BaseException as exc:
            failure = exc
            report.update(
                status="FAILED",
                failure_phase=phase,
                failure_type=type(exc).__name__,
                failure_reason=exc.reason
                if isinstance(exc, FeedUnavailable)
                else str(exc)
                if isinstance(exc, IntegrityError)
                else None,
                failure_code=exc.code if isinstance(exc, FeedUnavailable) else None,
            )
        count = await store.export_parquet(output / "events.parquet")
        archive = await seal_archive(output / "events.parquet", count)
        all_rows = (await store.snapshot(asset, now_ms(), "public_reference")).rows()
        with duckdb.connect() as db:
            result = db.execute("SELECT * FROM read_parquet(?)", [str(output / "events.parquet")])
            columns = [c[0] for c in result.description]
            restored = [
                MarketEvent.model_validate(dict(zip(columns, r, strict=True)))
                for r in result.fetchall()
            ]
        assert {e.event_id: e for e in all_rows} == {e.event_id: e for e in restored}
        assert count == len(all_rows)
        if failure is None:
            assert count == len({e.event_id for e in (*seed, *observations)})
            assert all(row in all_rows for row in seed)
        report.update(
            rows=count,
            seed_rows_copied=sum(row in all_rows for row in seed),
            funding_revision_view=funding_view_summary(
                all_rows,
                asset=asset,
                symbol=asset + "USDT",
                start_ms=start,
                end_ms=end,
                as_of_ms=now_ms(),
            ),
            archive_exact_match=True,
            parquet_sha256=archive.sha256,
            public_requests_attempted=collector.requests_attempted,
            public_responses_accepted=len(collector.responses),
            ended_at_ms=now_ms(),
        )
        for name, content in [("responses.json", collector.responses), ("report.json", report)]:
            await asyncio.to_thread(
                (output / name).write_text, json.dumps(content, indent=2) + "\n", encoding="utf-8"
            )
    finally:
        await store.close()
    if failure is None and seed:
        try:
            reloaded = await load_funding_seed(
                output / "events.duckdb",
                asset=asset,
                symbol=asset + "USDT",
                start_ms=start,
                end_ms=end,
                as_of_ms=now_ms(),
            )
            assert {r.event_id: r for r in reloaded} == {
                r.event_id: r for r in (*seed, *observations)
            }
            report["seed_committed_reload_verified"] = True
        except BaseException as exc:
            failure = exc
            report.update(
                status="FAILED", failure_phase="COMMITTED_RELOAD", failure_type=type(exc).__name__
            )
        await asyncio.to_thread(
            (output / "report.json").write_text,
            json.dumps(report, indent=2) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(report, indent=2))
    if failure:
        raise failure


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--asset", choices=("BTC", "ETH", "SOL", "PAXG"), default="BTC")
    parser.add_argument("--hours", type=int, default=48)
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument(
        "--resume-from",
        type=Path,
        help="Validate and retain scoped committed observations before fetching; use a fresh output directory",
    )
    args = parser.parse_args()
    asyncio.run(
        verify(
            args.output,
            args.asset,
            args.hours,
            args.page_size,
            args.max_pages,
            resume_from=args.resume_from,
        )
    )
