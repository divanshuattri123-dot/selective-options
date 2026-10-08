"""Finite capped public testnet refresh check; never a full-universe or continuity proof."""

import argparse
import asyncio
import json
from pathlib import Path

import duckdb

from selective_options import __version__
from selective_options.config import AppConfig
from selective_options.events import MarketEvent
from selective_options.ingestion import capture
from selective_options.quality import FreshnessMonitor
from selective_options.store import complete_thread


def inspect_saved(output: Path) -> dict[str, object]:
    with duckdb.connect(str(output / "events.duckdb"), read_only=True) as db:
        result = db.execute("SELECT * FROM events ORDER BY observed_at_ms, rowid")
        columns = [c[0] for c in result.description]
        rows = tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
            for row in result.fetchall()
        )
        pending = db.execute(
            "SELECT count(*) FROM delivery_outbox WHERE delivered = FALSE"
        ).fetchone()
        assert pending is not None and pending[0] == len(rows)
    monitor = FreshnessMonitor(5000)
    controls: list[dict[str, object]] = []
    inactive_checks = 0
    for row in rows:
        monitor.observe(row)
        payload = row.payload()
        reason = payload.get("reason")
        if row.kind == "status" and isinstance(reason, str) and reason.startswith("STREAM_"):
            controls.append(
                {
                    "reason": reason,
                    "observed_at_ms": row.observed_at_ms,
                    "event_id": row.event_id,
                    "payload": payload,
                }
            )
            if reason == "STREAM_REFRESH_DUE":
                states = monitor.option_freshness(row.observed_at_ms, 300000)
                assert states and all(state["state"] == "STREAM_INACTIVE" for state in states)
                inactive_checks += len(states)
    subscribed = [row for row in controls if row["reason"] == "STREAM_SUBSCRIBED"]
    refreshes = [row for row in controls if row["reason"] == "STREAM_REFRESH_DUE"]
    return {
        "saved_rows": len(rows),
        "pending_outbox_rows": pending[0],
        "stream_controls": controls,
        "subscribed_sessions": len(subscribed),
        "scheduled_refreshes": len(refreshes),
        "inactive_quote_checks": inactive_checks,
        "trade_history_continuity_verified": False,
        "full_universe_stream_verified": False,
    }


async def verify(output: Path, duration_s: float = 110) -> dict[str, object]:
    if not 90 <= duration_s <= 180:
        raise ValueError("refresh verification duration must be 90..180 seconds")
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["BTC"]},
            "ingestion": {
                "stream_universe_refresh_interval_s": 60,
                "max_stream_instruments_per_asset": 2,
                "chain_poll_interval_s": 3600,
                "derivatives_enabled": False,
                "spot_trades_enabled": False,
                "reference_sources": [],
                "policy_news_sources": [],
            },
        }
    )
    result = await capture(config, output, duration_s)
    saved = await complete_thread(inspect_saved, output)
    assert saved["scheduled_refreshes"] == result["deribit_universe_refreshes"]
    assert saved["saved_rows"] == result["rows"]
    refresh_count, subscribed_count = saved["scheduled_refreshes"], saved["subscribed_sessions"]
    connection_count = result["deribit_connections"]
    assert isinstance(refresh_count, int) and isinstance(subscribed_count, int)
    assert isinstance(connection_count, int) and subscribed_count <= connection_count
    observed = refresh_count >= 1 and subscribed_count >= 2
    report = {
        "version": __version__,
        "status": "PUBLIC_CAPPED_REFRESH_VERIFIED" if observed else "REFRESH_CYCLE_NOT_OBSERVED",
        "stream_cap_per_asset": 2,
        "asset": "BTC",
        "duration_s": duration_s,
        "actual_listing_or_expiry_change_proven": False,
        "milestone_2_complete": False,
        "orders_submitted": 0,
        "trading_enabled": False,
        **saved,
    }
    await complete_thread(
        (output / "verification.json").write_text,
        json.dumps(report, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {key: value for key, value in report.items() if key != "stream_controls"}, indent=2
        )
    )
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(verify(args.output))
