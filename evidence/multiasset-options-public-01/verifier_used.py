"""Finite uncapped testnet subscription checks for explicitly selected assets."""

import argparse
import asyncio
import json
from pathlib import Path

import duckdb

from selective_options import __version__
from selective_options.config import AppConfig
from selective_options.events import MarketEvent
from selective_options.ingestion import capture
from selective_options.option_coverage import coverage_at
from selective_options.store import complete_thread


def read_saved(output: Path, cutoff: int) -> tuple[tuple[MarketEvent, ...], int]:
    with duckdb.connect(str(output / "events.duckdb"), read_only=True) as db:
        result = db.execute(
            "SELECT * FROM events WHERE event_time_ms <= ? AND observed_at_ms <= ?",
            [cutoff, cutoff],
        )
        columns = [c[0] for c in result.description]
        rows = tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
            for row in result.fetchall()
        )
        total = db.execute("SELECT count(*) FROM events").fetchone()
        pending = db.execute("SELECT count(*) FROM delivery_outbox WHERE NOT delivered").fetchone()
        assert total is not None and pending is not None and total[0] == pending[0]
    return rows, int(total[0])


async def verify(
    output: Path, duration_s: float = 75, *, assets: tuple[str, ...] = ("BTC",)
) -> dict[str, object]:
    if isinstance(duration_s, bool) or not 40 <= duration_s <= 180:
        raise ValueError("option coverage probe must be 40..180 seconds")
    cfg = AppConfig.model_validate(
        {
            "system": {"assets": assets},
            "ingestion": {
                "max_stream_instruments_per_asset": None,
                "subscription_batch_size": 100,
                "chain_poll_interval_s": 3600,
                "derivatives_enabled": False,
                "spot_trades_enabled": False,
                "reference_sources": [],
                "policy_news_sources": [],
            },
        }
    )
    if await complete_thread(output.exists):
        raise FileExistsError("verification requires a fresh output directory")
    failure = None
    try:
        captured = await capture(cfg, output, duration_s)
    except Exception as exc:
        failure = type(exc).__name__
        captured = json.loads(
            await complete_thread((output / "report.json").read_text, encoding="utf-8")
        )
    cutoff = captured["ended_at_ms"]
    assert isinstance(cutoff, int)
    rows, count = await complete_thread(read_saved, output, cutoff)
    coverages = [
        coverage_at(
            rows,
            asset,
            cutoff,
            int(cfg.data.max_staleness_s * 1000),
            int(cfg.ingestion.option_universe_max_age_s * 1000),
        )
        for asset in cfg.system.assets
    ]
    saved_views = captured["option_stream_coverage"]
    assert isinstance(saved_views, list)
    assert coverages == saved_views
    acknowledged = all(view.get("full_discovered_membership") is True for view in coverages)
    report: dict[str, object] = {
        "version": __version__,
        "status": "UNCAPPED_SUBSCRIPTIONS_OBSERVED"
        if failure is None and acknowledged
        else "UNCAPPED_COVERAGE_NOT_VERIFIED",
        "failure_type": failure,
        "assets": list(cfg.system.assets),
        "duration_s": duration_s,
        "stream_cap_per_asset": None,
        "closed_reload_coverage_matches": True,
        "saved_rows": count,
        "option_stream_coverage": coverages,
        "all_requested_nonempty_universes_acknowledged": acknowledged,
        "all_requested_legs_fresh": all(
            view["state"] == "SELECTED_FEEDS_OBSERVED" for view in coverages
        ),
        "configured_leg_max_age_ms": int(cfg.data.max_staleness_s * 1000),
        "configured_universe_max_age_ms": int(cfg.ingestion.option_universe_max_age_s * 1000),
        "all_required_assets_or_feeds_verified": False,
        "trade_history_continuity_verified": False,
        "sustained_capacity_verified": False,
        "milestone_2_complete": False,
        "trading_enabled": False,
        "orders_submitted": 0,
    }
    if len(coverages) == 1:
        # Preserve the original single-asset evidence schema.
        report.update(asset=cfg.system.assets[0], coverage=coverages[0])
    await complete_thread(
        (output / "verification.json").write_text,
        json.dumps(report, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                key: value
                for key, value in report.items()
                if key not in {"coverage", "option_stream_coverage"}
            },
            indent=2,
        )
    )
    print(
        json.dumps(
            [{key: value for key, value in view.items() if key != "legs"} for view in coverages],
            indent=2,
        )
    )
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--assets", nargs="+", choices=("BTC", "ETH", "SOL", "PAXG"), default=["BTC"]
    )
    parser.add_argument("--duration-s", type=float, default=75)
    args = parser.parse_args()
    asyncio.run(verify(args.output, args.duration_s, assets=tuple(args.assets)))
