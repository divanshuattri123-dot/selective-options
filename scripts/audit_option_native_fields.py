"""Finite read-only native field inventory from preserved option captures; no requests."""

import argparse
import hashlib
import json
import math
from collections import Counter
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import duckdb

from selective_options.events import MarketEvent
from selective_options.option_coverage import Universe, coverage_at

ASSETS = {"BTC", "ETH", "SOL", "PAXG"}
LIMITATIONS = [
    "Saved finite testnet observations, not production, simultaneous four-asset or sustained feeds.",
    "Native volume keys and values are retained without inferring units or time windows.",
    "No summary receipt is substituted for a ticker; no IV ranks, moneyness, surfaces or GEX computed.",
    "Field validity does not establish executable liquidity, historical continuity or all M2 feeds.",
    "SQL scans and engine memory are not bounded by the application row budget.",
]


def number(value: Any, lower: float | None = None, upper: float | None = None) -> bool:
    if type(value) not in (int, float):
        return False
    try:
        finite = math.isfinite(value)
    except OverflowError:
        return False
    return finite and (lower is None or value >= lower) and (upper is None or value <= upper)


def positive(value: Any) -> bool:
    return number(value) and value > 0


def text_value(value: Any) -> bool:
    return type(value) is str and bool(value.strip())


FIELD_RULES: dict[str, Callable[[Any], bool]] = {
    "mark_price": lambda v: number(v, 0),
    "mark_iv": lambda v: number(v, 0),
    "best_bid_price": lambda v: number(v, 0),
    "best_ask_price": lambda v: number(v, 0),
    "bid_iv": lambda v: number(v, 0),
    "ask_iv": lambda v: number(v, 0),
    "open_interest": lambda v: number(v, 0),
    "stats.volume": lambda v: number(v, 0),
    "greeks.delta": lambda v: number(v, -1, 1),
    "greeks.gamma": number,
    "greeks.vega": number,
    "greeks.theta": number,
    "underlying_price": positive,
    "index_price": positive,
    "underlying_index": text_value,
}
METADATA_RULES: dict[str, Callable[[Any], bool]] = {
    "strike": positive,
    "price_index": text_value,
    "base_currency": text_value,
    "quote_currency": text_value,
    "settlement_currency": text_value,
}


def field(body: dict[str, Any], path: str, valid: Callable[[Any], bool]) -> dict[str, Any]:
    """Never coerce values, default absence to null or flatten invalid nested containers."""
    current: Any = body
    traversed: list[str] = []
    for key in path.split("."):
        if current is None:
            return {"state": "NULL_PARENT", "at": ".".join(traversed), "value": None}
        if type(current) is not dict:
            return {"state": "INVALID_PARENT", "at": ".".join(traversed), "value": current}
        traversed.append(key)
        if key not in current:
            return {"state": "ABSENT", "at": ".".join(traversed)}
        current = current[key]
    return {
        "state": "NULL" if current is None else "VALID" if valid(current) else "INVALID",
        "value": current,
    }


def policy(asset: str, cutoff: int, leg_age: int, universe_age: int, max_rows: int) -> None:
    if (
        asset not in ASSETS
        or any(type(v) is not int or v <= 0 for v in (cutoff, leg_age, universe_age, max_rows))
        or max_rows > 200000
    ):
        raise ValueError("invalid finite native audit policy")


def audit_events(
    events: Iterable[MarketEvent],
    asset: str,
    cutoff: int,
    leg_age: int = 30000,
    universe_age: int = 3600000,
    max_rows: int = 100000,
) -> dict[str, Any]:
    policy(asset, cutoff, leg_age, universe_age, max_rows)
    rows = []
    for row in events:
        if (
            row.source == "deribit"
            and row.environment == "testnet"
            and row.event_time_ms <= cutoff
            and row.observed_at_ms <= cutoff
        ):
            rows.append(row)
            if len(rows) > max_rows:
                raise ValueError("native audit application row budget exceeded")
    view: dict[str, Any] = coverage_at(rows, asset, cutoff, leg_age, universe_age, max_rows)
    report: dict[str, Any] = {
        "asset": asset,
        "as_of_ms": cutoff,
        "coverage": view,
        "members": [],
        "field_counts": {},
        "all_selected_ticker_fields_valid": False,
        "full_native_chain_fresh": False,
        "milestone_2_complete": False,
        "trading_enabled": False,
        "new_network_requests": 0,
        "orders_submitted": 0,
        "limitations": LIMITATIONS,
    }
    if view["blocker"] is not None:
        report["status"] = "CONTEXT_OR_LEG_BLOCKED"
        return report
    by_id = {row.event_id: row for row in rows}
    manifest = Universe.model_validate(by_id[str(view["manifest_event_id"])].payload())
    bindings = dict(zip(manifest.instruments, manifest.metadata_event_ids, strict=True))
    counts: dict[str, Counter[str]] = {key: Counter() for key in (*FIELD_RULES, *METADATA_RULES)}
    valid_all = True
    for leg in view["legs"]:
        if leg["kind"] != "ticker":
            continue
        symbol = leg["symbol"]
        metadata = by_id[bindings[symbol]]
        ticker = by_id.get(leg["event_id"])
        inspected = {
            key: field(metadata.payload(), key, rule) for key, rule in METADATA_RULES.items()
        }
        inspected.update(
            {
                key: {"state": "MISSING_TICKER"}
                if ticker is None
                else field(ticker.payload(), key, rule)
                for key, rule in FIELD_RULES.items()
            }
        )
        for key, finding in inspected.items():
            counts[key][finding["state"]] += 1
            valid_all = valid_all and finding["state"] == "VALID"
        body: dict[str, Any] | None = None if ticker is None else ticker.payload()
        report["members"].append(
            {
                "symbol": symbol,
                "ticker_leg_state": leg["state"],
                "fields": inspected,
                "metadata_event": metadata.model_dump(mode="json"),
                "ticker_event": None if ticker is None else ticker.model_dump(mode="json"),
                "source_age_ms": None if ticker is None else cutoff - ticker.event_time_ms,
                "receipt_age_ms": None if ticker is None else cutoff - ticker.observed_at_ms,
                "native_stats_keys": sorted(body["stats"])
                if body is not None and type(body.get("stats")) is dict
                else [],
                "additional_volume_inventory": {
                    key: field({} if body is None else body, key, lambda v: number(v, 0))
                    for key in ("volume", "stats.volume_usd")
                },
            }
        )
    report["field_counts"] = {key: dict(sorted(value.items())) for key, value in counts.items()}
    report["all_selected_ticker_fields_valid"] = bool(report["members"]) and valid_all
    report["full_native_chain_fresh"] = bool(
        report["all_selected_ticker_fields_valid"]
        and view["state"] == "SELECTED_FEEDS_OBSERVED"
        and view["full_discovered_membership"]
    )
    report["status"] = (
        "FINITE_NATIVE_CHAIN_FRESH"
        if report["full_native_chain_fresh"]
        else "NATIVE_FIELDS_OR_LEGS_INCOMPLETE"
    )
    return report


def saved_rows(
    path: Path, asset: str, cutoff: int, leg_age: int, universe_age: int, max_rows: int
) -> list[MarketEvent]:
    """Read-only SQL projection retains every exact latest source/receipt pair tie."""
    policy(asset, cutoff, leg_age, universe_age, max_rows)
    with duckdb.connect(str(path), read_only=True) as db:

        def select(sql: str, params: list[Any], budget: int) -> list[MarketEvent]:
            result = db.execute(sql + " LIMIT ?", [*params, budget + 1])
            columns = [entry[0] for entry in result.description]
            values = result.fetchall()
            if len(values) > budget:
                raise ValueError("native audit application row budget exceeded")
            return [
                MarketEvent.model_validate(dict(zip(columns, row, strict=True))) for row in values
            ]

        context = select(
            "SELECT * FROM events WHERE source='deribit' AND environment='testnet' AND event_time_ms<=? AND observed_at_ms<=? AND ((asset=? AND kind='instrument') OR (asset='SYSTEM' AND kind='status'))",
            [cutoff, cutoff, asset],
            max_rows,
        )
        view: dict[str, Any] = coverage_at(context, asset, cutoff, leg_age, universe_age, max_rows)
        if view["blocker"] is not None:
            return context
        ack = next(row for row in context if row.event_id == view["stream_control_event_id"])
        starts = [
            row.observed_at_ms
            for row in context
            if row.payload().get("reason") == "STREAM_RESYNC"
            and type(row.payload().get("connection")) is int
            and row.payload().get("connection") == ack.payload()["connection"]
            and row.observed_at_ms <= ack.observed_at_ms
        ]
        assert len(set(starts)) == 1
        symbols = sorted({leg["symbol"] for leg in view["legs"]})
        legs = select(
            "SELECT * FROM events WHERE source='deribit' AND environment='testnet' AND asset=? AND kind IN ('ticker','book') AND event_time_ms<=? AND observed_at_ms<=? AND observed_at_ms>=? AND symbol IN (SELECT unnest(?)) QUALIFY dense_rank() OVER (PARTITION BY symbol,kind ORDER BY event_time_ms DESC,observed_at_ms DESC)=1",
            [asset, cutoff, cutoff, starts[0], symbols],
            max_rows - len(context),
        )
        return [*context, *legs]


def file_hash(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def audit_capture(capture: Path, assets: tuple[str, ...], output: Path) -> dict[str, Any]:
    if not assets or len(set(assets)) != len(assets) or not set(assets) <= ASSETS:
        raise ValueError("invalid saved capture assets")
    if output.exists():
        raise FileExistsError("native audit requires fresh output")
    report_path = capture / "report.json"
    saved = json.loads(report_path.read_text(encoding="utf-8"))
    cutoff = saved["ended_at_ms"]
    config = json.loads((capture / "config.sanitized.json").read_text(encoding="utf-8"))
    leg_age = int(config["data"]["max_staleness_s"] * 1000)
    universe_age = int(config["ingestion"]["option_universe_max_age_s"] * 1000)
    for asset in assets:
        policy(asset, cutoff, leg_age, universe_age, 100000)
    files = sorted(path for path in capture.iterdir() if path.is_file())
    before = {str(path.resolve()): file_hash(path) for path in files}
    reports = [
        audit_events(
            saved_rows(capture / "events.duckdb", asset, cutoff, leg_age, universe_age, 100000),
            asset,
            cutoff,
            leg_age,
            universe_age,
        )
        for asset in assets
    ]
    saved_views = {view["asset"]: view for view in saved["option_stream_coverage"]}
    for item in reports:
        if item["coverage"] != saved_views[item["asset"]]:
            raise ValueError("native audit coverage differs from preserved capture")
    after = {str(path.resolve()): file_hash(path) for path in files}
    if before != after:
        raise ValueError("original capture bytes changed during read-only audit")
    result = {
        "capture": str(capture.resolve()),
        "source_hashes": before,
        "original_bytes_unchanged": True,
        "saved_coverage_exact": True,
        "assets": reports,
        "new_network_requests": 0,
        "orders_submitted": 0,
        "milestone_2_complete": False,
        "trading_enabled": False,
    }
    output.mkdir(parents=True)
    (output / "report.json").write_text(
        json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--assets", nargs="+", choices=sorted(ASSETS), required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = audit_capture(args.capture, tuple(args.assets), args.output)
    print(
        json.dumps(
            [
                {
                    key: value
                    for key, value in item.items()
                    if key not in {"members", "coverage", "limitations"}
                }
                for item in result["assets"]
            ],
            indent=2,
        )
    )
