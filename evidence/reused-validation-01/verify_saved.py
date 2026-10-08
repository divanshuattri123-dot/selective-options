"""Exact saved-native-envelope replay against the immutable 0.3.32 wheel; no network."""

import asyncio
import hashlib
import importlib.util
import json
import sys
import zipfile
from pathlib import Path
from statistics import median
from time import perf_counter_ns

import duckdb

import selective_options.collectors as current
from selective_options import __version__
from selective_options.events import MarketEvent

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "evidence/receive-timing-public-01/events.duckdb"
WHEEL = ROOT / "../../work/dist/selective_options-0.3.32-py3-none-any.whl"
EXTRACT = ROOT / "../../work/baseline_0332"
REPORT = Path(__file__).with_name("report.json")


def load_baseline():
    reference = json.loads((ROOT / "evidence/receive-timing-release-0332.json").read_text())
    assert hashlib.sha256(WHEEL.read_bytes()).hexdigest() == reference["wheel_sha256"]
    with zipfile.ZipFile(WHEEL) as archive:
        assert archive.testzip() is None
        assert (
            "Version: 0.3.32"
            in archive.read("selective_options-0.3.32.dist-info/METADATA").decode().splitlines()
        )
        names = [
            name
            for name in archive.namelist()
            if name.startswith("selective_options/") and name.endswith(".py")
        ]
        assert len(names) == 52
        for name in names:
            parts = Path(name).parts
            assert len(parts) == 2 and ".." not in parts
            target = EXTRACT / name
            data = archive.read(name)
            if target.exists():
                assert target.read_bytes() == data
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
    spec = importlib.util.spec_from_file_location(
        "baseline0332",
        EXTRACT / "selective_options/__init__.py",
        submodule_search_locations=[str(EXTRACT / "selective_options")],
    )
    package = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = package
    spec.loader.exec_module(package)
    return importlib.import_module("baseline0332.collectors")


async def replay(module, rows, instruments, exact):
    registry = module.ChannelRegistry()
    for instrument in instruments:
        payload = instrument.payload()
        registry.add_instrument("BTC", instrument.symbol, payload["price_index"])
    delivered = []

    async def sink(row):
        delivered.append(row.model_dump())

    collector = module.DeribitCollector(module.AppConfig(), sink)
    original_clock = module.now_ms

    class Socket:
        def __init__(self, raw):
            self.raw = raw

        async def recv(self):
            return self.raw

    try:
        started = perf_counter_ns()
        for row, raw in rows:
            module.now_ms = lambda: row.observed_at_ms
            count = len(delivered)
            await collector._receive(Socket(raw), registry)
            if exact:
                assert delivered[count:] == [row.model_dump()]
        elapsed = perf_counter_ns() - started
    finally:
        module.now_ms = original_clock
    return elapsed, delivered


async def main():
    assert not REPORT.exists(), "immutable saved replay already exists"
    before = hashlib.sha256(SOURCE.read_bytes()).hexdigest()
    with duckdb.connect(str(SOURCE), read_only=True) as db:
        query = db.execute(
            "SELECT * FROM events WHERE source='deribit' AND environment='testnet' AND asset='BTC' AND kind IN ('instrument','ticker','book','trade','index') ORDER BY observed_at_ms,rowid LIMIT 50001"
        )
        columns = [item[0] for item in query.description]
        native = query.fetchall()
        assert len(native) <= 50000
        selected = [
            MarketEvent.model_validate(dict(zip(columns, row, strict=True))) for row in native
        ]
    instruments = [row for row in selected if row.kind == "instrument"]
    rows = []
    for row in selected:
        if row.kind == "instrument":
            continue
        channel = {
            "ticker": f"ticker.{row.symbol}.100ms",
            "book": f"book.{row.symbol}.none.20.100ms",
            "trade": f"trades.{row.symbol}.100ms",
            "index": f"deribit_price_index.{row.symbol}",
        }[row.kind]
        data = [row.payload()] if row.kind == "trade" else row.payload()
        raw = json.dumps(
            {
                "jsonrpc": "2.0",
                "method": "subscription",
                "params": {"channel": channel, "data": data},
            },
            separators=(",", ":"),
        )
        rows.append((row, raw))
    baseline = load_baseline()
    baseline_elapsed, original = await replay(baseline, rows, instruments, True)
    current_elapsed, optimized = await replay(current, rows, instruments, True)
    assert original == optimized
    sample = rows[:250]
    timings = {"baseline0332": [], "compiled_reuse": []}
    for cycle in range(3):
        order = (
            (("baseline0332", baseline), ("compiled_reuse", current))
            if cycle % 2 == 0
            else (("compiled_reuse", current), ("baseline0332", baseline))
        )
        for name, module in order:
            elapsed, _ = await replay(module, sample, instruments, True)
            timings[name].append(elapsed)
    assert before == hashlib.sha256(SOURCE.read_bytes()).hexdigest()
    report = {
        "version": __version__,
        "status": "EXACT_BASELINE_AND_CURRENT_SAVED_NATIVE_ENVELOPES_MATCH",
        "source": "receive-timing-public-01/events.duckdb",
        "source_sha256": before,
        "baseline_wheel_sha256": hashlib.sha256(WHEEL.read_bytes()).hexdigest(),
        "scope": "RECONSTRUCTED_NATIVE_PAYLOAD_ENVELOPES_WITH_ORIGINAL_RECEIPTS_NOT_ORIGINAL_WIRE_FRAMES",
        "native_stream_rows": len(rows),
        "instrument_rows": len(instruments),
        "every_event_field_payload_identity_and_clock_matches": True,
        "full_replay_elapsed_ns": {
            "baseline0332": baseline_elapsed,
            "compiled_reuse": current_elapsed,
        },
        "sample_rows": len(sample),
        "alternating_cycles": 3,
        "sample_elapsed_ns": timings,
        "sample_median_ns": {name: median(values) for name, values in timings.items()},
        "wall_measurements_are_machine_specific": True,
        "speed_threshold_used_as_test": False,
        "source_bytes_unchanged": True,
        "network_requests": 0,
        "wire_order_or_dropped_duplicates_restored": False,
        "sustained_capacity_verified": False,
        "milestone_2_complete": False,
        "trading_enabled": False,
    }
    REPORT.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


asyncio.run(main())
