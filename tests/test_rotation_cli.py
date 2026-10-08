"""CLI integration for immutable both-clock rotation and complete restore."""

import asyncio
import hashlib
import json
from pathlib import Path
from unittest.mock import AsyncMock

import duckdb
import pytest

from selective_options.cli import main, parser
from selective_options.config import AppConfig
from selective_options.events import make_event
from selective_options.store import DuckStore


def invoke(monkeypatch, arguments):
    monkeypatch.setattr("sys.argv", ["selective-options", *arguments])
    return main()


def test_rotation_cli_forwards_explicit_cutoff_budgets(monkeypatch, tmp_path, capsys):
    operation = AsyncMock(return_value={"trading_enabled": False})
    monkeypatch.setattr("selective_options.cli.rotate_archive", operation)
    result = invoke(
        monkeypatch,
        [
            "rotate-archive",
            "--database",
            "source.duckdb",
            "--output",
            str(tmp_path / "chunks"),
            "--as-of-ms",
            "3000",
            "--partition-ms",
            "1000",
            "--max-partitions",
            "2",
            "--batch-size",
            "1",
        ],
    )
    assert result == 0 and json.loads(capsys.readouterr().out)["trading_enabled"] is False
    assert operation.await_args.args == (Path("source.duckdb"), tmp_path / "chunks", 3000)
    assert operation.await_args.kwargs == {
        "partition_ms": 1000,
        "batch_size": 1,
        "max_partitions": 2,
    }


def test_restore_rotation_cli_uses_configured_storage_policy(monkeypatch, tmp_path, capsys):
    config = AppConfig(ingestion={"storage_batch_size": 7}, data={"max_clock_skew_s": 3})
    monkeypatch.setattr("selective_options.cli.load_config", lambda *args: config)
    operation = AsyncMock(return_value={"trading_enabled": False})
    monkeypatch.setattr("selective_options.cli.restore_rotation", operation)
    assert (
        invoke(
            monkeypatch,
            [
                "restore-rotation",
                "--catalog",
                "catalog.json",
                "--output",
                str(tmp_path / "restored"),
            ],
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["trading_enabled"] is False
    assert operation.await_args.args == (Path("catalog.json"), tmp_path / "restored")
    assert operation.await_args.kwargs == {"batch_size": 7, "max_skew_ms": 3000}


def test_explicit_cutoff_required_and_optional_defaults_stay_offline():
    with pytest.raises(SystemExit):
        parser().parse_args(["rotate-archive", "--database", "source", "--output", "new"])
    args = parser().parse_args(
        ["rotate-archive", "--database", "source", "--output", "new", "--as-of-ms", "3000"]
    )
    assert args.partition_ms == 86400000 and args.max_partitions == 366 and args.batch_size is None


def test_real_cli_roundtrip_withholds_each_future_clock_and_requeues_acknowledged_event(
    tmp_path, monkeypatch, capsys
):
    source = tmp_path / "quote's source.duckdb"
    data = [
        make_event(
            source="cli_rotation_fixture",
            environment="public_reference",
            asset="BTC",
            symbol="FIXTURE",
            kind="status",
            event_time_ms=e,
            observed_at_ms=o,
            payload={"provenance": "synthetic CLI availability boundary", "case": i},
        )
        for i, (e, o) in enumerate([(999, 1001), (2999, 2999), (3001, 2999), (2999, 4001)])
    ]

    async def create():
        store = await DuckStore.open(source)
        try:
            await store.append(data, exact=True)
            await store.acknowledge_delivery([data[0].event_id])
        finally:
            await store.close()

    asyncio.run(create())
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    chunks = tmp_path / "quote's chunks"
    assert (
        invoke(
            monkeypatch,
            [
                "rotate-archive",
                "--database",
                str(source),
                "--output",
                str(chunks),
                "--as-of-ms",
                "3000",
                "--partition-ms",
                "1000",
                "--max-partitions",
                "2",
                "--batch-size",
                "1",
            ],
        )
        == 0
    )
    rotated = json.loads(capsys.readouterr().out)
    assert rotated["trading_enabled"] is False
    assert (chunks / "catalog.json").is_file()
    restored = tmp_path / "restored"
    assert (
        invoke(
            monkeypatch,
            [
                "restore-rotation",
                "--catalog",
                str(chunks / "catalog.json"),
                "--output",
                str(restored),
                "--batch-size",
                "1",
            ],
        )
        == 0
    )
    report = json.loads(capsys.readouterr().out)
    assert report["trading_enabled"] is False
    with duckdb.connect(str(source), read_only=True) as conn:
        expected = conn.execute(
            "SELECT * FROM events WHERE event_time_ms<=3000 AND observed_at_ms<=3000 ORDER BY event_id"
        ).fetchall()
        assert len(expected) == 2
        assert conn.execute(
            "SELECT delivered FROM delivery_outbox WHERE event_id=?", [data[0].event_id]
        ).fetchone() == (True,)
    with duckdb.connect(str(restored / "events.duckdb"), read_only=True) as conn:
        assert conn.execute("SELECT * FROM events ORDER BY event_id").fetchall() == expected
        assert conn.execute(
            "SELECT count(*) FROM delivery_outbox WHERE NOT delivered"
        ).fetchone() == (2,)
        for row in data[:2]:
            at = max(row.event_time_ms, row.observed_at_ms)
            assert conn.execute(
                "SELECT count(*) FROM events WHERE event_id=? AND event_time_ms<=? AND observed_at_ms<=?",
                [row.event_id, at - 1, at - 1],
            ).fetchone() == (0,)
            assert conn.execute(
                "SELECT count(*) FROM events WHERE event_id=? AND event_time_ms<=? AND observed_at_ms<=?",
                [row.event_id, at, at],
            ).fetchone() == (1,)
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before
    # Existing immutable catalogs/destinations are refused; source remains unchanged.
    assert (
        invoke(
            monkeypatch,
            [
                "rotate-archive",
                "--database",
                str(source),
                "--output",
                str(chunks),
                "--as-of-ms",
                "3000",
            ],
        )
        == 2
    )
    assert (
        invoke(
            monkeypatch,
            [
                "restore-rotation",
                "--catalog",
                str(chunks / "catalog.json"),
                "--output",
                str(restored),
            ],
        )
        == 2
    )
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before
