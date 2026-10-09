"""Portable verifier regressions on explicitly synthetic OHLC/database inputs."""

import importlib.util
import json
import subprocess
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from selective_options.errors import IntegrityError
from selective_options.events import PointInTimeSnapshot, make_event
from selective_options.range_volatility import compute_range_volatility
from selective_options.store import DuckStore

SPEC = importlib.util.spec_from_file_location(
    "verify_range_volatility",
    Path(__file__).resolve().parents[1] / "scripts/verify_range_volatility.py",
)
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


def changed(row, **payload):
    return make_event(
        source=row.source,
        environment=row.environment,
        asset=row.asset,
        symbol=row.symbol,
        kind=row.kind,
        event_time_ms=row.event_time_ms,
        observed_at_ms=row.observed_at_ms,
        payload=row.payload() | payload,
    )


def compute(snapshot):
    return compute_range_volatility(
        snapshot,
        source="synthetic_fixture",
        symbol="FIXTURE-BTC/USD",
        timeframe_s=300,
        windows=probe.WINDOWS,
        max_latest_age_ms=probe.MAX_LATEST_AGE_MS,
        max_bars=probe.MAX_BARS,
        annualization_periods_per_year=probe.ANNUALIZATION,
    )


def forged_snapshot(snapshot, rows):
    """Deliberately bypass construction to exercise the verifier's independent checks."""
    forged = replace(snapshot, _events=())
    object.__setattr__(forged, "_events", tuple(rows))
    return forged


def forged_payload(row, **changes):
    return row.model_copy(
        update={
            "payload_json": json.dumps(
                row.payload() | changes, sort_keys=True, separators=(",", ":")
            )
        }
    )


def check(report, snapshot):
    return probe.check_report(
        report, snapshot, source="synthetic_fixture", symbol="FIXTURE-BTC/USD", timeframe_s=300
    )


@pytest.fixture
async def saved(tmp_path):
    source = tmp_path / "source" / "synthetic-bars.duckdb"
    source.parent.mkdir()
    rows = probe.golden_snapshot().rows()
    rows = (changed(rows[0], complete=False), *rows[1:])
    store = await DuckStore.open(source)
    await store.append(rows)
    await store.close()
    return source


def policy(source):
    return {
        "expected_source_sha256": probe.file_digest(source),
        "source": "synthetic_fixture",
        "symbol": "FIXTURE-BTC/USD",
        "as_of_ms": 900_010,
        "history_start_ms": 600_000,
        "provenance": "synthetic_fixture",
    }


def test_analytical_golden_decimal_2_3_available_20_warmup():
    snapshot = probe.golden_snapshot()
    report = compute(snapshot)
    audit = check(report, snapshot)
    assert audit["selected_bars"] == audit["complete_bars"] == 3
    assert audit["partial_bars"] == 0
    assert [window["state"] for window in audit["windows"]] == ["AVAILABLE", "AVAILABLE", "WARMUP"]
    for estimator in probe.ESTIMATORS:
        variance, volatility = probe.decimal_reference(snapshot.rows(), estimator)
        assert variance > 0 and volatility > 0
        assert report.feature(f"{estimator}_variance_per_bar_3").value == pytest.approx(
            float(variance), rel=1e-12
        )
        assert report.feature(f"{estimator}_vol_3").value == pytest.approx(
            float(volatility), rel=1e-12
        )
    assert probe.ANNUALIZATION == 365 * 24 * 12
    assert all(row.source == "synthetic_fixture" for row in snapshot.rows())


def test_actual_api_cli_replay_preserves_source_and_honest_scopes(saved, tmp_path, monkeypatch):
    original = saved.read_bytes()
    runner = probe.subprocess.run
    calls = []

    def observe(command, **kwargs):
        calls.append((command, kwargs))
        return runner(command, **kwargs)

    monkeypatch.setattr(probe.subprocess, "run", observe)
    output = tmp_path / "proof"
    result = probe.verify(saved, output, **policy(saved))
    assert saved.read_bytes() == original
    assert not saved.with_suffix(".duckdb.wal").exists()
    assert json.loads((output / "verification.json").read_text()) == result
    assert result["data_kind"] == "synthetic_fixture"
    assert result["research_api_runs"] == result["actual_cli_runs"] == len(calls) == 2
    full, segment = result["source_scopes"]
    assert full["explicit_history_start_ms"] is None
    assert full["selected_bars"] == 3 and full["partial_bars"] == 1
    assert full["blockers"] == ["INCOMPLETE_BAR"]
    assert all(window["state"] == "UNAVAILABLE" for window in full["windows"])
    assert segment["explicit_history_start_ms"] == 600_000
    assert segment["selected_bars"] == segment["complete_bars"] == 2
    assert [window["state"] for window in segment["windows"]] == ["AVAILABLE", "WARMUP", "WARMUP"]
    assert result["synthetic_golden"]["data_kind"] == "synthetic_analytical_golden"
    assert [window["state"] for window in result["synthetic_golden"]["checks"]["windows"]] == [
        "AVAILABLE",
        "AVAILABLE",
        "WARMUP",
    ]
    assert result["decimal_comparison_absolute_tolerance"] == 0
    assert result["annualization_calendar_verified"] is False
    for name in (
        "historical_strategy_validation",
        "complete_7_14_30_day_history_verified",
        "current_feed_health_verified",
        "milestone_2_complete",
        "ready_for_signals",
        "trading_enabled",
    ):
        assert result[name] is False
    assert (
        result["new_public_requests"]
        == result["orders_submitted"]
        == result["new_strategy_trials"]
        == 0
    )
    for command, kwargs in calls:
        assert command[:4] == [
            probe.sys.executable,
            "-m",
            "selective_options.cli",
            "research-range-volatility",
        ]
        assert kwargs["timeout"] == 60 and kwargs["check"] is False
    for name, digest in result["result_artifact_sha256"].items():
        assert probe.file_digest(output / name) == digest
    assert len(result["result_artifact_sha256"]) == 5


@pytest.mark.parametrize("blocker", ["partial", "gap", "stale", "empty"])
def test_independent_audit_preserves_full_history_refusals(blocker):
    snapshot = probe.golden_snapshot()
    rows = snapshot.rows()
    if blocker == "partial":
        rows = (changed(rows[0], complete=False), *rows[1:])
    elif blocker == "gap":
        rows = (rows[0], rows[2])
    elif blocker == "empty":
        rows = ()
    cutoff = snapshot.as_of_ms + 700_000 if blocker == "stale" else snapshot.as_of_ms
    selected = PointInTimeSnapshot(cutoff, snapshot.asset, snapshot.environment, rows)
    report = compute(selected)
    audit = check(report, selected)
    expected = {
        "partial": "INCOMPLETE_BAR",
        "gap": "MISSING_INTERIOR_INTERVAL",
        "stale": "STALE_LATEST_BAR",
        "empty": "NO_CLOSED_BARS",
    }[blocker]
    assert audit["blockers"] == [expected]
    assert all(window["state"] == "UNAVAILABLE" for window in audit["windows"])
    assert report.input_event_ids == ()


@pytest.mark.parametrize(
    "tamper",
    [
        "formula",
        "history",
        "count",
        "scope",
        "input_ids",
        "reference",
        "feature_names",
        "units",
        "value",
        "warmup_value",
        "unsupported",
    ],
)
def test_independent_audit_refuses_tampered_reports(tamper):
    snapshot = probe.golden_snapshot()
    report = compute(snapshot)
    if tamper == "formula":
        report = report.model_copy(update={"formula_version": "forged-formula"})
    elif tamper in ("history", "count", "scope"):
        update = (
            {"history_sha256": "0" * 64}
            if tamper == "history"
            else {"bars_available": 2}
            if tamper == "count"
            else {"source": "forged-source"}
        )
        report = report.model_copy(update={"base": report.base.model_copy(update=update)})
    elif tamper == "input_ids":
        report = report.model_copy(update={"input_event_ids": report.input_event_ids[:-1]})
    elif tamper == "reference":
        reference = report.window_references[0].model_copy(update={"history_sha256": "0" * 64})
        report = report.model_copy(
            update={"window_references": (reference, *report.window_references[1:])}
        )
    elif tamper in ("feature_names", "units", "value"):
        update = (
            {"name": "invented"}
            if tamper == "feature_names"
            else {"unit": "price"}
            if tamper == "units"
            else {"value": report.features[0].value * 1.1}
        )
        feature = report.features[0].model_copy(update=update)
        report = report.model_copy(update={"features": (feature, *report.features[1:])})
    elif tamper == "warmup_value":
        feature = report.features[-1].model_copy(update={"value": 0.0})
        report = report.model_copy(update={"features": (*report.features[:-1], feature)})
    else:
        report = report.model_copy(update={"ready_for_signals": True})
    with pytest.raises(IntegrityError, match="RANGE_PROOF_"):
        check(report, snapshot)


@pytest.mark.parametrize("field", ["event_time_ms", "observed_at_ms"])
def test_independent_audit_refuses_either_future_clock(field):
    snapshot = probe.golden_snapshot()
    row = snapshot.rows()[0].model_copy(update={field: snapshot.as_of_ms + 1})
    forged = forged_snapshot(snapshot, (row, *snapshot.rows()[1:]))
    with pytest.raises(IntegrityError, match="NATIVE_IDENTITY_OR_CLOCKS"):
        check(compute(snapshot), forged)


def test_independent_audit_refuses_duplicate_tied_bar_and_invalid_ohlc():
    snapshot = probe.golden_snapshot()
    duplicated = replace(snapshot, _events=(*snapshot.rows(), snapshot.rows()[0]))
    with pytest.raises(IntegrityError, match="DUPLICATE_OR_TIED_INPUT"):
        check(compute(snapshot), duplicated)
    wrong = forged_payload(snapshot.rows()[0], low=109.0)
    malformed = forged_snapshot(snapshot, (wrong, *snapshot.rows()[1:]))
    with pytest.raises(IntegrityError, match="NATIVE_IDENTITY_OR_CLOCKS"):
        check(compute(snapshot), malformed)


@pytest.mark.parametrize(
    "update",
    [
        {"expected_source_sha256": "bad"},
        {"expected_source_sha256": "F" * 64},
        {"history_start_ms": True},
        {"history_start_ms": 0},
        {"history_start_ms": 1_000_000},
        {"as_of_ms": True},
        {"as_of_ms": 0},
        {"timeframe_s": 900},
        {"timeframe_s": True},
        {"provenance": "unlabelled"},
    ],
)
def test_invalid_verifier_policy_refuses_before_file_access(tmp_path, monkeypatch, update):
    monkeypatch.setattr(
        probe, "_path", lambda *args, **kwargs: pytest.fail("invalid policy touched a path")
    )
    output = tmp_path / "proof"
    kwargs = {
        "expected_source_sha256": "0" * 64,
        "as_of_ms": 900_010,
        "history_start_ms": 600_000,
        **update,
    }
    with pytest.raises(ValueError):
        probe.verify(tmp_path / "missing.duckdb", output, **kwargs)
    assert not output.exists()


def test_missing_native_source_is_not_replaced_with_fixture(tmp_path):
    output = tmp_path / "proof"
    with pytest.raises(FileNotFoundError):
        probe.verify(tmp_path / "missing-native.duckdb", output)
    assert not output.exists()


@pytest.mark.parametrize("failure", ["hash", "wal", "budget", "bytes", "output", "inside_source"])
def test_source_and_publication_guards_refuse_without_modifying_source(
    saved, tmp_path, monkeypatch, failure
):
    before = saved.read_bytes()
    output = tmp_path / "proof"
    kwargs = policy(saved)
    expected_error = IntegrityError
    if failure == "hash":
        kwargs["expected_source_sha256"] = "0" * 64
    elif failure == "wal":
        saved.with_suffix(".duckdb.wal").write_bytes(b"EXPLICIT_SYNTHETIC_UNCLOSED_WAL")
    elif failure == "budget":
        monkeypatch.setattr(probe, "MAX_BARS", 2)
    elif failure == "bytes":
        monkeypatch.setattr(probe, "MAX_SOURCE_BYTES", 1)
        monkeypatch.setattr(
            probe, "file_digest", lambda path: pytest.fail("oversized source was hashed")
        )
    elif failure == "output":
        output.mkdir()
        (output / "operator-marker").write_bytes(b"preserve")
        expected_error = ValueError
    else:
        output = saved.parent / "new-proof"
        expected_error = ValueError
    with pytest.raises(expected_error):
        probe.verify(saved, output, **kwargs)
    assert saved.read_bytes() == before
    if failure == "output":
        assert (output / "operator-marker").read_bytes() == b"preserve"
    else:
        assert not output.exists()


@pytest.mark.parametrize("failure", ["cli_exit", "cli_parity", "source_changed", "code_changed"])
def test_failed_verification_never_publishes_success(saved, tmp_path, monkeypatch, failure):
    before = saved.read_bytes()
    output = tmp_path / "proof"
    if failure.startswith("cli_"):
        monkeypatch.setattr(
            probe,
            "_run_cli",
            lambda command: subprocess.CompletedProcess(
                command,
                2 if failure == "cli_exit" else 0,
                stdout="{}",
                stderr="labelled fixture failure",
            ),
        )
    elif failure == "source_changed":
        original = probe.file_digest
        calls = 0

        def changed_digest(path):
            nonlocal calls
            if path == saved.resolve():
                calls += 1
                if calls >= 2:
                    return "0" * 64
            return original(path)

        monkeypatch.setattr(probe, "file_digest", changed_digest)
    else:
        values = iter(({"fixture-source": "before"}, {"fixture-source": "changed"}))
        monkeypatch.setattr(probe, "code_hashes", lambda: next(values))
    with pytest.raises(IntegrityError, match="RANGE_PROOF_"):
        probe.verify(saved, output, **policy(saved))
    assert not (output / "verification.json").exists()
    assert saved.read_bytes() == before


def test_decimal_reference_refuses_empty_unknown_and_negative_gk():
    snapshot = probe.golden_snapshot()
    with pytest.raises(ValueError):
        probe.decimal_reference((), "parkinson")
    with pytest.raises(ValueError):
        probe.decimal_reference(snapshot.rows(), "unknown")
    # Explicit malformed arithmetic input, never a plausible native OHLC sample.
    invalid = forged_payload(snapshot.rows()[0], high=101.0, low=100.0, open=1.0, close=100.0)
    with pytest.raises(IntegrityError, match="REFERENCE_NEGATIVE_VARIANCE"):
        probe.decimal_reference((invalid,), "garman_klass")
    assert probe._close(0.0, Decimal("1e-30")) is False


def test_script_main_forwards_explicit_parameters_without_secret_inputs(
    tmp_path, monkeypatch, capsys
):
    recorded = []

    def fixture_verify(database, output, **kwargs):
        recorded.append((database, output, kwargs))
        return {"status": "EXPLICIT_FIXTURE", "actual_cli_runs": 2}

    monkeypatch.setattr(probe, "verify", fixture_verify)
    monkeypatch.setattr(
        probe.sys,
        "argv",
        [
            "verify_range_volatility",
            "--database",
            str(tmp_path / "saved.duckdb"),
            "--source-sha256",
            "a" * 64,
            "--output",
            str(tmp_path / "proof"),
            "--as-of-ms",
            "900010",
            "--history-start-ms",
            "600000",
            "--provenance",
            "synthetic_fixture",
        ],
    )
    probe.main()
    assert recorded[0][2]["expected_source_sha256"] == "a" * 64
    assert recorded[0][2]["provenance"] == "synthetic_fixture"
    assert json.loads(capsys.readouterr().out) == {
        "status": "EXPLICIT_FIXTURE",
        "actual_cli_runs": 2,
        "trading_enabled": False,
    }
