"""Portable independent Keltner proof regressions using only synthetic inputs."""

import importlib.util
import json
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

from selective_options.errors import IntegrityError
from selective_options.events import PointInTimeSnapshot
from selective_options.keltner_features import compute_keltner_features
from selective_options.store import DuckStore

SPEC = importlib.util.spec_from_file_location(
    "verify_keltner", Path(__file__).resolve().parents[1] / "scripts/verify_keltner.py"
)
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


def compute(snapshot):
    return compute_keltner_features(
        snapshot,
        source="synthetic_fixture",
        symbol="FIXTURE-BTC/USD",
        timeframe_s=300,
        max_latest_age_ms=probe.MAX_LATEST_AGE_MS,
        max_bars=probe.MAX_BARS,
    )


def check(report, snapshot):
    return probe.check_report(
        report, snapshot, source="synthetic_fixture", symbol="FIXTURE-BTC/USD", timeframe_s=300
    )


def forged_snapshot(snapshot, rows):
    forged = replace(snapshot, _events=())
    object.__setattr__(forged, "_events", tuple(rows))
    return forged


@pytest.fixture
async def saved(tmp_path):
    source = tmp_path / "source" / "synthetic-bars.duckdb"
    source.parent.mkdir()
    rows = probe.golden_snapshot().rows()[:3]
    from selective_options.events import make_event

    first = rows[0]
    partial = make_event(
        source=first.source,
        environment=first.environment,
        asset=first.asset,
        symbol=first.symbol,
        kind="bar",
        event_time_ms=first.event_time_ms,
        observed_at_ms=first.observed_at_ms,
        payload=first.payload() | {"complete": False},
    )
    store = await DuckStore.open(source)
    await store.append([partial, *rows[1:]])
    await store.close()
    return source


def policy(source):
    return {
        "expected_source_sha256": probe.file_digest(source),
        "source": "synthetic_fixture",
        "symbol": "FIXTURE-BTC/USD",
        "as_of_ms": 1_200_005,
        "history_start_ms": 900_000,
        "provenance": "synthetic_fixture",
    }


def test_exact_rational_synthetic_golden_and_partial_component_warmup():
    snapshot = probe.golden_snapshot()
    audit = check(compute(snapshot), snapshot)
    assert audit["selected_bars"] == audit["complete_bars"] == 37
    assert set(audit["feature_states"].values()) == {"AVAILABLE"}
    assert all("/" in value for value in audit["rational_reference"].values())
    short = PointInTimeSnapshot(
        snapshot.rows()[10].observed_at_ms, "BTC", "public_reference", snapshot.rows()[:11]
    )
    audit = check(compute(short), short)
    assert audit["feature_states"]["keltner_atr"] == "AVAILABLE"
    assert audit["feature_states"]["keltner_center"] == "WARMUP"
    assert audit["rational_reference"]["keltner_atr"] is not None
    assert audit["rational_reference"]["keltner_center"] is None


def test_real_api_cli_saved_replay_keeps_source_bytes_and_labels_fixture(saved, tmp_path):
    original = saved.read_bytes()
    output = tmp_path / "proof"
    result = probe.verify(saved, output, **policy(saved))
    assert result["verification_passed"]
    assert result == json.loads((output / "verification.json").read_text())
    assert saved.read_bytes() == original
    assert result["source_checks"][0]["blockers"] == ["INCOMPLETE_BAR"]
    assert set(result["source_checks"][1]["feature_states"].values()) == {"WARMUP"}
    assert all(item["data_kind"] == "synthetic_fixture" for item in result["source_checks"])
    assert result["source_checks"][1]["selected_bars"] == 2
    assert all(
        probe.file_digest(output / name) == digest
        for name, digest in result["artifacts_sha256"].items()
    )
    assert result["native_numeric_channel_established"] is result["trading_enabled"] is False
    with pytest.raises(ValueError, match="fresh output"):
        probe.verify(saved, output, **policy(saved))


@pytest.mark.parametrize(
    "case",
    [
        "value",
        "unit",
        "warmup",
        "reason",
        "membership",
        "required",
        "policy",
        "seed",
        "scope",
        "history",
        "input_ids",
        "approval",
    ],
)
def test_independent_auditor_refuses_tampered_report(case):
    snapshot = probe.golden_snapshot()
    report = compute(snapshot)
    features = list(report.features)
    if case == "value":
        features[0] = features[0].model_copy(update={"value": features[0].value + 1})
    elif case == "unit":
        features[0] = features[0].model_copy(update={"unit": "fraction"})
    elif case == "warmup":
        features[0] = features[0].model_copy(update={"state": "WARMUP", "value": None})
    elif case == "reason":
        features[0] = features[0].model_copy(update={"reason": "invented"})
    elif case == "membership":
        features = [features[0], features[0], *features[2:]]
    elif case == "required":
        features[0] = features[0].model_copy(update={"bars_required": 1})
    elif case == "policy":
        report = report.model_copy(update={"atr_multiplier": 1.5})
    elif case == "seed":
        report = report.model_copy(update={"ema_seed": "tail_reseed"})
    elif case == "scope":
        report = report.model_copy(update={"base": report.base.model_copy(update={"as_of_ms": 1})})
    elif case == "history":
        report = report.model_copy(
            update={"base": report.base.model_copy(update={"history_sha256": "0" * 64})}
        )
    elif case == "input_ids":
        report = report.model_copy(update={"input_event_ids": report.input_event_ids[1:]})
    else:
        report = report.model_copy(update={"trading_enabled": True})
    report = report.model_copy(update={"features": tuple(features)})
    with pytest.raises(IntegrityError, match="KELTNER_PROOF_"):
        check(report, snapshot)


@pytest.mark.parametrize("case", ["receipt", "event", "identity", "scope", "duplicate", "tied"])
def test_independent_snapshot_auditor_refuses_clock_identity_and_scope_tampering(case):
    snapshot = probe.golden_snapshot()
    rows = list(snapshot.rows())
    original = rows[0]
    if case == "receipt":
        rows[0] = original.model_copy(update={"observed_at_ms": snapshot.as_of_ms + 1})
    elif case == "event":
        rows[0] = original.model_copy(update={"event_time_ms": snapshot.as_of_ms + 1})
    elif case == "identity":
        rows[0] = original.model_copy(update={"event_id": "0" * 64})
    elif case == "scope":
        rows[0] = original.model_copy(update={"asset": "ETH"})
    elif case == "duplicate":
        rows.append(original)
    else:
        rows.append(original.model_copy(update={"event_id": "0" * 64}))
    with pytest.raises(IntegrityError, match="KELTNER_PROOF_"):
        check(compute(snapshot), forged_snapshot(snapshot, rows))


@pytest.mark.parametrize("tamper", ["returncode", "stdout", "artifact", "source", "code"])
def test_verifier_refuses_cli_artifact_source_and_code_changes(
    saved, tmp_path, monkeypatch, tamper
):
    runner = probe.subprocess.run
    codes = probe.code_hashes()
    calls = []
    if tamper == "code":

        def changed_code():
            calls.append(1)
            return codes if len(calls) == 1 else {**codes, "changed": "0" * 64}

        monkeypatch.setattr(probe, "code_hashes", changed_code)

    def altered(command, **kwargs):
        result = runner(command, **kwargs)
        if tamper == "returncode":
            return subprocess.CompletedProcess(command, 1, result.stdout, result.stderr)
        if tamper == "stdout":
            return subprocess.CompletedProcess(command, 0, "{}", result.stderr)
        if tamper == "artifact":
            artifact = Path(command[command.index("--output") + 1]) / "keltner.json"
            artifact.write_text("{}")
        if tamper == "source":
            monkeypatch.setattr(probe, "file_digest", lambda _: "changed")
        return result

    monkeypatch.setattr(probe.subprocess, "run", altered)
    output = tmp_path / "tampered"
    with pytest.raises(IntegrityError, match="KELTNER_PROOF_"):
        probe.verify(saved, output, **policy(saved))
    assert not (output / "verification.json").exists()


@pytest.mark.parametrize(
    "invalid",
    [
        {"expected_source_sha256": "wrong"},
        {"as_of_ms": True},
        {"history_start_ms": 0},
        {"timeframe_s": 60},
        {"provenance": "live"},
    ],
)
def test_invalid_proof_policy_does_not_create_output(saved, tmp_path, invalid):
    output = tmp_path / "invalid"
    with pytest.raises(ValueError):
        probe.verify(saved, output, **(policy(saved) | invalid))
    assert not output.exists()
