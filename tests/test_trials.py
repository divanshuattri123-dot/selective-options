import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from selective_options.errors import IntegrityError
from selective_options.trials import (
    fail_trial,
    finish_trial,
    read_trials,
    run_registered,
    start_trial,
    trial_counts,
)


def test_registry_counts_every_variant_and_failed_or_unresolved_run(tmp_path):
    path = tmp_path / "trials.jsonl"
    first = start_trial(path, {"window": 8}, {"fixture": True})
    prefix = path.read_bytes()
    finish_trial(path, first, {"pnl": "-2.00"})
    assert path.read_bytes().startswith(prefix)
    second = start_trial(path, {"window": 21}, {"fixture": True})
    fail_trial(path, second, "IntegrityError")
    start_trial(path, {"window": 50}, {"fixture": True})
    assert trial_counts(path) == {
        "started": 3,
        "completed": 1,
        "failed": 1,
        "unresolved": 1,
        "trial_count_N": 3,
    }
    assert len(read_trials(path)) == 5


def test_start_is_durable_before_runner_and_result_summary(tmp_path):
    path = tmp_path / "trials.jsonl"

    def runner():
        rows = read_trials(path)
        assert len(rows) == 1 and rows[0].phase == "STARTED"
        return -2

    handle, result = run_registered(
        path, {"variant": "known-bad"}, {}, runner, lambda x: {"pnl": x}
    )
    assert result == -2
    assert read_trials(path)[-1].trial_id == handle.trial_id
    assert read_trials(path)[-1].result == {"pnl": -2}


@pytest.mark.parametrize("failure", [RuntimeError("do not log this"), KeyboardInterrupt()])
def test_runner_failure_remains_counted_without_payload(tmp_path, failure):
    path = tmp_path / "trials.jsonl"

    def runner():
        raise failure

    with pytest.raises(type(failure)):
        run_registered(path, {"variant": "bad"}, {}, runner, lambda result: {})
    assert read_trials(path)[-1].failure_type == type(failure).__name__
    assert b"do not log this" not in path.read_bytes()
    assert trial_counts(path)["failed"] == 1


def test_summary_failure_records_failed_trial(tmp_path):
    path = tmp_path / "trials.jsonl"

    def summarize(result):
        raise ValueError("broken summary")

    with pytest.raises(ValueError):
        run_registered(path, {}, {}, lambda: 2, summarize)
    assert trial_counts(path)["failed"] == 1


def test_abrupt_process_exit_preserves_started_trial(tmp_path):
    path = tmp_path / "trials.jsonl"
    code = (
        "import os,sys; from pathlib import Path; "
        "from selective_options.trials import start_trial; "
        "start_trial(Path(sys.argv[1]), {'variant':'crashed'}, {'fixture':True}); os._exit(31)"
    )
    outcome = subprocess.run([sys.executable, "-c", code, str(path)], capture_output=True)
    assert outcome.returncode == 31
    assert trial_counts(path)["unresolved"] == trial_counts(path)["trial_count_N"] == 1
    assert not Path(str(path) + ".lock").exists()


def test_exclusive_lock_refuses_conflicting_writer(tmp_path):
    path = tmp_path / "trials.jsonl"
    lock = Path(str(path) + ".lock")
    lock.write_text("manual inspection required", encoding="utf-8")
    with pytest.raises(IntegrityError, match="LOCKED"):
        start_trial(path, {}, {})
    assert not path.exists()
    assert lock.read_text(encoding="utf-8") == "manual inspection required"


@pytest.mark.parametrize("ending", [b" ", b"\n\n", b'{"unfinished"'])
def test_partial_or_blank_tail_refused_without_repair(tmp_path, ending):
    path = tmp_path / "trials.jsonl"
    start_trial(path, {}, {})
    path.write_bytes(path.read_bytes() + ending)
    before = path.read_bytes()
    with pytest.raises((IntegrityError, ValueError)):
        read_trials(path)
    with pytest.raises((IntegrityError, ValueError)):
        start_trial(path, {}, {})
    assert path.read_bytes() == before


def test_modified_or_reordered_chain_refused(tmp_path):
    path = tmp_path / "trials.jsonl"
    handle = start_trial(path, {"window": 8}, {})
    finish_trial(path, handle, {"pnl": "-2"})
    original = path.read_bytes()
    path.write_bytes(original.replace(b'"window":8', b'"window":9'))
    with pytest.raises(IntegrityError, match="CHAIN"):
        read_trials(path)
    path.write_bytes(b"\n".join(reversed(original.splitlines())) + b"\n")
    with pytest.raises(IntegrityError, match="CHAIN"):
        read_trials(path)


def test_duplicate_finish_and_wrong_parameter_handle_refused(tmp_path):
    path = tmp_path / "trials.jsonl"
    handle = start_trial(path, {}, {})
    bad = handle.model_copy(update={"parameters_sha256": "f" * 64})
    before = path.read_bytes()
    with pytest.raises(IntegrityError, match="FINISH"):
        finish_trial(path, bad, {})
    assert path.read_bytes() == before
    finish_trial(path, handle, {})
    before = path.read_bytes()
    with pytest.raises(IntegrityError, match="FINISH"):
        fail_trial(path, handle, "RuntimeError")
    assert path.read_bytes() == before


def test_record_overflow_and_nonfinite_parameters_do_not_append(tmp_path, monkeypatch):
    path = tmp_path / "trials.jsonl"
    with pytest.raises(ValueError):
        start_trial(path, {"bad": float("nan")}, {})
    assert not path.exists()
    monkeypatch.setattr("selective_options.trials.MAX_RECORD_BYTES", 30)
    with pytest.raises(IntegrityError, match="RECORD_BUDGET"):
        start_trial(path, {}, {})
    assert not path.exists()


def test_whole_file_and_record_count_bounds_refuse_append(tmp_path, monkeypatch):
    path = tmp_path / "trials.jsonl"
    start_trial(path, {}, {})
    before = path.read_bytes()
    monkeypatch.setattr("selective_options.trials.MAX_BYTES", len(before) - 1)
    with pytest.raises(IntegrityError, match="BYTE_BUDGET"):
        start_trial(path, {}, {})
    assert path.read_bytes() == before
    monkeypatch.setattr("selective_options.trials.MAX_BYTES", len(before) * 10)
    monkeypatch.setattr("selective_options.trials.MAX_RECORDS", 1)
    with pytest.raises(IntegrityError, match="RECORD_BUDGET"):
        start_trial(path, {}, {})
    assert path.read_bytes() == before


def test_short_write_is_not_retried_or_truncated(tmp_path, monkeypatch):
    path = tmp_path / "trials.jsonl"
    write = os.write

    def short(descriptor, content):
        return write(descriptor, content[:20])

    monkeypatch.setattr("selective_options.trials.os.write", short)
    with pytest.raises(IntegrityError, match="SHORT_WRITE"):
        start_trial(path, {}, {})
    assert path.stat().st_size == 20
    with pytest.raises(IntegrityError, match="INCOMPLETE"):
        read_trials(path)
    assert not Path(str(path) + ".lock").exists()


def test_journal_reads_missing_file_without_creating_it(tmp_path):
    path = tmp_path / "missing.jsonl"
    assert read_trials(path) == ()
    assert trial_counts(path)["trial_count_N"] == 0
    assert not path.exists()


def test_hashes_cover_provenance_and_actual_results(tmp_path):
    path = tmp_path / "trials.jsonl"
    handle = start_trial(path, {"latency_ms": 25}, {"input_sha256": "1" * 64})
    finish_trial(path, handle, {"pnl": "-2.05"})
    previous = "0" * 64
    for line in path.read_bytes().splitlines():
        record = json.loads(line)
        digest = record.pop("record_sha256")
        assert record["previous_sha256"] == previous
        encoded = json.dumps(
            record, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
        assert hashlib.sha256(encoded).hexdigest() == digest
        previous = digest
