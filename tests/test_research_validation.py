"""Independent publication, path and journal uncertainty regressions."""

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from selective_options import research, trials
from selective_options.errors import IntegrityError

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "backtest-known-bad.json"


def request_file(tmp_path):
    path = tmp_path / "input.json"
    path.write_bytes(EXAMPLE.read_bytes())
    return path


@pytest.mark.parametrize("alias", [False, True])
def test_report_journal_collision_refuses_before_start_or_output(tmp_path, monkeypatch, alias):
    source = request_file(tmp_path)
    source_before = source.read_bytes()
    output = tmp_path / "result"
    registry = output / "backtest.json"
    if alias:
        ancestor = tmp_path / "existing"
        ancestor.mkdir()
        registry = ancestor / ".." / "result" / "backtest.json"
    called = []
    monkeypatch.setattr(research, "simulate", lambda **kwargs: called.append(kwargs))
    with pytest.raises(ValueError):
        research.research_backtest(source, output, registry)
    assert called == []
    assert not output.exists()
    assert not registry.exists()
    assert not registry.with_name(registry.name + ".lock").exists()
    assert source.read_bytes() == source_before


def test_report_collision_preserves_an_existing_counted_journal(tmp_path):
    source = request_file(tmp_path)
    source_before = source.read_bytes()
    output = tmp_path / "existing-result"
    output.mkdir()
    registry = output / "backtest.json"
    trials.start_trial(registry, {"previous_variant": True}, {})
    before_journal = registry.read_bytes()
    with pytest.raises(ValueError, match="distinct"):
        research.research_backtest(source, output, registry)
    assert registry.read_bytes() == before_journal
    assert trials.trial_counts(registry)["trial_count_N"] == 1
    assert source.read_bytes() == source_before
    assert list(output.iterdir()) == [registry]


def mark_reparse(monkeypatch, component):
    """Use the actual Windows native attribute, without junction privileges."""
    original = Path.lstat
    attributes = getattr(component.lstat(), "st_file_attributes", 0) | 0x400
    mode = component.lstat().st_mode
    monkeypatch.setattr(
        Path,
        "lstat",
        lambda self: (
            SimpleNamespace(st_file_attributes=attributes, st_mode=mode)
            if self == component
            else original(self)
        ),
    )


@pytest.mark.parametrize("affected", ["input", "output", "registry"])
def test_research_reparse_ancestor_refuses_before_any_publication(tmp_path, monkeypatch, affected):
    source = request_file(tmp_path)
    source_before = source.read_bytes()
    output = tmp_path / "result"
    registry = tmp_path / "trials.jsonl"
    ancestor = tmp_path / "reparse-parent"
    ancestor.mkdir()
    if affected == "input":
        source = ancestor / "input.json"
        source.write_bytes(source_before)
    elif affected == "output":
        output = ancestor / "result"
    else:
        registry = ancestor / "trials.jsonl"
    mark_reparse(monkeypatch, ancestor)
    with pytest.raises(ValueError, match="reparse"):
        research.research_backtest(source, output, registry)
    assert source.read_bytes() == source_before
    assert not output.exists()
    assert not registry.exists()
    assert not registry.with_name(registry.name + ".lock").exists()


def test_trial_reparse_ancestor_refuses_before_new_descendant_or_lock(tmp_path, monkeypatch):
    ancestor = tmp_path / "reparse-parent"
    ancestor.mkdir()
    registry = ancestor / "new-descendant" / "trials.jsonl"
    mark_reparse(monkeypatch, ancestor)
    with pytest.raises(ValueError, match="reparse"):
        trials.start_trial(registry, {}, {})
    assert not registry.parent.exists()
    assert not registry.exists()


def test_trial_reparse_lock_preserved_without_journal_creation(tmp_path, monkeypatch):
    registry = tmp_path / "trials.jsonl"
    lock = registry.with_name(registry.name + ".lock")
    lock.write_bytes(b"operator-owned-marker")
    mark_reparse(monkeypatch, lock)
    with pytest.raises(ValueError, match="reparse"):
        trials.start_trial(registry, {}, {})
    assert lock.read_bytes() == b"operator-owned-marker"
    assert not registry.exists()


def test_started_fsync_failure_never_calls_runner_and_retains_evidence(tmp_path, monkeypatch):
    registry = tmp_path / "trials.jsonl"
    callbacks = []

    def fail_sync(descriptor):
        raise OSError("injected sync failure")

    monkeypatch.setattr(trials.os, "fsync", fail_sync)
    with pytest.raises(OSError):
        trials.run_registered(
            registry,
            {"variant": "sync-failure"},
            {},
            lambda: callbacks.append("runner"),
            lambda result: callbacks.append("summary"),
        )
    assert callbacks == []
    records = trials.read_trials(registry)
    assert len(records) == 1 and records[0].phase == "STARTED"
    assert trials.trial_counts(registry) == {
        "started": 1,
        "completed": 0,
        "failed": 0,
        "unresolved": 1,
        "trial_count_N": 1,
    }
    assert not registry.with_name(registry.name + ".lock").exists()


@pytest.mark.parametrize("failure", ["before_write", "short_write", "fsync"])
def test_terminal_append_uncertainty_preserves_prefix_and_has_no_report(
    tmp_path, monkeypatch, failure
):
    source = request_file(tmp_path)
    before_input = source.read_bytes()
    registry = tmp_path / "trials.jsonl"
    output = tmp_path / "result"
    original_simulate = research.simulate
    original_write = os.write
    original_sync = os.fsync
    started_prefix = []
    calls = []

    def simulate(**kwargs):
        rows = trials.read_trials(registry)
        assert len(rows) == 1 and rows[0].phase == "STARTED"
        started_prefix.append(registry.read_bytes())
        calls.append("simulate")
        return original_simulate(**kwargs)

    def write(descriptor, content):
        if b'"phase":"COMPLETED"' in content:
            if failure == "before_write":
                raise OSError("injected terminal write failure")
            if failure == "short_write":
                return original_write(descriptor, content[:20])
        return original_write(descriptor, content)

    sync_count = 0

    def sync(descriptor):
        nonlocal sync_count
        sync_count += 1
        if failure == "fsync" and sync_count == 2:
            raise OSError("injected terminal sync failure")
        return original_sync(descriptor)

    monkeypatch.setattr(research, "simulate", simulate)
    monkeypatch.setattr(trials.os, "write", write)
    monkeypatch.setattr(trials.os, "fsync", sync)
    with pytest.raises((OSError, IntegrityError)):
        research.research_backtest(source, output, registry)
    assert calls == ["simulate"]
    assert registry.read_bytes().startswith(started_prefix[0])
    assert source.read_bytes() == before_input
    assert not (output / "backtest.json").exists()
    assert not registry.with_name(registry.name + ".lock").exists()
    if failure == "before_write":
        assert registry.read_bytes() == started_prefix[0]
        assert trials.trial_counts(registry)["unresolved"] == 1
    elif failure == "short_write":
        assert len(registry.read_bytes()) == len(started_prefix[0]) + 20
        with pytest.raises(IntegrityError, match="INCOMPLETE"):
            trials.read_trials(registry)
    else:
        # A complete visible record does not establish that failed fsync was durable.
        assert [row.phase for row in trials.read_trials(registry)] == ["STARTED", "COMPLETED"]


def test_input_mutation_during_simulation_counts_failed_trial_without_success(
    tmp_path, monkeypatch
):
    source = request_file(tmp_path)
    original_input = source.read_bytes()
    registry = tmp_path / "trials.jsonl"
    output = tmp_path / "result"
    original_simulate = research.simulate
    calls = []

    def mutate(**kwargs):
        assert trials.read_trials(registry)[0].phase == "STARTED"
        calls.append("simulate")
        result = original_simulate(**kwargs)
        source.write_bytes(original_input + b"\n")
        return result

    monkeypatch.setattr(research, "simulate", mutate)
    with pytest.raises(IntegrityError, match="SOURCE_CHANGED"):
        research.research_backtest(source, output, registry)
    assert calls == ["simulate"]
    assert json.loads(source.read_bytes()) == json.loads(original_input)
    assert source.read_bytes() != original_input
    assert trials.trial_counts(registry) == {
        "started": 1,
        "completed": 0,
        "failed": 1,
        "unresolved": 0,
        "trial_count_N": 1,
    }
    rows = trials.read_trials(registry)
    assert [row.phase for row in rows] == ["STARTED", "FAILED"]
    assert rows[-1].failure_type == "IntegrityError"
    assert not (output / "backtest.json").exists()
    assert not registry.with_name(registry.name + ".lock").exists()


def test_report_publication_cannot_replace_a_file_created_during_run(tmp_path, monkeypatch):
    source = request_file(tmp_path)
    original_input = source.read_bytes()
    registry = tmp_path / "trials.jsonl"
    output = tmp_path / "result"
    report_path = output / "backtest.json"
    original_simulate = research.simulate
    marker = b"operator-created-file-do-not-replace"

    def concurrent_publication(**kwargs):
        result = original_simulate(**kwargs)
        report_path.write_bytes(marker)
        return result

    monkeypatch.setattr(research, "simulate", concurrent_publication)
    with pytest.raises(FileExistsError):
        research.research_backtest(source, output, registry)
    assert report_path.read_bytes() == marker
    assert source.read_bytes() == original_input
    assert [row.phase for row in trials.read_trials(registry)] == ["STARTED", "COMPLETED"]
    assert trials.trial_counts(registry)["trial_count_N"] == 1
    assert not registry.with_name(registry.name + ".lock").exists()
