"""Bind actual decoded bytes and inner manifest selection through transient swaps."""

import hashlib
import json
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest
from test_bar_checkpoint import CUTOFF, source

from selective_options import bar_checkpoint as checkpoint
from selective_options import periodic_bar_runtime as generation
from selective_options.bar_state import decode_bar_state
from selective_options.errors import IntegrityError


async def artifact(tmp_path):
    store, _, books = await source(tmp_path)
    directory = tmp_path / "generation-000000"
    directory.mkdir()
    try:
        manifest = await checkpoint.create_bar_checkpoint(
            store, directory / "checkpoint", books, CUTOFF
        )
    finally:
        await store.close()
    marker, digest = generation._seal(directory, 0, manifest)
    return directory, marker, digest


def alternatives(directory):
    state = directory / "checkpoint/state-000000.json"
    manifest = directory / "checkpoint/checkpoint.json"
    state_a, manifest_a = state.read_bytes(), manifest.read_bytes()
    value = json.loads(state_a)
    value["coverage_start_ms"] = 299000
    state_b = checkpoint._canonical(value)
    changed = json.loads(manifest_a)
    changed["books"][0].update(
        state_sha256=hashlib.sha256(state_b).hexdigest(), state_bytes=len(state_b)
    )
    manifest_b = checkpoint._canonical(changed) + b"\n"
    return state, manifest, state_a, manifest_a, state_b, manifest_b


async def load(directory, **kwargs):
    return await checkpoint.load_bar_checkpoint(
        directory / "checkpoint", 1000000, ("BTC",), (300, 900), 5000, **kwargs
    )


async def selected(directory, digest):
    return await generation.load_bar_generation(
        directory,
        1000000,
        ("BTC",),
        (300, 900),
        5000,
        expected_generation_sha256=digest,
    )


async def test_default_loader_and_matching_manifest_pin_preserve_exact_result(tmp_path):
    directory, marker, digest = await artifact(tmp_path)
    default = await load(directory)
    assert await load(directory, expected_manifest_sha256=None) == default
    assert await load(directory, expected_manifest_sha256=marker.checkpoint_sha256) == default
    assert await selected(directory, digest) == default
    assert decode_bar_state(default.books[0].state_blob).coverage_start_ms == 300000


@pytest.mark.parametrize("different_length", [False, True])
async def test_actual_state_a_to_valid_b_to_a_read_is_refused_even_when_path_hashes_restore(
    tmp_path, monkeypatch, different_length
):
    directory, _, digest = await artifact(tmp_path)
    state, _, state_a, _, state_b, _ = alternatives(directory)
    if different_length:
        state_b += b" "
    else:
        assert len(state_a) == len(state_b)
    assert decode_bar_state(state_b).coverage_start_ms == 299000
    read = checkpoint._read
    calls = []

    def transient(path, budget):
        if path != state:
            return read(path, budget)
        state.write_bytes(state_b)
        try:
            result = read(path, budget)
            calls.append(result)
            return result
        finally:
            state.write_bytes(state_a)

    with monkeypatch.context() as patch:
        patch.setattr(checkpoint, "_read", transient)
        with pytest.raises(IntegrityError, match="BAR_CHECKPOINT_STATE_CHANGED"):
            await selected(directory, digest)
    assert calls == [state_b] and state.read_bytes() == state_a
    assert (
        decode_bar_state((await selected(directory, digest)).books[0].state_blob).coverage_start_ms
        == 300000
    )


async def test_actual_manifest_a_to_valid_whitespace_b_to_a_read_refuses_before_verify(
    tmp_path, monkeypatch
):
    directory, _, digest = await artifact(tmp_path)
    _, manifest, _, manifest_a, _, _ = alternatives(directory)
    read = checkpoint._read

    def transient(path, budget):
        if path != manifest:
            return read(path, budget)
        manifest.write_bytes(manifest_a + b" ")
        try:
            return read(path, budget)
        finally:
            manifest.write_bytes(manifest_a)

    def forbidden(*args, **kwargs):
        pytest.fail("a manifest read with the wrong digest reached artifact verification")

    with monkeypatch.context() as patch:
        patch.setattr(checkpoint, "_read", transient)
        patch.setattr(checkpoint, "_verify", forbidden)
        with pytest.raises(IntegrityError, match="BAR_CHECKPOINT_MANIFEST_CHANGED"):
            await selected(directory, digest)
    assert manifest.read_bytes() == manifest_a
    assert await selected(directory, digest)


async def test_consistent_b_manifest_read_window_is_refused_before_verify_under_original_pin(
    tmp_path, monkeypatch
):
    directory, _, digest = await artifact(tmp_path)
    state, manifest, state_a, manifest_a, state_b, manifest_b = alternatives(directory)
    read = checkpoint._read
    calls = []

    def swap_at_read(path, budget):
        if path == manifest:
            state.write_bytes(state_b)
            manifest.write_bytes(manifest_b)
            calls.append("B available to the actual manifest reader")
        return read(path, budget)

    def forbidden(*args, **kwargs):
        pytest.fail("actual B manifest bytes were not checked against the A pre-read digest")

    try:
        with monkeypatch.context() as patch:
            patch.setattr(checkpoint, "_read", swap_at_read)
            patch.setattr(checkpoint, "_verify", forbidden)
            with pytest.raises(IntegrityError, match="BAR_CHECKPOINT_MANIFEST_CHANGED"):
                await selected(directory, digest)
    finally:
        state.write_bytes(state_a)
        manifest.write_bytes(manifest_a)
    assert calls == ["B available to the actual manifest reader"]
    assert await selected(directory, digest)


async def test_complete_consistent_b_inner_load_window_is_refused_by_outer_manifest_pin(
    tmp_path, monkeypatch
):
    directory, marker, digest = await artifact(tmp_path)
    state, manifest, state_a, manifest_a, state_b, manifest_b = alternatives(directory)
    # Establish that B is internally valid when explicitly loading the current
    # standalone checkpoint without a selection pin. The attack is substitution
    # of that different valid artifact under the already chosen A generation.
    try:
        state.write_bytes(state_b)
        manifest.write_bytes(manifest_b)
        assert (
            decode_bar_state((await load(directory)).books[0].state_blob).coverage_start_ms
            == 299000
        )
    finally:
        state.write_bytes(state_a)
        manifest.write_bytes(manifest_a)
    inner_load = generation.load_bar_checkpoint
    seen = []

    async def complete_window(*args, **kwargs):
        seen.append(kwargs.get("expected_manifest_sha256"))
        state.write_bytes(state_b)
        manifest.write_bytes(manifest_b)
        try:
            return await inner_load(*args, **kwargs)
        finally:
            state.write_bytes(state_a)
            manifest.write_bytes(manifest_a)

    with monkeypatch.context() as patch:
        patch.setattr(generation, "load_bar_checkpoint", complete_window)
        with pytest.raises(IntegrityError, match="BAR_CHECKPOINT_MANIFEST_DIGEST_MISMATCH"):
            await selected(directory, digest)
    assert seen == [marker.checkpoint_sha256]
    assert state.read_bytes() == state_a and manifest.read_bytes() == manifest_a
    assert (
        decode_bar_state((await selected(directory, digest)).books[0].state_blob).coverage_start_ms
        == 300000
    )


async def test_wrong_manifest_pin_refuses_before_actual_read(tmp_path, monkeypatch):
    directory, marker, _ = await artifact(tmp_path)
    assert marker.checkpoint_sha256 != "0" * 64

    def forbidden(*args):
        pytest.fail("a mismatched explicit manifest pin reached the reader")

    monkeypatch.setattr(checkpoint, "_read", forbidden)
    with pytest.raises(IntegrityError, match="BAR_CHECKPOINT_MANIFEST_DIGEST_MISMATCH"):
        await load(directory, expected_manifest_sha256="0" * 64)


def alternate_database(directory, temporary):
    database = directory / "checkpoint/events.duckdb"
    original = database.read_bytes()
    alternate = temporary / "alternate.duckdb"
    alternate.write_bytes(original)
    manifest = json.loads((directory / "checkpoint/checkpoint.json").read_bytes())
    identifier = manifest["books"][0]["input_ids"][1]
    with duckdb.connect(str(alternate)) as connection:
        old = connection.execute(
            "SELECT observed_at_ms FROM events WHERE event_id=?", [identifier]
        ).fetchone()[0]
        connection.execute(
            "UPDATE events SET observed_at_ms=observed_at_ms-1 WHERE event_id=?", [identifier]
        )
    changed = alternate.read_bytes()
    assert changed != original
    return database, original, changed, identifier, old


async def test_actual_database_a_to_b_to_a_read_refuses_changed_nonprototype_receipt(
    tmp_path, monkeypatch
):
    directory, _, digest = await artifact(tmp_path)
    database, original, changed, identifier, receipt = alternate_database(directory, tmp_path)
    read = checkpoint._read
    seen = []

    def transient(path, budget):
        if path != database:
            return read(path, budget)
        database.write_bytes(changed)
        try:
            seen.append("actual B bytes read")
            return read(path, budget)
        finally:
            database.write_bytes(original)

    with monkeypatch.context() as patch:
        patch.setattr(checkpoint, "_read", transient)
        with pytest.raises(IntegrityError, match="BAR_CHECKPOINT_DATABASE_CHANGED"):
            await selected(directory, digest)
    assert seen == ["actual B bytes read"] and database.read_bytes() == original
    result = await selected(directory, digest)
    assert (
        next(row for row in result.events if row.event_id == identifier).observed_at_ms == receipt
    )


async def test_source_database_swap_during_private_sql_open_cannot_change_pinned_receipts(
    tmp_path, monkeypatch
):
    directory, _, digest = await artifact(tmp_path)
    database, original, changed, identifier, receipt = alternate_database(directory, tmp_path)
    connect = checkpoint.duckdb.connect
    opened = []

    @contextmanager
    def swap_source(*args, **kwargs):
        path = Path(args[0])
        assert kwargs.get("read_only") is True
        assert path != database and path.parent.name.startswith("selective-options-bar-read-")
        opened.append(path)
        database.write_bytes(changed)
        try:
            with connect(*args, **kwargs) as connection:
                yield connection
        finally:
            database.write_bytes(original)

    with monkeypatch.context() as patch:
        patch.setattr(checkpoint.duckdb, "connect", swap_source)
        result = await selected(directory, digest)
    assert (
        next(row for row in result.events if row.event_id == identifier).observed_at_ms == receipt
    )
    assert opened and all(not path.exists() and not path.parent.exists() for path in opened)
    assert database.read_bytes() == original
    assert await selected(directory, digest) == result


async def test_private_read_snapshot_connection_closes_and_temp_copy_is_removed_on_sql_error(
    tmp_path, monkeypatch
):
    directory, _, digest = await artifact(tmp_path)
    database = directory / "checkpoint/events.duckdb"
    original = database.read_bytes()
    connect = checkpoint.duckdb.connect
    opened = []

    @contextmanager
    def observed(*args, **kwargs):
        opened.append(Path(args[0]))
        with connect(*args, **kwargs) as connection:
            yield connection

    def failed(connection):
        raise OSError("EXPLICIT_PRIVATE_SQL_VERIFICATION_FAILURE")

    with monkeypatch.context() as patch:
        patch.setattr(checkpoint.duckdb, "connect", observed)
        patch.setattr(checkpoint, "_schema", failed)
        with pytest.raises(OSError, match="EXPLICIT_PRIVATE_SQL_VERIFICATION_FAILURE"):
            await selected(directory, digest)
    assert opened and all(not path.exists() and not path.parent.exists() for path in opened)
    assert database.read_bytes() == original
    assert await selected(directory, digest)


@pytest.mark.parametrize("pin", [True, 0, "", "f" * 63, "g" * 64, "A" * 64])
async def test_invalid_optional_manifest_pin_refuses_before_filesystem_read(tmp_path, pin):
    with pytest.raises(ValueError, match="explicit lowercase SHA256"):
        await checkpoint.load_bar_checkpoint(
            tmp_path / "missing", 1000000, ("BTC",), (300, 900), 5000, expected_manifest_sha256=pin
        )
