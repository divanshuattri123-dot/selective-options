"""Replay saved public pages through both archive verifier entry paths, without network."""

import importlib
import json
from pathlib import Path

import pytest

from selective_options.bybit import Envelope

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "kind,directory,page_size,hours,expected",
    [
        ("funding", "bybit-funding-history-01", 2, 48, 6),
        ("open_interest", "bybit-oi-history-01", 5, 1, 12),
    ],
)
async def test_saved_public_verifier_replay(
    monkeypatch, tmp_path, kind, directory, page_size, hours, expected
):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    verifier = importlib.import_module("verify_funding_history")
    source = ROOT / "evidence" / directory
    responses = json.loads((source / "responses.json").read_text(encoding="utf-8"))
    original = json.loads((source / "report.json").read_text(encoding="utf-8"))
    pending = list(responses)
    clocks = iter([original["end_ms"], original["ended_at_ms"], original["ended_at_ms"]])
    monkeypatch.setattr(verifier, "now_ms", lambda: next(clocks))

    async def request(self, session, endpoint, params):
        saved = pending.pop(0)
        assert saved["endpoint"] == endpoint and saved["params"] == params
        self.responses.append(saved)
        return Envelope.model_validate(saved["response"]), saved["observed_at_ms"]

    monkeypatch.setattr(verifier.RecordingCollector, "request", request)
    await verifier.verify(tmp_path / "archive", "BTC", hours, page_size, 10, kind=kind)
    report = json.loads((tmp_path / "archive/report.json").read_text())
    assert not pending
    assert report["status"] == "ARCHIVE_VERIFIED"
    assert report["rows"] == report["pit_checks"] == expected
    assert report["archive_exact_match"]
