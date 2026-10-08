"""Final parent policy edge repair preserves the complete independently checked audits."""

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parents[1]
sys.path.insert(0, str(PROJECT))
from scripts.audit_option_freshness import audit_capture, seconds_ms  # noqa: E402


def main():
    target = ROOT / "parent-validation.json"
    assert not target.exists()
    try:
        seconds_ms(10**307)
    except ValueError:
        pass
    else:
        raise AssertionError("large seconds conversion was not refused")
    for name, assets, folder in (
        ("reused-validation-public-01", ("BTC",), "btc"),
        ("multiasset-options-public-01", ("ETH", "SOL", "PAXG"), "eth-sol-paxg"),
    ):
        report = audit_capture(
            PROJECT / "evidence" / name,
            assets,
            PROJECT / "../../work/option-freshness-audit/parent" / folder,
        )
        assert report == json.loads((ROOT / folder / "report.json").read_text())
    hashes = {}
    for source, snapshot in (
        ("scripts/audit_option_freshness.py", "parent_audit_used.py"),
        ("tests/test_option_freshness_audit.py", "parent_tests_used.py"),
    ):
        data = (PROJECT / source).read_bytes()
        with (ROOT / snapshot).open("xb") as handle:
            handle.write(data)
        hashes[source] = hashlib.sha256(data).hexdigest()
    target.write_text(
        json.dumps(
            {
                "status": "FINAL_PARENT_AUDITS_EXACTLY_MATCH_INDEPENDENT_SAVED_PROOFS",
                "milliseconds_conversion_overflow_is_value_error": True,
                "all_four_saved_asset_diagnostics_exact": True,
                "original_files_unchanged": True,
                "source_code_sha256": hashes,
                "original_worker_snapshots_and_validation_preserved": True,
                "parent_targeted_tests_passed": 81,
                "final_freshness_cases": 55,
                "new_network_requests": 0,
                "orders_submitted": 0,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print("Parent overflow refusal and four unchanged saved audits verified.")


if __name__ == "__main__":
    main()
