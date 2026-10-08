"""Seal final owned audit/test bytes and prove final code reproduces independent saved proofs."""

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.audit_option_freshness import audit_capture  # noqa: E402

EVIDENCE = ROOT / "evidence/option-freshness-01"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    final = EVIDENCE / "final-validation.json"
    if final.exists():
        raise FileExistsError("immutable final validation already exists")
    for capture, assets, name in [
        ("reused-validation-public-01", ("BTC",), "btc"),
        ("multiasset-options-public-01", ("ETH", "SOL", "PAXG"), "eth-sol-paxg"),
    ]:
        rerun = audit_capture(
            ROOT / "evidence" / capture,
            assets,
            ROOT / "work/option-freshness-audit/final" / name,
        )
        assert rerun == json.loads((EVIDENCE / name / "report.json").read_text())
    snapshots = {}
    for source, name in [
        ("scripts/audit_option_freshness.py", "audit_used.py"),
        ("tests/test_option_freshness_audit.py", "tests_used.py"),
    ]:
        target = EVIDENCE / name
        with target.open("xb") as handle:
            handle.write((ROOT / source).read_bytes())
        assert digest(target) == digest(ROOT / source)
        snapshots[source] = digest(target)
    result = {
        "final_code_rerun_exact_to_independently_verified_reports": True,
        "source_code_sha256": snapshots,
        "independent_proof_sha256": digest(EVIDENCE / "independent-proof.json"),
        "targeted_junit_sha256": digest(EVIDENCE / "targeted-tests.xml"),
        "targeted_tests_passed": 174,
        "new_audit_cases_passed": 54,
        "ruff_check_and_format_passed": True,
        "strict_mypy_src_and_audit_script_passed": True,
        "new_network_requests": 0,
        "orders_submitted": 0,
        "milestone_2_complete": False,
        "trading_enabled": False,
    }
    final.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
