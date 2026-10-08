"""CI cannot certify skipped cases or a JUnit aggregate hiding actual outcomes."""

import importlib.util
import json
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "tools/check_ci_junit.py"
SPEC = importlib.util.spec_from_file_location("check_ci_junit", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
junit = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(junit)


def report(tmp_path, content, name="report.xml"):
    path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    return path


def test_valid_multiple_suites_and_reports_are_combined(tmp_path, capsys):
    first = report(
        tmp_path,
        '<testsuites><testsuite tests="2" failures="0" errors="0" skipped="0">'
        '<testcase name="a"/><testcase name="b"/></testsuite>'
        '<testsuite tests="1" failures="0" errors="0" skipped="0">'
        '<testcase name="c"/></testsuite></testsuites>',
    )
    second = report(
        tmp_path,
        '<testsuite tests="1" failures="0" errors="0" skipped="0"><testcase/></testsuite>',
        "second.xml",
    )
    assert junit.main([str(first), str(second)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "PASSED"
    assert result["validated_report_counts"] == {
        "tests": 4,
        "failures": 0,
        "errors": 0,
        "skipped": 0,
    }


@pytest.mark.parametrize("outcome", ["failure", "error", "skipped"])
def test_aggregate_zero_cannot_hide_nonpassing_testcase(tmp_path, outcome):
    path = report(
        tmp_path,
        '<testsuite tests="1" failures="0" errors="0" skipped="0">'
        f"<testcase><{outcome}/></testcase></testsuite>",
    )
    with pytest.raises(junit.JUnitError, match="COUNTER_TESTCASE_MISMATCH"):
        junit.read_junit(path)


@pytest.mark.parametrize(
    ("outcome", "counts"),
    [
        ("failure", 'failures="1" errors="0" skipped="0"'),
        ("error", 'failures="0" errors="1" skipped="0"'),
        ("skipped", 'failures="0" errors="0" skipped="1"'),
    ],
)
def test_even_honestly_declared_nonpasses_fail_gate(tmp_path, outcome, counts, capsys):
    path = report(
        tmp_path,
        f'<testsuite tests="1" {counts}><testcase name="private-case">'
        f'<{outcome} message="private-token">private-failure-text</{outcome}>'
        "</testcase></testsuite>",
    )
    assert junit.main([str(path)]) == 1
    output = capsys.readouterr().out
    assert json.loads(output)["status"] == "FAILED"
    assert "private" not in output


def test_declared_tests_must_match_actual_cases(tmp_path):
    path = report(
        tmp_path,
        '<testsuite tests="100" failures="0" errors="0" skipped="0"><testcase/></testsuite>',
    )
    with pytest.raises(junit.JUnitError, match="COUNTER_TESTCASE_MISMATCH"):
        junit.read_junit(path)


def test_root_totals_cannot_override_measured_suites(tmp_path):
    path = report(
        tmp_path,
        '<testsuites tests="100" failures="0" errors="0" skipped="0">'
        '<testsuite tests="1" failures="0" errors="0" skipped="0">'
        "<testcase/></testsuite></testsuites>",
    )
    with pytest.raises(junit.JUnitError, match="ROOT_COUNTER_MISMATCH"):
        junit.read_junit(path)


@pytest.mark.parametrize(
    "content",
    [
        "<testsuite>",
        "<unrelated/>",
        '<testsuite tests="0" failures="0" errors="0" skipped="0"/>',
        '<testsuite tests="1" failures="0" errors="0"><testcase/></testsuite>',
        '<testsuite tests="-1" failures="0" errors="0" skipped="0"/>',
        '<testsuite tests="' + "9" * 5000 + '" failures="0" errors="0" skipped="0"/>',
        '<testsuite tests="1" failures="0" errors="0" skipped="0">'
        "<testcase><failure/><error/></testcase></testsuite>",
        '<testsuite tests="1" failures="0" errors="0" skipped="0">'
        "<testcase><testcase/></testcase></testsuite>",
        '<!DOCTYPE report [<!ENTITY leak SYSTEM "file:///private">]>'
        '<testsuite tests="1" failures="0" errors="0" skipped="0">'
        "<testcase>&leak;</testcase></testsuite>",
    ],
)
def test_empty_malformed_or_unsafe_reports_fail(tmp_path, content, capsys):
    path = report(tmp_path, content)
    assert junit.main([str(path)]) == 1
    assert json.loads(capsys.readouterr().out)["status"] == "FAILED"


def test_one_missing_report_cannot_pass_with_another_valid_report(tmp_path, capsys):
    valid = report(
        tmp_path,
        '<testsuite tests="1" failures="0" errors="0" skipped="0"><testcase/></testsuite>',
    )
    assert junit.main([str(valid), str(tmp_path / "missing.xml")]) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["reports"][1]["reason"] == "REPORT_MISSING"


def test_optional_github_summary_contains_only_safe_counts(tmp_path, monkeypatch, capsys):
    path = report(
        tmp_path,
        '<testsuite name="private-suite" tests="1" failures="0" errors="0" skipped="0">'
        '<testcase name="private-case"><system-out>private-output</system-out></testcase>'
        "</testsuite>",
    )
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    assert junit.main([str(path), "--github-summary"]) == 0
    content = summary.read_text()
    assert "PASSED" in content and '"tests": 1' in content
    assert "private" not in content
    assert "private" not in capsys.readouterr().out


def test_nested_suite_totals_are_validated_without_double_counting(tmp_path):
    path = report(
        tmp_path,
        '<testsuite tests="2" failures="0" errors="0" skipped="0">'
        '<testcase/><testsuite tests="1" failures="0" errors="0" skipped="0">'
        "<testcase/></testsuite></testsuite>",
    )
    assert junit.read_junit(path)["tests"] == 2
