"""Reject incomplete CI test reports without printing test names or failure contents."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from xml.etree.ElementTree import Element, ParseError

from defusedxml import ElementTree
from defusedxml.common import DefusedXmlException

COUNTERS = ("tests", "failures", "errors", "skipped")


class JUnitError(ValueError):
    """The supplied report cannot establish a complete, successful test run."""

    def __init__(self, reason: str, counts: dict[str, int] | None = None) -> None:
        super().__init__(reason)
        self.counts = counts


def declared_counts(element: Element) -> dict[str, int]:
    counts = {}
    for name in COUNTERS:
        value = element.attrib.get(name)
        if value is None or not value.isascii() or not value.isdecimal():
            raise JUnitError("INVALID_OR_MISSING_COUNTER")
        try:
            counts[name] = int(value)
        except ValueError as exc:
            raise JUnitError("INVALID_OR_MISSING_COUNTER") from exc
    return counts


def suite_counts(suite: Element) -> dict[str, int]:
    counts = dict.fromkeys(COUNTERS, 0)
    for child in suite:
        if child.tag == "testsuite":
            nested = suite_counts(child)
            for name in COUNTERS:
                counts[name] += nested[name]
        elif child.tag == "testcase":
            counts["tests"] += 1
            if any(
                node.tag
                not in {"failure", "error", "skipped", "properties", "system-out", "system-err"}
                for node in child
            ):
                raise JUnitError("UNEXPECTED_TESTCASE_ELEMENT")
            outcomes = [node.tag for node in child if node.tag in {"failure", "error", "skipped"}]
            if len(outcomes) > 1:
                raise JUnitError("MULTIPLE_TESTCASE_OUTCOMES")
            if outcomes:
                counter = {"failure": "failures", "error": "errors", "skipped": "skipped"}[
                    outcomes[0]
                ]
                counts[counter] += 1
        elif child.tag not in {"properties", "system-out", "system-err"}:
            raise JUnitError("UNEXPECTED_SUITE_ELEMENT")
    if declared_counts(suite) != counts:
        raise JUnitError("COUNTER_TESTCASE_MISMATCH")
    return counts


def read_junit(path: Path) -> dict[str, int]:
    """Require every supplied file to contain executed cases with zero non-passes."""
    try:
        document = ElementTree.parse(path).getroot()
    except FileNotFoundError as exc:
        raise JUnitError("REPORT_MISSING") from exc
    except (OSError, ParseError, DefusedXmlException) as exc:
        raise JUnitError("REPORT_UNREADABLE_OR_MALFORMED") from exc
    if document is None:
        raise JUnitError("INVALID_REPORT_STRUCTURE")
    if document.tag == "testsuite":
        counts = suite_counts(document)
    elif document.tag == "testsuites":
        suites = list(document)
        if not suites or any(suite.tag != "testsuite" for suite in suites):
            raise JUnitError("INVALID_REPORT_STRUCTURE")
        counts = dict.fromkeys(COUNTERS, 0)
        for suite in suites:
            measured = suite_counts(suite)
            for name in COUNTERS:
                counts[name] += measured[name]
        if any(name in document.attrib for name in COUNTERS):
            if declared_counts(document) != counts:
                raise JUnitError("ROOT_COUNTER_MISMATCH")
    else:
        raise JUnitError("INVALID_REPORT_STRUCTURE")
    if counts["tests"] <= 0:
        raise JUnitError("NO_EXECUTED_CASES", counts)
    if any(counts[name] for name in ("failures", "errors", "skipped")):
        raise JUnitError("FAILED_ERRORED_OR_SKIPPED_CASES", counts)
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", type=Path, nargs="+")
    parser.add_argument(
        "--github-summary", action="store_true", help="Append counts to GITHUB_STEP_SUMMARY"
    )
    args = parser.parse_args(argv)
    total = dict.fromkeys(COUNTERS, 0)
    results = []
    passed = True
    for index, path in enumerate(args.reports, 1):
        try:
            counts = read_junit(path)
            for name in COUNTERS:
                total[name] += counts[name]
            results.append({"report_index": index, "status": "PASSED", **counts})
        except JUnitError as exc:
            passed = False
            results.append(
                {
                    "report_index": index,
                    "status": "FAILED",
                    "reason": str(exc),
                    **(exc.counts or {}),
                }
            )
    result = {
        "status": "PASSED" if passed else "FAILED",
        "validated_report_counts": total,
        "reports": results,
    }
    if args.github_summary:
        summary = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary:
            with Path(summary).open("a", encoding="utf-8") as stream:
                stream.write("\n### Complete CI test-report gate\n\n```json\n")
                stream.write(json.dumps(result, indent=2) + "\n```\n")
    print(json.dumps(result, sort_keys=True))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
