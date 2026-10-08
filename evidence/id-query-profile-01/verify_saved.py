"""Recheck scalar JSON identity/symbol set equivalence on unchanged native evidence."""

import hashlib
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT.parent / "reused-validation-public-01/events.duckdb"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    output = ROOT / "verification.json"
    assert not output.exists(), "verification requires fresh output"
    profile = json.loads((ROOT / "report.json").read_text())
    before = digest(SOURCE)
    assert before == profile["source_sha256_before"] == profile["source_sha256_after"]
    results = []
    with duckdb.connect(str(SOURCE), read_only=True) as db:
        original = db.execute("SELECT * FROM events ORDER BY event_id").fetchall()
        assert len(original) == 62089
        for field, sql in (
            (
                0,
                "SELECT e.* FROM events e SEMI JOIN json_each(?) ids ON e.event_id=(ids.value ->> '$')",
            ),
            (0, "SELECT * FROM events WHERE event_id IN (SELECT value ->> '$' FROM json_each(?))"),
            (4, "SELECT * FROM events WHERE symbol IN (SELECT value ->> '$' FROM json_each(?))"),
        ):
            for count in (0, 1000, 20001, 62089):
                keys = list(dict.fromkeys(row[field] for row in original))[:count]
                requested = keys + keys[:20] + ["missing"]
                requested_set = set(requested)
                expected = [row for row in original if row[field] in requested_set]
                actual = db.execute(sql, [json.dumps(requested)]).fetchall()
                assert sorted(actual) == expected
                results.append(
                    {
                        "sql": sql,
                        "requested": len(requested),
                        "returned": len(actual),
                        "exact_native_rows_equal": True,
                    }
                )
    assert before == digest(SOURCE)
    output.write_text(
        json.dumps(
            {
                "status": "EXACT_NATIVE_JSON_SET_LOOKUPS_VERIFIED",
                "native_rows": len(original),
                "checks": results,
                "source_sha256": before,
                "original_bytes_unchanged": True,
                "new_network_requests": 0,
                "orders_submitted": 0,
                "performance_threshold_claimed": False,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print("12 native ID/symbol set checks passed; original bytes unchanged.")


if __name__ == "__main__":
    main()
