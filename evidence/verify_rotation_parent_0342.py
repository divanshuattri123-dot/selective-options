"""Independent read-only final rotation cohort, native entities and restore audit."""
import asyncio
import hashlib
import json
from email.utils import parsedate_to_datetime
from io import BytesIO
from pathlib import Path

import duckdb
from defusedxml.minidom import parse

from selective_options.crypto_news import crypto_news_quality
from selective_options.events import MarketEvent
from selective_options.stablecoin_supply import stablecoin_supply_quality
from selective_options.store import DuckStore

ROOT = Path.cwd()
PROOF = ROOT / "evidence/rotation-saved-public-01"
OUTPUT = ROOT / "evidence/rotation-parent-validation-0342.json"


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def hashes(path):
    return {p.relative_to(path).as_posix(): digest(p) for p in path.rglob("*") if p.is_file()}


def events(conn, sql, params=None):
    cursor = conn.execute(sql, params or [])
    columns = [entry[0] for entry in cursor.description]
    return [MarketEvent.model_validate(dict(zip(columns, row, strict=True)), strict=True)
            for row in cursor.fetchall()]


def child(parent, name):
    return [x for x in parent.childNodes if x.nodeType == x.ELEMENT_NODE and x.tagName == name]


def field(parent, name):
    found = child(parent, name)
    assert len(found) == 1
    return "".join(x.data for x in found[0].childNodes if x.nodeType in (x.TEXT_NODE, x.CDATA_SECTION_NODE))


async def main():
    assert not OUTPUT.exists()
    news = ROOT / "evidence/crypto-news-public-02"
    supply = ROOT / "evidence/stablecoin-supply-public-01"
    originals = {p.name: hashes(p) for p in (news, supply)}
    proof_before = hashes(PROOF)
    report = read(PROOF / "report.json")
    assert report["status"] == "OFFLINE_SAVED_PUBLIC_ROTATION_RESTORE_VERIFIED"
    assert report["real_public_requests_attempted"] == 0
    assert (report["source_records"], report["eligible_records"], report["saved_native_records"], report["synthetic_records"], report["excluded_future_clock_fixtures"]) == (39, 35, 28, 11, 4)
    assert report["verifier_sha256"] == digest(ROOT / "scripts/verify_rotation.py") == digest(PROOF / "verification-script.py")
    for name, value in report["code_sha256"].items():
        assert digest(ROOT / "src/selective_options" / name) == value
    assert report["source_hashes_before"] == report["source_hashes_after"] == {"news": originals[news.name], "supply": originals[supply.name]}
    cutoff = report["as_of_ms"]
    source = duckdb.connect(str(PROOF / "source.duckdb"), read_only=True)
    native = []
    for path, expected_count in ((news, 26), (supply, 2)):
        with duckdb.connect(str(path / "events.duckdb"), read_only=True) as conn:
            saved = events(conn, "SELECT * FROM events ORDER BY event_id")
            assert len(saved) == expected_count
            assert saved == events(conn, "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(path / "events.parquet")])
            native.extend(saved)
    all_rows = events(source, "SELECT * FROM events ORDER BY event_id")
    eligible = events(source, "SELECT * FROM events WHERE event_time_ms<=? AND observed_at_ms<=? ORDER BY event_id", [cutoff, cutoff])
    assert len(all_rows) == 39 and len(eligible) == 35
    excluded = [r for r in all_rows if r not in eligible]
    assert len(excluded) == 4 and all(r.source == "rotation_verification_fixture" for r in excluded)
    assert sorted(native, key=lambda r: r.event_id) == [r for r in eligible if r.event_id in {e.event_id for e in native}]
    assert source.execute("SELECT event_id,delivered FROM delivery_outbox ORDER BY event_id").fetchall() == [tuple(v) for v in report["source_outbox_before"]] == [tuple(v) for v in report["source_outbox_after"]]
    assert source.execute("SELECT count(*) FROM delivery_outbox WHERE delivered").fetchone() == (1,)
    assert digest(PROOF / "source.duckdb") == report["working_source_sha256_before"] == report["working_source_sha256_after"]
    catalog = read(PROOF / "rotation/catalog.json")
    assert catalog == report["rotation"] and catalog["total_rows"] == 35 and len(catalog["parts"]) == 2
    assert catalog["source_sha256"] == digest(PROOF / "source.duckdb")
    assert catalog["source_history_modified"] is catalog["delivery_acknowledgements_copied"] is catalog["history_complete"] is catalog["ready_for_signals"] is catalog["trading_enabled"] is False
    archived = []
    for index, part in enumerate(catalog["parts"]):
        assert part["index"] == index and part["end_ms"] - part["start_ms"] == catalog["partition_ms"]
        path = PROOF / "rotation" / part["file"]
        manifest = path.with_suffix(path.suffix + ".manifest.json")
        m = read(manifest)
        assert part["sha256"] == m["sha256"] == digest(path)
        assert part["manifest_sha256"] == digest(manifest)
        assert part["bytes"] == m["bytes"] == path.stat().st_size
        rows = events(source, "SELECT * FROM read_parquet(?,hive_partitioning=false) ORDER BY event_id", [str(path)])
        assert len(rows) == part["rows"] == m["rows"]
        assert all(part["start_ms"] <= max(r.event_time_ms,r.observed_at_ms) < part["end_ms"] and r.event_time_ms <= cutoff and r.observed_at_ms <= cutoff for r in rows)
        archived.extend(rows)
    assert sorted(archived, key=lambda r:r.event_id) == eligible
    source.close()
    restore = read(PROOF / "restored/restore.json")
    assert restore == report["restore"] and restore["rows"] == restore["redis_delivery_pending"] == 35
    assert restore["catalog_sha256"] == digest(PROOF / "rotation/catalog.json")
    conn = duckdb.connect(str(PROOF / "restored/events.duckdb"), read_only=True)
    restored = events(conn, "SELECT * FROM events ORDER BY event_id")
    assert restored == eligible
    assert conn.execute("SELECT event_id,delivered FROM delivery_outbox ORDER BY event_id").fetchall() == [(r.event_id,False) for r in eligible]
    pit_checks = 0
    for row in restored:
        available = max(row.event_time_ms, row.observed_at_ms)
        for at, expected in ((available-1,False),(available,True)):
            seen = conn.execute("SELECT count(*) FROM events WHERE event_id=? AND event_time_ms<=? AND observed_at_ms<=?", [row.event_id,at,at]).fetchone()[0]
            assert bool(seen) == expected
            pit_checks += 1
    for expected in report["pit_checks"]:
        at = expected["as_of_ms"]
        ids = [v[0] for v in conn.execute("SELECT event_id FROM events WHERE event_time_ms<=? AND observed_at_ms<=? ORDER BY event_id", [at,at]).fetchall()]
        assert ids == expected["event_ids"] and len(ids) == expected["available_rows"]
    store = DuckStore(conn,5000)
    quality_checks = 0
    try:
        for view in report["shared_quality_checks"]:
            at = view["as_of_ms"]
            assert view["crypto_news"] == crypto_news_quality(restored,as_of_ms=at) == await store.crypto_news_quality(at)
            assert view["stablecoin_supply"] == stablecoin_supply_quality(restored,as_of_ms=at) == await store.stablecoin_supply_quality(at)
            quality_checks += 2
        assert report["shared_quality_checks"][-1]["crypto_news"]["state"] == "BLOCKED"
    finally:
        await store.close()
    n = read(news / "report.json")
    raw = (news / "response.xml").read_bytes()
    assert n["entities"][0]["sha256"] == digest(news / "response.xml") and n["entities"][0]["bytes"] == len(raw)
    dom = parse(BytesIO(raw),forbid_dtd=True,forbid_entities=True,forbid_external=True)
    channel = child(dom.documentElement,"channel")
    assert len(channel) == 1
    items = child(channel[0],"item")
    assert len(items) == 25
    for item in items:
        matches = [r for r in native if r.kind == "news" and r.payload()["native_guid"] == field(item,"guid")]
        assert len(matches) == 1
        row = matches[0]
        for xml,payload in (("title","native_title"),("link","native_link"),("guid","native_guid"),("pubDate","native_pub_date")):
            assert row.payload()[payload] == field(item,xml)
        assert row.event_time_ms == int(parsedate_to_datetime(field(item,"pubDate")).timestamp()*1000)
        assert row.observed_at_ms == n["entities"][0]["received_at_ms"]
    s = read(supply / "report.json")
    responses = []
    for i, entry in enumerate(s["entities"],1):
        path = supply / f"response-{i:02}.json"
        assert digest(path) == entry["sha256"] and path.read_bytes().hex() == entry["body_hex"]
        response = read(path)
        assert response["id"] == i and "error" not in response
        responses.append(response["result"])
    assert len(responses) == 7 and responses[0] == "0x1" and responses[1] == responses[-1]
    pair = [r for r in native if r.kind == "on_chain"]
    assert len(pair) == 1
    row = pair[0]
    p = row.payload()
    header = responses[1]
    assert p["block_number"] == int(header["number"],16) and p["block_hash"] == header["hash"]
    assert row.event_time_ms == int(header["timestamp"],16)*1000 and row.observed_at_ms == s["entities"][-1]["received_at_ms"]
    for name, supply_index, decimal_index in (("usdc",2,3),("usdt",4,5)):
        assert p[name]["total_supply_abi_word"] == responses[supply_index]
        assert p[name]["total_supply_base_units"] == str(int(responses[supply_index],16))
        assert p[name]["decimals_abi_word"] == responses[decimal_index]
        assert p[name]["decimals"] == int(responses[decimal_index],16)
    assert report["corrupt_restore"]["final_database_published"] is False
    assert not (PROOF / "corrupt-restore/events.duckdb").exists()
    assert read(PROOF / "corrupt-restore/restore-failed.json")["status"] == "FAILED"
    assert not (PROOF / "over-budget").exists()
    previous = ROOT.parent / "selective-options-0.3.41.zip"
    assert digest(previous) == "92bacd0103d727e2f34f88cad8525fce7f59f7021aac27468332a1a754fee5bb"
    assert proof_before == hashes(PROOF) and originals == {p.name:hashes(p) for p in (news,supply)}
    result = {"version":"0.3.42","status":"INDEPENDENT_ROTATION_COHORT_AND_NATIVE_RESTORE_VERIFIED","new_public_requests":0,"exact_source_records":39,"exact_eligible_restored_records":35,"native_records_preserved":28,"native_news_items_independently_compared":25,"native_supply_entities_independently_compared":7,"synthetic_records":11,"future_clock_records_excluded":4,"sealed_parts_verified":2,"per_record_sql_pit_checks":pit_checks,"reported_sql_cutoff_views_verified":len(report["pit_checks"]),"quality_views_verified":quality_checks,"source_acknowledgement_preserved":True,"restored_pending_delivery_records":35,"corrupt_restore_refused":True,"partition_budget_refused_before_output":True,"proof_evidence_unchanged":True,"original_public_evidence_unchanged":True,"previous_release_unchanged":True,"evidence_hashes":proof_before,"original_hashes":originals,"source_hashes":{p.relative_to(ROOT).as_posix():digest(p) for p in sorted((ROOT / "src/selective_options").glob("*.py"))},"history_complete":False,"milestone_2_complete":False,"trading_enabled":False}
    with OUTPUT.open("x",encoding="utf-8") as stream:
        stream.write(json.dumps(result,indent=2)+"\n")
    print(json.dumps({k:v for k,v in result.items() if "hash" not in k}))


asyncio.run(main())
