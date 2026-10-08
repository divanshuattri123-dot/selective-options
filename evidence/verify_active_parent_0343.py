"""Read-only parent audit of actual offline supervised capture and open-store snapshots."""
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
PROOF = ROOT / "evidence/active-rotation-saved-public-01"
OUTPUT = ROOT / "evidence/active-rotation-parent-validation-0343.json"


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def hashes(path):
    return {p.relative_to(path).as_posix():digest(p) for p in path.rglob("*") if p.is_file()}


def events(conn, sql, params=None):
    cursor = conn.execute(sql,params or [])
    columns = [entry[0] for entry in cursor.description]
    return [MarketEvent.model_validate(dict(zip(columns,row,strict=True)),strict=True) for row in cursor.fetchall()]


def child(parent,name):
    return [x for x in parent.childNodes if x.nodeType == x.ELEMENT_NODE and x.tagName == name]


def field(parent,name):
    matches = child(parent,name)
    assert len(matches) == 1
    return "".join(x.data for x in matches[0].childNodes if x.nodeType in (x.TEXT_NODE,x.CDATA_SECTION_NODE))


def archive(conn,path,expected):
    manifest = read(path.with_suffix(path.suffix+'.manifest.json'))
    assert manifest['sha256'] == digest(path) and manifest['bytes'] == path.stat().st_size
    assert manifest['rows'] == len(expected)
    assert events(conn,'SELECT * FROM read_parquet(?,hive_partitioning=false) ORDER BY event_id',[str(path)]) == expected


def pit(conn,items):
    for row in items:
        boundary = max(row.event_time_ms,row.observed_at_ms)
        for at,expected in ((boundary-1,0),(boundary,1)):
            assert conn.execute('SELECT count(*) FROM events WHERE event_id=? AND event_time_ms<=? AND observed_at_ms<=?',[row.event_id,at,at]).fetchone() == (expected,)
    return len(items)*2


async def main():
    assert not OUTPUT.exists()
    baseline = read((ROOT/'../../work/active-native-baseline-0343.json').resolve())
    for directory,expected in baseline['native_and_previous_proof_files'].items():
        assert hashes(ROOT/'evidence'/directory) == expected
    for path,expected in baseline['unchanged_previous_modules'].items():
        assert digest(ROOT/path) == expected
    assert digest(ROOT.parent/'selective-options-0.3.42.zip') == baseline['previous_zip_sha256']
    before = hashes(PROOF)
    proof = read(PROOF/'report.json')
    assert proof['status'] == 'OFFLINE_SUPERVISED_ACTIVE_ROTATION_VERIFIED'
    assert proof['real_public_requests_attempted'] == 0
    assert proof['saved_native_records'] == 28 and proof['synthetic_records'] == 4
    assert proof['captured_records'] == 32
    assert proof['code_sha256_before'] == proof['code_sha256_after']
    for name,expected in proof['code_sha256_before'].items():
        assert digest(ROOT/'src/selective_options'/name) == expected
    assert proof['verifier_sha256'] == digest(ROOT/'scripts/verify_active_rotation.py') == digest(PROOF/'verification-script.py')
    assert proof['native_helper_sha256'] == digest(ROOT/'scripts/verify_rotation.py')
    assert proof['later_commit_before_first_partition_validation'] is True
    assert proof['queue_empty_after_shutdown'] is proof['producer_cancelled_and_drained'] is proof['runtime_cancelled_and_drained'] is True
    capture_dir = PROOF/'capture'
    capture = read(capture_dir/'report.json')
    assert capture == proof['capture_report']
    assert digest(capture_dir/'events.duckdb') == proof['closed_capture_sha256']
    assert capture['status'] == 'CAPTURE_FINISHED' and capture['trading_enabled'] is False and capture['milestone_2_complete'] is False
    runtime = capture['archive_rotation']
    assert runtime['enabled'] is True and runtime['status'] == 'STOPPED'
    assert runtime['completed_snapshots'] == runtime['attempts'] == len(runtime['snapshots']) == 2
    assert runtime['history_complete'] is runtime['source_history_modified'] is runtime['delivery_acknowledgements_copied'] is runtime['trading_enabled'] is False
    native = []
    for name,count in (('crypto-news-public-02',26),('stablecoin-supply-public-01',2)):
        with duckdb.connect(str(ROOT/'evidence'/name/'events.duckdb'),read_only=True) as conn:
            rows = events(conn,'SELECT * FROM events ORDER BY event_id')
            assert len(rows) == count
            archive(conn,ROOT/'evidence'/name/'events.parquet',rows)
            native.extend(rows)
    native = sorted(native,key=lambda row:row.event_id)
    native_ids = {row.event_id for row in native}
    with duckdb.connect(str(capture_dir/'events.duckdb'),read_only=True) as source:
        all_rows = events(source,'SELECT * FROM events ORDER BY event_id')
        assert len(all_rows) == capture['rows'] == 32
        assert [row for row in all_rows if row.event_id in native_ids] == native
        archive(source,capture_dir/'events.parquet',all_rows)
        delivered = source.execute('SELECT event_id,delivered FROM delivery_outbox ORDER BY event_id').fetchall()
        assert len(delivered) == 32 and sum(flag for _,flag in delivered) == 1
        assert next(identity for identity,flag in delivered if flag) in native_ids
        assert capture['redis_delivery_pending'] == 31
        checks = pit(source,all_rows)
    snapshots = []
    quality_checks = 0
    for index,expected_count in enumerate((28,31)):
        relative = f'archive-rotations/snapshot-{index:06d}'
        directory = capture_dir/relative
        active = read(directory/'active-rotation.json')
        assert runtime['snapshots'][index] == {'directory':relative,**active}
        assert active['status'] == 'ACTIVE_ROTATION_VERIFIED' and active['rows'] == expected_count
        assert active['source_database_hash_claimed'] is active['delivery_acknowledgements_copied'] is active['history_complete'] is active['current_feed_health_verified'] is active['source_history_modified'] is active['later_commits_included'] is False
        assert active['producer_lock_released_before_partition_validation'] is True
        catalog = read(directory/'rotation/catalog.json')
        assert catalog['total_rows'] == expected_count and catalog['as_of_ms'] == active['as_of_ms']
        assert catalog['source_sha256'] == active['snapshot_sha256'] == digest(directory/'snapshot.duckdb')
        assert active['catalog_sha256'] == digest(directory/'rotation/catalog.json')
        with duckdb.connect(str(directory/'snapshot.duckdb'),read_only=True) as conn:
            rows = events(conn,'SELECT * FROM events ORDER BY event_id')
            assert len(rows) == expected_count and [r for r in rows if r.event_id in native_ids] == native
            assert conn.execute('SHOW TABLES').fetchall() == [('events',)]
            assert all(r.event_time_ms <= active['as_of_ms'] and r.observed_at_ms <= active['as_of_ms'] for r in rows)
            sealed = []
            for part in catalog['parts']:
                path = directory/'rotation'/part['file']
                manifest_path = path.with_suffix(path.suffix+'.manifest.json')
                manifest = read(manifest_path)
                assert manifest['sha256'] == part['sha256'] == digest(path)
                assert manifest['bytes'] == part['bytes'] == path.stat().st_size
                assert digest(manifest_path) == part['manifest_sha256']
                chunk = events(conn,'SELECT * FROM read_parquet(?,hive_partitioning=false) ORDER BY event_id',[str(path)])
                assert len(chunk) == part['rows'] == manifest['rows']
                assert all(part['start_ms'] <= max(r.event_time_ms,r.observed_at_ms) < part['end_ms'] for r in chunk)
                sealed.extend(chunk)
            assert sorted(sealed,key=lambda row:row.event_id) == rows
            checks += pit(conn,rows)
        restored = PROOF/f'restored-{index:02d}'
        restored_report = read(restored/'restore.json')
        assert restored_report['rows'] == restored_report['redis_delivery_pending'] == expected_count
        assert restored_report['catalog_sha256'] == active['catalog_sha256']
        conn = duckdb.connect(str(restored/'events.duckdb'),read_only=True)
        assert events(conn,'SELECT * FROM events ORDER BY event_id') == rows
        assert conn.execute('SELECT event_id,delivered FROM delivery_outbox ORDER BY event_id').fetchall() == [(row.event_id,False) for row in rows]
        checks += pit(conn,rows)
        store = DuckStore(conn,5000)
        try:
            for at in sorted({r.observed_at_ms for r in native if r.kind == 'feed_health'} | {active['as_of_ms']}):
                assert crypto_news_quality(rows,as_of_ms=at) == await store.crypto_news_quality(at)
                assert stablecoin_supply_quality(rows,as_of_ms=at) == await store.stablecoin_supply_quality(at)
                quality_checks += 2
        finally:
            await store.close()
        snapshots.append({'rows':expected_count,'as_of_ms':active['as_of_ms'],'snapshot_sha256':active['snapshot_sha256'],'catalog_sha256':active['catalog_sha256'],'restored_pending':expected_count})
    first = [p for p in all_rows if p.event_id not in native_ids]
    assert len(first) == 4
    news = ROOT/'evidence/crypto-news-public-02'
    report = read(news/'report.json')
    dom = parse(BytesIO((news/'response.xml').read_bytes()),forbid_dtd=True,forbid_entities=True,forbid_external=True)
    channels = child(dom.documentElement,'channel')
    assert len(channels) == 1
    items = child(channels[0],'item')
    assert len(items) == 25
    for item in items:
        matches = [r for r in native if r.kind == 'news' and r.payload()['native_guid'] == field(item,'guid')]
        assert len(matches) == 1
        row = matches[0]
        for xml,payload in (('title','native_title'),('link','native_link'),('guid','native_guid'),('pubDate','native_pub_date')):
            assert row.payload()[payload] == field(item,xml)
        assert row.event_time_ms == int(parsedate_to_datetime(field(item,'pubDate')).timestamp()*1000)
        assert row.observed_at_ms == report['entities'][0]['received_at_ms']
    supply = ROOT/'evidence/stablecoin-supply-public-01'
    report = read(supply/'report.json')
    results = []
    for index,entity in enumerate(report['entities'],1):
        path = supply/f'response-{index:02}.json'
        assert digest(path) == entity['sha256'] and path.read_bytes().hex() == entity['body_hex']
        raw = read(path)
        assert raw['id'] == index and 'error' not in raw
        results.append(raw['result'])
    assert len(results) == 7 and results[0] == '0x1' and results[1] == results[-1]
    row = next(r for r in native if r.kind == 'on_chain')
    payload = row.payload()
    assert payload['block_number'] == int(results[1]['number'],16) and payload['block_hash'] == results[1]['hash']
    assert row.event_time_ms == int(results[1]['timestamp'],16)*1000 and row.observed_at_ms == report['entities'][-1]['received_at_ms']
    for name,supply_index,decimal_index in (('usdc',2,3),('usdt',4,5)):
        assert payload[name]['total_supply_abi_word'] == results[supply_index]
        assert payload[name]['total_supply_base_units'] == str(int(results[supply_index],16))
        assert payload[name]['decimals_abi_word'] == results[decimal_index]
        assert payload[name]['decimals'] == int(results[decimal_index],16)
    assert before == hashes(PROOF)
    for name,expected in baseline['native_and_previous_proof_files'].items():
        assert hashes(ROOT/'evidence'/name) == expected
    result = {'version':'0.3.43','status':'INDEPENDENT_RUNNING_CAPTURE_ROTATION_VERIFIED','new_public_requests':0,'capture_records':32,'native_records_preserved':28,'synthetic_records':4,'periodic_snapshots_verified':2,'snapshot_rows':[28,31],'restored_pending_total':59,'original_source_pending':31,'original_source_acknowledged':1,'per_record_sql_pit_checks':checks,'quality_views_verified':quality_checks,'native_news_items_independently_compared':25,'native_supply_entities_independently_compared':7,'later_commit_before_first_partition_validation_verified':True,'actual_queue_barriers_drained':True,'producer_and_runtime_drained':True,'snapshot_metadata':snapshots,'proof_evidence_unchanged':True,'original_public_evidence_unchanged':True,'previous_release_unchanged':True,'previous_modules_unchanged':True,'evidence_hashes':before,'baseline':baseline,'source_hashes':{p.relative_to(ROOT).as_posix():digest(p) for p in sorted((ROOT/'src/selective_options').glob('*.py'))},'history_complete':False,'new_live_collection_performed':False,'seven_day_validation_completed':False,'milestone_2_complete':False,'trading_enabled':False}
    with OUTPUT.open('x',encoding='utf-8') as stream:
        stream.write(json.dumps(result,indent=2)+'\n')
    print(json.dumps({key:value for key,value in result.items() if key not in {'baseline','evidence_hashes','source_hashes'}}))


asyncio.run(main())
