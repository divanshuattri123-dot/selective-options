"""Independent read-only saved news and supply boundary/archive review; no network."""
import asyncio, hashlib, json
from pathlib import Path
import duckdb
from selective_options.events import MarketEvent
from selective_options.store import DuckStore
from selective_options.crypto_news import crypto_news_quality
from selective_options.stablecoin_supply import stablecoin_supply_quality
from selective_options.stablecoin_checkpoint import load_stablecoin_checkpoint
from selective_options.errors import IntegrityError

ROOT=Path.cwd()
OUT=ROOT/'evidence/news-supply-parent-validation-0340.json'
assert not OUT.exists()
def digest(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p): return json.loads(p.read_text(encoding='utf-8'))
def hashes(p): return {q.name:digest(q) for q in p.iterdir() if q.is_file()}
def read_rows(c,sql,params=None):
    cur=c.execute(sql,params or []); cols=[v[0] for v in cur.description]
    return [MarketEvent.model_validate(dict(zip(cols,r,strict=True))) for r in cur.fetchall()]
def boundaries(c,rows):
    for row in rows:
        at=max(row.event_time_ms,row.observed_at_ms)
        for cutoff,visible in [(at-1,False),(at,True)]:
            ids={v[0] for v in c.execute('SELECT event_id FROM events WHERE event_time_ms<=? AND observed_at_ms<=?',[cutoff,cutoff]).fetchall()}
            assert (row.event_id in ids)==visible
    return 2*len(rows)
def archive(c,p,expected):
    m=read(p.with_suffix(p.suffix+'.manifest.json'))
    assert m['sha256']==digest(p) and m['bytes']==p.stat().st_size and m['rows']==len(expected)
    result=read_rows(c,'SELECT * FROM read_parquet(?) ORDER BY event_id',[str(p)])
    assert result==sorted(expected,key=lambda r:r.event_id)
async def main():
    news=ROOT/'evidence/crypto-news-public-01'; supply=ROOT/'evidence/stablecoin-restart-01'
    native=ROOT/'evidence/stablecoin-supply-public-01'
    before={p.name:hashes(p) for p in [news,supply,native]}
    n=read(news/'report.json'); s=read(supply/'report.json'); correction=read(news/'post-probe-provenance.json')
    assert digest(news/'source-snapshot.py')==digest(ROOT/'src/selective_options/crypto_news.py')==n['source_code_sha256_before']==n['source_code_sha256_after']
    historical=(news/'verification-script.py').read_bytes(); final=(ROOT/'scripts/verify_crypto_news.py').read_bytes()
    assert hashlib.sha256(historical).hexdigest()==n['verifier_code_sha256_before']==correction['historical_probe_verifier_sha256']
    assert hashlib.sha256(final).hexdigest()==correction['final_current_verifier_sha256']
    assert historical.replace(b'exact decoded HTTP entity bytes passed to production parser',b'exact decoded HTTP entity bytes observed by production response hook')==final
    entity=n['entities'][0]; raw=(news/'response.xml').read_bytes()
    assert len(raw)==entity['bytes']==15 and digest(news/'response.xml')==entity['sha256']
    assert entity['http_status']==308 and n['request_attempts']==1 and n['retries']==0 and not n['redirects_allowed']
    c=duckdb.connect(str(news/'events.duckdb'),read_only=True)
    rows=read_rows(c,'SELECT * FROM events ORDER BY event_id')
    assert len(rows)==1 and rows[0].kind=='feed_health' and rows[0].payload()['http_status']==308
    archive(c,news/'events.parquet',rows)
    assert c.execute('SELECT event_id, delivered FROM delivery_outbox ORDER BY event_id').fetchall()==[(r.event_id,False) for r in rows]
    checks=boundaries(c,rows); store=DuckStore(c,5000)
    try:
        assert crypto_news_quality(rows,as_of_ms=n['quality']['as_of_ms'])==n['quality']==await store.crypto_news_quality(n['quality']['as_of_ms'])
        assert n['quality']['state']=='BLOCKED' and n['persisted_metadata_items']==0
    finally: await store.close()
    for name,h in s['source_hashes_before'].items(): assert digest(native/name)==h==s['source_hashes_after'][name]
    for name,h in s['current_verification_code_hashes'].items():
        code=ROOT/('scripts' if name.startswith('verify_') else 'src/selective_options')/name
        assert digest(code)==h
    c=duckdb.connect(str(supply/'events.duckdb'),read_only=True)
    rows=read_rows(c,'SELECT * FROM events ORDER BY event_id')
    assert len(rows)==7 and c.execute('SELECT event_id, delivered FROM delivery_outbox ORDER BY event_id').fetchall()==[(r.event_id,False) for r in rows]
    originals=read_rows(c,'SELECT * FROM read_parquet(?) ORDER BY event_id',[str(native/'events.parquet')])
    assert len(originals)==2 and all(r in rows for r in originals)
    synthetic=[MarketEvent.model_validate(r) for r in s['synthetic_records']]
    assert len(synthetic)==5 and set(r.event_id for r in rows)==set(r.event_id for r in originals+synthetic)
    phase_rows={}
    for name,phase in s['phases'].items():
        p=supply/phase['archive']; assert digest(p)==phase['sha256']
        phase_rows[name]=read_rows(c,'SELECT * FROM read_parquet(?) ORDER BY event_id',[str(p)])
        assert len(phase_rows[name])==phase['rows']
        archive(c,p,phase_rows[name])
    assert phase_rows['original']==originals
    assert all(r in rows for values in phase_rows.values() for r in values)
    checks+=boundaries(c,rows); store=DuckStore(c,5000); quality_checks=1
    try:
        for name,expected in s['quality_scenarios'].items():
            at=expected['as_of_ms']; pure=stablecoin_supply_quality(rows,as_of_ms=at)
            assert pure==expected==await store.stablecoin_supply_quality(at)
            if expected['state']=='POLL_OBSERVED_VERIFIED':
                checkpoint=await load_stablecoin_checkpoint(supply/'events.duckdb',at)
                assert checkpoint==next(r for r in rows if r.event_id==expected['accepted_event_id'])
            else:
                try: await load_stablecoin_checkpoint(supply/'events.duckdb',at)
                except IntegrityError as exc: assert str(exc)==expected['blocker']
                else: raise AssertionError('failed checkpoint accepted')
            quality_checks+=1
    finally: await store.close()
    assert before=={p.name:hashes(p) for p in [news,supply,native]}
    result={'version':'0.3.40','status':'INDEPENDENT_SAVED_NEWS_SUPPLY_ARCHIVE_QUALITY_CHECKPOINT_VERIFIED','new_public_requests':0,'sql_pit_boundary_checks':checks,'canonical_quality_checks':quality_checks,'news_http_status':308,'news_native_metadata':0,'news_failure_persisted':True,'news_historical_verifier_sha256':digest(news/'verification-script.py'),'news_current_verifier_sha256':digest(ROOT/'scripts/verify_crypto_news.py'),'news_label_only_correction_verified':True,'supply_original_native_rows':2,'supply_synthetic_rows':5,'supply_archives_verified':3,'supply_failure_checkpoint_refused':True,'source_evidence_hashes':before,'source_hashes':{p.relative_to(ROOT).as_posix():digest(p) for p in sorted((ROOT/'src/selective_options').glob('*.py'))},'milestone_2_complete':False,'trading_enabled':False}
    with OUT.open('x',encoding='utf-8') as f: f.write(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if 'hash' not in k}))
asyncio.run(main())
