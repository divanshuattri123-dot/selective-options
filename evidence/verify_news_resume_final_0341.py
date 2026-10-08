"""Independent read-only .41 native news, historical failure, supply capture and archive checks."""
import asyncio,hashlib,json,zipfile
from pathlib import Path
from io import BytesIO
from html.parser import HTMLParser
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin
import duckdb
from defusedxml.minidom import parse
from selective_options.events import MarketEvent
from selective_options.store import DuckStore
from selective_options.crypto_news import crypto_news_quality,FEED_URL,LEGACY_FEED_URL
from selective_options.stablecoin_checkpoint import load_stablecoin_resume
from selective_options.stablecoin_supply import stablecoin_supply_quality
from selective_options.errors import IntegrityError
ROOT=Path.cwd();OUTPUT=ROOT/'evidence/news-resume-final-validation-0341.json'
assert not OUTPUT.exists()
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def hashes(p):return {q.relative_to(p).as_posix():digest(q) for q in p.rglob('*') if q.is_file()}
def rows(c,sql,params=None):
    cur=c.execute(sql,params or []);cols=[v[0] for v in cur.description]
    return [MarketEvent.model_validate(dict(zip(cols,r,strict=True))) for r in cur.fetchall()]
def pit(c,items):
    for row in items:
        boundary=max(row.event_time_ms,row.observed_at_ms)
        for at,expected in [(boundary-1,False),(boundary,True)]:
            ids={v[0] for v in c.execute('SELECT event_id FROM events WHERE event_time_ms<=? AND observed_at_ms<=?',[at,at]).fetchall()}
            assert (row.event_id in ids)==expected
    return len(items)*2
def archive(c,path,items):
    m=read(path.with_suffix(path.suffix+'.manifest.json'))
    assert m['sha256']==digest(path) and m['rows']==len(items) and m['bytes']==path.stat().st_size
    assert rows(c,'SELECT * FROM read_parquet(?) ORDER BY event_id',[str(path)])==sorted(items,key=lambda r:r.event_id)
def outbox(c,items):
    assert c.execute('SELECT event_id,delivered FROM delivery_outbox ORDER BY event_id').fetchall()==[(r.event_id,False) for r in sorted(items,key=lambda r:r.event_id)]
class Links(HTMLParser):
    def __init__(self):super().__init__();self.hrefs=[]
    def handle_starttag(self,tag,attrs):
        if tag=='a':self.hrefs.extend(v for k,v in attrs if k=='href' and v is not None)
def child(parent,name):return [x for x in parent.childNodes if x.nodeType==x.ELEMENT_NODE and x.tagName==name]
def native(parent,name):
    found=child(parent,name);assert len(found)==1
    return ''.join(x.data for x in found[0].childNodes if x.nodeType in (x.TEXT_NODE,x.CDATA_SECTION_NODE))
async def main():
    discovery=ROOT/'evidence/news-endpoint-discovery-01';news=ROOT/'evidence/crypto-news-public-02';old=ROOT/'evidence/crypto-news-public-01';resume=ROOT/'evidence/stablecoin-capture-resume-01';original=ROOT/'evidence/stablecoin-supply-public-01'
    directories=[discovery,news,old,resume,original];before={p.name:hashes(p) for p in directories}
    d=read(discovery/'discovery.json');assert d['real_public_requests_attempted']==3 and d['automatic_redirect_follows']==d['retries']==0
    e=d['responses'];assert len(e)==3
    for item in e:
        path=discovery/item['body_file'];assert digest(path)==item['body_sha256'] and path.stat().st_size==item['body_bytes']
        assert item['started_at_ms']<=item['received_at_ms'] and not item['redirects_allowed']
    assert [v['http_status'] for v in e]==[308,200,200]
    assert e[0]['url']==LEGACY_FEED_URL and urljoin(e[0]['url'],e[0]['location_header'])==e[2]['url']==FEED_URL
    links=Links();links.feed((discovery/e[1]['body_file']).read_text(encoding='utf-8'))
    assert any(urljoin(e[1]['url'],href).split('?')[0].rstrip('/')=='https://www.coindesk.com/arc/outboundfeeds/rss' for href in links.hrefs)
    n=read(news/'report.json');assert n['status']=='PUBLIC_CRYPTO_NEWS_METADATA_VERIFIED' and n['request_attempts']==1 and n['retries']==0 and not n['redirects_allowed']
    entity=n['entities'][0];raw=(news/'response.xml').read_bytes();assert len(raw)==entity['bytes']==30250 and digest(news/'response.xml')==entity['sha256']
    assert entity['http_status']==200 and entity['location_header']['state']=='ABSENT'
    dom=parse(BytesIO(raw),forbid_dtd=True,forbid_entities=True,forbid_external=True)
    channel=child(dom.documentElement,'channel');assert len(channel)==1;items=child(channel[0],'item');assert len(items)==25
    c=duckdb.connect(str(news/'events.duckdb'),read_only=True);saved=rows(c,'SELECT * FROM events ORDER BY event_id');assert len(saved)==26
    metadata=[r for r in saved if r.kind=='news'];assert len(metadata)==25
    for item in items:
        guid=native(item,'guid');matches=[r for r in metadata if r.payload()['native_guid']==guid];assert len(matches)==1;row=matches[0];payload=row.payload()
        for native_name,field in [('title','native_title'),('link','native_link'),('guid','native_guid'),('pubDate','native_pub_date')]: assert payload[field]==native(item,native_name)
        assert row.event_time_ms==int(parsedate_to_datetime(native(item,'pubDate')).timestamp()*1000)
        assert row.observed_at_ms==entity['received_at_ms'] and payload['feed_url']==FEED_URL and not payload['first_publication_independently_verified']
    archive(c,news/'events.parquet',saved);outbox(c,saved);checks=pit(c,saved);quality_checks=0;store=DuckStore(c,5000)
    try:
        expected=n['quality'];assert crypto_news_quality(saved,as_of_ms=expected['as_of_ms'])==expected==await store.crypto_news_quality(expected['as_of_ms']);quality_checks+=1
        assert (await store.crypto_news_quality(expected['as_of_ms'],max_rows=25))['blocker']=='CRYPTO_NEWS_QUALITY_OVER_BUDGET';quality_checks+=1
    finally:await store.close()
    assert n['source_code_sha256_before']==n['source_code_sha256_after']==digest(news/'source-snapshot.py')
    provenance=read(ROOT/'evidence/news-provenance-0341/report.json')
    assert digest(ROOT/'src/selective_options/crypto_news.py')==provenance['final_current_source_sha256']
    assert digest(news/'source-snapshot.py')==provenance['historical_probe_source_sha256']
    old_source=(news/'source-snapshot.py').read_bytes().replace(b'\r\n',b'\n')
    final_source=(ROOT/'src/selective_options/crypto_news.py').read_bytes().replace(b'\r\n',b'\n')
    assert old_source.replace(b'            row = news[identity]\n',b'            row = news[identity]\n            if row.payload()["feed_url"] != status.feed_url:\n                raise IntegrityError("CRYPTO_NEWS_POLL_PROVENANCE_MISMATCH")\n',1)==final_source
    assert digest(ROOT/'scripts/verify_crypto_news.py')==n['verifier_code_sha256_before']==n['verifier_code_sha256_after']==digest(news/'verification-script.py')
    oldreport=read(old/'report.json');c=duckdb.connect(str(old/'events.duckdb'),read_only=True);oldrows=rows(c,'SELECT * FROM events ORDER BY event_id');store=DuckStore(c,5000)
    try:assert crypto_news_quality(oldrows,as_of_ms=oldreport['quality']['as_of_ms'])==oldreport['quality']==await store.crypto_news_quality(oldreport['quality']['as_of_ms']);quality_checks+=1
    finally:await store.close()
    migration=read(discovery/'verification.json')
    for rel,h in migration['historical_0340_files_sha256'].items():assert digest(ROOT/rel)==h
    r=read(resume/'report.json');assert r['real_public_requests']==0 and r['mock_http_attempts']==8
    for rel,h in r['current_code_hashes'].items():assert digest(ROOT/rel)==h
    for name,h in r['source_hashes_unchanged'].items():assert digest(original/name)==h
    for phase,expected in r['phases'].items():
        path=resume/phase;c=duckdb.connect(str(path/'events.duckdb'),read_only=True);saved=rows(c,'SELECT * FROM events ORDER BY event_id')
        assert len(saved)==expected['rows'];archive(c,path/'events.parquet',saved);outbox(c,saved);checks+=pit(c,saved)
        assert sorted([MarketEvent.model_validate(v) for v in expected['copied_pair']+expected['synthetic_emitted']],key=lambda v:v.event_id)==saved
        store=DuckStore(c,5000)
        try:
            q=expected['quality'];at=q['as_of_ms'];assert stablecoin_supply_quality(saved,as_of_ms=at)==q==await store.stablecoin_supply_quality(at);quality_checks+=1
            if phase=='failure':
                try:await load_stablecoin_resume(path/'events.duckdb',at)
                except IntegrityError as exc:assert str(exc)==q['blocker']
                else:raise AssertionError('failed resume accepted')
            else:
                pair=await load_stablecoin_resume(path/'events.duckdb',at);assert pair[0].event_id==q['accepted_event_id'] and pair[1].event_id==q['health_event_id']
        finally:await store.close()
    assert not (resume/'refused').exists()
    previous=ROOT.parent/'selective-options-0.3.40.zip'
    assert digest(previous)=='0a5410f1dde58768b934a0a8ee17235659e5bb5ef9bdc4ae7a7c7411fa1712d4'
    with zipfile.ZipFile(previous) as z:
        oldsource=z.read('selective-options/src/selective_options/crypto_news.py');assert hashlib.sha256(oldsource).hexdigest()==migration['original_0340_source_sha256']
        history=read(ROOT/'evidence/stablecoin-restart-01/report.json')
        assert hashlib.sha256(z.read('selective-options/src/selective_options/stablecoin_checkpoint.py')).hexdigest()==history['current_verification_code_hashes']['stablecoin_checkpoint.py']
    assert before=={p.name:hashes(p) for p in directories}
    result={'version':'0.3.41','status':'INDEPENDENT_CANONICAL_NEWS_AND_CAPTURE_RESUME_VERIFIED','new_public_requests':0,'native_news_items_independently_compared':25,'discovery_requests_verified':3,'public_collector_requests_verified':1,'sql_pit_checks':checks,'quality_views_verified':quality_checks,'historical_news_failure_quality_unchanged':True,'actual_capture_resume_archives_verified':2,'saved_native_rows_preserved':2,'supply_later_outcomes_synthetic':True,'failed_capture_resume_refused':True,'previous_release_unchanged':True,'evidence_hashes':before,'post_probe_guard_verified':True,'source_hashes':{p.relative_to(ROOT).as_posix():digest(p) for p in sorted((ROOT/'src/selective_options').glob('*.py'))},'milestone_2_complete':False,'trading_enabled':False}
    with OUTPUT.open('x',encoding='utf-8') as f:f.write(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if 'hash' not in k}))
asyncio.run(main())
