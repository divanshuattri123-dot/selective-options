import hashlib,json,duckdb
from pathlib import Path
from selective_options.events import MarketEvent
from selective_options.eth_gas_quality import eth_gas_quality
from selective_options.eth_checkpoint import validate_checkpoint
from selective_options.news_quality import policy_news_quality
root=Path.cwd()
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def rows(path):
    with duckdb.connect(str(path),read_only=True) as db:
        c=db.execute('SELECT * FROM events ORDER BY event_id')
        cols=[x[0] for x in c.description]
        return [MarketEvent.model_validate(dict(zip(cols,r,strict=True))) for r in c.fetchall()]
def archive(path):
    manifest=json.loads(path.with_suffix(path.suffix+'.manifest.json').read_text())
    assert sha(path)==manifest['sha256'] and path.stat().st_size==manifest['bytes']
    with duckdb.connect() as db:
        values=db.execute('SELECT * FROM read_parquet(?) ORDER BY event_id',[str(path)]).fetchall()
    assert len(values)==manifest['rows']
    return values
ethdir=root/'evidence/eth-gas-runtime-public-01'
report=json.loads((ethdir/'report.json').read_text())
assert report['status']=='PUBLIC_GAS_RESTART_VERIFIED' and report['public_requests_attempted']==4
assert report['source_hashes_unchanged'] and report['first_database_unchanged_after_handoff']
for name,digest in report['source_hashes_before'].items(): assert sha(root/name)==digest
checks=[]
for stage in ['first','restarted']:
    folder=ethdir/stage
    saved=rows(folder/'events.duckdb')
    assert archive(folder/'events.parquet')==[tuple(e.model_dump().values()) for e in saved]
    phase=report[stage]
    assert len(saved)==phase['rows']==2
    with duckdb.connect(str(folder/'events.duckdb'),read_only=True) as db:
        assert db.execute('SELECT count(*) FROM delivery_outbox WHERE NOT delivered').fetchone()==(2,)
    responses=json.loads((folder/'raw-responses.json').read_text())
    assert len(responses)==2
    assert [r['method'] for r in responses]==['eth_chainId','eth_getBlockByNumber']
    decoded=[]
    for r in responses:
        body=bytes.fromhex(r['raw_body_hex']);assert hashlib.sha256(body).hexdigest()==r['body_sha256']
        rpc=json.loads(body);assert rpc['jsonrpc']=='2.0' and type(rpc['id']) is int and rpc['id']==r['rpc_id']
        assert r['http_status']==200 and r['params']==([] if r['method']=='eth_chainId' else ['finalized',False])
        decoded.append(rpc['result'])
    assert int(decoded[0],16)==1
    raw=decoded[1]
    gas=next(e for e in saved if e.kind=='on_chain'); health=next(e for e in saved if e.kind=='feed_health')
    payload=gas.payload()
    for target,native in [('block_number','number'),('gas_limit','gasLimit'),('gas_used','gasUsed'),('base_fee_per_gas_wei','baseFeePerGas')]:
        assert int(payload[target])==int(raw[native],16)
    assert payload['block_hash']==raw['hash'].lower() and payload['parent_hash']==raw['parentHash'].lower()
    assert payload['transaction_count']==len(raw['transactions'])
    assert gas.event_time_ms==int(raw['timestamp'],16)*1000
    assert health.payload()['response_received_at_ms']==responses[1]['observed_at_ms']
    assert gas.observed_at_ms==report['first']['quality']['accepted_observed_at_ms']
    validate_checkpoint(gas,phase['quality']['as_of_ms'],provider='drpc')
    assert eth_gas_quality(saved,provider='drpc',as_of_ms=phase['quality']['as_of_ms'],max_poll_age_ms=600000,max_block_age_ms=1800000)==phase['quality']
    checks.append({'stage':stage,'rows':2,'raw_rpc_exact_native_fields':True,'quality_exact':True,'archive_outbox_exact':True,'retained_receipt':gas.observed_at_ms,'response_receipt':responses[1]['observed_at_ms'],'db_sha256':sha(folder/'events.duckdb')})
newsdir=root/'evidence/news-restart-01'
news=json.loads((newsdir/'report.json').read_text())
assert news['real_public_requests']==0 and news['persisted_records']==19 and news['exact_closed_reloads']==3
assert news['source_hashes_before']==news['source_hashes_after']
for name,digest in news['source_hashes_before'].items(): assert sha(Path(news['source'])/name)==digest
native=rows(Path(news['source'])/'events.duckdb'); final=rows(newsdir/'events.duckdb')
assert all(e in final for e in native)
assert {e.event_id for e in final}-{e.event_id for e in native}=={v['event_id'] for v in news['synthetic_health_records'].values()}
assert archive(newsdir/'events.parquet')==[tuple(e.model_dump().values()) for e in final]
assert len(archive(newsdir/'failure.parquet'))==18 and len(archive(newsdir/'before-restart.parquet'))==17
for name,scenario in news['quality_scenarios'].items():
    assert policy_news_quality(final,source='federal_reserve',as_of_ms=scenario['as_of_ms'])==scenario, name
result={'status':'INDEPENDENT_SAVED_RESTART_EVIDENCE_VERIFIED','eth_public_poll_stages':checks,'same_native_block_in_both_real_polls':True,'eth_public_requests':4,'source_hashes_exact_current_code':True,'news_native_records':16,'news_labelled_synthetic_health':3,'news_exact_original_rows_preserved':True,'news_real_requests':0,'news_quality_scenarios_exact':len(news['quality_scenarios']),'milestone_2_complete':False,'trading_enabled':False}
with (root/'evidence/restart-parent-validation-0338.json').open('x') as f: json.dump(result,f,indent=2);f.write('\n')
print(json.dumps(result))
