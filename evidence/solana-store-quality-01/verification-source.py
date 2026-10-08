import asyncio,json,hashlib,duckdb
from pathlib import Path
from selective_options.events import MarketEvent,make_event
from selective_options.solana_quality import network_quality
from selective_options.store import DuckStore
from selective_options.archive import seal_archive
root=Path.cwd();source=root/'evidence/solana-network-public-01';output=root/'evidence/solana-store-quality-01'
names=['events.duckdb','events.parquet','events.parquet.manifest.json','report.json','results.json']
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
hashes={n:sha(source/n) for n in names}
with duckdb.connect(str(source/'events.duckdb'),read_only=True) as db:
    c=db.execute('SELECT * FROM events ORDER BY event_id');cols=[x[0] for x in c.description]
    native=[MarketEvent.model_validate(dict(zip(cols,r,strict=True))) for r in c.fetchall()]
original=json.loads((source/'report.json').read_text(encoding='utf-8'))
asof=original['quality']['as_of_ms'];assert len(native)==3
noise=[make_event(source='solana_public',environment='public_reference',asset='SOL',symbol='UNRELATED_FIXTURE',kind='on_chain',event_time_ms=1+i,observed_at_ms=1+i,payload={'fixture_origin':'UNRELATED_SCOPE_NOT_PUBLIC'}) for i in range(1000)]
rows=native+noise
async def run():
    output.mkdir(exist_ok=False)
    store=await DuckStore.open(output/'events.duckdb')
    try:
        await store.append(rows,exact=True)
        calls=[];select=store._select
        def tracked(sql,params):
            result=select(sql,params);calls.append((params[-1] if params else None,len(result)));return result
        store._select=tracked
        current=await store.solana_network_quality(asof,max_rows=3)
        assert current==original['quality'];assert calls==[(4,3)]
        checks=0
        for row in native:
            for cutoff in [max(row.event_time_ms,row.observed_at_ms)-1,max(row.event_time_ms,row.observed_at_ms)]:
                assert await store.solana_network_quality(cutoff,max_rows=3)==network_quality(rows,cutoff,max_rows=3);checks+=1
        assert (await store.solana_network_quality(asof,max_rows=2))['blocker']=='SOL_QUALITY_OVER_BUDGET'
        count=await store.export_parquet(output/'events.parquet');await seal_archive(output/'events.parquet',count)
        assert await store.pending_delivery_count()==1003
    finally:await store.close()
    store=await DuckStore.open(output/'events.duckdb')
    try:
        assert await store.solana_network_quality(asof,max_rows=3)==original['quality']
        assert set(await store.read_events([r.event_id for r in native]))==set(native)
        assert await store.pending_delivery_count()==1003
    finally:await store.close()
    with duckdb.connect(str(output/'events.duckdb'),read_only=True) as db:
        saved=db.execute('SELECT * FROM events ORDER BY event_id').fetchall()
        archived=db.execute('SELECT * FROM read_parquet(?) ORDER BY event_id',[str(output/'events.parquet')]).fetchall()
        assert saved==archived and len(saved)==1003
    assert hashes=={n:sha(source/n) for n in names}
    report={'status':'SAVED_PUBLIC_SOL_BOUNDED_QUERY_VERIFIED','native_rows':3,'labelled_unrelated_fixture_rows':1000,'retained_rows':1003,'selected_rows_at_exact_native_budget':3,'overflow_sentinel_fetch_limit':4,'native_quality_exact':True,'pit_boundary_checks':checks,'exact_archive_outbox_closed_reload':True,'source_hashes_unchanged':hashes,'new_public_requests':0,'sql_cost_bounded':False,'full_history_complete':False,'trading_enabled':False}
    (output/'report.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(report))
asyncio.run(run())
