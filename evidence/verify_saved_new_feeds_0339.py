"""Read-only independent SQL/archive/native checks of the finite .39 evidence."""
import asyncio, hashlib, json
from pathlib import Path
import duckdb
from selective_options.events import MarketEvent
from selective_options.store import DuckStore
from selective_options.stablecoin_supply import stablecoin_supply_quality
from selective_options.tips_yield import tips_yield_quality
from selective_options.liquidations import liquidations_quality
from selective_options.solana_quality import network_quality

ROOT=Path.cwd()
OUTPUT=ROOT/'evidence/new-feed-parent-validation-0339.json'
assert not OUTPUT.exists(), 'immutable parent proof already exists'
def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def read(path): return json.loads(path.read_text(encoding='utf-8'))
async def verify(name,feed):
    path=ROOT/'evidence'/name
    report=read(path/'report.json')
    names=['report.json','events.duckdb','events.parquet','events.parquet.manifest.json']
    hashes={n:digest(path/n) for n in names}
    manifest=read(path/'events.parquet.manifest.json')
    assert manifest['sha256']==digest(path/'events.parquet')
    assert manifest['bytes']==(path/'events.parquet').stat().st_size
    conn=duckdb.connect(str(path/'events.duckdb'),read_only=True)
    cursor=conn.execute('SELECT * FROM events ORDER BY event_id');cols=[d[0] for d in cursor.description];tuples=cursor.fetchall()
    assert tuples==conn.execute('SELECT * FROM read_parquet(?) ORDER BY event_id',[str(path/'events.parquet')]).fetchall()
    rows=[MarketEvent.model_validate(dict(zip(cols,r,strict=True))) for r in tuples]
    outbox=conn.execute('SELECT event_id, delivered FROM delivery_outbox ORDER BY event_id').fetchall()
    assert outbox==[(r.event_id,False) for r in rows]
    assert len(rows)==manifest['rows']
    for row in rows:
        boundary=max(row.event_time_ms,row.observed_at_ms)
        for at,visible in [(boundary-1,False),(boundary,True)]:
            ids={r[0] for r in conn.execute('SELECT event_id FROM events WHERE event_time_ms<=? AND observed_at_ms<=?',[at,at]).fetchall()}
            assert (row.event_id in ids)==visible
    store=DuckStore(conn,5000)
    checks=0
    try:
        if feed in ['stablecoin_supply','tips_yield']:
            expected=report['quality'];at=expected['as_of_ms']
            pure=stablecoin_supply_quality(rows,as_of_ms=at) if feed=='stablecoin_supply' else tips_yield_quality(rows,as_of_ms=at)
            assert pure==expected
            assert await getattr(store,feed+'_quality')(at)==expected;checks+=1
            native=[r for r in rows if r.kind!='feed_health']
            source=ROOT/'src/selective_options'/f'{feed}.py'
            assert digest(source)==report['source_code_sha256_before']==report['source_code_sha256_after']
            if feed=='stablecoin_supply':
                assert len(native)==1 and len(rows)==2 and report['real_public_requests_attempted']==7
                values=[json.loads(bytes.fromhex(e['body_hex']))['result'] for e in report['entities']]
                for e in report['entities']:
                    b=bytes.fromhex(e['body_hex']);assert len(b)==e['bytes'] and hashlib.sha256(b).hexdigest()==e['sha256']
                payload=native[0].payload()
                assert values[0]=='0x1' and values[1]==values[6]
                assert int(values[1]['timestamp'],16)*1000==native[0].event_time_ms
                assert native[0].observed_at_ms==report['entities'][6]['received_at_ms']
                for symbol,index in [('usdc',2),('usdt',4)]:
                    assert payload[symbol]['total_supply_base_units']==str(int(values[index],16))
                    assert payload[symbol]['total_supply_abi_word']==values[index]
                    assert payload[symbol]['decimals']==int(values[index+1],16)==6
                assert expected['state']!='BLOCKED'
            else:
                assert native==[] and len(rows)==1 and report['real_public_requests_attempted']==1
                assert report['entities']==[] and expected['state']=='BLOCKED' and expected['error_type']=='TimeoutError'
        elif feed=='liquidations':
            assert digest(ROOT/'src/selective_options/liquidations.py')==report['source_code_sha256_before']==report['source_code_sha256_after']
            for field,cutoff in [('quality_before_stop','pre_stop_as_of_ms'),('quality_after_stop','final_as_of_ms')]:
                for asset,expected in report[field].items():
                    assert liquidations_quality(rows,asset=asset,as_of_ms=report[cutoff])==expected
                    assert await store.liquidations_quality(asset,report[cutoff])==expected;checks+=1
            frames=read(path/'frames.json')
            for e in frames:
                raw=bytes.fromhex(e['body_hex']);assert len(raw)==e['bytes'] and hashlib.sha256(raw).hexdigest()==e['sha256']
            native=[r for r in rows if r.kind=='liquidation']
            for row in native:
                matches=[f for f in frames if json.loads(bytes.fromhex(f['body_hex'])).get('data')==row.payload()['native']]
                assert matches and row.observed_at_ms==min(f['received_at_ms'] for f in matches)
            assert len(native)==report['native_observations']
            assert report['real_public_connection_attempts']==1 and report['retries']==0
        else:
            original=ROOT/'evidence/solana-network-public-01'
            expected=read(original/'report.json')['quality'];at=expected['as_of_ms']
            assert len(rows)==1003 and len([r for r in rows if r.symbol!='UNRELATED_FIXTURE'])==3
            assert network_quality(rows,at,max_rows=3)==expected
            assert await store.solana_network_quality(at,max_rows=3)==expected;checks+=1
            assert (await store.solana_network_quality(at,max_rows=2))['state']=='BLOCKED';checks+=1
            for n,h in report['source_hashes_unchanged'].items(): assert digest(original/n)==h
    finally: await store.close()
    assert hashes=={n:digest(path/n) for n in names}
    return {'evidence':name,'retained_rows':len(rows),'independent_sql_pit_checks':len(rows)*2,'canonical_quality_checks':checks,'archive_outbox_exact':True,'saved_hashes_unchanged':hashes,'public_outcome':report['status']}
async def main():
    results=[]
    for name,feed in [('stablecoin-supply-public-01','stablecoin_supply'),('tips-yield-public-01','tips_yield'),('liquidations-public-01','liquidations'),('solana-store-quality-01','solana')]: results.append(await verify(name,feed))
    report={'status':'INDEPENDENT_SAVED_NATIVE_ARCHIVE_AND_QUALITY_VERIFIED','version':'0.3.39','results':results,'new_public_requests':0,'source_hashes':{p.relative_to(ROOT).as_posix():digest(p) for p in sorted((ROOT/'src/selective_options').glob('*.py'))},'milestone_2_complete':False,'trading_enabled':False}
    with OUTPUT.open('x',encoding='utf-8') as f: f.write(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'status':report['status'],'results':[{k:v for k,v in r.items() if k!='saved_hashes_unchanged'} for r in results]}))
asyncio.run(main())
