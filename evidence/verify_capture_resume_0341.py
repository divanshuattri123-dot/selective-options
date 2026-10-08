"""Finite offline saved-native proof of actual capture startup and failed-pair refusal."""
import asyncio,hashlib,json
from pathlib import Path
import sys
sys.path.insert(0,str(Path.cwd()))
from unittest.mock import patch
import duckdb
from scripts.verify_stablecoin_restart import preflight,ReplaySession,read_rows,hashes
from selective_options import ingestion,stablecoin_supply
from selective_options.archive import _verify_archive
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.stablecoin_checkpoint import load_stablecoin_resume

ROOT=Path.cwd();OUT=ROOT/'evidence/stablecoin-capture-resume-01'
assert not OUT.exists()
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
async def idle(*a):await asyncio.Event().wait()
async def main():
    source=ROOT/'evidence/stablecoin-supply-public-01'
    saved=preflight(source,ROOT/'../../work/stablecoin-restart-0340/uncreated-capture-preflight')
    before=hashes(source);OUT.mkdir()
    config=AppConfig(system={'assets':['BTC']},ingestion={'stablecoin_supply_enabled':True,'reference_sources':[],'derivatives_enabled':False,'spot_trades_enabled':False,'policy_news_sources':[],'flush_interval_s':0.001})
    selected=[];attempts=[];phases={};original=list(read_rows(source/'events.duckdb',source/'events.parquet'))
    for phase,src,offset,failure,seed in [
        ('repeat',source/'events.duckdb',10000,False,saved.snapshot),
        ('failure',OUT/'repeat/events.duckdb',20000,True,None),
    ]:
        health_clock=saved.health.observed_at_ms+offset
        pair=await load_stablecoin_resume(src,health_clock)
        assert pair[0]==(seed if seed is not None else next(r for r in read_rows(src) if r.kind=='on_chain' and r.event_id==phases['repeat']['quality']['accepted_event_id']))
        session=ReplaySession(saved,health_clock,offset,failure)
        emitted=[]
        async def produce(collector):
            assert collector.previous==pair[0];selected.append(collector.previous.model_dump(mode='json'))
            native_sink=collector.sink
            async def recorded(row):
                emitted.append(row)
                await native_sink(row)
            collector.sink=recorded
            async def hook(request,status,body,received):
                assert status==200
                session.receipt=health_clock
            collector.response_received=hook
            accepted=await collector.collect(session)
            assert accepted!=failure
            await idle()
        with patch.object(ingestion,'now_ms',lambda:health_clock+1),patch.object(stablecoin_supply,'now_ms',lambda:session.receipt),patch.object(ingestion.DeribitCollector,'run',idle),patch.object(ingestion,'poll_chains',idle),patch.object(stablecoin_supply.StablecoinSupplyCollector,'run',produce):
            report=await ingestion.capture(config,OUT/phase,0.1,resume_stablecoin_from=src)
        rows=read_rows(OUT/phase/'events.duckdb',OUT/phase/'events.parquet')
        assert sorted(rows,key=lambda r:r.event_id)==sorted([*pair,*emitted],key=lambda r:r.event_id)
        assert report['stablecoin_resume']['anchor_event_id']==pair[0].event_id
        assert not report['stablecoin_resume']['complete_history_copied'] and not report['trading_enabled'] and not report['ready_for_signals']
        manifest=_verify_archive(OUT/phase/'events.parquet');checks=0
        with duckdb.connect(str(OUT/phase/'events.duckdb'),read_only=True) as c:
            assert c.execute('SELECT event_id,delivered FROM delivery_outbox ORDER BY event_id').fetchall()==[(r.event_id,False) for r in sorted(rows,key=lambda r:r.event_id)]
            for row in rows:
                cutoff=max(row.event_time_ms,row.observed_at_ms)
                for at,visible in [(cutoff-1,False),(cutoff,True)]:
                    ids={r[0] for r in c.execute('SELECT event_id FROM events WHERE event_time_ms<=? AND observed_at_ms<=?',[at,at]).fetchall()}
                    assert (row.event_id in ids)==visible;checks+=1
        if phase=='repeat':
            assert all(row in rows for row in original) and len(emitted)==2 and len(rows)==4
            assert report['stablecoin_supply_quality']['state']=='POLL_OBSERVED_VERIFIED'
        else:
            assert len(emitted)==1 and len(rows)==3 and report['stablecoin_supply_quality']['state']=='BLOCKED'
        attempts.append(session.attempts)
        phases[phase]={'rows':len(rows),'copied_pair':[r.model_dump(mode='json') for r in pair],'synthetic_emitted':[r.model_dump(mode='json') for r in emitted],'quality':report['stablecoin_supply_quality'],'resume':report['stablecoin_resume'],'sql_pit_checks':checks,'archive_sha256':manifest.sha256,'pending_delivery':len(rows),'capture_report_sha256':digest(OUT/phase/'report.json')}
    started_before=len(selected);at=saved.health.observed_at_ms+30000
    with patch.object(ingestion,'now_ms',lambda:at),patch.object(ingestion.DeribitCollector,'run',idle),patch.object(ingestion,'poll_chains',idle):
        try:await ingestion.capture(config,OUT/'refused',0.1,resume_stablecoin_from=OUT/'failure/events.duckdb')
        except IntegrityError as e:assert str(e)=='STABLECOIN_FEED_UNAVAILABLE'
        else:raise AssertionError('latest failure was hidden')
    assert len(selected)==started_before and not (OUT/'refused').exists()
    assert before==hashes(source) and attempts==[7,1]
    codepaths=['src/selective_options/stablecoin_supply.py','src/selective_options/stablecoin_checkpoint.py','src/selective_options/ingestion.py','src/selective_options/cli.py','scripts/verify_stablecoin_restart.py']
    result={'version':'0.3.41','status':'OFFLINE_SAVED_PUBLIC_CAPTURE_RESUME_VERIFIED','provenance':'Both original public records retain exact native identities/receipts. Later HTTP receipts, repeat poll and one RPC failure are synthetic fixtures. Capture code runs with every public producer replaced by idle or a bounded saved-entity replay. No new network, actual outage/recovery, current source availability or complete history claimed.','source_hashes_unchanged':before,'current_code_hashes':{name:digest(ROOT/name) for name in codepaths},'real_public_requests':0,'mock_http_attempts':8,'phases':phases,'closed_archive_reload_verified':True,'exact_pending_outbox_verified':True,'failure_refused_before_new_output':True,'original_public_rows_preserved':2,'total_sql_pit_checks':sum(v['sql_pit_checks'] for v in phases.values()),'historical_clock_fixture':True,'complete_history_copied':False,'actual_transport_recovery_verified':False,'milestone_2_complete':False,'trading_enabled':False,'orders_submitted':0}
    (OUT/'report.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'status':result['status'],'rows':[v['rows'] for v in phases.values()],'pit_checks':result['total_sql_pit_checks'],'new_network':0}))
asyncio.run(main())
