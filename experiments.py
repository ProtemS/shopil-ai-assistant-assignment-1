"""Own experiments use course inputs and engine, never sorted by event time."""
import asyncio
import json
import tempfile
from datetime import timedelta
from pathlib import Path
from streaming_lab import StreamEngine, teaching_fixture, batch_truth, run_live, run_microbatch, Event
from streaming_lab.events import T0
from streaming_lab.reporting import canonical_results
from evidence_store import ROOT

def write(name,value):
    (ROOT/'evidence'/name).write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str),encoding='utf-8')

async def main():
    data=teaching_fixture()
    write('stream_input.json',[d.to_dict() for d in data])
    with tempfile.TemporaryDirectory() as tmp:
        tmp=Path(tmp)
        runs={}
        for mode,runner in [('live',run_live),('microbatch',run_microbatch)]:
            with StreamEngine(tmp/(mode+'.sqlite')) as e:
                trace=[]
                args=dict(deliveries=data,duration_s=None,interval_s=.001,queue_maxsize=3,
                          on_snapshot=lambda s: trace.append(s),finalize=True)
                if mode=='microbatch': args.update(max_batch_size=5,max_wait_s=.05)
                runs[mode]=await runner(e,**args)
                assert runs[mode]['produced']==runs[mode]['consumed']==17
                write(mode+'_trace.json',trace)
        assert canonical_results(runs['live']['snapshot'])==canonical_results(runs['microbatch']['snapshot'])
        assert sum(runs['microbatch']['batch_sizes'])==17
        write('stream_modes.json',runs)
        policies={}
        for grace in (0,10,120):
            with StreamEngine(tmp/f'grace{grace}.sqlite',grace_seconds=grace) as e:
                decisions=[e.ingest(d.event,offset=d.offset) for d in data]
                policies[grace]={'decisions':decisions,'snapshot':e.finalize_demo()}
        s=policies[10]['snapshot']
        truth=batch_truth(data)
        assert s['totals']['revenue_cents']==12200
        assert s['totals']['late_order_cents']==1300
        assert truth['revenue_cents']==13500
        assert len(s['alerts'])==1 and s['alerts'][0]['negative_reviews']==3
        assert s['review_windows'][0]['negative_reviews']==4
        assert s['revenue_windows'][0]['revenue_cents']==2500
        # Half-open boundary: exactly 60 seconds belongs to second window;
        # its arrival after watermark=135 exceeds end=120 plus grace=10.
        boundary=[r for r in s['late_events'] if r['event_id']=='order-boundary'][0]
        assert boundary['amount_cents']==800
        write('grace_policies.json',policies)
        write('reconciliation.json',{'batch_truth':truth,'accepted_cents':12200,'late_cents':1300,
              'sum_cents':13500,'pass':True,'manual_first_window_cents':'1000 + 1500 = 2500',
              'boundary':boundary})
        flush={}
        for name,size,wait,interval,n in [('max_size',2,.2,.001,4),('max_wait',10,.02,.08,2),('final_drain',10,1,.001,3)]:
            with StreamEngine(tmp/(name+'.sqlite')) as e:
                r=await run_microbatch(e,data[:n],duration_s=None,interval_s=interval,
                                      max_batch_size=size,max_wait_s=wait)
                assert r['flush_counts'][name]>0
                assert sum(r['batch_sizes'])==r['consumed']==r['produced']==n
                flush[name]=r
        write('batch_flush.json',flush)
        path=tmp/'replay.sqlite'
        with StreamEngine(path) as e:
            first=e.ingest(data[0].event,offset=0)
            duplicate=e.ingest(data[0].event,offset=1)
            before=canonical_results(e.snapshot())
        with StreamEngine(path) as e:
            replay=e.ingest(data[0].event,offset=1)
            assert canonical_results(e.snapshot())==before
            assert duplicate['status']=='duplicate' and replay['status']=='replayed'
            write('dedupe_replay.json',{'first':first,'duplicate':duplicate,'replay':replay,'unchanged_after_reopen':True,'state':before})
    # Dedicated nonfinalized DB for chat experiments; a live app uses live.sqlite.
    path=ROOT/'chat_demo.sqlite'
    if path.exists(): raise RuntimeError('Move existing chat_demo.sqlite before rerunning experiments')
    with StreamEngine(path) as e:
        for d in data[:8]: e.ingest(d.event,offset=d.offset)
    print('Streaming mode, policy, flush, alert, reconciliation and replay experiments PASS')

if __name__=='__main__': asyncio.run(main())
