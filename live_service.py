"""Supervise the course runner; recover deterministic source offsets after I/O faults."""
import asyncio
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from itertools import count
from pathlib import Path
from streaming_lab.engine import StreamEngine
from streaming_lab.events import Delivery, iso_time
from streaming_lab.live import run_live, _live_event
from streaming_lab.microbatch import run_microbatch

def is_storage_error(exc):
    if isinstance(exc,BaseExceptionGroup): return all(is_storage_error(e) for e in exc.exceptions)
    return isinstance(exc,sqlite3.OperationalError)

async def supervise(path,mode,state,on_snapshot,*,repeat=True,retry_wait_s=.5,max_retries=3,runner=None):
    failures=0
    runner=runner or (run_live if mode=='live' else run_microbatch)
    while True:
        try:
            with StreamEngine(path) as engine:
                state.update(status='running',checkpoint=engine.checkpoint)
                def update(snapshot):
                    state.update(status='running',checkpoint=snapshot['checkpoint'],
                                 last_commit_at=datetime.now(timezone.utc).isoformat())
                    on_snapshot(snapshot)
                source=(Delivery(i,_live_event(i)) for i in count(engine.next_offset))
                await runner(engine,deliveries=source,duration_s=3600,interval_s=.25,
                             queue_maxsize=8,on_snapshot=update)
            failures=0
            if not repeat: return
        except Exception as exc:
            failures+=1
            state.update(status='recovering',last_error=repr(exc))
            # Recreate the deterministic source from the durable checkpoint;
            # no admitted but uncommitted record is silently skipped.
            if not is_storage_error(exc) or failures>max_retries:
                state.update(status='stopped')
                return
            state['restarts']=state.get('restarts',0)+1
            await asyncio.sleep(retry_wait_s*2**(failures-1))

def read_status(path):
    """Monitor through a read-only transaction; never initialize or write the DB."""
    with closing(sqlite3.connect(f'file:{Path(path).as_posix()}?mode=ro',uri=True,timeout=10)) as c:
        c.row_factory=sqlite3.Row
        c.execute('BEGIN')
        meta=dict(c.execute('select * from stream_meta').fetchone())
        stats={r['name']:r['value'] for r in c.execute('select * from counters')}
        config=json.loads(meta['config_json'])
        width=config['window_seconds']*1000
        def rows(table):
            result=[dict(r) for r in c.execute('select * from '+table+' order by window_start_ms desc limit 8')]
            for row in result:
                row['window_start']=iso_time(row['window_start_ms'])
                row['window_end']=iso_time(row['window_start_ms']+width)
            return result
        totals=dict(c.execute('''select
            (select coalesce(sum(orders),0) from order_state)+(select coalesce(sum(orders),0) from realtime_revenue) orders,
            (select coalesce(sum(revenue_cents),0) from order_state)+(select coalesce(sum(revenue_cents),0) from realtime_revenue) revenue_cents''').fetchone())
        result=dict(checkpoint=meta['checkpoint'],watermark=iso_time(meta['watermark_ms']),
                    stats=stats,totals=totals,open_windows=rows('order_state'),alerts=rows('review_alerts'))
        c.commit()
        return result
