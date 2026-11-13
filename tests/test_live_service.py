import asyncio
import sqlite3
from live_service import supervise,read_status
from streaming_lab import StreamEngine,Event
from streaming_lab.events import T0

def test_storage_failure_reopens_from_durable_checkpoint(tmp_path):
    calls=[]
    snapshots=[]
    async def runner(e,**kwargs):
        calls.append(e.next_offset)
        i=e.next_offset
        e.ingest(Event('persistent-'+str(i),'order',T0,5,amount_cents=100),offset=i)
        if len(calls)==1:
            raise ExceptionGroup('simulated storage failure',[sqlite3.OperationalError('disk I/O error')])
        kwargs['on_snapshot']({**e.snapshot(),'queue':{'produced':1,'consumed':1}})
    state={'restarts':0}
    asyncio.run(supervise(tmp_path/'live.sqlite','live',state,snapshots.append,
                          repeat=False,retry_wait_s=0,runner=runner))
    assert calls==[0,1] and state['restarts']==1 and state['status']=='running'
    final=read_status(tmp_path/'live.sqlite')
    assert final['checkpoint']==1 and final['totals']['revenue_cents']==200

def test_persistent_failure_is_visible_and_bounded(tmp_path):
    async def broken(e,**kwargs): raise sqlite3.OperationalError('disk I/O error')
    state={'restarts':0}
    asyncio.run(supervise(tmp_path/'live.sqlite','live',state,lambda s:None,
                          repeat=False,retry_wait_s=0,max_retries=2,runner=broken))
    assert state['status']=='stopped' and state['restarts']==2 and 'disk I/O' in state['last_error']

def test_monitor_is_read_only(tmp_path):
    path=tmp_path/'live.sqlite'
    with StreamEngine(path) as e:
        e.ingest(Event('one','order',T0,5,amount_cents=100),offset=0)
    with sqlite3.connect(path) as c:
        before=c.execute('select * from stream_meta').fetchall()
    assert read_status(path)['totals']['orders']==1
    with sqlite3.connect(path) as c:
        assert c.execute('select * from stream_meta').fetchall()==before
