"""Observe a running producer through read-only snapshots; no synthetic approval."""
import argparse
import json
import time
from datetime import datetime,timezone
from pathlib import Path
from live_service import read_status
from evidence_store import ROOT

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--stream-db',default='live.sqlite')
    ap.add_argument('--wait',type=float,default=2)
    args=ap.parse_args()
    if not 0<args.wait<=30: ap.error('--wait must be between 0 and 30 seconds')
    path=Path(args.stream_db)
    if not path.is_absolute(): path=ROOT/path
    before=read_status(path)
    time.sleep(args.wait)
    after=read_status(path)
    assert after['checkpoint']>before['checkpoint'],'Producer is not progressing; inspect runtime status.'
    report={'observed_at':datetime.now(timezone.utc).isoformat(),'database':path.name,
        'wait_s':args.wait,'checkpoint_before':before['checkpoint'],'checkpoint_after':after['checkpoint'],
        'accepted_before':before['stats']['accepted'],'accepted_after':after['stats']['accepted'],
        'pass':True,'scope':'Actual running isolated preview; no injected events or model decisions.',
        'storage_note':'An earlier instance stopped on disk I/O error. Underlying cause not established. Supervisor retry and visible-stop paths are separately tested with injected faults.'}
    (ROOT/'evidence/runtime_health.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print('Runtime progress PASS',report['checkpoint_before'],'->',report['checkpoint_after'])

if __name__=='__main__': main()
