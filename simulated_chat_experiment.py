"""Explicitly SIMULATED model outputs to exercise failure and isolation paths."""
import json
import tempfile
from pathlib import Path
from datetime import timedelta
from chat import Chat
from evidence_store import ROOT,capture
from streaming_lab import StreamEngine,Event
from streaming_lab.events import T0

class SimulatedModel:
    def __init__(self,decisions): self.decisions=iter(decisions); self.packages=[]
    def call(self,system,payload,structured=False):
        self.packages.append(payload['evidence'])
        return next(self.decisions) if structured else 'טיוטה מדומה לבדיקת בקרת הזרימה בלבד'

def main():
    logs=[]
    with tempfile.TemporaryDirectory() as tmp:
        p=Path(tmp)/'stream.sqlite'
        with StreamEngine(p) as e:
            e.ingest(Event('first','order',T0,5,amount_cents=100),offset=0)
        reject=json.dumps({'is_acceptable':False,'feedback':'SIMULATED rejection for protocol testing'})
        m=SimulatedModel([reject]*3)
        _,log=Chat(m,ROOT/'evidence/simulated_chat_audit.jsonl').answer('בדיקת שלוש דחיות מדומה',stream_path=p)
        assert log['status']=='rejected_limit' and len(log['attempts'])==3
        logs.append({'test':'SIMULATED three rejections','status':log['status'],'attempts':3})
        def incoming():
            with StreamEngine(p) as e:
                e.ingest(Event('during','order',T0+timedelta(seconds=1),5,amount_cents=200),offset=1)
        m=SimulatedModel([reject,json.dumps({'is_acceptable':True,'feedback':'SIMULATED approval'})])
        _,log=Chat(m,ROOT/'evidence/simulated_chat_audit.jsonl').answer('בדיקת צילום מצב מדומה',stream_path=p,after_capture=incoming)
        checkpoints=[x['stream']['stream_meta'][0]['checkpoint'] for x in m.packages]
        new=json.loads(capture('שאלה חדשה',p))
        assert set(checkpoints)=={0} and new['stream']['stream_meta'][0]['checkpoint']==1
        logs.append({'test':'SIMULATED model / REAL concurrent ingestion',
                     'old_checkpoints':checkpoints,'new_checkpoint':1,'old_revenue_cents':100,
                     'new_open_window':new['stream']['order_state']})
    (ROOT/'evidence/simulated_chat_results.json').write_text(json.dumps(logs,ensure_ascii=False,indent=2),encoding='utf-8')

if __name__=='__main__': main()
