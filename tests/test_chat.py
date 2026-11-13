"""Simulated model contract tests, explicitly distinct from real model logs."""
import json
from datetime import timedelta
import pytest
from chat import Chat,grounding_check,normalize_draft
from evidence_store import capture, ROOT
from streaming_lab import StreamEngine, Event
from streaming_lab.events import T0

class MockModel:
    def __init__(self, decisions):
        self.decisions=iter(decisions)
        self.evidence=[]
    def call(self,system,payload,structured=False):
        self.evidence.append(payload['evidence'])
        if structured: return next(self.decisions)
        return 'תשובת בדיקה מדומה'

def setup(tmp_path):
    p=tmp_path/'stream.sqlite'
    with StreamEngine(p) as e:
        e.ingest(Event('initial','order',T0,5,amount_cents=100),offset=0)
    return p

def test_three_rejections(tmp_path):
    p=setup(tmp_path)
    m=MockModel(['{"is_acceptable":false,"feedback":"תקן"}']*3)
    answer,log=Chat(m,tmp_path/'audit.jsonl').answer('בדיקה',stream_path=p)
    assert log['status']=='rejected_limit' and len(log['attempts'])==3
    assert len({json.dumps(e,sort_keys=True) for e in m.evidence})==1

@pytest.mark.parametrize('bad',['{}','{"is_acceptable":"true","feedback":"x"}','not-json'])
def test_bad_schema_stops(tmp_path,bad):
    p=setup(tmp_path)
    m=MockModel([bad])
    _,log=Chat(m,tmp_path/'audit.jsonl').answer('בדיקה',stream_path=p)
    assert log['status']=='error' and len(log['attempts'])==1

def test_snapshot_unchanged_during_ingestion(tmp_path):
    p=setup(tmp_path)
    m=MockModel(['{"is_acceptable":false,"feedback":"תקן"}', '{"is_acceptable":true,"feedback":"תקין"}'])
    def arriving():
        with StreamEngine(p) as e:
            e.ingest(Event('concurrent','order',T0+timedelta(seconds=5),5,amount_cents=200),offset=1)
    _,log=Chat(m,tmp_path/'audit.jsonl').answer('בדיקה',stream_path=p,after_capture=arriving)
    assert log['status']=='approved'
    assert all(x['stream']['stream_meta'][0]['checkpoint']==0 for x in m.evidence)
    assert json.loads(capture('שאלה חדשה',p))['stream']['stream_meta'][0]['checkpoint']==1

def test_call_error_stops(tmp_path):
    p=setup(tmp_path)
    class Broken:
        def call(self,*args): raise RuntimeError('simulated connection failure')
    _,log=Chat(Broken(),tmp_path/'audit.jsonl').answer('בדיקה',stream_path=p)
    assert log['status']=='error'

def test_numeric_guard_rejects_incorrect_model_approval(tmp_path):
    p=setup(tmp_path)
    package=json.loads(capture('כמה הזמנות ששולמו ומה ההכנסה?',p))
    assert grounding_check(package['question'],'יש 549 הזמנות ששולמו והכנסתן 97210.10 שקלים.',package)
    assert grounding_check(package['question'],'יש 402 הזמנות ששולמו והכנסתן 97210.10 שקלים.',package) is None
    assert grounding_check('מה הרווח הנקי?','הרווח הנקי הוא 97210.10 שקלים.',package)
    assert grounding_check('מה הרווח הנקי?','לא ניתן לחשב רווח נקי כי חסרים נתוני הוצאות.',package) is None
    assert grounding_check('מה הרווח הנקי?','נתוני הוצאות חסרים. Expenses and costs are unavailable so profit cannot be calculated.',package)
    assert grounding_check('מה טווח התאריכים?','התקופה היא 01-01-2026 עד 27-09-2026.',package) is None
    assert normalize_draft('😀 התקופה היא 2026-01-01 עד 2026-09-27. Orig: {"batch":{}}')=='התקופה היא 2026-01-01 עד 2026-09-27.'
