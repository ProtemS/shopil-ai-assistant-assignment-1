"""Real local or hosted model calls. No mocked decisions in this script."""
import json
from datetime import timedelta
from datetime import datetime,timezone
from chat import Chat,Model
from evidence_store import ROOT
from streaming_lab import StreamEngine,Event
from streaming_lab.events import T0

def main():
    # Preserve the shipped verification evidence when the experiment is rerun.
    stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    for name in ('real_model_audit.jsonl','real_model_results.json'):
        p=ROOT/'evidence'/name
        if p.exists(): p.rename(p.with_name(p.stem+'.previous-'+stamp+p.suffix))
    model=Model()
    chat=Chat(model,ROOT/'evidence/real_model_audit.jsonl')
    history=[]
    path=ROOT/'chat_demo.sqlite'
    results=[]
    def incoming():
        with StreamEngine(path) as e:
            i=e.next_offset
            e.ingest(Event('during-answer-'+str(i),'order',T0+timedelta(seconds=25),5,amount_cents=300),offset=i)
    questions=[
        ('כמה הזמנות תקינות ששולמו יש בנתוני העבר ומה ההכנסה שלהן בשקלים?',
         'יש 999 הזמנות ששולמו והכנסתן 999 שקלים.',incoming),
        ('ומה טווח התאריכים של נתוני העבר האלה?',None,None),
        ('מה הרווח הנקי של החנות אחרי הוצאות?',None,None)]
    for question,wrong,callback in questions:
        text,log=chat.answer(question,history,stream_path=path,test_draft=wrong,after_capture=callback)
        results.append({'question':question,'answer':text,'status':log['status'],
                        'snapshot_id':log['snapshot_id'],'attempts':len(log['attempts'])})
        print(json.dumps(results[-1],ensure_ascii=False),flush=True)
        if log['status']=='approved':
            history += [{'role':'user','content':question},{'role':'assistant','content':text}]
    (ROOT/'evidence/real_model_results.json').write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding='utf-8')

if __name__=='__main__': main()
