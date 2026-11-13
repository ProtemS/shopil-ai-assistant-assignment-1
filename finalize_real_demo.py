"""Continue the verified real-model demo after presentation normalization.

The successful paid-order correction is preserved verbatim from its actual run.
Date and missing-profit answers are newly generated and evaluated by real models.
"""
import json
from chat import Chat,Model
from evidence_store import ROOT

def main():
    evidence=ROOT/'evidence'
    original=[json.loads(line) for line in (evidence/'before_normalization_audit.jsonl').read_text(encoding='utf-8').splitlines()]
    first=original[0]
    assert first['status']=='approved' and len(first['attempts'])==2
    audit=evidence/'real_model_audit.jsonl'
    audit.write_text(json.dumps(first,ensure_ascii=False)+'\n',encoding='utf-8')
    p=first['evidence']
    answer=first['attempts'][-1]['draft']+f"\n\nצילום מצב {p['snapshot_id']} · {p['captured_at']} · checkpoint {p['stream']['stream_meta'][0]['checkpoint']}"
    results=[{'question':p['question'],'answer':answer,'status':'approved','snapshot_id':p['snapshot_id'],'attempts':2}]
    history=[{'role':'user','content':p['question']},{'role':'assistant','content':answer}]
    chat=Chat(Model(),audit)
    for question in ('ומה טווח התאריכים של נתוני העבר האלה?','מה הרווח הנקי של החנות אחרי הוצאות?'):
        text,log=chat.answer(question,history,stream_path=ROOT/'chat_demo.sqlite')
        results.append({'question':question,'answer':text,'status':log['status'],
                        'snapshot_id':log['snapshot_id'],'attempts':len(log['attempts'])})
        print(json.dumps(results[-1],ensure_ascii=False),flush=True)
        if log['status']=='approved': history += [{'role':'user','content':question},{'role':'assistant','content':text}]
        (evidence/'real_model_results.json').write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding='utf-8')

if __name__=='__main__': main()
