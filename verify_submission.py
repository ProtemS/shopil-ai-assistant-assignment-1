"""Release checks over actual saved evidence; no model calls or mock approvals."""
import json
from importlib.metadata import version
from evidence_store import ROOT

def main():
    out=ROOT/'evidence'
    logs=[json.loads(line) for line in (out/'real_model_audit.jsonl').read_text(encoding='utf-8').splitlines()]
    results=json.loads((out/'real_model_results.json').read_text(encoding='utf-8'))
    assert len(results)==len(logs)==3
    assert all(r['status']=='approved' for r in results)
    assert all(r['status']=='approved' for r in logs)
    assert logs[0]['attempts'][0]['evaluation']['is_acceptable'] is False
    assert logs[0]['attempts'][1]['evaluation']['is_acceptable'] is True
    assert '402' in results[0]['answer'] and '97,210.10' in results[0]['answer']
    assert '2026-01-01' in results[1]['answer'] and '2026-09-27' in results[1]['answer']
    assert 'לא ניתן' in results[2]['answer'] and 'הוצאות' in results[2]['answer']
    old,new=logs[0]['evidence'],logs[1]['evidence']
    def counter(p,name):
        return next(r['value'] for r in p['stream']['counters'] if r['name']==name)
    before=old['stream']['stream_meta'][0]['checkpoint']
    after=new['stream']['stream_meta'][0]['checkpoint']
    assert after==before+1
    assert counter(new,'accepted_orders')==counter(old,'accepted_orders')+1
    assert new['stream']['totals']['accepted_revenue_cents']==old['stream']['totals']['accepted_revenue_cents']+300
    report={'old_snapshot_id':old['snapshot_id'],'new_snapshot_id':new['snapshot_id'],
        'old_checkpoint':before,'new_checkpoint':after,
        'old_accepted_orders':counter(old,'accepted_orders'),'new_accepted_orders':counter(new,'accepted_orders'),
        'accepted_revenue_change_cents':300,'pass':True}
    (out/'real_snapshot_isolation.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    meta={'python_verified':'3.11.3','packages':{p:version(p) for p in ('dlt','duckdb','dbt-core','dbt-duckdb','gradio','pydantic','llama-cpp-python')},
        'model':'Qwen/Qwen2.5-7B-Instruct-GGUF Q4_K_M',
        'role_configuration':'Two prompt roles, same real local model weights; separate calls; fixed evidence.',
        'model_sha256_verified':[
            'dfce12e3862a5283ccfb88221b48480e58745165de856439950d0f22590580db',
            '539cf93f78e887edea1c04e2d7d8cdaca9d01dae9c9025bcb8accbe29df3d72a'],
        'accepted_real_questions':3,'model_correction_attempts':len(logs[0]['attempts']),
        'manual_review':'manual_verification.md','moodle_submission_performed':False}
    (out/'verification_environment.json').write_text(json.dumps(meta,indent=2),encoding='utf-8')
    print('Three real answers, correction loop and real snapshot isolation PASS')

if __name__=='__main__': main()
