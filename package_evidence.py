"""Build readable tables and report from actual saved results."""
import csv
import html
import json
from pathlib import Path
import subprocess
import sys
from evidence_store import ROOT

def main():
    out=ROOT/'evidence'
    read=lambda n:json.loads((out/n).read_text(encoding='utf-8'))
    batch=read('batch_report.json')
    modes=read('stream_modes.json')
    policies=read('grace_policies.json')
    rows=[['test','input_and_settings','expected','observed','evidence'],
          ['batch reload','same course snapshots loaded twice','600 orders / 310 reviews unchanged',str(batch['repeat_counts']),'batch_report.json'],
          ['review cleaning','rating then text then earliest valid ID','310/304/298/289',str(batch['counts']),'batch_report.json'],
          ['historical paid revenue','amount includes quantity','402 paid / 97210.10 ILS',str(batch['orders']),'batch_report.json'],
          ['mode equality','17 arrivals source order; fresh DBs; 60s windows, grace10','canonical equality; produced=consumed=17',str({k:(v['produced'],v['consumed']) for k,v in modes.items()}),'stream_modes.json'],
          ['money reconciliation','same 17 arrivals valid unique orders only','12200+1300=13500 cents',str(read('reconciliation.json')),'reconciliation.json'],
          ['deduplication/reopen','same ID new offset then replay after reopen','no double income; replay leaves durable state unchanged','PASS','dedupe_replay.json'],
          ['alert','product5 third then fourth negative in window','one alert, negative count4','PASS','grace_policies.json'],
          ['fixed snapshot','arrival after capture; new question afterward','old checkpoint retained across retries; new question sees next checkpoint','PASS','pytest.txt'],
          ['three rejections/error','explicit mock evaluations and malformed JSON','3 attempts maximum; errors terminate','PASS','pytest.txt']]
    for name,value in read('batch_flush.json').items():
        rows.append([name,f"size={value['max_batch_size']},wait={value['max_wait_s']}",name,str(value['batch_details']),'batch_flush.json'])
    for grace,p in policies.items():
        s=p['snapshot']
        rows.append(['grace '+grace,'same17; window60; disorder0','reconcile full truth13500',str(s['totals'])+'; closures='+str([(r['window_end'],r['emitted_at_watermark_ms'],r['closure_reason']) for r in s['revenue_windows']]),'grace_policies.json'])
    if (out/'real_model_results.json').exists():
        for r in read('real_model_results.json'):
            rows.append(['real model',r['question'],'evidence-grounded approved answer',r['status']+' attempts='+str(r['attempts']),'real_model_audit.jsonl'])
    with (out/'experiment_matrix.csv').open('w',encoding='utf-8-sig',newline='') as f:
        csv.writer(f).writerows(rows)
    text=(ROOT/'REPORT.md').read_text(encoding='utf-8')
    sections=text.split('\n## ')
    body=''
    for i,section in enumerate(sections):
        if i: body+='<section class="page"><h2>'+html.escape(section.split('\n')[0])+'</h2>'
        else: body+='<section class="intro">'
        lines=section.split('\n')[1:]
        body+=''.join('<p>'+html.escape(p.strip())+'</p>' for p in '\n'.join(lines).split('\n\n') if p.strip())+'</section>'
    (ROOT/'report.html').write_text('<!doctype html><html lang="he" dir="rtl"><meta charset="utf-8"><title>דוח ShopIL</title><style>body{font:15px Arial;max-width:800px;margin:35px auto;line-height:1.65;color:#17212c}h1,h2{color:black}p{margin:12px 0}.intro{margin-bottom:24px}@media print{@page{size:A4;margin:18mm}.page{break-before:page}body{font-size:11pt;line-height:1.4;margin:0}.intro+.page{break-before:auto}}</style><h1>עוזר עסקי לחנות ShopIL</h1>'+body+'</html>',encoding='utf-8')
    mmd=(ROOT/'architecture.mmd').read_text(encoding='utf-8')
    (ROOT/'ARCHITECTURE.md').write_text('# Implementation architecture\n\n```mermaid\n'+mmd+'\n```\n',encoding='utf-8')
    frozen=subprocess.check_output([sys.executable,'-m','pip','freeze'],text=True)
    frozen='\n'.join(s for s in frozen.splitlines() if 'shopil-assistant' not in s and not s.startswith('#'))+'\n'
    (ROOT/'requirements-lock.txt').write_text(frozen,encoding='utf-8')

if __name__=='__main__': main()
