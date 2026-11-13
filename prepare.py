"""Reproducible batch preparation, rerun proof and query-based report."""
import json
import os
from pathlib import Path
import subprocess
import sys
import duckdb

ROOT=Path(__file__).resolve().parent
OUT=ROOT/'evidence'

def run(args,name,cwd=ROOT):
    env={**os.environ,'PYTHONIOENCODING':'utf-8','PYTHONUTF8':'1','DLT_TELEMETRY':'false','DO_NOT_TRACK':'1'}
    p=subprocess.run(args,cwd=cwd,env=env,capture_output=True,text=True,encoding='utf-8',errors='replace')
    (OUT/name).write_text(p.stdout+'\n'+p.stderr,encoding='utf-8')
    print(name,p.returncode,flush=True)
    if p.returncode: raise RuntimeError(f'See evidence/{name}')

def main():
    OUT.mkdir(exist_ok=True)
    counts=[]
    for i in (1,2):
        run([sys.executable,'pipelines/02_dlt_pipeline.py','--offline'],f'load_base_{i}.txt')
        run([sys.executable,'pipelines/04_reviews_pipeline.py'],f'load_reviews_{i}.txt')
        with duckdb.connect(str(ROOT/'warehouse.duckdb'),read_only=True) as c:
            counts.append({t:c.execute('select count(*) from raw.'+t).fetchone()[0] for t in ('customers','orders','products','reviews','reviews__tags')})
    assert counts[0]==counts[1],counts
    dbt=str(Path(sys.executable).parent/'dbt.exe') if os.name=='nt' else str(Path(sys.executable).parent/'dbt')
    run([dbt,'build','--profiles-dir','.'], 'dbt_build.txt',ROOT/'dbt_project')
    run([sys.executable,'tests/course_check.py','1','2','3'],'course_reviews_check.txt')
    with duckdb.connect(str(ROOT/'warehouse.duckdb'),read_only=True) as c:
        def query(sql):
            r=c.execute(sql)
            return [dict(zip([d[0] for d in r.description],row)) for row in r.fetchall()]
        report={'repeat_counts':counts,
          'counts':query("select (select count(*) from raw.reviews) raw, (select count(*) from raw.reviews where rating between 1 and 5) after_rating, (select count(*) from raw.reviews where rating between 1 and 5 and nullif(trim(text),'') is not null) after_text, (select count(*) from staging.stg_reviews) after_dedupe"),
          'orders':query("select count(*) valid_orders, count(*) filter(where status='paid') paid, sum(amount) filter(where status='paid') revenue from marts.fct_orders"),
          'products':query('select * from marts.mart_product_reviews order by product_id'),
          'retained_reviews':query('select * from staging.stg_reviews order by review_id'),
          'period':query('select min(order_date) start_date,max(order_date) end_date from marts.fct_orders')}
        assert list(report['counts'][0].values())==[310,304,298,289]
        assert report['orders'][0]['valid_orders']==549 and report['orders'][0]['paid']==402
        assert str(report['orders'][0]['revenue'])=='97210.10'
        assert len(report['products'])==8
    (OUT/'batch_report.json').write_text(json.dumps(report,ensure_ascii=False,default=str,indent=2),encoding='utf-8')
    print('Batch reference values and reload proof PASS')

if __name__=='__main__': main()
