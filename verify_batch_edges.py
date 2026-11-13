"""Check normalized tag links and the actual compiled mart's empty-product case."""
import json
import shutil
import tempfile
from pathlib import Path
import duckdb
from evidence_store import ROOT

def main():
    with duckdb.connect(str(ROOT/'warehouse.duckdb'),read_only=True) as c:
        orphan_tags=c.execute('''select count(*) from raw.reviews__tags t
            left join raw.reviews r on t._dlt_parent_id=r._dlt_id
            where r._dlt_id is null''').fetchone()[0]
        tags=c.execute('select count(*) from raw.reviews__tags').fetchone()[0]
    reviews=json.loads((ROOT/'data/raw/reviews.json').read_text(encoding='utf-8'))
    assert orphan_tags==0 and tags==sum(len(r.get('tags',[])) for r in reviews)
    compiled=ROOT/'dbt_project/target/compiled/shop_il/models/marts/mart_product_reviews.sql'
    sql=compiled.read_text(encoding='utf-8')
    with tempfile.TemporaryDirectory() as tmp:
        copy=Path(tmp)/'warehouse.duckdb'
        shutil.copyfile(ROOT/'warehouse.duckdb',copy)
        with duckdb.connect(str(copy)) as c:
            c.execute("""insert into raw.products select * replace(
                '999' as product_id, 'edge-case product' as product_name,
                'test' as category, '1.00' as list_price, 'edge-product-999' as _dlt_id)
                from raw.products limit 1""")
            r=c.execute(sql)
            rows=[dict(zip([d[0] for d in r.description],v)) for v in r.fetchall()]
            empty=[v for v in rows if v['product_id']==999][0]
            assert len(rows)==9 and empty['review_count']==0 and empty['avg_rating'] is None
            assert empty['negative_reviews']==0 and empty['paid_orders']==0 and empty['revenue']==0
    report={'tag_count':tags,'source_tag_count':sum(len(r.get('tags',[])) for r in reviews),
            'orphan_tags':orphan_tags,'isolated_input_change':'Added product 999 only to a temporary copy of the warehouse; production unchanged.',
            'expected':'9 products; product999 review_count0, avg_rating null, paid_orders0, revenue0',
            'observed_empty_product':empty,'pass':True}
    (ROOT/'evidence/batch_edge_cases.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    print('Tag normalization and product-without-reviews checks PASS')

if __name__=='__main__': main()
