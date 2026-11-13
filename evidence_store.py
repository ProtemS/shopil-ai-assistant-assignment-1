"""Allowlisted SQL; immutable batch database plus one SQLite read transaction."""
import hashlib
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parent
STREAM_TABLES = ('stream_meta','counters','order_state','review_state',
                 'realtime_revenue','review_windows','review_alerts','late_events','dead_letters')

def capture(question, stream_path=None, product_id=None):
    batch = {}
    with duckdb.connect(str(ROOT/'warehouse.duckdb'), read_only=True) as c:
        def query(sql, args=()):
            result = c.execute(sql,args)
            return [dict(zip([d[0] for d in result.description],r)) for r in result.fetchall()]
        batch['summary'] = query("select count(*) valid_orders, count(*) filter(where status='paid') paid_orders, sum(amount) filter(where status='paid') revenue_ils, min(order_date) period_start, max(order_date) period_end from marts.fct_orders")
        wants_products=product_id is not None or any(w in question.lower() for w in ('מוצר','דירוג','ביקור','product','rating','review'))
        batch['products'] = query('select * from marts.mart_product_reviews '+('where product_id=? ' if product_id else '')+'order by product_id',(product_id,) if product_id else ()) if wants_products else []
        batch['reviews'] = query('select * from staging.stg_reviews where product_id=? order by created_at,review_id limit 10',(product_id,)) if product_id else []
    path = Path(stream_path or ROOT/'live.sqlite')
    wants_stream=any(w in question.lower() for w in ('זרם','חלון','התרא','אירוע','stream','window','עדכנ','עכשיו','כרגע','חדשות'))
    with closing(sqlite3.connect(f'file:{path.as_posix()}?mode=ro',uri=True,timeout=10)) as c:
        c.row_factory = sqlite3.Row
        c.execute('BEGIN')
        # Keep model context bounded even after hours of continuous ingestion.
        stream = {}
        for t in STREAM_TABLES:
            if not wants_stream and t not in ('stream_meta','counters','order_state','review_state'):
                stream[t]=[]
                continue
            suffix=''
            if t not in ('stream_meta','counters'):
                order='source_offset' if t in ('late_events','dead_letters') else 'window_start_ms'
                suffix=' order by '+order+' desc limit 8'
            stream[t]=[dict(r) for r in c.execute('select * from '+t+suffix)]
        stream['totals'] = dict(c.execute('''select
            (select coalesce(sum(revenue_cents),0) from order_state)+(select coalesce(sum(revenue_cents),0) from realtime_revenue) accepted_revenue_cents,
            (select coalesce(sum(orders),0) from order_state)+(select coalesce(sum(orders),0) from realtime_revenue) accepted_orders,
            (select count(*) from accepted_reviews) accepted_reviews,
            (select coalesce(sum(amount_cents),0) from late_events where kind='order') late_order_cents''').fetchone())
        stream['window_counts']=dict(c.execute('''select
            (select count(*) from order_state) open_order_windows,
            (select count(*) from review_state) open_product_review_windows,
            (select count(*) from realtime_revenue) final_order_windows''').fetchone())
        stream['accepted_reviews'] = [dict(r) for r in c.execute('select * from accepted_reviews where product_id=? order by event_time_ms,event_id limit 10',(product_id,))] if product_id else []
        c.commit()
    package = dict(question=question, captured_at=datetime.now(timezone.utc).isoformat(),
        product_id=product_id, batch=batch, stream=stream,
        stream_row_details_requested=wants_stream,
        retrieval_scope='Open window rows are included. Final-window and side-output rows are fetched only for stream questions, latest 8 per table; an unrequested empty list means NOT FETCHED. Totals, window counts and counters cover all saved events. Products and review texts are fetched only for product questions or a selected product.',
        sources={'batch':['marts.fct_orders','marts.mart_product_reviews','staging.stg_reviews'],
                 'stream':list(STREAM_TABLES)},
        units={'batch_revenue':'ILS; paid orders only; amount includes quantity',
               'stream_revenue':'integer agorot; no payment status'},
        limitations=['Do not add batch and stream revenues: overlap and semantic equality unproven.',
                      'Stream product IDs use the course catalog; unknown IDs are not joined.',
                      'Missing review average is null, not zero.',
                      'Open window counts are provisional; realtime_revenue contains final windows.',
                      'Net profit, expenses, inventory and customer-level demographics are unavailable.'])
    text = json.dumps(package,ensure_ascii=False,default=str,sort_keys=True)
    package['snapshot_id'] = hashlib.sha256(text.encode()).hexdigest()[:20]
    return json.dumps(package,ensure_ascii=False,default=str,sort_keys=True)
