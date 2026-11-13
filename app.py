"""Run from the project root. Streaming runs throughout the Gradio session."""
import asyncio
import json
import threading
from pathlib import Path
import gradio as gr
from chat import Chat, Model
from evidence_store import ROOT
from streaming_lab.engine import StreamEngine
from live_service import supervise, read_status

LATEST={}
SERVICE={'status':'starting','restarts':0}
CSS='''
.gradio-container {max-width:1120px!important; margin:auto!important; font-family:Arial,sans-serif!important}
.hero {direction:rtl;padding:24px 28px;background:#122d42;color:white;border-radius:16px;margin-bottom:18px}
.hero h1 {font-size:30px;margin:0 0 8px;color:#fff!important}.hero p{margin:0;color:#d8e7ed}
.rtl {direction:rtl;text-align:right}.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;direction:rtl;margin:10px 0}
.metric{background:#f0f6f6;border:1px solid #d8e5e5;border-radius:12px;padding:16px;color:#173b46}
.metric b{display:block;font-size:25px;margin-top:6px}.metric span{font-size:13px}
.meta{direction:rtl;color:#526572;font-size:13px;margin:12px 0}
@media(max-width:640px){.metrics{grid-template-columns:repeat(2,1fr)}}
'''

def producer(mode,stream_path):
    def update(s):
        LATEST.update(s['queue'])
        print(json.dumps({'queue':s['queue'],'totals':s['totals']},ensure_ascii=False),flush=True)
    asyncio.run(supervise(stream_path,mode,SERVICE,update))

def main():
    import argparse
    ap=argparse.ArgumentParser()
    ap.add_argument('--mode',choices=['live','microbatch'],default='live')
    ap.add_argument('--port',type=int,default=7860)
    ap.add_argument('--stream-db',default='live.sqlite')
    args=ap.parse_args()
    stream_path=Path(args.stream_db)
    if not stream_path.is_absolute(): stream_path=ROOT/stream_path
    if not (ROOT/'warehouse.duckdb').exists():
        ap.error('Run python prepare.py before starting the chat.')
    with StreamEngine(stream_path): pass
    chat=Chat(Model())
    threading.Thread(target=producer,args=(args.mode,stream_path),daemon=True).start()
    with gr.Blocks(title='ShopIL') as demo:
        gr.HTML('<div class="hero"><h1>ShopIL · מבינים את מצב החנות</h1><p>נתוני עבר, הזמנות וביקורות שממשיכות להגיע — תשובות בעברית עם מקור ומועד נתונים.</p></div>')
        gr.Markdown('### מצב הקליטה',elem_classes='rtl')
        live_summary=gr.HTML()
        saved=ROOT/'evidence/real_model_results.json'
        if saved.exists():
            approved=[r for r in json.loads(saved.read_text(encoding='utf-8')) if r['status']=='approved']
            if approved:
                with gr.Accordion('תשובה מאושרת מהניסוי השמור',open=True):
                    gr.Markdown('**'+approved[0]['question']+'**\n\n'+approved[0]['answer'],elem_classes='rtl')
        conversation=gr.Chatbot(label='השיחה עם עוזר החנות',height=280,elem_classes='rtl')
        history=gr.State([])
        with gr.Row():
            question=gr.Textbox(label='השאלה שלך',placeholder='למשל: כמה הכנסות התקבלו מהזמנות ששולמו?',scale=4,elem_classes='rtl')
            product=gr.Dropdown(choices=list(range(1,9)),value=None,label='מוצר לביקורות (רשות)',scale=1)
        send=gr.Button('שליחה לבדיקה ולתשובה',variant='primary')
        gr.Markdown('הכנסות העבר מתייחסות להזמנות ששולמו בשקלים. אירועי הזרם נשמרים בנפרד באגורות, ללא מידע על מצב תשלום. יצירת תשובה מקומית עשויה לקחת זמן.',elem_classes='rtl')
        with gr.Accordion('פרטי חלונות, התראות וקליטה',open=False):
            status=gr.JSON(label='צילום מצב של הזרם')
        def respond(q,h,p,current):
            if not q or not q.strip(): return current,h,q
            text,log=chat.answer(q,h,product_id=p,stream_path=stream_path)
            # Only approved answers enter future model conversation context.
            pair=[{'role':'user','content':q},{'role':'assistant','content':text}]
            display=(current or [])+pair
            return display, (h+pair)[-8:] if log['status']=='approved' else h,''
        send.click(respond,[question,history,product,conversation],[conversation,history,question])
        def monitor():
            try:
                s=read_status(stream_path)
            except Exception as exc:
                return '<div class="meta" style="color:#a21d28">לא ניתן לקרוא את מצב הקליטה. יש לבדוק את שגיאת האחסון.</div>',{'read_error':repr(exc),'service':dict(SERVICE)}
            q=dict(LATEST)
            items=[('אירועים שנוצרו בהרצה',q.get('produced',0)),('אירועים שעובדו בהרצה',q.get('consumed',0)),('הזמנות שנקלטו במצטבר',s['totals']['orders']),('התראות שנוצרו במצטבר',s['stats']['alerts_emitted'])]
            cards='<div class="metrics">'+''.join(f'<div class="metric"><span>{label}</span><b>{value:,}</b></div>' for label,value in items)+'</div>'
            service=dict(SERVICE)
            labels={'starting':'מתחיל','running':'פעיל','recovering':'מתאושש משגיאת אחסון','stopped':'נעצר עקב שגיאה'}
            color='#18704e' if service['status']=='running' else '#a21d28'
            cards+=f'<div class="meta" style="color:{color}"><b>מצב: {labels.get(service["status"],service["status"])} · הפעלות התאוששות: {service["restarts"]}</b></div>'
            cards+=f'<div class="meta">מסלול: {args.mode} · checkpoint: {s["checkpoint"]} · Watermark: {s["watermark"]}<br>התור מוגבל ל־8 רשומות. המונים העליונים מתייחסים להרצת היצרן הנוכחית; המצב המצטבר נשמר גם לאחר הפעלה מחדש.</div>'
            detail={'queue_live':q,**{k:s[k] for k in ('checkpoint','watermark','stats','totals','open_windows')}}
            detail['service']=service
            detail['latest_alerts']=s['alerts'][-8:]
            return cards,detail
        gr.Timer(1).tick(monitor,outputs=[live_summary,status])
        demo.load(monitor,outputs=[live_summary,status])
    demo.launch(server_name='127.0.0.1',server_port=args.port,css=CSS)

if __name__=='__main__': main()
