"""REAL RabbitMQ/Kafka/SMTP/PDF checks. Run only on disposable local demo data."""
import json
import os
from pathlib import Path
import subprocess
import time
import uuid
from urllib.error import HTTPError
from urllib.request import Request,urlopen

BASE=os.getenv('SHOP_TEST_URL','http://127.0.0.1:8000')
MAIL=os.getenv('SHOP_TEST_MAIL','http://127.0.0.1:8025')


def call(path,body=None,method=None,status=None):
    req=Request(BASE+'/api/shop'+path,data=json.dumps(body).encode() if body is not None else None,
                headers={'Content-Type':'application/json'},method=method or ('POST' if body is not None else 'GET'))
    with urlopen(req,timeout=60) as response:
        if status:self_assert(response.status==status,'unexpected HTTP status')
        return json.load(response)


def self_assert(condition,message):
    if not condition:raise AssertionError(message)


def wait(predicate,timeout=80):
    deadline=time.monotonic()+timeout;state=None
    while time.monotonic()<deadline:
        state=call('/state')
        if predicate(state):return state
        time.sleep(.4)
    raise AssertionError(json.dumps(state,ensure_ascii=False)[-12000:])


def order(state,oid):return next(o for o in state['orders'] if o['id']==oid)
def live(state,transport,role):return [w for w in state['workers'] if w['transport']==transport and w['role']==role and w['alive']]
def compose(*args):subprocess.run(['docker','compose',*args],check=True,capture_output=True)


def checkout(key=None):
    return call('/checkout',dict(customer=dict(name='Nguyễn Minh Anh',email='minhanh@example.test',address='12 Đường Mộc, TP. Hồ Chí Minh'),
                items=[dict(sku='tee',size='M',quantity=2,unit_price=1)],idempotency_key=key or uuid.uuid4().hex),status=202)


def ensure(transport,role,count):
    state=call('/state')
    for _ in range(max(0,count-len(live(state,transport,role)))):call('/workers',dict(transport=transport,role=role))


def main():
    state=call('/state')
    self_assert(not any(w['alive'] for w in state['workers']),'Use a fresh disposable project, no active shop workers')
    self_assert(state['totals']['orders']==0,'Use a fresh disposable database, do not run on orders you need to keep')
    call('/settings',{'seconds':.15})
    key=uuid.uuid4().hex;first=checkout(key);oid=first['order_id']
    self_assert(first['order']['total']==523000,'client price was trusted')
    self_assert(checkout(key)['order_id']==oid,'duplicate HTTP checkout created another order')
    state=wait(lambda s:s['services']['rabbitmq']['roles'].get('invoice',{}).get('ready')==1)
    self_assert(order(state,oid)['invoice'] is None,'invoice created without a worker')
    print('PASS durable checkout: real HTTP 202, idempotency, server pricing, queue waiting with 0 workers',flush=True)
    ensure('rabbitmq','invoice',1)
    state=wait(lambda s:order(s,oid)['invoice'] is not None)
    self_assert(order(state,oid)['delivery'] is None,'email sent without email consumer')
    with urlopen(BASE+'/api/shop/orders/'+oid+'/invoice.pdf') as r:pdf=r.read()
    self_assert(pdf.startswith(b'%PDF'),'invoice is not a PDF')
    evidence=Path('test-results');evidence.mkdir(exist_ok=True);(evidence/'shop-invoice.pdf').write_bytes(pdf)
    ensure('rabbitmq','email',1)
    # Counters and order detail use separate reads; wait for both to settle.
    state=wait(lambda s:order(s,oid)['status']=='completed' and s['totals']['invoices']==1 and s['totals']['emails']==1)
    with urlopen(MAIL+'/api/v1/messages') as response:messages=json.load(response)
    self_assert(messages['total']>=1,'Mailpit did not capture SMTP')
    number=order(state,oid)['invoice']['number']
    mail=next(m for m in messages['messages'] if number in m['Subject'])
    with urlopen(MAIL+'/api/v1/message/'+mail['ID']) as response:captured=json.load(response)
    self_assert(any(a['ContentType']=='application/pdf' and a['Size']>1000 for a in captured['Attachments']),'SMTP message has no real PDF attachment')
    print('PASS two-stage fulfillment: detailed PDF, email consumer and real SMTP captured by Mailpit',flush=True)
    before=state['totals'].copy();call('/orders/'+oid+'/duplicate',{})
    state=wait(lambda s:any(e['kind']=='DEDUP' and e['order_id']==oid for e in s['events']))
    self_assert(state['totals']['invoices']==before['invoices'] and state['totals']['emails']==before['emails'],'duplicate side effects')
    print('PASS duplicate invoice task: one invoice and one email outbox',flush=True)
    for mode in ('rabbitmq','kafka'):
        ensure(mode,'invoice',4);ensure(mode,'email',1)
    state=wait(lambda s:len(live(s,'kafka','invoice'))==4 and sum(bool(w['assignment']) for w in live(s,'kafka','invoice'))==2 and s['services']['rabbitmq']['roles']['invoice']['consumers']==4,timeout=100)
    self_assert(sum(w['state']=='unassigned' for w in live(state,'kafka','invoice'))==2,'Kafka extra consumers are not idle as expected')
    run=call('/seed',dict(transport='both',scenario='normal',count=6))
    state=wait(lambda s:all(order(s,i)['status']=='completed' for i in run['order_ids']),timeout=100)
    print('PASS same business work: 12 real orders, 12 invoices, 12 emails; Kafka 4 consumers / 2 partition owners',flush=True)
    bad=call('/seed',dict(transport='both',scenario='invalid_email',count=3))
    def fault_ready(s):
        orders=[order(s,i) for i in bad['order_ids']]
        rabbit=[o for o in orders if o['transport']=='rabbitmq'];kafka=[o for o in orders if o['transport']=='kafka']
        return sum(o['delivery'] is not None for o in rabbit)==2 and any(t['status']=='blocked' for o in kafka for t in o['tasks']) and all(o['invoice'] for o in kafka)
    state=wait(fault_ready,100)
    koh=[order(state,i) for i in bad['order_ids'] if order(state,i)['transport']=='kafka']
    self_assert(all(o['delivery'] is None for o in koh),'Kafka same-partition policy did not hold later tasks')
    kt=next(t for o in koh for t in o['tasks'] if t['status']=='blocked')
    call('/tasks/'+kt['id']+'/dead-letter',{})
    state=wait(lambda s:sum(order(s,i)['delivery'] is not None for i in bad['order_ids'] if order(s,i)['transport']=='kafka')==2)
    replay=call('/replay',dict(role='email',dead=True))
    self_assert(any(r['order_id']==kt['order_id'] for r in replay['records']),'Kafka application DLQ topic has no actual error record')
    print('PASS failure comparison: RabbitMQ isolates bad email; Kafka pauses only its partition; custom DLQ publish unblocks followers',flush=True)
    for mode in ('rabbitmq','kafka'):
        state=call('/state');target=next(order(state,i) for i in bad['order_ids'] if order(state,i)['transport']==mode and order(state,i)['customer']['email']=='thieu-dia-chi-email')
        call('/orders/'+target['id']+'/email',{'email':'fixed@example.test'},method='PATCH')
        call('/tasks/'+target['id']+':email/retry',{})
        wait(lambda s:order(s,target['id'])['status']=='completed')
    print('PASS repair and retry: same order/task identity, corrected email gets real attachment',flush=True)
    for mode in ('rabbitmq','kafka'):
        crash=call('/seed',dict(transport=mode,scenario='crash',count=1));cid=crash['order_ids'][0]
        state=wait(lambda s:order(s,cid)['status']=='completed' and any(e['kind']=='DEDUP' and e['task_id']==cid+':invoice' for e in s['events']),timeout=100)
        self_assert(next(t for t in order(state,cid)['tasks'] if t['role']=='invoice')['deliveries']>=2,'invoice task was not redelivered')
        print('PASS crash after invoice commit + recovery on '+mode,flush=True)
    before=call('/state')['totals'].copy();replay=call('/replay',{})
    after=call('/state')['totals']
    self_assert(replay['records'] and before==after,'analytics replay created side effects')
    print('PASS analytics group replays real retained orders without creating invoices/emails',flush=True)
    # Rabbit outage: checkout still persists order + outbox. Never claim queued.
    compose('stop','rabbitmq')
    try:
        queued=checkout();qid=queued['order_id']
        time.sleep(2)
        state=call('/state');self_assert(order(state,qid)['invoice'] is None,'invoice generated while Rabbit unavailable')
        self_assert(any(r['task_id']==qid+':invoice' and r['status']=='pending' for r in state['outbox']),'missing durable outbox')
    finally:compose('start','rabbitmq')
    ensure('rabbitmq','invoice',1);ensure('rabbitmq','email',1)
    wait(lambda s:order(s,qid)['status']=='completed',timeout=100)
    print('PASS broker outage: order survives in outbox and completes after recovery',flush=True)
    print('ALL SHOP INTEGRATION CHECKS PASSED',flush=True)


if __name__=='__main__':main()
