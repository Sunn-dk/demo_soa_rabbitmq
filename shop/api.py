"""Local storefront API and teaching controls. Not an authenticated public shop."""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from shop import broker as bus
from shop import store

router=APIRouter(prefix='/api/shop',tags=['Cửa hàng thực tế'])
ROOT=Path(__file__).resolve().parents[1]
PROCESSES={}
LOCK=threading.RLock()
DISPATCH_LOCKS={k:threading.Lock() for k in ('rabbitmq','kafka')}
STOP=threading.Event()
BACKGROUND=[]
CACHE={}


def dispatch(transport):
    while not STOP.is_set():
        try:
            with DISPATCH_LOCKS[transport]:
                rows=store.pending_outbox(100,transport)
                if rows:
                    if transport=='rabbitmq':
                        with bus.connection() as conn:
                            ch=bus.topology(conn);ch.confirm_delivery()
                            for row in rows:
                                try:
                                    bus.rabbit_publish(ch,json.loads(row['payload']))
                                    store.published(row,f"{bus.EXCHANGE} -> {bus.queue(row['role'])}")
                                except Exception as exc:
                                    store.publish_error(row,exc);break
                    else:
                        for row in rows:
                            try:
                                pos=bus.kafka_publish(json.loads(row['payload']))
                                store.published(row,json.dumps(pos))
                            except Exception as exc:
                                store.publish_error(row,exc);break
        except Exception as exc:
            for row in store.pending_outbox(1,transport):
                store.publish_error(row,exc)
        STOP.wait(.6)


async def startup():
    store.init();STOP.clear()
    for transport in ('rabbitmq','kafka'):
        BACKGROUND.append(asyncio.create_task(asyncio.to_thread(dispatch,transport)))


async def shutdown():
    STOP.set()
    for name,p in list(PROCESSES.items()):
        if p.poll() is None:
            p.kill();p.wait(timeout=8)
        store.stop_worker(name)
    if BACKGROUND:
        await asyncio.gather(*BACKGROUND,return_exceptions=True)
    BACKGROUND.clear()


class Item(BaseModel):
    sku: str = Field(min_length=1,max_length=30)
    size: str = Field(min_length=1,max_length=10)
    quantity: int = Field(ge=1,le=10)


class Customer(BaseModel):
    name: str = Field(min_length=2,max_length=100)
    email: str = Field(min_length=3,max_length=190)
    address: str = Field(min_length=5,max_length=250)


class Checkout(BaseModel):
    customer: Customer
    items: list[Item] = Field(min_length=1,max_length=20)
    idempotency_key: str = Field(min_length=10,max_length=100)


class WorkerRequest(BaseModel):
    transport: str = Field(pattern='^(rabbitmq|kafka)$')
    role: str = Field(pattern='^(invoice|email)$')


class Seed(BaseModel):
    transport: str = Field('both',pattern='^(rabbitmq|kafka|both)$')
    scenario: str = Field('normal',pattern='^(normal|invalid_email|crash)$')
    count: int = Field(6,ge=1,le=12)


class Delay(BaseModel):
    seconds: float = Field(ge=0,le=8)


class Fix(BaseModel):
    email: str = Field(min_length=3,max_length=190)


class Replay(BaseModel):
    role: str = Field('invoice',pattern='^(invoice|email)$')
    dead: bool = False


@router.get('/catalog')
def catalog():
    return dict(products=store.CATALOG,free_shipping=1000000,shipping=25000,mailpit_url=f"http://localhost:{os.getenv('MAILPIT_WEB_PORT','8025')}",management_url=f"http://localhost:{os.getenv('MANAGEMENT_PORT','15672')}")


@router.post('/checkout',status_code=202)
def checkout(req:Checkout):
    start=time.perf_counter()
    try:
        oid,created=store.create_order(req.customer.model_dump(),[i.model_dump() for i in req.items],req.idempotency_key)
    except ValueError as exc:
        raise HTTPException(422,str(exc)) from exc
    return dict(order_id=oid,created=created,status='accepted',api_ms=round((time.perf_counter()-start)*1000,1),
                meaning='Đơn hàng và outbox đã được lưu bền. Chỉ hiển thị queued sau khi broker xác nhận publish.',order=store.order(oid))


@router.get('/orders/{oid}')
def get_order(oid:str):
    result=store.order(oid)
    if result is None:
        raise HTTPException(404,'Không có đơn hàng')
    return result


@router.get('/orders/{oid}/invoice.pdf')
def invoice_pdf(oid:str):
    inv=store.invoice(oid)
    if not inv or not (store.FILES/inv['filename']).is_file():
        raise HTTPException(404,'Worker chưa tạo hóa đơn PDF')
    return FileResponse(store.FILES/inv['filename'],media_type='application/pdf',filename=inv['number']+'.pdf')


def service_status():
    now=time.monotonic()
    if now-CACHE.get('at',-100)>4:
        result=dict(rabbitmq=bus.rabbit_metrics())
        try:
            from confluent_kafka.admin import AdminClient
            metadata=AdminClient({'bootstrap.servers':bus.bootstrap(),'socket.timeout.ms':1000}).list_topics(timeout=1)
            names=[bus.topic(r) for r in bus.ROLES]
            result['kafka']=dict(ok=True,topics=[dict(name=n,partitions=len(metadata.topics[n].partitions)) for n in names if n in metadata.topics],
                                 configured_partitions=bus.PARTITIONS,group_protocol='classic')
        except Exception as exc:
            result['kafka']=dict(ok=False,error=type(exc).__name__,topics=[],configured_partitions=bus.PARTITIONS)
        CACHE.update(at=now,result=result)
    return CACHE['result']


@router.get('/state')
def state():
    result=store.snapshot()
    with LOCK:
        for w in result['workers']:
            p=PROCESSES.get(w['name'])
            w['alive']=p is not None and p.poll() is None
        known={w['name'] for w in result['workers']}
        for name,p in PROCESSES.items():
            if name not in known and p.poll() is None:
                transport,role=PROCESS_META[name]
                result['workers'].append(dict(name=name,pid=p.pid,transport=transport,role=role,state='starting',assignment=[],alive=True,order_id=None))
    result['services']=service_status()
    return result


PROCESS_META={}


@router.post('/workers',status_code=201)
def add_worker(req:WorkerRequest):
    with LOCK:
        if sum(p.poll() is None and PROCESS_META[n]==(req.transport,req.role) for n,p in PROCESSES.items())>=4:
            raise HTTPException(409,'Giới hạn 4 worker cho mỗi loại và mỗi broker trong demo')
        name=('R' if req.transport=='rabbitmq' else 'K')+('-HD-' if req.role=='invoice' else '-MAIL-')+uuid.uuid4().hex[:5]
        (ROOT/'data').mkdir(exist_ok=True)
        with (ROOT/'data'/(name+'.log')).open('a',encoding='utf8') as log:
            kwargs={'creationflags':subprocess.CREATE_NO_WINDOW} if os.name=='nt' else {}
            p=subprocess.Popen([sys.executable,'-u','-m','shop.worker',name,req.transport,req.role],cwd=ROOT,stdout=log,stderr=log,**kwargs)
        PROCESSES[name]=p;PROCESS_META[name]=(req.transport,req.role)
    return dict(name=name,pid=p.pid)


@router.post('/workers/{name}/crash')
def crash_worker(name:str):
    with LOCK:
        p=PROCESSES.get(name)
        if not p or p.poll() is not None:
            raise HTTPException(404,'Worker đã dừng')
        p.kill();p.wait(timeout=8);store.stop_worker(name)
        transport,role=PROCESS_META[name]
        store.log('PROCESS_KILLED',worker=name,transport=transport,detail=role+': process thật đã bị kill; broker quyết định giao lại/assign lại')
    return dict(stopped=name)


@router.post('/settings')
def observation(req:Delay):
    return store.settings(req.seconds)


@router.post('/seed',status_code=202)
def seed(req:Seed):
    experiment=('same-partition-' if req.scenario=='invalid_email' else 'batch-')+uuid.uuid4().hex[:8]
    ids=[]
    for transport in (('rabbitmq','kafka') if req.transport=='both' else (req.transport,)):
        for i in range(req.count):
            customer=dict(name=f'Khách thử {i+1}',email='thieu-dia-chi-email' if req.scenario=='invalid_email' and i==0 else f'khach{i+1}@example.test',address='12 Đường Mộc, Phường Bến Thành, TP. Hồ Chí Minh')
            item=store.CATALOG[i%len(store.CATALOG)]
            oid,_=store.create_order(customer,[dict(sku=item['sku'],size=item['sizes'][1],quantity=1)],uuid.uuid4().hex,
                transport,experiment,req.scenario=='crash' and i==0,allow_invalid=True)
            ids.append(oid)
    return dict(experiment=experiment,order_ids=ids,
                note='Cùng catalogue, giá và hàm tạo PDF/gửi SMTP. Với lỗi email, các đơn cùng đi partition 0 để quan sát chính sách giữ thứ tự.')


@router.patch('/orders/{oid}/email')
def fix_email(oid:str,req:Fix):
    try:
        store.fix_email(oid,req.email)
    except ValueError as exc:
        raise HTTPException(422,str(exc)) from exc
    return store.order(oid)


@router.post('/tasks/{task_id}/retry')
def retry_task(task_id:str):
    task=store.task(task_id)
    if not task or task['status'] not in ('blocked','dead_letter'):
        raise HTTPException(409,'Chỉ retry tác vụ đang bị chặn hoặc ở DLQ')
    if task['transport']=='kafka':
        if task['status']=='blocked':
            store.resolve(task_id,'retry')
        else:
            store.retry(task_id)
    else:
        with DISPATCH_LOCKS['rabbitmq'],bus.connection() as conn:
            ch=bus.topology(conn);ch.confirm_delivery();found=False
            for _ in range(200):
                method,props,body=ch.basic_get(bus.dlq(task['role']),auto_ack=False)
                if method is None:
                    break
                payload=json.loads(body)
                if payload['task_id']==task_id:
                    store.finish(task_id,'queued')
                    bus.rabbit_publish(ch,payload)
                    ch.basic_ack(method.delivery_tag);found=True
                    store.log('DLQ_REPUBLISHED',task['order_id'],task_id,transport='rabbitmq',detail='Publish confirm trước ACK bản DLQ; giữ nguyên task ID')
                    break
            if not found:
                raise HTTPException(409,'Chưa lấy được tin trong DLQ; chờ broker cập nhật rồi thử lại')
            # Closing the connection requeues other inspected, unacked DLQ items.
    return dict(retry_requested=True)


@router.post('/tasks/{task_id}/dead-letter')
def kafka_dead_letter(task_id:str):
    task=store.task(task_id)
    if not task or task['transport']!='kafka' or task['status']!='blocked':
        raise HTTPException(409,'Chỉ dành cho partition Kafka đang bị chặn')
    store.resolve(task_id,'dead_letter')
    return dict(requested=True,meaning='Worker sẽ publish topic lỗi do ứng dụng xây, chờ delivery rồi commit offset nguồn')


@router.post('/orders/{oid}/duplicate')
def duplicate(oid:str):
    if not store.order(oid) or not store.invoice(oid):
        raise HTTPException(409,'Chờ tạo hóa đơn rồi mới gửi trùng tác vụ')
    store.retry(oid+':invoice')
    return dict(queued=True)


@router.post('/replay')
async def analytics_replay(req:Replay):
    def read():
        from confluent_kafka import Consumer,TopicPartition
        bus.ensure_kafka()
        name=bus.topic(req.role)+('.dead' if req.dead else '');group=name+'.analytics-'+uuid.uuid4().hex[:8]
        consumer=Consumer({'bootstrap.servers':bus.bootstrap(),'group.id':group,'auto.offset.reset':'earliest',
                           'enable.auto.commit':False,'session.timeout.ms':10000,'socket.timeout.ms':4000})
        rows=[]
        try:
            metadata=consumer.list_topics(name,timeout=4)
            target=0
            for partition in metadata.topics[name].partitions:
                low,high=consumer.get_watermark_offsets(TopicPartition(name,partition),timeout=4)
                target+=high-low
            if target:
                consumer.subscribe([name])
                deadline=time.monotonic()+25
                while len(rows)<min(target,100) and time.monotonic()<deadline:
                    message=consumer.poll(.5)
                    if message is None:
                        continue
                    if message.error():
                        raise RuntimeError(str(message.error()))
                    data=json.loads(message.value())
                    rows.append(dict(order_id=data['order_id'],task_id=data['task_id'],partition=message.partition(),offset=message.offset()))
                    consumer.commit(message=message,asynchronous=False)
        finally:
            consumer.close()
        if len(rows)<min(target,100):
            raise RuntimeError('Chưa đọc đủ phạm vi snapshot của topic; thử lại khi broker ổn định')
        store.log('ANALYTICS_REPLAY',transport='kafka',detail=f'{group}: đọc {len(rows)} records; không gọi tạo hóa đơn hay gửi SMTP')
        return dict(topic=name,group=group,records=rows,unique_orders=len({r['order_id'] for r in rows}),
                    meaning='Group mới đọc log còn được giữ, độc lập offset group xử lý. API này không tạo hóa đơn hoặc gửi email.')
    try:
        return await asyncio.to_thread(read)
    except Exception as exc:
        raise HTTPException(503,'Không đọc được Kafka: '+str(exc)) from exc


@router.post('/reset')
def reset():
    with LOCK,DISPATCH_LOCKS['rabbitmq'],DISPATCH_LOCKS['kafka']:
        if any(p.poll() is None for p in PROCESSES.values()):
            raise HTTPException(409,'Dừng toàn bộ worker cửa hàng trước khi làm mới phiên')
        with bus.connection() as conn:
            ch=bus.topology(conn)
            for role in bus.ROLES:
                if ch.queue_declare(bus.queue(role),passive=True).method.consumer_count:
                    raise HTTPException(409,'Còn consumer bên ngoài đang nhận việc')
                ch.queue_purge(bus.queue(role));ch.queue_purge(bus.dlq(role))
        store.reset();PROCESSES.clear();PROCESS_META.clear();CACHE.clear()
    return dict(reset=True,meaning='Phiên Kafka mới dùng topic mới; topic cũ giữ theo retention. File PDF cũ vẫn ở data/invoices nhưng không còn trong danh sách đơn hiện tại.')
