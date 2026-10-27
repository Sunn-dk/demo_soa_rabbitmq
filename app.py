"""Seminar API. Run one server process; consumers are separate OS processes."""
import asyncio
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Annotated
import broker
import store
import shop_api

ROOT = Path(__file__).parent
PROCESSES = {}
PROCESS_LOCK = threading.Lock()
PUBLISH_LOCK = threading.Lock()


def stop_worker(name):
    with PROCESS_LOCK:
        process = PROCESSES.get(name)
        if not process or process.poll() is not None:
            raise HTTPException(404, 'Worker không còn chạy')
        process.kill()
        process.wait(timeout=10)
    store.event('CRASH', None, name, 'Dừng tiến trình thật, chờ broker giao lại tin chưa ACK')


@asynccontextmanager
async def lifespan(app):
    store.init()
    await shop_api.startup()
    try:
        yield
    finally:
        await shop_api.shutdown()
    for name in list(PROCESSES):
        if PROCESSES[name].poll() is None:
            stop_worker(name)


app = FastAPI(title='RabbitMQ Seminar Lab', version='3.0', lifespan=lifespan)
app.mount('/static', StaticFiles(directory=ROOT / 'static'), name='static')
app.include_router(shop_api.router)


class JobRequest(BaseModel):
    count: int = Field(1, ge=1, le=30)
    seconds: float = Field(3, ge=0, le=15)
    poison: bool = False
    crash_after_commit: bool = False
    durations: list[Annotated[float, Field(ge=0, le=15)]] | None = Field(None, min_length=1, max_length=30)


@app.get('/', include_in_schema=False)
def index():
    return FileResponse(ROOT / 'index.html')


@app.get('/lab', include_in_schema=False)
def legacy_lab():
    return FileResponse(ROOT / 'static' / 'legacy.html')


@app.get('/health', include_in_schema=False)
def health():
    return {'status': 'ok'}


@app.get('/api/state')
def state():
    result = store.snapshot()
    result['broker'] = broker.metrics()
    with PROCESS_LOCK:
        result['workers'] = [dict(name=n, pid=p.pid, alive=p.poll() is None) for n, p in PROCESSES.items()]
    return result


@app.post('/api/jobs', status_code=202)
def jobs(req: JobRequest):
    started = time.perf_counter()
    accepted = []
    attempted_id = None
    with PUBLISH_LOCK:
        try:
            with broker.connection() as conn:
                ch = broker.topology(conn)
                ch.confirm_delivery()
                for delay in req.durations or [req.seconds] * req.count:
                    job = store.new_job(delay, req.poison, req.crash_after_commit)
                    attempted_id = job['id']
                    try:
                        broker.publish(ch, job)
                    except Exception:
                        store.publish_result(job['id'], False)
                        raise
                    store.publish_result(job['id'], True)
                    accepted.append(job['id'])
        except Exception as exc:
            raise HTTPException(503, detail={'message': 'Không nhận thêm việc. Publish lỗi hoặc chưa rõ kết quả; kiểm tra job_id trước khi thử lại.',
                'confirmed_job_ids': accepted, 'last_attempted_job_id': attempted_id,
                'error': type(exc).__name__}) from exc
    elapsed = round((time.perf_counter() - started) * 1000, 1)
    store.event('HTTP_202', None, 'API', f'{len(accepted)} job, {elapsed} ms tới publisher confirm; chưa phải hoàn tất')
    return {'job_ids': accepted, 'status': 'accepted', 'api_ms': elapsed, 'meaning': 'Broker đã confirm. Worker có thể chưa xử lý.'}


@app.post('/api/sync')
def sync(req: JobRequest):
    if req.count != 1 or req.poison or req.crash_after_commit or req.durations:
        raise HTTPException(422, 'Đối chứng đồng bộ chỉ xử lý một job bình thường')
    started = time.perf_counter()
    time.sleep(req.seconds)
    return {'status': 'completed', 'api_ms': round((time.perf_counter() - started) * 1000, 1),
        'meaning': 'HTTP chờ hết xử lý giả lập; không đi qua RabbitMQ'}


@app.get('/api/jobs/{job_id}')
def inspect_job(job_id: str):
    job = store.get_job(job_id)
    if not job:
        raise HTTPException(404, 'Không có job')
    return {'body': job, 'properties': {'message_id': job['id'], 'content_type': 'application/json', 'delivery_mode': 2},
        'publish': {'exchange': broker.EXCHANGE, 'routing_key': broker.KEY, 'mandatory': True},
        'note': 'Bản dữ liệu ứng dụng đã dùng để publish; không phải thao tác đọc hay lấy tin khỏi queue.'}


@app.post('/api/workers', status_code=201)
def add_worker():
    with PROCESS_LOCK:
        if sum(p.poll() is None for p in PROCESSES.values()) >= 4:
            raise HTTPException(409, 'Demo giới hạn 4 worker')
        name = f'W{len(PROCESSES) + 1}'
        (ROOT / 'data').mkdir(exist_ok=True)
        with (ROOT / 'data' / f'{name}.log').open('a', encoding='utf-8') as log:
            kwargs = {'creationflags': subprocess.CREATE_NO_WINDOW} if os.name == 'nt' else {}
            process = subprocess.Popen([sys.executable, '-u', str(ROOT / 'worker.py'), name], cwd=ROOT, stdout=log, stderr=log, **kwargs)
        PROCESSES[name] = process
    return {'name': name, 'pid': process.pid}


@app.post('/api/workers/{name}/crash')
def crash(name: str):
    stop_worker(name)
    return {'stopped': name}


@app.post('/api/dlq/retry')
def retry():
    count = 0
    try:
        with PUBLISH_LOCK, broker.connection() as conn:
            ch = broker.topology(conn)
            ch.confirm_delivery()
            while count < 100:
                method, props, body = ch.basic_get(broker.DLQ, auto_ack=False)
                if not method:
                    break
                import json
                job = json.loads(body)
                job['poison'] = job['crash_after_commit'] = False
                store.mark_retry(job['id'])
                broker.publish(ch, job)
                ch.basic_ack(method.delivery_tag)
                store.event('RETRY', job['id'], 'API', 'Sửa dữ liệu, confirm publish rồi ACK bản trong DLQ')
                count += 1
    except Exception as exc:
        raise HTTPException(503, 'Retry lỗi; tin chưa ACK sẽ về DLQ. Kiểm tra event log.') from exc
    return {'retried': count}


@app.post('/api/jobs/{job_id}/duplicate', status_code=202)
def duplicate(job_id: str):
    job = store.get_job(job_id)
    if not job:
        raise HTTPException(404, 'Không có job')
    try:
        with PUBLISH_LOCK, broker.connection() as conn:
            ch = broker.topology(conn)
            ch.confirm_delivery()
            broker.publish(ch, job)
        store.event('DUPLICATE_SENT', job_id, 'API', 'Gửi lại cùng message_id để kiểm tra idempotency')
    except Exception as exc:
        raise HTTPException(503, 'Publish chưa xác định kết quả') from exc
    return {'job_id': job_id}


@app.post('/api/reset')
def reset():
    with PROCESS_LOCK:
        if any(p.poll() is None for p in PROCESSES.values()):
            raise HTTPException(409, 'Dừng tất cả worker trước khi reset. Chỉ dùng cho dữ liệu demo.')
        with PUBLISH_LOCK:
            try:
                with broker.connection() as conn:
                    ch = broker.topology(conn)
                    if ch.queue_declare(queue=broker.QUEUE, passive=True).method.consumer_count:
                        raise HTTPException(409, 'Còn worker CLI đang kết nối; hãy dừng worker đó')
                    ch.queue_purge(broker.QUEUE)
                    ch.queue_purge(broker.DLQ)
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(503, 'Không reset vì chưa kết nối được broker') from exc
            store.reset()
            PROCESSES.clear()
    return {'reset': True}


@app.post('/api/kafka/replay')
async def kafka_replay():
    from kafka_lab import replay_experiment
    try:
        return await asyncio.to_thread(replay_experiment)
    except Exception as exc:
        raise HTTPException(503, f'Kafka lab chưa chạy được: {type(exc).__name__}: {exc}') from exc
