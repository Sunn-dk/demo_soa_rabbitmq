"""Shop domain: server-priced orders, transactional outbox, invoice and mail ledgers."""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
DB = Path(os.getenv('SHOP_DB', str(ROOT / 'data' / 'shop.sqlite3')))
FILES = Path(os.getenv('SHOP_FILES', str(ROOT / 'data' / 'invoices')))
CATALOG = [
    dict(sku='tee', name='Áo thun Everyday', category='Áo', price=249000, image='tshirt.jpg', color='Trắng kem', sizes=['S','M','L','XL']),
    dict(sku='hoodie', name='Hoodie Essential', category='Áo', price=549000, image='hoodie.jpg', color='Đen than', sizes=['S','M','L','XL']),
    dict(sku='denim', name='Quần jeans Straight', category='Quần', price=599000, image='jeans.jpg', color='Xanh nhạt', sizes=['28','30','32','34']),
    dict(sku='bomber', name='Áo khoác Olive', category='Áo', price=790000, image='jacket.jpg', color='Xanh olive', sizes=['S','M','L','XL']),
    dict(sku='dress', name='Váy Floral Weekend', category='Váy', price=649000, image='dress.jpg', color='Hồng hoa', sizes=['S','M','L']),
    dict(sku='sneaker', name='Giày City Runner', category='Giày', price=890000, image='sneaker.jpg', color='Trắng / cam', sizes=['37','38','39','40','41','42']),
]
PRODUCTS = {p['sku']: p for p in CATALOG}


@contextmanager
def connect():
    DB.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB, timeout=15)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA busy_timeout=15000')
    try:
        with db:
            yield db
    finally:
        db.close()


def init():
    FILES.mkdir(parents=True, exist_ok=True)
    with connect() as db:
        db.execute('PRAGMA journal_mode=WAL')
        db.executescript('''
        CREATE TABLE IF NOT EXISTS orders (
          id TEXT PRIMARY KEY, request_key TEXT UNIQUE NOT NULL, fingerprint TEXT NOT NULL,
          transport TEXT NOT NULL, customer TEXT NOT NULL, items TEXT NOT NULL,
          subtotal INTEGER NOT NULL, shipping INTEGER NOT NULL, total INTEGER NOT NULL,
          created REAL NOT NULL, experiment TEXT, crash_after_commit INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS tasks (
          id TEXT PRIMARY KEY, order_id TEXT NOT NULL, role TEXT NOT NULL, transport TEXT NOT NULL,
          status TEXT NOT NULL, deliveries INTEGER NOT NULL DEFAULT 0,
          redeliveries INTEGER NOT NULL DEFAULT 0, worker TEXT, started REAL, completed REAL,
          error TEXT, resolution TEXT, topic TEXT, partition_id INTEGER, offset_id INTEGER);
        CREATE TABLE IF NOT EXISTS outbox (
          id TEXT PRIMARY KEY, dedupe_key TEXT UNIQUE NOT NULL, task_id TEXT NOT NULL,
          role TEXT NOT NULL, transport TEXT NOT NULL, payload TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
          error TEXT, created REAL NOT NULL, published REAL);
        CREATE TABLE IF NOT EXISTS invoices (
          order_id TEXT PRIMARY KEY, number TEXT UNIQUE NOT NULL, filename TEXT NOT NULL,
          snapshot TEXT NOT NULL, created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS deliveries (
          order_id TEXT PRIMARY KEY, recipient TEXT NOT NULL, message_id TEXT NOT NULL,
          sent REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS events (
          seq INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL, kind TEXT NOT NULL,
          order_id TEXT, task_id TEXT, worker TEXT, transport TEXT, detail TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS workers (
          name TEXT PRIMARY KEY, transport TEXT NOT NULL, role TEXT NOT NULL, pid INTEGER,
          state TEXT NOT NULL, assignment TEXT NOT NULL DEFAULT '[]', order_id TEXT, heartbeat REAL);
        CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT OR IGNORE INTO settings VALUES ('observation_delay','2');
        ''')
        db.execute('INSERT OR IGNORE INTO settings VALUES (?,?)',('generation',uuid.uuid4().hex[:8]))


def log(kind, order_id=None, task_id=None, worker=None, transport=None, detail='', db=None):
    args = (time.time(), kind, order_id, task_id, worker, transport, str(detail))
    if db is not None:
        db.execute('INSERT INTO events(ts,kind,order_id,task_id,worker,transport,detail) VALUES(?,?,?,?,?,?,?)', args)
    else:
        with connect() as conn:
            conn.execute('INSERT INTO events(ts,kind,order_id,task_id,worker,transport,detail) VALUES(?,?,?,?,?,?,?)', args)


def email_valid(email):
    return isinstance(email, str) and len(email) < 200 and re.fullmatch(r'[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+', email) is not None


def quote(items):
    if not isinstance(items, list) or not 1 <= len(items) <= 20:
        raise ValueError('Giỏ hàng cần từ 1 đến 20 dòng sản phẩm')
    result = []
    seen = set()
    for row in items:
        product = PRODUCTS.get(row.get('sku'))
        qty = row.get('quantity')
        size = row.get('size')
        if not product or type(qty) is not int or not 1 <= qty <= 10 or size not in product['sizes']:
            raise ValueError('Sản phẩm, kích cỡ hoặc số lượng không hợp lệ')
        if (product['sku'], size) in seen:
            raise ValueError('Hãy gộp số lượng của cùng sản phẩm và kích cỡ')
        seen.add((product['sku'], size))
        result.append(dict(sku=product['sku'], name=product['name'], size=size, color=product['color'],
                           quantity=qty, unit_price=product['price'], line_total=qty * product['price'], image=product['image']))
    subtotal = sum(r['line_total'] for r in result)
    shipping = 0 if subtotal >= 1000000 else 25000
    return dict(items=result, subtotal=subtotal, shipping=shipping, total=subtotal + shipping)


def enqueue(db, order_id, role, transport, dedupe_key=None):
    task_id = order_id + ':' + role
    body = dict(task_id=task_id, order_id=order_id, role=role)
    row = db.execute('SELECT experiment FROM orders WHERE id=?',(order_id,)).fetchone()
    if row and (row['experiment'] or '').startswith('same-partition'):
        body['partition'] = 0
    payload = json.dumps(body, ensure_ascii=False)
    db.execute('INSERT OR IGNORE INTO tasks(id,order_id,role,transport,status) VALUES(?,?,?,?,?)',
               (task_id, order_id, role, transport, 'waiting_dispatch'))
    db.execute('INSERT OR IGNORE INTO outbox(id,dedupe_key,task_id,role,transport,payload,created) VALUES(?,?,?,?,?,?,?)',
               (uuid.uuid4().hex, dedupe_key or task_id, task_id, role, transport, payload, time.time()))
    return task_id


def create_order(customer, items, request_key, transport='rabbitmq', experiment=None, crash_after_commit=False, allow_invalid=False):
    if transport not in ('rabbitmq', 'kafka'):
        raise ValueError('Transport không hợp lệ')
    if not allow_invalid and not email_valid(customer.get('email')):
        raise ValueError('Email nhận hóa đơn không hợp lệ')
    for field in ('name', 'address'):
        if not isinstance(customer.get(field), str) or not 2 <= len(customer[field].strip()) <= 250:
            raise ValueError('Vui lòng nhập tên và địa chỉ giao hàng')
    priced = quote(items)
    fingerprint = hashlib.sha256(json.dumps(dict(customer=customer, items=priced['items'], transport=transport), sort_keys=True).encode()).hexdigest()
    oid = 'DH-' + uuid.uuid4().hex
    with connect() as db:
        # BEGIN IMMEDIATE serializes conflicting idempotency keys across requests.
        db.execute('BEGIN IMMEDIATE')
        prior = db.execute('SELECT id,fingerprint FROM orders WHERE request_key=?', (request_key,)).fetchone()
        if prior:
            if prior['fingerprint'] != fingerprint:
                raise ValueError('Khóa gửi lại đã được dùng cho một giỏ hàng khác')
            return prior['id'], False
        db.execute('INSERT INTO orders VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                   (oid, request_key, fingerprint, transport, json.dumps(customer, ensure_ascii=False),
                    json.dumps(priced['items'], ensure_ascii=False), priced['subtotal'], priced['shipping'], priced['total'],
                    time.time(), experiment, int(crash_after_commit)))
        task_id = enqueue(db, oid, 'invoice', transport)
        log('ORDER_SAVED', oid, task_id, transport=transport, detail='Đơn hàng và outbox được lưu trong cùng transaction', db=db)
    return oid, True


def order(oid):
    with connect() as db:
        row = db.execute('SELECT * FROM orders WHERE id=?', (oid,)).fetchone()
        if not row:
            return None
        result = dict(row)
        result['customer'] = json.loads(result['customer'])
        result['items'] = json.loads(result['items'])
        inv = db.execute('SELECT * FROM invoices WHERE order_id=?', (oid,)).fetchone()
        delivery = db.execute('SELECT * FROM deliveries WHERE order_id=?', (oid,)).fetchone()
        tasks = [dict(t) for t in db.execute('SELECT * FROM tasks WHERE order_id=? ORDER BY role', (oid,))]
        result['invoice'] = dict(inv) if inv else None
        if inv:
            result['invoice']['snapshot'] = json.loads(inv['snapshot'])
        result['delivery'] = dict(delivery) if delivery else None
        result['tasks'] = tasks
        result['status'] = ('completed' if delivery else 'needs_attention' if any(t['status'] in ('blocked','dead_letter') for t in tasks)
                            else 'invoice_ready' if inv else 'processing' if any(t['status'] == 'processing' for t in tasks) else 'pending')
        return result


def task(task_id):
    with connect() as db:
        row = db.execute('SELECT * FROM tasks WHERE id=?', (task_id,)).fetchone()
        return dict(row) if row else None


def received(payload, worker, transport, redelivered=False, position=None):
    with connect() as db:
        row = db.execute('SELECT * FROM tasks WHERE id=?', (payload['task_id'],)).fetchone()
        if not row or row['order_id'] != payload['order_id'] or row['role'] != payload['role'] or row['transport'] != transport:
            raise ValueError('Task không thuộc đơn hàng/transport hiện tại')
        repeat = bool(redelivered or (transport == 'kafka' and row['deliveries']))
        pos = position or (None, None, None)
        db.execute('UPDATE tasks SET status=?,worker=?,started=?,deliveries=deliveries+1,redeliveries=redeliveries+?,error=NULL,resolution=NULL,topic=?,partition_id=?,offset_id=? WHERE id=?',
                   ('processing',worker,time.time(),int(repeat),*pos,payload['task_id']))
        log('REDELIVER' if repeat else 'DELIVER', payload['order_id'], payload['task_id'], worker, transport,
            f"{payload['role']} | " + (f'topic={pos[0]} partition={pos[1]} offset={pos[2]}' if position else 'AMQP manual ACK, prefetch=1'), db=db)


def finish(task_id, status='done', error=None):
    with connect() as db:
        db.execute('UPDATE tasks SET status=?,completed=?,error=? WHERE id=?', (status,time.time(),error,task_id))


def invoice(oid):
    with connect() as db:
        row = db.execute('SELECT * FROM invoices WHERE order_id=?', (oid,)).fetchone()
        return dict(row) if row else None


def commit_invoice(oid, number, filename, snapshot, worker):
    with connect() as db:
        row = db.execute('SELECT transport FROM orders WHERE id=?', (oid,)).fetchone()
        cursor = db.execute('INSERT OR IGNORE INTO invoices VALUES(?,?,?,?,?)', (oid,number,filename,json.dumps(snapshot,ensure_ascii=False),time.time()))
        if cursor.rowcount:
            enqueue(db, oid, 'email', row['transport'])
            log('INVOICE_COMMIT',oid,oid+':invoice',worker,row['transport'],'PDF + hóa đơn; tác vụ gửi email được thêm vào outbox trong cùng transaction',db=db)
        return cursor.rowcount == 1


def sent(oid):
    with connect() as db:
        return db.execute('SELECT 1 FROM deliveries WHERE order_id=?', (oid,)).fetchone() is not None


def record_sent(oid, recipient, message_id, worker, transport):
    with connect() as db:
        db.execute('INSERT OR IGNORE INTO deliveries VALUES(?,?,?,?)',(oid,recipient,message_id,time.time()))
        log('SMTP_ACCEPTED',oid,oid+':email',worker,transport,f'Mailpit SMTP đã nhận email tới {recipient}; Message-ID {message_id}',db=db)


def pending_outbox(limit=30, transport=None):
    with connect() as db:
        if transport:
            return [dict(r) for r in db.execute("SELECT * FROM outbox WHERE status='pending' AND transport=? ORDER BY created LIMIT ?",(transport,limit))]
        return [dict(r) for r in db.execute("SELECT * FROM outbox WHERE status='pending' ORDER BY created LIMIT ?", (limit,))]


def published(row, detail):
    with connect() as db:
        db.execute("UPDATE outbox SET status='published',published=?,attempts=attempts+1,error=NULL WHERE id=?", (time.time(),row['id']))
        db.execute("UPDATE tasks SET status='queued' WHERE id=? AND status='waiting_dispatch'", (row['task_id'],))
        body=json.loads(row['payload'])
        log('PUBLISH_CONFIRMED',body['order_id'],row['task_id'],transport=row['transport'],detail=detail,db=db)


def publish_error(row, error):
    with connect() as db:
        db.execute('UPDATE outbox SET attempts=attempts+1,error=? WHERE id=?',(type(error).__name__,row['id']))


def update_worker(name, transport, role, state='idle', assignment=None, order_id=None):
    with connect() as db:
        if assignment is None:
            prior=db.execute('SELECT assignment FROM workers WHERE name=?',(name,)).fetchone()
            encoded_assignment=prior['assignment'] if prior else '[]'
        else:
            encoded_assignment=json.dumps(assignment)
        db.execute('INSERT INTO workers(name,transport,role,pid,state,assignment,order_id,heartbeat) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET state=excluded.state,assignment=excluded.assignment,order_id=excluded.order_id,heartbeat=excluded.heartbeat',
                   (name,transport,role,os.getpid(),state,encoded_assignment,order_id,time.time()))


def stop_worker(name):
    with connect() as db:
        db.execute("UPDATE workers SET state='stopped',order_id=NULL,heartbeat=? WHERE name=?",(time.time(),name))


def settings(delay=None):
    with connect() as db:
        if delay is not None:
            db.execute("UPDATE settings SET value=? WHERE key='observation_delay'",(str(delay),))
        return {'observation_delay':float(db.execute("SELECT value FROM settings WHERE key='observation_delay'").fetchone()[0]),
                'generation':db.execute("SELECT value FROM settings WHERE key='generation'").fetchone()[0]}


def reset():
    with connect() as db:
        for table in ('orders','tasks','outbox','invoices','deliveries','events','workers'):
            db.execute('DELETE FROM '+table)
        db.execute("UPDATE settings SET value=? WHERE key='generation'",(uuid.uuid4().hex[:8],))


def fix_email(oid, email):
    if not email_valid(email):
        raise ValueError('Email mới không hợp lệ')
    with connect() as db:
        row=db.execute('SELECT customer FROM orders WHERE id=?',(oid,)).fetchone()
        if not row:
            raise ValueError('Không có đơn hàng')
        customer=json.loads(row['customer']);customer['email']=email
        db.execute('UPDATE orders SET customer=? WHERE id=?',(json.dumps(customer,ensure_ascii=False),oid))
        log('CUSTOMER_FIXED',oid,detail='Đã cập nhật email nhận hóa đơn',db=db)


def resolve(task_id, resolution):
    with connect() as db:
        db.execute('UPDATE tasks SET resolution=? WHERE id=?',(resolution,task_id))


def retry(task_id):
    with connect() as db:
        row=db.execute('SELECT * FROM tasks WHERE id=?',(task_id,)).fetchone()
        if not row:
            raise ValueError('Task không tồn tại')
        db.execute("UPDATE tasks SET status='waiting_dispatch',error=NULL,resolution=NULL WHERE id=?",(task_id,))
        enqueue(db,row['order_id'],row['role'],row['transport'],task_id+':retry:'+uuid.uuid4().hex)
        log('RETRY_REQUESTED',row['order_id'],task_id,transport=row['transport'],detail='Giữ cùng ID, đưa yêu cầu retry vào outbox',db=db)


def snapshot():
    with connect() as db:
        ids=[r[0] for r in db.execute('SELECT id FROM orders ORDER BY created DESC LIMIT 100')]
        result=dict(events=[dict(r) for r in db.execute('SELECT * FROM events ORDER BY seq DESC LIMIT 250')],
                    workers=[dict(r) for r in db.execute('SELECT * FROM workers ORDER BY name')],
                    totals=dict(orders=db.execute('SELECT count(*) FROM orders').fetchone()[0],
                                invoices=db.execute('SELECT count(*) FROM invoices').fetchone()[0],
                                emails=db.execute('SELECT count(*) FROM deliveries').fetchone()[0],
                                pending_outbox=db.execute("SELECT count(*) FROM outbox WHERE status='pending'").fetchone()[0]),
                    blocked=[dict(r) for r in db.execute("SELECT * FROM tasks WHERE status IN ('blocked','dead_letter')")])
        result['outbox']=[dict(r) for r in db.execute("SELECT id,task_id,transport,role,status,attempts,error,payload FROM outbox ORDER BY created DESC LIMIT 100")]
        result['transport_totals']={r['transport']:dict(r) for r in db.execute('SELECT o.transport,count(o.id) AS orders,count(i.order_id) AS invoices,count(d.order_id) AS emails FROM orders o LEFT JOIN invoices i ON i.order_id=o.id LEFT JOIN deliveries d ON d.order_id=o.id GROUP BY o.transport')}
        for mode in ('rabbitmq','kafka'):
            counts=result['transport_totals'].setdefault(mode,dict(orders=0,invoices=0,emails=0))
            counts['pending_outbox']=db.execute("SELECT count(*) FROM outbox WHERE transport=? AND status='pending'",(mode,)).fetchone()[0]
    result['orders']=[order(oid) for oid in ids]
    for w in result['workers']:
        w['assignment']=json.loads(w['assignment'])
    result['settings']=settings()
    return result
