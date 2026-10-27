"""SQLite observation log and transactional, idempotent invoice effect."""
import json
import os
from pathlib import Path
import sqlite3
import time
import uuid
from contextlib import contextmanager

DB = Path(os.getenv('SEMINAR_DB', str(Path(__file__).parent / 'data' / 'seminar.sqlite3')))


@contextmanager
def connect():
    DB.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA busy_timeout=10000')
    try:
        with db:
            yield db
    finally:
        db.close()


def init():
    with connect() as db:
        db.execute('PRAGMA journal_mode=WAL')
        db.executescript('''
        CREATE TABLE IF NOT EXISTS jobs (
          id TEXT PRIMARY KEY, body TEXT NOT NULL, status TEXT NOT NULL,
          created REAL NOT NULL, started REAL, completed REAL, worker TEXT,
          deliveries INTEGER NOT NULL DEFAULT 0, redelivered INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS invoices (
          job_id TEXT PRIMARY KEY, invoice_no TEXT NOT NULL, amount INTEGER NOT NULL, created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS events (
          seq INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL, kind TEXT NOT NULL,
          job_id TEXT, worker TEXT, detail TEXT);
        ''')


def event(kind, job_id=None, worker=None, detail=''):
    with connect() as db:
        db.execute('INSERT INTO events(ts,kind,job_id,worker,detail) VALUES(?,?,?,?,?)', (time.time(), kind, job_id, worker, detail))


def new_job(seconds, poison=False, crash_after_commit=False):
    job = {'id': 'J-' + uuid.uuid4().hex[:8], 'seconds': seconds, 'poison': poison,
        'crash_after_commit': crash_after_commit, 'amount': 250000, 'created': time.time()}
    with connect() as db:
        db.execute('INSERT INTO jobs(id,body,status,created) VALUES(?,?,?,?)', (job['id'], json.dumps(job), 'publishing', job['created']))
    return job


def publish_result(job_id, ok):
    with connect() as db:
        db.execute("UPDATE jobs SET status=? WHERE id=? AND status='publishing'", ('ready' if ok else 'publish_unknown', job_id))
    event('CONFIRM' if ok else 'PUBLISH_UNKNOWN', job_id, 'API', 'Broker xác nhận nhận tin' if ok else 'Kết quả gửi chưa xác định')


def delivered(job, worker, redelivered):
    with connect() as db:
        db.execute('INSERT OR IGNORE INTO jobs(id,body,status,created) VALUES(?,?,?,?)', (job['id'], json.dumps(job), 'ready', job['created']))
        db.execute("UPDATE jobs SET status='processing',started=?,worker=?,deliveries=deliveries+1,redelivered=redelivered+? WHERE id=?", (time.time(), worker, int(redelivered), job['id']))
    event('REDELIVER' if redelivered else 'DELIVER', job['id'], worker, 'Broker giao tin, manual ACK; prefetch=1')


def invoice_exists(job_id):
    with connect() as db:
        return db.execute('SELECT 1 FROM invoices WHERE job_id=?', (job_id,)).fetchone() is not None


def commit_invoice(job):
    # UNIQUE and the complete business effect are in the SAME transaction.
    with connect() as db:
        cursor = db.execute('INSERT OR IGNORE INTO invoices(job_id,invoice_no,amount,created) VALUES(?,?,?,?)', (job['id'], 'INV-' + job['id'], job['amount'], time.time()))
        return cursor.rowcount == 1


def finish(job_id, status):
    with connect() as db:
        db.execute('UPDATE jobs SET status=?,completed=? WHERE id=?', (status, time.time(), job_id))


def mark_retry(job_id):
    with connect() as db:
        row = db.execute('SELECT body FROM jobs WHERE id=?', (job_id,)).fetchone()
        if row:
            job = json.loads(row['body'])
            job['poison'] = job['crash_after_commit'] = False
            db.execute("UPDATE jobs SET body=?,status='ready',completed=NULL WHERE id=?", (json.dumps(job), job_id))


def get_job(job_id):
    with connect() as db:
        row = db.execute('SELECT body FROM jobs WHERE id=?', (job_id,)).fetchone()
        return json.loads(row['body']) if row else None


def snapshot():
    with connect() as db:
        jobs = [dict(r) for r in db.execute("SELECT id,status,created,started,completed,worker,deliveries,redelivered,json_extract(body,'$.seconds') AS seconds FROM jobs ORDER BY created DESC LIMIT 100")]
        events = [dict(r) for r in db.execute('SELECT * FROM events ORDER BY seq DESC LIMIT 100')]
        invoices = [dict(r) for r in db.execute('SELECT * FROM invoices ORDER BY created DESC LIMIT 100')]
        totals = dict(db.execute('SELECT status,count(*) FROM jobs GROUP BY status').fetchall())
        effects = db.execute('SELECT count(*) FROM invoices').fetchone()[0]
    return {'jobs': jobs, 'events': events, 'invoices': invoices, 'totals': totals, 'effects': effects}


def reset():
    with connect() as db:
        for table in ('jobs', 'events', 'invoices'):
            db.execute(f'DELETE FROM {table}')
