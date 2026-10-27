"""Black-box tests against REAL Docker brokers. Uses only disposable seminar data."""
import json
import os
from pathlib import Path
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError

BASE = os.getenv('SEMINAR_TEST_URL', 'http://127.0.0.1:8000').rstrip('/')


def call(path, data=None):
    req = Request(BASE + path, data=json.dumps(data).encode() if data is not None else None,
        headers={'Content-Type': 'application/json'})
    with urlopen(req, timeout=90) as response:
        return json.load(response)


def wait(predicate, timeout=25):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = call('/api/state')
        if predicate(last):
            return last
        time.sleep(.25)
    raise AssertionError(last)


def job(state, job_id):
    return next(j for j in state['jobs'] if j['id'] == job_id)


def main():
    for worker in call('/api/state')['workers']:
        if worker['alive']:
            call('/api/workers/' + worker['name'] + '/crash', {})
    wait(lambda s: s['broker']['consumers'] == 0)
    call('/api/reset', {})
    first = call('/api/jobs', {'seconds': 1})
    jid = first['job_ids'][0]
    assert first['status'] == 'accepted'
    s = wait(lambda s: s['broker']['ready'] == 1, 15)
    assert s['effects'] == 0
    assert call('/api/jobs/' + jid)['properties']['message_id'] == jid
    print('PASS async: confirm with 0 workers, Ready=1, invoices=0', flush=True)
    w1 = call('/api/workers', {})
    s = wait(lambda s: job(s, jid)['status'] == 'done')
    assert s['effects'] == 1
    call('/api/jobs/' + jid + '/duplicate', {})
    s = wait(lambda s: job(s, jid)['deliveries'] == 2 and job(s, jid)['status'] == 'done')
    assert s['effects'] == 1
    print('PASS duplicate: 2 deliveries, 1 invoice', flush=True)
    w2 = call('/api/workers', {})
    ids = call('/api/jobs', {'durations': [6, 1]})['job_ids']
    s = wait(lambda s: job(s, ids[1])['status'] == 'done')
    assert job(s, ids[0])['status'] == 'processing'
    assert job(s, ids[0])['worker'] != job(s, ids[1])['worker']
    print('PASS parallel + ordering: later 1s job finishes before earlier 6s job', flush=True)
    wait(lambda s: job(s, ids[0])['status'] == 'done')
    crash_id = call('/api/jobs', {'seconds': 6})['job_ids'][0]
    s = wait(lambda s: job(s, crash_id)['status'] == 'processing')
    owner = job(s, crash_id)['worker']
    call('/api/workers/' + owner + '/crash', {})
    s = wait(lambda s: job(s, crash_id)['status'] == 'done')
    assert job(s, crash_id)['redelivered'] >= 1
    print('PASS real process kill: broker redelivers unacked job', flush=True)
    cid = call('/api/jobs', {'seconds': .2, 'crash_after_commit': True})['job_ids'][0]
    wait(lambda s: any(e['kind'] == 'CRASH_BEFORE_ACK' and e['job_id'] == cid for e in s['events']))
    call('/api/workers', {})
    s = wait(lambda s: job(s, cid)['status'] == 'done')
    assert sum(i['job_id'] == cid for i in s['invoices']) == 1
    assert any(e['kind'] == 'DEDUP' and e['job_id'] == cid for e in s['events'])
    print('PASS crash after COMMIT: redelivery deduplicates committed business effect', flush=True)
    poison_id = call('/api/jobs', {'seconds': .1, 'poison': True})['job_ids'][0]
    good_id = call('/api/jobs', {'seconds': .1})['job_ids'][0]
    wait(lambda s: job(s, poison_id)['status'] == 'failed' and job(s, good_id)['status'] == 'done')
    wait(lambda s: s['broker']['dlq'] == 1, 15)
    assert call('/api/dlq/retry', {})['retried'] == 1
    wait(lambda s: job(s, poison_id)['status'] == 'done')
    print('PASS DLQ: bad job isolated, following good job completes, retry works', flush=True)
    try:
        call('/api/reset', {})
        raise AssertionError('Reset must reject live workers')
    except HTTPError as exc:
        assert exc.code == 409
    print('PASS reset guard: refuses while workers are alive', flush=True)
    replay = call('/api/kafka/replay', {})
    assert replay['same_events'] and len(replay['first']['records']) == len(replay['replay']['records']) == 6
    assert len({r['partition'] for r in replay['first']['records']}) == 3
    print('PASS Kafka: 6 records across 3 partitions replayed by second group after commit', flush=True)
    result_dir = Path(os.getenv('SEMINAR_TEST_OUTPUT', 'test-results'))
    result_dir.mkdir(parents=True, exist_ok=True)
    with (result_dir / 'kafka-evidence.json').open('w', encoding='utf8') as stream:
        json.dump(replay, stream, ensure_ascii=False, indent=2)
    print('ALL INTEGRATION CHECKS PASSED', flush=True)


if __name__ == '__main__':
    main()
