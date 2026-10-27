"""Lifecycle checks for this disposable local seminar environment."""
import subprocess
import time
from urllib.error import HTTPError
from integration import call, wait, job


def compose(*args):
    subprocess.run(['docker', 'compose', *args], check=True, capture_output=True)


def main():
    for worker in call('/api/state')['workers']:
        if worker['alive']:
            call('/api/workers/' + worker['name'] + '/crash', {})
    wait(lambda s: s['broker']['consumers'] == 0)
    call('/api/reset', {})
    jid = call('/api/jobs', {'seconds': .2})['job_ids'][0]
    wait(lambda s: s['broker']['ready'] == 1)
    compose('restart', 'web')
    for _ in range(40):
        try:
            s = call('/api/state')
            break
        except Exception:
            time.sleep(.5)
    assert job(s, jid)['status'] == 'ready' and s['effects'] == 0
    print('PASS API restart: pending job retained, no startup purge', flush=True)
    compose('stop', 'rabbitmq')
    try:
        try:
            call('/api/jobs', {'seconds': .2})
            raise AssertionError('Broker down must not return success')
        except HTTPError as exc:
            assert exc.code == 503
        print('PASS broker down: HTTP 503, no fabricated acceptance', flush=True)
    finally:
        compose('start', 'rabbitmq')
    wait(lambda s: s['broker']['ok'] and s['broker']['ready'] == 1, timeout=60)
    call('/api/workers', {})
    s = wait(lambda s: job(s, jid)['status'] == 'done')
    assert s['effects'] == 1
    print('PASS broker restart: durable queue and persistent job processed after restart', flush=True)


if __name__ == '__main__':
    main()
