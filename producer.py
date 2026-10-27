"""CLI producer for the SAME queue as the dashboard."""
import argparse
import json
import broker
import store


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', type=int, default=1)
    parser.add_argument('--seconds', type=float, default=3)
    parser.add_argument('--poison', action='store_true')
    parser.add_argument('--crash-after-commit', action='store_true')
    args = parser.parse_args()
    if not 1 <= args.count <= 30 or not 0 <= args.seconds <= 15:
        parser.error('count: 1..30; seconds: 0..15')
    store.init()
    with broker.connection() as conn:
        ch = broker.topology(conn)
        ch.confirm_delivery()
        for _ in range(args.count):
            job = store.new_job(args.seconds, args.poison, args.crash_after_commit)
            try:
                broker.publish(ch, job)
            except Exception:
                store.publish_result(job['id'], False)
                raise
            store.publish_result(job['id'], True)
            print(json.dumps({'confirmed': job['id']}))


if __name__ == '__main__':
    main()
