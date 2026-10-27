"""Separate consumer process: manual ACK, real crash, idempotent DB effect."""
import json
import os
import sys
import time
import broker
import store


def main():
    name = sys.argv[1] if len(sys.argv) > 1 else f'CLI-{os.getpid()}'
    store.init()
    while True:
        try:
            with broker.connection() as conn:
                ch = broker.topology(conn)
                ch.basic_qos(prefetch_count=1)

                def consume(channel, method, props, body):
                    try:
                        job = json.loads(body)
                        if (not isinstance(job.get('id'), str) or not isinstance(job.get('seconds'), (int, float))
                            or not isinstance(job.get('created'), (int, float)) or not isinstance(job.get('amount'), int)):
                            raise ValueError('invalid job')
                    except (ValueError, TypeError, AttributeError):
                        store.event('INVALID', None, name, 'Payload sai định dạng, chuyển DLQ')
                        channel.basic_nack(method.delivery_tag, requeue=False)
                        return
                    store.delivered(job, name, method.redelivered)
                    print(f"{name} DELIVER {job['id']} redelivered={method.redelivered}", flush=True)
                    if store.invoice_exists(job['id']):
                        store.event('DEDUP', job['id'], name, 'Hóa đơn đã có, bỏ qua hiệu ứng nghiệp vụ')
                    elif job.get('poison'):
                        store.finish(job['id'], 'failed')
                        channel.basic_nack(method.delivery_tag, requeue=False)
                        store.event('NACK', job['id'], name, 'requeue=False, broker định tuyến qua DLX đến DLQ')
                        return
                    else:
                        # Slow business work for observation, not broker latency.
                        conn.sleep(max(0, min(float(job['seconds']), 15)))
                        created = store.commit_invoice(job)
                        store.event('COMMIT', job['id'], name, 'Ghi hóa đơn trong SQLite; UNIQUE(job_id)')
                        if created and job.get('crash_after_commit'):
                            store.event('CRASH_BEFORE_ACK', job['id'], name, 'DB đã commit, tiến trình thoát trước basic_ack')
                            os._exit(42)
                    channel.basic_ack(method.delivery_tag)
                    store.finish(job['id'], 'done')
                    store.event('ACK_SENT', job['id'], name, 'Đã gửi ACK; đối chiếu số đếm broker sau lấy mẫu')

                ch.basic_consume(queue=broker.QUEUE, on_message_callback=consume, auto_ack=False)
                store.event('WORKER_UP', None, name, 'Đăng ký basic_consume, prefetch=1')
                ch.start_consuming()
        except KeyboardInterrupt:
            break
        except Exception as exc:
            store.event('WORKER_ERROR', None, name, type(exc).__name__)
            time.sleep(2)


if __name__ == '__main__':
    main()
