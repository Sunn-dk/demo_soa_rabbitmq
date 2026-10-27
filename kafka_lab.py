"""Real Kafka: publish six records, then read the same retained log in two groups."""
import json
import os
import time
import uuid
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', '127.0.0.1:9092')


def replay_experiment():
    # Isolated topic: no resetting offsets or deleting classmates' data.
    topic = 'seminar-replay-' + uuid.uuid4().hex[:8]
    admin = AdminClient({'bootstrap.servers': BOOTSTRAP, 'socket.timeout.ms': 5000})
    future = admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1,
        config={'retention.ms': '3600000'})], operation_timeout=10, request_timeout=15)[topic]
    future.result(timeout=20)
    producer = Producer({'bootstrap.servers': BOOTSTRAP, 'enable.idempotence': True,
        'acks': 'all', 'message.timeout.ms': 10000})
    delivery_errors = []
    records = []
    def delivered(error, message):
        if error:
            delivery_errors.append(str(error))
    for i in range(6):
        event = {'event_id': f'E-{i+1}', 'invoice': 250000, 'kind': 'invoice.created'}
        records.append(event)
        producer.produce(topic, partition=i % 3, key=event['event_id'], value=json.dumps(event), on_delivery=delivered)
    if producer.flush(15) or delivery_errors:
        raise RuntimeError('Kafka chưa confirm toàn bộ bản ghi: ' + str(delivery_errors))

    def read(group):
        consumer = Consumer({'bootstrap.servers': BOOTSTRAP, 'group.id': group,
            'auto.offset.reset': 'earliest', 'enable.auto.commit': False,
            'session.timeout.ms': 10000, 'socket.timeout.ms': 5000})
        rows = []
        try:
            consumer.subscribe([topic])
            deadline = time.monotonic() + 25
            while len(rows) < 6 and time.monotonic() < deadline:
                msg = consumer.poll(1)
                if msg is None:
                    continue
                if msg.error():
                    raise RuntimeError(str(msg.error()))
                data = json.loads(msg.value())
                rows.append({'event_id': data['event_id'], 'partition': msg.partition(), 'offset': msg.offset()})
                consumer.commit(message=msg, asynchronous=False)
        finally:
            consumer.close()
        if len(rows) != 6:
            raise RuntimeError(f'Chỉ đọc được {len(rows)}/6 bản ghi')
        return {'group': group, 'records': rows}

    first = read(topic + '-billing')
    second = read(topic + '-analytics')
    return {'topic': topic, 'partitions': 3, 'published': 6, 'first': first, 'replay': second,
        'same_events': sorted(r['event_id'] for r in first['records']) == sorted(r['event_id'] for r in second['records']),
        'meaning': 'Group B đọc lại đủ dữ liệu dù group A đã commit. Commit không xóa log; retention quyết định thời gian lưu.',
        'scope': 'Consumer Groups truyền thống. Share Groups là cơ chế khác, không chạy trong lab này.'}


def main():
    print(json.dumps(replay_experiment(), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
