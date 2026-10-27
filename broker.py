"""Shared AMQP 0-9-1 topology. No connections cross thread boundaries."""
import base64
import json
import os
from urllib.request import Request, urlopen
import pika

EXCHANGE = 'seminar.v3.tasks'
QUEUE = 'seminar.v3.invoices'
KEY = 'invoice.create'
DLX = 'seminar.v3.errors'
DLQ = 'seminar.v3.invoices.failed'


def connection():
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=os.getenv('RABBITMQ_HOST', '127.0.0.1'),
        port=int(os.getenv('RABBITMQ_PORT', '5672')),
        credentials=pika.PlainCredentials(os.getenv('RABBITMQ_USER', 'guest'), os.getenv('RABBITMQ_PASS', 'guest')),
        heartbeat=30, blocked_connection_timeout=5, socket_timeout=3, connection_attempts=1))


def topology(conn):
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type='direct', durable=True)
    ch.exchange_declare(exchange=DLX, exchange_type='direct', durable=True)
    ch.queue_declare(queue=DLQ, durable=True, arguments={'x-queue-type': 'classic'})
    ch.queue_bind(queue=DLQ, exchange=DLX, routing_key='failed')
    ch.queue_declare(queue=QUEUE, durable=True, arguments={
        'x-queue-type': 'classic', 'x-dead-letter-exchange': DLX,
        'x-dead-letter-routing-key': 'failed', 'x-max-length': 1000, 'x-overflow': 'reject-publish'})
    ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=KEY)
    return ch


def publish(ch, job):
    ch.basic_publish(exchange=EXCHANGE, routing_key=KEY,
        body=json.dumps(job, ensure_ascii=False).encode('utf-8'),
        properties=pika.BasicProperties(content_type='application/json', delivery_mode=2, message_id=job['id']), mandatory=True)


def metrics():
    try:
        token = base64.b64encode((os.getenv('RABBITMQ_USER', 'guest') + ':' + os.getenv('RABBITMQ_PASS', 'guest')).encode()).decode()
        req = Request(os.getenv('RABBITMQ_MANAGEMENT', 'http://127.0.0.1:15672') + '/api/queues/%2F', headers={'Authorization': 'Basic ' + token})
        with urlopen(req, timeout=2) as response:
            queues = {q['name']: q for q in json.load(response)}
        q, d = queues.get(QUEUE, {}), queues.get(DLQ, {})
        return {'ok': True, 'queue': QUEUE, 'ready': q.get('messages_ready', 0),
            'unacked': q.get('messages_unacknowledged', 0), 'consumers': q.get('consumers', 0),
            'dlq': d.get('messages_ready', 0), 'source': 'RabbitMQ Management API, có độ trễ lấy mẫu'}
    except Exception as exc:
        return {'ok': False, 'error': type(exc).__name__, 'ready': None, 'unacked': None, 'consumers': None, 'dlq': None}
