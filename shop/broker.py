"""Two transports for the SAME invoice/email tasks. No Kafka Share Groups here."""
import base64
import json
import os
import threading
import pika
from urllib.request import Request, urlopen

PREFIX = 'seminar.shop.v4'
EXCHANGE = PREFIX + '.tasks'
DLX = PREFIX + '.errors'
PARTITIONS = 2
ROLES = ('invoice', 'email')
_kafka_ready = None
_kafka_lock = threading.Lock()


def connection():
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=os.getenv('RABBITMQ_HOST','127.0.0.1'),
        port=int(os.getenv('RABBITMQ_PORT','5672')),
        credentials=pika.PlainCredentials(os.getenv('RABBITMQ_USER','guest'),os.getenv('RABBITMQ_PASS','guest')),
        heartbeat=30,blocked_connection_timeout=5,socket_timeout=3,connection_attempts=1))


def queue(role):
    return PREFIX + '.' + role


def dlq(role):
    return queue(role) + '.failed'


def topology(conn):
    ch = conn.channel()
    ch.exchange_declare(EXCHANGE, exchange_type='direct', durable=True)
    ch.exchange_declare(DLX, exchange_type='direct', durable=True)
    for role in ROLES:
        ch.queue_declare(dlq(role), durable=True, arguments={'x-queue-type':'classic'})
        ch.queue_bind(dlq(role), DLX, role + '.failed')
        ch.queue_declare(queue(role), durable=True, arguments={'x-queue-type':'classic',
            'x-dead-letter-exchange':DLX,'x-dead-letter-routing-key':role+'.failed',
            'x-max-length':10000,'x-overflow':'reject-publish'})
        ch.queue_bind(queue(role), EXCHANGE, role)
    return ch


def rabbit_publish(ch, payload):
    ch.basic_publish(EXCHANGE, payload['role'], json.dumps(payload).encode(),
        properties=pika.BasicProperties(delivery_mode=2,content_type='application/json',message_id=payload['task_id']),mandatory=True)


def bootstrap():
    return os.getenv('KAFKA_BOOTSTRAP','127.0.0.1:9092')


def topic(role):
    from shop import store
    return PREFIX + '.' + store.settings()['generation'] + '.' + role


def ensure_kafka():
    global _kafka_ready
    with _kafka_lock:
        marker=topic('invoice')
        if _kafka_ready==marker:
            return
        from confluent_kafka import KafkaException, KafkaError
        from confluent_kafka.admin import AdminClient, NewTopic
        admin=AdminClient({'bootstrap.servers':bootstrap(),'socket.timeout.ms':4000})
        topics=[NewTopic(topic(role)+suffix,num_partitions=PARTITIONS if not suffix else 1,replication_factor=1,
                         config={'retention.ms':'86400000'}) for role in ROLES for suffix in ('','.dead')]
        futures=admin.create_topics(topics,operation_timeout=5,request_timeout=8)
        for future in futures.values():
            try:
                future.result(timeout=10)
            except KafkaException as exc:
                if exc.args[0].code()!=KafkaError.TOPIC_ALREADY_EXISTS:
                    raise
        _kafka_ready=marker


def kafka_publish(payload, dead=False, producer=None):
    from confluent_kafka import Producer
    ensure_kafka()
    p=producer or Producer({'bootstrap.servers':bootstrap(),'enable.idempotence':True,'acks':'all','message.timeout.ms':7000})
    result=[]
    def delivered(error,msg):
        result.append(error if error else dict(topic=msg.topic(),partition=msg.partition(),offset=msg.offset()))
    args=dict(key=payload['order_id'],value=json.dumps(payload),on_delivery=delivered)
    if 'partition' in payload and not dead:
        args['partition']=payload['partition']
    p.produce(topic(payload['role'])+('.dead' if dead else ''),**args)
    if p.flush(9) or not result or not isinstance(result[0],dict):
        raise RuntimeError('Kafka delivery chưa được xác nhận')
    return result[0]


def rabbit_metrics():
    try:
        auth=base64.b64encode((os.getenv('RABBITMQ_USER','guest')+':'+os.getenv('RABBITMQ_PASS','guest')).encode()).decode()
        req=Request(os.getenv('RABBITMQ_MANAGEMENT','http://127.0.0.1:15672')+'/api/queues/%2F',headers={'Authorization':'Basic '+auth})
        with urlopen(req,timeout=2) as response:
            queues={q['name']:q for q in json.load(response)}
        result={}
        for role in ROLES:
            q=queues.get(queue(role),{});d=queues.get(dlq(role),{})
            result[role]=dict(queue=queue(role),ready=q.get('messages_ready',0),unacked=q.get('messages_unacknowledged',0),
                              consumers=q.get('consumers',0),dlq=d.get('messages_ready',0))
        return dict(ok=True,roles=result,source='RabbitMQ Management API; có độ trễ lấy mẫu')
    except Exception as exc:
        return dict(ok=False,error=type(exc).__name__,roles={})
