"""Real consumer processes. The same fulfillment function runs on both brokers."""
import argparse
import json
import time
import broker
import shop_broker as bus
import shop_store as store
from shop_fulfillment import fulfill


def complete(payload, name, transport):
    store.finish(payload['task_id'])
    store.log('ACK_SENT' if transport=='rabbitmq' else 'OFFSET_COMMIT',payload['order_id'],payload['task_id'],name,transport,
              'Xác nhận sau side effect; COMMIT DB và xác nhận broker không phải một transaction')


def rabbit(name, role):
    while True:
        try:
            store.update_worker(name,'rabbitmq',role,'connecting')
            with broker.connection() as conn:
                ch=bus.topology(conn);ch.basic_qos(prefetch_count=1)
                def consume(channel,method,props,body):
                    payload=None
                    try:
                        payload=json.loads(body)
                        store.received(payload,name,'rabbitmq',method.redelivered)
                        fulfill(payload,name,'rabbitmq',conn.sleep)
                    except Exception as exc:
                        if payload and 'task_id' in payload:
                            store.finish(payload['task_id'],'dead_letter',str(exc))
                            store.log('NACK_DLQ',payload.get('order_id'),payload['task_id'],name,'rabbitmq',str(exc))
                        else:
                            store.log('INVALID_DLQ',worker=name,transport='rabbitmq',detail=type(exc).__name__)
                        channel.basic_nack(method.delivery_tag,requeue=False)
                    else:
                        # A lost connection while ACKing is not a business failure.
                        # Reconnect and let broker redelivery + idempotency recover.
                        channel.basic_ack(method.delivery_tag)
                        complete(payload,name,'rabbitmq')
                    finally:
                        store.update_worker(name,'rabbitmq',role,'idle')
                ch.basic_consume(bus.queue(role),consume,auto_ack=False)
                store.update_worker(name,'rabbitmq',role,'idle')
                store.log('WORKER_UP',worker=name,transport='rabbitmq',detail=f'{role}: {bus.queue(role)}, prefetch=1')
                ch.start_consuming()
        except KeyboardInterrupt:
            return
        except Exception as exc:
            store.update_worker(name,'rabbitmq',role,'reconnecting')
            store.log('WORKER_RECONNECT',worker=name,transport='rabbitmq',detail=type(exc).__name__)
            time.sleep(2)


def kafka(name, role):
    from confluent_kafka import Consumer, TopicPartition
    while True:
        consumer=None
        try:
            store.update_worker(name,'kafka',role,'connecting',[])
            bus.ensure_kafka()
            assigned=[];blocked={}
            topic=bus.topic(role)
            consumer=Consumer({'bootstrap.servers':bus.bootstrap(),'group.id':topic+'.workers',
                'group.protocol':'classic','auto.offset.reset':'earliest','enable.auto.commit':False,
                'session.timeout.ms':10000,'heartbeat.interval.ms':2000,'max.poll.interval.ms':300000,
                'partition.assignment.strategy':'roundrobin','socket.timeout.ms':5000})
            def on_assign(c,partitions):
                assigned[:]=[p.partition for p in partitions]
                c.assign(partitions)
                store.update_worker(name,'kafka',role,'idle' if assigned else 'unassigned',assigned)
                store.log('PARTITION_ASSIGNED',worker=name,transport='kafka',detail=f'{topic}: partitions {assigned}')
            def on_revoke(c,partitions):
                assigned.clear();blocked.clear()
                store.update_worker(name,'kafka',role,'rebalancing',[])
                store.log('PARTITION_REVOKED',worker=name,transport='kafka',detail='Consumer group đang rebalance')
            consumer.subscribe([topic],on_assign=on_assign,on_revoke=on_revoke)
            last_update=0

            def work(msg,payload,repeat=False):
                store.received(payload,name,'kafka',repeat,(msg.topic(),msg.partition(),msg.offset()))
                fulfill(payload,name,'kafka',time.sleep)
                consumer.commit(message=msg,asynchronous=False)
                complete(payload,name,'kafka')

            while True:
                for partition,(msg,payload) in list(blocked.items()):
                    task=store.task(payload['task_id'])
                    resolution=task and task['resolution']
                    if not resolution:
                        continue
                    tp=TopicPartition(topic,partition)
                    try:
                        if resolution=='dead_letter':
                            envelope=dict(payload,source=dict(topic=topic,partition=partition,offset=msg.offset()),error=task['error'])
                            pos=bus.kafka_publish(envelope,dead=True)
                            consumer.commit(message=msg,asynchronous=False)
                            store.finish(payload['task_id'],'dead_letter',task['error'])
                            store.log('KAFKA_APP_DLQ',payload['order_id'],payload['task_id'],name,'kafka',json.dumps(pos)+'; ứng dụng publish topic lỗi trước khi commit offset nguồn')
                        else:
                            work(msg,payload,True)
                        consumer.resume([tp]);del blocked[partition]
                    except Exception as exc:
                        store.finish(payload['task_id'],'blocked',str(exc));store.resolve(payload['task_id'],None)
                        store.log('PARTITION_BLOCKED',payload['order_id'],payload['task_id'],name,'kafka',str(exc))
                msg=consumer.poll(.3)
                if msg is not None:
                    if msg.error():
                        raise RuntimeError(str(msg.error()))
                    payload=json.loads(msg.value())
                    try:
                        work(msg,payload)
                    except Exception as exc:
                        # Explicit policy: preserve order on this partition. Other
                        # partitions keep running. This is NOT a Kafka-wide freeze.
                        store.finish(payload['task_id'],'blocked',str(exc))
                        consumer.pause([TopicPartition(topic,msg.partition())])
                        blocked[msg.partition()]=(msg,payload)
                        store.log('PARTITION_BLOCKED',payload['order_id'],payload['task_id'],name,'kafka',
                                  f'partition={msg.partition()}, offset={msg.offset()}: {exc}. Chưa commit; chờ sửa/retry hoặc DLQ do ứng dụng xây.')
                if time.monotonic()-last_update>1:
                    store.update_worker(name,'kafka',role,'blocked' if blocked else 'idle' if assigned else 'unassigned',assigned,
                                        next(iter(blocked.values()))[1]['order_id'] if blocked else None)
                    last_update=time.monotonic()
        except KeyboardInterrupt:
            return
        except Exception as exc:
            store.update_worker(name,'kafka',role,'reconnecting',[])
            store.log('WORKER_RECONNECT',worker=name,transport='kafka',detail=type(exc).__name__+': '+str(exc))
            time.sleep(2)
        finally:
            if consumer:
                consumer.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('name');parser.add_argument('transport',choices=['rabbitmq','kafka']);parser.add_argument('role',choices=['invoice','email'])
    args=parser.parse_args();store.init()
    (rabbit if args.transport=='rabbitmq' else kafka)(args.name,args.role)
