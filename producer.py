from fastapi import FastAPI
import pika
import json
import time
import sys
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Đảm bảo hiển thị đúng tiếng Việt và Emoji trên Windows console
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
        sys.stderr.reconfigure(encoding='utf-8', line_buffering=True)
    except AttributeError:
        pass

app = FastAPI(
    title="RabbitMQ vs Kafka - Side-by-Side Demo",
    description="Bấm 1 nút để gửi đồng thời vào cả RabbitMQ (worker.py) và Kafka (worker2.py) nhằm đối chiếu trực tiếp."
)

import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", 'localhost:9092')
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", 'order_kafka_topic')
RABBITMQ_HOST   = os.getenv("RABBITMQ_HOST", 'localhost')
RABBITMQ_PORT   = int(os.getenv("RABBITMQ_PORT", '5672'))
RABBITMQ_USER   = os.getenv("RABBITMQ_USER")
RABBITMQ_PASS   = os.getenv("RABBITMQ_PASS")

def now_ms():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

def get_rmq_channel():
    credentials = (
        pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        if (RABBITMQ_USER and RABBITMQ_PASS)
        else None
    )
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=60,
            blocked_connection_timeout=5,
        )
    )
    return conn, conn.channel()

kafka_producer = None
try:
    kafka_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})
except Exception as e:
    print(f"⚠️ Lưu ý khởi tạo Kafka Producer: {e}")

def setup_topology():
    # 1. Thiết lập RabbitMQ Topology
    try:
        conn, ch = get_rmq_channel()
        ch.exchange_declare(exchange='dlx_exchange', exchange_type='direct')
        ch.queue_declare(queue='dead_letter_queue', durable=True)
        ch.queue_bind(exchange='dlx_exchange', queue='dead_letter_queue', routing_key='poison_msg')

        ch.exchange_declare(exchange='order_exchange', exchange_type='direct')
        queue_args = {
            'x-max-priority': 10,
            'x-dead-letter-exchange': 'dlx_exchange',
            'x-dead-letter-routing-key': 'poison_msg'
        }
        ch.queue_declare(queue='order_main_queue', durable=True, arguments=queue_args)
        ch.queue_bind(exchange='order_exchange', queue='order_main_queue', routing_key='order_key')
        conn.close()
        print("✅ Đã thiết lập RabbitMQ Topology (CLI)")
    except Exception as e:
        print(f"⚠️ Lưu ý kết nối RabbitMQ (CLI): {e}")

    # 2. Thiết lập Kafka Topic với 1 Partition (Để chứng minh giới hạn Scale Worker của Kafka)
    try:
        admin = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP, 'socket.timeout.ms': 2000})
        topic_list = [NewTopic(KAFKA_TOPIC, num_partitions=1, replication_factor=1)]
        fs = admin.create_topics(topic_list, request_timeout=2.0)
        for topic, f in fs.items():
            try:
                f.result(timeout=1.0)
                print(f"✅ Đã tạo Kafka Topic '{topic}' (1 Partition)")
            except Exception:
                pass
    except Exception as e:
        print(f"⚠️ Lưu ý kết nối Kafka: {e}")

setup_topology()

def publish_both(ch, order_id: str, priority: int, simulate_error: bool, note: str, enqueue_pos: str = "", send_to_rabbitmq: bool = True, send_to_kafka: bool = True):
    timestamp = now_ms()
    payload = {
        "order_id": order_id,
        "priority": priority,
        "simulate_error": simulate_error,
        "note": note,
        "enqueue_pos": enqueue_pos,
        "enqueue_time": timestamp
    }
    body_str = json.dumps(payload)

    # 1. Gửi vào RabbitMQ (nếu bật)
    if send_to_rabbitmq and ch:
        try:
            ch.basic_publish(
                exchange='order_exchange',
                routing_key='order_key',
                body=body_str,
                properties=pika.BasicProperties(delivery_mode=2, priority=priority)
            )
        except Exception as e:
            print(f"⚠️ Lỗi gửi RabbitMQ ({order_id}): {e}")

    # 2. Gửi vào Kafka (nếu bật)
    if send_to_kafka and kafka_producer:
        try:
            kafka_producer.produce(
                KAFKA_TOPIC,
                key=order_id.encode('utf-8'),
                value=body_str.encode('utf-8'),
                headers=[('priority', str(priority).encode('utf-8'))]
            )
            kafka_producer.flush()
        except Exception as e:
            print(f"⚠️ Lỗi gửi Kafka ({order_id}): {e}")

    dest = "RabbitMQ + Kafka" if (send_to_rabbitmq and send_to_kafka) else ("RabbitMQ" if send_to_rabbitmq else "Kafka")
    log_line = f"[Nạp {enqueue_pos} | {timestamp}] 📤 {order_id:<15} | Priority: {priority:<2} | ({dest})"
    print(log_line)
    return log_line

@app.post("/0-reset-queues", tags=["0. Tiện ích: Dọn dẹp Hàng đợi (Reset Demo)"])
def reset_cli_queues():
    conn, ch = get_rmq_channel()
    p1 = ch.queue_purge(queue='order_main_queue')
    p2 = ch.queue_purge(queue='dead_letter_queue')
    conn.close()
    return {
        "status": "success",
        "message": f"Đã dọn sạch hàng đợi CLI: xóa {p1.method.message_count} đơn từ 'order_main_queue' và {p2.method.message_count} đơn từ 'dead_letter_queue'."
    }

@app.post("/1-async-single-order", tags=["1. Giao tiếp Bất đồng bộ (Async)"])
def create_single_order(order_id: str = "ORD-101", priority: int = 1, simulate_error: bool = False):
    start = time.time()
    conn, ch = get_rmq_channel()
    log = publish_both(ch, order_id, priority, simulate_error, "Đơn hàng đơn lẻ", "#1/1")
    conn.close()
    elapsed_ms = round((time.time() - start) * 1000, 2)
    return {
        "status": "202 Accepted",
        "response_time_ms": f"{elapsed_ms} ms",
        "insertion_log": log
    }

@app.post("/2-scenario-priority-queue", tags=["2. Ưu thế 1: Priority Queue (RabbitMQ vs Kafka)"])
def scenario_priority_queue():
    conn, ch = get_rmq_channel()
    logs = []

    # Giữ bận Worker RabbitMQ nếu đang bật sẵn để gom đủ 6 đơn vào hàng đợi
    queue_state = ch.queue_declare(queue='order_main_queue', passive=True)
    active_workers = queue_state.method.consumer_count
    if active_workers > 0:
        for w in range(active_workers):
            publish_both(
                ch, f"BUSY-HOLD-0{w+1}", priority=1, simulate_error=False,
                note="Tác vụ giữ nhịp RabbitMQ", enqueue_pos="#0", send_to_rabbitmq=True, send_to_kafka=False
            )
        time.sleep(0.2)

    # Giữ bận Worker Kafka 2.5s để 6 đơn cùng nằm sẵn trong Topic trước khi đọc
    publish_both(
        ch, "BUSY-HOLD-KAFKA", priority=1, simulate_error=False,
        note="Tác vụ giữ nhịp Kafka", enqueue_pos="#0", send_to_rabbitmq=False, send_to_kafka=True
    )
    time.sleep(0.2)

    # Nạp 5 đơn THƯỜNG trước (#1/6 -> #5/6) vào cả hai bên
    for i in range(1, 6):
        log = publish_both(
            ch, f"NORMAL-0{i}", priority=1, simulate_error=False,
            note=f"Đơn thường (Vào trước #{i}/6)", enqueue_pos=f"#{i}/6"
        )
        logs.append(log)
        time.sleep(0.15)

    # Nạp 1 đơn VIP vào CUỐI CÙNG (#6/6) vào cả hai bên
    vip_log = publish_both(
        ch, "VIP-999", priority=10, simulate_error=False,
        note="Đơn VIP khẩn cấp (Vào CUỐI CÙNG #6/6)", enqueue_pos="#6/6 (CUỐI CÙNG)"
    )
    logs.append(vip_log)
    conn.close()

    return {
        "scenario": "So sánh Priority Queue: worker.py (RabbitMQ) vs worker2.py (Kafka)",
        "expected_rabbitmq": "VIP-999 nhảy lên xử lý ĐẦU TIÊN (#1).",
        "expected_kafka": "VIP-999 bị ép xử lý CUỐI CÙNG (#6) vì Kafka đọc tuần tự theo Offset.",
        "insertion_timeline": logs
    }

@app.post("/3-scenario-dlq-isolation", tags=["3. Ưu thế 2: Per-message NACK & DLQ (RabbitMQ vs Kafka)"])
def scenario_dlq_isolation():
    conn, ch = get_rmq_channel()
    logs = [
        publish_both(ch, "ORD-GOOD-01", priority=5, simulate_error=False, note="Đơn hợp lệ 1", enqueue_pos="#1/3"),
        publish_both(ch, "ORD-POISON-ERR", priority=5, simulate_error=True, note="Đơn chứa dữ liệu lỗi!", enqueue_pos="#2/3"),
        publish_both(ch, "ORD-GOOD-02", priority=5, simulate_error=False, note="Đơn hợp lệ 2 (Phía sau đơn lỗi)", enqueue_pos="#3/3")
    ]
    conn.close()
    return {
        "scenario": "So sánh xử lý lỗi: worker.py (RabbitMQ) vs worker2.py (Kafka)",
        "expected_rabbitmq": "Cô lập ORD-POISON-ERR sang Dead Letter Queue và xử lý ngay ORD-GOOD-02.",
        "expected_kafka": "Kẹt cứng (Head-of-Line Blocking) tại Offset của ORD-POISON-ERR, làm ORD-GOOD-02 bị chặn đứng!",
        "insertion_timeline": logs
    }

@app.post("/4-scenario-free-scaling", tags=["4. Ưu thế 4: Scale Worker tự do (RabbitMQ vs Kafka)"])
def scenario_free_scaling(total_orders: int = 8):
    conn, ch = get_rmq_channel()
    for i in range(1, total_orders + 1):
        publish_both(
            ch, f"BATCH-{i:02d}", priority=1, simulate_error=False,
            note="Tải hàng loạt", enqueue_pos=f"#{i}/{total_orders}"
        )
    conn.close()
    return {
        "scenario": "So sánh Scale Worker: Mở 2-3 cửa sổ worker.py và 2-3 cửa sổ worker2.py",
        "expected_rabbitmq": "Tất cả các Worker đều nhận việc chia tải đều.",
        "expected_kafka": "Chỉ 1 Worker duy nhất chạy, các Worker còn lại ĐỨNG IM (Idle) vì Topic chỉ có 1 Partition."
    }