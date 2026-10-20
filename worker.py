import pika
import json
import time
import sys
from datetime import datetime

# Hiển thị đúng tiếng Việt, emoji và ký tự đặc biệt trên Windows console
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
        sys.stderr.reconfigure(encoding='utf-8', line_buffering=True)
    except AttributeError:
        pass

import os

worker_name = sys.argv[1] if len(sys.argv) > 1 else "Worker-1"
process_counter = 0

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS")

def now_ms():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

def main():
    try:
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
        ch = conn.channel()
    except Exception as e:
        print(f"❌ [{worker_name}] Không thể kết nối tới RabbitMQ ({RABBITMQ_HOST}:{RABBITMQ_PORT}): {e}")
        print("👉 Vui lòng đảm bảo Docker container đang chạy: 'docker compose up -d'")
        return

    # Đảm bảo topology hàng đợi đã được khai báo chuẩn xác kể cả khi bật worker trước producer
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

    # Fair Dispatch: Chỉ nhận 1 message tại 1 thời điểm
    ch.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        global process_counter
        try:
            data = json.loads(body)
            order_id = data['order_id']
            priority = data.get('priority', 1)
        except Exception as e:
            print(f"⚠️  [{worker_name}] Bỏ qua tin nhắn không đúng định dạng: {e}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        note = data.get('note', '')
        enqueue_pos = data.get('enqueue_pos', 'N/A')
        enqueue_time = data.get('enqueue_time', 'N/A')

        # Nếu là tác vụ giữ nhịp tự động thì xử lý gọn để gom hàng đợi
        if order_id.startswith("BUSY-HOLD"):
            print(f"\n⚙️  [{worker_name}] Đang bận xử lý tác vụ hiện tại ({order_id} - 2.5s)... (Trong lúc này 6 đơn mới đang xếp hàng vào Queue)")
            time.sleep(2.5)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            process_counter = 0 # Reset bộ đếm để đếm chuẩn 6 đơn chính
            print(f"✨ [{worker_name}] Đã rảnh! Bắt đầu bốc 6 đơn đang chờ trong Queue ra xử lý:")
            return

        process_counter += 1
        print("\n" + "─"*75)

        if priority >= 10:
            print(f"👑 [{worker_name}] [LƯỢT XỬ LÝ #{process_counter}] PHÁT HIỆN ĐƠN VIP NHẢY HÀNG!")
            print(f"   ├── Mã đơn hàng       : {order_id} (Priority: {priority})")
            print(f"   ├── Bằng chứng lúc nạp: Vị trí {enqueue_pos} | Nạp lúc {enqueue_time}")
            print(f"   └── Kết quả định tuyến: Vào CUỐI CÙNG nhưng được bốc ra làm ĐẦU TIÊN (Vượt qua NORMAL-01..05)!")
        else:
            print(f"📥 [{worker_name}] [LƯỢT XỬ LÝ #{process_counter}] NHẬN ĐƠN: {order_id} (Priority: {priority})")
            print(f"   ├── Bằng chứng lúc nạp: Vị trí {enqueue_pos} | Nạp lúc {enqueue_time}")
            print(f"   └── Ghi chú           : {note}")

        print(f"⏳ [{worker_name}] Đang xử lý nghiệp vụ (tốn 3 giây)...")
        time.sleep(3)

        if data.get("simulate_error"):
            print(f"❌ [{worker_name}] PHÁT HIỆN LỖI tại {order_id}! Gửi NACK (requeue=False) -> Cô lập sang Dead Letter Queue!")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        else:
            print(f"✅ [{worker_name}] HOÀN TẤT {order_id} lúc {now_ms()}! Đã gửi ACK về Broker.")
            ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue='order_main_queue', on_message_callback=callback)
    print(f"🚀 [{worker_name}] Đang trực chiến tại 'order_main_queue'... (Nhấn Ctrl+C để thoát)")
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        print(f"\n🛑 [{worker_name}] Đã dừng hoạt động.")
        conn.close()

if __name__ == "__main__":
    main()