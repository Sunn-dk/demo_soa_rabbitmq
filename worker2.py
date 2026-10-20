from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
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

worker_name = sys.argv[1] if len(sys.argv) > 1 else "Kafka-Worker-1"
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'order_kafka_topic')
process_counter = 0

def now_ms():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

def on_assign(consumer, partitions):
    print("\n" + "="*75)
    if len(partitions) == 0:
        print(f"⚠️  [{worker_name}] KHÔNG ĐƯỢC CẤP PARTITION NÀO (IDLE!)")
        print(f"   └── Lý do: Số lượng Kafka Worker trong Group đã vượt quá số Partition của Topic (1 Partition)!")
        print(f"   └── So với RabbitMQ: RabbitMQ chia việc cho mọi Worker, còn Kafka khóa cứng theo Partition.")
    else:
        part_list = [f"Partition-{p.partition}" for p in partitions]
        print(f"🔗 [{worker_name}] Đã được gán độc quyền: {', '.join(part_list)}")
    print("="*75)

def main():
    global process_counter

    # Đảm bảo topic tồn tại và có đúng 1 partition kể cả khi worker2 chạy trước producer
    try:
        admin = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP})
        topic_list = [NewTopic(KAFKA_TOPIC, num_partitions=1, replication_factor=1)]
        fs = admin.create_topics(topic_list)
        for topic, f in fs.items():
            try:
                f.result()
            except Exception:
                pass
    except Exception:
        pass

    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': 'demo-kafka-comparison-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False  # Quản lý commit thủ công tương đương ACK bên RabbitMQ
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC], on_assign=on_assign)

    print(f"🚀 [{worker_name}] (KAFKA) Đang lắng nghe topic '{KAFKA_TOPIC}'... (Nhấn Ctrl+C để thoát)")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Lỗi Kafka: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                order_id = data['order_id']
                priority = data.get('priority', 1)
            except Exception as e:
                print(f"⚠️  [{worker_name}] Bỏ qua tin nhắn không đúng định dạng (Offset {msg.offset()}): {e}")
                consumer.commit(msg)
                continue

            enqueue_pos = data.get('enqueue_pos', 'N/A')
            enqueue_time = data.get('enqueue_time', 'N/A')
            offset = msg.offset()
            partition = msg.partition()

            # Tác vụ giữ nhịp để gom đơn
            if order_id.startswith("BUSY-HOLD"):
                print(f"\n⚙️  [{worker_name}] Đang bận xử lý tác vụ hiện tại (2.5s)... (6 đơn mới đang nối đuôi vào Kafka Log)")
                time.sleep(2.5)
                consumer.commit(msg)
                process_counter = 0
                print(f"✨ [{worker_name}] Bắt đầu đọc tuần tự từ Kafka Partition Log:")
                continue

            process_counter += 1
            print("\n" + "─"*75)

            # 1. Chứng minh Kafka KHÔNG thể ưu tiên đơn VIP
            if priority >= 10:
                print(f"🐢 [{worker_name}] [LƯỢT ĐỌC #{process_counter} | Offset: {offset}] ĐƠN VIP BỊ XỬ LÝ MUỘN!")
                print(f"   ├── Mã đơn hàng       : {order_id} (Priority gắn kèm: {priority})")
                print(f"   ├── Bằng chứng lúc nạp: Vị trí {enqueue_pos} | Nạp lúc {enqueue_time}")
                print(f"   └── Hạn chế của Kafka : Do cấu trúc Append-only Log bất biến, {order_id} buộc phải")
                print(f"                           đứng chờ ở Offset {offset} sau toàn bộ 5 đơn thường!")
            else:
                print(f"📄 [{worker_name}] [LƯỢT ĐỌC #{process_counter} | Offset: {offset}] ĐỌC ĐƠN: {order_id} (Priority: {priority})")
                print(f"   └── Bằng chứng lúc nạp: Vị trí {enqueue_pos} | Nạp lúc {enqueue_time}")

            print(f"⏳ [{worker_name}] Đang xử lý nghiệp vụ (tốn 3 giây)...")
            time.sleep(3)

            # 2. Chứng minh Kafka bị kẹt luồng (Head-of-Line Blocking) khi gặp tin nhắn lỗi
            if data.get("simulate_error"):
                print(f"\n💥 [{worker_name}] LỖI NGHIỆP VỤ tại {order_id} (Offset {offset})!")
                print(f"   ├── Hạn chế của Kafka : Kafka KHÔNG có lệnh NACK đẩy tự động sang Dead Letter Queue cho từng tin.")
                print(f"   └── Hậu quả           : Không thể Commit Offset {offset}! Luồng đọc bị KẸT CỨNG (Head-of-Line Blocking)!")

                # Mô phỏng Kafka retry lại đúng Offset bị lỗi đó làm nghẽn toàn bộ đơn phía sau
                for retry in range(1, 4):
                    print(f"   🔄 [Thử lại lần {retry}/3 tại Offset {offset}] Đang cố đọc lại {order_id}... (Đơn ORD-GOOD-02 ở Offset {offset+1} đang bị CHẶN ĐỨNG!)")
                    time.sleep(2)

                print(f"   ⚠️  [CẢNH BÁO] Để không treo hệ thống vĩnh viễn, Dev buộc phải tự viết code bỏ qua Offset {offset} thủ công (Mất tin nhắn nếu không tự code thêm Producer bắn sang Topic khác)!")
                consumer.commit(msg)
            else:
                consumer.commit(msg)
                print(f"✅ [{worker_name}] HOÀN TẤT {order_id} (Đã Commit Offset {offset} lúc {now_ms()}).")

    except KeyboardInterrupt:
        print(f"\n🛑 [{worker_name}] Đã dừng hoạt động.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()