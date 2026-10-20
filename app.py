r"""
FlashOrder Pro - Backend API (app.py)
Chay o cong 8000:  .venv\Scripts\python.exe -m uvicorn app:app --reload --port 8000

Su dung queue rieng (flashorder_*) de khong xung dot voi producer.py/worker.py (cong 8001).
"""
import os
import sys
import json
import time
import copy
import logging
import threading
from datetime import datetime

# Đảm bảo hiển thị đúng tiếng Việt, emoji và ký tự đặc biệt trên Windows console
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
        sys.stderr.reconfigure(encoding='utf-8', line_buffering=True)
    except AttributeError:
        pass

import pika
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

# ─────────────────────────────────────────────────────────────────────────────
# Bộ lọc log: Ẩn request polling /api/state định kỳ để không spam Terminal
# ─────────────────────────────────────────────────────────────────────────────
class EndpointFilter(logging.Filter):
    """Lọc bỏ các request polling ngầm /api/state để Terminal luôn sạch sẽ."""
    def filter(self, record: logging.LogRecord) -> bool:
        return "/api/state" not in record.getMessage()

logging.getLogger("uvicorn.access").addFilter(EndpointFilter())

# ─────────────────────────────────────────────────────────────────────────────
# Cấu hình ứng dụng & Môi trường (Hỗ trợ cấu hình qua biến môi trường)
# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
INDEX_FILE = os.path.join(BASE_DIR, "index.html")

app = FastAPI(
    title="FlashOrder Pro – Hệ thống Điều phối Đơn hàng RabbitMQ",
    description=(
        "Web Dashboard thời gian thực: đặt hàng bất đồng bộ, Priority Queue, "
        "Dead Letter Queue, mở rộng Worker tự do và đối chiếu hạn chế Kafka."
    ),
    version="1.0.0",
)

# ─────────────────────────────────────────────────────────────────────────────
# Hằng số RabbitMQ – cấu hình linh hoạt, không hardcode credentials
# ─────────────────────────────────────────────────────────────────────────────
RABBIT_HOST   = os.getenv("RABBITMQ_HOST", "localhost")
RABBIT_PORT   = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBIT_USER   = os.getenv("RABBITMQ_USER")
RABBIT_PASS   = os.getenv("RABBITMQ_PASS")

EXCHANGE      = "flashorder_exchange"
ROUTING_KEY   = "flashorder_key"
MAIN_QUEUE    = "flashorder_main_queue"
DLX_EXCHANGE  = "flashorder_dlx"
DLQ_ROUTING   = "flashorder_poison"
DLQ_QUEUE     = "flashorder_dlq"

# ─────────────────────────────────────────────────────────────────────────────
# Trạng thái toàn cục – được cập nhật bởi nhiều luồng Worker
# ─────────────────────────────────────────────────────────────────────────────
STATE_LOCK = threading.Lock()
SYSTEM_STATE: dict = {
    "last_response_ms": 0.0,
    "last_order_msg":  "Sẵn sàng nhận đơn hàng mới",
    "waiting_orders":  [],   # Đơn đang xếp hàng trong RabbitMQ
    "dlq_orders":      [],   # Đơn bị lỗi cô lập trong Dead Letter Queue
    "completed_orders":[],   # Lịch sử 10 đơn hoàn tất gần nhất
    "workers":         {},   # Trạng thái từng Trạm Worker
    "kafka_sim": {           # Mô phỏng băng chuyền Kafka đối chiếu
        "log":                [],
        "current_order":      None,
        "progress":           0,
        "status":             "IDLE",   # IDLE | PROCESSING | BLOCKED
        "error_msg":          "",
        "extra_workers_idle": 0,
    },
}

ORDER_SEQ    = 0
WORKER_THREADS: dict[str, tuple] = {}   # {worker_id: (Thread, stop_evt, crash_evt)}


# ─────────────────────────────────────────────────────────────────────────────
# Tiện ích
# ─────────────────────────────────────────────────────────────────────────────
def now_str() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def get_rmq_connection() -> pika.BlockingConnection:
    credentials = (
        pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
        if (RABBIT_USER and RABBIT_PASS)
        else None
    )
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBIT_HOST,
            port=RABBIT_PORT,
            credentials=credentials,
            heartbeat=60,
            blocked_connection_timeout=5,
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# Khởi tạo Topology RabbitMQ
# ─────────────────────────────────────────────────────────────────────────────
def init_rabbitmq_topology() -> bool:
    try:
        conn = get_rmq_connection()
        ch   = conn.channel()

        # Dead Letter Exchange & Queue
        ch.exchange_declare(exchange=DLX_EXCHANGE, exchange_type="direct", durable=True)
        ch.queue_declare(queue=DLQ_QUEUE, durable=True)
        ch.queue_bind(exchange=DLX_EXCHANGE, queue=DLQ_QUEUE, routing_key=DLQ_ROUTING)

        # Main Exchange & Priority Queue (x-max-priority: 10)
        ch.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)
        ch.queue_declare(
            queue=MAIN_QUEUE,
            durable=True,
            arguments={
                "x-max-priority":           10,
                "x-dead-letter-exchange":   DLX_EXCHANGE,
                "x-dead-letter-routing-key": DLQ_ROUTING,
            },
        )
        ch.queue_bind(exchange=EXCHANGE, queue=MAIN_QUEUE, routing_key=ROUTING_KEY)

        # Dọn queue cũ khi khởi động lại để demo luôn bắt đầu sạch
        ch.queue_purge(queue=MAIN_QUEUE)
        ch.queue_purge(queue=DLQ_QUEUE)
        conn.close()
        print(f"[{now_str()}] 🐰 RabbitMQ Topology sẵn sàng (Queue: {MAIN_QUEUE}, DLQ: {DLQ_QUEUE})")
        return True
    except Exception as e:
        print(f"[{now_str()}] ⚠️ Chưa kết nối được RabbitMQ Topology (vui lòng bật container 'docker compose up -d'): {e}")
        return False


init_rabbitmq_topology()


# ─────────────────────────────────────────────────────────────────────────────
# Sắp xếp hàng đợi hiển thị đúng theo cơ chế Priority Queue của RabbitMQ
# ─────────────────────────────────────────────────────────────────────────────
def sort_waiting_queue() -> None:
    """Priority cao đứng trước; cùng Priority thì FIFO theo seq."""
    SYSTEM_STATE["waiting_orders"].sort(key=lambda x: (-x["priority"], x["seq"]))


# ─────────────────────────────────────────────────────────────────────────────
# Gửi đơn hàng vào RabbitMQ & cập nhật trạng thái hiển thị
# ─────────────────────────────────────────────────────────────────────────────
def publish_order_to_system(
    product: str,
    customer: str,
    is_vip: bool,
    simulate_error: bool,
) -> dict:
    global ORDER_SEQ
    with STATE_LOCK:
        ORDER_SEQ += 1
        current_seq = ORDER_SEQ

    order_id  = f"VIP-{100 + current_seq}" if is_vip else f"ORD-{100 + current_seq}"
    priority  = 10 if is_vip else 1
    timestamp = now_str()

    order_data = {
        "seq":            current_seq,
        "order_id":       order_id,
        "product":        product,
        "customer":       customer,
        "is_vip":         is_vip,
        "priority":       priority,
        "simulate_error": simulate_error,
        "created_at":     timestamp,
    }

    # 1. Đẩy thật vào RabbitMQ (nếu kết nối được)
    try:
        conn = get_rmq_connection()
        ch   = conn.channel()
        ch.basic_publish(
            exchange=EXCHANGE,
            routing_key=ROUTING_KEY,
            body=json.dumps(order_data, ensure_ascii=False),
            properties=pika.BasicProperties(delivery_mode=2, priority=priority),
        )
        conn.close()
    except Exception as e:
        print(f"[{timestamp}] ⚠️ Không thể đẩy đơn {order_id} vào RabbitMQ (Broker chưa sẵn sàng): {e}")

    # 2. Cập nhật state hiển thị (RabbitMQ side + Kafka simulation side)
    with STATE_LOCK:
        SYSTEM_STATE["waiting_orders"].append(order_data)
        sort_waiting_queue()

        # Kafka: luôn nối đuôi (Append-only Log), bất chấp VIP
        kafka_item = dict(order_data)
        kafka_item["offset"] = current_seq - 1
        SYSTEM_STATE["kafka_sim"]["log"].append(kafka_item)

    vip_badge = "👑 VIP (Priority=10)" if is_vip else f"📦 Đơn thường (Priority={priority})"
    err_badge = " [💣 SIMULATE ERROR]" if simulate_error else ""
    print(f"[{timestamp}] 📥 [NHẬN ĐƠN] {order_id}: {product} | Khách: {customer} | {vip_badge}{err_badge}")

    return order_data


# ─────────────────────────────────────────────────────────────────────────────
# Vòng lặp Worker thật kết nối RabbitMQ (chạy trên thread riêng)
# ─────────────────────────────────────────────────────────────────────────────
def rabbitmq_worker_loop(
    worker_id:   str,
    stop_event:  threading.Event,
    crash_event: threading.Event,
) -> None:

    with STATE_LOCK:
        SYSTEM_STATE["workers"][worker_id] = {
            "id":           worker_id,
            "status":       "IDLE",
            "current_order": None,
            "progress":     0,
            "last_note":    "Đang chờ đơn hàng từ Queue...",
        }

    conn = None
    ch = None

    while not stop_event.is_set():
        # Kiểm tra crash request (bấm nút "Giả lập Sập nguồn" khi đang rảnh)
        if crash_event.is_set():
            if conn and conn.is_open:
                try:
                    conn.close()
                except Exception:
                    pass
            with STATE_LOCK:
                SYSTEM_STATE["workers"].pop(worker_id, None)
                SYSTEM_STATE["kafka_sim"]["extra_workers_idle"] = max(
                    0, len(SYSTEM_STATE["workers"]) - 1
                )
            return

        # Đảm bảo kết nối RabbitMQ luôn sẵn sàng
        if conn is None or conn.is_closed or ch is None or ch.is_closed:
            try:
                conn = get_rmq_connection()
                ch   = conn.channel()
                ch.basic_qos(prefetch_count=1)
                with STATE_LOCK:
                    if worker_id in SYSTEM_STATE["workers"]:
                        SYSTEM_STATE["workers"][worker_id]["last_note"] = "Đang chờ đơn hàng từ Queue..."
            except Exception:
                with STATE_LOCK:
                    if worker_id in SYSTEM_STATE["workers"]:
                        SYSTEM_STATE["workers"][worker_id]["last_note"] = "Đang chờ kết nối RabbitMQ..."
                time.sleep(2)
                continue

        # Lấy 1 tin nhắn từ Queue (non-blocking)
        try:
            method_frame, _, body = ch.basic_get(queue=MAIN_QUEUE, auto_ack=False)
        except Exception:
            # Kết nối bị đứt – reset để reconnect
            try:
                if conn and conn.is_open:
                    conn.close()
            except Exception:
                pass
            conn = None
            ch = None
            time.sleep(1)
            continue

        if not method_frame:
            time.sleep(0.6)
            continue

        # Parse tin nhắn an toàn
        try:
            data     = json.loads(body)
            order_id = data["order_id"]
        except Exception:
            try:
                ch.basic_ack(delivery_tag=method_frame.delivery_tag)
            except Exception:
                pass
            continue

        # Cập nhật trạng thái PROCESSING
        with STATE_LOCK:
            SYSTEM_STATE["waiting_orders"] = [
                o for o in SYSTEM_STATE["waiting_orders"]
                if o["order_id"] != order_id
            ]
            SYSTEM_STATE["workers"][worker_id].update({
                "status":       "PROCESSING",
                "current_order": data,
                "progress":     10,
                "last_note":    f"Đang xuất kho & tạo hóa đơn cho {order_id}...",
            })
        print(f"[{now_str()}] ⚙️  [{worker_id}] Bắt đầu xử lý {order_id} ({data['product']}) | P:{data['priority']}")

        # Xử lý giả lập 4 giây (8 bước → Progress Bar mượt từ 0→100%)
        crashed = False
        for step in range(1, 9):
            if crash_event.is_set():
                crashed = True
                break
            time.sleep(0.5)
            with STATE_LOCK:
                if worker_id in SYSTEM_STATE["workers"]:
                    SYSTEM_STATE["workers"][worker_id]["progress"] = int((step / 8) * 100)

        # ── Giả lập sập nguồn giữa chừng ──────────────────────────────────
        if crashed:
            # Đóng kết nối không gửi ACK → RabbitMQ tự thu hồi (requeue) đơn
            try:
                if conn and conn.is_open:
                    conn.close()
            except Exception:
                pass
            conn = None
            ch = None
            with STATE_LOCK:
                SYSTEM_STATE["waiting_orders"].append(data)
                sort_waiting_queue()
                SYSTEM_STATE["workers"].pop(worker_id, None)
                SYSTEM_STATE["kafka_sim"]["extra_workers_idle"] = max(
                    0, len(SYSTEM_STATE["workers"]) - 1
                )
            print(f"[{now_str()}] 💥 [{worker_id}] SẬP NGUỒN! RabbitMQ tự động thu hồi (requeue) {data['order_id']} về Queue!")
            return

        # ── Xử lý ACK / NACK ────────────────────────────────────────────────
        if data.get("simulate_error"):
            # NACK requeue=False → đơn tự động sang Dead Letter Queue
            try:
                ch.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
            except Exception as e:
                print(f"[{now_str()}] ⚠️ Lỗi gửi NACK: {e}")
            with STATE_LOCK:
                data["error_reason"] = "Lỗi xác thực thanh toán / Địa chỉ không hợp lệ"
                data["failed_at"]    = now_str()
                SYSTEM_STATE["dlq_orders"].insert(0, data)
                SYSTEM_STATE["workers"][worker_id].update({
                    "status":       "IDLE",
                    "current_order": None,
                    "progress":     0,
                    "last_note":    f"❌ Đã cô lập đơn lỗi {order_id} sang DLQ!",
                })
            print(f"[{now_str()}] ❌ [{worker_id}] Phát hiện đơn lỗi {order_id}! Gửi NACK → Tự động cô lập vào DLQ ({DLQ_QUEUE})")
        else:
            try:
                ch.basic_ack(delivery_tag=method_frame.delivery_tag)
            except Exception as e:
                print(f"[{now_str()}] ⚠️ Lỗi gửi ACK: {e}")
            with STATE_LOCK:
                data["completed_at"] = now_str()
                data["handled_by"]   = worker_id
                SYSTEM_STATE["completed_orders"].insert(0, data)
                # Giữ tối đa 10 bản ghi lịch sử
                SYSTEM_STATE["completed_orders"] = SYSTEM_STATE["completed_orders"][:10]
                SYSTEM_STATE["workers"][worker_id].update({
                    "status":       "IDLE",
                    "current_order": None,
                    "progress":     0,
                    "last_note":    f"✅ Hoàn tất xuất kho {order_id}",
                })
            print(f"[{now_str()}] ✅ [{worker_id}] Hoàn tất xuất kho {order_id}! Gửi ACK về RabbitMQ")

    # Dừng bình thường (stop_event được set)
    if conn and conn.is_open:
        try:
            conn.close()
        except Exception:
            pass
    with STATE_LOCK:
        SYSTEM_STATE["workers"].pop(worker_id, None)


# ─────────────────────────────────────────────────────────────────────────────
# Luồng mô phỏng Băng chuyền Kafka đối chiếu (chạy ngầm liên tục)
# ─────────────────────────────────────────────────────────────────────────────
def kafka_comparison_loop() -> None:
    """Mô phỏng Kafka Consumer đọc tuần tự theo Offset, bị kẹt khi gặp lỗi."""
    while True:
        with STATE_LOCK:
            ksim = SYSTEM_STATE["kafka_sim"]
            if ksim["status"] not in ("BLOCKED",) and ksim["current_order"] is None and ksim["log"]:
                ksim["current_order"] = ksim["log"].pop(0)
                ksim["status"]        = "PROCESSING"
                ksim["progress"]      = 10
                ksim["error_msg"]     = ""

        if SYSTEM_STATE["kafka_sim"]["status"] == "PROCESSING":
            for step in range(1, 9):
                time.sleep(0.5)
                with STATE_LOCK:
                    if SYSTEM_STATE["kafka_sim"]["status"] != "PROCESSING":
                        break
                    SYSTEM_STATE["kafka_sim"]["progress"] = int((step / 8) * 100)

            with STATE_LOCK:
                cur = SYSTEM_STATE["kafka_sim"]["current_order"]
                if cur and cur.get("simulate_error"):
                    # Kafka không có NACK → kẹt cứng (Head-of-Line Blocking)
                    SYSTEM_STATE["kafka_sim"]["status"]    = "BLOCKED"
                    blocked_count = len(SYSTEM_STATE["kafka_sim"]["log"])
                    SYSTEM_STATE["kafka_sim"]["error_msg"] = (
                        f"KẸT CỨNG TẠI OFFSET #{cur['offset']} ({cur['order_id']})! "
                        f"Kafka không có NACK/DLQ tự động — "
                        f"toàn bộ {blocked_count} đơn phía sau bị chặn đứng!"
                    )
                else:
                    SYSTEM_STATE["kafka_sim"].update({
                        "current_order": None,
                        "status":        "IDLE",
                        "progress":      0,
                    })
        else:
            time.sleep(0.3)


threading.Thread(target=kafka_comparison_loop, daemon=True).start()


# ─────────────────────────────────────────────────────────────────────────────
# Spawn / quản lý Worker threads
# ─────────────────────────────────────────────────────────────────────────────
def spawn_worker() -> str:
    idx = 1
    while f"Trạm-Kho-{idx}" in WORKER_THREADS:
        idx += 1
    w_id     = f"Trạm-Kho-{idx}"
    stop_ev  = threading.Event()
    crash_ev = threading.Event()
    t = threading.Thread(
        target=rabbitmq_worker_loop,
        args=(w_id, stop_ev, crash_ev),
        daemon=True,
    )
    WORKER_THREADS[w_id] = (t, stop_ev, crash_ev)
    t.start()
    with STATE_LOCK:
        SYSTEM_STATE["kafka_sim"]["extra_workers_idle"] = max(
            0, len(WORKER_THREADS) - 1
        )
    print(f"[{now_str()}] 🏭 Trạm Worker '{w_id}' đã khởi động và lắng nghe '{MAIN_QUEUE}'")
    return w_id


# Khởi tạo sẵn 1 Trạm Worker khi server bật
spawn_worker()


@app.on_event("startup")
def startup_event():
    filter_inst = EndpointFilter()
    u_access = logging.getLogger("uvicorn.access")
    u_access.addFilter(filter_inst)
    for h in u_access.handlers:
        h.addFilter(filter_inst)
    print(f"[{now_str()}] 🛡️  Đã kích hoạt Log Filter: Ẩn request polling /api/state khỏi console.")
    print(f"[{now_str()}] 🚀 FlashOrder Pro sẵn sàng: http://localhost:8000")


# ─────────────────────────────────────────────────────────────────────────────
# Schema
# ─────────────────────────────────────────────────────────────────────────────
class OrderRequest(BaseModel):
    product:        str
    customer:       str
    is_vip:         bool = False
    simulate_error: bool = False


# ─────────────────────────────────────────────────────────────────────────────
# API Endpoints
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def serve_ui():
    """Trả về giao diện Web Dashboard (index.html)."""
    with open(INDEX_FILE, "r", encoding="utf-8") as f:
        return f.read()


@app.get("/api/state", summary="Lấy toàn bộ trạng thái hệ thống (polling)")
def get_state():
    with STATE_LOCK:
        # Deep-copy để tránh race condition khi serialize
        return copy.deepcopy(SYSTEM_STATE)


@app.post("/api/orders", summary="Đặt 1 đơn hàng mới (bất đồng bộ, phản hồi < 200ms)")
def api_create_order(req: OrderRequest):
    start = time.time()
    order = publish_order_to_system(req.product, req.customer, req.is_vip, req.simulate_error)
    elapsed_ms = round((time.time() - start) * 1000, 2)
    with STATE_LOCK:
        SYSTEM_STATE["last_response_ms"] = elapsed_ms
        SYSTEM_STATE["last_order_msg"] = (
            f"Đặt thành công {order['order_id']} "
            f"({order['product']}) trong {elapsed_ms} ms!"
        )
    return {"status": "202 Accepted", "response_ms": elapsed_ms, "order": order}


@app.post("/api/orders/flash-sale", summary="Bắn nhanh Combo: 4 đơn thường + 1 đơn VIP (đặt cuối) → thấy VIP nhảy hàng")
def api_flash_sale():
    items = [
        ("Tai nghe AirPods 4",       "Nguyễn Văn A",              False, False),
        ("Chuột Logitech MX Master", "Trần Thị B",                False, False),
        ("Bàn phím Cơ Keychron K2",  "Lê Văn C",                  False, False),
        ("Màn hình LG 27 inch 4K",   "Phạm Thị D",                False, False),
        ("👑 iPhone 16 Pro Max 1TB", "Chủ tịch VIP (Đặt sau cùng)", True, False),
    ]
    start = time.time()
    for prod, cust, vip, err in items:
        publish_order_to_system(prod, cust, vip, err)
        time.sleep(0.05)
    elapsed_ms = round((time.time() - start) * 1000, 2)
    with STATE_LOCK:
        SYSTEM_STATE["last_response_ms"] = elapsed_ms
        SYSTEM_STATE["last_order_msg"] = (
            "Flash Sale! 5 đơn đã vào Queue — "
            "Đơn VIP đặt cuối nhưng đã nhảy lên đầu hàng đợi RabbitMQ 👑"
        )
    return {"status": "ok", "response_ms": elapsed_ms}


@app.post("/api/workers/add", summary="Mở thêm 1 Trạm Worker mới kết nối RabbitMQ")
def api_add_worker():
    w_id = spawn_worker()
    return {"worker_id": w_id, "message": f"Trạm '{w_id}' đã sẵn sàng nhận việc."}


@app.post("/api/workers/{worker_id}/crash", summary="Giả lập sập nguồn Worker (đơn đang làm dở sẽ được RabbitMQ tự thu hồi)")
def api_crash_worker(worker_id: str):
    if worker_id in WORKER_THREADS:
        _, _, crash_ev = WORKER_THREADS.pop(worker_id)
        crash_ev.set()
        return {"status": "crashed", "message": f"'{worker_id}' đã bị ngắt kết nối — RabbitMQ đang thu hồi đơn chưa ACK..."}
    return {"status": "not_found"}


@app.post("/api/dlq/retry-all", summary="Sửa lỗi toàn bộ đơn trong DLQ và đẩy lại vào hàng đợi chính")
def api_retry_dlq():
    # Xóa sạch DLQ thật trên Broker trước (nếu kết nối được)
    try:
        conn = get_rmq_connection()
        ch   = conn.channel()
        while True:
            method_frame, _, _ = ch.basic_get(queue=DLQ_QUEUE, auto_ack=True)
            if not method_frame:
                break
        conn.close()
    except Exception as e:
        print(f"[{now_str()}] ⚠️ Lưu ý dọn DLQ Broker: {e}")

    with STATE_LOCK:
        dlq_items = list(SYSTEM_STATE["dlq_orders"])
        SYSTEM_STATE["dlq_orders"].clear()

    for item in dlq_items:
        publish_order_to_system(
            product        = item["product"] + " ✅ (Đã sửa lỗi)",
            customer       = item["customer"],
            is_vip         = item.get("is_vip", False),
            simulate_error = False,   # Lần này hợp lệ
        )
    return {"retried": len(dlq_items), "message": f"Đã phục hồi và tái xử lý {len(dlq_items)} đơn."}


@app.post("/api/kafka/unblock", summary="Force-commit để gỡ kẹt Kafka (giải phóng Head-of-Line Blocking)")
def api_unblock_kafka():
    with STATE_LOCK:
        SYSTEM_STATE["kafka_sim"].update({
            "current_order": None,
            "status":        "IDLE",
            "progress":      0,
            "error_msg":     "",
        })
    return {"status": "unblocked", "message": "Đã bỏ qua Offset lỗi, Kafka tiếp tục đọc từ Offset kế tiếp."}