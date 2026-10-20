# 🐇 FlashOrder Pro — RabbitMQ vs Kafka Side-by-Side Demo

> Dự án minh hoạ kiến trúc **Message Broker** trong **SOA / Microservices** bằng cách xây dựng một sản phẩm thực tế: hệ thống đặt hàng & điều phối kho vận bất đồng bộ dùng **RabbitMQ**, kết hợp bảng đối chiếu trực quan những hạn chế của **Apache Kafka** khi áp dụng vào bài toán Task Queue tương tự.

---

## 📐 Kiến trúc tổng quan

```
[Trình duyệt / Swagger UI]
          │ HTTP REST
          ▼
 ┌─────────────────────────┐
 │  FastAPI (Producer)     │ ─── Gửi message ──▶ RabbitMQ Broker ──▶ Worker Threads
 │  app.py  (cổng 8000)    │                    (Priority Queue,       (ACK / NACK)
 │  producer.py (cổng 8001)│                     DLQ, Persistence)
 └─────────────────────────┘
          │                      ┌──────────────────────────────┐
          └── Gửi message ──────▶│ Apache Kafka (Cổng 9092)     │
                                 │ (Minh hoạ: FIFO Offset Log,  │
                                 │  Head-of-Line Blocking,       │
                                 │  giới hạn Partition)          │
                                 └──────────────────────────────┘
```

### Bảng đối chiếu hai chiều: RabbitMQ vs Apache Kafka

| Tiêu chí | 🐇 RabbitMQ (Message Broker) | 🐘 Apache Kafka (Event Streaming Log) |
|:---|:---|:---|
| **Bản chất kiến trúc** | **Smart Broker / Dumb Consumer**: Broker quản lý trạng thái, routing, xóa tin khi ACK | **Dumb Broker / Smart Consumer**: Broker chỉ ghi Append-only Log, Consumer tự quản lý Offset |
| **Độ ưu tiên (Priority)** | ✅ **Native `x-max-priority`**: Đơn VIP tự động nhảy hàng lên đầu Queue | ❌ **Không hỗ trợ native**: Bắt buộc đọc tuần tự FIFO theo Offset |
| **Xử lý tin lỗi (Poison Pill)** | ✅ **Per-message NACK**: Đơn lỗi tự nhảy sang Dead Letter Queue, không chặn luồng | ❌ **Head-of-Line Blocking**: Kẹt cứng tại Offset lỗi nếu không tự code bỏ qua |
| **Mở rộng Worker (Scale)** | ✅ **Tự do tuyệt đối**: Bao nhiêu Consumer cùng lắng nghe 1 Queue cũng được (Fair Dispatch) | ⚠️ **Khóa cứng theo Partition**: Số Consumer tối đa = Số Partition (thừa sẽ IDLE) |
| **Độ tin cậy (Delivery)** | ✅ **At-least-once** (hoặc At-most-once tùy cờ ACK/NACK từng tin) | ✅ **At-least-once** / Exactly-once (Transactional Producer + Idempotent Consumer) |
| **Lưu trữ dữ liệu (Retention)**| 🗑️ **Tạm thời**: Tin nhắn bị xóa ngay sau khi Consumer gửi ACK thành công | 💾 **Lâu dài / Vĩnh viễn**: Giữ log nhiều ngày/tháng, cho phép **Replay** dữ liệu cũ |
| **Thông lượng (Throughput)** | ⚡ Hàng chục nghìn msg/giây (Tối ưu độ trễ microsecond/millisecond) | 🚀 **Hàng triệu msg/giây** (Tối ưu nhờ Sequential I/O, Page Cache, Zero-copy) |
| **Khi nào nên chọn?** | **Task Queue, Order Processing, RPC, hệ thống cần định tuyến phức tạp** | **Big Data, Event Sourcing, Log Aggregation, Clickstream, IoT Stream** |

---

## 🏛️ Kiến trúc AMQP 0-9-1 & 4 Loại Exchange trong RabbitMQ

Trong chuẩn **AMQP 0-9-1**, Producer **không bao giờ gửi trực tiếp vào Queue**, mà luôn gửi tới một **Exchange**. Exchange phân phối tin nhắn vào các Queue dựa trên **Binding Rule** và **Routing Key**:

```
[Producer] ──(Message + Routing Key)──▶ [Exchange]
                                             │
                       ┌─────────────────────┼─────────────────────┐
             (Binding: "key_A")    (Binding: "key_B")    (Binding: "key_*")
                       ▼                     ▼                     ▼
                  [Queue A]              [Queue B]             [Queue C]
                       │                     │                     │
                       ▼                     ▼                     ▼
                  [Consumer]            [Consumer]            [Consumer]
```

### 4 Loại Exchange phổ biến:

| Loại Exchange | Cơ chế định tuyến | Use case thực tế trong SOA / Microservices |
|:---|:---|:---|
| **Direct** *(Dự án dùng)* | Khớp chính xác 100% giữa `routing_key` và `binding_key` | Điều phối tác vụ định danh cụ thể (1-to-1): Xử lý đơn hàng, gửi SMS OTP |
| **Fanout** | Bỏ qua routing key, sao chép tin broadcast tới **tất cả Queue** được bind | Publish/Subscribe: Đơn hàng mới kích hoạt đồng thời Email, Kho vận, Analytics |
| **Topic** | Khớp mẫu linh hoạt với ký tự đại diện: `*` (1 từ), `#` (0 hoặc nhiều từ) | Định tuyến đa vùng, đa kênh: `order.vietnam.vip`, `order.*.urgent`, `order.#` |
| **Headers** | Định tuyến dựa trên các cặp key-value trong Message Header | Định tuyến khi tiêu chí không thể biểu diễn bằng chuỗi ký tự đơn giản |

---

## 🛡️ Độ tin cậy (Delivery Guarantees) & Mẫu Idempotent Consumer

### 1. Delivery Semantics trong RabbitMQ:
- **At-most-once (`auto_ack=True`):** Broker xóa tin nhắn ngay khi vừa gửi đi. Nếu Worker gặp sự cố/sập nguồn giữa chừng thì **mất dữ liệu vĩnh viễn**.
- **At-least-once (`auto_ack=False` - Manual ACK):** Broker chỉ xóa tin nhắn sau khi nhận được `basic_ack`. Nếu Worker sập nguồn (như kịch bản "Giả lập Sập nguồn"), RabbitMQ **tự động Requeue tin nhắn** cho Worker khác làm lại.

### 2. Giải bài toán trùng lặp khi Requeue (Idempotent Consumer):
Khi một tin nhắn bị Requeue do Worker trước crash lúc đang thực hiện dở, có nguy cơ khách hàng bị **trừ tiền hoặc xuất kho 2 lần**. Trong kiến trúc SOA, Consumer bắt buộc phải áp dụng mẫu **Idempotent**:
1. **Redis Lock / Cache:** Gọi `SET order_id PENDING NX EX 300`. Nếu đã tồn tại key thì từ chối xử lý lại.
2. **Database Unique Constraint:** Đặt khoá `UNIQUE` cho trường `order_id` trong bảng giao dịch.
3. **State Machine:** Chỉ thực thi nếu trạng thái đơn hàng là `NEW/PENDING`. Nếu đã là `PROCESSED` thì bỏ qua và gửi ngay `ACK`.

---

## 📂 Cấu trúc thư mục

```
demo_soa_rabbitmq/
│
├── .vscode/
│   └── settings.json           # Trỏ interpreter về .venv (không dùng ${workspaceFolder})
├── docker-compose.yml          # Khởi chạy RabbitMQ (5672/15672) + Kafka KRaft (9092)
├── requirements.txt            # fastapi, uvicorn, pika, confluent-kafka, pydantic
│
├── ── PHẦN 1: Giới thiệu Code Kỹ thuật (Terminal + Swagger) ──────────
├── producer.py                 # FastAPI Swagger API (cổng 8001)
│                               #   → Gửi đồng thời vào cả RabbitMQ & Kafka
│                               #   → 4 kịch bản demo: async, priority, DLQ, scaling
├── worker.py                   # RabbitMQ Consumer thuần
│                               #   → basic_qos, basic_ack, basic_nack → DLQ
│                               #   → Đơn VIP nhảy hàng (Priority Queue)
├── worker2.py                  # Kafka Consumer thuần
│                               #   → Poll theo Offset (FIFO), bị kẹt đơn lỗi
│                               #   → Worker thừa in cảnh báo IDLE
│
├── ── PHẦN 2: Sản phẩm Web Dashboard (trình diễn chính) ─────────────
├── app.py                      # FastAPI Backend (cổng 8000) — kết nối RabbitMQ thật
│                               #   → Worker thread nội bộ: basic_get + ACK/NACK
│                               #   → API đầy đủ: đặt hàng, thêm trạm, crash, DLQ retry
├── index.html                  # Giao diện Web thời gian thực (TailwindCSS + Font Awesome)
│                               #   → 4 khu vực: Đặt hàng, Queue, Workers, Kafka đối chiếu
│
├── .venv/                      # Môi trường ảo Python (không commit vào Git)
└── README.md                   # Tài liệu này
```

---

## 🛠️ Yêu cầu môi trường

| Thành phần | Phiên bản |
|:---|:---|
| **Docker Desktop** | Bất kỳ (cần Docker Engine đang chạy) |
| **Python** | 3.10+ |
| **RAM** | ≥ 4GB (Docker cần ~2GB cho RabbitMQ + Kafka) |

---

## 🚀 Cài đặt từ A–Z

### Bước 1 — Khởi động Broker bằng Docker Compose

```powershell
# Tại thư mục dự án
docker compose up -d

# Kiểm tra 2 container đang chạy
docker ps
```

Bạn sẽ thấy:
- `demo-rabbitmq` — cổng `5672` (AMQP) và `15672` (Admin UI)
- `demo-kafka` — cổng `9092` (KRaft, không cần Zookeeper)

**RabbitMQ Admin UI:** [http://localhost:15672](http://localhost:15672)

---

### Bước 2 — Tạo môi trường ảo & cài thư viện

```powershell
# Tạo venv nếu chưa có
python -m venv .venv

# Kích hoạt
.\.venv\Scripts\Activate.ps1

# Cài đặt tất cả thư viện
pip install -r requirements.txt
```

> **Tip VS Code:** Nếu thấy gạch đỏ dưới `pika` hoặc `confluent_kafka`, nhấn `Ctrl+Shift+P` → `Python: Select Interpreter` → chọn `.venv\Scripts\python.exe`.

---

## ▶️ Cách 1 — Chạy Sản phẩm Web Dashboard (Khuyên dùng khi thuyết trình)

```powershell
.\.venv\Scripts\python.exe -m uvicorn app:app --reload --port 8000
```

Mở trình duyệt: **[http://localhost:8000](http://localhost:8000)**

### Kịch bản trình diễn 4 bước (≈ 5 phút):

| Bước | Thao tác | Quan sát |
|:---|:---|:---|
| **① Priority Queue** | Bấm **"Combo Flash Sale"** | 4 đơn thường vào trước, đơn 👑 VIP vào cuối nhưng **nhảy lên đầu Queue ngay lập tức** |
| **② Dead Letter Queue** | Bấm **"Bắn 1 Đơn Lỗi"** | Đơn bị `NACK` → vào ô **DLQ**, các đơn hợp lệ phía sau vẫn chạy bình thường |
| **③ Scale Worker** | Bấm **"Thêm Trạm Worker"** 2-3 lần | Các trạm chia việc song song, Queue giải phóng nhanh gấp đôi |
| **④ Fault Tolerance** | Bấm **"Giả lập Sập nguồn"** | Đơn đang xử lý dở bị thu hồi, tự động quay về Queue cho trạm khác làm tiếp |

> **Đồng thời quan sát** bảng **"Đối chiếu Kafka"** phía dưới: cùng 1 luồng đơn, Kafka phải xử lý tuần tự theo Offset và bị kẹt khi gặp đơn lỗi.

---

## 🔬 Cách 2 — Demo Code Kỹ thuật (Terminal Side-by-Side + Swagger)

Dành cho trình bày chi tiết nguyên lý code với `worker.py` & `worker2.py` song song trên Terminal.

> **Lưu ý:** `app.py` (cổng 8000) dùng queue riêng (`flashorder_*`) nên **bạn có thể bật cả hai cùng lúc** mà không bị xung đột.

#### Terminal 1 — Producer Swagger API (cổng 8001)

```powershell
.\.venv\Scripts\python.exe -m uvicorn producer:app --reload --port 8001
```

→ Mở Swagger UI: [http://localhost:8001/docs](http://localhost:8001/docs)

#### Terminal 2 — Worker RabbitMQ

```powershell
.\.venv\Scripts\python.exe worker.py Worker-1
```

#### Terminal 3 — Worker Kafka

```powershell
.\.venv\Scripts\python.exe worker2.py Kafka-Worker-1
```

Đặt **Terminal 2 & 3 cạnh nhau**, sau đó trên Swagger bấm từng kịch bản:

| Endpoint Swagger | Mục đích |
|:---|:---|
| `POST /0-reset-queues` | **Dọn sạch hàng đợi CLI** (`order_main_queue` & `dead_letter_queue`) để reset về 0 đơn |
| `POST /1-async-single-order` | Minh hoạ API phản hồi < 100ms dù Worker cần 3s xử lý |
| `POST /2-scenario-priority-queue` | **VIP-999 vào cuối #6/6 nhưng RabbitMQ bốc ra #1; Kafka bị ép làm #6** |
| `POST /3-scenario-dlq-isolation` | **RabbitMQ cô lập đơn lỗi sang DLQ; Kafka kẹt cứng toàn luồng** |
| `POST /4-scenario-free-scaling` | Mở thêm Worker: **RabbitMQ chia đều; Kafka thừa Worker ngồi chơi** |
| `GET /5-amqp-exchange-types` | Tra cứu lý thuyết & bảng so sánh 4 loại AMQP Exchange (Direct, Fanout, Topic, Headers) |
| `GET /6-reliability-and-idempotency` | Tra cứu lý thuyết At-least-once, rủi ro Requeue và mẫu thiết kế Idempotent Consumer |

#### (Tùy chọn) Scale thêm Worker cho kịch bản 4:

```powershell
# Terminal 4: thêm Worker RabbitMQ thứ 2
.\.venv\Scripts\python.exe worker.py Worker-2

# Terminal 5: thêm Worker Kafka thứ 2 (sẽ thấy cảnh báo IDLE vì chỉ có 1 Partition)
.\.venv\Scripts\python.exe worker2.py Kafka-Worker-2
```

---

## ❓ Câu hỏi thường gặp & Quan sát RabbitMQ Admin UI (localhost:15672)

### 1. Tại sao RabbitMQ Admin báo `Ready: 16` nhưng Web lại báo "Hàng đợi trống"?
> **Giải thích:** Dự án tách biệt 2 luồng Queue độc lập:
> - **Web Dashboard (`app.py` cổng 8000)**: Dùng `flashorder_main_queue` — Web báo trống vì đơn hàng đã được Worker xử lý xong.
> - **Kỹ thuật CLI (`producer.py` cổng 8001)**: Dùng `order_main_queue`. Khi bạn bấm các kịch bản test trên Swagger mà **chưa bật `worker.py`**, các tin nhắn sẽ nằm chờ sẵn trong `order_main_queue`.
> 👉 **Cách xử lý:** Bật Worker để tiêu thụ: `.\.venv\Scripts\python.exe worker.py Worker-1`, hoặc bấm `POST /0-reset-queues` trên Swagger để dọn sạch.

### 2. Terminal chạy `app.py` có bị ngập log `GET /api/state` không?
> **Không:** Đã tích hợp sẵn `EndpointFilter` chặn 100% access log polling ngầm của frontend. Terminal chỉ hiển thị sự kiện nghiệp vụ thật: nhận đơn, đơn VIP nhảy hàng, Worker xuất kho ACK, đơn lỗi NACK và sập nguồn requeue.

---

## 🛑 Dừng & Dọn dẹp

```powershell
# Nhấn Ctrl+C tại mỗi Terminal chạy Python

# Dừng container
docker compose down

# Xóa cả volume (dữ liệu RabbitMQ/Kafka sẽ mất)
docker compose down -v
```

---

## 🔧 Cấu hình kỹ thuật nhanh

| Thông số | Giá trị |
|:---|:---|
| RabbitMQ AMQP | `localhost:5672` |
| RabbitMQ Admin | `localhost:15672` |
| Kafka Bootstrap | `localhost:9092` |
| Queue chính (Web) | `flashorder_main_queue` (x-max-priority: 10) |
| DLQ (Web) | `flashorder_dlq` |
| Queue chính (CLI) | `order_main_queue` (x-max-priority: 10) |
| DLQ (CLI) | `dead_letter_queue` |
| Kafka Topic | `order_kafka_topic` (1 Partition) |

> Hai hệ thống Web (cổng 8000) và CLI (cổng 8001) dùng **queue tách biệt hoàn toàn**, bạn có thể chạy song song không lo xung đột.

---

## 📋 Kiểm tra cú pháp nhanh

```powershell
.\.venv\Scripts\python.exe -m py_compile producer.py worker.py worker2.py app.py
# Không có output = tất cả OK ✅
```

---

*Built for SOA / Microservices course demo — RabbitMQ 3.x + Apache Kafka 4.x (KRaft) + FastAPI 0.115+*
