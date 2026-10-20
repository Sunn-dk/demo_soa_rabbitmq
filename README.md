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

### Tại sao chọn RabbitMQ cho bài toán Order Fulfillment?

| Tiêu chí | 🐇 RabbitMQ | 🐘 Apache Kafka |
|:---|:---|:---|
| **Bản chất** | Smart Broker / Dumb Consumer | Dumb Broker / Smart Consumer (Log) |
| **Priority Queue** | ✅ `x-max-priority`: đơn VIP tự nhảy lên đầu | ❌ Append-only, bắt buộc FIFO theo Offset |
| **Xử lý lỗi (Poison Pill)** | ✅ `basic_nack(requeue=False)` → DLQ tự động | ❌ Kẹt cứng tại Offset lỗi (Head-of-Line Blocking) |
| **Scale Worker tự do** | ✅ Thêm bao nhiêu Consumer cũng được (Fair Dispatch) | ⚠️ Tối đa = số Partition (bị khóa cứng) |
| **At-least-once** | ✅ ACK / NACK từng message | ✅ Commit Offset (nhưng toàn batch) |
| **Use case phù hợp** | Task Queue, Order Processing, RPC | Event Streaming, Log Aggregation, Analytics |

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
