# Kiểm tra trên broker thật

Chỉ chạy với dữ liệu thực hành có thể reset của bộ seminar. Test sẽ dừng worker và dọn queue/SQLite demo. Không dùng với dữ liệu cần giữ.

Khởi động cả RabbitMQ, web và Kafka trước:

```powershell
docker compose --profile kafka up -d --build --wait --wait-timeout 240
```

Từ thư mục gốc dự án, dùng Python 3.12+ (hai test chỉ dùng thư viện chuẩn):

```powershell
python tests/integration.py
python tests/durability.py
```

`integration.py` xác minh async khi 0 worker, idempotency, hai worker chia việc và hoàn tất khác thứ tự, kill worker thật, crash sau COMMIT trước ACK, DLQ/retry, reset guard và Kafka replay.

`durability.py` restart web, dừng/khởi động lại RabbitMQ, kiểm tra job còn tồn tại và HTTP 503 khi broker vắng mặt. Đây là kiểm tra restart của một broker, không phải kiểm tra mất node/đĩa hay HA.

Test ghi kết quả vào `test-results/`, tự tạo thư mục này và không phụ thuộc file tạm ngoài repo. Có thể đặt `SEMINAR_TEST_URL` nếu web chạy cổng khác, ví dụ `http://127.0.0.1:18000`. Khi test một Compose project riêng, đặt cùng các biến `COMPOSE_PROJECT_NAME`, `WEB_PORT`, `AMQP_PORT`, `MANAGEMENT_PORT`, `KAFKA_PORT` cho các lệnh Compose và tiến trình test.
