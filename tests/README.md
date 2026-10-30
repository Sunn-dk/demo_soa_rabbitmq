# Kiểm thử cửa hàng và lab

## Cửa hàng Mộc Studio

`shop_unit.py` chạy với DB/file tạm, không cần broker. Dùng môi trường có requirements và thêm pypdf để kiểm tra nội dung PDF:

```powershell
python -m pip install -r requirements.txt pypdf
python tests/shop_unit.py
```

Sáu kiểm tra: giá server/phí giao hàng, request đồng thời cùng idempotency key, invoice + email outbox atomic khi rollback, nội dung PDF tiếng Việt, backlog một broker không giữ dispatcher broker khác, assignment Kafka không biến mất khi worker chuyển trạng thái làm việc.

`shop_integration.py` dùng HTTP, RabbitMQ, Kafka và Mailpit thật; chạy trong **bản clone/Compose project thử nghiệm trống**, không có worker cửa hàng đang sống. Script tạo đơn và dừng/khởi động RabbitMQ của project đang chọn:

```powershell
docker compose --profile kafka up -d --build --wait --wait-timeout 240
python tests/shop_integration.py
```

Nó kiểm tra 202/outbox khi 0 worker, giá server và idempotency, hai bước PDF/SMTP có attachment, duplicate task, assignment 4 worker/2 partition, cùng 12 đơn ở hai broker, lỗi email/DLQ và retry, crash sau commit trước ACK ở cả hai broker, analytics không sinh side effect, checkout vẫn lưu khi RabbitMQ vắng rồi hoàn tất khi broker trở lại. Test này chỉ cần thư viện chuẩn của Python trên máy chạy lệnh.

Nếu dùng cổng riêng, đặt `SHOP_TEST_URL`, `SHOP_TEST_MAIL`, cùng COMPOSE_PROJECT_NAME và toàn bộ biến cổng giống lệnh Compose. Kết quả PDF nằm ở `test-results/`.

`shop_ui.js` chạy qua playwright-cli sau integration (có consumer hóa đơn/email hoạt động). Mở trình duyệt vào URL cửa hàng trước:

```powershell
playwright-cli -s=shop open http://127.0.0.1:8000 --browser=msedge
playwright-cli -s=shop run-code --filename=tests/shop_ui.js
```

Kiểm tra đặt hàng bằng form thật, HTTP 202, đúng hóa đơn của khách vừa đặt, modal, bốn tab và không tràn ngang ở màn hình 390px. Nếu dùng browser khác, thay tùy chọn tương ứng. Không quay video.

Sau đó chạy `playwright-cli -s=shop run-code --filename=tests/shop_comparison_ui.js` để kiểm tra nút chuẩn bị worker, nạp cùng lô đơn, tình huống lỗi ở hai broker, chuyển topic lỗi cho Kafka và đọc lại bằng group analytics trên giao diện thật.


`python tests/shop_practical.py` chạy sau integration: bỏ nhịp quan sát (0s), đặt đơn nhiều dòng, kiểm tra tổng/phí giao hàng, so sánh SHA-256 PDF tải từ API với PDF lấy từ MIME email SMTP; kiểm tra dữ liệu sai và route lab cũ đã bỏ.
