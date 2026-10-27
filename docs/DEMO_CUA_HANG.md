# Kịch bản Mộc Studio — 15–20 phút

Chạy `docker compose --profile kafka up -d --build --wait --wait-timeout 240`. Mở web cổng 8000 và Mailpit cổng 8025 trước giờ trình bày. Bắt đầu với 0 worker, chọn nhịp quan sát 4s. Nếu dùng dữ liệu cũ, dừng worker và reset cửa hàng trước. Không cần quay video.

## 0–3 phút: vấn đề thực tế và đặt hàng

**Nói:** “Một trang bán hàng cần ghi nhận đơn nhanh. Tạo PDF và gửi email là việc phía sau; nếu gộp hết vào HTTP request, khách phải chờ cả hai bước. Tôi sẽ đặt một đơn, rồi mở từng loại worker để thấy sự tách biệt.”

Thêm 2 áo thun, nhập thông tin ví dụ, đặt hàng. Tổng 498.000 + giao hàng 25.000 = **523.000 đ**. Trang chuyển sang chi tiết đơn. HTTP 202 xuất hiện trong thông báo; có thể mở Network → checkout để chỉ status thật.

**Nói:** “202 nghĩa là server đã lưu đơn và yêu cầu xử lý vào cùng transaction. Chưa có worker nên chưa có hóa đơn. Dispatcher gửi yêu cầu trong outbox tới broker và chờ confirm; khác consumer ACK sau xử lý.”

## 3–6 phút: tách hai consumer

Sang **Phòng vận hành → RabbitMQ**. Chỉ queue hóa đơn có Ready. Thêm một consumer **Tạo hóa đơn PDF**, xem mã đơn trên thẻ worker và Unacked nếu kịp chu kỳ cập nhật. Sau đó chỉ ra PDF đã có, queue email còn chờ. Mở chi tiết đơn và xem hóa đơn.

Thêm consumer **Gửi email kèm PDF**, mở Mailpit, chọn thư theo số hóa đơn, tải file đính kèm.

**Nói:** “Đây là file PDF được tạo từ từng dòng hàng, không phải chỉ đổi trạng thái trên giao diện. Email đi qua SMTP và có file đính kèm. Mailpit giữ thư trên máy để cả lớp chạy không cần tài khoản email. Consumer hóa đơn và consumer email có thể tăng/giảm độc lập.”

## 6–9 phút: backlog và thêm worker

Đặt nhịp 4s, bấm **Tạo 6 đơn hàng**. Khi worker đầu đang bận, thêm consumer hóa đơn thứ hai rồi thứ ba. Chỉ các mã đơn khác nhau đang xử lý ở các process/PID khác nhau. Mở payload/outbox để thấy `task_id`, `order_id`, `role`.

**Nói:** “Queue đệm công việc khi đầu vào lớn hơn năng lực xử lý. prefetch=1 giới hạn mỗi consumer giữ một tác vụ chưa ACK. Chúng ta tăng năng lực tạo PDF mà không đổi API đặt hàng. Bốn giây ở đây là nhịp quan sát có thể tắt, không phải RabbitMQ truyền một tin mất bốn giây.”

## 9–12 phút: tin lỗi và phục hồi

Bấm **Đơn lỗi email + 2 đơn tốt**. Chỉ PDF của cả ba đã tạo, một tác vụ email sai ở DLQ, hai đơn tốt nhận thư. Sửa email và retry, mở thư vừa được gửi.

Bấm **Sập sau lưu hóa đơn** khi có từ 2 consumer hóa đơn; theo mã đơn trong log: `INVOICE_COMMIT`, `CRASH_BEFORE_ACK`, `REDELIVER`, `DEDUP`, `ACK_SENT`. Nếu chỉ có một consumer thì thêm consumer mới sau crash.

**Nói:** “ACK là consumer báo đã xử lý xong lượt giao. Consumer có thể đã lưu hóa đơn nhưng chết trước ACK nên broker giao lại. Mã đơn duy nhất và transaction giúp không tạo thêm hóa đơn. Đây không phải lời hứa exactly-once cho mọi thứ; SMTP và DB vẫn có một khoảng lỗi có thể làm gửi thư lặp.”

## 12–17 phút: Kafka cùng nghiệp vụ

1. Mở **RabbitMQ & Kafka**, bấm **Chuẩn bị 4 worker hóa đơn mỗi bên** và đợi thông báo sẵn sàng; bấm **Nạp 6 đơn thật mỗi bên**. Quan sát assignment: RabbitMQ có 4 consumer trên queue, Kafka có 4 process nhưng chỉ 2 process được giao partition.
2. Bấm **Chạy tình huống lỗi trên cả hai broker**. RabbitMQ gửi được hai thư tốt; Kafka giữ record email sai ở partition 0 nên hai record sau trong partition đó chưa gửi.
3. Vào phòng vận hành, chọn Kafka; bấm **Chuyển sang topic lỗi**. Chỉ `KAFKA_APP_DLQ`, `OFFSET_COMMIT` của record tiếp theo, hai email tốt xuất hiện. Có thể sửa và retry đơn đã chuyển lỗi.
4. Quay lại so sánh, bấm **Đọc lại bằng group analytics mới**. Chỉ group ID, partition, offset; tổng PDF/email không tăng.

**Nói:** “Với công việc gửi hóa đơn độc lập, RabbitMQ cho cơ chế ACK/NACK từng việc và DLX sẵn trong broker, thêm consumer trên queue thuận tiện. Kafka classic group gắn mức song song với partition. Ứng dụng này giữ thứ tự nên phải quyết định retry hoặc DLQ khi record lỗi. Kafka vẫn làm được; tăng partition hoặc xây retry/DLQ là phần thiết kế thêm. Khi cần lịch sử để ứng dụng khác đọc lại, Kafka lại thuận tiện.”

Không gọi đây là benchmark tốc độ. Hai bên dùng cùng code PDF/SMTP, số partition đã ghi rõ. Kafka Share Groups và RabbitMQ Streams là các mô hình khác ngoài phần đang chạy.

## 17–20 phút: chốt ý và để lớp thử

Chỉ README trên GitHub, lệnh clone và một lệnh Compose. Mỗi người chạy trên máy mình. Mời người nghe dự đoán: nếu tắt consumer email thì API đặt hàng, PDF và thư thay đổi thế nào? Sau đó thử ngay trên giao diện.

Nếu chỉ có 15 phút, rút phần chia việc còn 1 phút và giữ một tình huống crash; không bỏ bước mở email và file PDF vì đó là kết quả nghiệp vụ cụ thể.
