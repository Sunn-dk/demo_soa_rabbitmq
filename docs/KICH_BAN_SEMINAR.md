# Kịch bản trình bày 15–20 phút

Mục tiêu: cả lớp giải thích được một message cụ thể đi đâu, ai tạo việc, ai xử lý, vì sao bất đồng bộ và cơ chế nào xử lý sự cố. Trình bày theo câu chuyện **tạo hóa đơn chậm**. Không đọc lần lượt các định nghĩa.

## Chuẩn bị trước giờ học

- Chạy `docker compose up -d --build` để tải image trước. Nếu demo Kafka, chạy thêm `docker compose --profile kafka up -d`.
- Mở dashboard, thử gửi một job, kiểm tra worker. Dừng mọi worker và reset để trở về 0.
- Mở PowerPoint, dashboard và Management ở ba tab/cửa sổ. Có thể mở sẵn `broker.py`, `worker.py`, `store.py` để trả lời câu hỏi code.
- Gửi link GitHub và hướng dẫn clone cho mọi người. Mỗi bạn mở Docker Desktop rồi chạy bản riêng.
- Điền tên nhóm/thành viên vào trang bìa nếu môn học yêu cầu. Bổ sung thông tin nhóm theo yêu cầu môn học.

## Nhịp trình bày khoảng 18 phút

Phần này là kịch bản demo độc lập với PPTX lý thuyết. Có thể dành 6-7 phút giải thích sơ đồ và 10-12 phút thao tác, sau đó để lớp làm lại.

| Khoảng thời gian demo | Nội dung |
|---|---|
| 0-2 phút | 0 worker, gửi job, xem message và HTTP 202, bật worker |
| 2-4 phút | Chia việc và thứ tự: gửi tác vụ 6 giây rồi 1 giây |
| 4-6 phút | Crash trước ACK và quan sát giao lại |
| 6-8 phút | Crash sau COMMIT trước ACK, xem DEDUP |
| 8-9 phút | Tin lỗi vào DLQ, sửa rồi gửi lại |
| 9-11 phút | Kafka replay bằng hai group |
| Phần còn lại | Người nghe làm lại một tình huống và hỏi đáp |

## Lời dẫn và kết luận cho từng demo

### 1. HTTP trả về nhưng việc đã xong chưa?

Nói: “Tôi tắt hết worker. API còn nhận được việc không?”

Giữ 0 worker. Bấm gửi 1 job 3 giây. Chỉ HTTP 202, ID vừa sinh, Ready tăng sau lấy mẫu và hóa đơn bằng 0. Bấm Job ID, đối chiếu `message_id` với `body.id`, giải thích JSON được mã hóa UTF-8 thành bytes. Bấm thêm worker, chỉ DELIVER, COMMIT, ACK_SENT.

Kết luận: producer không phải chờ consumer hoàn tất. HTTP 202 chỉ báo nhận việc, khác với HTTP 200 của đối chứng đồng bộ. Giao tiếp giữa dịch vụ bất đồng bộ dù Pika dùng `BlockingConnection` bên trong một process.

### 2. Thêm consumer thay đổi điều gì?

Gửi 6 job khi có một worker, sau đó thêm worker thứ hai khi còn backlog. Chỉ hai job processing có worker khác nhau. `prefetch_count=1` giới hạn tin chưa ACK mỗi consumer, không giới hạn số tin trong queue.

Kết luận: nhiều consumer chia việc. Không tuyên bố tốc độ luôn tăng chính xác gấp đôi. CPU, DB, broker và số job còn lại có thể thành nút thắt.

### 3. FIFO có nghĩa mọi việc hoàn tất đúng thứ tự không?

Đợi queue trống và cả hai worker rảnh. Bấm “Thứ tự: gửi 6s rồi 1s”. Mở log: job B gửi sau nhưng có thể ACK trước job A.

Kết luận: cần phân biệt **enqueue order**, **delivery order**, **completion order**. Queue không hiểu “lấy gạo trước, nấu cơm sau” hay “tạo hóa đơn trước, phát hành sau”. Nếu B phụ thuộc A, ứng dụng phải chỉ gửi B sau khi A thành công, hoặc dùng workflow/state machine với quy tắc thứ tự thích hợp. Một worker xử lý tuần tự giúp đơn giản hóa một số tình huống, nhưng retry/requeue và nhiều producer vẫn đòi hỏi thiết kế cẩn thận.

### 4. Tin đang làm có mất khi worker sập?

Đặt 6 giây, gửi 1 job. Chờ log DELIVER, xác định đúng worker đang giữ ID đó rồi bấm Dừng/crash. Thêm worker mới nếu không còn worker sống. Tìm REDELIVER, cùng ID, số lượt nhận tăng.

Kết luận: khi phát hiện connection/channel mất trước ACK, broker requeue tin chưa ACK. Có thể cần thời gian phát hiện lỗi. Không chứng minh mọi loại lỗi đều không mất dữ liệu bằng một thí nghiệm này.

### 5. Đã tạo hóa đơn rồi còn có thể nhận lại không?

Giữ một worker, bấm “Sập sau COMMIT, trước ACK”. Worker ghi hóa đơn rồi thoát thật. Bấm thêm worker. Chỉ COMMIT, CRASH_BEFORE_ACK, REDELIVER, DEDUP. Số lượt nhận tăng nhưng một `job_id` chỉ có một dòng hóa đơn.

Kết luận: manual ACK cho phép giao lại, nên consumer cần idempotency. Hiệu ứng demo là INSERT hóa đơn vào SQLite với PRIMARY KEY(job_id). Khóa và hiệu ứng nằm cùng transaction. Nếu tác vụ là thanh toán qua API ngoài, transaction SQLite này không tự bảo vệ thanh toán đó.

### 6. Tin lỗi có làm dừng toàn bộ queue?

Gửi job lỗi và một job thường. NACK với `requeue=False` đưa tin lỗi qua DLX đến DLQ đã cấu hình; job tốt vẫn hoàn tất. Bấm “Sửa & gửi lại DLQ”. Giữ ID cũ, sửa cờ lỗi và publish confirm trước khi ACK bản trong DLQ.

Kết luận: DLQ là nơi quan sát và xử lý tin không thực hiện được. Không retry vô hạn. Kafka cũng có thể thiết kế retry/error topics, nên không kết luận Kafka luôn kẹt khi gặp một tin lỗi.

### 7. Kafka khác ở vòng đời dữ liệu như thế nào?

Bấm thí nghiệm Kafka. Chờ kết quả. Đối chiếu sáu `event_id`, `partition`, `offset` trong hai group. Group A đã commit nhưng B mới vẫn đọc lại được.

Kết luận: trong lab này, consumer commit vị trí đọc; retention giữ log. Bắt đầu lại **cùng group** với `auto.offset.reset=earliest` không tự bỏ committed offset. Dùng group mới là cách lab chứng minh một ứng dụng khác vẫn đọc được lịch sử còn lưu.

## Câu hỏi nên chuẩn bị

1. Producer nhận confirm rồi consumer có chắc xử lý xong? Chưa, đây là hai chặng độc lập.
2. ACK có tạo ra exactly-once nghiệp vụ? Không. Crash sau hiệu ứng và trước ACK vẫn có thể giao lại.
3. Giao bất đồng bộ có buộc HTTP implementation phải dùng async/await? Không, đó là hai tầng khái niệm khác nhau.
4. Tại sao hai service khác chức năng không dùng chung một queue để đều nhận sự kiện? Vì consumer cùng queue chia việc; muốn mỗi service có một bản, dùng queue riêng và binding phù hợp.
5. RabbitMQ có replay không? Classic work queue thường bỏ tin sau ACK; RabbitMQ Streams có replay.
6. Kafka chỉ cho một consumer trên một partition? Đây là quy tắc của Consumer Groups truyền thống. Share Groups khác và cho chia sẻ record.
7. `delivery_mode=2` đủ đảm bảo không mất tin? Cần xét cả queue durable, confirm, loại queue, replication và failure model.
8. Vì sao web ghi ACK_SENT thay vì “broker đã nhận ACK”? Gửi ACK từ consumer không phải một giao thức xác nhận ngược rằng broker đã áp dụng ACK. Đối chiếu thêm số đếm broker.
9. Tại sao Ready trên web có thể cập nhật chậm? Management API dùng thống kê lấy mẫu. Bảng job là projection của ứng dụng, không thay thế broker metrics.
10. Vì sao không giữ benchmark “RabbitMQ vài ms, Kafka chậm”? Chưa có benchmark kiểm soát payload, persistence, replication, batch, phần cứng và cách đo để kết luận như vậy.
