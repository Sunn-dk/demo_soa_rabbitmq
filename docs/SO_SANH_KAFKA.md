# So sánh cho bài toán tạo hóa đơn và gửi email

Đối chiếu tài liệu chính thức ngày 28/10/2026. Demo cố định RabbitMQ 4.3.6 và Kafka 4.2.0; không khẳng định đây là bản mới nhất.

## Điều kiện chung và khác biệt có chủ ý

Hai transport cùng catalogue, giá server, SQLite, `shop_fulfillment.fulfill()`, PDF ReportLab và SMTP Mailpit; nhịp quan sát cùng một setting. Mỗi hệ chạy một broker trên cùng máy. RabbitMQ dùng classic work queues, Kafka dùng Consumer Groups truyền thống, mỗi process xử lý tuần tự bản ghi đã poll. Kafka có 2 partition/topic, RF=1; không dùng Share Groups. Đây là so sánh mô hình phục vụ bài toán, không phải benchmark năng lực cực đại.

| Bài toán | RabbitMQ đang chạy | Kafka đang chạy | Kết luận đúng phạm vi |
|---|---|---|---|
| Thêm 4 worker xử lý các hóa đơn độc lập | 4 consumer cùng queue, prefetch=1 | 4 consumer trong group, 2 partition → 2 consumer có assignment | RabbitMQ thêm mức song song mà không cần tăng partition cho bài toán này |
| Một email sai, hai email đúng phía sau | NACK riêng tin lỗi, DLX route tới DLQ; tin tốt tiếp tục | Cùng partition 0, chính sách giữ thứ tự pause ở lỗi, chưa commit | RabbitMQ có ACK/NACK và dead-letter routing từng tin; Kafka app cần chiến lược offset/retry phù hợp |
| Khôi phục lỗi Kafka | Không áp dụng | App publish `.email.dead`, đợi delivery, commit nguồn rồi resume | Kafka làm được DLQ; trong demo đây là code ứng dụng, không phải Kafka tự tạo DLQ |
| Sập sau lưu PDF trước xác nhận | Unacked được giao lại khi kết nối chết | Record chưa commit được group đọc lại sau đổi owner | Cả hai cần idempotency với hiệu ứng bên ngoài broker |
| Thêm ứng dụng phân tích sau khi đã xử lý | Tin trong classic queue đã ACK không còn để consumer mới đọc lại | Group analytics mới đọc lịch sử còn retention, không làm lại nghiệp vụ | Kafka log thuận tiện cho nhiều ứng dụng độc lập dùng lại lịch sử |

## Thí nghiệm 1: partition là đơn vị gán việc của classic group

`shop_broker.PARTITIONS=2`; mỗi vai trò có topic riêng. `shop_worker.kafka()` đăng ký callback `on_assign`, lưu assignment thực vào SQLite. UI đếm consumer có partition, không đếm process đang chạy thành năng lực xử lý. Nút chuẩn bị chờ 4 RabbitMQ consumer và 2 Kafka consumer có assignment trước khi báo sẵn sàng.

Có thể tăng partition để có 4 consumer hoạt động, hoặc lập lịch song song trong consumer; cần thiết kế xử lý offset khi hoàn tất lệch thứ tự và bảo vệ thứ tự theo key. Tăng partition cũng có ảnh hưởng phân bố key. Không diễn giải kết quả là Kafka chỉ chạy được hai worker hoặc RabbitMQ luôn nhanh hơn.

## Thí nghiệm 2: lỗi nghiệp vụ khác lỗi broker

Seed tạo email thiếu dấu @ để handler thật từ chối; không ném lỗi riêng chỉ bên Kafka. Cùng ba đơn được route partition 0 trong thí nghiệm để dễ nhìn thấy thứ tự. Kafka callback giữ message lỗi trong bộ nhớ và pause đúng partition đó; vẫn poll giữ group membership. Partition khác không bị pause bởi quy tắc này.

Ứng dụng có hai cách khôi phục: sửa địa chỉ rồi xử lý lại record chưa commit, hoặc publish error envelope vào topic `.dead`, đợi xác nhận rồi commit nguồn và resume. Crash giữa publish DLQ và commit có thể sinh bản lỗi lặp. Kafka transactions có thể giải quyết một số luồng Kafka→Kafka; không tự làm SQLite/SMTP thành transaction Kafka.

RabbitMQ khai báo DLX và binding tới `.failed`; `basic_nack(requeue=False)` kích hoạt routing lỗi. Với classic queue một node, đây không phải bảo đảm tuyệt đối chặng DLQ không mất tin khi đích lỗi. Có thể chọn quorum queues và chính sách phù hợp khi triển khai thật.

## Thí nghiệm 3: lợi thế log của Kafka

`POST /api/shop/replay` tạo group analytics ID mới, đọc topic hiện tại từ earliest còn retention và commit offset của group riêng. Chỉ đọc tối đa 100 record, không gọi `fulfill()`. Cửa hàng hiển thị bản ghi công việc (task), không giả vờ đó là hệ thống phân tích doanh thu đầy đủ. Tiền và hóa đơn trong DB không thay đổi vì đọc lại.

Muốn fanout RabbitMQ tới nhiều ứng dụng, có thể route bản sao tới các queue riêng ngay từ đầu. Muốn lưu log/replay theo mô hình RabbitMQ, xem Streams; không nói RabbitMQ nói chung không thể replay. Kafka Share Groups có chia sẻ record và xác nhận record, khác mô hình classic group của thí nghiệm.

## Kết luận dùng để thuyết trình

“Trong luồng gửi hóa đơn gồm các công việc độc lập, RabbitMQ cho cách phân phối, xác nhận và cô lập lỗi trực tiếp ở mức message. Kafka mạnh khi coi dữ liệu là log để nhiều ứng dụng đọc và đọc lại. Sự phù hợp phụ thuộc mô hình xử lý, thứ tự, lịch sử và vận hành; không kết luận từ tên sản phẩm hoặc vài giây trên một laptop.”

## Nguồn học chính thức

- [RabbitMQ Work Queues](https://www.rabbitmq.com/tutorials/tutorial-two-python): chia việc, manual ACK, persistence, prefetch.
- [RabbitMQ ACK và publisher confirms](https://www.rabbitmq.com/docs/confirms): hai chặng xác nhận độc lập, Unacked và requeue khi kết nối mất.
- [RabbitMQ Dead Letter Exchanges](https://www.rabbitmq.com/docs/dlx): điều kiện dead-letter, routing và giới hạn an toàn.
- [Apache Kafka 4.2 Introduction](https://kafka.apache.org/42/getting-started/introduction/): topic, partition, consumer groups và retention.
- [Apache Kafka 4.2 Design](https://kafka.apache.org/42/design/design/): assignment theo partition, delivery semantics, offset và Share Groups.
- [RabbitMQ Streams](https://www.rabbitmq.com/docs/streams): mô hình log/replay khác classic work queue.
- [Mailpit](https://mailpit.axllent.org/docs/): SMTP, giao diện xem thư, attachments và API kiểm thử.
