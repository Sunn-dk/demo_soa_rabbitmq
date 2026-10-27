# Tài liệu RabbitMQ và Kafka từ nguồn chính thức

Đối chiếu ngày **27/10/2026**. Nguồn chính là tài liệu của dự án RabbitMQ, Apache Kafka và Pika. Link `/docs/` có thể chuyển sang bản mới theo thời gian; khi học sâu hãy chọn đúng phiên bản của broker. Lab cố định image RabbitMQ **4.3.6** và Kafka **4.2.0** để thực hành lặp lại; không khẳng định đây là phiên bản mới nhất.

## Học theo thứ tự này

| Mã | Tài liệu | Cần hiểu / bài tập |
|---|---|---|
| R0 | [RabbitMQ Hello World, Python](https://www.rabbitmq.com/tutorials/tutorial-one-python) | Gửi và nhận một message. Xác định process producer và consumer. |
| R1 | [RabbitMQ Work Queues](https://www.rabbitmq.com/tutorials/tutorial-two-python) | Manual ACK, persistence, prefetch. Tắt consumer, gửi vài tin rồi bật hai consumer. |
| R2 | [RabbitMQ Publishers](https://www.rabbitmq.com/docs/publishers) | Body/properties, routing key, mandatory publish, trường hợp không route được. |
| R3 | [Consumer Acknowledgements and Publisher Confirms](https://www.rabbitmq.com/docs/confirms) | Phân biệt hai chặng xác nhận, Unacked, tự requeue khi connection mất. |
| R4 | [RabbitMQ Routing, Python](https://www.rabbitmq.com/tutorials/tutorial-four-python) | Direct exchange, binding key khớp routing key. Thử key không có binding. |
| R5 | [RabbitMQ Reliability Guide](https://www.rabbitmq.com/docs/reliability) | Trách nhiệm producer/broker/consumer, failure, redelivery và idempotency. |
| R6 | [Dead Letter Exchanges](https://www.rabbitmq.com/docs/dlx) | Điều kiện dead-letter, cách route đến DLQ và giới hạn an toàn. |
| R7 | [RabbitMQ Publish/Subscribe, Python](https://www.rabbitmq.com/tutorials/tutorial-three-python) | Fanout đến hai queue, khác hai consumer cùng một queue. |
| R8 | [RabbitMQ Streams and Superstreams](https://www.rabbitmq.com/docs/streams) | Lưu log, đọc lại, khác work queue xóa sau ACK. |
| R9 | [RabbitMQ Queues: Message Ordering](https://www.rabbitmq.com/docs/queues#message-ordering) | Thứ tự enqueue/delivery và tác động của consumer song song, priority, requeue. |
| K1 | [Apache Kafka Introduction 4.2](https://kafka.apache.org/42/getting-started/introduction/) | Topic, partition, consumer group, retention và nhiều ứng dụng đọc. |
| K2 | [Apache Kafka Design 4.2](https://kafka.apache.org/42/design/design/) | Delivery semantics, log, offset, Share Consumer/Share Groups. |
| K3 | [Apache Kafka Quickstart 4.2](https://kafka.apache.org/42/getting-started/quickstart/) | Tạo topic, produce, consume, đọc dữ liệu từ đầu. |
| P1 | [Pika FAQ: thread safety](https://pika.github.io/pika/latest/faq/) | Không chia sẻ tùy ý một BlockingConnection qua nhiều thread. |

### Đọc thêm khi triển khai thật

- [RabbitMQ Quorum Queues](https://www.rabbitmq.com/docs/quorum-queues): replication và giới hạn của từng loại queue.
- [RabbitMQ Consumer Prefetch](https://www.rabbitmq.com/docs/consumer-prefetch): phạm vi prefetch, ứng xử khi có nhiều consumer.
- [RabbitMQ Release Information](https://www.rabbitmq.com/release-information): phiên bản và vòng đời hỗ trợ, thay vì chọn image `latest` mà không biết đã đổi bản nào.
- [Docker Get Docker](https://docs.docker.com/get-started/introduction/get-docker/): chuẩn bị môi trường cho lớp.

## Trích dẫn ngắn để dùng trong seminar

Các diễn giải sau là **tóm lược bằng tiếng Việt**, không phải trích nguyên văn:

- **R1:** Work queue tách việc nhận tác vụ khỏi việc xử lý tốn thời gian. Nhiều worker có thể cùng nhận từ một queue.
- **R3:** Publisher confirm nói về chặng gửi tới broker. Consumer ACK nói về một lượt giao tới consumer. Hai cơ chế độc lập.
- **R5:** Giao lại có thể xảy ra khi lỗi kết nối, nên consumer phải xử lý an toàn khi gặp cùng tác vụ lần nữa.
- **R6:** `basic_nack(requeue=False)` có thể dẫn đến dead-letter nếu queue đã cấu hình DLX. DLQ không tự xuất hiện chỉ vì có lỗi.
- **R9:** Các cơ chế như giao lại và nhiều consumer khiến thứ tự quan sát/xử lý phức tạp hơn FIFO đơn giản.
- **K1/K2:** Kafka giữ bản ghi theo log và chính sách retention; committed offset là vị trí đọc của group.

Mỗi slide đã có mã nguồn tham chiếu và URL trong speaker notes. Khi nộp bài, có thể dùng danh mục này làm trang tài liệu tham khảo.

## Những điều dễ học sai từ demo cũ

| Cách nói dễ sai | Cách nói chính xác hơn |
|---|---|
| Queue làm nghiệp vụ chạy nhanh hơn | Queue tách thời điểm, đệm backlog và hỗ trợ phân phối việc; tốc độ xử lý còn phụ thuộc consumer và tài nguyên |
| Bất đồng bộ nghĩa là mọi job hoàn tất theo thứ tự | Async mô tả sự độc lập thời điểm chờ; completion order phụ thuộc xử lý và điều phối |
| Có ACK thì không thể trùng | Consumer có thể đã tạo hiệu ứng rồi sập trước ACK; cần idempotency |
| Redis SETNX trước xử lý đủ chống mọi lỗi | Cần xét crash window và atomicity với hiệu ứng nghiệp vụ; khóa đơn thuần có thể khiến việc chưa làm bị coi là đã làm |
| Durable/persistent đồng nghĩa tuyệt đối không mất tin | Cần xét confirms, replication, node failure và loại queue |
| Kafka gặp tin lỗi thì toàn hệ thống bắt buộc kẹt | Đây có thể là chính sách retry của ứng dụng; có thể dùng error/DLQ topics và các chiến lược khác |
| Kafka không thể có nhiều consumer hơn partition | Giới hạn gán partition đang nói tới Consumer Groups truyền thống; Share Groups có mô hình khác |
| RabbitMQ không có replay | Phải nêu rõ classic work queue; RabbitMQ Streams hỗ trợ log/replay |
| Đã commit thì Kafka xóa message | Commit cập nhật offset, dữ liệu còn phụ thuộc retention/compaction |
| RabbitMQ luôn nhanh hơn Kafka / Kafka luôn đạt triệu tin/s | Thiếu ngữ cảnh cấu hình và phép đo kiểm soát, không dùng làm kết luận seminar |

## Bài tập tự kiểm tra

1. Vẽ lại message của một job cụ thể: body bytes, properties, routing key và queue đích.
2. Không bật worker, gửi ba tin. Điều gì tăng? HTTP trả ý nghĩa gì?
3. Bật hai worker, gửi tác vụ 6 giây rồi 1 giây. Phân biệt thứ tự gửi, giao và hoàn tất.
4. Kill worker sau DELIVER. Broker xử lý tin chưa ACK ra sao?
5. Kill sau DB COMMIT trước ACK. Vì sao một hóa đơn vẫn có hai lượt nhận?
6. Cho Email và Analytics đều nhận một sự kiện: cần mấy queue? Khác thêm worker như thế nào?
7. Trong Kafka, dùng group B mới và group A đã commit có điểm bắt đầu khác nhau thế nào?
8. Nếu DB ghi thành công nhưng publish thất bại, đoạn demo này chưa giải quyết đầy đủ vấn đề nào? Tìm hiểu transactional outbox.
