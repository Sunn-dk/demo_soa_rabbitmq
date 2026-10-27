# Giải thích code theo từng phần demo Mộc Studio

Các vị trí dưới đây dùng tên hàm để tra cứu bền vững; số dòng là mốc của bản cửa hàng ngày 28/10/2026, có thể đổi sau khi sửa code. Lab v3 ở `/lab` có luồng khác. Mở file trong IDE, tìm tên hàm hoặc dùng `rg -n 'def checkout' shop_api.py`.

## 1. Bấm đặt hàng: HTTP 202 nằm ở đâu?

Trong `static/shop.js`, handler `checkout-form.onsubmit` gửi `POST /api/shop/checkout` bằng fetch và đọc `response.status` thật. `shop_api.py` dòng 120 khai báo `@router.post('/checkout',status_code=202)`; hàm `checkout()` gọi `store.create_order()` rồi trả mã đơn, số tiền và ý nghĩa kết quả.

`shop_store.create_order()` dòng 125 kiểm tra khách, gọi `quote()` để lấy giá từ catalogue server, tính tổng và phí giao hàng. `BEGIN IMMEDIATE` dòng 138 bảo vệ thao tác kiểm tra/ghi khóa idempotency khỏi request đồng thời. Trong cùng transaction, code ghi `orders` và gọi `enqueue()` ghi `tasks` + `outbox`.

**Vì sao viết vậy:** nếu lưu đơn thành công nhưng publish broker thất bại, đơn không bị bỏ quên vì outbox vẫn còn để gửi lại. Nếu trình duyệt retry cùng idempotency key và cùng nội dung, server trả lại cùng đơn; dùng key cũ với nội dung khác bị từ chối. HTTP 202 chỉ nói đã tiếp nhận bền vào DB, chưa nói PDF đã tạo hoặc broker đã nhận.

## 2. Producer là ai, message chứa gì?

`shop_api.dispatch()` dòng 28 chạy một vòng lặp riêng cho mỗi transport, đọc `pending_outbox()` đúng broker, publish rồi `store.published()` khi có xác nhận. Hai vòng độc lập để Kafka hỏng không giữ việc RabbitMQ và ngược lại.

`shop_store.enqueue()` dòng 111 tạo message gọn: `task_id`, `order_id`, `role`. Dữ liệu khách/sản phẩm đã snapshot trong DB, worker đọc theo mã đơn. Thí nghiệm lỗi cùng partition thêm `partition: 0`, chỉ để chọn vị trí Kafka rõ ràng khi minh họa chính sách giữ thứ tự.

`shop_broker.topology()` khai báo direct exchange, queue durable, binding theo vai trò `invoice`/`email`, DLX và DLQ. `rabbit_publish()` đặt persistent message và mandatory routing; dispatcher bật `confirm_delivery()`. Confirm là broker xác nhận chặng publisher, độc lập với ACK của consumer.

**Vì sao không nhét toàn bộ PDF vào message:** broker phân phối yêu cầu làm việc; file được tạo/lưu ở nơi chuyên giữ dữ liệu. Demo chung một SQLite/file volume cho dễ chạy trên máy học; dịch vụ tách máy thật cần DB/API/object storage phù hợp.

## 3. Consumer hóa đơn làm gì? Hóa đơn lưu ở đâu?

Nút **+ Thêm consumer** gọi `/api/shop/workers`; `shop_api.add_worker()` dòng 183 mở subprocess `shop_worker.py <name> <transport> <role>`. Đây là process riêng, không phải một thẻ HTML giả.

`shop_worker.rabbit()` đăng ký `basic_consume(auto_ack=False)` và `basic_qos(prefetch_count=1)`. Callback nhận payload, ghi `DELIVER`/`REDELIVER`, rồi gọi `shop_fulfillment.fulfill()`.

Với role invoice, `fulfill()` dòng 60 kiểm tra hóa đơn đã tồn tại chưa. Nếu có thì `DEDUP` và bỏ qua tạo thêm tác vụ email. Nếu chưa, `render_invoice()` dòng 29 dùng ReportLab tạo PDF: khách hàng, địa chỉ, COD, mã đơn, số hóa đơn, từng dòng hàng, giá, tiền hàng, phí giao hàng và tổng. Font DejaVu Sans nhúng để hiển thị tiếng Việt. File tạm được `os.replace()` sang file cuối.

**Lưu hai phần:** bản ghi và snapshot hóa đơn ở bảng `invoices` trong `data/shop.sqlite3`; file ở `data/invoices/<order_id>.pdf`. API `invoice_pdf()` dòng 140 trả file bằng FileResponse. Modal xem hóa đơn lấy snapshot; nút **Tải PDF** tải file thật, không chụp HTML thành ảnh.

`shop_store.commit_invoice()` dòng 204 dùng `order_id` làm PRIMARY KEY, thêm bản ghi invoice và outbox email trong cùng transaction. Nếu transaction rollback thì cả hai cùng rollback. PDF trên đĩa không phải một phần transaction SQLite; file dư có thể được ghi lại khi retry.

## 4. ACK là gì và gửi tại dòng nào?

ACK là acknowledgement: consumer báo đã xử lý xong lượt giao. Trong `shop_worker.py` dòng 39, `channel.basic_ack(method.delivery_tag)` chạy **sau** `fulfill()`. `delivery_tag` xác định lượt giao trên channel, khác mã đơn nghiệp vụ. `complete()` ghi `ACK_SENT` sau khi gọi ACK thành công phía client.

**Vì sao ACK sau xử lý:** ACK sớm rồi worker chết có thể khiến broker loại bỏ việc chưa làm. ACK muộn cho phép giao lại khi worker chết, nhưng có khả năng xử lý lặp; vì thế cần idempotency. `prefetch=1` là mỗi consumer chỉ giữ tối đa một delivery chưa ACK trong cấu hình này, không phải cả hệ thống chỉ xử lý một đơn.

## 5. Gửi email ở đâu? Có gửi Gmail không?

Tác vụ email được publish qua outbox sau commit hóa đơn. Role email cũng gọi `fulfill()` nhưng vào nhánh `elif role=='email'`. Nó kiểm tra sổ gửi `store.sent()`, xác thực email, đọc file PDF, tạo EmailMessage và `add_attachment(...,maintype='application',subtype='pdf')`.

`smtp.send_message(message)` ở `shop_fulfillment.py` dòng 103 gửi SMTP thật tới service Mailpit trong Compose. Sau SMTP nhận thư, `store.record_sent()` ghi bảng `deliveries` và event `SMTP_ACCEPTED`. Worker mới ACK/commit sau đó. Mailpit UI cổng 8025 cho xem người nhận, nội dung và file đính kèm.

**Vì sao dùng Mailpit:** mọi người clone và chạy được mà không xin password email hay API key, không gửi ra người thật. Cơ chế SMTP vẫn có thật. Đây chưa phải kiểm tra email đến hộp thư Internet. SMTP và SQLite không có transaction chung; crash sau SMTP nhưng trước `record_sent()` có thể gửi trùng. Stable Message-ID hỗ trợ đối chiếu, không tự bảo đảm exactly-once.

## 6. Giả lập thời gian nằm ở đâu?

Setting `observation_delay` trong SQLite được đổi bằng `/api/shop/settings`. Ở `shop_fulfillment.py` dòng 74 và 92, `pause(store.settings()['observation_delay'])` giãn trước tạo PDF/gửi SMTP. Rabbit dùng `conn.sleep` để Pika phục vụ kết nối trong khi chờ; Kafka dùng `time.sleep` ngắn, nhỏ hơn max.poll.interval.

Chọn **0s** để bỏ hoàn toàn giãn thời gian. PDF và SMTP vẫn chạy thật. Cả hai transport dùng cùng giá trị; đồng hồ trên trang có cả DB/dispatcher/worker/SMTP, không phải phép đo tốc độ riêng của broker.

## 7. REDELIVER và DEDUP xuất hiện thế nào?

Crash thủ công kill process khi task chưa ACK; RabbitMQ requeue delivery khi mất channel/connection. Consumer tiếp theo đọc cờ `method.redelivered`; `shop_store.received()` dòng 180 ghi `REDELIVER` và tăng lượt nhận.

Tình huống **Sập sau lưu hóa đơn** đặt `crash_after_commit` cho đơn. Trong `fulfill()`, sau commit invoice + email outbox, code ghi `CRASH_BEFORE_ACK` và gọi `os._exit(42)`. Consumer khác nhận lại cùng task, thấy invoice đã có nên ghi `DEDUP`, không tạo thêm invoice/outbox email, rồi ACK.

Kafka chưa commit offset thì record cũng có thể được đọc lại sau rebalance; demo ghi `REDELIVER` dựa trên lịch sử task đã được nhận, không giả vờ Kafka có cùng cờ AMQP redelivered.

## 8. Email sai: DLQ của RabbitMQ và partition pause của Kafka

Seed ghi email sai để mô phỏng dữ liệu nhập qua kênh quản trị/tích hợp khác; checkout công khai vẫn chặn email sai. Handler email thật ném BusinessError ở bước validation. Trong `shop_worker.rabbit()`, lỗi xử lý dẫn tới `basic_nack(requeue=False)` dòng 35; cấu hình DLX route message vào queue `.failed`. Consumer tiếp tục nhận tin tốt.

`shop_api.retry_task()` lấy đúng message trong Rabbit DLQ, publish lại với confirm rồi ACK bản DLQ. Các message DLQ đã xem nhưng không phải đích được requeue khi đóng kết nối. Giữ cùng task ID để dedup; có thể publish lặp nếu crash ở khoảng giữa.

Trong `shop_worker.kafka()`, lỗi khiến `consumer.pause()` dòng 117 trên đúng partition, chưa commit record đó. Đây là chính sách giữ thứ tự của app. Nút sửa/retry xử lý lại message giữ trong worker. Nút chuyển topic lỗi publish error envelope, chờ delivery rồi `consumer.commit()` dòng 97 và resume partition. Hai đơn tốt phía sau mới đi tiếp; các partition khác vẫn có thể hoạt động.

## 9. Vì sao thêm Kafka worker chưa chắc tăng mức song song?

`shop_broker.PARTITIONS=2`. `shop_worker.kafka()` đăng ký `on_assign`/`on_revoke`, lưu partition thực của từng consumer. Với classic group và bốn consumer cùng topic hai partition, chỉ hai consumer có assignment. Có thể tăng partition hoặc thiết kế xử lý song song; demo đang cho thấy phần thiết kế thêm so với work queue có consumer nhận từng việc độc lập.

`shop_api.analytics_replay()` dòng 285 tạo group mới đọc log retained, trả order_id/partition/offset, không gọi nghiệp vụ tạo PDF/email. Đây là điểm mạnh Kafka; không loại khỏi bài chỉ để RabbitMQ trông tốt hơn. Đọc thêm `SO_SANH_KAFKA.md`.

## 10. Đối chiếu kết quả cụ thể

- HTTP 202: Network → POST checkout; response có order_id/total và nghĩa “đơn + outbox đã lưu”.
- Hóa đơn: bảng invoices + file PDF; unique order_id bảo vệ việc tạo bản ghi trùng.
- Email: bảng deliveries + Mailpit thư có attachment; bảng DB một dòng không đủ chứng minh SMTP không từng gửi lặp.
- Broker: Ready/Unacked/DLQ lấy Management API, có độ trễ; Kafka assignment lấy callback consumer thật.
- Luồng cùng mã đơn: ORDER_SAVED → PUBLISH_CONFIRMED → DELIVER → PDF_RENDER → INVOICE_COMMIT → ACK_SENT → tác vụ email → SMTP_ACCEPTED → ACK_SENT. Hai chặng có thể xen kẽ trong log vì process chạy đồng thời.

Nguồn chính thức: [Work Queues](https://www.rabbitmq.com/tutorials/tutorial-two-python), [ACK/confirms](https://www.rabbitmq.com/docs/confirms), [DLX](https://www.rabbitmq.com/docs/dlx), [Kafka Design 4.2](https://kafka.apache.org/42/design/design/), [Mailpit](https://mailpit.axllent.org/docs/).
