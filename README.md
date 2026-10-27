# Mộc Studio — cửa hàng thực hành RabbitMQ

Demo seminar 15–20 phút: khách chọn quần áo, đặt đơn COD, nhận hóa đơn PDF chi tiết qua email. Hai loại consumer **tạo hóa đơn** và **gửi email** chạy bằng tiến trình thật. Phòng vận hành hiển thị queue, worker, ACK, redelivery, DLQ và payload. Kafka thực hiện **cùng hàm nghiệp vụ** để so sánh cách chia việc, xử lý lỗi và đọc lại log.

## Clone và chạy

Cài [Git](https://git-scm.com/downloads) và [Docker Desktop](https://docs.docker.com/get-started/introduction/get-docker/), mở Docker và đợi Engine sẵn sàng. Windows dùng Linux containers/WSL 2. Không cần cài Python hoặc Node nếu chạy bằng Docker.

```powershell
git clone https://github.com/Sunn-dk/demo_soa_rabbitmq.git
cd demo_soa_rabbitmq
docker compose --profile kafka up -d --build --wait --wait-timeout 240
```

| Địa chỉ | Chức năng |
|---|---|
| http://localhost:8000 | Cửa hàng → Đơn hàng → Phòng vận hành → RabbitMQ & Kafka |
| http://localhost:8025 | Mailpit: email SMTP và PDF đính kèm, giữ trong máy thực hành |
| http://localhost:15672 | RabbitMQ Management, tài khoản demo `guest` / `guest` |
| http://localhost:8000/docs | API, nhóm **Cửa hàng thực tế** |
| http://localhost:8000/lab | Lab v3 trước đây để tham khảo |

Windows có thể bấm `start-demo.cmd`. Nếu chỉ học RabbitMQ, dùng `docker compose up -d --build --wait --wait-timeout 240`; phần Kafka cần profile riêng.

## Thử ngay theo đúng luồng

1. **Cửa hàng:** thêm sản phẩm, chọn kích cỡ, nhập tên/email/địa chỉ và đặt hàng. Dùng email ví dụ `minhanh@example.test`. Server tự tính giá từ catalogue.
2. Ban đầu **0 worker**. Đơn đã lưu, HTTP **202**, nhưng chưa có hóa đơn. Tại **Phòng vận hành**, chọn RabbitMQ: outbox được publish rồi Ready tăng. HTTP 202 ở cửa hàng nghĩa là **đơn + outbox đã commit**, chưa hứa hoàn thành hoặc broker đã nhận.
3. Bấm **+ Thêm consumer** ở **Tạo hóa đơn PDF**. Xem Ready → Unacked, worker nhận mã đơn, `PDF_RENDER`, `INVOICE_COMMIT`, `ACK_SENT`. Đơn có PDF thật; email vẫn chờ vì chưa có consumer gửi email.
4. Thêm consumer ở **Gửi email kèm PDF**. Mở **Hộp thư thực hành**, đọc thư và tải PDF. Không cần Gmail, API key hoặc mật khẩu SMTP ngoài.
5. Tạo 6 đơn và thêm worker để thấy chia việc. Đặt **Nhịp quan sát 4s** nếu cần nhìn rõ; chọn **0s** để bỏ khoảng giãn. Khoảng giãn áp dụng giống nhau cho hai broker, không phải thời gian truyền tin.

Đây là cửa hàng cục bộ cho seminar: không thu tiền, không có giao vận thật; PDF là chứng từ minh họa, không phải hóa đơn điện tử thuế. Mailpit nhận thư qua SMTP nhưng không tự chuyển ra hộp thư Internet.

## Các tình huống minh chứng

| Tình huống | Thao tác / bằng chứng |
|---|---|
| Bất đồng bộ | Đặt hàng khi 0 worker: đơn đã lưu, queue có tin, chưa có PDF/email; thêm từng loại consumer để tiếp tục |
| Producer–consumer / routing | Dispatcher gửi `invoice` qua exchange `seminar.shop.v4.tasks`; worker hóa đơn tạo tác vụ `email` qua outbox; hai queue, hai vai trò |
| Chia việc | Nhịp 4s, tạo 6 đơn, thêm 2–4 consumer hóa đơn: thẻ worker và log có mã đơn khác nhau |
| Worker chết | Crash khi đang tạo PDF: worker khác nhận lại cùng task, log `REDELIVER`; không cần đặt lại đơn |
| Sập sau COMMIT trước ACK | Bấm **Sập sau lưu hóa đơn**, có ít nhất 2 consumer hóa đơn hoặc thêm consumer mới sau crash: `INVOICE_COMMIT → CRASH_BEFORE_ACK → REDELIVER → DEDUP → ACK_SENT` |
| Sai email | **Đơn lỗi email + 2 đơn tốt**: PDF vẫn được tạo, tác vụ email sai vào DLQ; hai email hợp lệ tiếp tục gửi |
| Sửa lỗi | Nhập email hợp lệ tại **DLQ & tác vụ bị chặn** rồi **Sửa email & thử lại**; giữ nguyên mã đơn/task |
| Broker vắng mặt | Đơn và outbox vẫn lưu cùng transaction; dispatcher thử lại, chỉ đánh dấu published khi nhận confirm |

Ready/Unacked/DLQ lấy từ Management API, có độ trễ lấy mẫu. Trạng thái worker và log là quan sát ứng dụng. `ACK_SENT` nghĩa là ứng dụng đã gửi ACK, không phải một biên nhận ACK ngược từ broker.

## Kafka: cùng công việc, khác cách chia việc

Ở tab **RabbitMQ & Kafka**, chạy lần lượt:

1. **Chuẩn bị 4 worker hóa đơn mỗi bên**, chờ sẵn sàng, rồi **Nạp 6 đơn thật mỗi bên**. RabbitMQ có 4 consumer trên một queue; Kafka classic group có topic 2 partition nên chỉ 2 consumer có assignment. Thẻ worker hiển thị assignment thực, không gán số giả trên giao diện.
2. **Chạy tình huống lỗi trên cả hai broker**: một email sai rồi hai email đúng. Bên Kafka cố ý đặt cùng partition 0 và dùng chính sách giữ thứ tự; partition đó pause ở record lỗi. RabbitMQ NACK riêng tác vụ lỗi sang DLQ, nhận tác vụ tốt tiếp theo. Tại phòng vận hành Kafka, bấm **Chuyển sang topic lỗi** để ứng dụng publish DLQ topic, commit offset nguồn rồi gửi tiếp hai thư tốt; hoặc sửa email và retry tại chỗ.
3. **Đọc lại bằng group analytics mới**: Kafka đọc topic hóa đơn đã xử lý, không tạo lại PDF/email. Đây là điểm mạnh của log có retention khi thêm ứng dụng phân tích sau này.

RabbitMQ thuận tiện cho phân phối và xác nhận/từ chối từng công việc độc lập. Kafka có thể tăng partition, xây retry/DLQ hoặc dùng mô hình khác. Pause là chính sách của ứng dụng, không phải Kafka bắt buộc dừng toàn hệ thống. Không dùng đồng hồ demo để kết luận throughput chung. Xem [phân tích và phản biện](docs/SO_SANH_KAFKA.md).

## Code và dữ liệu

```text
Trình duyệt → POST /api/shop/checkout → SQLite (đơn + outbox) → HTTP 202
                              ↓ dispatcher / publisher confirm
         exchange → queue invoice → consumer tạo PDF
                                          ↓ commit hóa đơn + outbox email
         exchange → queue email → consumer SMTP → Mailpit + PDF đính kèm
```

| File | Trách nhiệm |
|---|---|
| `shop_api.py` | Checkout 202, dispatcher outbox, tạo/kill worker, tình huống, retry, Kafka replay |
| `shop_store.py` | Giá server, snapshot đơn, transaction, idempotency, invoices/deliveries/events |
| `shop_broker.py` | RabbitMQ topology/publish/metrics; Kafka topic/partition, producer xác nhận delivery |
| `shop_worker.py` | RabbitMQ manual ACK/NACK, prefetch=1; Kafka assignment/pause/retry/manual commit |
| `shop_fulfillment.py` | Cùng hàm nghiệp vụ cho hai broker: PDF tiếng Việt và SMTP kèm file |
| `index.html`, `static/shop.js`, `static/shop.css` | Cửa hàng light mode, đơn hàng, điều khiển và quan sát |
| `app.py` | Ghép API cửa hàng, giữ API/lab v3 |

- Đơn, outbox, hóa đơn, sổ gửi và log: **`data/shop.sqlite3`**. PDF: **`data/invoices/<order_id>.pdf`**.
- Mailpit: Docker volume **mail-data**, giữ tối đa 500 email.
- Queue: **`seminar.shop.v4.invoice`**, **`seminar.shop.v4.email`**, mỗi queue có `.failed` DLQ.
- Topic: **`seminar.shop.v4.<generation>.invoice/email`**, mỗi topic 2 partition; topic lỗi `.dead`.
- Web chỉ hiển thị 100 đơn/100 outbox gần nhất; tổng toàn DB đếm riêng. API trả 250 event mới nhất.

## Độ tin cậy và giới hạn

Order + outbox và invoice + outbox email được commit cùng transaction tương ứng. Dispatcher có thể publish lặp nếu sập giữa confirm và đánh dấu DB. Consumer dùng mã đơn duy nhất để tránh thêm hóa đơn/tác vụ email. SQLite, broker và SMTP không có transaction chung. Nếu SMTP đã nhận thư nhưng worker chưa ghi sổ, email vẫn có thể lặp; Message-ID ổn định không đảm bảo bên nhận tự loại trùng.

Mỗi hệ chỉ có một broker: RabbitMQ classic durable queues, Kafka replication factor 1, không phải HA. Classic DLX có giới hạn khi chặng đích lỗi. Bộ điều khiển không có đăng nhập, chỉ bind cổng vào `127.0.0.1` và dùng một web process. Triển khai thật cần auth, quản lý worker riêng, retry/backoff, giám sát, backup và replication. Kafka còn có **Share Groups**; RabbitMQ còn có **Streams**.

## Cập nhật, cấu hình, reset và dừng

```powershell
git pull --ff-only
docker compose --profile kafka up -d --build --wait --wait-timeout 240
```

Mặc định không cần `.env`. Cổng bận thì `Copy-Item .env.example .env` rồi sửa WEB_PORT/MAILPIT_WEB_PORT/AMQP_PORT/MANAGEMENT_PORT/KAFKA_PORT/SMTP_PORT. macOS/Linux dùng `cp`. Không commit `.env`, dữ liệu khách, database hoặc token. Tài khoản ví dụ chỉ dùng demo; đổi biến RabbitMQ không tự đổi tài khoản trong volume cũ.

**Reset cửa hàng:** dừng các worker, chờ broker nhận mất kết nối, bấm **Làm mới phiên thực hành**. Chỉ dọn dữ liệu/queue cửa hàng; PDF cũ, thư Mailpit và topic Kafka cũ được giữ, không còn liên kết từ phiên mới. Reset đổi generation topic; retention không tự xóa topic. Lab v3 có DB/namespace và nút reset riêng.

```powershell
docker compose --profile kafka down
```

Lệnh dừng giữ volume và `data/`. Lần sau thêm consumer để xử lý backlog. Web không tự mở worker sau restart.

| Lỗi | Cách xử lý |
|---|---|
| Docker cannot connect | Mở Docker Desktop, đợi Engine |
| Cổng đã dùng | `docker ps`, dừng bản cũ hoặc đổi cổng `.env` |
| Đơn có, PDF chưa có | Thêm consumer **Tạo hóa đơn PDF**, kiểm tra outbox/Ready |
| Có PDF, chưa có email | Thêm consumer **Gửi email**, kiểm tra `docker compose ps mailpit` và DLQ |
| Kafka chưa có partition | Với 4 worker/2 partition, 2 worker rảnh là đúng; nếu tất cả chưa có, bật profile và chờ rebalance |
| Outbox pending lâu | `docker compose ps`; `docker compose logs --tail 40 web rabbitmq kafka` |
| Reset bị từ chối | Dừng worker liên quan, đợi consumer count cập nhật |

## Tài liệu và kiểm chứng

- [Bản PDF để in: cách chạy, lời thuyết trình, giải thích code và Kafka](docs/Huong_Dan_Demo_Cua_Hang_RabbitMQ.pdf). Tạo lại từ Markdown bằng `python tests/build_shop_guide.py` trong môi trường có requirements.
- [Kịch bản cửa hàng trong 15–20 phút](docs/DEMO_CUA_HANG.md).
- [Giải thích code theo từng bước](docs/GIAI_THICH_CUA_HANG.md).
- [So sánh Kafka và phạm vi kết luận](docs/SO_SANH_KAFKA.md).
- [Nguồn chính thức RabbitMQ, Kafka, Pika](docs/TAI_LIEU_HOC.md).
- [Chạy kiểm thử](tests/README.md).

Word **Huong_Dan_Clone_Va_Chay_Demo_SOA.docx** và **KICH_BAN_SEMINAR.md** trước đây hướng dẫn lab v3 tại `/lab`; dùng hướng dẫn cửa hàng cho giao diện mới. Ảnh sản phẩm lấy từ bộ ảnh có sẵn trong dự án. PDF nhúng DejaVu Sans; giữ giấy phép ở `static/fonts/LICENSE-DejaVu.txt`.
