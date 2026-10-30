# Sổ tay đọc code và trả lời giảng viên

Dành cho người đã dùng AI hỗ trợ viết code nhưng muốn tự hiểu và giải thích được. Bản ngày 31/10/2026, theo cấu trúc `shop/`. Không học thuộc tất cả: đọc một đơn hàng đi qua đâu, sau đó hiểu các điểm có thể hỏng. Phụ lục cuối tài liệu chỉ đúng dòng bắt đầu/kết thúc của từng hàm trong mã hiện tại.

## 1. Đề tài này giải quyết vấn đề thực tế gì?

Khách đặt một đơn gồm sản phẩm, kích cỡ và số lượng. Server phải tính tiền đúng và ghi nhận đơn. Tạo file PDF và gọi máy chủ email là hai việc có thể chậm hoặc lỗi riêng. Nếu API chờ xong cả hai, thời gian khách chờ phụ thuộc toàn bộ quy trình. Ở demo, API lưu đơn và yêu cầu xử lý rồi trả HTTP 202; worker tiếp tục làm phía sau.

Kết quả nghiệp vụ có thật: bản ghi đơn hàng, file PDF có từng dòng hàng và một email SMTP có PDF đính kèm trong Mailpit. Nhịp 2/4/6 giây chỉ làm chậm để nhìn được; đặt 0 thì file và thư vẫn được tạo. Queue không làm phần tạo PDF tự nhanh hơn: nó đệm việc và giúp phân phối cho worker.

Một đơn thưa thớt, PDF rất nhẹ và không cần phục hồi có thể viết đồng bộ đơn giản hơn. Giá trị của queue rõ hơn khi có backlog, cần tăng worker, email hỏng mà vẫn nhận đơn hoặc worker bị dừng giữa chừng. Đó là lý do buổi demo phải có các tình huống, không chỉ bấm nút rồi thấy số tăng.

**Câu trả lời ngắn:** “Em tách việc nhận đơn khỏi tạo PDF và gửi email. RabbitMQ giữ và phân phối công việc; kết quả cuối là PDF và thư cụ thể. Em chứng minh thêm khả năng xử lý backlog và giao lại khi worker chết.”

## 2. Các từ cần hiểu trước khi nhìn code

- **Process:** một chương trình đang chạy với bộ nhớ riêng và PID. Web là một process; mỗi consumer được nút thêm worker mở thành process khác.
- **Thread:** luồng chạy trong một process. Dispatcher chạy ở thread nền của web; nó khác các process worker.
- **HTTP request/response:** trình duyệt gửi yêu cầu và server trả câu trả lời của lần gọi đó. HTTP 202 không phải một message ACK của RabbitMQ.
- **API/route:** cửa vào của server, ví dụ `POST /api/shop/checkout`; POST thực hiện hành động, GET đọc dữ liệu, PATCH sửa một phần.
- **Producer:** bên gửi yêu cầu làm việc vào broker. Ở đây dispatcher là producer trực tiếp; handler HTTP tạo yêu cầu trong outbox trước.
- **Broker:** hệ thống trung gian quản lý truyền/giữ dữ liệu; RabbitMQ và Kafka là hai broker khác mô hình.
- **Message/task:** phong bì yêu cầu làm việc. `order_id` chỉ đơn; `task_id` thêm vai trò, như `DH-...:invoice`; `role` chọn invoice/email.
- **Exchange/binding/routing key:** exchange nhận message và đối chiếu binding; routing key `invoice` hoặc `email` chọn queue phù hợp. Exchange không phải nơi lưu backlog lâu dài.
- **Queue:** nơi chờ của các delivery công việc RabbitMQ. Nhiều consumer cùng queue chia việc; không mặc định mỗi consumer nhận một bản sao.
- **Consumer/worker:** consumer là vai trò nhận message; worker là process của demo vừa nhận vừa thực hiện nghiệp vụ. Một process không tự đồng nghĩa có một partition Kafka.
- **Transaction/commit/rollback:** một nhóm thay đổi DB được chốt cùng nhau hoặc hủy cùng nhau. Không suy ra transaction SQLite bao trùm luôn broker, file và SMTP.
- **Outbox:** bảng lưu yêu cầu cần publish trong cùng transaction với dữ liệu nghiệp vụ. Đây là hộp việc “phải gửi”, không phải hộp thư của khách.
- **Idempotency:** cùng yêu cầu hoặc cùng task được gặp lại nhưng không tạo thêm hiệu ứng nghiệp vụ đã được bảo vệ. Không phải cấm broker giao lặp.
- **ACK:** consumer xác nhận lượt giao đã xử lý. **NACK:** từ chối lượt giao; `requeue` quyết định có đưa lại queue nguồn hay không.
- **Publisher confirm:** xác nhận của broker với publisher, độc lập với consumer ACK. Có confirm không chứng minh khách đã nhận email.
- **Ready/Unacked:** Ready còn chờ giao; Unacked đã giao nhưng chưa ACK. Số đếm Management có độ trễ lấy mẫu.
- **DLX/DLQ:** DLX là exchange route lỗi; DLQ là queue đích giữ tin lỗi. Cần khai báo và binding, không tự có chỉ vì Python ném exception.
- **Redelivery:** giao lại công việc; **dedup:** nhận ra hiệu ứng đã có để tránh tạo thêm.
- **Topic/partition/offset/group:** Kafka lưu log trong topic chia partition; offset là vị trí trong một partition; consumer group giữ tiến độ riêng. Offset không duy nhất trên cả topic.
- **SMTP/MIME:** SMTP vận chuyển email; MIME biểu diễn nội dung và file đính kèm. Mailpit nhận SMTP trong máy thực hành, không phải Gmail.

## 3. Bản đồ file và thứ tự nên đọc

Đọc `app.py` để biết server mở cái gì; `static/shop.js` để tìm nút; `shop/api.py` để tìm route tương ứng; `shop/store.py` để biết dữ liệu được ghi ra sao; `shop/broker.py` để biết message đi đâu; `shop/worker.py` để thấy ACK/lỗi; cuối cùng `shop/fulfillment.py` để xem PDF và email thật.

`app.py` chỉ gắn router và static files, đồng thời dùng lifespan gọi startup/shutdown. `shop/__init__.py` giúp Python coi thư mục là package; không có nghiệp vụ. Các file trong `tests/` kiểm tra ứng dụng, không nằm trong luồng đặt hàng. `tools/build_guide.py` chỉ tạo tài liệu in, xóa nó không làm cửa hàng ngừng chạy.

HTML là khung form/nút. JS lấy dữ liệu và điều khiển sự kiện. CSS chỉ trình bày. Không tìm logic ACK trong CSS, cũng không coi trạng thái do JS vẽ là bằng chứng broker đã ACK. Chức năng broker phải đối chiếu callback worker, DB và số đếm broker.

Sơ đồ đầy đủ từng nút và luồng gọi nằm ở `docs/LUONG_XU_LY.md`. Phần sơ đồ trong PDF phân biệt đường HTTP với đường message; đây là hai loại giao tiếp khác nhau.

## 4. Từ nút đặt hàng đến HTTP 202

**Mở file:** `static/index.html`, form `checkout-form`; `static/shop.js`, handler `onsubmit`; `shop/api.py`, model `Checkout` và hàm `checkout`.

Trình duyệt gom tên/email/địa chỉ và giỏ hàng. JS tạo `crypto.randomUUID()` cho idempotency key; khi đang gọi thì khóa nút để giảm nhấp đúp. `api()` dùng fetch, JSON.stringify và Content-Type application/json. Không dùng `setTimeout` để tự tuyên bố đơn đã hoàn tất.

Server dùng Pydantic giới hạn độ dài chuỗi, số dòng giỏ và số lượng; validation nghiệp vụ sâu tiếp tục ở `store.quote()` và `create_order()`. Frontend kiểm tra form để thuận tiện; server vẫn phải kiểm tra vì người dùng có thể gọi API trực tiếp. `esc()` trong JS và `escape()` khi viết PDF biến ký tự đặc biệt thành dữ liệu để tên khách không bị hiểu thành HTML.

Đoạn quyết định mã HTTP ở `api.py`:

```python
@router.post('/checkout',status_code=202)
def checkout(req:Checkout):
    # create_order lưu đơn và outbox rồi mới trả kết quả
    oid,created=store.create_order(...)
```

Đây là trích rút gọn để giải thích; dấu `...` là phần tham số đã lược, không phải hướng dẫn thay code thật bằng dấu ba chấm. Trong file gốc, decorator và lời gọi đầy đủ nằm trong hàm `checkout`.

**Vì sao 202:** đã tiếp nhận để xử lý tiếp, không xác nhận PDF/email xong. Cơ sở là commit đơn + outbox, không phải chờ broker ở trong request. Broker mất kết nối vẫn có thể nhận đơn và giữ outbox pending. HTTP 422 báo dữ liệu không hợp lệ; 404 khi chưa có tài nguyên/file; 409 khi hành động xung đột như thêm quá số worker hoặc reset khi worker còn chạy; 503 ở replay khi Kafka không đọc được.

**Nếu xóa/thay:** đổi 202 thành 200 chỉ đổi hợp đồng HTTP, không tự làm code đồng bộ. Xóa idempotency key và chỉ giữ khóa nút không ngăn request bị gửi lại do mất kết nối. Dùng FastAPI BackgroundTasks thay cho outbox có thể chạy nền nhưng không tự giữ việc bền khi process chết. Chạy PDF/SMTP ngay trong route làm khách phải chờ và làm lỗi email ảnh hưởng response đặt hàng.

**Câu trả lời GV:** “202 của em nghĩa là đơn và outbox đã commit. Broker confirm, PDF và SMTP là các mốc sau đó; em không dùng 202 để khẳng định đã gửi email.”

## 5. Server tính tiền ở đâu và vì sao không tin giá trên trình duyệt?

**Mở file:** `shop/store.py`, `CATALOG`, `PRODUCTS`, `quote()`; `shop/api.py`, `Item`.

JS tính tạm tổng để khách nhìn thấy, nhưng server chỉ dùng SKU, size và quantity để tra catalogue của mình. `quote()` xác minh sản phẩm, kích cỡ, số lượng và từ chối hai dòng trùng cùng SKU/size; nhân unit_price với quantity, cộng subtotal, rồi tính shipping. Mốc miễn phí giao hàng là 1.000.000 đ, còn lại 25.000 đ.

Tiền lưu bằng số nguyên VND. Không dùng float để cộng tiền rồi xử lý sai số nhị phân. `unit_price`, `line_total`, tên và màu được chụp lại vào đơn khi đặt; catalogue đổi sau đó không làm hóa đơn cũ đổi tiền. Đây là snapshot giá, không phải luôn tra lại giá mới lúc worker tạo PDF.

**Ví dụ:** 2 áo thun giá 249.000 đ và 1 hoodie giá 549.000 đ: subtotal 1.047.000 đ, shipping 0, total 1.047.000 đ. Client cố gửi `unit_price=1` không được dùng để tính tiền. Kiểm thử thực tế dùng đúng tình huống này.

**Nếu xóa/thay:** tin total/price do JS gửi thì ai gọi API cũng có thể sửa giá. Xóa snapshot rồi đọc catalogue ở worker khiến cùng đơn có thể bị tính theo giá mới. Thay bằng DB sản phẩm là hợp lý khi nhiều sản phẩm hoặc có quản trị giá; nguyên tắc kiểm giá ở server và lưu snapshot vẫn giữ. Sổ tồn kho/thanh toán thật là nghiệp vụ bổ sung chưa có trong demo này.

**Câu trả lời GV:** “Frontend chỉ dự tính để hiển thị. Số tiền chính thức do server tính từ catalogue và được lưu theo đơn bằng số nguyên VND.”

## 6. SQLite đang giữ gì? Transaction có tác dụng gì?

**Mở file:** `shop/store.py`, `init()`, `connect()`, `create_order()` và `commit_invoice()`.

`orders` là đơn và snapshot dòng hàng. `tasks` là trạng thái từng việc invoice/email và lượt nhận. `outbox` là các yêu cầu publish. `invoices` là số hóa đơn, tên file và snapshot; PDF thực nằm trên đĩa. `deliveries` là sổ ứng dụng đã ghi nhận SMTP nhận thư. `events` ghi dấu vết; `workers` là quan sát process/assignment; `settings` giữ nhịp quan sát và Kafka generation.

`connect()` là context manager. `with db:` commit khi khối chạy thành công, rollback khi có exception; finally đóng connection. `row_factory` giúp đọc cột theo tên. `busy_timeout` cho SQLite chờ khóa thay vì lỗi ngay; WAL hỗ trợ đọc trong khi có ghi, nhưng SQLite vẫn chỉ có một writer tại một thời điểm. Nhiều worker không đồng nghĩa DB ghi vô hạn song song.

`create_order()` dùng BEGIN IMMEDIATE, kiểm tra request_key cũ và fingerprint. Key cũ cùng nội dung trả cùng order_id; cùng key nhưng nội dung khác bị từ chối. UUID sinh mã có xác suất trùng cực thấp; constraint UNIQUE của DB mới là lớp bảo vệ cuối, không phải tin tuyệt đối vào chuỗi random.

Hai nhóm nguyên tử quan trọng: (1) order + invoice task + outbox invoice; (2) invoice + email task + outbox email. Nếu ghi invoice thành công nhưng ghi outbox email lỗi thì transaction thứ hai rollback, không để hóa đơn có trong DB mà mất yêu cầu email. Unit test cố gây lỗi enqueue để kiểm tra rollback này.

**Nếu xóa/thay:** bỏ transaction thì có thể lưu nửa chừng. Chỉ kiểm tra “đã tồn tại chưa” trong Python mà bỏ UNIQUE vẫn có race khi hai process chạy cùng lúc. Xóa DB làm mất sổ đơn, task, dedup và liên kết tới PDF; queue còn tin nhưng không còn dữ liệu đơn để xử lý. Thay SQLite bằng PostgreSQL được khi cần nhiều writer/triển khai tách máy; phải mang cả schema, unique key và transaction sang, không chỉ sửa chuỗi kết nối.

**Câu trả lời GV:** “Em dùng SQLite để cả lớp chạy gọn. Em vẫn dùng constraint và transaction để minh họa tính đúng; đây chưa phải phép thử DB chịu tải thương mại.”

## 7. Outbox: vì sao không publish ngay sau INSERT là xong?

**Mở file:** `shop/store.py`, `enqueue/pending_outbox/published/publish_error`; `shop/api.py`, `dispatch/startup/shutdown`.

Hãy xét code ngây thơ: lưu đơn rồi publish. Nếu process chết đúng khoảng giữa, đơn tồn tại nhưng không có message. Đảo lại publish rồi lưu đơn cũng có khoảng lỗi: worker nhận message nhưng không tìm thấy đơn. Outbox lưu đơn và yêu cầu gửi trong một transaction DB nên cả hai cùng có hoặc cùng không có.

Dispatcher đọc các dòng pending. RabbitMQ: mở connection/channel, khai báo topology, bật confirm_delivery, publish mandatory/persistent và chỉ gọi `store.published()` khi chặng publish thành công. Kafka: đợi callback delivery/flush thành công rồi mới đánh dấu. Có lỗi thì ghi số lần thử và quay lại sau; không xóa outbox chỉ vì mất kết nối.

```text
commit đơn + outbox
    -> publish -> broker xác nhận -> đánh dấu published trong DB
```

Khoảng giữa broker xác nhận và đánh dấu DB vẫn tồn tại. Process chết ở đó có thể publish lại cùng task; vì vậy outbox giảm nguy cơ bỏ quên việc, không bảo đảm broker chỉ nhận một lần. Consumer phải xử lý lặp an toàn. Một vòng dispatcher riêng cho mỗi transport giúp Kafka lỗi không làm hàng RabbitMQ chờ phía sau Kafka trong cùng vòng.

`asyncio.to_thread()` cho vòng lặp đồng bộ chạy ở thread khác để không giữ event loop phục vụ HTTP. `STOP.wait(.6)` vừa giãn lần quét vừa có thể dừng sớm khi shutdown. Đây là polling outbox đơn giản cho một web process; không có cơ chế claim để nhiều bản dispatcher cùng tranh việc. Không chạy uvicorn nhiều worker với thiết kế này.

**Nếu xóa/thay:** bỏ outbox thì demo happy path vẫn có thể chạy nhưng mất câu trả lời đáng tin cho lỗi giữa DB và publish. Đổi sang Redis list không tự làm INSERT DB và đẩy Redis nguyên tử. CDC/outbox relay riêng là cách triển khai khác khi lớn hơn; vẫn phải xử lý publish lặp. Không cần thêm CDC vào bài seminar này.

**Câu trả lời GV:** “Outbox bảo vệ ý định phải gửi cùng với đơn, chứ không biến DB và broker thành một transaction. Em chấp nhận giao lặp và chống tạo thêm hóa đơn ở consumer.”

## 8. RabbitMQ: message được đưa vào đúng queue bằng cách nào?

**Mở file:** `shop/broker.py`, `connection/topology/rabbit_publish/queue/dlq`.

`connection()` đọc host/port/credential từ environment và tạo Pika BlockingConnection. Heartbeat phát hiện kết nối hỏng; socket timeout và blocked connection timeout giới hạn một số tình huống chờ. Không chia sẻ connection đó tùy tiện giữa các thread. Connection là đường kết nối; channel là kênh AMQP bên trong đường đó.

`topology()` khai báo direct exchange `seminar.shop.v4.tasks`. Hai queue là `.invoice` và `.email`, binding tương ứng routing key `invoice` và `email`. Mỗi queue có `.failed` riêng và được nối với DLX `seminar.shop.v4.errors`. Tên queue là địa chỉ công việc; tên consumer là process nhận việc, hai thứ không phải một.

```text
exchange tasks -- key invoice --> queue invoice --> consumer tạo PDF
               -- key email   --> queue email   --> consumer gửi SMTP
```

`exchange_type='direct'` so khớp routing key với binding key. Đây không phải fanout. Muốn hai hệ thống đều nhận một bản sao thì cần queue riêng cho mỗi bên và binding phù hợp; thêm hai consumer vào cùng một queue là chia việc, không phải broadcast.

`durable=True` giữ định nghĩa exchange/queue qua restart; `delivery_mode=2` đánh dấu message persistent. Hai lựa chọn khác nhau và chưa đủ cho câu “không bao giờ mất tin”. `mandatory=True` giúp publisher nhận biết trường hợp không route đến queue; confirm_delivery giúp nhận kết quả chặng publish. `message_id` giữ task ID để đối chiếu, không tự khiến RabbitMQ bỏ trùng.

Queue chính có max-length 10.000 và overflow reject-publish. Nếu queue đầy và broker từ chối, dispatcher không được coi việc đã publish thành công; outbox còn pending để xử lý lại. Mốc 10.000 là giới hạn tài nguyên demo, không phải dung lượng tối đa của RabbitMQ nói chung.

**Nếu xóa/thay:** bỏ binding hoặc đổi routing key sai làm tin không vào đúng queue. Bỏ mandatory có thể khiến publisher không phát hiện tin không route được. Bỏ persistent/durable giảm bảo đảm khi restart. Đổi cấu hình một queue đã tồn tại mà không đồng bộ định nghĩa có thể gây PRECONDITION_FAILED; đừng sửa tên/loại queue ngay trước buổi trình bày. Hệ thống thật nên cân nhắc quorum queues và cấu hình DLX bằng policy; classic queue một node của bài này không có HA.

**Câu trả lời GV:** “Exchange route yêu cầu theo role; queue giữ backlog. Em dùng direct vì invoice/email là hai loại công việc xác định, không cần tự sáng tạo cách route trong frontend.”

## 9. Nút thêm worker có mở chương trình thật không?

**Mở file:** `shop/api.py`, `add_worker/crash_worker`; `shop/worker.py`, phần `if __name__=='__main__'`.

Nút gọi POST workers với transport và role. Server dùng subprocess.Popen và cùng Python interpreter để chạy lệnh dưới đây; stdout/stderr ghi vào file log trong `data/`:

```text
python -u -m shop.worker R-HD-xxxxx rabbitmq invoice
python -u -m shop.worker R-MAIL-xxxxx rabbitmq email
```

`-m shop.worker` chạy module trong package từ thư mục dự án. `-u` giảm đệm output để log xuất hiện kịp quan sát. CLI phân tích tên/transport/role rồi chọn `rabbit()` hoặc `kafka()`. Worker hóa đơn và worker email chạy cùng file nhưng tham số role khác; không cần hai bản code gần giống nhau.

API lưu Popen trong PROCESSES và metadata transport/role trong bộ nhớ để biết process nào nó quản lý. PID trên thẻ lấy từ process thật; `p.poll()` kiểm tra còn sống. Dòng worker trong DB là quan sát gần nhất, không tự chứng minh process còn sống nếu không đối chiếu. `p.kill()` dừng thật, chưa phải shutdown mềm chờ xử lý xong.

Giới hạn bốn worker mỗi role/transport tránh bấm quá nhiều trên laptop. Đây là giới hạn UI/API của demo, không phải RabbitMQ chỉ cho bốn consumer. Nếu web restart, các worker do nó quản lý được dừng và không tự mở lại; thêm worker mới để chạy backlog. Process ngoài API không được vẽ là process sống do API quản lý.

**Nếu xóa/thay:** bỏ Popen và chỉ tạo thêm thẻ HTML thì không còn minh chứng nhiều consumer. Dùng thread để làm worker cũng có thể nhận broker nhưng không chứng minh kill process riêng theo cách hiện tại. Triển khai thật thường dùng worker service/Compose replicas hoặc trình quản lý process; không nên cho người dùng công khai nút mở/kill process không xác thực.

**Câu trả lời GV:** “Mỗi consumer trong demo là process có PID. Em chọn Popen để lớp điều khiển ngay trên web; khi triển khai tách dịch vụ sẽ để hệ thống vận hành quản lý worker.”

## 10. Tạo hóa đơn: code nào tạo file và code nào ghi DB?

**Mở file:** `shop/fulfillment.py`, `fulfill/render_invoice`; `shop/store.py`, `invoice/commit_invoice`; `shop/api.py`, `invoice_pdf`.

Nhánh invoice lấy đơn đã snapshot từ DB. Nếu đã có invoice, nó ghi DEDUP và bỏ qua tạo thêm tác vụ email; nếu file bị thiếu thì có thể tạo lại file từ thông tin hiện có. Nếu chưa có, worker ghi PDF_RENDER, giãn theo setting, đặt số hóa đơn rồi gọi render_invoice.

ReportLab xây tài liệu A4 bằng Paragraph, Table, Spacer. Mỗi dòng sản phẩm có tên/màu/size, quantity, unit_price và line_total; phía dưới có subtotal, shipping và total. Font DejaVu Sans được nhúng để người khác tải PDF vẫn đọc tiếng Việt. `escape()` ngăn tên/address bị hiểu thành cú pháp markup của Paragraph.

File được ghi vào tên tạm riêng rồi `os.replace()` thành `data/invoices/<order_id>.pdf`. Việc thay file cuối tránh đưa ra một file PDF đang ghi dở. Nó không đồng nghĩa file và DB đã commit chung hoặc an toàn trước mọi tình huống mất điện/đĩa. Sau khi tạo file, `commit_invoice()` mới chốt invoice cùng outbox email.

`order_id` là primary key của invoices và number có unique constraint. Hai process cùng tạo file cho một đơn có thể làm công thừa, nhưng chỉ lần INSERT invoice thành công mới thêm outbox email. Đừng nói “worker không bao giờ tạo file hai lần”; điều được bảo vệ là bản ghi invoice và yêu cầu email theo đơn.

Modal xem hóa đơn trên web được JS dựng từ snapshot invoice. Nút tải PDF gọi FileResponse lấy file thật; modal không phải nguồn tạo file. Số hóa đơn minh họa có ngày và phần đuôi mã đơn; đây không phải tích hợp hệ thống hóa đơn điện tử thuế.

**Nếu xóa/thay:** bỏ unique order_id thì redelivery có thể tạo thêm invoice. Ghi email outbox trước khi file sẵn sàng có thể khiến worker email nhận việc nhưng thiếu attachment. Dùng HTML làm hóa đơn được nếu người dùng chỉ cần xem/in HTML; muốn tiếp tục đính kèm PDF phải có bước chuyển đổi phù hợp. Đổi file local thành object storage hợp lý khi worker chạy nhiều máy; khi đó message hoặc DB phải tham chiếu tài nguyên mà các worker đều truy cập được.

**Câu trả lời GV:** “File nằm trong data/invoices; bản ghi và snapshot nằm trong SQLite. Em không nhầm hiển thị modal với tạo PDF. Hai tài nguyên file/DB có các khoảng lỗi được xử lý bằng retry và idempotency, không phải transaction phân tán.”

## 11. Email có thật không, nhận ở đâu và giới hạn chống trùng là gì?

**Mở file:** `shop/fulfillment.py`, nhánh email trong `fulfill`; `shop/store.py`, `sent/record_sent`; service mailpit trong Compose.

Worker kiểm tra sổ gửi, email hợp lệ, bản ghi invoice và file có tồn tại. EmailMessage tạo From, To, Subject, Date, Message-ID và nội dung. `add_attachment` đọc byte PDF, khai báo application/pdf, đặt filename theo số hóa đơn. `smtplib.SMTP` nối tới Mailpit; `send_message` gửi SMTP, không dùng API giả tăng bộ đếm.

Mailpit nhận và giữ email trên máy. Bạn nhập địa chỉ thử bất kỳ hợp lệ nhưng mở thư tại cổng 8025, không chờ Gmail thật. Điều này tránh cấu hình tài khoản/mật khẩu gửi thư cho cả lớp và vẫn kiểm chứng MIME/attachment. SMTP accepted là Mailpit nhận thư, không chứng minh hệ thống Internet hoặc người nhận cuối đã nhận/đọc.

Sau khi SMTP nhận thành công, code mới gọi record_sent và sau đó ACK/commit. Nếu process chết đúng sau SMTP nhưng trước record_sent, lượt sau chưa thấy sổ gửi nên có thể gửi lại. Hai worker gửi cùng đơn đồng thời cũng không được bảo vệ trọn vẹn bằng việc check sổ rồi gửi. Primary key chỉ ngăn thêm hai dòng deliveries, không thu hồi được thư đã đi.

Message-ID ổn định giúp đối chiếu cùng đơn; không có bảo đảm mọi SMTP/mail client sẽ tự loại trùng. Test bình thường chứng minh một tác vụ invoice lặp không thêm invoice/email outbox, chứ không chứng minh exactly-once cho mọi crash window của SMTP.

**Nếu xóa/thay:** bỏ record_sent thì redelivery email bình thường sẽ gửi lại nhiều hơn. Ghi sổ gửi trước SMTP để tránh trùng có rủi ro khác: đánh dấu đã gửi rồi chết trước send_message khiến mất thư. Muốn đổi sang email Internet phải chọn SMTP thật, credential/secret, TLS và cấu hình gửi phù hợp; không chỉ đổi link Mailpit. Khi cần chặt chẽ hơn, cân nhắc dịch vụ gửi có khóa idempotency và đối soát, vẫn phải hiểu phạm vi bảo đảm của nhà cung cấp.

**Câu trả lời GV:** “Em gửi SMTP thật vào Mailpit có PDF. Sổ gửi giúp bỏ qua những lần đã ghi thành công, nhưng em không hứa exactly-once qua ranh giới SMTP và SQLite.”

## 12. ACK, prefetch và crash: cần hiểu ba thời điểm

**Mở file:** `shop/worker.py`, `rabbit/complete`; `shop/store.py`, `received`; `shop/fulfillment.py`, nhánh crash_after_commit.

Worker đăng ký auto_ack=False, prefetch_count=1. Callback giải mã JSON, xác minh task thuộc order/role/transport hiện tại, ghi lượt nhận rồi gọi fulfill. Thành công mới `channel.basic_ack(method.delivery_tag)`; sau đó log ACK_SENT. delivery_tag là số lượt giao trong channel hiện tại, không dùng làm khóa hóa đơn lâu dài.

**Thời điểm A, trước commit invoice:** worker chết khi đang làm, chưa ACK; broker giao lại, worker khác làm từ đầu. **B, sau commit nhưng trước ACK:** invoice đã có, delivery vẫn có thể được giao lại; lần sau DEDUP, không thêm invoice/outbox email. **C, sau gửi ACK:** broker có thể đã xử lý hoặc client mất kết nối trước khi chắc chắn; ACK không có biên nhận ACK ngược cho từng lời gọi như publisher confirm.

Nút sập sau lưu hóa đơn tạo flag cho đơn. Sau commit, `os._exit(42)` kết thúc process ngay để cố ý không chạy bước ACK. Nếu chỉ có một worker thì cần thêm worker mới; có nhiều worker thì worker còn sống nhận lại. Cờ method.redelivered do AMQP cấp giúp ghi REDELIVER; DEDUP là logic DB của ứng dụng, không phải tên một lệnh broker.

prefetch=1 giúp worker bận chỉ giữ một delivery chưa ACK, consumer khác còn cơ hội nhận việc. Không bảo đảm phân phối thời gian hoàn tất tuyệt đối công bằng; xử lý nhiều consumer có thể hoàn thành khác thứ tự. Đổi prefetch=10 có thể tăng số tin mỗi consumer giữ và ảnh hưởng cách chia backlog; bỏ giới hạn không tự làm CPU nhanh hơn.

**Nếu xóa/thay:** auto_ack=True xác nhận khi giao theo chế độ đó, worker chết trong nghiệp vụ có thể không được giao lại việc chưa xong. ACK trước fulfill cũng có rủi ro mất việc tương tự. Không ACK và không NACK thì delivery giữ Unacked, với prefetch=1 consumer không nhận tiếp cho tới khi xử lý/xuất hiện mất kết nối. Xóa dedup làm side effect lặp có thể thành thêm hóa đơn. `requeue=True` liên tục với tin không thể sửa sẽ tạo vòng lặp nóng.

**Câu trả lời GV:** “Em ACK sau xử lý để broker có thể giao lại khi process chết; vì giao lại có thể xảy ra sau commit nên em cần idempotency. ACK và dedup giải quyết hai vấn đề khác nhau.”

## 13. Lỗi email, DLQ và retry đi qua những hàm nào?

**Mở file:** `shop/worker.py`, nhánh except của callback Rabbit; `shop/api.py`, `fix_email/retry_task`; `shop/broker.py`, DLX bindings.

Checkout bình thường chặn email sai. Seed quản trị cố ý tạo email không hợp lệ để mô phỏng dữ liệu lỗi từ kênh tích hợp khác, không phải bảo checkout luôn cho nhập sai. Cả RabbitMQ và Kafka cùng gặp lỗi từ hàm fulfillment đó.

Ở RabbitMQ, lỗi xử lý ghi NACK_DLQ và gọi basic_nack với requeue=False. Nếu có DLX/binding đúng, broker route message vào queue failed. Consumer tiếp tục nhận tin khác; PDF của đơn lỗi email vẫn giữ vì đó là bước trước đã hoàn thành.

Nút sửa gửi PATCH email rồi POST retry. API lấy đúng message của task từ DLQ, publish lại vào queue chính có confirm, sau đó ACK bản đang giữ từ DLQ. Tin đã xem nhưng không phải task cần retry chưa ACK nên sẽ được requeue khi connection đóng. Code duyệt tối đa 200 bản; đây là cách gọn cho demo, không phải công cụ tìm kiếm DLQ lớn. Nếu không thấy, API trả lỗi để người dùng chờ/kiểm tra thay vì giả báo đã thử lại.

Tất cả exception trong xử lý Rabbit hiện đều đi nhánh DLQ, kể cả lỗi tạm thời của SMTP/file/DB. Demo chưa phân loại retry tạm thời tự động theo backoff. Sau khi dịch vụ hồi phục, người vận hành retry; với lỗi PDF có thể POST task invoice retry qua API, trường sửa email trên UI chủ yếu phục vụ tình huống lỗi email. Khi triển khai thật cần phân loại transient/permanent và tránh yêu cầu sửa email cho lỗi đĩa.

**Nếu xóa/thay:** bỏ DLX rồi vẫn NACK requeue=False có thể loại message khỏi queue mà không giữ chỗ để sửa. ACK bản DLQ trước khi publish lại có nguy cơ mất việc nếu publish thất bại. Publish trước ACK giúp giảm mất việc nhưng vẫn có thể trùng nếu chết ở giữa; giữ task ID và dedup là cần thiết. Tự động requeue ngay tin sai email không sửa được email.

**Câu trả lời GV:** “DLQ là nơi giữ công việc không xử lý được để quan sát, sửa và đưa lại. Nó không tự sửa lỗi; app của em retry sau khi đã có quyết định hoặc dữ liệu đúng.”

## 14. Kafka: khác ở transport, không đổi nghiệp vụ để thiên vị

**Mở file:** `shop/broker.py`, `ensure_kafka/kafka_publish`; `shop/worker.py`, `kafka`; `shop/api.py`, `analytics_replay`.

Kafka tạo topic invoice/email theo generation phiên, hai partition cho mỗi topic công việc và một partition cho mỗi topic dead. Key là order_id. Producer dùng enable.idempotence và acks=all, đợi callback delivery. RF=1 nên acks=all ở đây không tương đương nhiều bản sao hoặc HA. Producer idempotence không chống mọi lần publish lặp do DB outbox hoặc producer mới tạo ở lần sau.

Consumer dùng classic group, group ID riêng theo topic/role, auto commit tắt. on_assign/on_revoke nhận assignment thật và lưu để UI hiển thị. Với hai partition, tối đa hai consumer trong cùng group được gán partition tại một thời điểm. Bốn process vẫn có thể tồn tại nhưng hai process rảnh; thêm consumer không tự tạo partition.

Thành công gọi cùng fulfill rồi commit offset đồng bộ. Commit một record thường lưu vị trí kế tiếp để lần đọc sau bắt đầu sau record đã hoàn thành; không xóa record khỏi log. earliest chỉ có tác dụng khi group chưa có offset hợp lệ cho partition hoặc cần reset theo cấu hình, không có nghĩa mỗi lần poll đều đọc từ đầu.

Lỗi nghiệp vụ làm pause đúng partition và giữ record chưa commit trong bộ nhớ worker. Consumer vẫn poll để duy trì hoạt động group; partition khác được giao vẫn có thể chạy. Seed lỗi đặt ba đơn cùng partition 0 có chủ ý để dễ nhìn chính sách giữ thứ tự; không đại diện mọi cách phân bố thực tế.

Sửa/retry xử lý lại record lỗi. Chuyển topic lỗi tạo envelope chứa source topic/partition/offset, publish và đợi delivery rồi commit nguồn, resume partition. Kafka không bắt buộc có chính sách pause đó; app chọn giữ thứ tự. Khi crash giữa publish lỗi và commit nguồn có thể thêm bản lỗi lặp. Kafka transactions có thể hỗ trợ một số luồng Kafka→Kafka nhưng không tự bao trùm file, SQLite và SMTP.

Replay tạo group analytics mới đọc log retained hiện tại, trả order_id/partition/offset và không gọi fulfill. Record vẫn có sau commit group xử lý nên có dữ liệu đọc lại; giới hạn 100 record và một khoảng chờ để phù hợp seminar. Đây là đọc lại task log, chưa phải một mô hình phân tích doanh thu đầy đủ.

**Nếu xóa/thay:** bật auto commit có thể ghi tiến độ trước nghiệp vụ xong. Commit trước fulfill có rủi ro bỏ việc sau crash; không commit có thể gặp lại record. Tăng partition có thể tăng số consumer có assignment nhưng cần xét key/order và phân bố. Retry topic/DLQ hoặc xử lý song song trong consumer là cách khác; cần tự thiết kế offset và thứ tự. Share Groups là mô hình khác ngoài classic group đang chạy. RabbitMQ Streams cũng có log/replay, nên không nói RabbitMQ nói chung không thể replay.

**Câu trả lời GV:** “Em đang so work queue với Kafka classic group hai partition. RabbitMQ thuận tiện cho công việc độc lập; Kafka thuận tiện giữ lịch sử cho group khác. Em cho thấy cả cách gỡ lỗi Kafka và lợi thế replay, không kết luận từ đồng hồ trên laptop rằng một broker luôn nhanh hơn.”

## 15. Docker và từng thư viện đảm nhận phần nào?

`docker-compose.yml` mở web, rabbitmq, mailpit và Kafka khi bật profile kafka. Host `rabbitmq`, `mailpit`, `kafka` là tên service trong network Docker. Trong container web, localhost là chính container web, không phải container RabbitMQ; vì thế không tự đổi host bên trong thành localhost.

`127.0.0.1:8000:8000` bind web trên máy hiện tại. Port đầu là cổng máy bạn, port sau là cổng trong container. Đổi WEB_PORT đổi URL mở trên máy, không đổi cổng Python bên trong. RabbitMQ có AMQP 5672 để code nói chuyện và Management 15672 cho UI/API quản trị; nhầm hai cổng sẽ không kết nối đúng giao thức.

`./data:/lab/data` là bind mount giữ DB/PDF/log ngoài container. `rabbit-data`, `kafka-data`, `mail-data` là named volumes. `docker compose down` giữ chúng; `down -v` là xóa volume, không dùng khi cần giữ dữ liệu. Khi sửa code phải build lại image để container nhận code mới vì source không được mount live.

Dockerfile cài requirements rồi copy app/shop/static. Thư mục docs/tests/data/.env không được đưa vào image qua build context theo dockerignore; file data được mount lúc chạy. Requirements cố định phiên bản để lớp dùng cùng bộ thư viện, không khẳng định là các bản mới nhất. `.env.example` chứa cấu hình demo; không thay bằng token/mật khẩu thật rồi commit.

FastAPI nhận HTTP/route/lifespan; Uvicorn chạy server ASGI; Pydantic đọc và kiểm cấu trúc dữ liệu. Pika nói AMQP với RabbitMQ. confluent-kafka nói Kafka với producer/admin/consumer. ReportLab tạo PDF. sqlite3, smtplib, subprocess, asyncio, uuid, json và pathlib là thư viện chuẩn Python. Frontend dùng JavaScript và CSS thuần, không cần Node khi người học chạy Docker.

**Nếu xóa/thay:** bỏ font tiếng Việt có thể tạo PDF thiếu dấu/lỗi font. Bỏ volume data làm dữ liệu gắn với container và dễ mất khi recreate. Có thể dùng framework HTTP khác nhưng phải thay toàn bộ route/validation/lifespan, không có lợi cho mục tiêu seminar hiện tại. Chỉ muốn học RabbitMQ thì không bật Kafka; muốn so sánh phải bật đúng profile.

## 16. Phần nào có thể bỏ, phần nào không nên xóa khi học?

- Có thể bỏ ảnh đẹp hoặc thay CSS mà giữ nghiệp vụ. Không bỏ id/selector nút trong HTML nếu JS vẫn dùng chúng; JS sẽ không tìm thấy element và có thể dừng khởi tạo.
- Có thể bỏ nhịp quan sát bằng setting 0; không phải xóa worker/queue. Tạo PDF/SMTP vẫn diễn ra.
- Có thể bỏ Kafka khỏi buổi học RabbitMQ bằng cách không bật profile. Không xóa chỉ một nhánh import nhưng giữ nút và test Kafka rồi cho rằng dự án vẫn hoàn chỉnh.
- Có thể bỏ công cụ build PDF tài liệu nếu chỉ chạy demo; mất khả năng tái tạo tài liệu in, không mất invoice vì invoice có render riêng.
- Không bỏ validation giá server, transaction, unique keys, outbox và ACK muộn nếu vẫn muốn bảo vệ các tình huống lỗi đang trình bày.
- Không xóa dữ liệu chỉ vì “không phải code”. DB và volume có thể chứa đơn cần giữ; reset hoặc dọn dữ liệu là hành động riêng.

Các file v3 trùng mục tiêu đã được loại khỏi source: API/worker/producer/SQLite cũ, Kafka replay riêng cũ, legacy HTML/JS/CSS, test v3 và hướng dẫn v3. Chỉ còn bộ API/store/broker/worker/fulfillment của cửa hàng. Phần connection AMQP dùng chung trước đây được chuyển vào broker của cửa hàng, không xóa mất nó. Endpoint `/lab` và `/api/state` v3 không còn; dùng `/api/shop/state`.

Các môi trường cá nhân như .venv/.vscode và dữ liệu đang có không bị tự xóa. Những bản test/cache có thể cất ngoài thư mục dự án sau khi dừng container tương ứng. Giấy phép font phải giữ khi phát hành font.

## 17. Hai mươi câu hỏi GV hay xoáy và cách trả lời ngắn

**1. Sao không viết hết trong một file?** “Có thể, nhưng HTTP, lưu trữ, giao tiếp broker, consumer và nghiệp vụ có trách nhiệm khác nhau. Năm module đủ lần theo luồng mà không tạo thêm tầng trừu tượng.”

**2. Producer chính xác nằm đâu?** “Dispatcher trong api.py trực tiếp publish qua broker.py. Handler checkout tạo order/outbox; việc ghi outbox chưa phải broker đã nhận message.”

**3. API có chờ worker không?** “Không chờ PDF/email; nó chờ phần validation và transaction DB. HTTP vẫn có xử lý đồng bộ bên trong, nhưng nghiệp vụ phía sau được tách qua queue.”

**4. Sao đã có queue vẫn cần DB?** “Queue giữ yêu cầu làm việc, DB giữ sự thật nghiệp vụ và trạng thái/idempotency. Message chỉ tham chiếu mã đơn, không phải toàn bộ dữ liệu cửa hàng.”

**5. Hai consumer cùng queue có cùng nhận một message không?** “Bình thường broker phân phối delivery cho một consumer; nhiều consumer chia việc. Vẫn có thể gặp cùng task ở lần giao lại hoặc khi producer publish lặp.”

**6. Em chứng minh async bằng gì?** “Đặt hàng lúc 0 worker: nhận 202, DB có đơn/outbox, queue có Ready nhưng chưa có PDF. Bật từng role mới xuất hiện PDF rồi email.”

**7. Em chứng minh worker thật bằng gì?** “Mỗi worker có PID, log riêng và callback nhận message; kill process làm kết nối broker mất và có giao lại hoặc Kafka rebalance.”

**8. Có ACK thì không bao giờ mất/trùng?** “Không. ACK chỉ xác nhận một lượt giao. Cần xét thời điểm crash, idempotency, xác nhận publisher, loại queue, replication và dữ liệu bên ngoài broker.”

**9. Có thể ACK ở frontend không?** “Không dùng luồng đó ở đây. Frontend chỉ gọi HTTP; delivery_tag/channel thuộc consumer đang kết nối broker.”

**10. REDELIVER là em tự giả lập chữ?** “Với RabbitMQ, code dựa vào method.redelivered từ broker và log lượt nhận. Tình huống crash thật khiến broker giao lại. Với Kafka, chữ đó là log app suy ra task đã từng nhận, không phải cờ AMQP.”

**11. DEDUP có phải RabbitMQ tự làm?** “Không. Em kiểm bản ghi invoice unique theo order_id và chỉ tạo email outbox ở lần insert invoice thành công.”

**12. Tắt email worker có mất đơn không?** “Không vì công việc email còn ở queue/outbox theo trạng thái publish. PDF và đơn đã lưu vẫn có; thêm email consumer để tiếp tục.”

**13. PDF nằm đâu nếu xóa container?** “Với Compose hiện tại, data là bind mount nên còn trên máy host. Xóa thư mục data là xóa dữ liệu khác với chỉ recreate web container.”

**14. Khi broker tắt có nhận đơn được không?** “Có nếu DB/web còn hoạt động. Transaction lưu order/outbox; dispatcher gửi lại sau. DB hỏng thì không có cơ sở để trả 202 thành công.”

**15. Nếu web process chết thì sao?** “Đơn/outbox đã commit còn trong data. Khởi động web lại chạy dispatcher, nhưng worker cần được mở lại. Đó là giới hạn quản lý process cục bộ của demo.”

**16. DLQ có tự retry theo lịch không?** “Không ở bản này. Người vận hành sửa/ra quyết định rồi retry; tự động retry/backoff là phần cần thiết kế thêm khi triển khai.”

**17. Kafka không thể retry à?** “Có. Demo có retry tại partition hoặc chuyển topic lỗi rồi đi tiếp. Em chỉ ra chi phí thiết kế offset/thứ tự trong mô hình đang chọn.”

**18. Thêm hai partition có làm Kafka bằng RabbitMQ không?** “Có thể cho nhiều consumer có assignment hơn, nhưng hai hệ vẫn khác mô hình log và work queue. Cần xét key, thứ tự, retention và nhu cầu, không kết luận chung từ số worker.”

**19. Demo này đã là hệ thống SOA production chưa?** “Chưa. Có tách HTTP, broker và worker nhưng vẫn chia sẻ SQLite/file local cho dễ clone. Chứng minh cơ chế tích hợp bất đồng bộ, không phải hoàn chỉnh ranh giới dịch vụ thương mại.”

**20. Em tự hiểu code hay chỉ nhờ AI?** “Em có dùng AI hỗ trợ, nhưng em lần được checkout→transaction/outbox→publish→consumer→PDF/SMTP→ACK; em chỉ rõ các điểm lỗi, code bảo vệ và giới hạn còn lại. Em có kiểm thử thực tế thay vì chỉ đọc tên pattern.”

## 18. Cách tập đọc code trong 40 phút

10 phút đầu: mở sơ đồ, tìm handler checkout trong JS và route checkout trong api.py; đọc create_order đến enqueue. Đặt một đơn lúc 0 worker và đối chiếu DB/outbox, không nhảy ngay sang code Kafka.

10 phút tiếp: mở broker.topology và worker.rabbit. Viết ra ba dòng riêng “publish confirm”, “fulfill”, “consumer ACK”; giải thích bên nào gửi mỗi loại xác nhận. Thêm worker hóa đơn trước, xem PDF; thêm worker email sau, mở Mailpit.

10 phút tiếp: mở commit_invoice và nhánh crash_after_commit. Dự đoán nếu process chết trước/sau commit thì việc gì còn; chạy thí nghiệm để kiểm tra dự đoán. Tập nói giới hạn SMTP trùng bằng chính hai dòng send_message/record_sent.

10 phút cuối: mở Kafka consumer config, on_assign, commit và pause. So sánh cùng 4 worker/2 partition; chạy lỗi rồi bấm chuyển topic lỗi, cuối cùng replay. Chọn câu hỏi ở phần 17 và trả lời không nhìn tài liệu.

Không cần học thuộc import, từng mã màu CSS hoặc tên class của ReportLab. Cần tự chỉ được đúng file/hàm và giải thích thứ tự gây ra kết quả. Khi muốn thử xóa/đổi một cơ chế, dùng nhánh Git hoặc bản clone thử và dữ liệu riêng; đừng phá bản chạy buổi seminar.

## 19. Nguồn chính thức để kiểm tra lời giải thích

Các diễn giải là tóm lược gắn với code demo, không trích nguyên văn. RabbitMQ /docs có thể mở phiên bản mới hơn image đang chạy; dùng bộ khái niệm đúng loại queue và cấu hình, không tự đổi image trước buổi trình bày.

- [RabbitMQ Work Queues](https://www.rabbitmq.com/tutorials/tutorial-two-python): work distribution, ACK, persistence và prefetch.
- [Consumer Acknowledgements and Publisher Confirms](https://www.rabbitmq.com/docs/confirms): hai chặng xác nhận độc lập, requeue và giới hạn.
- [RabbitMQ Dead Letter Exchanges](https://www.rabbitmq.com/docs/dlx): điều kiện dead-letter, binding, policy và an toàn.
- [Kafka 4.2 Design](https://kafka.apache.org/42/design/design/): log, partition/group, offset và delivery semantics.
- [RabbitMQ Streams](https://www.rabbitmq.com/docs/streams): log/replay ngoài classic work queues.
- [Mailpit API](https://mailpit.axllent.org/docs/api-v1/): xem message, raw MIME và attachment để đối chiếu.

Phụ lục vị trí hàm trong PDF được lấy từ AST của chính source khi tạo tài liệu, tránh học theo số dòng của bản file cũ.
