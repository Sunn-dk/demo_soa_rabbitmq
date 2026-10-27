# RabbitMQ Seminar Lab

Bài thực hành SOA trong 15–20 phút: nhìn thấy message đi qua producer, exchange, queue và consumer. Giao diện dùng RabbitMQ thật, worker là tiến trình riêng. Kafka là phần mở rộng về replay, không phải mô phỏng bất lợi cho Kafka.

## Chạy nhanh trên máy của bạn

1. Cài [Docker Desktop](https://docs.docker.com/get-started/introduction/get-docker/). Windows dùng Linux containers/WSL 2 theo hướng dẫn của Docker. macOS/Linux dùng Docker Engine và Compose tương ứng.
2. Mở Docker Desktop và đợi Engine chạy. Cài [Git](https://git-scm.com/downloads), sau đó mở PowerShell/Terminal và clone:

```powershell
git clone https://github.com/Sunn-dk/demo_soa_rabbitmq.git
cd demo_soa_rabbitmq
```

3. Khởi động RabbitMQ và web, đợi cả hai báo healthy:

```powershell
docker compose up -d --build --wait --wait-timeout 180
```

4. Mở [dashboard](http://localhost:8000). Ban đầu có **0 worker** để chứng minh bất đồng bộ. Bấm **Gửi 1 job qua RabbitMQ**, đợi số Ready cập nhật, rồi bấm **+ Thêm worker**.

Không cần cài Python/Node trên máy nếu chạy bằng Docker. Lần đầu cần Internet để tải image và thư viện. Có thể bấm `start-demo.cmd` trên Windows thay cho gõ lệnh. Nếu cổng 8000/5672/15672 đã có ứng dụng khác sử dụng, dừng bản demo cũ trước.

Hướng dẫn Word để tải về hoặc gửi cho người học: [Hướng dẫn clone và chạy demo](docs/Huong_Dan_Clone_Va_Chay_Demo_SOA.docx).

| Địa chỉ | Mục đích |
|---|---|
| http://localhost:8000 | Demo và hướng dẫn thao tác ngay trên web |
| http://localhost:8000/docs | API để cả lớp thử lại |
| http://localhost:15672 | RabbitMQ Management, `guest` / `guest` |

Các cổng được bind vào 127.0.0.1 để chạy trên từng máy. Mỗi bạn chạy bản riêng, không cần truy cập máy của người thuyết trình.

## Những thí nghiệm cần làm

| Thí nghiệm | Thao tác | Bằng chứng cần chỉ ra |
|---|---|---|
| Bất đồng bộ | 0 worker, gửi 1 job | HTTP 202 sau confirm, Ready tăng, hóa đơn chưa có |
| Message cụ thể | Bấm Job ID trong bảng | Body, properties, exchange, routing key của cùng ID |
| Chia việc | Gửi 6 job, thêm worker thứ hai | Hai job processing trên hai worker khác nhau |
| Thứ tự | 2 worker rảnh, queue trống, bấm **Thứ tự: gửi 6s rồi 1s** | Job gửi sau hoàn tất trước; FIFO không tự điều phối phụ thuộc nghiệp vụ |
| Worker sập | Job 6 giây, bấm Dừng/crash khi processing | REDELIVER, cùng ID, lượt nhận tăng |
| Sập sau ghi DB | Giữ 1 worker, bấm **Sập sau COMMIT, trước ACK**, sau đó thêm worker mới | COMMIT, CRASH_BEFORE_ACK, REDELIVER, DEDUP; 2 lượt nhận nhưng 1 hóa đơn |
| Tin lỗi | Gửi job lỗi rồi job thường | NACK, DLQ tăng, job thường vẫn chạy; sửa và retry được |
| Kafka replay | Bật Kafka, chạy lab cuối trang | Group B đọc lại 6 event đã được group A đọc và commit |

Thời gian 1/3/6 giây là **nghiệp vụ cố ý làm chậm**, không phải RabbitMQ mất từng đó thời gian để truyền tin. Chỉ số HTTP đo cả kết nối, khai báo topology, publish confirm và ghi log. Không dùng nó để kết luận RabbitMQ nhanh hơn Kafka.

Ready/Unacked/DLQ lấy từ Management API, có độ trễ lấy mẫu vài giây. Bảng job và event log là quan sát ứng dụng, không phải bản sao chính xác tức thời của bộ nhớ broker. Trạng thái `processing` còn có thể là quan sát cuối trước khi worker sập.

## Bật phần Kafka (tùy chọn)

```powershell
docker compose --profile kafka up -d --wait --wait-timeout 240
```

Đợi Kafka khởi động, sau đó bấm **Chạy thí nghiệm Kafka replay**. Mỗi lần tạo topic riêng, 3 partition, 6 event, retention 1 giờ. Group A đọc và commit, group B mới dùng `earliest` đọc lại. Đợi khoảng vài đến vài chục giây, không bấm liên tục. Topic vẫn tồn tại sau khi dữ liệu hết retention.

Kafka lab dùng **Consumer Groups truyền thống**. Kafka Share Groups là một cơ chế khác có chia sẻ record và ACK từng record. RabbitMQ cũng có Streams để lưu log và replay. Xem [tài liệu học](docs/TAI_LIEU_HOC.md) để tránh khái quát sai.

## Reset và dừng

Để reset từ web, dừng mọi worker bằng nút Dừng/crash, chờ broker nhận mất kết nối, sau đó bấm **Reset dữ liệu demo**. Nếu còn worker CLI thì dừng terminal đó. Reset chỉ dọn queue `seminar.v3.invoices`, DLQ tương ứng và SQLite của demo, không dọn queue khác.

```powershell
docker compose --profile kafka down
```

Lệnh dừng giữ dữ liệu broker trong Docker volumes, dữ liệu ứng dụng ở `data/`. API khởi động lại **không tự purge queue**. Khi mở lại, thêm worker để xử lý backlog còn lại. Chỉ chạy một web container/process cho bộ điều khiển worker cục bộ này.

## Đọc code theo luồng chạy

| File | Vai trò |
|---|---|
| `app.py` | API nhận job, HTTP 202/503, mở và dừng worker |
| `broker.py` | Direct exchange, binding, queue durable, persistent message, mandatory publish |
| `producer.py` | Producer CLI dùng cùng queue với dashboard |
| `worker.py` | basic_consume, prefetch=1, ACK/NACK, crash thật |
| `store.py` | Hóa đơn và log SQLite, UNIQUE(job_id), transaction chống hiệu ứng trùng |
| `kafka_lab.py` | Publish và đọc lại log Kafka thật bằng hai group |
| `worker2.py` | Entry point tương thích gọi Kafka lab mới |
| `static/lab.js`, `static/lab.css` | Dashboard tiếng Việt, không tải thư viện frontend bên ngoài |

Queue `seminar.v3.invoices` là classic queue trên **một broker**. Demo chưa có replication/HA. DLX khai báo bằng arguments cho dễ mang đi học; hệ thống thực thường dùng policy để dễ thay đổi. Classic dead-lettering có giới hạn an toàn khi chặng đích lỗi. Không gọi cấu hình này là “không bao giờ mất tin”.

## Chạy CLI để xem ít dòng code hơn

Sau khi khởi động Docker:

```powershell
docker compose exec web python producer.py --count 3 --seconds 3
```

Ở terminal thứ hai:

```powershell
docker compose exec web python worker.py CLI-1
```

Nhấn Ctrl+C để dừng consumer CLI. CLI và web dùng **cùng queue**. Worker CLI xuất hiện trong consumer count/log; nó không nằm trong danh sách process do nút trên web quản lý.

Nếu muốn chạy Python trực tiếp trên máy, dùng Python 3.12 và tạo môi trường riêng:

```powershell
docker compose stop web
python -m venv .venv-seminar
.\.venv-seminar\Scripts\python.exe -m pip install -r requirements.txt
.\.venv-seminar\Scripts\python.exe -m uvicorn app:app --host 127.0.0.1 --port 8000
```

Để tắt phiên Python dùng Ctrl+C. Không chạy đồng thời web container và API local trên cùng cổng hoặc cùng DB. Docker là cách khuyến nghị khi cả lớp thực hành.

## Cập nhật bản clone

Dừng các worker, đợi hết việc cần giữ rồi chạy:

```powershell
git pull --ff-only
docker compose up -d --build --wait --wait-timeout 180
```

Nếu đang dùng Kafka, thêm `--profile kafka` vào lệnh Compose. Khi chuyển từ bản demo cũ sang bản mới, nên clone vào thư mục mới và dùng các cổng còn trống. Nếu đã sửa code trong bản clone, hãy lưu thay đổi đó trước khi pull.

## Cấu hình cục bộ và đổi cổng

Mặc định chạy ngay được, không cần `.env`. `.env.example` chỉ chứa giá trị demo công khai. Nếu cần đổi cổng, sao chép thành `.env` rồi sửa các giá trị tương ứng:

```powershell
Copy-Item .env.example .env
```

Trên macOS/Linux dùng `cp .env.example .env`. Ví dụ `WEB_PORT=8080` thì mở `http://localhost:8080`. Cổng trong container vẫn giữ nguyên. Nếu đổi cổng Management, dùng cổng đã đặt khi mở trang quản trị; liên kết nhanh trên web dùng cổng mặc định. Đổi user/password RabbitMQ chỉ có hiệu lực khởi tạo với volume dữ liệu mới; không tự đổi tài khoản trong volume cũ.

Không commit `.env`, mật khẩu thật, token hay database. Repo chỉ chứa code, cấu hình mẫu và tài liệu. `guest/guest` là tài khoản demo cục bộ, các cổng chỉ lắng nghe trên 127.0.0.1.

## Lỗi thường gặp

| Hiện tượng | Kiểm tra và xử lý |
|---|---|
| `docker` chưa có / cannot connect daemon | Cài và mở Docker Desktop, đợi Engine khởi động |
| Cổng bị chiếm | `docker ps`; dừng demo cũ hoặc ứng dụng dùng cùng cổng |
| Web chưa mở ngay sau up | `docker compose ps`, `docker compose logs --tail 30 web`; đợi khởi động xong |
| HTTP 503 lúc gửi job | Broker chưa sẵn sàng hoặc chưa xác định được kết quả publish; kiểm tra log/job_id trước khi gửi lại |
| Ready/Unacked hiển thị trễ | Số đếm broker lấy mẫu theo chu kỳ; đợi vài giây, đối chiếu Management |
| Worker đã dừng nhưng job còn processing | Đây là quan sát cuối của ứng dụng; bật worker mới để broker giao lại |
| Không thấy job cũ trên bảng | Bảng hiển thị 100 job mới nhất, tổng hóa đơn vẫn đếm toàn DB |
| Kafka lỗi kết nối | Bật profile kafka, đợi broker sẵn sàng, xem `docker compose logs --tail 30 kafka` |
| Reset bị từ chối | Dừng tất cả worker web và CLI trước |

## Tài liệu kèm theo

- [Kịch bản nói và thao tác trong 18 phút](docs/KICH_BAN_SEMINAR.md)
- [Nguồn chính thức, lộ trình học và câu hỏi tự kiểm tra](docs/TAI_LIEU_HOC.md)
- [Hướng dẫn clone và chạy để chia sẻ cho lớp](docs/Huong_Dan_Clone_Va_Chay_Demo_SOA.docx)
- [Kiểm thử với broker thật](tests/README.md)
