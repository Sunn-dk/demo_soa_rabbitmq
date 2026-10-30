# Đánh giá demo sau kiểm thử ngày 31/10/2026

## Kết luận theo bằng chứng

**Đủ ý nghĩa và trực quan cho seminar về RabbitMQ trong luồng xử lý đơn hàng.** Sản phẩm đầu ra là đơn được tính tiền/lưu ở server, PDF có chi tiết và email SMTP có PDF thật. Giá trị queue xuất hiện khi chưa có consumer, có backlog, worker sập hoặc một email sai. Không chỉ là mô phỏng giao diện đổi trạng thái.

**Chưa phải chứng minh một cửa hàng production đã hoàn chỉnh.** Không có thu tiền, tồn kho, vận chuyển thật, auth, nhiều máy, replication/HA, benchmark tải lớn hoặc bảo đảm không gửi email trùng trong mọi trường hợp. Mailpit là SMTP cục bộ. Phần Kafka cho thấy khác biệt của cấu hình classic group hai partition và chính sách giữ thứ tự của ứng dụng.

## Môi trường và cách thực hiện

Kiểm tra bản cửa hàng đã gom vào package shop, dùng Python 3.12 trong Docker, RabbitMQ 4.3.6, Kafka 4.2.0, Mailpit 1.27.4. Dựng Compose project thử riêng `moc-final-20261031`, web 18000, Mailpit 18025, DB/volume thử riêng; không reset DB đơn hàng của bản chính. Source frontend sau định dạng và backend sau di chuyển đã được build vào image kiểm tra.

Chạy lại sáu unit tests, bài integration với broker thật, kiểm tra nghiệp vụ ở nhịp 0s, bài trình duyệt mua hàng và bài trình duyệt so sánh. Dùng Edge qua Playwright ở 1440×1000 và 390×844. Không quay video.

## Những gì đã qua kiểm tra

- **PASS - 0 consumer:** HTTP 202 thật, giá server 523.000 đ cho hai áo và giao hàng, cùng key trả cùng đơn, queue Ready=1, chưa có invoice.
- **PASS - hai vai trò tách biệt:** consumer invoice tạo PDF khi email consumer chưa mở; thêm email consumer mới có SMTP/email.
- **PASS - PDF và attachment:** có file PDF thật, email có application/pdf lớn hơn 1 KB. Unit test trích được tên tiếng Việt, tên sản phẩm và tổng tiền trong PDF.
- **PASS - task trùng:** gửi lại task invoice cùng mã, có DEDUP, không tăng số invoice/email đã ghi.
- **PASS - cùng nghiệp vụ hai broker:** 6 đơn mỗi bên, tổng 12 đơn tạo PDF và email qua cùng fulfillment.
- **PASS - chia việc:** RabbitMQ có 4 consumer hóa đơn; Kafka 4 consumer nhưng chỉ 2 consumer có assignment trên topic hai partition, 2 consumer unassigned.
- **PASS - email lỗi:** RabbitMQ cô lập một tin lỗi và gửi hai thư tốt; Kafka cùng partition 0 pause theo chính sách hiện tại, hai thư tốt chưa đi tiếp trước quyết định xử lý.
- **PASS - Kafka DLQ thật:** chuyển lỗi publish được record trong topic dead; group mới đọc thấy record đó; hai email tốt phía sau tiếp tục.
- **PASS - sửa và retry:** giữ mã đơn/task, sửa email và gửi được thư ở cả hai transport.
- **PASS - crash sau commit trước ACK/offset:** cả hai transport nhận lại invoice task, có DEDUP và nhiều lượt nhận nhưng không tạo thêm invoice.
- **PASS - analytics:** group mới đọc log thực và không làm tăng invoice/email.
- **PASS - RabbitMQ tạm dừng:** checkout vẫn lưu order/outbox; sau khi broker trở lại, tác vụ tiếp tục và đơn hoàn tất.
- **PASS - nghiệp vụ nhịp 0s:** hai dòng sản phẩm, tổng 1.047.000 đ, miễn giao hàng; byte PDF lấy từ MIME email có SHA-256 giống PDF tải qua API.
- **PASS - dữ liệu không hợp lệ:** email sai, size không tồn tại và dùng key cũ với nội dung khác bị HTTP 422; giá client gửi không được dùng tính tiền.
- **PASS - giao diện:** đặt đơn bằng form, 202, mở đúng PDF của đơn vừa đặt; bốn tab không tràn ngang ở 390px, không có lỗi JavaScript pageerror trong bài kiểm tra.
- **PASS - nút so sánh:** chuẩn bị worker, nạp hai lô 6 đơn, quan sát 2/3 và 0/3 thư, chuyển lỗi rồi cả hai bên 2/3; replay hiển thị partition/offset thật.
- **PASS - dọn lab:** đường dẫn /lab, /api/state cũ và tài nguyên legacy trả 404; luồng /api/shop tiếp tục chạy sau khi chuyển package.

Sáu unit tests còn kiểm tra: request đồng thời cùng key chỉ tạo một đơn; rollback invoice/email outbox cùng nhau; backlog Kafka không chặn truy vấn outbox RabbitMQ; assignment Kafka được giữ khi worker đổi trạng thái đang làm việc.

## Một sai lệch của kiểm thử đã được sửa

Lần kiểm tra ban đầu dừng ở phép so sánh tổng số email trước/sau task trùng. Snapshot lấy counters và chi tiết đơn bằng các lần đọc riêng nên có thể thấy order completed trong khi counters ở lần đọc trước vẫn là 0. Quan sát sau đó có đúng một invoice/một email, không phải đã tạo hai email. Điều kiện test được đổi thành chờ cả trạng thái và counters ổn định trước khi lấy mốc so sánh; thêm guard chỉ chạy integration trên DB thử trống. Bài integration sau sửa chạy qua toàn bộ. Đây không phải kiểm thử chứng minh snapshot có tính nguyên tử.

## Cách trình bày để người nghe thấy ý nghĩa

1. Bắt đầu với 0 worker, đặt hàng qua cửa hàng chứ không chỉ nạp job. Chỉ rõ đơn đã lưu nhưng PDF chưa có.
2. Mở consumer invoice trước, email sau; tải file và mở thư. Giữ cùng mã đơn để người nghe nối được các bước.
3. Khi nói chia việc, đặt nhịp 4s và thêm worker trong lúc backlog còn. Nếu để 0s có thể hoàn tất nhanh đến mức khó nhìn, dù nghiệp vụ vẫn thật.
4. Dùng email sai để nói về cô lập lỗi: một khách cần sửa thông tin không giữ các khách tốt trong work queue.
5. Dùng crash sau commit để nói về độ tin cậy: không chỉ “bật lại worker”, mà phải giải thích vì sao nhận lại không được tạo thêm hóa đơn.
6. Với Kafka, chỉ assignment/partition và chính sách xử lý lỗi. Thử luôn cách gỡ lỗi Kafka và đọc lại log; không dùng chênh lệch vài giây để kết luận sản phẩm nào nhanh hơn.

Ở máy chiếu nên zoom trình duyệt 110–125% nếu chữ trong log nhỏ. Đừng đọc toàn bộ JSON/event; chọn một mã đơn và ba đến năm mốc liên quan. Chỉ số Ready/Unacked có thể trễ lấy mẫu nên đối chiếu cùng log/worker, không đợi mọi ô đổi đồng thời.

## Phạm vi chưa kiểm tra và rủi ro cần nói đúng

Chưa đo tải lớn, ngắt điện/đĩa, nhiều node, failover, lỗi mạng mọi thời điểm, email Internet hay gửi SMTP trùng trong mọi crash window. Bài này không kiểm thử việc người dùng học có hiểu hết hay dự đoán được điểm giảng viên cho. Bảo đảm trong demo chỉ đúng dữ liệu/cấu hình và các tình huống đã thử.

SMTP và DB không có transaction chung; email có thể lặp. Classic DLX một broker có giới hạn an toàn. Mọi dữ liệu dùng chung SQLite/file volume nên chưa phải phân ranh giới dịch vụ độc lập hoàn chỉnh. UI/API điều khiển process chỉ dùng cục bộ và một web process. Các lỗi tạm thời của SMTP/PDF hiện vào DLQ, cần thao tác retry; chưa có retry/backoff tự động theo nhóm lỗi.

Không cần thêm hệ thanh toán/tồn kho chỉ để demo “trông thật hơn”; chúng làm loãng cơ chế RabbitMQ. Với mục tiêu seminar, luồng đơn→PDF→email cộng backlog/lỗi/phục hồi đã có đầu vào, đầu ra và lý do kỹ thuật rõ ràng.
