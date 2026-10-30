"""Build the print guide from the same Markdown documentation shipped in Git."""
from html import escape
import ast
import hashlib
from pathlib import Path
import re
import textwrap
from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Preformatted, Flowable
from reportlab.platypus.tableofcontents import TableOfContents

ROOT=Path(__file__).resolve().parents[1]
pdfmetrics.registerFont(TTFont('Guide',str(ROOT/'static/fonts/DejaVuSans.ttf')))
pdfmetrics.registerFontFamily('Guide',normal='Guide',bold='Guide',italic='Guide',boldItalic='Guide')
styles={
    'body':ParagraphStyle('body',fontName='Guide',fontSize=9.5,leading=15,spaceAfter=8),
    'h1':ParagraphStyle('h1',fontName='Guide',fontSize=21,leading=28,textColor=colors.HexColor('#25543e'),spaceAfter=19),
    'h2':ParagraphStyle('h2',fontName='Guide',fontSize=13,leading=19,textColor=colors.HexColor('#25543e'),spaceBefore=15,spaceAfter=8,keepWithNext=True),
    'code':ParagraphStyle('code',fontName='Guide',fontSize=8,leading=12,backColor=colors.HexColor('#f1f3ed'),borderPadding=8,spaceAfter=12),
}


class GuideDocument(SimpleDocTemplate):
    def afterFlowable(self,flowable):
        if isinstance(flowable,Paragraph) and flowable.style.name in ('h1','h2'):
            self.notify('TOCEntry',(0 if flowable.style.name=='h1' else 1,flowable.getPlainText(),self.page))


class Lanes(Flowable):
    """Two simple process lanes; vector text stays sharp when printed."""
    def __init__(self,left_title,left,right_title,right):
        Flowable.__init__(self)
        self.width=500;self.height=415
        self.columns=[(left_title,left),(right_title,right)]

    def draw(self):
        c=self.canv
        for column,(title,steps) in enumerate(self.columns):
            x=column*260;c.setFont('Guide',10);c.setFillColor(colors.HexColor('#25543e'))
            c.drawString(x,self.height-12,title)
            for i,step in enumerate(steps):
                y=self.height-72-i*61
                c.setFillColor(colors.HexColor('#f1f4ee'));c.setStrokeColor(colors.HexColor('#b5c7b8'))
                c.roundRect(x,y,238,45,6,fill=1,stroke=1)
                c.setFont('Guide',9);c.setFillColor(colors.HexColor('#253d2d'))
                for j,line in enumerate(step.split('\n')):c.drawString(x+10,y+27-j*14,line)
                if i<len(steps)-1:
                    c.setStrokeColor(colors.HexColor('#25543e'));c.line(x+119,y-1,x+119,y-13)
                    c.line(x+119,y-13,x+115,y-9);c.line(x+119,y-13,x+123,y-9)


def code_block(lines):
    wrapped=[]
    for line in lines:
        wrapped.extend(textwrap.wrap(line,width=90,replace_whitespace=False,drop_whitespace=False,subsequent_indent='    ') or [''])
    return Preformatted('\n'.join(wrapped),styles['code'])

def inline(text):
    text=escape(text)
    text=re.sub(r'\[([^]]+)\]\(([^)]+)\)',lambda m:'<link href="'+m[2]+'" color="#25543e">'+m[1]+'</link>',text)
    text=re.sub(r'\*\*(.+?)\*\*',r'<b>\1</b>',text)
    return text.replace('`','')

story=[Paragraph('MỘC STUDIO / SEMINAR RABBITMQ',styles['h1']),
       Paragraph('Hướng dẫn chạy, lời thuyết trình và giải thích code',styles['h2']),
       Paragraph('Bản ngày 31/10/2026, cấu trúc shop/. Giải thích cho người mới đọc code: chức năng, lý do dùng, hậu quả khi xóa/đổi, câu trả lời GV và vị trí hàm lấy từ source hiện tại.',styles['body']),
       Paragraph('Chạy nhanh',styles['h2']),
       code_block(['git clone https://github.com/Sunn-dk/demo_soa_rabbitmq.git','cd demo_soa_rabbitmq','docker compose --profile kafka up -d --build --wait --wait-timeout 240']),
       Paragraph('Cài Git và Docker Desktop, mở Docker trước. Web: http://localhost:8000. Hộp thư Mailpit: http://localhost:8025. RabbitMQ Management: http://localhost:15672 (guest / guest). Không cần cài Python/Node khi chạy Docker.',styles['body']),
       Paragraph('Bắt đầu với 0 worker. Đặt đơn, thêm consumer tạo hóa đơn, sau đó thêm consumer gửi email. Hai bước tách biệt để quan sát bất đồng bộ. Mailpit nhận SMTP và file PDF thật trong máy; không gửi thư ra Internet.',styles['body']),
       Paragraph('Các phần trong bản in',styles['h2']),
       Paragraph('Đọc sơ đồ trước, sau đó sổ tay theo từng bước. Dùng phần câu hỏi để tập trả lời; phụ lục có dòng mã và trích đoạn thật. Kịch bản thao tác vẫn dành cho 15-20 phút; sổ tay dài hơn để học và tra cứu.',styles['body']),PageBreak()]

toc=TableOfContents()
toc.levelStyles=[ParagraphStyle('toc1',fontName='Guide',fontSize=10,leading=15,spaceBefore=8),
                 ParagraphStyle('toc2',fontName='Guide',fontSize=8.5,leading=13,leftIndent=12)]
story += [Paragraph('Mục lục',styles['h1']),toc,PageBreak(),Paragraph('Sơ đồ file và hai chặng xử lý',styles['h1']),
          Paragraph('Đường HTTP bên trái chốt đơn trước. Dispatcher bên phải chạy độc lập; nó đọc outbox chứ không đợi người dùng bấm tạo PDF.',styles['body']),
          Lanes('A / NHẬN ĐƠN',[
              'static/index.html + shop.js\nKhách điền form, fetch checkout',
              'app.py\nFastAPI gắn router shop.api',
              'shop/api.py: checkout\nKiểm dữ liệu qua model',
              'shop/store.py: create_order\nCommit order + task + outbox',
              'data/shop.sqlite3\nLưu bền ý định phải xử lý',
              'HTTP 202 về trình duyệt\nChưa đồng nghĩa PDF/email xong'],
              'B / TẠO HÓA ĐƠN',[
              'shop/api.py: dispatch\nĐọc outbox pending',
              'shop/broker.py: publish\nExchange route queue invoice',
              'RabbitMQ\nConfirm publisher, giữ backlog',
              'shop/worker.py: rabbit\nNhận delivery, chưa ACK',
              'shop/fulfillment.py: fulfill\nTạo PDF, commit invoice + outbox email',
              'shop/worker.py: basic_ack\nSau xử lý, chặng email còn riêng']),
          Paragraph('store.py giữ DB; broker.py giữ cách nói chuyện broker; worker.py điều phối nhận/xác nhận; fulfillment.py làm nghiệp vụ. static/shop.css chỉ trình bày. Mũi tên chỉ thứ tự công việc, không phải tất cả đều là lời gọi Python trực tiếp.',styles['body']),PageBreak(),
          Paragraph('Sơ đồ email và khôi phục',styles['h1']),
          Lanes('C / EMAIL KÈM PDF',[
              'api.dispatch -> broker.publish\nTác vụ role=email từ outbox',
              'RabbitMQ queue email\nChờ consumer vai trò email',
              'worker.py -> fulfillment.py\nKiểm sổ gửi và file invoice',
              'fulfillment.py: send_message\nSMTP + MIME attachment thật',
              'Mailpit nhận thư -> record_sent\nSMTP và DB không chung transaction',
              'worker.py ACK sau xử lý\nUI đọc state, mở thư ở Mailpit'],
              'D / CRASH VÀ LỖI',[
              'Crash sau commit invoice\nChưa ACK: delivery còn cần xác nhận',
              'RabbitMQ giao lại\nWorker khác ghi REDELIVER',
              'DB đã có invoice -> DEDUP\nKhông thêm invoice/email outbox',
              'Riêng email sai: NACK\nDLX route vào queue failed',
              'Sửa dữ liệu và retry có confirm\nGiữ cùng order_id/task_id',
              'Kafka: pause partition lỗi\nRetry hoặc DLQ rồi commit/resume']),
          Paragraph('Email vẫn có thể lặp nếu SMTP đã nhận nhưng process chết trước khi ghi sổ. Kafka pause ở đây là chính sách giữ thứ tự của app; partition khác không bị bắt buộc dừng. Sơ đồ Mermaid từng nút/file nằm ở docs/LUONG_XU_LY.md.',styles['body'])]

for name in ['SO_TAY_DOC_CODE.md','DEMO_CUA_HANG.md','SO_SANH_KAFKA.md','KET_QUA_KIEM_THU.md']:
    story.append(PageBreak())
    lines=(ROOT/'docs'/name).read_text(encoding='utf8').splitlines()
    paragraph=[];code=[];in_code=False
    def flush():
        if paragraph:
            story.append(Paragraph(inline(' '.join(paragraph)),styles['body']));paragraph.clear()
    for line in lines:
        if line.startswith('```'):
            flush()
            if in_code:
                story.append(code_block(code));code=[]
            in_code=not in_code;continue
        if in_code:code.append(line);continue
        if line.startswith('|'):
            flush()
            cells=[x.strip() for x in line.strip('|').split('|')]
            if all(re.fullmatch(r'[-: ]+',x) for x in cells):continue
            # Paragraph rows allow the comparison to print at a readable size.
            labels=['Bài toán','RabbitMQ','Kafka','Kết luận']
            if cells[0]=='Bài toán':continue
            if len(cells)==4:
                story.append(Paragraph(inline(' · '.join(labels[i]+': '+c for i,c in enumerate(cells))),styles['body']))
            continue
        if not line.strip():flush();continue
        if line.startswith('# '):flush();story.append(Paragraph(inline(line[2:]),styles['h1']));continue
        if line.startswith('## '):flush();story.append(Paragraph(inline(line[3:]),styles['h2']));continue
        if line.startswith('- '):flush();story.append(Paragraph('- '+inline(line[2:]),styles['body']));continue
        paragraph.append(line)
    flush()

# Source positions are computed, never carried over from the old root modules.
story += [PageBreak(),Paragraph('Phụ lục: mở đúng hàm, đúng dòng',styles['h1']),
          Paragraph('Dòng được tính trực tiếp từ source khi build ngày 31/10/2026. Đổi code thì tạo lại PDF. Mã SHA-256 đầu mỗi file dùng đối chiếu phiên bản nội dung, không phải mật khẩu.',styles['body'])]
for relative in ['app.py','shop/api.py','shop/store.py','shop/broker.py','shop/worker.py','shop/fulfillment.py']:
    source=(ROOT/relative).read_text(encoding='utf8')
    story.append(Paragraph(relative+' | SHA-256 '+hashlib.sha256(source.encode()).hexdigest()[:12],styles['h2']))
    nodes=sorted((n for n in ast.walk(ast.parse(source)) if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef))),key=lambda n:n.lineno)
    for n in nodes:story.append(Paragraph(escape(f'{n.name} - dòng {n.lineno}-{n.end_lineno}'),styles['body']))
story.append(Paragraph('static/shop.js: điểm vào từ giao diện',styles['h2']))
for i,line in enumerate((ROOT/'static/shop.js').read_text(encoding='utf8').splitlines(),1):
    match=re.match(r'(?:async )?function (\w+)',line)
    if match:story.append(Paragraph(f'{match[1]} - dòng {i}',styles['body']))
    elif '.onsubmit =' in line:story.append(Paragraph(f'checkout-form.onsubmit - dòng {i}',styles['body']))

story += [PageBreak(),Paragraph('Phụ lục: trích đoạn mã thật có số dòng',styles['h1']),
          Paragraph('Các dòng dài được ngắt hiển thị để vừa A4; số bên trái là dòng gốc. Trích đoạn là phần để đối chiếu, không phải file Python độc lập để copy chạy.',styles['body'])]
for relative,start,end,label in [
    ('shop/api.py',119,128,'HTTP 202 và lưu đơn'),
    ('shop/store.py',135,150,'Idempotency và transaction order/outbox'),
    ('shop/store.py',204,211,'Invoice và outbox email cùng transaction'),
    ('shop/broker.py',34,50,'Topology, DLX và persistent publish'),
    ('shop/worker.py',20,43,'Nhận, xử lý, NACK hoặc ACK'),
    ('shop/fulfillment.py',64,81,'Dedup invoice và crash sau commit'),
    ('shop/fulfillment.py',98,109,'Attachment, SMTP và ghi sổ gửi'),
    ('shop/worker.py',78,84,'Kafka xử lý rồi commit'),
    ('shop/worker.py',108,122,'Kafka pause partition sau lỗi')]:
    lines=(ROOT/relative).read_text(encoding='utf8').splitlines()
    assert 1<=start<=end<=len(lines),(relative,start,end)
    story.append(Paragraph(label+' | '+relative,styles['h2']))
    story.append(code_block([f'{i:3} | {lines[i-1]}' for i in range(start,end+1)]))

def footer(canvas,doc):
    canvas.setFont('Guide',8);canvas.setFillColor(colors.HexColor('#727970'))
    canvas.drawString(46,27,'Mộc Studio | RabbitMQ seminar | 31/10/2026')
    canvas.drawRightString(A4[0]-46,27,str(doc.page))

output=ROOT/'output/pdf/Huong_Dan_Demo_Cua_Hang_RabbitMQ.pdf'
output.parent.mkdir(parents=True,exist_ok=True)
GuideDocument(str(output),pagesize=A4,leftMargin=46,rightMargin=46,topMargin=45,bottomMargin=48,
              title='Mộc Studio - Sổ tay đọc code và trả lời GV',author='RabbitMQ Seminar').multiBuild(story,onFirstPage=footer,onLaterPages=footer)
(ROOT/'docs/Huong_Dan_Demo_Cua_Hang_RabbitMQ.pdf').write_bytes(output.read_bytes())
print(output)
