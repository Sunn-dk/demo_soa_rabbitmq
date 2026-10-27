"""Build the print guide from the same Markdown documentation shipped in Git."""
from html import escape
from pathlib import Path
import re
from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Preformatted

ROOT=Path(__file__).resolve().parents[1]
pdfmetrics.registerFont(TTFont('Guide',str(ROOT/'static/fonts/DejaVuSans.ttf')))
pdfmetrics.registerFontFamily('Guide',normal='Guide',bold='Guide',italic='Guide',boldItalic='Guide')
styles={
    'body':ParagraphStyle('body',fontName='Guide',fontSize=9.5,leading=15,spaceAfter=8),
    'h1':ParagraphStyle('h1',fontName='Guide',fontSize=21,leading=28,textColor=colors.HexColor('#25543e'),spaceAfter=19),
    'h2':ParagraphStyle('h2',fontName='Guide',fontSize=13,leading=19,textColor=colors.HexColor('#25543e'),spaceBefore=15,spaceAfter=8,keepWithNext=True),
    'code':ParagraphStyle('code',fontName='Guide',fontSize=7.2,leading=11,backColor=colors.HexColor('#f1f3ed'),borderPadding=8,spaceAfter=12),
}

def inline(text):
    text=escape(text)
    text=re.sub(r'\[([^]]+)\]\(([^)]+)\)',lambda m:'<link href="'+m[2]+'" color="#25543e">'+m[1]+'</link>',text)
    text=re.sub(r'\*\*(.+?)\*\*',r'<b>\1</b>',text)
    return text.replace('`','')

story=[Paragraph('MỘC STUDIO / SEMINAR RABBITMQ',styles['h1']),
       Paragraph('Hướng dẫn chạy, lời thuyết trình và giải thích code',styles['h2']),
       Paragraph('Bản cửa hàng ngày 28/10/2026. Dùng cùng mã nguồn tại github.com/Sunn-dk/demo_soa_rabbitmq. Tên hàm là mốc tra cứu chính; số dòng có thể đổi ở phiên bản sau.',styles['body']),
       Paragraph('Chạy nhanh',styles['h2']),
       Preformatted('git clone https://github.com/Sunn-dk/demo_soa_rabbitmq.git\ncd demo_soa_rabbitmq\ndocker compose --profile kafka up -d --build --wait --wait-timeout 240',styles['code']),
       Paragraph('Cài Git và Docker Desktop, mở Docker trước. Web: http://localhost:8000. Hộp thư Mailpit: http://localhost:8025. RabbitMQ Management: http://localhost:15672 (guest / guest). Không cần cài Python/Node khi chạy Docker.',styles['body']),
       Paragraph('Bắt đầu với 0 worker. Đặt đơn, thêm consumer tạo hóa đơn, sau đó thêm consumer gửi email. Hai bước tách biệt để quan sát bất đồng bộ. Mailpit nhận SMTP và file PDF thật trong máy; không gửi thư ra Internet.',styles['body']),
       Paragraph('Các phần trong bản in',styles['h2']),
       Paragraph('1. Kịch bản trình bày 15-20 phút và thao tác cụ thể.<br/>2. Giải thích code theo luồng HTTP, outbox, PDF, email, ACK và phục hồi.<br/>3. So sánh Kafka, giới hạn và nguồn chính thức.',styles['body'])]

for name in ['DEMO_CUA_HANG.md','GIAI_THICH_CUA_HANG.md','SO_SANH_KAFKA.md']:
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
                story.append(Preformatted('\n'.join(code),styles['code']));code=[]
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

def footer(canvas,doc):
    canvas.setFont('Guide',8);canvas.setFillColor(colors.HexColor('#727970'))
    canvas.drawString(46,27,'Mộc Studio | RabbitMQ seminar | 28/10/2026')
    canvas.drawRightString(A4[0]-46,27,str(doc.page))

output=ROOT/'output/pdf/Huong_Dan_Demo_Cua_Hang_RabbitMQ.pdf'
output.parent.mkdir(parents=True,exist_ok=True)
SimpleDocTemplate(str(output),pagesize=A4,leftMargin=46,rightMargin=46,topMargin=45,bottomMargin=48,
                  title='Mộc Studio - Hướng dẫn và giải thích code RabbitMQ',author='RabbitMQ Seminar').build(story,onFirstPage=footer,onLaterPages=footer)
(ROOT/'docs/Huong_Dan_Demo_Cua_Hang_RabbitMQ.pdf').write_bytes(output.read_bytes())
print(output)
