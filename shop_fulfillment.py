"""Real business work shared by RabbitMQ and Kafka: PDF invoice and local SMTP."""
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from email.utils import format_datetime
from html import escape
import os
from pathlib import Path
import smtplib
import uuid
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
import shop_store as store

LOCAL_TZ=timezone(timedelta(hours=7))


class BusinessError(Exception):
    pass


def money(n):
    return f'{n:,}'.replace(',','.')+' đ'


def render_invoice(order, number):
    font=Path(__file__).parent/'static/fonts/DejaVuSans.ttf'
    if 'Receipt' not in pdfmetrics.getRegisteredFontNames():
        pdfmetrics.registerFont(TTFont('Receipt',str(font)))
    filename=order['id']+'.pdf'
    final=store.FILES/filename
    temporary=store.FILES/(order['id']+'-'+uuid.uuid4().hex+'.tmp')
    normal=ParagraphStyle('receipt',fontName='Receipt',fontSize=10,leading=15,spaceAfter=7)
    title=ParagraphStyle('title',parent=normal,fontSize=24,leading=30,spaceAfter=12)
    small=ParagraphStyle('small',parent=normal,fontSize=8,leading=12)
    def p(text,style=normal):return Paragraph(escape(str(text)).replace('\n','<br/>'),style)
    customer=order['customer']
    content=[p('MỘC STUDIO',title),p('HÓA ĐƠN ĐƠN HÀNG'),
             p(f"Số: {number}\nMã đơn: {order['id']}\nNgày đặt: {datetime.fromtimestamp(order['created'],LOCAL_TZ).strftime('%d/%m/%Y %H:%M')} (UTC+7)"),
             Spacer(1,9),p(f"Khách hàng: {customer['name']}\nGiao đến: {customer['address']}\nThanh toán: COD - thanh toán khi nhận hàng"),Spacer(1,12)]
    rows=[[p(x,small) for x in ['Sản phẩm / kích cỡ','SL','Đơn giá','Thành tiền']]]
    for item in order['items']:
        rows.append([p(item['name']+'\n'+item['color']+' / '+item['size'],small),p(item['quantity'],small),p(money(item['unit_price']),small),p(money(item['line_total']),small)])
    table=Table(rows,colWidths=[235,35,100,115],repeatRows=1)
    table.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#e9eee9')),('VALIGN',(0,0),(-1,-1),'TOP'),
        ('LEFTPADDING',(0,0),(-1,-1),9),('RIGHTPADDING',(0,0),(-1,-1),9),('TOPPADDING',(0,0),(-1,-1),10),
        ('BOTTOMPADDING',(0,0),(-1,-1),10),('LINEBELOW',(0,0),(-1,-1),.4,colors.HexColor('#dbe1da'))]))
    content+=[table,Spacer(1,16),p('Tạm tính: '+money(order['subtotal'])),p('Phí giao hàng: '+money(order['shipping'])),
              p('TỔNG CỘNG: '+money(order['total'])),Spacer(1,20),p('Cảm ơn bạn đã mua sắm tại Mộc Studio!'),
              p('Chứng từ minh họa của seminar. Không xác nhận đã thu tiền và không phải hóa đơn điện tử thuế.',small)]
    SimpleDocTemplate(str(temporary),pagesize=A4,leftMargin=55,rightMargin=55,topMargin=50,bottomMargin=50,
                      title='Hóa đơn '+number,author='Mộc Studio Seminar').build(content)
    os.replace(temporary,final)
    return filename


def fulfill(payload, worker, transport, pause):
    oid=payload['order_id'];role=payload['role']
    order=store.order(oid)
    if not order:
        raise BusinessError('Không tìm thấy đơn hàng')
    if role=='invoice':
        prior=store.invoice(oid)
        if prior:
            if not (store.FILES/prior['filename']).is_file():
                render_invoice(order,prior['number'])
            store.log('DEDUP',oid,payload['task_id'],worker,transport,'Hóa đơn đã có; không tạo lại side effect hoặc tác vụ email')
            return
        store.update_worker(worker,transport,role,'rendering',order_id=oid)
        store.log('PDF_RENDER',oid,payload['task_id'],worker,transport,'Tính tiền theo snapshot giá server; tạo PDF chi tiết có font tiếng Việt')
        pause(store.settings()['observation_delay'])
        number='MS-'+datetime.fromtimestamp(order['created'],LOCAL_TZ).strftime('%Y%m%d')+'-'+oid[-8:].upper()
        filename=render_invoice(order,number)
        created=store.commit_invoice(oid,number,filename,order,worker)
        if created and order['crash_after_commit']:
            store.log('CRASH_BEFORE_ACK',oid,payload['task_id'],worker,transport,'Hóa đơn và outbox email đã commit; process thoát trước xác nhận broker')
            os._exit(42)
    elif role=='email':
        if store.sent(oid):
            store.log('DEDUP',oid,payload['task_id'],worker,transport,'Sổ gửi đã có; bỏ qua gửi email lặp trong lần nhận này')
            return
        recipient=order['customer']['email']
        if not store.email_valid(recipient):
            raise BusinessError('Địa chỉ email nhận hóa đơn không hợp lệ: '+recipient)
        invoice=store.invoice(oid)
        if not invoice or not (store.FILES/invoice['filename']).is_file():
            raise BusinessError('Chưa có file hóa đơn để đính kèm')
        store.update_worker(worker,transport,role,'sending',order_id=oid)
        pause(store.settings()['observation_delay'])
        message=EmailMessage()
        message['From']='Moc Studio <orders@moc-studio.test>'
        message['To']=recipient
        message['Subject']='Hóa đơn đơn hàng '+invoice['number']
        message['Date']=format_datetime(datetime.now(LOCAL_TZ))
        message_id=f'<{oid}@moc-studio.test>'
        message['Message-ID']=message_id
        message.set_content(f"Xin chào {order['customer']['name']},\n\nĐơn hàng của bạn đã được xác nhận.\nTổng cộng: {money(order['total'])}.\nHóa đơn chi tiết nằm trong PDF đính kèm.\nThanh toán COD khi nhận hàng.\n\nĐây là email được gửi tới Mailpit cục bộ của seminar.")
        message.add_attachment((store.FILES/invoice['filename']).read_bytes(),maintype='application',subtype='pdf',filename=invoice['number']+'.pdf')
        with smtplib.SMTP(os.getenv('SHOP_SMTP_HOST','127.0.0.1'),int(os.getenv('SHOP_SMTP_PORT','1025')),timeout=8) as smtp:
            refused=smtp.send_message(message)
            if refused:
                raise BusinessError('SMTP từ chối người nhận')
        # SMTP and SQLite are not one transaction. A crash here can resend email.
        store.record_sent(oid,recipient,message_id,worker,transport)
    else:
        raise BusinessError('Loại tác vụ không được hỗ trợ')
