"""Business proof after shop_integration.py: no delay, real attachment bytes."""
import hashlib
import json
import uuid
from email import policy
from email.parser import BytesParser
from urllib.error import HTTPError
from urllib.request import urlopen
import shop_integration as demo


def main():
    demo.call('/settings',{'seconds':0})
    body=dict(customer=dict(name='Khách <Mộc> & An',email='thucte@example.test',address='12 Đường Mộc, TP. Hồ Chí Minh'),
              items=[dict(sku='tee',size='L',quantity=2,unit_price=1),dict(sku='hoodie',size='M',quantity=1)],
              idempotency_key=uuid.uuid4().hex)
    response=demo.call('/checkout',body,status=202);oid=response['order_id']
    assert response['order']['total']==1047000 and response['order']['shipping']==0
    assert demo.call('/checkout',body,status=202)['order_id']==oid
    state=demo.wait(lambda s:demo.order(s,oid)['status']=='completed')
    invoice=demo.order(state,oid)['invoice']
    with urlopen(demo.BASE+'/api/shop/orders/'+oid+'/invoice.pdf') as r:pdf=r.read()
    with urlopen(demo.MAIL+'/api/v1/messages') as r:messages=json.load(r)['messages']
    target=next(m for m in messages if invoice['number'] in m['Subject'])
    with urlopen(demo.MAIL+'/api/v1/message/'+target['ID']+'/raw') as r:email=BytesParser(policy=policy.default).parsebytes(r.read())
    attachments=list(email.iter_attachments())
    assert len(attachments)==1 and attachments[0].get_content_type()=='application/pdf'
    assert hashlib.sha256(attachments[0].get_payload(decode=True)).digest()==hashlib.sha256(pdf).digest()
    print('PASS practical: 0s delay, 2 product lines, free shipping, server total 1,047,000 VND, SMTP attachment equals downloaded PDF',flush=True)
    for label,patch in [('unknown size',{'items':[dict(sku='tee',size='BAD',quantity=1)]}),
                        ('invalid email',{'customer':dict(body['customer'],email='invalid')}),
                        ('changed content for existing key',{'items':[dict(sku='tee',size='L',quantity=1)]})]:
        try:demo.call('/checkout',dict(body,**patch))
        except HTTPError as exc:assert exc.code==422,(label,exc.code)
        else:raise AssertionError('Accepted '+label)
    print('PASS validation: invalid product size/email and conflicting idempotency key rejected',flush=True)
    # Extra non-business scripts/screens from the removed lab must not remain exposed.
    for path in ['/lab','/api/state','/static/legacy.html','/static/lab.js']:
        try:urlopen(demo.BASE+path)
        except HTTPError as exc:assert exc.code==404,(path,exc.code)
        else:raise AssertionError('Legacy route still reachable: '+path)
    print('PASS cleanup: old lab endpoints/assets removed; current business routes work',flush=True)


if __name__=='__main__':main()
