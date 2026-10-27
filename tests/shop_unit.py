"""Domain invariants; no broker required. Uses the project's reportlab dependency."""
import concurrent.futures
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import shop_store as store
from shop_fulfillment import render_invoice


class ShopDomain(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='moc-shop-')
        self.addCleanup(self.temp.cleanup)
        self.old_db,self.old_files=store.DB,store.FILES
        store.DB=Path(self.temp.name)/'shop.sqlite3';store.FILES=Path(self.temp.name)/'invoices'
        self.addCleanup(setattr,store,'DB',self.old_db);self.addCleanup(setattr,store,'FILES',self.old_files)
        store.init()
        self.customer=dict(name='Nguyễn Minh Anh',email='anh@example.test',address='12 Đường Mộc, TP. Hồ Chí Minh')
        self.items=[dict(sku='tee',size='M',quantity=2,unit_price=1)]

    def create(self,key='test-request-000001',items=None):
        return store.create_order(self.customer,items or self.items,key)

    def test_server_recalculates_price_and_shipping(self):
        quote=store.quote(self.items)
        self.assertEqual(quote['subtotal'],498000)
        self.assertEqual(quote['total'],523000)
        self.assertEqual(store.quote([dict(sku='tee',size='M',quantity=5)])['shipping'],0)
        for bad in [dict(sku='unknown',size='M',quantity=1),dict(sku='tee',size='bogus',quantity=1),dict(sku='tee',size='M',quantity=True)]:
            with self.assertRaises(ValueError):store.quote([bad])

    def test_concurrent_checkout_is_idempotent(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            results=list(executor.map(lambda _:self.create(),range(4)))
        self.assertEqual(len({oid for oid,_ in results}),1)
        self.assertEqual(sum(created for _,created in results),1)
        with self.assertRaises(ValueError):self.create(items=[dict(sku='tee',size='M',quantity=1)])
        self.assertEqual(store.snapshot()['totals']['orders'],1)
        self.assertEqual(len(store.pending_outbox()),1)

    def test_invoice_and_email_outbox_share_transaction(self):
        oid,_=self.create()
        with patch.object(store,'enqueue',side_effect=RuntimeError('forced enqueue failure')):
            with self.assertRaises(RuntimeError):store.commit_invoice(oid,'MS-1','one.pdf',store.order(oid),'unit')
        self.assertIsNone(store.invoice(oid))
        self.assertTrue(store.commit_invoice(oid,'MS-1','one.pdf',store.order(oid),'unit'))
        self.assertFalse(store.commit_invoice(oid,'MS-1','one.pdf',store.order(oid),'unit'))
        self.assertEqual(len([r for r in store.pending_outbox() if r['role']=='email']),1)

    def test_real_pdf_contains_order_lines_and_totals(self):
        oid,_=self.create()
        name=render_invoice(store.order(oid),'MS-UNIT')
        pdf=(store.FILES/name).read_bytes()
        self.assertTrue(pdf.startswith(b'%PDF'))
        from pypdf import PdfReader
        text=''.join(page.extract_text() for page in PdfReader(store.FILES/name).pages)
        for expected in ['Nguyễn Minh Anh','Áo thun Everyday','523.000','MS-UNIT']:
            self.assertIn(expected,text)

    def test_broker_backlog_cannot_starve_other_dispatcher(self):
        for i in range(4):
            store.create_order(self.customer,self.items,'kafka-backlog-'+str(i),'kafka')
        oid,_=self.create()
        pending=store.pending_outbox(limit=1,transport='rabbitmq')
        self.assertEqual(len(pending),1)
        self.assertEqual(pending[0]['task_id'],oid+':invoice')

    def test_work_state_preserves_actual_kafka_assignment(self):
        store.update_worker('K-TEST','kafka','invoice','idle',[1])
        store.update_worker('K-TEST','kafka','invoice','rendering',order_id='unit-order')
        worker=store.snapshot()['workers'][0]
        self.assertEqual(worker['assignment'],[1])
        self.assertEqual(worker['state'],'rendering')
        store.update_worker('K-TEST','kafka','invoice','rebalancing',[])
        self.assertEqual(store.snapshot()['workers'][0]['assignment'],[])


if __name__=='__main__':unittest.main(verbosity=2)
