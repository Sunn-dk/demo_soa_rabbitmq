// Run with: playwright-cli -s=shop run-code --filename=tests/shop_ui.js
async page => {
  const errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  await page.setViewportSize({width:1440,height:1000});
  await page.goto(new URL(page.url()).origin);
  await page.getByRole('button',{name:'Thêm Áo thun Everyday vào giỏ',exact:true}).click();
  await page.getByRole('textbox',{name:'Họ và tên',exact:true}).fill('Khách kiểm tra giao diện');
  await page.getByRole('textbox',{name:'Email nhận hóa đơn',exact:true}).fill('giaodien@example.test');
  await page.getByRole('textbox',{name:'Địa chỉ giao hàng',exact:true}).fill('12 Đường Mộc, TP. Hồ Chí Minh');
  const response=page.waitForResponse(r=>r.url().endsWith('/api/shop/checkout') && r.request().method()==='POST');
  await page.getByRole('button',{name:'Đặt hàng & nhận hóa đơn →',exact:true}).click();
  const checkout=await response;
  if(checkout.status()!==202)throw Error('Checkout must return HTTP 202');
  const data=await checkout.json();
  if(data.order.total!==274000)throw Error('Unexpected server total');
  await page.locator('#order-detail [data-invoice]').waitFor({timeout:45000});
  await page.locator('#order-detail [data-invoice]').click();
  await page.locator('#invoice-dialog[open]').waitFor();
  const receipt=await page.locator('#invoice-preview').innerText();
  if(!receipt.includes('Khách kiểm tra giao diện') || !receipt.includes('274.000'))throw Error('Receipt missing details');
  await page.screenshot({path:'test-results/shop-receipt.png'});
  await page.getByRole('button',{name:'Đóng hóa đơn',exact:true}).click();
  await page.getByRole('button',{name:'Phòng vận hành',exact:true}).click();
  await page.locator('#pipeline .flow-node').first().waitFor();
  await page.screenshot({path:'test-results/shop-operations.png'});
  await page.getByRole('button',{name:'RabbitMQ & Kafka',exact:true}).click();
  await page.locator('#comparison-live .compare-broker').first().waitFor();
  await page.screenshot({path:'test-results/shop-comparison.png'});
  for(const tab of ['Cửa hàng','Đơn hàng','Phòng vận hành','RabbitMQ & Kafka']){
    await page.setViewportSize({width:390,height:844});
    await page.locator('nav').getByRole('button',{name:new RegExp('^'+tab)}).click();
    const overflow=await page.evaluate(()=>document.documentElement.scrollWidth>window.innerWidth);
    if(overflow)throw Error('Horizontal page overflow on mobile: '+tab);
  }
  await page.getByRole('button',{name:'Cửa hàng',exact:true}).click();
  await page.screenshot({path:'test-results/shop-mobile.png'});
  await page.setViewportSize({width:1440,height:1000});
  await page.screenshot({path:'test-results/shop-store.png'});
  if(errors.length)throw Error(errors.join('\n'));
  return {http:checkout.status(),order:data.order_id,total:data.order.total,receipt:'verified',mobile:'4 tabs no overflow',pageErrors:errors};
}
