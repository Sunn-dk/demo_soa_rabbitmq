// Requires both brokers running; can follow shop_integration.py / shop_ui.js.
async page => {
  await page.setViewportSize({width:1440,height:1000});
  await page.goto(new URL(page.url()).origin+'/#compare');
  await page.getByRole('button',{name:'Chuẩn bị 4 worker hóa đơn mỗi bên',exact:true}).click();
  await page.waitForFunction(()=>!document.querySelector('#prepare-comparison').disabled,null,{timeout:60000});
  await page.waitForFunction(()=>document.querySelector('#comparison-live').textContent.includes('2 consumer có partition'));
  const seed=page.waitForResponse(r=>r.url().endsWith('/api/shop/seed')&&r.request().method()==='POST');
  await page.getByRole('button',{name:'Nạp 6 đơn thật mỗi bên',exact:true}).click();
  const batch=await (await seed).json();
  if(batch.order_ids.length!==12)throw Error('Must create 6 orders for each broker');
  await page.waitForFunction(()=>[...document.querySelectorAll('#comparison-live .compare-broker')].every(e=>e.textContent.includes('6 / 6 hóa đơn PDF')),null,{timeout:60000});
  await page.screenshot({path:'test-results/shop-comparison-running.png'});
  const faults=page.waitForResponse(r=>r.url().endsWith('/api/shop/seed')&&r.request().method()==='POST');
  await page.getByRole('button',{name:'Chạy tình huống lỗi trên cả hai broker',exact:true}).click();
  const fault=await (await faults).json();
  await page.waitForFunction(()=>{
    const text=document.querySelector('#error-comparison-result').textContent;
    return text.includes('2 / 3 email đã gửi')&&text.includes('0 / 3 email đã gửi')&&text.includes('Partition lỗi đang chờ');
  },null,{timeout:60000});
  await page.locator('#error-comparison-result').scrollIntoViewIfNeeded();
  await page.screenshot({path:'test-results/shop-comparison-error.png'});
  await page.getByRole('button',{name:'Phòng vận hành',exact:true}).click();
  await page.locator('#ops-transport [data-transport=kafka]').click();
  await page.getByRole('button',{name:'Chuyển sang topic lỗi',exact:true}).click();
  await page.getByRole('button',{name:'RabbitMQ & Kafka',exact:true}).click();
  await page.waitForFunction(()=>{
    const text=document.querySelector('#error-comparison-result').textContent;
    return (text.match(/2 \/ 3 email đã gửi/g)||[]).length===2&&!text.includes('Partition lỗi đang chờ');
  },null,{timeout:60000});
  await page.getByRole('button',{name:'Đọc lại bằng group analytics mới',exact:true}).click();
  await page.waitForFunction(()=>document.querySelector('#shop-replay-result').textContent.includes('Group mới:'),null,{timeout:60000});
  const result=await page.locator('#shop-replay-result').innerText();
  if(!result.includes('partition=')||!result.includes('offset='))throw Error('Missing actual replay positions');
  return {batch:batch.experiment,fault:fault.experiment,workers:'4 Rabbit, 2 assigned Kafka',errorRecovery:'Kafka custom DLQ resumed followers',replay:'actual records shown'};
}
