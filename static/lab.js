'use strict';
const $ = id => document.getElementById(id);
const steps = [
 ['Bất đồng bộ: worker đang tắt', 'API nhận tác vụ tạo hóa đơn, còn consumer xử lý sau. Bất đồng bộ tách thời điểm nhận việc và hoàn tất.', ['Giữ 0 worker. Bấm gửi 1 job qua RabbitMQ.', 'Quan sát HTTP 202 và Ready tăng, hóa đơn vẫn bằng 0.', 'Thêm worker. Ready giảm, Unacked tăng rồi hóa đơn xuất hiện.']],
 ['Một queue, nhiều consumer chia việc', 'prefetch=1 giới hạn mỗi worker giữ một tin chưa ACK. Thứ tự nhận tin không tự quyết định thứ tự hoàn tất nghiệp vụ.', ['Gửi 6 job. Cho 1 worker xử lý, sau đó thêm worker thứ 2.', 'Khi queue trống và 2 worker rảnh, bấm Thứ tự: gửi 6s rồi 1s.', 'Job gửi sau hoàn tất trước. Các bước phụ thuộc nhau cần workflow hoặc chỉ gửi bước sau khi bước trước thành công.']],
 ['Giao lại tin và hiệu ứng nghiệp vụ trùng', 'ACK xác nhận một lượt giao. Sập trước ACK có thể khiến cùng job được xử lý lại.', ['Giữ 1 worker, gửi job 6 giây, bấm Dừng / crash khi đang processing.', 'Thêm worker mới. Tìm REDELIVER và số lượt giao tăng.', 'Bấm Sập sau COMMIT, trước ACK. Thêm worker nếu cần: DEDUP, vẫn chỉ 1 hóa đơn cho job đó.']],
 ['Tin lỗi chuyển qua Dead Letter Exchange', 'Tin không xử lý được cần có nơi kiểm tra. DLQ nhận tin nhờ cấu hình DLX và NACK với requeue=False.', ['Bật worker, bấm Gửi job lỗi rồi gửi 1 job thường.', 'Tìm NACK trong log. DLQ tăng, job thường vẫn hoàn tất.', 'Bấm Sửa & gửi lại DLQ. Job giữ cùng ID, dữ liệu lỗi được sửa.']],
 ['Kafka giữ log sau khi consumer commit', 'So sánh khác biệt về vòng đời dữ liệu thay vì kết luận công cụ nào luôn nhanh hơn.', ['Bật Kafka bằng lệnh hiển thị ở cuối trang.', 'Bấm Chạy thí nghiệm Kafka replay. Đợi kết quả từ broker thật.', 'So sánh event_id, partition, offset của hai group. Group B vẫn đọc đủ 6 sự kiện.']]
];
let current = null, refreshing = false;
const labels = {publishing:'Đang publish',ready:'Chờ nhận',processing:'Đang xử lý',done:'Đã gửi ACK',failed:'Lỗi / DLQ',publish_unknown:'Chưa rõ publish'};
function escapeHtml(value) { return String(value ?? '').replace(/[&<>"']/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function selectStep(i) {
  $('step-label').textContent = `THÍ NGHIỆM 0${i+1}`;
  $('step-title').textContent = steps[i][0]; $('step-description').textContent = steps[i][1];
  $('instructions').innerHTML = steps[i][2].map(s=>`<li>${escapeHtml(s)}</li>`).join('');
  document.querySelectorAll('[data-step]').forEach(b=>b.classList.toggle('selected',Number(b.dataset.step)===i));
}
document.querySelectorAll('[data-step]').forEach(b=>b.onclick=()=>selectStep(Number(b.dataset.step)));
async function post(url, body={}) {
  const res = await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  const data = await res.json();
  if(!res.ok) throw Error(typeof data.detail==='string'?data.detail:JSON.stringify(data.detail));
  return data;
}
async function action(button, url, body={}) {
  button.disabled = true; $('response').classList.remove('error'); $('response').textContent='Đang thực hiện…';
  const start = performance.now();
  try {
    const result=await post(url,body);
    const http = (performance.now()-start).toFixed(0);
    $('response').textContent = result.api_ms!==undefined ? `${result.status==='accepted'?'HTTP 202 · Đã nhận việc':'HTTP 200 · Hoàn tất đối chứng'} · ${result.api_ms} ms ở server · ${http} ms HTTP từ trình duyệt. ${result.meaning}` : JSON.stringify(result);
    await refresh(); return result;
  } catch(error) { $('response').textContent=error.message; $('response').classList.add('error'); }
  finally {button.disabled=false;}
}
function payload(extra={}) { return {seconds:Number($('seconds').value),...extra}; }
$('send').onclick=()=>action($('send'),'/api/jobs',payload());
$('burst').onclick=()=>action($('burst'),'/api/jobs',payload({count:6}));
$('ordering').onclick=()=>action($('ordering'),'/api/jobs',payload({durations:[6,1]}));
$('sync').onclick=()=>action($('sync'),'/api/sync',payload());
$('add').onclick=()=>action($('add'),'/api/workers');
$('poison').onclick=()=>action($('poison'),'/api/jobs',payload({poison:true}));
$('commit-crash').onclick=()=>action($('commit-crash'),'/api/jobs',payload({crash_after_commit:true}));
$('retry').onclick=()=>action($('retry'),'/api/dlq/retry');
$('reset').onclick=()=>action($('reset'),'/api/reset');
$('workers').onclick=e=>{const b=e.target.closest('[data-crash]');if(b)action(b,`/api/workers/${encodeURIComponent(b.dataset.crash)}/crash`);};
$('jobs').onclick=async e=>{const b=e.target.closest('[data-duplicate]');if(b)action(b,`/api/jobs/${encodeURIComponent(b.dataset.duplicate)}/duplicate`);const j=e.target.closest('[data-inspect]');if(j){const r=await fetch(`/api/jobs/${encodeURIComponent(j.dataset.inspect)}`);if(r.ok)$('message-body').textContent=JSON.stringify(await r.json(),null,2);}};
$('kafka-run').onclick=async()=>{
  $('kafka-result').textContent='Đang tạo topic, publish và đọc bằng hai consumer group thật…';
  const result=await action($('kafka-run'),'/api/kafka/replay');
  if(result) $('kafka-result').textContent=`Topic: ${result.topic}\n${result.partitions} partitions · ${result.published} sự kiện\n\n`+[result.first,result.replay].map(g=>`${g.group}\n`+g.records.map(r=>`  ${r.event_id}   partition=${r.partition}   offset=${r.offset}`).join('\n')).join('\n\n')+`\n\nCùng 6 event_id: ${result.same_events?'ĐÚNG':'KHÔNG'}\n${result.meaning}`;
  else $('kafka-result').textContent=$('response').textContent;
};
async function refresh(){
  if(refreshing)return; refreshing=true;
  try{
    const response=await fetch('/api/state'); if(!response.ok)throw Error('API chưa sẵn sàng');
    current=await response.json();const b=current.broker;
    $('connection').textContent=b.ok?'● Broker thật đang kết nối':'● Chưa kết nối broker';
    for(const key of ['ready','unacked','dlq'])$(key).textContent=b[key]??'—';
    $('consumer-count').textContent=b.consumers??'—';$('effects').textContent=current.effects;
    $('counts-source').textContent=b.ok?b.source:'Không lấy được số đếm broker. Không suy ra Ready/Unacked từ nhật ký ứng dụng.';
    $('workers').innerHTML=current.workers.length?current.workers.map(w=>{
      const job=current.jobs.find(j=>j.status==='processing'&&j.worker===w.name);
      const age=job?Math.max(0,(Date.now()/1000-job.started)):0;
      return `<div class="worker ${w.alive?'':'dead'}"><div class="worker-head"><strong>${escapeHtml(w.name)}</strong>${w.alive?`<button data-crash="${escapeHtml(w.name)}">Dừng / crash</button>`:'Đã dừng'}</div><small>PID ${w.pid} · ${w.alive?'Đang chạy':'Mất kết nối'}</small><p>${job?`${escapeHtml(job.id)} · ${age.toFixed(1)}s từ lúc nhận`:w.alive?'Chờ nhận việc':'Tin chưa ACK có thể được giao lại'}</p><div class="progress"><i style="width:${job&&w.alive?Math.min(100,age/(job.seconds||1)*100):0}%"></i></div></div>`;
    }).join(''):'<p>Chưa có worker. API vẫn có thể gửi job vào queue.</p>';
    $('jobs').innerHTML=current.jobs.map(j=>`<tr><td><button data-inspect="${escapeHtml(j.id)}">${escapeHtml(j.id)}</button></td><td class="status ${escapeHtml(j.status)}">${escapeHtml(labels[j.status]||j.status)}</td><td>${escapeHtml(j.worker||'—')}</td><td>${j.deliveries}</td><td>${j.redelivered}</td><td>${j.status==='done'?`<button data-duplicate="${escapeHtml(j.id)}">Gửi trùng ID</button>`:''}</td></tr>`).join('')||'<tr><td colspan="6">Chưa có job. Bấm gửi để bắt đầu.</td></tr>';
    $('events').innerHTML=current.events.map(e=>`<div class="event"><span class="time">${new Date(e.ts*1000).toLocaleTimeString('vi-VN')}</span> <b>${escapeHtml(e.kind)}</b> ${escapeHtml(e.job_id||'')} ${escapeHtml(e.worker||'')}<small>${escapeHtml(e.detail)}</small></div>`).join('')||'Chưa có sự kiện.';
  }catch(error){$('connection').textContent='● API chưa kết nối';}
  finally{refreshing=false;}
}
selectStep(0);refresh();setInterval(refresh,1000);
