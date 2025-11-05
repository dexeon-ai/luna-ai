(function(){
  // --- Height slider ⇄ CSS var
  const hs = document.getElementById('hslider');
  if (hs){
    const apply = v => document.documentElement.style.setProperty('--tile-big-h', `${v}px`);
    apply(hs.value);
    hs.addEventListener('input', e => apply(e.target.value));
  }

  // --- Expand modal
  const modal      = document.getElementById('modal');
  const modalClose = document.getElementById('modalClose');
  const modalPlot  = document.getElementById('modalPlot');

  function openModal(){ modal.classList.remove('hidden'); }
  function closeModal(){ modal.classList.add('hidden'); try{ Plotly.purge(modalPlot); }catch(e){} }
  if (modalClose) modalClose.addEventListener('click', closeModal);
  if (modal) modal.addEventListener('click', (e)=>{ if(e.target===modal) closeModal(); });

  async function expandKey(key){
    const url = new URL(URL_BASE, window.location.origin);
    const params = new URLSearchParams(window.location.search);
    url.searchParams.set('symbol', SYMBOL_RAW);
    url.searchParams.set('tf', params.get('tf') || TF || '12h');
    url.searchParams.set('key', key);
    const view = params.get('view'); if (view) url.searchParams.set('view', view);
    const scale = params.get('scale'); if (scale) url.searchParams.set('scale', scale);

    const res = await fetch(url.toString());
    if (!res.ok){ alert('Expand failed.'); return; }
    const data = await res.json();
    openModal();
    Plotly.newPlot(modalPlot, data.fig.data, data.fig.layout || {}, {responsive:true});
  }

  document.querySelectorAll('button.expand').forEach(btn=>{
    btn.addEventListener('click', ()=> expandKey(btn.dataset.key));
  });

  // --- Cap/Price & Log/Lin toggles (URL param flips)
  function setUrlParam(name, value){
    const u = new URL(window.location.href);
    if (value) u.searchParams.set(name, value); else u.searchParams.delete(name);
    window.location.href = u.toString();
  }
  const tCap = document.getElementById('toggleCap');
  if (tCap){
    tCap.addEventListener('click', ()=>{
      const u = new URL(window.location.href);
      const cur = (u.searchParams.get('view') || '').toLowerCase();
      setUrlParam('view', cur==='cap' ? 'price' : 'cap');
    });
  }
  const tScale = document.getElementById('toggleScale');
  if (tScale){
    tScale.addEventListener('click', ()=>{
      const u = new URL(window.location.href);
      const cur = (u.searchParams.get('scale') || '').toLowerCase();
      setUrlParam('scale', cur==='log' ? 'lin' : 'log');
    });
  }

  // --- Ask Luna (uses /api/luna)
  const askInput = document.getElementById('askInput');
  const askSend  = document.getElementById('askSend');
  const askReset = document.getElementById('askReset');
  const answer   = document.getElementById('answer');

  async function ask(){
    const text = (askInput.value || '').trim();
    if (!text) return;
    answer.textContent = '…thinking…';
    const res = await fetch('/api/luna', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ symbol: SYMBOL_RAW, tf: TF, text })
    });
    const js = await res.json();
    answer.textContent = js.reply || '(no answer)';
  }
  if (askSend)  askSend.addEventListener('click', ask);
  if (askReset) askReset.addEventListener('click', ()=>{ askInput.value=''; answer.textContent=''; });

})();
