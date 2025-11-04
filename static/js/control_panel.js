(function () {
  function qs(sel){return document.querySelector(sel)}
  function qsa(sel){return Array.from(document.querySelectorAll(sel))}

  // Expand handlers
  qsa('.btn-expand').forEach(btn=>{
    btn.addEventListener('click', async ()=>{
      const key = btn.dataset.key || 'PRICE';
      const params = new URLSearchParams({symbol: SYMBOL, tf: TF, key});
      const r = await fetch(`/expand_json?${params.toString()}`);
      const js = await r.json();
      const el = qs('#modalChart');
      el.innerHTML = '';
      Plotly.newPlot(el, js.fig.data, js.fig.layout, {responsive:true});
      qs('#modalTalk').textContent = js.talk;
      qs('#modal').classList.remove('hidden');
    });
  });
  qs('#modalClose').addEventListener('click', ()=> qs('#modal').classList.add('hidden'));
  qs('#modal').addEventListener('click', (e)=>{ if(e.target.id==='modal') qs('#modal').classList.add('hidden'); });

  // Ask Luna
  qs('#askBtn').addEventListener('click', async ()=>{
    const text = (qs('#askBox').value||'').trim();
    const r = await fetch('/expand_json?'+new URLSearchParams({symbol:SYMBOL, tf:TF, key:'PRICE'}).toString());
    const js = await r.json();
    // quick reuse of the same answer engine with question
    const res = await fetch('/api/luna', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({symbol: SYMBOL, tf: TF, text})
    });
    const ans = await res.json();
    qs('#answerText').textContent = ans.reply || js.talk || '—';
  });
})();
