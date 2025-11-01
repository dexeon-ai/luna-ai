(function () {
  const SYMBOL = window.__SYMBOL__ || 'ETH';
  const TF     = window.__TF__ || '4h';

  // ========== Expand modal ==========
  const modal  = document.getElementById('modal');
  const mClose = document.getElementById('m-close');
  const mTF    = document.getElementById('m-tf');
  const mChart = document.getElementById('m-chart');
  const mTalk  = document.getElementById('m-talk');
  const mTitle = document.getElementById('m-title');

  function openModal(key, tf) {
    mTitle.textContent = key;
    modal.classList.remove('hidden');
    mTF.value = tf || TF;
    loadExpand(key, mTF.value);
  }
  function closeModal() {
    modal.classList.add('hidden');
    mChart.innerHTML = '';
    mTalk.textContent = '';
  }
  mClose.addEventListener('click', closeModal);
  mTF.addEventListener('change', () => loadExpand(mTitle.textContent, mTF.value));

  function loadExpand(key, tf) {
    const url = `/expand_json?symbol=${encodeURIComponent(SYMBOL)}&key=${encodeURIComponent(key)}&tf=${encodeURIComponent(tf)}`;
    fetch(url).then(r => r.json()).then(data => {
      mChart.innerHTML = '';
      Plotly.newPlot(mChart, data.fig.data, data.fig.layout, {responsive:true, displaylogo:false});
      mTalk.textContent = data.talk || '';
    }).catch(console.error);
  }

  document.querySelectorAll('.expand').forEach(btn => {
    btn.addEventListener('click', () => openModal(btn.dataset.key, TF));
  });

  // ========== Refresh ==========
  const refreshBtn = document.getElementById('refreshBtn');
  if (refreshBtn) {
    refreshBtn.addEventListener('click', () => {
      const sym = document.getElementById('symbolSelect').value || SYMBOL;
      fetch(`/api/refresh/${encodeURIComponent(sym)}`)
        .then(() => window.location.reload());
    });
  }

  // ========== Ask Luna ==========
  const askBox  = document.getElementById('askBox');
  const askSend = document.getElementById('askSend');
  const qaQ     = document.getElementById('qaQ');
  const qaA     = document.getElementById('qaA');

  function appendQA(q, a) {
    const li = document.createElement('li');
    li.textContent = q;
    qaQ.appendChild(li);

    const p = document.createElement('p');
    p.textContent = a;
    qaA.appendChild(p);
    qaA.scrollTop = qaA.scrollHeight;
  }

  if (askSend && askBox) {
    askSend.addEventListener('click', () => {
      const text = (askBox.value || '').trim();
      if (!text) return;
      fetch('/api/luna', {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ symbol: SYMBOL, tf: TF, text })
      })
      .then(r => r.json())
      .then(j => { appendQA(text, j.reply); askBox.value=''; })
      .catch(console.error);
    });

    askBox.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') askSend.click();
    });
  }

  // ========== Coin search (suggest + resolve) ==========
  const searchInput = document.getElementById('coinSearch');
  const searchGo    = document.getElementById('coinGo');
  const coinsList   = document.getElementById('coinsList');
  const tfSelect    = document.getElementById('tfSelect');

  function setDatalist(items){
    coinsList.innerHTML = '';
    (items || []).forEach(s => {
      const o = document.createElement('option');
      o.value = s; coinsList.appendChild(o);
    });
  }

  // initial suggestions
  fetch('/api/suggest').then(r => r.json()).then(j => setDatalist(j.symbols || []));

  // live suggest (debounced)
  let tmr = null;
  if (searchInput) {
    searchInput.addEventListener('input', () => {
      const q = (searchInput.value || '').trim();
      clearTimeout(tmr);
      tmr = setTimeout(() => {
        fetch(`/api/suggest?q=${encodeURIComponent(q)}`)
          .then(r => r.json())
          .then(j => setDatalist(j.symbols || []))
          .catch(()=>{});
      }, 120);
    });
  }

  function goSearch(){
    const q = (searchInput.value || '').trim();
    if (!q) return;
    fetch(`/api/resolve?query=${encodeURIComponent(q)}`)
      .then(r => r.json())
      .then(j => {
        const sym = j.symbol || q.toUpperCase();
        const tf  = (tfSelect && tfSelect.value) ? tfSelect.value : TF;
        window.location.href = `/analyze?symbol=${encodeURIComponent(sym)}&tf=${encodeURIComponent(tf)}`;
      })
      .catch(()=>{ /* last resort: try as-is */ 
        const tf  = (tfSelect && tfSelect.value) ? tfSelect.value : TF;
        window.location.href = `/analyze?symbol=${encodeURIComponent(q)}&tf=${encodeURIComponent(tf)}`;
      });
  }
  if (searchGo) searchGo.addEventListener('click', goSearch);
  if (searchInput) searchInput.addEventListener('keydown', (e)=>{ if(e.key==='Enter') goSearch(); });

})();
