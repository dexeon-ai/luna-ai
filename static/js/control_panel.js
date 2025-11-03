// static/js/control_panel.js
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
  if (mClose) mClose.addEventListener('click', closeModal);
  if (mTF) mTF.addEventListener('change', () => loadExpand(mTitle.textContent, mTF.value));

  function loadExpand(key, tf) {
    const url = `/expand_json?symbol=${encodeURIComponent(SYMBOL)}&key=${encodeURIComponent(key)}&tf=${encodeURIComponent(tf)}`;
    fetch(url).then(r => r.json()).then(data => {
      mChart.innerHTML = '';
      Plotly.newPlot(mChart, data.fig.data, data.fig.layout, {responsive:true, displaylogo:false});
      mTalk.textContent = data.talk || '';
    }).catch(err => {
      console.error(err);
      mTalk.textContent = 'Error loading chart.';
    });
  }

  document.querySelectorAll('.expand').forEach(btn => {
    btn.addEventListener('click', () => openModal(btn.dataset.key, TF));
  });

  // ========== Refresh ==========
  const refreshBtn = document.getElementById('refreshBtn');
  if (refreshBtn) {
    refreshBtn.addEventListener('click', () => {
      const sym = (document.getElementById('symbolSelect') && document.getElementById('symbolSelect').value) || SYMBOL;
      fetch(`/api/refresh/${encodeURIComponent(sym)}`).then(() => window.location.reload());
    });
  }

  // ========== Ask Luna ==========
  const askBox  = document.getElementById('askBox');
  const askSend = document.getElementById('askSend');
  const qaQ     = document.getElementById('qaQ');
  const qaA     = document.getElementById('qaA');

  function appendQA(q, a) {
    if (qaQ && q) {
      const li = document.createElement('li');
      li.textContent = q;
      qaQ.appendChild(li);
    }
    let pEl = null;
    if (qaA && (a !== undefined && a !== null)) {
      pEl = document.createElement('p');
      pEl.textContent = a;
      qaA.appendChild(pEl);
      qaA.scrollTop = qaA.scrollHeight;
    }
    return pEl;
  }

  // Append question + temp "analyzing…" and return the <p> we will update
  function pushQuestionAndSpinner(qText) {
    appendQA(qText, null);
    return appendQA(null, 'Luna is analyzing…');
  }

  let sending = false;
  function sendQuestion() {
    if (sending) return; // simple throttle
    const text = (askBox && askBox.value || '').trim();
    if (!text) return;
    sending = true;

    const bubble = pushQuestionAndSpinner(text);
    const payload = { symbol: SYMBOL, tf: TF, text };

    fetch('/api/luna', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    })
    .then(r => r.json())
    .then(j => {
      if (bubble) bubble.textContent = j.reply || 'No reply.';
      if (askBox) askBox.value = '';
    })
    .catch(err => {
      console.error(err);
      if (bubble) bubble.textContent = 'Error: ' + String(err);
    })
    .finally(() => { sending = false; });
  }

  if (askSend) askSend.addEventListener('click', sendQuestion);
  if (askBox)  askBox.addEventListener('keydown', (e) => { if (e.key === 'Enter') sendQuestion(); });

  // ========== Coin search (suggest + resolve) ==========
  const searchInput = document.getElementById('coinSearch');
  const searchGo    = document.getElementById('coinGo');
  const coinsList   = document.getElementById('coinsList');
  const tfSelect    = document.getElementById('tfSelect');

  function setDatalist(items){
    if (!coinsList) return;
    coinsList.innerHTML = '';
    (items || []).forEach(s => {
      const o = document.createElement('option');
      o.value = s; coinsList.appendChild(o);
    });
  }

  // initial suggestions
  fetch('/api/suggest').then(r => r.json()).then(j => setDatalist(j.symbols || [])).catch(()=>{});

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
    const q = (searchInput && searchInput.value || '').trim();
    if (!q) return;
    const tfSel  = (tfSelect && tfSelect.value) ? tfSelect.value : TF;

    fetch(`/api/resolve?query=${encodeURIComponent(q)}`)
      .then(r => r.json())
      .then(j => {
        const sym = j.symbol || q.toUpperCase();
        window.location.href = `/analyze?symbol=${encodeURIComponent(sym)}&tf=${encodeURIComponent(tfSel)}`;
      })
      .catch(()=>{ 
        window.location.href = `/analyze?symbol=${encodeURIComponent(q)}&tf=${encodeURIComponent(tfSel)}`;
      });
  }
  if (searchGo)    searchGo.addEventListener('click', goSearch);
  if (searchInput) searchInput.addEventListener('keydown', (e)=>{ if(e.key==='Enter') goSearch(); });

})();
