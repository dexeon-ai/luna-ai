/* Luna control_panel.js — robust wiring for Search, Expand and Ask */

(function () {
  const $ = (sel, root=document) => root.querySelector(sel);
  const $$ = (sel, root=document) => Array.from(root.querySelectorAll(sel));

  const symbolSel = $('#symbol');       // dropdown
  const tfSel     = $('#tf');           // dropdown
  const searchBox = $('#searchBox');    // free text
  const refresh   = $('#refreshBtn');
  const askBox    = $('#askBox');
  const askSend   = $('#askSend');

  const modal     = $('#modal');
  const mTitle    = $('#m-title');
  const mTF       = $('#m-tf');
  const mClose    = $('#m-close');
  const mChart    = $('#m-chart');
  const mTalk     = $('#m-talk');

  function currentSymbol() {
    // prefer dropdown; if user typed something new, use that when they hit Go
    return (symbolSel && symbolSel.value) ? symbolSel.value.trim() : (window.__SYMBOL__ || 'BTC');
  }
  function currentTF() {
    return (tfSel && tfSel.value) ? tfSel.value.trim() : (window.__TF__ || '12h');
  }

  // Top-bar form submission (Apply)
  const goLine = $('.go-line');
  if (goLine) {
    goLine.addEventListener('submit', (e) => {
      // Respect the dropdown choices; searchBox is for "Go" only
      // Native form submit ok
    });
  }

  // "Go" from search box → navigate with ?symbol=<entered>&tf=<tf>
  const goBtn = $('#goBtn');
  if (goBtn && searchBox) {
    goBtn.addEventListener('click', (e) => {
      e.preventDefault();
      const q = searchBox.value.trim();
      if (!q) return;
      const tf = currentTF();
      window.location.href = `/analyze?symbol=${encodeURIComponent(q)}&tf=${encodeURIComponent(tf)}`;
    });
    searchBox.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        goBtn.click();
      }
    });
  }

  // Refresh
  if (refresh) {
    refresh.addEventListener('click', () => {
      window.location.href = `/analyze?symbol=${encodeURIComponent(currentSymbol())}&tf=${encodeURIComponent(currentTF())}`;
    });
  }

  // Expand tile → open modal and fetch fresh data
  function openModalFor(key) {
    if (!key) return;
    modal.classList.remove('hidden');
    mTalk.textContent = 'Loading…';
    mChart.innerHTML = '';
    fetch(`/expand_json?symbol=${encodeURIComponent(currentSymbol())}&key=${encodeURIComponent(key)}&tf=${encodeURIComponent(currentTF())}`)
      .then(r => r.json())
      .then(data => {
        mTitle.textContent = data.title || (`${key} — ${currentSymbol()} (${currentTF()})`);
        mTalk.textContent = data.explain || '';
        if (data.html) {
          mChart.innerHTML = data.html;
        } else if (data.figure) {
          Plotly.newPlot(mChart, data.figure.data, data.figure.layout || {}, {responsive:true});
        } else {
          mChart.innerHTML = '<div class="chart-missing">No chart data.</div>';
        }
      })
      .catch(() => {
        mTalk.textContent = 'Error loading chart.';
        mChart.innerHTML = '<div class="chart-missing">Error.</div>';
      });
  }

  $$('.expand').forEach(btn => {
    btn.addEventListener('click', () => openModalFor(btn.dataset.key || btn.getAttribute('data-key')));
  });

  if (mClose) mClose.addEventListener('click', () => modal.classList.add('hidden'));
  if (mTF) {
    mTF.addEventListener('change', () => {
      const key = (mTitle.textContent || '').split('—')[0].trim();
      openModalFor(key);
    });
  }

  // Ask Luna
  function appendQA(q, a) {
    const qWrap = $('#qaQ'), aWrap = $('#qaA');
    if (qWrap) qWrap.insertAdjacentHTML('beforeend', `<div class="bubble q">• ${q}</div>`);
    if (aWrap) aWrap.insertAdjacentHTML('beforeend', `<div class="bubble a">${a}</div>`);
    if (aWrap) aWrap.scrollTop = aWrap.scrollHeight;
  }

  function ask() {
    const q = (askBox && askBox.value || '').trim();
    if (!q) return;
    const payload = { symbol: currentSymbol(), tf: currentTF(), q };
    appendQA(q, '…');
    askBox.value = '';
    fetch('/api/luna', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload) })
      .then(r => r.json()).then(data => {
        const last = $('#qaA .bubble.a:last-child');
        if (last) last.textContent = data.answer || '(no answer)';
      })
      .catch(() => {
        const last = $('#qaA .bubble.a:last-child');
        if (last) last.textContent = 'Error.';
      });
  }

  if (askSend) askSend.addEventListener('click', ask);
  if (askBox) askBox.addEventListener('keydown', (e) => { if (e.key === 'Enter') ask(); });
})();
