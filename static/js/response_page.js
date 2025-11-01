// response_page.js — Expand modal: fetch Plotly JSON and render
(() => {
  const modal = document.getElementById('modal');
  if (!modal) return;

  const titleEl   = modal.querySelector('.modal-title');
  const plotEl    = modal.querySelector('#modal-plot');
  const blurbEl   = modal.querySelector('#modal-blurb');
  const statsEl   = modal.querySelector('#modal-stats');
  const bulletsEl = modal.querySelector('#modal-bullets');
  const tfBtns    = modal.querySelectorAll('[data-tf]');
  const linkTfBtn = modal.querySelector('#modal-link-tf'); // optional toggle
  const autoBtn   = modal.querySelector('#modal-autoscale');
  const hSlider   = modal.querySelector('#modal-h'); // <input type="range" ...>

  // normalize UI TF → backend TF keys
  const tfAlias = { "1d":"24h", "3m":"3mo", "6m":"6mo", "9m":"9mo" };

  let state = {
    symbol: '',
    key: 'MAIN',
    tf: '1y',
    linkGlobal: true,
    h: 520
  };

  const open  = () => modal.classList.add('open');
  const close = () => modal.classList.remove('open');
  modal.querySelector('#modal-close').onclick = close;

  // Open handlers on all ".expand-btn" links
  document.querySelectorAll('.expand-btn').forEach(btn => {
    btn.addEventListener('click', e => {
      e.preventDefault();
      const u = new URL(btn.href);
      state.symbol = u.searchParams.get('symbol') || (window.currentSymbol || 'BTC');
      state.key    = (u.searchParams.get('key') || 'MAIN').toUpperCase();
      state.tf     = '1y';
      // highlight default TF
      tfBtns.forEach(b => b.classList.toggle('active', b.dataset.tf === state.tf));
      fetchAndRender();
      open();
    });
  });

  function canonicalTf(tf) {
    return tfAlias[tf] || tf;
  }

  async function fetchAndRender() {
    const tf = canonicalTf(state.tf);
    const url = `/expand_json?symbol=${encodeURIComponent(state.symbol)}&key=${encodeURIComponent(state.key)}&tf=${encodeURIComponent(tf)}&h=${encodeURIComponent(state.h)}`;

    try {
      const r = await fetch(url, { cache: 'no-store' });
      const data = await r.json();

      titleEl.textContent = data.title || state.key;

      // Plotly figure
      if (data.fig) {
        const fig = data.fig;
        Plotly.react(plotEl, fig.data || [], fig.layout || {}, { responsive: true });
      } else {
        plotEl.innerHTML = '<div style="padding:18px;color:#a8acc4">No data</div>';
      }

      // Blurb + detail
      blurbEl.innerHTML = `
        <div class="b">${data.blurb || ''}</div>
        <div class="d">${data.detail || ''}</div>
      `;

      // Stats cards
      statsEl.innerHTML = (data.stats || []).map(s =>
        `<div class="card"><div class="k">${s.label}</div><div class="v">${s.value}</div></div>`
      ).join('');

      // Bullets
      bulletsEl.innerHTML = (data.bullets || []).map(b => `<li>${b}</li>`).join('');
    } catch (err) {
      plotEl.innerHTML = `<div style="padding:18px;color:#ff9ea1">Failed to load: ${err}</div>`;
    }
  }

  // timeframe buttons
  tfBtns.forEach(b => {
    b.addEventListener('click', () => {
      state.tf = b.dataset.tf;
      tfBtns.forEach(x => x.classList.toggle('active', x === b));
      fetchAndRender();
    });
  });

  // Link TF to global (optional switch if you added it)
  if (linkTfBtn) {
    linkTfBtn.addEventListener('click', () => {
      state.linkGlobal = !state.linkGlobal;
      linkTfBtn.classList.toggle('active', state.linkGlobal);
    });
  }

  // Height slider (if present)
  if (hSlider) {
    hSlider.addEventListener('input', () => {
      state.h = parseInt(hSlider.value || '520', 10);
      fetchAndRender();
    });
  }

  // Copy & Save
  document.getElementById('modal-copy').onclick = () => {
    const text = (blurbEl.textContent || '') + '\n' +
      Array.from(bulletsEl.querySelectorAll('li')).map(li => '- ' + li.textContent).join('\n');
    navigator.clipboard.writeText(text);
  };
  document.getElementById('modal-savepng').onclick = () => {
    Plotly.downloadImage(plotEl, { format: 'png', filename: `${state.symbol}_${state.key}_${state.tf}` });
  };
})();
