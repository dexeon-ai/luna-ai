// control_panel.js — add-on behaviors without breaking the grid

(function() {
  // ---------- helpers ----------
  function qs(sel, root) { return (root || document).querySelector(sel); }
  function qsa(sel, root) { return Array.from((root || document).querySelectorAll(sel)); }
  function getParam(name) {
    const u = new URL(window.location.href);
    return u.searchParams.get(name);
  }
  function setParam(name, val) {
    const u = new URL(window.location.href);
    if (val == null) u.searchParams.delete(name); else u.searchParams.set(name, val);
    window.location.href = u.toString();
  }

  // ---------- main tile toggles (Cap/Price + Lin/Log) ----------
  function injectMainToggles() {
    const tile = qs('#tile-PRICE') || qs('[data-key="PRICE"]') || qs('.tile-price') || qs('.tile[data-key="PRICE"]');
    if (!tile) return;
    const wrap = document.createElement('div');
    wrap.style.position = 'absolute';
    wrap.style.top = '6px';
    wrap.style.right = '6px';
    wrap.style.display = 'flex';
    wrap.style.gap = '6px';
    wrap.style.zIndex = 5;

    const viewPref = (localStorage.getItem('luna_view_pref') || '').toLowerCase(); // 'cap' or 'price'
    const scalePref = (localStorage.getItem('luna_scale_pref') || '').toLowerCase(); // 'log' or 'lin'

    const btnView = document.createElement('button');
    btnView.textContent = viewPref === 'price' ? 'Price' : 'Cap';
    btnView.title = 'Toggle Cap/Price';
    btnView.className = 'luna-mini-btn';
    btnView.onclick = function() {
      const next = (btnView.textContent === 'Cap') ? 'price' : 'cap';
      localStorage.setItem('luna_view_pref', next);
      // round-trip via URL param ?view=
      setParam('view', next);
    };

    const btnScale = document.createElement('button');
    btnScale.textContent = scalePref === 'log' ? 'Log' : 'Lin';
    btnScale.title = 'Toggle Linear/Log';
    btnScale.className = 'luna-mini-btn';
    btnScale.onclick = function() {
      const next = (btnScale.textContent === 'Lin') ? 'log' : 'lin';
      localStorage.setItem('luna_scale_pref', next);
      setParam('scale', next);
    };

    wrap.appendChild(btnView);
    wrap.appendChild(btnScale);
    tile.style.position = tile.style.position || 'relative';
    tile.appendChild(wrap);

    // ensure URL params reflect prefs if user opens a fresh link
    // only if no explicit URL param provided
    const urlView = getParam('view');
    if (!urlView && viewPref) setParam('view', viewPref);
    const urlScale = getParam('scale');
    if (!urlScale && scalePref) setParam('scale', scalePref);
  }

  // ---------- Ask-Luna speak (Web Speech API; safe no-op if unsupported) ----------
  function wireAskLunaSpeech() {
    // expect an element that receives the answer text
    // we try a few likely IDs; non-destructive
    const answerBox = qs('#luna-answer') || qs('#answer') || qs('#answers') || qs('.luna-answer');
    if (!answerBox) return;

    // simple observer: when textContent changes, speak it
    if (!('SpeechSynthesisUtterance' in window)) return;
    let lastSpoken = '';
    const obs = new MutationObserver(() => {
      const txt = (answerBox.textContent || '').trim();
      if (!txt || txt === lastSpoken) return;
      lastSpoken = txt;
      try {
        const u = new SpeechSynthesisUtterance(txt);
        u.rate = 1.05; u.pitch = 1.10;
        window.speechSynthesis.cancel();
        window.speechSynthesis.speak(u);
      } catch(e) {}
    });
    obs.observe(answerBox, { childList: true, subtree: true, characterData: true });
  }

  // ---------- init ----------
  document.addEventListener('DOMContentLoaded', function() {
    try { injectMainToggles(); } catch(e) { console.warn(e); }
    try { wireAskLunaSpeech(); } catch(e) { console.warn(e); }
  });
})();
