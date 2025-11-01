/* =========================================================
   LUNA — Plotly Resizer (Safe Add‑On)
   ---------------------------------------------------------
   • Keeps all charts snug inside the square tiles.
   • Works with dynamic content and modals.
   • No dependency on your existing JS – drop-in only.
   ========================================================= */

(function(){
  const TILE_KEYS = [
    "PRICE","RSI","MACD","MCAP","OBV",
    "BANDS","VOL","LIQ","ADX",
    "ALT","ALT_L","SENT","EXTRA_LEFT","EXTRA_RIGHT"
  ];

  function resizeGraphsIn(el){
    if(!el) return;
    el.querySelectorAll('.js-plotly-plot').forEach(g=>{
      try { window.Plotly.Plots.resize(g); } catch(e){}
    });
  }

  // Resize everything on load and on window resize
  function resizeAll(){
    TILE_KEYS.forEach(k=>{
      document.querySelectorAll(`.tile[data-key="${k}"]`).forEach(t=>resizeGraphsIn(t));
    });
  }

  // Keep modals aligned too (if you use Plotly in expand dialogs)
  document.addEventListener('click', (e)=>{
    const isExpand = e.target.closest('.expand') || e.target.matches('.expand');
    if(isExpand){
      // give your expand AJAX a moment to inject content, then resize
      setTimeout(resizeAll, 150);
      setTimeout(resizeAll, 400);
    }
  });

  // Use a ResizeObserver for each tile so Plotly keeps up with CSS-driven changes
  const ro = ('ResizeObserver' in window) ? new ResizeObserver(entries=>{
    for(const entry of entries){
      const el = entry.target;
      const g  = el.querySelector('.js-plotly-plot');
      if(g){ try { window.Plotly.Plots.resize(g); } catch(e){} }
    }
  }) : null;

  function watchTiles(){
    TILE_KEYS.forEach(k=>{
      document.querySelectorAll(`.tile[data-key="${k}"]`).forEach(t=>{
        if(ro){ ro.observe(t); }
      });
    });
  }

  window.addEventListener('load', ()=>{
    watchTiles();
    // Staggered passes to catch late-loaded graphs
    resizeAll();
    setTimeout(resizeAll, 120);
    setTimeout(resizeAll, 360);
  });

  window.addEventListener('resize', ()=>{
    resizeAll();
  });

  // Optional: allow manual trigger from elsewhere in your app
  window.LunaForceResize = resizeAll;
})();
