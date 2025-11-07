// Luna AI — control_panel.js (expand modal + Ask Luna)

(function () {
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));

  // --- Expand modal ---
  const modal = $("#modal");
  const mClose = $("#m-close");
  const mChart = $("#m-chart");
  const mTalk  = $("#m-talk");
  const mTf    = $("#m-tf");
  let currentKey = null;

  function openModal(key) {
    currentKey = key;
    modal.classList.remove("hidden");
    fetch(`/expand_json?symbol=${encodeURIComponent(window.__SYMBOL__)}&key=${encodeURIComponent(key)}&tf=${encodeURIComponent(mTf.value)}`)
      .then(r => r.json())
      .then(j => {
        mChart.innerHTML = "";
        Plotly.newPlot(mChart, j.fig.data, j.fig.layout || {}, {responsive: true});
        mTalk.textContent = j.talk || "";
      })
      .catch(err => { mTalk.textContent = "Error loading chart."; console.error(err); });
  }

  function closeModal() { modal.classList.add("hidden"); }

  mClose.addEventListener("click", closeModal);
  modal.addEventListener("click", (e) => { if (e.target === modal) closeModal(); });
  mTf.addEventListener("change", () => { if (currentKey) openModal(currentKey); });

  $$(".expand").forEach(btn => {
    btn.addEventListener("click", () => {
      const key = btn.dataset.key || btn.closest(".tile")?.dataset?.key;
      if (key) openModal(key);
    });
  });

  // --- Ask Luna ---
  const askBox  = $("#askBox");
  const askSend = $("#askSend");
  const qaQ = $("#qaQ"), qaA = $("#qaA");

  function pushQA(q, a) {
    if (q) {
      const li = document.createElement("div");
      li.className = "bubble q";
      li.textContent = "• " + q;
      qaQ.appendChild(li);
      qaQ.scrollTop = qaQ.scrollHeight;
    }
    if (a) {
      const li = document.createElement("div");
      li.className = "bubble a";
      li.textContent = a;
      qaA.appendChild(li);
      qaA.scrollTop = qaA.scrollHeight;
    }
  }

  askSend?.addEventListener("click", () => {
    const q = (askBox.value || "").trim();
    if (!q) return;
    pushQA(q, null);
    askBox.value = "";
    fetch("/api/luna", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({symbol: window.__SYMBOL__, tf: $("#tf")?.value || window.__TF__, text: q})
    })
    .then(r => r.json())
    .then(j => pushQA(null, j.reply))
    .catch(err => pushQA(null, "Error: " + String(err)));
  });

  // --- Refresh button ---
  $("#refreshBtn")?.addEventListener("click", () => {
    fetch(`/api/refresh/${encodeURIComponent(window.__SYMBOL__)}`)
      .then(() => window.location.reload());
  });

})();
