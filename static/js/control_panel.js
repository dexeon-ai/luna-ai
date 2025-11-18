/* Luna control_panel.js — FULL CLEAN WORKING VERSION */

(function () {
  const $  = (sel, root=document) => root.querySelector(sel);
  const $$ = (sel, root=document) => Array.from(root.querySelectorAll(sel));

  // Grab DOM elements
  const askBox  = $('#askBox');
  const askSend = $('#askSend');
  const qaQ     = $('#qaQ');
  const qaA     = $('#qaA');

  const symbolSel = $('#symbol');
  const tfSel     = $('#tf');
  const searchBox = $('#searchBox');
  const refresh   = $('#refreshBtn');

  const modal  = $('#modal');
  const mTitle = $('#m-title');
  const mTF    = $('#m-tf');
  const mClose = $('#m-close');
  const mChart = $('#m-chart');
  const mTalk  = $('#m-talk');

  /* Utility: Get symbol & timeframe from page */
  function currentSymbol() {
    return document.body.getAttribute("data-symbol") || symbolSel?.value || "BTC";
  }

  function currentTF() {
    return document.body.getAttribute("data-tf-default") || tfSel?.value || "12h";
  }

  /* Utility: Add Q & A bubbles */
  function pushQA(q, a) {
    if (q) {
      const el = document.createElement("div");
      el.className = "bubble q";
      el.textContent = "• " + q;
      qaQ.appendChild(el);
      qaQ.scrollTop = qaQ.scrollHeight;
    }
    if (a) {
      const el = document.createElement("div");
      el.className = "bubble a";
      el.textContent = a;
      qaA.appendChild(el);
      qaA.scrollTop = qaA.scrollHeight;
    }
  }

  /* Search box “Go” button */
  const goBtn = $('#goBtn');
  if (goBtn && searchBox) {
    goBtn.addEventListener("click", (e) => {
      e.preventDefault();
      const q = searchBox.value.trim();
      if (!q) return;
      window.location.href =
        `/analyze?symbol=${encodeURIComponent(q)}&tf=${encodeURIComponent(currentTF())}`;
    });

    searchBox.addEventListener("keydown", (e) => {
      if (e.key === "Enter") goBtn.click();
    });
  }

  /* Refresh Button */
  if (refresh) {
    refresh.addEventListener("click", () => {
      window.location.href =
        `/analyze?symbol=${encodeURIComponent(currentSymbol())}&tf=${encodeURIComponent(currentTF())}`;
    });
  }

  /* Expand tile modal */
  function openModalFor(key) {
    if (!key) return;

    modal.classList.remove("hidden");
    mTalk.textContent = "Loading…";
    mChart.innerHTML = "";

    fetch(`/expand_json?symbol=${encodeURIComponent(currentSymbol())}&key=${encodeURIComponent(key)}&tf=${encodeURIComponent(currentTF())}`)
      .then(r => r.json())
      .then(data => {
        mTitle.textContent = `${key} — ${currentSymbol()} (${currentTF()})`;

        mTalk.textContent = data.talk || "No commentary.";
        if (data.fig) {
          Plotly.newPlot(mChart, data.fig.data, data.fig.layout || {}, {responsive:true});
        } else {
          mChart.innerHTML = "<div class='chart-missing'>No chart available.</div>";
        }
      })
      .catch(err => {
        mTalk.textContent = "Error loading chart.";
        mChart.innerHTML = "<div class='chart-missing'>Error loading.</div>";
      });
  }

  $$(".expand").forEach(btn => {
    btn.addEventListener("click", () => {
      const key = btn.dataset.key || btn.getAttribute("data-key");
      openModalFor(key);
    });
  });

  if (mClose) {
    mClose.addEventListener("click", () => modal.classList.add("hidden"));
  }

  if (mTF) {
    mTF.addEventListener("change", () => {
      const key = (mTitle.textContent || "").split("—")[0].trim();
      openModalFor(key);
    });
  }

  /* ASK LUNA — FULL FIXED VERSION */
  if (askSend && askBox) {
    askSend.addEventListener("click", () => {
      const q = (askBox.value || "").trim();
      if (!q) return;

      pushQA(q, null);

      const payload = {
        symbol: currentSymbol(),
        tf: currentTF(),
        text: q
      };

      fetch("/api/luna", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(payload)
      })
      .then(r => r.json())
      .then(j => {
        pushQA(null, j.reply || "(no response)");
      })
      .catch(err => {
        pushQA(null, "Error: " + String(err));
      });

      askBox.value = "";
    });

    askBox.addEventListener("keydown", (e) => {
      if (e.key === "Enter") askSend.click();
    });
  }
})();
