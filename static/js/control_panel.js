(function () {
  const $ = (sel) => document.querySelector(sel);

  const askBtn    = $("#ask-btn");
  const askInput  = $("#ask-input");
  const answerBox = $("#luna-answer-text");
  const symEl     = $("#ask-symbol");
  const tfEl      = $("#ask-tf");

  async function askLuna() {
    const symbol = symEl ? symEl.textContent.trim() : "ETH";
    const tf     = tfEl ? tfEl.textContent.trim() : "12h";
    const text   = (askInput && askInput.value || "").trim();

    if (!text) {
      answerBox.textContent = "Ask me something specific (e.g., ‘bearish next 24h?’, ‘what coin is this?’, ‘is this a rug?’).";
      return;
    }

    answerBox.textContent = "Thinking…";

    try {
      const resp = await fetch("/api/luna", {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({ symbol, tf, text })
      });
      const js = await resp.json();
      if (js && js.reply) {
        answerBox.textContent = js.reply;
      } else {
        answerBox.textContent = "No answer right now.";
      }
    } catch (e) {
      answerBox.textContent = "Network error asking Luna.";
    }
  }

  if (askBtn) {
    askBtn.addEventListener("click", askLuna);
  }
})();
