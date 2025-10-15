/* Luna Widget — Full Frontend Logic
   Requires Chart.js + chartjs-adapter-date-fns (optional fallback)
*/
(() => {
  "use strict";

  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));

  const ui = {
    statusDot: $("#statusDot"),
    qLeft: $("#qLeft"),
    meterFill: $("#meterFill"),
    latency: $("#latency"),
    chartEmpty: $("#chartEmpty"),
    quickStats: {
      symbol: $("#qsSymbol"),
      change: $("#qsChange"),
      price: $("#qsPrice"),
      vol: $("#qsVol"),
      mcap: $("#qsMcap"),
      risk: $("#qsRisk"),
    },
    avatar: $("#avatar"),
    avatarBase: $("#avatarBase"),
    mouthRest: $("#mouthRest"),
    mouthMid: $("#mouthMid"),
    mouthOpen: $("#mouthOpen"),
    prompt: $("#prompt"),
    sendBtn: $("#sendBtn"),
    sessionIdLabel: $("#sessionIdLabel"),
    audioState: $("#audioState"),
    replay: $("#replay"),
    copyTranscript: $("#copyTranscript"),
    resetSession: $("#resetSession"),
    transcript: $("#transcript"),
    toast: $("#toast"),
    metricSelect: $("#metricSelect"),
    sentimentScore: $("#sentimentScore"),
    suggestionList: $("#suggestionList"),
    voiceToggle: $("#voiceToggle"),
    voiceStyle: $("#voiceStyle"),
    overlayImg: $("#overlayImg"),
    analysisContent: $("#analysisContent"),
  };

  const CONFIG = {
    API_BASE: (window.LUNA_CONFIG && window.LUNA_CONFIG.API_BASE) || "https://luna-ai-j4dn.onrender.com",
    MAX_Q: 21,
    STATUS_POLL_MS: 45000,
    AVATAR_BASE: (window.LUNA_CONFIG?.ASSETS?.AVATAR_BASE) || "./assets/avatar_base.png",
    MOUTH_REST: (window.LUNA_CONFIG?.ASSETS?.MOUTH_REST) || "./assets/mouth_rest.png",
    MOUTH_MID: (window.LUNA_CONFIG?.ASSETS?.MOUTH_MID) || "./assets/mouth_mid.png",
    MOUTH_OPEN: (window.LUNA_CONFIG?.ASSETS?.MOUTH_OPEN) || "./assets/mouth_open.png",
  };

  const state = {
    sessionId: null,
    remaining: CONFIG.MAX_Q,
    transcript: [],
    lastVoice: null,
    lastOverlayUrl: null,
    lastAnswerText: null,
    isBusy: false,
    audio: null,
    audioStartAt: null,
    lipsyncTicker: null,
    lipsync: null,
    latencyMs: null,
    voiceEnabled: true,
    voiceStyle: "default",
    loadingInterval: null,
    lastSymbol: null,
  };

  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  function guessSymbolFromText(text) {
    if (!text) return null;
    const m = text.toUpperCase().match(/\b[A-Z]{2,5}\b/);
    return m ? m[0] : null;
  }

  function showLoading() {
    const el = $("#luna-loading");
    if (!el) return;
    el.style.display = "block";
    const messages = [
      "Fetching contract data…",
      "Analyzing token metrics…",
      "Generating chart…",
      "Preparing Luna’s response…"
    ];
    let i = 0;
    el.textContent = messages[0];
    clearInterval(state.loadingInterval);
    state.loadingInterval = setInterval(() => {
      i = (i + 1) % messages.length;
      el.textContent = messages[i];
    }, 5000);
  }

  function hideLoading() {
    const el = $("#luna-loading");
    if (!el) return;
    el.style.display = "none";
    clearInterval(state.loadingInterval);
    state.loadingInterval = null;
  }

  function showToast(msg, timeout = 2200) {
    ui.toast.textContent = msg;
    ui.toast.classList.add("show");
    setTimeout(() => ui.toast.classList.remove("show"), timeout);
  }

  function setSpeaking(on) {
    ui.avatar.classList.toggle("speaking", !!on);
    ui.audioState.textContent = on ? "speaking" : "idle";
  }

  function setMeter(val, max = CONFIG.MAX_Q) {
    const pct = Math.max(0, Math.min(1, (max - val) / max)) * 100;
    ui.meterFill.style.width = `${pct.toFixed(1)}%`;
  }

  function updateQLeft(n) {
    state.remaining = typeof n === "number" ? n : state.remaining;
    ui.qLeft.textContent = state.remaining;
    setMeter(state.remaining);
    ui.copyTranscript.classList.toggle("hide", state.remaining > 0);
  }

  function setStatusDot(ok) {
    ui.statusDot.style.background = ok ? "#37f59f" : "#f87171";
    ui.statusDot.style.boxShadow = ok
      ? "0 0 10px #37f59f66, 0 0 20px #37f59f33"
      : "0 0 10px #f8717166, 0 0 20px #f8717133";
  }

  function cacheBust(url) {
    if (!url) return url;
    const sep = url.includes("?") ? "&" : "?";
    return `${url}${sep}_=${Date.now()}`;
  }

  function sanitizeText(t) {
    return (t || "").toString().replace(/\s+/g, " ").trim();
  }

  function appendTranscript(q, a) {
    state.transcript.push({ q, a });
    const wrap = document.createElement("div");
    wrap.className = "qa";
    const qEl = document.createElement("div");
    qEl.className = "q"; qEl.textContent = `You: ${q}`;
    const aEl = document.createElement("div");
    aEl.className = "a"; aEl.textContent = a || "…";
    wrap.appendChild(qEl); wrap.appendChild(aEl);
    ui.transcript.appendChild(wrap);
    ui.transcript.scrollTop = ui.transcript.scrollHeight;
  }

  async function copyTranscript() {
    const lines = state.transcript.map(({ q, a }, i) => `Q${i + 1}: ${q}\nA${i + 1}: ${a}\n`);
    const text = lines.join("\n");
    try { await navigator.clipboard.writeText(text); showToast("Transcript copied to clipboard."); }
    catch { showToast("Couldn’t access clipboard."); }
  }

  function showSuggestions(symbol, metrics) {
    const suggestions = [
      symbol ? `What is the recent trading volume for ${symbol}?` : "What is the trading volume trend?",
      symbol ? `How volatile is ${symbol} compared to others?` : "Compare volatility across top tokens.",
      metrics?.market_cap ? `What drives ${symbol}'s market cap?` : "What factors influence market cap?",
    ];
    const list = ui.suggestionList;
    list.innerHTML = "";
    suggestions.forEach(s => {
      const btn = document.createElement("button");
      btn.textContent = s;
      btn.style.cssText = "padding: 5px 10px; background: #141a40; color: #E8EDFF; border: 1px solid #3a3f66; border-radius: 4px; cursor: pointer;";
      btn.addEventListener("click", () => { ui.prompt.value = s; ui.prompt.focus(); });
      list.appendChild(btn);
    });
    const wrap = $("#suggestions");
    if (wrap) wrap.style.display = suggestions.length ? "block" : "none";
  }

  // -------- Session ----------
  async function startSession() {
    try {
      const res = await fetch(`${CONFIG.API_BASE}/session/start`, {
        method: "POST", headers: { "Content-Type": "application/json" }, mode: "cors",
        body: JSON.stringify({ client: "luna_widget" }),
      });
      if (!res.ok) throw new Error("start session failed");
      const data = await res.json();
      state.sessionId = data.session_id || data.id || data.session || null;
      const remaining = data.remaining_questions ?? data.remaining ?? CONFIG.MAX_Q;
      updateQLeft(remaining);
      ui.sessionIdLabel.textContent = state.sessionId || "—";
      setStatusDot(true);
      return true;
    } catch (err) {
      setStatusDot(false);
      showToast("Could not start session. Check API_BASE & CORS.");
      return false;
    }
  }

  async function pollStatusLoop() {
    for (;;) {
      await sleep(CONFIG.STATUS_POLL_MS);
      if (!state.sessionId) continue;
      try {
        const res = await fetch(`${CONFIG.API_BASE}/session/status?session_id=${encodeURIComponent(state.sessionId)}`, { mode: "cors" });
        if (!res.ok) throw new Error("status fail");
        const data = await res.json();
        const remaining = data.remaining_questions ?? data.remaining ?? state.remaining;
        updateQLeft(remaining);
        setStatusDot(true);
      } catch {
        setStatusDot(false);
      }
    }
  }

  async function resetSession() {
    try {
      if (state.sessionId) {
        fetch(`${CONFIG.API_BASE}/session/end`, {
          method: "POST", headers: { "Content-Type": "application/json" }, mode: "cors",
          body: JSON.stringify({ session_id: state.sessionId }),
        }).catch(() => {});
      }
    } finally {
      stopAudioAndLipsync();
      state.sessionId = null; state.remaining = CONFIG.MAX_Q; state.transcript = [];
      state.lastVoice = null; state.lastOverlayUrl = null; state.lastSymbol = null;
      ui.sessionIdLabel.textContent = "—"; ui.transcript.innerHTML = "";
      updateQLeft(CONFIG.MAX_Q);
      ui.overlayImg.style.display = "none";
      ui.chartEmpty.classList.remove("hide");
      ui.copyTranscript.classList.add("hide");
      setSpeaking(false);
      const s = $("#suggestions"); if (s) s.style.display = "none";
      const sb = $("#sentimentBadge"); if (sb) sb.style.display = "none";
      await startSession();
    }
  }

  // -------- QA Flow ----------
  function normalizeQaPayload(data) {
    return {
      answer: data.answer || data.text || data.reply || data.response || "",
      overlayUrl: data.overlay_url || data.overlay_path || (data.overlay && (data.overlay.url || data.overlay.path)) || null,
      voicePath: data.voice_path || data.audio_path || data.voice || null,
      lipsyncPath: data.lipsync_path || data.viseme_path || null,
      remaining: data.remaining_questions ?? data.remaining ?? null,
      symbol: data.symbol || data.ticker || null,
      metrics: data.metrics || null,
      token_balance: data.token_balance ?? null,
    };
  }

  function pickRiskBadge(riskText) {
    if (!riskText) return { text: "—", cls: "risk-med" };
    const t = riskText.toLowerCase();
    if (t.includes("low")) return { text: riskText, cls: "risk-low" };
    if (t.includes("high")) return { text: riskText, cls: "risk-high" };
    return { text: riskText, cls: "risk-med" };
  }

  async function updateSentiment(symbol) {
    if (!symbol) { const sb = $("#sentimentBadge"); if (sb) sb.style.display = "none"; return; }
    try {
      const res = await fetch(`${CONFIG.API_BASE}/sentiment?symbol=${encodeURIComponent(symbol)}`, { mode: "cors" });
      if (!res.ok) throw new Error("Sentiment fetch failed");
      const data = await res.json();
      const score = data.sentiment_score || 0;
      const label = score > 0.3 ? "Positive" : score < -0.3 ? "Negative" : "Neutral";
      ui.sentimentScore.textContent = `${label} (${(score * 100).toFixed(0)}%)`;
      const sb = $("#sentimentBadge");
      if (sb) {
        sb.style.background = score > 0.3 ? "#37f59f" : score < -0.3 ? "#f87171" : "#3a3f66";
        sb.style.display = "block";
      }
    } catch { const sb = $("#sentimentBadge"); if (sb) sb.style.display = "none"; }
  }

  function updateQuickStats({ symbol, metrics }) {
    if (symbol) { ui.quickStats.symbol.textContent = symbol.toUpperCase(); updateSentiment(symbol); }
    if (metrics && typeof metrics === "object") {
      const { price, change_24h, volume_24h, market_cap, risk } = metrics;
      if (price != null) ui.quickStats.price.textContent = String(price);
      if (volume_24h != null) ui.quickStats.vol.textContent = String(volume_24h);
      if (market_cap != null) ui.quickStats.mcap.textContent = String(market_cap);
      if (change_24h != null) ui.quickStats.change.textContent = `${change_24h}`;
      if (risk != null) {
        const { text, cls } = pickRiskBadge(String(risk));
        ui.quickStats.risk.textContent = text;
        ui.quickStats.risk.classList.remove("risk-low", "risk-med", "risk-high");
        ui.quickStats.risk.classList.add(cls);
      }
    }
  }

  async function askLuna(questionRaw) {
    if (state.isBusy) return;
    const question = sanitizeText(questionRaw);
    if (!question) { showToast("Type a question first."); return; }
    if (!state.sessionId) {
      showToast("No session. Starting new session…");
      const ok = await startSession();
      if (!ok) return;
    }

    state.isBusy = true; ui.sendBtn.disabled = true; showLoading();
    try {
      const res = await fetch(`${CONFIG.API_BASE}/qa`, {
        method: "POST", headers: { "Content-Type": "application/json" }, mode: "cors",
        body: JSON.stringify({ session_id: state.sessionId, question }),
      });
      if (!res.ok) throw new Error(`/qa failed: ${res.status}`);
      const data = await res.json();
      const norm = normalizeQaPayload(data);

      state.lastAnswerText = norm.answer || "…";
      appendTranscript(question, state.lastAnswerText);
      ui.analysisContent.textContent = state.lastAnswerText || "No insights available.";
      if (norm.remaining != null) updateQLeft(norm.remaining);
      updateQuickStats({ symbol: norm.symbol, metrics: norm.metrics });
      showSuggestions(norm.symbol, norm.metrics);

      state.lastSymbol = (norm.symbol || state.lastSymbol || guessSymbolFromText(question) || "BTC").toUpperCase();
      state.lastOverlayUrl = norm.overlayUrl;
      showChart(norm.overlayUrl);

      if (norm.remaining === 0 && (norm.token_balance == null || norm.token_balance <= 0)) {
        showToast("Session limit reached and no Luna Tokens left. Start a new session or recharge tokens.");
      }

      if (norm.voicePath) {
        const voiceObj = { audioUrl: absolutize(norm.voicePath), lipsyncUrl: norm.lipsyncPath ? absolutize(norm.lipsyncPath) : null };
        await enrichAndPlayVoice(voiceObj).catch(e => showToast(`Voice error: ${e.message}`));
      } else {
        showToast("Voice response unavailable.");
      }
    } catch (err) {
      console.error("[Luna error]", err);
      showToast(`Error getting Luna’s response: ${err.message}`);
      setStatusDot(false);
    } finally {
      state.isBusy = false; ui.sendBtn.disabled = false; ui.prompt.value = ""; ui.prompt.focus(); hideLoading();
    }
  }

  // -------- Voice & Lipsync --------
  function absolutize(pathOrUrl) {
    if (!pathOrUrl) return null;
    if (/^https?:\/\//i.test(pathOrUrl)) return pathOrUrl;
    return `${CONFIG.API_BASE}${pathOrUrl.startsWith("/") ? "" : "/"}${pathOrUrl}`;
  }

  async function fetchLatestVoice() {
    const url = `${CONFIG.API_BASE}/voice/latest.json?session_id=${encodeURIComponent(state.sessionId)}&_=${Date.now()}`;
    const res = await fetch(url, { mode: "cors" });
    if (!res.ok) throw new Error("latest voice not available");
    const j = await res.json();
    const audioUrl = absolutize(j.audio_url || j.voice_path || j.url || j.audio);
    const lipsyncUrl = absolutize(j.lipsync_url || j.lipsync_path || j.viseme_url);
    const duration = j.duration || null;
    const visemes = Array.isArray(j.visemes) ? j.visemes : null;
    return { audioUrl, lipsyncUrl, duration, visemes };
  }

  async function enrichAndPlayVoice(voiceObj) {
    let { audioUrl, lipsyncUrl, duration, visemes } = voiceObj;
    if (!visemes && lipsyncUrl) {
      try {
        const res = await fetch(cacheBust(lipsyncUrl), { mode: "cors" });
        if (res.ok) {
          const lj = await res.json();
          if (Array.isArray(lj)) { visemes = lj; }
          else if (lj && Array.isArray(lj.visemes)) { visemes = lj.visemes; duration = duration || lj.duration || null; }
        }
      } catch {}
    }
    const final = { audioUrl, lipsync: visemes ? { visemes, duration: duration || null } : null };
    await playVoiceWithLipsync(final);
  }

  function stopAudioAndLipsync() {
    try { if (state.audio) { state.audio.pause(); state.audio.src = ""; } } catch {}
    state.audio = null; state.audioStartAt = null;
    if (state.lipsyncTicker) cancelAnimationFrame(state.lipsyncTicker);
    state.lipsyncTicker = null; state.lipsync = null;
    setSpeaking(false); showMouth("rest");
  }

  async function playVoiceWithLipsync({ audioUrl, lipsync }) {
    if (!state.voiceEnabled) return;
    stopAudioAndLipsync();
    if (!audioUrl) { showToast("No audio URL in response."); return; }
    const styledAudioUrl = audioUrl.replace(".wav", `_${state.voiceStyle}.wav`);
    state.audio = new Audio();
    state.audio.crossOrigin = "anonymous";
    state.audio.preload = "auto";
    state.audio.src = cacheBust(styledAudioUrl);
    let resolved = false;
    const t0 = performance.now();
    await new Promise((resolve) => {
      const onCanPlay = () => { if (resolved) return; resolved = true; state.latencyMs = Math.max(0, performance.now() - t0); ui.latency.textContent = `${Math.round(state.latencyMs)} ms`; resolve(); };
      const onError = () => { if (resolved) return; resolved = true; ui.latency.textContent = "—"; resolve(); };
      state.audio.addEventListener("canplay", onCanPlay, { once: true });
      state.audio.addEventListener("error", onError, { once: true });
    });
    setSpeaking(true);
    await state.audio.play().catch(() => {});
    state.audioStartAt = performance.now();
    if (lipsync && Array.isArray(lipsync.visemes) && lipsync.visemes.length) {
      state.lipsync = lipsync;
      runLipsyncTicker();
    } else {
      runFallbackMouth();
    }
    state.audio.addEventListener("ended", () => { stopAudioAndLipsync(); }, { once: true });
  }

  function runLipsyncTicker() {
    const audioEl = state.audio;
    if (!audioEl || !state.lipsync) return;
    const frames = state.lipsync.visemes.map((f) => ({ t: Number(f.t) || 0, v: String(f.v || f.viseme || "rest").toLowerCase() })).sort((a, b) => a.t - b.t);
    let i = 0;
    const step = () => {
      if (!audioEl || audioEl.ended || audioEl.paused) return;
      const t = audioEl.currentTime;
      while (i + 1 < frames.length && frames[i + 1].t <= t) i++;
      const cur = frames[i] || { v: "rest" };
      showMouth(visemeToMouth(cur.v));
      state.lipsyncTicker = requestAnimationFrame(step);
    };
    state.lipsyncTicker = requestAnimationFrame(step);
  }

  function runFallbackMouth() {
    const audioEl = state.audio;
    if (!audioEl) return;
    let phase = 0;
    const step = () => {
      if (!audioEl || audioEl.ended || audioEl.paused) return;
      phase = (phase + 1) % 3;
      showMouth(phase === 0 ? "rest" : phase === 1 ? "mid" : "open");
      state.lipsyncTicker = requestAnimationFrame(step);
    };
    state.lipsyncTicker = requestAnimationFrame(step);
  }

  function visemeToMouth(v) {
    const s = v.toLowerCase();
    if (/(sil|rest|m|b|p)/.test(s)) return "rest";
    if (/(e|ee|ih|ae|eh|nx|l|s|z|ch|th)/.test(s)) return "mid";
    return "open";
  }

  function showMouth(which) {
    ui.mouthRest.classList.remove("show");
    ui.mouthMid.classList.remove("show");
    ui.mouthOpen.classList.remove("show");
    if (which === "open") ui.mouthOpen.classList.add("show");
    else if (which === "mid") ui.mouthMid.classList.add("show");
    else ui.mouthRest.classList.add("show");
  }

  // -------- Chart ----------
  function showChart(overlayUrlOrDataUrl) {
    const img = ui.overlayImg;
    if (img && overlayUrlOrDataUrl && /\/overlays\//.test(overlayUrlOrDataUrl)) {
      img.src = cacheBust(overlayUrlOrDataUrl);
      img.style.display = "block";
      ui.chartEmpty.classList.add("hide");
      return;
    }
    ui.chartEmpty.classList.remove("hide");
    if (state.lastOverlayUrl && !/\/overlays\//.test(state.lastOverlayUrl)) {
      fetch(state.lastOverlayUrl, { mode: "cors" })
        .then(res => res.json())
        .then(data => {
          if (!data || !Array.isArray(data.labels) || !Array.isArray(data.datasets)) {
            ui.chartEmpty.textContent = "No chart data available.";
            return;
          }
          ui.chartEmpty.classList.add("hide");
        })
        .catch(() => {
          ui.chartEmpty.textContent = "Could not load chart data.";
        });
    }
  }

  function initChartControls() {
    $$("#chartControls button").forEach(btn => {
      btn.addEventListener("click", () => {
        $$("#chartControls button").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        const timeFrame = btn.dataset.time || "1d";
        fetchChartData(timeFrame, ui.metricSelect ? ui.metricSelect.value : "price");
      });
    });
    if (ui.metricSelect) {
      ui.metricSelect.addEventListener("change", () => {
        const activeBtn = $$("#chartControls button.active")[0] || $$("#chartControls button")[0];
        const tf = activeBtn ? activeBtn.dataset.time : "1d";
        fetchChartData(tf, ui.metricSelect.value);
      });
    }
  }

  async function fetchChartData(timeFrame, metric) {
    const symbol = (state.lastSymbol || ui.quickStats.symbol.textContent || "").trim() || "BTC";
    const url = `${CONFIG.API_BASE}/chart/data?symbol=${encodeURIComponent(symbol)}&time=${timeFrame}&metric=${metric}` +
                (state.sessionId ? `&session_id=${encodeURIComponent(state.sessionId)}` : "");
    showLoading();
    try {
      const res = await fetch(url, { mode: "cors" });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "chart data error");
      showChart(url);
    } catch (e) {
      console.error(e);
      ui.chartEmpty.textContent = "Could not load chart data.";
      ui.chartEmpty.classList.remove("hide");
    } finally { hideLoading(); }
  }

  // -------- Events ----------
  async function onSend() { await askLuna(ui.prompt.value); }
  function onReplay() { if (!state.lastVoice) { showToast("No previous audio to replay."); return; } playVoiceWithLipsync(state.lastVoice).catch(() => {}); }

  // -------- Init ----------
  async function preloadAssets() {
    ui.avatarBase.src = CONFIG.AVATAR_BASE;
    ui.mouthRest.src = CONFIG.MOUTH_REST;
    ui.mouthMid.src = CONFIG.MOUTH_MID;
    ui.mouthOpen.src = CONFIG.MOUTH_OPEN;
    await Promise.allSettled([imgPromise(ui.avatarBase), imgPromise(ui.mouthRest), imgPromise(ui.mouthMid), imgPromise(ui.mouthOpen)]);
  }

  function imgPromise(imgEl) {
    return new Promise((resolve) => {
      if (!imgEl) return resolve();
      if (imgEl.complete) return resolve();
      imgEl.onload = () => resolve();
      imgEl.onerror = () => resolve();
    });
  }

  async function init() {
    await preloadAssets();
    if (ui.sendBtn) ui.sendBtn.addEventListener("click", onSend);
    if (ui.prompt) ui.prompt.addEventListener("keydown", (e) => { if (e.key === "Enter" && !e.shiftKey) onSend(); });
    if (ui.replay) ui.replay.addEventListener("click", onReplay);
    if (ui.copyTranscript) ui.copyTranscript.addEventListener("click", copyTranscript);
    if (ui.resetSession) ui.resetSession.addEventListener("click", resetSession);
    if (ui.voiceToggle) ui.voiceToggle.addEventListener("click", () => { state.voiceEnabled = !state.voiceEnabled; ui.voiceToggle.textContent = state.voiceEnabled ? "Mute Voice" : "Enable Voice"; });
    if (ui.voiceStyle) ui.voiceStyle.addEventListener("change", () => { state.voiceStyle = ui.voiceStyle.value; });
    initChartControls();
    const ok = await startSession();
    if (ok) setStatusDot(true);
    pollStatusLoop();
    if (ui.prompt) ui.prompt.focus();
  }

  const _origEnrichAndPlayVoice = enrichAndPlayVoice;
  enrichAndPlayVoice = async function(voiceObj) { state.lastVoice = voiceObj; await _origEnrichAndPlayVoice(voiceObj); };
  const _origPlayVoiceWithLipsync = playVoiceWithLipsync;
  playVoiceWithLipsync = async function(obj) { state.lastVoice = obj; await _origPlayVoiceWithLipsync(obj); };
  const _origFetchLatestVoice = fetchLatestVoice;
  fetchLatestVoice = async function() { const v = await _origFetchLatestVoice(); state.lastVoice = v; return v; };

  init();
})();