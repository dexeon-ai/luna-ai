/* Luna Widget — Full Frontend Logic
   Drop next to luna_widget.html and ensure the ASSETS + API_BASE config above is correct.
*/

(() => {
  "use strict";

  /** ==============================
   *  DOM Helpers
   *  ============================== */
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));

  const ui = {
    statusDot: $("#statusDot"),
    qLeft: $("#qLeft"),
    meterFill: $("#meterFill"),
    latency: $("#latency"),
    chartImg: $("#chartImg"),
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
  };

  /** ==============================
   *  Config
   *  ============================== */
  const CONFIG = {
    API_BASE: (window.LUNA_CONFIG && window.LUNA_CONFIG.API_BASE) || "https://luna-ai-j4dn.onrender.com",
    MAX_Q: 21,
    STATUS_POLL_MS: 45000, // refresh session status
    // Expected asset paths (safe defaults; override via window.LUNA_CONFIG if needed)
    AVATAR_BASE: (window.LUNA_CONFIG && window.LUNA_CONFIG.ASSETS && window.LUNA_CONFIG.ASSETS.AVATAR_BASE) || "./assets/avatar_base.png",
    MOUTH_REST: (window.LUNA_CONFIG && window.LUNA_CONFIG.ASSETS && window.LUNA_CONFIG.ASSETS.MOUTH_REST) || "./assets/mouth_rest.png",
    MOUTH_MID:  (window.LUNA_CONFIG && window.LUNA_CONFIG.ASSETS && window.LUNA_CONFIG.ASSETS.MOUTH_MID)  || "./assets/mouth_mid.png",
    MOUTH_OPEN: (window.LUNA_CONFIG && window.LUNA_CONFIG.ASSETS && window.LUNA_CONFIG.ASSETS.MOUTH_OPEN) || "./assets/mouth_open.png",
  };

  /** ==============================
   *  State
   *  ============================== */
  const state = {
    sessionId: null,
    remaining: CONFIG.MAX_Q,
    transcript: [], // {q, a}
    lastVoice: null,  // { audioUrl, lipsyncUrl, visemes, duration }
    lastOverlayUrl: null,
    lastAnswerText: null,
    isBusy: false,
    audio: null,
    audioStartAt: null,
    lipsyncTicker: null,
    lipsync: null, // {visemes: [{t:number, v:string}], duration:number} OR null
    latencyMs: null,
  };

  /** ==============================
   *  Utilities
   *  ============================== */
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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
    const lines = state.transcript.map(({ q, a }, i) => {
      return `Q${i + 1}: ${q}\nA${i + 1}: ${a}\n`;
    });
    const text = lines.join("\n");
    try {
      await navigator.clipboard.writeText(text);
      showToast("Transcript copied to clipboard.");
    } catch {
      showToast("Couldn’t access clipboard.");
    }
  }

  /** ==============================
   *  Session API
   *  ============================== */
  async function startSession() {
    try {
      const res = await fetch(`${CONFIG.API_BASE}/session/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        mode: "cors",
        body: JSON.stringify({ client: "luna_widget" }),
      });
      if (!res.ok) throw new Error("start session failed");
      const data = await res.json();
      // Normalization: accept multiple naming conventions
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
        // Fire and forget
        fetch(`${CONFIG.API_BASE}/session/end`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          mode: "cors",
          body: JSON.stringify({ session_id: state.sessionId }),
        }).catch(() => {});
      }
    } finally {
      // Clear state
      stopAudioAndLipsync();
      state.sessionId = null;
      state.remaining = CONFIG.MAX_Q;
      state.transcript = [];
      state.lastVoice = null;
      state.lastOverlayUrl = null;
      ui.sessionIdLabel.textContent = "—";
      ui.transcript.innerHTML = "";
      updateQLeft(CONFIG.MAX_Q);
      ui.chartImg.classList.remove("visible");
      ui.chartEmpty.classList.remove("hide");
      ui.copyTranscript.classList.add("hide");
      setSpeaking(false);
      await startSession();
    }
  }

  /** ==============================
   *  QA API (Core Ask Flow)
   *  ============================== */
  function normalizeQaPayload(data) {
    // This is designed to be forgiving to backend payload shapes.
    const out = {
      answer: data.answer || data.text || data.reply || data.response || "",
      overlayUrl:
        data.overlay_url || data.overlay_path || (data.overlay && (data.overlay.url || data.overlay.path)) || null,
      voicePath: data.voice_path || data.audio_path || data.voice || null,
      lipsyncPath: data.lipsync_path || data.viseme_path || null,
      remaining: data.remaining_questions ?? data.remaining ?? null,
      symbol: data.symbol || data.ticker || null,
      metrics: data.metrics || null, // {price, change_24h, volume_24h, market_cap, risk}
    };
    return out;
  }

  function pickRiskBadge(riskText) {
    if (!riskText) return { text: "—", cls: "risk-med" };
    const t = riskText.toString().toLowerCase();
    if (t.includes("low")) return { text: riskText, cls: "risk-low" };
    if (t.includes("high")) return { text: riskText, cls: "risk-high" };
    return { text: riskText, cls: "risk-med" };
  }

  function updateQuickStats({ symbol, metrics }) {
    if (symbol) ui.quickStats.symbol.textContent = symbol.toUpperCase();
    if (metrics && typeof metrics === "object") {
      const { price, change_24h, volume_24h, market_cap, risk } = metrics;
      if (price != null) ui.quickStats.price.textContent = String(price);
      if (volume_24h != null) ui.quickStats.vol.textContent = String(volume_24h);
      if (market_cap != null) ui.quickStats.mcap.textContent = String(market_cap);
      if (change_24h != null) {
        ui.quickStats.change.textContent = `${change_24h}`;
      }
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
    if (!question) {
      showToast("Type a question first.");
      return;
    }
    if (!state.sessionId) {
      showToast("No session. Attempting to reconnect…");
      const ok = await startSession();
      if (!ok) return;
    }
    state.isBusy = true;
    ui.sendBtn.disabled = true;

    const t0 = performance.now();
    try {
      const res = await fetch(`${CONFIG.API_BASE}/qa`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        mode: "cors",
        body: JSON.stringify({ session_id: state.sessionId, question }),
      });
      if (!res.ok) throw new Error(`/qa failed: ${res.status}`);

      const data = await res.json();
      const norm = normalizeQaPayload(data);

      // Transcript rendering (answer text might be refined after we fetch /voice/latest.json)
      state.lastAnswerText = norm.answer || "…";
      appendTranscript(question, state.lastAnswerText);

      // Update remaining questions
      if (norm.remaining != null) {
        updateQLeft(norm.remaining);
      }

      // Update quick stats (symbol, metrics)
      updateQuickStats({ symbol: norm.symbol, metrics: norm.metrics });

      // Overlay image (chart screenshot)
      let overlayUrl = norm.overlayUrl;
      if (!overlayUrl) {
        // Fallback: if your backend exposes latest overlay
        overlayUrl = `${CONFIG.API_BASE}/overlays/latest.png`;
      }
      state.lastOverlayUrl = overlayUrl;
      showChart(overlayUrl);

      // Voice + Lipsync: if /qa didn’t include paths, query /voice/latest.json
      if (!norm.voicePath || !norm.lipsyncPath) {
        const voiceObj = await fetchLatestVoice();
        await playVoiceWithLipsync(voiceObj);
      } else {
        const voiceObj = {
          audioUrl: absolutize(norm.voicePath),
          lipsyncUrl: absolutize(norm.lipsyncPath),
        };
        await enrichAndPlayVoice(voiceObj);
      }

    } catch (err) {
      console.error(err);
      showToast("Error getting Luna’s response.");
      setStatusDot(false);
    } finally {
      state.isBusy = false;
      ui.sendBtn.disabled = false;
      ui.prompt.value = "";
      ui.prompt.focus();
    }
  }

  /** ==============================
   *  Voice & Lipsync
   *  ============================== */

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

    /* Accept flexible schemas:
       Example A:
       { "audio_url": "/voice/reply_123.wav", "lipsync_url": "/voice/reply_123.json", "duration": 3.82 }
       Example B:
       { "voice_path": "...", "lipsync_path": "...", "visemes": [{t:0.00,v:"rest"},...], "duration": 3.6 }
    */
    const audioUrl = absolutize(j.audio_url || j.voice_path || j.url || j.audio);
    const lipsyncUrl = absolutize(j.lipsync_url || j.lipsync_path || j.viseme_url);
    const duration = j.duration || null;
    const visemes = Array.isArray(j.visemes) ? j.visemes : null;
    return { audioUrl, lipsyncUrl, duration, visemes };
  }

  async function enrichAndPlayVoice(voiceObj) {
    let { audioUrl, lipsyncUrl, duration, visemes } = voiceObj;

    // If no inline visemes, load from lipsyncUrl
    if (!visemes && lipsyncUrl) {
      try {
        const res = await fetch(cacheBust(lipsyncUrl), { mode: "cors" });
        if (res.ok) {
          const lj = await res.json();
          // Accept either {visemes:[{t,v}], duration} or array directly
          if (Array.isArray(lj)) {
            visemes = lj;
          } else if (lj && Array.isArray(lj.visemes)) {
            visemes = lj.visemes;
            duration = duration || lj.duration || null;
          }
        }
      } catch {
        // ignore, will fallback to fake lipsync
      }
    }

    const final = { audioUrl, lipsync: visemes ? { visemes, duration: duration || null } : null };
    await playVoiceWithLipsync(final);
  }

  function stopAudioAndLipsync() {
    try {
      if (state.audio) {
        state.audio.pause();
        state.audio.src = "";
      }
    } catch {}
    state.audio = null;
    state.audioStartAt = null;
    if (state.lipsyncTicker) cancelAnimationFrame(state.lipsyncTicker);
    state.lipsyncTicker = null;
    state.lipsync = null;
    setSpeaking(false);
    showMouth("rest");
  }

  async function playVoiceWithLipsync({ audioUrl, lipsync }) {
    stopAudioAndLipsync();
    if (!audioUrl) {
      showToast("No audio URL in response.");
      return;
    }

    // Prepare <audio>
    state.audio = new Audio();
    state.audio.crossOrigin = "anonymous";
    state.audio.preload = "auto";
    state.audio.src = cacheBust(audioUrl);

    let resolved = false;
    const t0 = performance.now();

    await new Promise((resolve) => {
      const onCanPlay = () => {
        if (resolved) return;
        resolved = true;
        state.latencyMs = Math.max(0, performance.now() - t0);
        ui.latency.textContent = `${Math.round(state.latencyMs)} ms`;
        resolve();
      };
      const onError = () => {
        if (resolved) return;
        resolved = true;
        ui.latency.textContent = "—";
        resolve();
      };
      state.audio.addEventListener("canplay", onCanPlay, { once: true });
      state.audio.addEventListener("error", onError, { once: true });
    });

    setSpeaking(true);
    await state.audio.play().catch(() => {}); // Safari sometimes rejects without user gesture; handled by Ask click

    state.audioStartAt = performance.now();

    // If we have a lipsync timeline, drive mouths by time; else fallback to simple rhythmic animation.
    if (lipsync && Array.isArray(lipsync.visemes) && lipsync.visemes.length) {
      state.lipsync = lipsync;
      runLipsyncTicker();
    } else {
      // Fallback: cycle rest → mid → open while audio playing
      runFallbackMouth();
    }

    state.audio.addEventListener("ended", () => {
      stopAudioAndLipsync();
    }, { once: true });
  }

  function runLipsyncTicker() {
    const audioEl = state.audio;
    if (!audioEl || !state.lipsync) return;

    const frames = state.lipsync.visemes
      .map((f) => ({ t: Number(f.t) || 0, v: String(f.v || f.viseme || "rest").toLowerCase() }))
      .sort((a, b) => a.t - b.t);

    let i = 0;

    const step = () => {
      if (!audioEl || audioEl.ended || audioEl.paused) return;
      const t = audioEl.currentTime;
      // Advance frames as time passes
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
    // Map general viseme labels -> {rest, mid, open}
    // Common buckets: "sil"/"rest", "M/B/P" -> rest; "E", "AE" -> mid; "AA","IY","UW","AH","AO" -> open
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

  /** ==============================
   *  Chart & Overlay
   *  ============================== */
  function showChart(url) {
    if (!url) return;
    const finalUrl = absolutize(url);
    ui.chartEmpty.classList.add("hide");
    ui.chartImg.classList.remove("visible");
    ui.chartImg.onload = () => ui.chartImg.classList.add("visible");
    ui.chartImg.onerror = () => {
      ui.chartEmpty.textContent = "Could not load overlay chart image.";
      ui.chartEmpty.classList.remove("hide");
    };
    ui.chartImg.src = cacheBust(finalUrl);
  }

  /** ==============================
   *  Events
   *  ============================== */
  async function onSend() {
    await askLuna(ui.prompt.value);
  }

  function onReplay() {
    if (!state.lastVoice) {
      showToast("No previous audio to replay.");
      return;
    }
    playVoiceWithLipsync(state.lastVoice).catch(() => {});
  }

  /** ==============================
   *  Init
   *  ============================== */
  async function preloadAssets() {
    // Make sure avatar & mouths reflect config paths
    ui.avatarBase.src = CONFIG.AVATAR_BASE;
    ui.mouthRest.src = CONFIG.MOUTH_REST;
    ui.mouthMid.src = CONFIG.MOUTH_MID;
    ui.mouthOpen.src = CONFIG.MOUTH_OPEN;
    await Promise.allSettled([
      imgPromise(ui.avatarBase),
      imgPromise(ui.mouthRest),
      imgPromise(ui.mouthMid),
      imgPromise(ui.mouthOpen),
    ]);
  }

  function imgPromise(imgEl) {
    return new Promise((resolve) => {
      if (imgEl.complete) return resolve();
      imgEl.onload = () => resolve();
      imgEl.onerror = () => resolve();
    });
  }

  async function init() {
    await preloadAssets();

    ui.sendBtn.addEventListener("click", onSend);
    ui.prompt.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && !e.shiftKey) onSend();
    });
    ui.replay.addEventListener("click", onReplay);
    ui.copyTranscript.addEventListener("click", copyTranscript);
    ui.resetSession.addEventListener("click", resetSession);

    // Start session
    const ok = await startSession();
    if (ok) setStatusDot(true);
    pollStatusLoop();
    ui.prompt.focus();
  }

  // Store last voice object whenever we have one
  const _origEnrichAndPlayVoice = enrichAndPlayVoice;
  enrichAndPlayVoice = async function(voiceObj) {
    state.lastVoice = voiceObj;
    await _origEnrichAndPlayVoice(voiceObj);
  };

  const _origPlayVoiceWithLipsync = playVoiceWithLipsync;
  playVoiceWithLipsync = async function(obj) {
    state.lastVoice = obj;
    await _origPlayVoiceWithLipsync(obj);
  };

  // Patch askLuna so lastVoice is set even on fetchLatestVoice path
  const _origFetchLatestVoice = fetchLatestVoice;
  fetchLatestVoice = async function() {
    const v = await _origFetchLatestVoice();
    state.lastVoice = v;
    return v;
  };

  // Kick everything off
  init();

})();
