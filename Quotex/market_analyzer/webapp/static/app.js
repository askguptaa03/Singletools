// ══════════════════════════════════════════════════════════════════════════════
// SignalPro front-end — talks ONLY to the existing endpoints:
//   POST /api/signal        (asset, timeframe) -> full analysis pipeline result
//   GET  /api/live-prices   -> top OTC pair prices
//   GET  /healthz           -> connection check
// Nothing here places an order or invents a backend route.
// ══════════════════════════════════════════════════════════════════════════════

const $ = (id) => document.getElementById(id);

/* ────────────────────────────────────────────────────────────────────────────
 * Live Quotex Asset Availability (Steps 2-7) — single client-side source of
 * truth for "which OTC assets are currently tradeable right now", backed
 * ONLY by GET /api/assets/live (which itself wraps the existing live
 * Quotex session — no fabrication, no static-list fallback presented as
 * "available"). window._OTC_ASSETS (server-rendered from
 * api_quotex/constants.py) remains the KNOWN/REFERENCE universe only —
 * used here purely for a "Known X / Available Y" comparison, and for
 * Backtest, which intentionally stays on the static/historical universe
 * per the live-vs-historical distinction (Step 9) and is NOT wired to
 * this layer.
 * ──────────────────────────────────────────────────────────────────────────── */
let _liveOtcState = {
  loaded: false,        // has a live fetch ever completed (success or documented failure)?
  success: false,       // did the last fetch actually reach a live Quotex session?
  assets: [],           // currently-available OTC symbols, exactly as Quotex reported
  knownCount: 0,        // static reference-universe OTC count (constants.py)
  availableCount: 0,
  error: null,          // human-readable reason when success is false
  discoveredAt: null,
};
let _liveOtcListeners = [];  // callbacks to re-render whenever the state changes

function onLiveOtcAssetsChanged(cb) {
  _liveOtcListeners.push(cb);
}

function _notifyLiveOtcListeners() {
  for (const cb of _liveOtcListeners) {
    try { cb(_liveOtcState); } catch (e) { /* one bad listener shouldn't break the rest */ }
  }
}

async function refreshLiveOtcAssets(forceRefresh) {
  try {
    const url = '/api/assets/live' + (forceRefresh ? '?refresh=1' : '');
    const res = await fetch(url);
    const data = await res.json();
    if (data.success) {
      _liveOtcState = {
        loaded: true, success: true,
        assets: data.available_otc_assets || [],
        knownCount: data.known_otc_count ?? 0,
        availableCount: data.available_otc_count ?? 0,
        error: null, discoveredAt: data.discovered_at || null,
      };
    } else {
      _liveOtcState = {
        loaded: true, success: false, assets: [],
        knownCount: data.known_otc_count ?? 0, availableCount: 0,
        error: data.error || 'Live asset availability unavailable.',
        discoveredAt: null,
      };
    }
  } catch (e) {
    _liveOtcState = {
      loaded: true, success: false, assets: [],
      knownCount: _liveOtcState.knownCount, availableCount: 0,
      error: `Failed to reach live asset endpoint: ${e.message}`,
      discoveredAt: null,
    };
  }
  _notifyLiveOtcListeners();
  return _liveOtcState;
}

// Renders the "Known OTC Universe: X · Currently Available OTC: Y" banner
// (or the zero-assets / session-error message) into any container. Shared
// by every live-relevant asset selector so the wording/logic lives once.
function renderLiveOtcBanner(containerId) {
  const el = $(containerId);
  if (!el) return;
  const s = _liveOtcState;
  if (!s.loaded) {
    el.textContent = 'Checking live Quotex asset availability…';
    el.className = 'page-intro small';
    return;
  }
  if (!s.success) {
    el.textContent = `⚠ Live asset availability unavailable — ${s.error}`;
    el.className = 'error-box';
    return;
  }
  if (s.availableCount === 0) {
    el.textContent = 'No OTC assets currently available from Quotex.';
    el.className = 'error-box';
    return;
  }
  el.textContent = `Known OTC Universe: ${s.knownCount} · Currently Available OTC: ${s.availableCount}`;
  el.className = 'page-intro small';
}

/* ────────────────────────────────────────────────────────────────────────────
 * Formatting helpers
 * ──────────────────────────────────────────────────────────────────────────── */

function fmtLabel(sym) {
  const isOtc = sym.toLowerCase().endsWith('_otc');
  const base = isOtc ? sym.slice(0, -4) : sym;
  const label = /^[A-Z]{6}$/.test(base) ? base.slice(0, 3) + '/' + base.slice(3) : base;
  return isOtc ? label + ' (OTC)' : label;
}

function tfLabel(tf) {
  if (!tf) return '—';
  const m = tf.match(/^(\d+)(s|m|h)$/);
  if (!m) return tf.toUpperCase();
  const unit = m[2] === 's' ? 'S' : m[2] === 'm' ? 'M' : 'H';
  return unit + m[1];
}

function timeAgo(ts) {
  if (!ts) return '—';
  const diff = Math.max(0, Math.floor((Date.now() - ts) / 1000));
  if (diff < 5) return 'Just now';
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  return `${Math.floor(diff / 3600)}h ago`;
}

function directionWord(sig) {
  if (sig === 'BUY') return 'Strong Bullish';
  if (sig === 'SELL') return 'Strong Bearish';
  return 'No Clear Setup';
}

function trendWord(trend) {
  if (!trend) return '—';
  const t = String(trend).toUpperCase();
  if (t.includes('BULL')) return 'Bullish';
  if (t.includes('BEAR')) return 'Bearish';
  return 'Neutral';
}

function todayKey() {
  const d = new Date();
  return `${d.getFullYear()}-${d.getMonth() + 1}-${d.getDate()}`;
}

/* ────────────────────────────────────────────────────────────────────────────
 * Count-up / gauge animation helpers
 * ──────────────────────────────────────────────────────────────────────────── */

function animateCount(el, target, duration = 900) {
  const start = 0;
  const t0 = performance.now();
  function step(now) {
    const p = Math.min(1, (now - t0) / duration);
    const eased = 1 - Math.pow(1 - p, 3);
    el.textContent = Math.round(start + (target - start) * eased);
    if (p < 1) requestAnimationFrame(step);
  }
  requestAnimationFrame(step);
}

function setGauge(circleEl, valueEl, percent, signalClass) {
  const R = 52;
  const C = 2 * Math.PI * R;
  circleEl.setAttribute('stroke-dasharray', C.toFixed(1));
  circleEl.classList.remove('sell', 'wait');
  if (signalClass === 'sell') circleEl.classList.add('sell');
  else if (signalClass === 'wait') circleEl.classList.add('wait');
  requestAnimationFrame(() => {
    const offset = C - (Math.max(0, Math.min(100, percent)) / 100) * C;
    circleEl.style.strokeDashoffset = offset.toFixed(1);
  });
  animateCount(valueEl, Math.round(percent));
}

/* ────────────────────────────────────────────────────────────────────────────
 * Shared signal-card renderer — used by both the Auto Scanner hero card and
 * the Manual Analyzer result card. Built from the <template id="signal-card-template">.
 * ──────────────────────────────────────────────────────────────────────────── */

function buildSignalCard() {
  const tpl = $('signal-card-template');
  return tpl.content.firstElementChild.cloneNode(true);
}

function fieldsOf(card) {
  const map = {};
  card.querySelectorAll('[data-f]').forEach((el) => { map[el.dataset.f] = el; });
  return map;
}

/* ── Part 2-6: shared signal-actionable countdown ("Entry Window") ──────────
 * One shared 1-second ticker drives every visible countdown element on the
 * page (hero card, manual analyzer result card, scanner result rows) by
 * re-querying [data-entry-window-end] each tick — no per-card timers to
 * create/leak/clear. Purely a display concern: never affects signal
 * generation, never places a trade. Distinct from "Expiry" (Quotex trade
 * duration label) — this counts down to the NEXT CANDLE BOUNDARY the
 * backend computed (entry_window_end), derived from the actual candle
 * timestamp server-side, not from page-load time.
 * ────────────────────────────────────────────────────────────────────────── */
function _formatCountdownRemaining(msRemaining) {
  if (msRemaining <= 0) return null; // caller shows the expired state
  const totalSeconds = Math.ceil(msRemaining / 1000);
  const mm = Math.floor(totalSeconds / 60);
  const ss = totalSeconds % 60;
  return `${String(mm).padStart(2, '0')}:${String(ss).padStart(2, '0')}`;
}

function _renderCountdownInto(el, isoEnd, expiredText) {
  const endMs = Date.parse(isoEnd);
  if (Number.isNaN(endMs)) { el.textContent = '—'; return; }
  const remaining = endMs - Date.now();
  const formatted = _formatCountdownRemaining(remaining);
  if (formatted) {
    el.textContent = formatted;
    el.classList.remove('countdown-expired');
  } else {
    el.textContent = expiredText || 'Signal expired';
    el.classList.add('countdown-expired');
  }
}

function _tickAllCountdowns() {
  document.querySelectorAll('[data-entry-window-end]').forEach((el) => {
    const isoEnd = el.dataset.entryWindowEnd;
    if (!isoEnd) return;
    const isRow = el.dataset.countdownKind === 'row';
    _renderCountdownInto(el, isoEnd, isRow ? 'EXPIRED' : 'Signal expired');
  });
}
setInterval(_tickAllCountdowns, 1000);

function renderFilters(container, data) {
  const factors = {};
  (data.factors || []).forEach((f) => { factors[f.key] = f; });
  const ind = data.indicators || {};
  const mt = data.multi_tf;

  const items = [
    { label: 'EMA 50', state: data.trend && trendWord(data.trend) !== 'Neutral' ? 'pass' : 'fail' },
    { label: 'EMA 200', state: mt ? (mt.status === 'CONFIRMED' ? 'pass' : 'fail') : 'na' },
    { label: 'ADX > 25', state: ind.adx != null ? (ind.adx > 25 ? 'pass' : 'fail') : 'na' },
    { label: 'ATR', state: ind.atr_pct != null ? 'pass' : 'na' },
    { label: 'RSI', state: factors.rsi_div ? (factors.rsi_div.vote !== 'NEUTRAL' ? 'pass' : 'fail') : 'na' },
    { label: 'Bollinger Bands', state: factors.bb ? (factors.bb.vote !== 'NEUTRAL' ? 'pass' : 'fail') : 'na' },
    { label: 'OBV', state: 'na' },
    { label: 'Candlestick', state: (ind.candlestick_pattern || (factors.candle && factors.candle.vote !== 'NEUTRAL')) ? 'pass' : 'fail' },
    { label: 'Multi Timeframe', state: mt ? (mt.status === 'CONFIRMED' ? 'pass' : 'fail') : 'na' },
  ];

  container.innerHTML = items.map((it) => {
    const icon = it.state === 'pass' ? '✓' : it.state === 'fail' ? '✕' : '–';
    return `<div class="filter-pill ${it.state}"><span class="fi">${icon}</span>${it.label}</div>`;
  }).join('');
}

function renderMetrics(container, data) {
  const ind = data.indicators || {};
  const conf = data.confluence || { confidence: 0 };
  const adx = ind.adx;
  let strength = 'N/A';
  if (adx != null) {
    strength = adx >= 40 ? 'Very Strong' : adx >= 25 ? 'Strong' : adx >= 15 ? 'Moderate' : 'Weak';
  }
  const volatility = ind.volatility_level || 'N/A';
  const volumeConfirm = data.agree === true ? 'Confirmed' : data.agree === false ? 'Unconfirmed' : 'N/A';

  const cells = [
    ['Trend Strength', strength, adx != null ? `ADX ${adx}` : ''],
    ['Volatility', volatility, ind.atr_pct != null ? `ATR ${ind.atr_pct}%` : ''],
    ['Confidence', `${conf.confidence}%`, 'Confluence score'],
    ['Volume Confirmation', volumeConfirm, 'Cross-check vs legacy vote'],
  ];
  container.innerHTML = cells.map(([k, v, sub]) => `
    <div class="metric-card">
      <span class="k">${k}</span>
      <span class="v">${v}</span>
      ${sub ? `<span class="sub">${sub}</span>` : ''}
    </div>
  `).join('');
}

let _chartRegistry = new WeakMap();

function renderCardChart(canvas, data) {
  if (!window.Chart || !canvas) return;

  // Chart.js needs the canvas to be attached to the live DOM so it can
  // calculate dimensions/styles correctly. Signal cards are cloned from
  // a <template>, so the first render can happen while the canvas is still
  // detached from the document.
  if (!canvas.isConnected) {
    requestAnimationFrame(() => renderCardChart(canvas, data));
    return;
  }

  const hist = (data && data.price_history) || [];
  if (hist.length < 2) { canvas.parentElement.style.display = 'none'; return; }
  canvas.parentElement.style.display = '';

  const sig = (data.confluence || {}).signal || 'WAIT';
  const color = sig === 'BUY' ? '#00e676' : sig === 'SELL' ? '#ff3d5a' : '#9d5cff';
  const fill = sig === 'BUY' ? 'rgba(0,230,118,0.08)' : sig === 'SELL' ? 'rgba(255,61,90,0.08)' : 'rgba(157,92,255,0.06)';
  const labels = hist.map((_, i) => i + 1);

  let chart = _chartRegistry.get(canvas);
  if (chart) {
    chart.data.labels = labels;
    chart.data.datasets[0].data = hist;
    chart.data.datasets[0].borderColor = color;
    chart.data.datasets[0].backgroundColor = fill;
    chart.update('none');
    return;
  }

  chart = new Chart(canvas, {
    type: 'line',
    data: { labels, datasets: [{ data: hist, borderColor: color, backgroundColor: fill, borderWidth: 1.6, pointRadius: 0, tension: 0.35, fill: true }] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 400 },
      plugins: {
        legend: { display: false },
        tooltip: {
          mode: 'index', intersect: false,
          callbacks: { label: (ctx) => `Price: ${ctx.parsed.y}` },
          backgroundColor: 'rgba(10,15,20,0.92)', titleColor: '#8592a8', bodyColor: '#fff',
          borderColor: 'rgba(255,255,255,0.08)', borderWidth: 1,
        },
      },
      scales: {
        x: { display: false },
        y: {
          grid: { color: 'rgba(255,255,255,0.04)', drawBorder: false },
          ticks: { color: '#4c5a70', font: { size: 10, family: "'JetBrains Mono', monospace" }, maxTicksLimit: 4 },
          border: { display: false },
        },
      },
    },
  });
  _chartRegistry.set(canvas, chart);
}

/**
 * Fill a cloned signal-card template with a /api/signal response.
 * opts: { expiry, tfOverride, showChart }
 */
function fillSignalCard(card, data, opts = {}) {
  const f = fieldsOf(card);
  const sig = (data.confluence || {}).signal || 'WAIT';
  const conf = (data.confluence || {}).confidence || 0;
  const sigClass = sig === 'BUY' ? 'buy' : sig === 'SELL' ? 'sell' : 'wait';

  f.asset.textContent = fmtLabel(data.asset);
  f['tf-badge'].textContent = `[${tfLabel(data.timeframe)}]${data.is_otc ? ' · OTC' : ''}`;
  f.direction.textContent = sig;
  f.direction.className = `hero-direction ${sigClass}`;
  f.subtitle.textContent = directionWord(sig);

  setGauge(f['gauge-circle'], f['gauge-value'], conf, sigClass);
  animateCount(f.score, conf);
  f.trend.textContent = trendWord(data.trend);

  const compareTf = data.multi_tf ? data.multi_tf.compare_tf : null;
  f.timeframes.textContent = compareTf ? `${tfLabel(data.timeframe)} + ${tfLabel(compareTf)}` : tfLabel(data.timeframe);
  f.expiry.textContent = opts.expiry || tfLabel(data.timeframe).replace('M', '') + ' Minute';

  // Part 2-6: signal actionable countdown ("Entry Window") — distinct from
  // the "Expiry" fact above (that's the Quotex trade-duration label, not
  // touched). Tied to the candle boundary the backend computed
  // (entry_window_end), NOT to page-load time or a fixed fake timer. Only
  // shown for an actual BUY/SELL — a WAIT never gets a misleading
  // countdown. The actual per-second update happens in the shared
  // _tickAllCountdowns() ticker (see below) so one interval drives every
  // visible countdown (hero card, manual result card, scanner rows) rather
  // than one setInterval per card.
  const ewEl = f['entry-window'];
  if (ewEl) {
    if (sig !== 'WAIT' && data.entry_window_end) {
      ewEl.dataset.entryWindowEnd = data.entry_window_end;
      ewEl.dataset.countdownKind = 'card';
      _renderCountdownInto(ewEl, data.entry_window_end);
    } else {
      delete ewEl.dataset.entryWindowEnd;
      ewEl.textContent = '—';
      ewEl.classList.remove('countdown-expired');
    }
  }

  // Req 8: show analysis timestamp instead of static "Just Now"
  const _now = new Date();
  f.updated.textContent = _now.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', hour12: true })
    + ' · ' + _now.toLocaleDateString('en-IN', { day: '2-digit', month: 'short' });
  f.payout.textContent = data.payout_pct ? `${data.payout_pct}%` : '—';

  renderFilters(f.filters, data);
  renderMetrics(f.metrics, data);

  if (opts.showChart === false) {
    f['chart-wrap'].style.display = 'none';
  } else {
    const canvas = f['chart-wrap'].querySelector('canvas');
    renderCardChart(canvas, data);
  }

  return f;
}

/* ────────────────────────────────────────────────────────────────────────────
 * Networking — bounded-timeout fetch against /api/signal
 * ──────────────────────────────────────────────────────────────────────────── */

const REQUEST_TIMEOUT_MS = 20000;
const MANUAL_REQUEST_TIMEOUT_MS = 60000;

function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }

async function fetchSignal(asset, timeframe, timeoutMs = REQUEST_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch('/api/signal', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ asset, timeframe }),
      signal: controller.signal,
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data.error) return { ok: false, error: data.error || `Request failed (${res.status}).` };
    return { ok: true, data };
  } catch (err) {
    return { ok: false, error: err.name === 'AbortError' ? `Timed out after ${Math.round(timeoutMs / 1000)}s waiting for a response.` : `Network error: ${err.message}` };
  } finally {
    clearTimeout(timer);
  }
}

/* ────────────────────────────────────────────────────────────────────────────
 * Single-flight request lock — guarantees at most one /api/signal request is
 * ever in flight at a time, whether it originates from the Auto Scanner loop
 * or a Manual Analyze click. Every caller must go through enqueueSignalRequest
 * instead of calling fetchSignal directly.
 * ──────────────────────────────────────────────────────────────────────────── */

let _requestChain = Promise.resolve();

function enqueueSignalRequest(asset, timeframe, timeoutMs = REQUEST_TIMEOUT_MS) {
  const run = _requestChain.then(() => fetchSignal(asset, timeframe, timeoutMs));
  _requestChain = run.then(() => undefined, () => undefined);
  return run;
}

/* ────────────────────────────────────────────────────────────────────────────
 * Connection indicator (via existing /healthz)
 * ──────────────────────────────────────────────────────────────────────────── */

async function pingHealth() {
  const el = $('conn-indicator');
  try {
    const res = await fetch('/healthz', { cache: 'no-store' });
    el.classList.toggle('online', res.ok);
    el.classList.toggle('offline', !res.ok);
  } catch (_) {
    el.classList.remove('online');
    el.classList.add('offline');
  }
}
pingHealth();
setInterval(pingHealth, 20000);

/* ────────────────────────────────────────────────────────────────────────────
 * Header: drawer + notifications
 * ──────────────────────────────────────────────────────────────────────────── */

const drawer = $('drawer');
const drawerOverlay = $('drawer-overlay');

function openDrawer() {
  drawer.hidden = false;
  drawerOverlay.hidden = false;
  drawer.setAttribute('aria-hidden', 'false');
}
function closeDrawer() {
  drawer.hidden = true;
  drawerOverlay.hidden = true;
  drawer.setAttribute('aria-hidden', 'true');
}
$('menu-btn').addEventListener('click', openDrawer);
$('drawer-close').addEventListener('click', closeDrawer);
drawerOverlay.addEventListener('click', closeDrawer);
drawer.querySelectorAll('.drawer-link').forEach((btn) => {
  btn.addEventListener('click', () => { closeDrawer(); navigateTo(btn.dataset.nav); });
});

const notifPanel = $('notif-panel');
let _notifFeed = [];
let _unseenNotifs = 0;

function pushNotification(data) {
  const sig = (data.confluence || {}).signal;
  if (sig !== 'BUY' && sig !== 'SELL') return;
  _notifFeed.unshift({
    asset: data.asset, signal: sig, confidence: (data.confluence || {}).confidence || 0, ts: Date.now(),
  });
  _notifFeed = _notifFeed.slice(0, 20);
  _unseenNotifs += 1;
  $('notif-dot').hidden = _unseenNotifs === 0;
  renderNotifPanel();
}

function renderNotifPanel() {
  const list = $('notif-list');
  if (_notifFeed.length === 0) {
    list.innerHTML = '<div class="tab-empty small">No signals yet — run a scan to populate this feed.</div>';
    return;
  }
  list.innerHTML = _notifFeed.map((n) => `
    <div class="notif-row">
      <div class="notif-row-left">
        <span class="notif-asset">${fmtLabel(n.asset)}</span>
        <span class="notif-time">${timeAgo(n.ts)}</span>
      </div>
      <span class="dir-chip ${n.signal.toLowerCase()}">${n.signal} ${n.confidence}%</span>
    </div>
  `).join('');
}

$('notif-btn').addEventListener('click', () => {
  notifPanel.hidden = !notifPanel.hidden;
  if (!notifPanel.hidden) { _unseenNotifs = 0; $('notif-dot').hidden = true; }
});
document.addEventListener('click', (e) => {
  if (!notifPanel.hidden && !notifPanel.contains(e.target) && e.target !== $('notif-btn')) notifPanel.hidden = true;
});

/* ────────────────────────────────────────────────────────────────────────────
 * Navigation: bottom nav + segmented tabs share the same view state
 * ──────────────────────────────────────────────────────────────────────────── */

const VIEWS = ['scanner', 'analyzer', 'signals', 'history', 'settings', 'backtest', 'indicators', 'session', 'smart-scanner', 'validation', 'learning', 'ai-health'];
let _activeView = 'scanner';
let _scanTimer = null;
let _marketsTimer = null;

// Phase 7.4 — explicit opt-in only. This flag (and _scanTimer) live purely
// in memory: nothing here writes to localStorage/sessionStorage, so a
// fresh page load ALWAYS starts with the scanner OFF, satisfying "browser
// reopen -> scanner remains OFF" without any special-case code needed.
let _autoScanEnabled = false;

function navigateTo(name) {
  if (!VIEWS.includes(name)) return;
  _activeView = name;

  $('view-scanner').hidden = name !== 'scanner';
  $('view-analyzer').hidden = name !== 'analyzer';
  $('page-signals').hidden = name !== 'signals';
  $('page-history').hidden = name !== 'history';
  $('page-settings').hidden = name !== 'settings';
  $('page-backtest').hidden = name !== 'backtest';
  $('page-indicators').hidden = name !== 'indicators';
  $('page-session').hidden = name !== 'session';
  $('page-smart-scanner').hidden = name !== 'smart-scanner';
  $('page-validation').hidden = name !== 'validation';
  $('page-learning').hidden = name !== 'learning';

  document.querySelectorAll('.nav-item[data-nav]').forEach((b) => b.classList.toggle('active', b.dataset.nav === name));
  document.querySelectorAll('.seg-tab[data-view]').forEach((b) => {
    const active = (name === 'scanner' && b.dataset.view === 'scanner') || (name === 'analyzer' && b.dataset.view === 'analyzer');
    b.classList.toggle('active', active);
  });

  // Phase 7.4: navigating to (or away from) the Auto Scanner tab NEVER
  // starts or stops scanning by itself anymore — only the explicit
  // "Enable Auto Scanner" toggle button does that. Visiting the tab just
  // reflects whatever the current toggle state already is.
  if (name === 'scanner') syncAutoScanToggleUI();

  if (name === 'signals') {
    loadMarkets();
    _marketsTimer = setInterval(loadMarkets, 15000);
    renderFullSignalsList();
  } else {
    clearInterval(_marketsTimer);
    _marketsTimer = null;
  }

  if (name === 'history') renderHistory();
  if (name === 'settings') initSettingsPage();
  if (name === 'backtest') initBacktestPage();
  if (name === 'indicators') loadIndicatorsPage();
  if (name === 'session') loadSessionPage();
  // Phase 8.2: navigating to the Smart Scanner tab NEVER starts the
  // scanner — initSmartScannerPage() only issues a GET
  // /api/scanner/status to reflect whatever's already true server-side
  // (e.g. it was started earlier and is still running in the
  // background), exactly like visiting the Backtest tab doesn't start a
  // backtest. Only the Start button click below calls POST /api/scanner/start.
  if (name === 'smart-scanner') {
    initSmartScannerPage();
  } else {
    // Phase 8.3: stop the client-side status/results polling the moment
    // the user leaves this tab — this only stops the UI refresh timer,
    // NOT the scanner itself (which keeps running server-side exactly as
    // before). Re-visiting the tab calls pollScannerStatus() once via
    // initSmartScannerPage(), which resumes the interval automatically if
    // the server reports the scanner is still running.
    if (typeof stopScannerPolling === 'function') stopScannerPolling();
  }
  if (name === 'validation') {
    initValidationPage();
  } else if (typeof vStopPolling === 'function') {
    // Same guarantee as Smart Scanner's stopScannerPolling(): only stops
    // the client-side status-poll timer. The validation run itself keeps
    // going server-side (ValidationEngine, on the shared _BG_LOOP)
    // regardless of which tab is open. Revisiting the tab resyncs
    // automatically via initValidationPage()'s first pollValidationStatus() call.
    vStopPolling();
  }
  if (name === 'learning') {
    initLearningPage();
  } else if (typeof lStopPolling === 'function') {
    // Same pattern as Validation/Smart Scanner above — only stops the
    // client-side poll timer; nothing server-side is running to begin
    // with (Learning has no background job, only on-demand computation),
    // so this just avoids polling a tab the user isn't looking at.
    lStopPolling();
  }
  if (name === 'ai-health') {
    initAiHealthPage();
  }
}

document.querySelectorAll('.nav-item[data-nav]').forEach((btn) => btn.addEventListener('click', () => navigateTo(btn.dataset.nav)));
document.querySelectorAll('.seg-tab[data-view]').forEach((btn) => btn.addEventListener('click', () => navigateTo(btn.dataset.view)));
document.querySelectorAll('.quick-link-btn[data-nav]').forEach((btn) => btn.addEventListener('click', () => navigateTo(btn.dataset.nav)));

/* ── Auto Scanner explicit toggle ────────────────────────────────────────── */
function syncAutoScanToggleUI() {
  const btn = $('auto-scan-toggle-btn');
  const sub = $('auto-scan-status-sub');
  if (!btn) return;
  btn.setAttribute('aria-checked', String(_autoScanEnabled));
  if (sub) sub.textContent = _autoScanEnabled ? 'On — scanning every few minutes' : 'Off — press Enable to start scanning';
}

function toggleAutoScan() {
  _autoScanEnabled = !_autoScanEnabled;
  if (_autoScanEnabled) startAutoScan(); else stopAutoScan();
  syncAutoScanToggleUI();
}

const _autoScanToggleBtn = $('auto-scan-toggle-btn');
if (_autoScanToggleBtn) _autoScanToggleBtn.addEventListener('click', toggleAutoScan);

// "When the website closes, stop the scanner." — the interval dies with the
// page anyway, but this makes the intent explicit and also covers a
// same-tab hard navigation away from the app.
window.addEventListener('beforeunload', () => { stopAutoScan(); });

/* ────────────────────────────────────────────────────────────────────────────
 * Overview stat card
 * ──────────────────────────────────────────────────────────────────────────── */

function setStatus(kind, label) {
  const el = $('stat-status');
  el.innerHTML = `<span class="status-dot ${kind}"></span>${label}`;
}
function setUpdated(ts) { $('stat-updated').textContent = timeAgo(ts); }
function setPayout(pct) { $('stat-payout').textContent = pct != null ? `${pct}%` : '—'; }

/* ────────────────────────────────────────────────────────────────────────────
 * Signals-today counter (persisted per calendar day)
 * ──────────────────────────────────────────────────────────────────────────── */

const SIGNALS_TODAY_KEY = 'signalpro_signals_today_v1';

function bumpSignalsToday(count) {
  if (count <= 0) return;
  let store = { date: todayKey(), count: 0 };
  try { store = JSON.parse(localStorage.getItem(SIGNALS_TODAY_KEY)) || store; } catch (_) {}
  if (store.date !== todayKey()) store = { date: todayKey(), count: 0 };
  store.count += count;
  try { localStorage.setItem(SIGNALS_TODAY_KEY, JSON.stringify(store)); } catch (_) {}
  $('mini-signals-today').textContent = store.count;
}

function loadSignalsToday() {
  let store = { date: todayKey(), count: 0 };
  try { store = JSON.parse(localStorage.getItem(SIGNALS_TODAY_KEY)) || store; } catch (_) {}
  if (store.date !== todayKey()) store = { date: todayKey(), count: 0 };
  $('mini-signals-today').textContent = store.count;
}

/* ────────────────────────────────────────────────────────────────────────────
 * AUTO SCANNER (Home dashboard)
 * Scans the CURRENT LIVE OTC snapshot sequentially through the existing
 * /api/signal pipeline (one request at a time — keeps the single gunicorn
 * worker responsive instead of saturating it with concurrent scans).
 * ──────────────────────────────────────────────────────────────────────────── */

function buildScanList() {
  // Live Asset Availability fix: this used to pre-filter to a hardcoded
  // 6-letter-symbol regex + a fixed 4-symbol commodity/crypto whitelist,
  // then cap the result at 16 — meaning the Home dashboard's scan target
  // was NEVER the actual live OTC count (e.g. it silently dropped any
  // live symbol that didn't match those patterns, and truncated
  // everything else to 16 regardless of how many were really live).
  // That directly violated "live OTC count = scan target" and caused a
  // fixed ~16-asset ceiling independent of what Quotex actually reports.
  //
  // Fixed: scan target is now the live OTC snapshot exactly as-is — no
  // pattern filter, no cap. If Quotex has 5, this scans 5; 40, this scans
  // 40; 0, this scans 0. Same rule the backend ScannerEngine already
  // follows for the Scanner tab (Finding E) — this just brings the Home
  // dashboard's own scan loop in line with it.
  //
  // Before the first live fetch resolves, fall back to the static
  // window._OTC_ASSETS universe ONLY so the dashboard isn't empty on
  // first paint — this is a loading placeholder, not a live-availability
  // fallback: it is replaced the moment the live listener below fires,
  // including replacing it with an empty list if live discovery reports
  // zero/unavailable (never silently kept as if it were live).
  return _liveOtcState.loaded ? [..._liveOtcState.assets] : (window._OTC_ASSETS || []);
}

let SCAN_LIST = buildScanList();

// Live Asset Availability: rebuild SCAN_LIST whenever a live refresh
// completes and reflect the new count on the visible stat badge. Does not
// touch a scan that is already mid-run (runScan() below captures its own
// loop bound from SCAN_LIST.length at iteration time).
onLiveOtcAssetsChanged(() => {
  SCAN_LIST = buildScanList();
  const el = $('stat-assets');
  if (el) el.textContent = SCAN_LIST.length;
});

const SCAN_TIMEFRAME = window._DEFAULT_TF || '5m';
const SCAN_REQUEST_GAP_MS = 700;
const SCAN_INTERVAL_MS = 3 * 60 * 1000;

let _scanning = false;
let _scanPaused = false;
let _lastScanResults = [];

/** Blocks the scanner loop while a Manual Analyze request holds the pause flag. */
async function waitWhileScanPaused() {
  while (_scanPaused) {
    await sleep(150);
    if (!_scanning) return;
  }
}

function fmtExpiryFromTf(tf) {
  const m = String(tf || '').match(/^(\d+)(s|m|h)$/);
  if (!m) return tf;
  const n = parseInt(m[1], 10);
  if (m[2] === 's') return `${n} Second${n === 1 ? '' : 's'}`;
  if (m[2] === 'm') return `${n} Minute${n === 1 ? '' : 's'}`;
  return `${n} Hour${n === 1 ? '' : 's'}`;
}

async function runScan() {
  if (_scanning || _scanPaused) return;
  _scanning = true;
  setStatus('scanning', 'Scanning');
  const results = [];

  for (let i = 0; i < SCAN_LIST.length; i++) {
    await waitWhileScanPaused(); // yield to a Manual Analyze request if one starts
    if (!_scanning) return; // cancelled mid-scan (tab switched away)

    const asset = SCAN_LIST[i];
    $('scan-progress-text').textContent = `Scanning ${i + 1}/${SCAN_LIST.length} — ${fmtLabel(asset)}`;
    const res = await enqueueSignalRequest(asset, SCAN_TIMEFRAME, REQUEST_TIMEOUT_MS);
    if (res.ok) {
      results.push(res.data);
      saveToHistory(res.data);
      if ((res.data.confluence || {}).signal !== 'WAIT') pushNotification(res.data);
    }
    if (i < SCAN_LIST.length - 1) await sleep(SCAN_REQUEST_GAP_MS);
    if (!_scanning) return; // cancelled mid-scan (tab switched away)
  }

  _lastScanResults = results;
  const nonWait = results.filter((r) => (r.confluence || {}).signal !== 'WAIT');
  bumpSignalsToday(nonWait.length);

  renderScanResults(results);
  setStatus('live', 'Live');
  setUpdated(Date.now());
  _scanning = false;
}

function renderScanResults(results) {
  const nonWait = results.filter((r) => (r.confluence || {}).signal !== 'WAIT');
  const sorted = [...nonWait].sort((a, b) => (b.confluence.confidence || 0) - (a.confluence.confidence || 0));

  const topBuy = sorted.find((r) => r.confluence.signal === 'BUY');
  const topSell = sorted.find((r) => r.confluence.signal === 'SELL');
  $('mini-top-buy').textContent = topBuy ? `${fmtLabel(topBuy.asset)} · ${topBuy.confluence.confidence}%` : '—';
  $('mini-top-sell').textContent = topSell ? `${fmtLabel(topSell.asset)} · ${topSell.confluence.confidence}%` : '—';

  const heroData = sorted[0];
  const heroSlot = $('hero-slot');
  if (heroData) {
    const card = buildSignalCard();
    fillSignalCard(card, heroData, { expiry: fmtExpiryFromTf(heroData.timeframe) });
    heroSlot.innerHTML = '';
    heroSlot.appendChild(card);
    setPayout(heroData.payout_pct);
  } else {
    heroSlot.innerHTML = `
      <div class="glass-card signal-hero placeholder">
        <span class="placeholder-icon">🌙</span>
        <p>No active BUY/SELL setups right now.</p>
        <p class="placeholder-sub">All scanned pairs are in WAIT or the market is closed. The scanner will try again shortly.</p>
      </div>`;
  }

  const listEl = $('signals-list');
  if (sorted.length === 0) {
    listEl.innerHTML = '<div class="tab-empty">No qualifying signals this cycle — check back soon.</div>';
  } else {
    listEl.innerHTML = sorted.map((r) => signalRowHtml(r)).join('');
    bindSignalRowClicks(listEl, sorted);
  }
}

function signalRowHtml(r) {
  const sig = r.confluence.signal;
  const cls = sig.toLowerCase();
  const trendIco = sig === 'BUY' ? '▲' : sig === 'SELL' ? '▼' : '●';
  const hasCountdown = sig !== 'WAIT' && r.entry_window_end;
  const countdownHtml = hasCountdown
    ? `<span class="signal-row-countdown" data-entry-window-end="${r.entry_window_end}" data-countdown-kind="row">--:--</span>`
    : '';
  return `
    <div class="signal-row" data-asset="${r.asset}">
      <div class="signal-row-left">
        <span class="trend-ico" style="color:${sig === 'BUY' ? 'var(--bull)' : sig === 'SELL' ? 'var(--bear)' : 'var(--wait)'}">${trendIco}</span>
        <div class="signal-row-pair">
          <span class="signal-row-asset">${fmtLabel(r.asset)}</span>
          <span class="signal-row-meta">${tfLabel(r.timeframe)}${r.payout_pct ? ' · ' + r.payout_pct + '% payout' : ''}</span>
        </div>
      </div>
      <div class="signal-row-right">
        ${countdownHtml}
        <span class="signal-row-score">${r.confluence.confidence}/100</span>
        <span class="dir-chip ${cls}">${sig}</span>
      </div>
    </div>`;
}

function bindSignalRowClicks(container, results) {
  container.querySelectorAll('.signal-row').forEach((row) => {
    row.addEventListener('click', () => {
      const data = results.find((r) => r.asset === row.dataset.asset);
      if (!data) return;
      selectManualAsset(data.asset);
      $('man-timeframe').value = data.timeframe;
      navigateTo('analyzer');
      renderManualResult(data);
    });
  });
}

function renderFullSignalsList() {
  const el = $('signals-list-full');
  if (!el) return;
  if (_lastScanResults.length === 0) {
    el.innerHTML = '<div class="tab-empty">Run the Auto Scanner to populate this list.</div>';
    return;
  }
  const sorted = [..._lastScanResults].sort((a, b) => (b.confluence.confidence || 0) - (a.confluence.confidence || 0));
  el.innerHTML = sorted.map((r) => signalRowHtml(r)).join('');
  bindSignalRowClicks(el, sorted);
}

function startAutoScan() {
  $('stat-assets').textContent = SCAN_LIST.length;
  if (_lastScanResults.length === 0) runScan();
  clearInterval(_scanTimer);
  _scanTimer = setInterval(() => { if (!_scanPaused) runScan(); }, SCAN_INTERVAL_MS);
}
function stopAutoScan() {
  clearInterval(_scanTimer);
  _scanTimer = null;
}

$('rescan-btn').addEventListener('click', () => { if (!_scanning && !_scanPaused) runScan(); });

/* ────────────────────────────────────────────────────────────────────────────
 * MANUAL ANALYZER
 * ──────────────────────────────────────────────────────────────────────────── */

function initAssetGrid(gridId, searchId, hiddenId) {
  const grid = $(gridId);
  const search = $(searchId);
  const hidden = $(hiddenId);
  if (!grid || !hidden) return null;

  let all = [];

  function buildList() {
    // Live Asset Availability: once a live fetch has completed, the OTC
    // portion of this grid is EXACTLY the live-discovered list — never the
    // static window._OTC_ASSETS universe. Before the first live fetch
    // resolves, fall back to the static list so the grid isn't empty on
    // first paint; that fallback is replaced the moment refreshLiveOtcAssets()
    // completes, via the onLiveOtcAssetsChanged() listener registered below.
    const otcSyms = _liveOtcState.loaded ? _liveOtcState.assets : (window._OTC_ASSETS || []);
    const otcList = otcSyms.map((s) => ({ sym: s, group: 'OTC' }));
    all = otcList;
  }

  function render(q) {
    q = (q || '').toLowerCase();
    const shown = q ? all.filter((a) => a.sym.toLowerCase().includes(q) || fmtLabel(a.sym).toLowerCase().includes(q)) : all;
    const frag = document.createDocumentFragment();
    let lastGroup = '';
    for (const { sym, group } of shown) {
      if (group !== lastGroup) {
        const lbl = document.createElement('div');
        lbl.className = 'asset-group-label';
        lbl.textContent = group;
        frag.appendChild(lbl);
        lastGroup = group;
      }
      const chip = document.createElement('button');
      chip.type = 'button';
      chip.role = 'option';
      chip.className = 'asset-chip' + (sym === hidden.value ? ' active' : '');
      chip.dataset.sym = sym;
      chip.title = sym;
      chip.textContent = fmtLabel(sym);
      chip.addEventListener('click', () => {
        hidden.value = sym;
        grid.querySelectorAll('.asset-chip.active').forEach((c) => c.classList.remove('active'));
        chip.classList.add('active');
        if (search) search.value = '';
        // Req 7: scroll active chip into view within the scrollable grid
        chip.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'nearest' });
        // Req 2: keep selected asset visible in the badge above the grid
        const selDisplay = $('man-selected-display');
        if (selDisplay) selDisplay.textContent = fmtLabel(sym);
      });
      frag.appendChild(chip);
    }
    grid.innerHTML = '';
    if (shown.length === 0) grid.innerHTML = '<div class="asset-empty">No matches — try a different query</div>';
    else grid.appendChild(frag);
  }

  function rebuild() {
    buildList();
    render(search ? search.value : '');
  }

  buildList();
  render('');

  // Req 7 + 8: initialise the selected-asset badge and scroll active chip into view
  const selDisplay = $('man-selected-display');
  if (selDisplay) selDisplay.textContent = fmtLabel(hidden.value || '—');
  setTimeout(() => {
    const activeChip = grid.querySelector('.asset-chip.active');
    if (activeChip) activeChip.scrollIntoView({ block: 'nearest', inline: 'nearest' });
  }, 120);

  if (search) search.addEventListener('input', () => render(search.value));

  // Live Asset Availability: whenever a live refresh completes anywhere in
  // the app, re-render this grid so it reflects the latest availability
  // without needing a page reload.
  onLiveOtcAssetsChanged(rebuild);

  return rebuild;
}

function selectManualAsset(sym) {
  const hidden = $('man-asset');
  hidden.value = sym;
  document.querySelectorAll('#man-asset-grid .asset-chip').forEach((c) => {
    const isActive = c.dataset.sym === sym;
    c.classList.toggle('active', isActive);
    if (isActive) c.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'nearest' });
  });
  // Req 2: keep selected asset visible in the badge
  const selDisplay = $('man-selected-display');
  if (selDisplay) selDisplay.textContent = fmtLabel(sym);
}

let _manLoading = false;

async function runManualAnalysis() {
  // Guard against duplicate clicks/taps while a request is already in progress.
  if (_manLoading || $('man-analyze-btn').disabled) return;
  _manLoading = true;

  const btn = $('man-analyze-btn');
  const errorBox = $('man-error-box');
  const loadingSlot = $('man-loading-slot');
  const asset = $('man-asset').value;
  const timeframe = $('man-timeframe').value;
  const expiry = $('man-expiry').value;

  // Step 5 — Live Asset Availability preflight. Only applies to OTC
  // symbols (the live-availability system's scope); non-OTC "LIVE" assets
  // are unaffected. Refreshes live availability first (never trusts a
  // possibly-stale previous check), then verifies the selected asset is
  // actually in that live snapshot BEFORE any candle fetch — never lets
  // an unavailable-asset condition surface as a raw JS/fetch error.
  if (asset && asset.toLowerCase().endsWith('_otc')) {
    $('man-placeholder').hidden = true;
    $('man-result-slot').innerHTML = '';
    errorBox.hidden = true;

    const state = await refreshLiveOtcAssets(true);
    if (!state.success) {
      // SESSION_INVALID / discovery error — distinct from "asset unavailable"
      errorBox.textContent = `Live asset availability unavailable — ${state.error}`;
      errorBox.hidden = false;
      _manLoading = false;
      return;
    }
    if (!state.assets.includes(asset)) {
      // ASSET_UNAVAILABLE — the asset is currently closed/not offered by
      // Quotex; stop before requesting candles, never fabricate a result.
      errorBox.textContent = `${fmtLabel(asset)} is not currently available on Quotex. It may be closed or temporarily unlisted.`;
      errorBox.hidden = false;
      _manLoading = false;
      return;
    }
  }

  // Req 4: hide placeholder immediately (don't wait for the response)
  $('man-placeholder').hidden = true;
  // Clear any previous result so the slot is empty while loading
  $('man-result-slot').innerHTML = '';
  errorBox.hidden = true;

  // Req 3: show loading skeleton with the asset being analyzed
  if (loadingSlot) {
    const loadAssetEl = $('man-load-asset');
    if (loadAssetEl) loadAssetEl.textContent = fmtLabel(asset || '—');
    loadingSlot.hidden = false;
    // Req 6: scroll loading card into view immediately
    loadingSlot.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  btn.disabled = true;
  btn.setAttribute('aria-busy', 'true');
  btn.querySelector('.btn-spinner').hidden = false;
  btn.querySelector('.btn-label').textContent = 'Analyzing…';

  // Pause the Auto Scanner (if it's running) so only one /api/signal request
  // is ever in flight at a time. The single-flight lock in enqueueSignalRequest
  // also waits for any scan request already in progress before this one starts.
  const scannerWasActive = !!_scanTimer;
  _scanPaused = true;
  if (scannerWasActive) setStatus('paused', 'Paused — Manual Analysis');

  let res;
  try {
    res = await enqueueSignalRequest(asset, timeframe, MANUAL_REQUEST_TIMEOUT_MS);
  } finally {
    // Re-enable the Auto Scanner only after the manual request completes or fails.
    _scanPaused = false;
    if (scannerWasActive) setStatus(_scanning ? 'scanning' : 'live', _scanning ? 'Scanning' : 'Live');

    btn.disabled = false;
    btn.removeAttribute('aria-busy');
    btn.querySelector('.btn-spinner').hidden = true;
    btn.querySelector('.btn-label').textContent = 'Analyze';
    _manLoading = false;
    // Req 3: always hide loading skeleton when request settles
    if (loadingSlot) loadingSlot.hidden = true;
  }

  if (!res.ok) {
    errorBox.textContent = res.error;
    errorBox.hidden = false;
    errorBox.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    return;
  }

  saveToHistory(res.data);
  if ((res.data.confluence || {}).signal !== 'WAIT') pushNotification(res.data);
  try {
    renderManualResult(res.data, expiry);
  } catch (err) {
    console.error('[SignalPro] renderManualResult error:', err);
    errorBox.textContent = `Result display error: ${err.message}`;
    errorBox.hidden = false;
    errorBox.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }
}

function renderManualResult(data, expiry) {
  const card = buildSignalCard();
  fillSignalCard(card, data, { expiry: expiry || fmtExpiryFromTf(data.timeframe) });
  const slot = $('man-result-slot');
  slot.innerHTML = '';
  slot.appendChild(card);
  $('man-placeholder').hidden = true;
  card.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

$('man-analyze-btn').addEventListener('click', runManualAnalysis);

/* ────────────────────────────────────────────────────────────────────────────
 * Signals & Markets page — live prices via existing /api/live-prices
 * ──────────────────────────────────────────────────────────────────────────── */

async function loadMarkets() {
  const list = $('markets-list');
  if (!list || _activeView !== 'signals') return;
  list.innerHTML = '<div class="tab-loading">Fetching prices…</div>';
  try {
    const res = await fetch('/api/live-prices');
    const data = await res.json();
    if (data.error) { list.innerHTML = `<div class="tab-error">⚠ ${data.error}</div>`; return; }
    const prices = data.prices || {};
    const syms = Object.keys(prices);
    if (syms.length === 0) { list.innerHTML = '<div class="tab-empty">No price data available.</div>'; return; }

    list.innerHTML = syms.map((sym) => {
      const { price, change_pct: chg } = prices[sym];
      const cls = chg > 0 ? 'up' : chg < 0 ? 'down' : 'flat';
      const sgn = chg > 0 ? '+' : '';
      return `
        <div class="market-row" data-sym="${sym}" role="button" tabindex="0">
          <div class="market-row-left">
            <span class="market-sym">${fmtLabel(sym)}</span>
            <span class="market-sym-raw">${sym}</span>
          </div>
          <div class="market-row-right">
            <span class="market-price">${price}</span>
            <span class="market-chg ${cls}">${sgn}${chg}%</span>
          </div>
        </div>`;
    }).join('');

    list.querySelectorAll('.market-row').forEach((row) => {
      const go = () => { selectManualAsset(row.dataset.sym); navigateTo('analyzer'); };
      row.addEventListener('click', go);
      row.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') go(); });
    });
  } catch (err) {
    list.innerHTML = `<div class="tab-error">⚠ ${err.message}</div>`;
  }
}

$('refresh-markets-btn').addEventListener('click', loadMarkets);

/* ────────────────────────────────────────────────────────────────────────────
 * History page (localStorage — same key as the legacy dashboard)
 * ──────────────────────────────────────────────────────────────────────────── */

const HIST_KEY = 'signal_deck_history_v1';

function saveToHistory(data) {
  if (!data || data.error) return;
  const entry = {
    ts: new Date().toISOString(),
    asset: data.asset,
    timeframe: data.timeframe,
    signal: (data.confluence || {}).signal || '—',
    confidence: (data.confluence || {}).confidence || 0,
    payout: data.payout_pct || null,
  };
  let hist = [];
  try { hist = JSON.parse(localStorage.getItem(HIST_KEY) || '[]'); } catch (_) {}
  hist.unshift(entry);
  if (hist.length > 150) hist = hist.slice(0, 150);
  try { localStorage.setItem(HIST_KEY, JSON.stringify(hist)); } catch (_) {}
}

function renderHistory() {
  const list = $('history-list');
  if (!list) return;
  let hist = [];
  try { hist = JSON.parse(localStorage.getItem(HIST_KEY) || '[]'); } catch (_) {}
  if (hist.length === 0) { list.innerHTML = '<div class="tab-empty">No signals generated yet this session.</div>'; return; }
  list.innerHTML = hist.map((e) => {
    const dt = new Date(e.ts);
    const ts = dt.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', hour12: true });
    const ds = dt.toLocaleDateString('en-IN', { day: '2-digit', month: 'short' });
    const sc = (e.signal || '').toLowerCase();
    const payout = e.payout ? ` · ${e.payout}% payout` : '';
    return `
      <div class="history-row">
        <div class="history-left">
          <span class="history-asset">${fmtLabel(e.asset)}</span>
          <span class="history-meta">${tfLabel(e.timeframe)}${payout}</span>
        </div>
        <div class="history-right">
          <span class="history-signal ${sc}">${e.signal}</span>
          <span class="history-conf">${e.confidence}%</span>
          <span class="history-time">${ts} · ${ds}</span>
        </div>
      </div>`;
  }).join('');
}

$('clear-history-btn').addEventListener('click', () => {
  try { localStorage.removeItem(HIST_KEY); } catch (_) {}
  renderHistory();
});

/* ────────────────────────────────────────────────────────────────────────────
 * PHASE 7.4 — SETTINGS PAGE
 * Talks only to /api/settings*, already implemented and regression-tested.
 * ──────────────────────────────────────────────────────────────────────────── */

let _settingsCache = null;
let _settingsLoaded = false;

function setSettingsSaveState(text, cls) {
  const el = $('settings-save-state');
  if (!el) return;
  el.textContent = text;
  el.className = 'settings-save-state' + (cls ? ' ' + cls : '');
}

function settingsFieldRow(label, inputHtml) {
  return `<div class="settings-field-row"><span class="field-label">${label}</span>${inputHtml}</div>`;
}

function renderSettingsForm(settings) {
  const slot = $('settings-form-slot');
  if (!slot) return;
  const g = settings.general, f = settings.filters, sc = settings.scanner;

  slot.innerHTML = `
    <div class="settings-field-group">
      <div class="settings-field-group-title">General</div>
      ${settingsFieldRow('Default Timeframe', `<select class="field-select" data-path="general.default_timeframe">
        ${(window._TIMEFRAMES || []).map((tf) => `<option value="${tf}" ${tf === g.default_timeframe ? 'selected' : ''}>${tf}</option>`).join('')}
      </select>`)}
      ${settingsFieldRow('Refresh Interval (s)', `<input class="field-input" type="number" min="5" max="600" data-path="general.refresh_interval_seconds" value="${g.refresh_interval_seconds}">`)}
      ${settingsFieldRow('Scanner Speed', `<select class="field-select" data-path="general.scanner_speed">
        ${['slow', 'normal', 'fast'].map((v) => `<option value="${v}" ${v === g.scanner_speed ? 'selected' : ''}>${v}</option>`).join('')}
      </select>`)}
      ${settingsFieldRow('Auto Refresh', `<button type="button" class="switch settings-switch" data-path="general.auto_refresh" aria-checked="${!!g.auto_refresh}"><span class="switch-knob"></span></button>`)}
    </div>

    <div class="settings-field-group">
      <div class="settings-field-group-title">Filters</div>
      ${settingsFieldRow('ADX Threshold', `<input class="field-input" type="number" step="0.5" data-path="filters.adx_threshold" value="${f.adx_threshold}">`)}
      ${settingsFieldRow('ATR Threshold (%)', `<input class="field-input" type="number" step="0.1" data-path="filters.atr_threshold" value="${f.atr_threshold}">`)}
      ${settingsFieldRow('Min Payout (%)', `<input class="field-input" type="number" step="1" data-path="filters.min_payout" value="${f.min_payout}">`)}
      ${settingsFieldRow('Min Confidence (%)', `<input class="field-input" type="number" step="1" data-path="filters.min_confidence" value="${f.min_confidence}">`)}
      ${settingsFieldRow('Min Filter Score', `<input class="field-input" type="number" step="1" data-path="filters.min_filter_score" value="${f.min_filter_score}">`)}
    </div>

    <div class="settings-field-group">
      <div class="settings-field-group-title">Scanner</div>
      ${settingsFieldRow('Refresh (s)', `<input class="field-input" type="number" min="10" data-path="scanner.refresh_seconds" value="${sc.refresh_seconds}">`)}
      ${settingsFieldRow('Top N Results', `<input class="field-input" type="number" min="1" max="50" data-path="scanner.top_n" value="${sc.top_n}">`)}
      ${settingsFieldRow('Min Confidence (%)', `<input class="field-input" type="number" step="1" data-path="scanner.min_confidence" value="${sc.min_confidence}">`)}
    </div>

    <button class="generate-btn" id="settings-save-btn" type="button" style="margin-top:18px;"><span class="btn-label">Save Settings</span></button>
  `;

  slot.querySelectorAll('.settings-switch').forEach((btn) => {
    btn.addEventListener('click', () => {
      const on = btn.getAttribute('aria-checked') === 'true';
      btn.setAttribute('aria-checked', String(!on));
    });
  });

  $('settings-save-btn').addEventListener('click', saveSettingsForm);
}

function readSettingsFormPatch() {
  const slot = $('settings-form-slot');
  const patch = {};
  slot.querySelectorAll('[data-path]').forEach((el) => {
    const [section, key] = el.dataset.path.split('.');
    if (!patch[section]) patch[section] = {};
    let val;
    if (el.classList.contains('switch')) {
      val = el.getAttribute('aria-checked') === 'true';
    } else if (el.tagName === 'SELECT') {
      val = el.value;
    } else {
      val = el.value === '' ? null : Number(el.value);
    }
    patch[section][key] = val;
  });
  return patch;
}

async function saveSettingsForm() {
  const btn = $('settings-save-btn');
  const patch = readSettingsFormPatch();
  setSettingsSaveState('Saving…', '');
  if (btn) btn.disabled = true;
  try {
    const res = await fetch('/api/settings', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(patch),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Save failed');
    _settingsCache = data;
    setSettingsSaveState('✓ Saved', 'saved');
  } catch (e) {
    setSettingsSaveState('Save failed', 'error');
    showSettingsError(e.message);
  } finally {
    if (btn) btn.disabled = false;
  }
}

function showSettingsError(msg) {
  const box = $('settings-error-box');
  if (!box) return;
  box.textContent = msg;
  box.hidden = false;
  setTimeout(() => { box.hidden = true; }, 6000);
}

async function loadSettings() {
  const res = await fetch('/api/settings');
  const data = await res.json();
  _settingsCache = data;
  renderSettingsForm(data);
  return data;
}

async function loadSettingsBackups() {
  const list = $('settings-backups-list');
  if (!list) return;
  try {
    const res = await fetch('/api/settings/backups');
    const data = await res.json();
    const backups = data.backups || [];
    if (backups.length === 0) { list.innerHTML = '<div class="tab-empty small">No backups yet.</div>'; return; }
    list.innerHTML = backups.slice().reverse().map((b) => `
      <div class="settings-backup-row">
        <div class="settings-backup-meta">
          <span class="settings-backup-reason">${b.reason}</span>
          <span class="settings-backup-time">${b.timestamp}</span>
        </div>
        <button class="settings-backup-restore-btn" data-index="${b.index}" type="button">Restore</button>
      </div>`).join('');
    list.querySelectorAll('.settings-backup-restore-btn').forEach((btn) => {
      btn.addEventListener('click', async () => {
        if (!confirm('Restore this backup? Current settings will be overwritten (a fresh backup is not auto-created).')) return;
        try {
          const res = await fetch('/api/settings/backups/restore', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ index: Number(btn.dataset.index) }),
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || 'Restore failed');
          _settingsCache = data;
          renderSettingsForm(data);
          setSettingsSaveState('✓ Restored', 'saved');
        } catch (e) { showSettingsError(e.message); }
      });
    });
  } catch (e) { list.innerHTML = '<div class="tab-empty small">Failed to load backups.</div>'; }
}

function initSettingsPage() {
  loadSettings();
  loadSettingsBackups();

  const backupBtn = $('settings-backup-now-btn');
  if (backupBtn && !backupBtn._wired) {
    backupBtn._wired = true;
    backupBtn.addEventListener('click', async () => {
      try {
        await fetch('/api/settings/backup', {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ reason: 'manual' }),
        });
        loadSettingsBackups();
      } catch (e) { showSettingsError('Backup failed: ' + e.message); }
    });
  }

  const exportBtn = $('settings-export-btn');
  if (exportBtn && !exportBtn._wired) {
    exportBtn._wired = true;
    exportBtn.addEventListener('click', async () => {
      const res = await fetch('/api/settings/export');
      const data = await res.json();
      const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'signalpro-settings.json';
      a.click();
      URL.revokeObjectURL(a.href);
    });
  }

  const importInput = $('settings-import-file');
  if (importInput && !importInput._wired) {
    importInput._wired = true;
    importInput.addEventListener('change', async () => {
      const file = importInput.files[0];
      if (!file) return;
      try {
        const text = await file.text();
        const parsed = JSON.parse(text);
        const res = await fetch('/api/settings/import', {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(parsed),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Import failed');
        _settingsCache = data;
        renderSettingsForm(data);
        setSettingsSaveState('✓ Imported', 'saved');
      } catch (e) { showSettingsError('Import failed: ' + e.message); }
      importInput.value = '';
    });
  }

  const resetBtn = $('settings-reset-btn');
  if (resetBtn && !resetBtn._wired) {
    resetBtn._wired = true;
    resetBtn.addEventListener('click', async () => {
      if (!confirm('Reset ALL settings to their shipped defaults? This does not affect DEFAULT_CONFLUENCE_WEIGHTS or _DEFAULT_8F_WEIGHTS in analyzer.py/backtest.py, only your Settings overrides.')) return;
      try {
        const res = await fetch('/api/settings/reset', { method: 'POST' });
        const data = await res.json();
        _settingsCache = data;
        renderSettingsForm(data);
        setSettingsSaveState('✓ Reset to defaults', 'saved');
      } catch (e) { showSettingsError('Reset failed: ' + e.message); }
    });
  }
}

/* ────────────────────────────────────────────────────────────────────────────
 * PHASE 7.4 — BACKTEST PAGE
 * Talks only to /api/backtest*, already implemented and regression-tested.
 * ──────────────────────────────────────────────────────────────────────────── */

let _btSelectedAssets = [];
let _btPollTimer = null;
let _btInited = false;

function btBuildAssetGrid() {
  const grid = $('bt-asset-grid');
  if (!grid) return;
  const all = window._OTC_ASSETS || [];
  grid.innerHTML = all.map((sym) => `<button type="button" class="asset-chip" data-sym="${sym}">${fmtLabel(sym)}</button>`).join('');
  grid.querySelectorAll('.asset-chip').forEach((chip) => {
    chip.addEventListener('click', () => {
      const sym = chip.dataset.sym;
      const idx = _btSelectedAssets.indexOf(sym);
      if (idx === -1) { _btSelectedAssets.push(sym); chip.classList.add('active'); }
      else { _btSelectedAssets.splice(idx, 1); chip.classList.remove('active'); }
    });
  });
}

function btShowError(msg) {
  const box = $('bt-error-box');
  if (!box) return;
  box.textContent = msg;
  box.hidden = false;
  setTimeout(() => { box.hidden = true; }, 6000);
}

function fmtSeconds(s) {
  if (s === null || s === undefined) return '—';
  s = Math.round(s);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60), rem = s % 60;
  return `${m}m ${rem}s`;
}

function btRenderStatus(status) {
  const card = $('bt-progress-card');
  if (!card) return;
  const running = status.running || status.paused;
  card.hidden = !(running || status.completed || status.cancelled || (status.percent_complete > 0));

  $('bt-progress-state').textContent = status.state;
  $('bt-progress-pct').textContent = `${status.percent_complete}%`;
  $('bt-progress-bar-fill').style.width = `${status.percent_complete}%`;
  $('bt-current-asset').textContent = status.current_asset ? fmtLabel(status.current_asset) : '—';
  $('bt-current-indicator').textContent = status.current_indicator || '—';
  $('bt-elapsed').textContent = fmtSeconds(status.elapsed_time);
  $('bt-eta').textContent = fmtSeconds(status.estimated_remaining);

  $('bt-run-btn').hidden = running;
  $('bt-pause-btn').hidden = !status.running;
  $('bt-resume-btn').hidden = !status.paused;
  $('bt-stop-btn').hidden = !running;

  if (status.completed || status.cancelled) {
    stopBacktestPolling();
    loadBacktestResults();
  }
}

function btRenderResults(payload) {
  const slot = $('bt-results-slot');
  if (!slot) return;
  const summary = payload.summary;
  if (!summary || summary.assets_backtested === 0) {
    slot.innerHTML = '<div class="tab-empty">Run a backtest to see suggested weights and per-asset accuracy.</div>';
    return;
  }
  const rows = Object.entries(summary.suggested_weights).map(([k, w]) => {
    const sample = summary.min_sample_sizes[k];
    return `<tr><td>${k}</td><td>${w.toFixed(2)}</td><td>${sample}</td></tr>`;
  }).join('');

  slot.innerHTML = `
    <div class="bt-results-summary">
      <span>Assets backtested: <b>${summary.assets_backtested}</b></span>
      <span>Assets failed: <b>${summary.assets_failed}</b></span>
      <span>Candle target: <b>${summary.candle_count_target}</b></span>
      <span>All met candle count: <b>${summary.all_assets_met_candle_count ? 'Yes' : 'No'}</b></span>
    </div>
    <table class="bt-suggested-table">
      <thead><tr><th>Factor</th><th>Suggested Weight</th><th>Min Sample Size</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
    <button class="generate-btn bt-apply-weights-btn" id="bt-apply-weights-btn" type="button"><span class="btn-label">Apply Suggested Weights</span></button>
    <p id="bt-apply-reasons" class="page-intro small" style="margin-top:8px;"></p>
  `;

  $('bt-apply-weights-btn').addEventListener('click', async () => {
    const btn = $('bt-apply-weights-btn');
    const reasonsEl = $('bt-apply-reasons');
    btn.disabled = true;
    reasonsEl.textContent = '';
    try {
      const res = await fetch('/api/backtest/apply-weights', { method: 'POST' });
      const data = await res.json();
      if (!res.ok || !data.ok) {
        reasonsEl.textContent = '⚠ Cannot apply: ' + (data.reasons || [data.error || 'unknown reason']).join('; ');
      } else {
        reasonsEl.textContent = '✓ Applied — settings.json updated (a backup was created automatically).';
        if (_settingsCache) { _settingsCache = data.settings; }
      }
    } catch (e) {
      reasonsEl.textContent = 'Apply failed: ' + e.message;
    } finally {
      btn.disabled = false;
    }
  });
}

async function loadBacktestResults() {
  try {
    const res = await fetch('/api/backtest/results');
    const data = await res.json();
    btRenderResults(data);
  } catch (e) { /* results panel stays as-is on transient failure */ }
}

async function pollBacktestStatus() {
  try {
    const res = await fetch('/api/backtest/status');
    const data = await res.json();
    btRenderStatus(data);
  } catch (e) { /* keep polling — a single failed poll shouldn't stop it */ }
}

function startBacktestPolling() {
  stopBacktestPolling();
  pollBacktestStatus();
  _btPollTimer = setInterval(pollBacktestStatus, 2000);
}
function stopBacktestPolling() {
  clearInterval(_btPollTimer);
  _btPollTimer = null;
}

function initBacktestPage() {
  if (_btInited) { pollBacktestStatus(); return; }
  _btInited = true;
  btBuildAssetGrid();

  $('bt-mode-all').addEventListener('click', () => {
    $('bt-mode-all').classList.add('active');
    $('bt-mode-custom').classList.remove('active');
    $('bt-asset-grid').hidden = true;
  });
  $('bt-mode-custom').addEventListener('click', () => {
    $('bt-mode-custom').classList.add('active');
    $('bt-mode-all').classList.remove('active');
    $('bt-asset-grid').hidden = false;
  });

  $('bt-run-btn').addEventListener('click', async () => {
    const timeframe = $('bt-timeframe').value;
    const candle_count = Number($('bt-candles').value);
    const useAll = $('bt-mode-all').classList.contains('active');
    const assets = useAll ? (window._OTC_ASSETS || []) : _btSelectedAssets;
    if (!useAll && assets.length === 0) { btShowError('Choose at least one asset, or switch to "All OTC".'); return; }
    try {
      const res = await fetch('/api/backtest/run', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ assets, timeframe, candle_count }),
      });
      const data = await res.json();
      if (!res.ok || !data.ok) throw new Error(data.message || data.error || 'Failed to start backtest');
      startBacktestPolling();
    } catch (e) { btShowError(e.message); }
  });

  $('bt-pause-btn').addEventListener('click', async () => { await fetch('/api/backtest/pause', { method: 'POST' }); pollBacktestStatus(); });
  $('bt-resume-btn').addEventListener('click', async () => { await fetch('/api/backtest/resume', { method: 'POST' }); pollBacktestStatus(); });
  $('bt-stop-btn').addEventListener('click', async () => {
    if (!confirm('Stop the running backtest?')) return;
    await fetch('/api/backtest/stop', { method: 'POST' });
    stopBacktestPolling();
    pollBacktestStatus();
  });

  pollBacktestStatus();
  loadBacktestResults();
}

/* ────────────────────────────────────────────────────────────────────────────
 * PHASE 7.4 — INDICATORS PAGE
 * Talks only to GET /api/indicators + PATCH-via-POST /api/settings.
 * ──────────────────────────────────────────────────────────────────────────── */

async function loadIndicatorsPage() {
  const slot = $('indicators-list-slot');
  if (!slot) return;
  slot.innerHTML = '<div class="tab-loading">Loading indicator registry…</div>';
  try {
    const res = await fetch('/api/indicators');
    const data = await res.json();
    renderIndicatorsList(data.indicators || []);
  } catch (e) {
    slot.innerHTML = '<div class="tab-empty">Failed to load indicators.</div>';
  }
}

function renderIndicatorsList(indicators) {
  const slot = $('indicators-list-slot');
  slot.innerHTML = indicators.map((ind) => {
    const disconnected = !ind.in_confluence;
    const acc = ind.accuracy !== null && ind.accuracy !== undefined ? `${(ind.accuracy * 100).toFixed(1)}%` : '—';
    const dw = ind.dynamic_weight !== null && ind.dynamic_weight !== undefined ? ind.dynamic_weight.toFixed(2) : '—';
    const sample = ind.sample_size !== null && ind.sample_size !== undefined ? ind.sample_size : '—';
    return `
      <div class="indicator-row ${disconnected ? 'disconnected' : ''}">
        <div class="indicator-row-top">
          <div class="indicator-row-name">
            <b>${ind.name}</b>
            <span class="indicator-row-group">${ind.group}${disconnected ? ' · not connected' : ''}</span>
          </div>
          <button type="button" class="switch indicator-toggle" data-id="${ind.id}"
                  aria-checked="${!!ind.enabled}" ${disconnected ? 'disabled title="Not yet wired into the confluence vote — toggling this has no effect on signals yet"' : ''}>
            <span class="switch-knob"></span>
          </button>
        </div>
        <div class="indicator-row-meta">
          <span>Weight: <b>${ind.weight}</b></span>
          <span>Dynamic Weight: <b>${dw}</b></span>
          <span>Accuracy: <b>${acc}</b></span>
          <span>Samples: <b>${sample}</b></span>
        </div>
        ${disconnected ? '<div class="indicator-not-connected-note">Registered, not yet connected to the Confluence vote this phase — enabling/disabling it has no effect on live signals.</div>' : ''}
      </div>`;
  }).join('');

  slot.querySelectorAll('.indicator-toggle:not([disabled])').forEach((btn) => {
    btn.addEventListener('click', async () => {
      const id = btn.dataset.id;
      const newEnabled = btn.getAttribute('aria-checked') !== 'true';
      btn.setAttribute('aria-checked', String(newEnabled));
      try {
        await fetch('/api/settings', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ indicators: { [id]: { enabled: newEnabled } } }),
        });
        loadIndicatorsPage();
      } catch (e) {
        btn.setAttribute('aria-checked', String(!newEnabled)); // revert on failure
      }
    });
  });
}

/* ────────────────────────────────────────────────────────────────────────────
 * PHASE 7.4 — QUOTEX SESSION PAGE
 * Talks only to /api/session*, already implemented and regression-tested.
 * ──────────────────────────────────────────────────────────────────────────── */

async function loadSessionPage() {
  const card = $('session-status-card');
  if (!card) return;
  card.innerHTML = '<div class="tab-loading">Loading session status…</div>';
  try {
    const res = await fetch('/api/session/status');
    const s = await res.json();
    card.innerHTML = `
      <div class="session-status-row"><span class="k">Active Source</span><span class="v">${s.active_source}</span></div>
      <div class="session-status-row"><span class="k">Session File</span><span class="v ${s.session_file_ssid_present ? 'ok' : ''}">${s.session_file_ssid_present ? 'SSID present' : 'No SSID saved'}</span></div>
      <div class="session-status-row"><span class="k">Env Var Override</span><span class="v">${s.env_ssid_set ? 'Set (takes priority)' : 'Not set'}</span></div>
      <div class="session-status-row"><span class="k">Fetcher</span><span class="v ${s.fetcher_connected ? 'ok' : 'bad'}">${s.fetcher_connected ? 'Connected' : 'Not connected'}</span></div>
      <div class="session-status-row"><span class="k">Last Validated</span><span class="v">${s.last_validated_at || 'Never'}</span></div>
      <div class="session-status-row"><span class="k">Last Status</span><span class="v ${s.last_validation_status === 'valid' ? 'ok' : s.last_validation_status === 'invalid' ? 'bad' : ''}">${s.last_validation_status}</span></div>
    `;
  } catch (e) {
    card.innerHTML = '<div class="tab-empty">Failed to load session status.</div>';
  }

  const saveBtn = $('session-save-btn');
  if (saveBtn && !saveBtn._wired) {
    saveBtn._wired = true;
    saveBtn.addEventListener('click', async () => {
      const ssid = $('session-ssid-input').value.trim();
      const box = $('session-error-box');
      box.hidden = true;
      if (!ssid) { box.textContent = 'Paste an SSID first.'; box.hidden = false; return; }
      saveBtn.disabled = true;
      try {
        const res = await fetch('/api/session/update', {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ssid }),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Save failed');
        $('session-ssid-input').value = '';
        loadSessionPage();
        // Step 7 — a new SSID is a new session; refresh live availability.
        refreshLiveOtcAssets(true);
      } catch (e) {
        box.textContent = e.message;
        box.hidden = false;
      } finally {
        saveBtn.disabled = false;
      }
    });
  }

  const validateBtn = $('session-validate-btn');
  if (validateBtn && !validateBtn._wired) {
    validateBtn._wired = true;
    validateBtn.addEventListener('click', async () => {
      const resultBox = $('session-validate-result');
      validateBtn.disabled = true;
      resultBox.hidden = false;
      resultBox.className = 'glass-card session-validate-result';
      resultBox.innerHTML = '<div class="tab-loading">Validating…</div>';
      try {
        const res = await fetch('/api/session/validate', { method: 'POST' });
        const data = await res.json();
        resultBox.classList.add(data.ok ? 'ok' : 'bad');
        resultBox.innerHTML = `
          <div class="session-validate-row"><span class="dot ${data.connected_to_server ? 'ok' : 'bad'}"></span>Connected to Server</div>
          <div class="session-validate-row"><span class="dot ${data.connected_to_gateway ? 'ok' : 'bad'}"></span>Connected to Gateway</div>
          <div class="session-validate-row"><span class="dot ${data.session_valid ? 'ok' : 'bad'}"></span>Session Valid</div>
          <p class="page-intro small" style="margin-top:8px;">${data.message || data.error || ''}</p>
        `;
      } catch (e) {
        resultBox.className = 'glass-card session-validate-result bad';
        resultBox.innerHTML = `<div class="session-validate-row"><span class="dot bad"></span>Validation request failed: ${e.message}</div>`;
      } finally {
        validateBtn.disabled = false;
        loadSessionPage();
        // Step 7 — refresh live asset availability after session validate,
        // since a session transition (connected/disconnected) directly
        // changes what Quotex will report as available.
        refreshLiveOtcAssets(true);
      }
    });
  }
}

/* ────────────────────────────────────────────────────────────────────────────
 * PHASE 8.2 — SMART SCANNER PAGE (backend ScannerEngine, /api/scanner/*)
 * Distinct from the "Auto Scanner" tab (view-scanner / _autoScanEnabled
 * above), which is a separate, older client-side polling loop hitting
 * /api/signal directly per asset and has never talked to ScannerEngine —
 * see HANDOFF.md's "single most important gotcha". This page creates NO
 * new scanner system: it only calls the existing, already-tested
 * /api/scanner/status|results|start|stop|pause|resume routes.
 * ──────────────────────────────────────────────────────────────────────────── */
let _ssPollTimer = null;
let _ssInited = false;
let _ssSelectedAssets = [];       // [] == "All OTC" (matches settings default semantics)
let _ssSelectedTimeframes = [];   // [] == "Default timeframes" (matches settings default semantics)

function ssShowError(msg) {
  const box = $('ss-error-box');
  if (!box) return;
  box.textContent = msg;
  box.hidden = false;
  setTimeout(() => { box.hidden = true; }, 6000);
}

function ssSetMsg(msg) {
  const box = $('ss-set-msg');
  if (!box) return;
  box.textContent = msg;
}

function fmtAge(iso) {
  if (!iso) return '—';
  const then = Date.parse(iso);
  if (Number.isNaN(then)) return '—';
  const secs = Math.max(0, Math.round((Date.now() - then) / 1000));
  return `${fmtSeconds(secs)} ago`;
}

function ssRenderStatus(status) {
  $('ss-state').textContent = status.state;
  $('ss-progress-pct').textContent = `${status.percent_complete ?? 0}%`;
  $('ss-progress-bar-fill').style.width = `${status.percent_complete ?? 0}%`;
  $('ss-current-asset').textContent = status.current_asset ? fmtLabel(status.current_asset) : '—';
  $('ss-current-tf').textContent = status.current_timeframe || '—';
  $('ss-assets-completed').textContent = `${status.assets_completed ?? 0} / ${status.total_assets ?? 0}`;
  $('ss-current-cycle').textContent = status.current_cycle ?? '—';
  $('ss-elapsed').textContent = fmtSeconds(status.elapsed_time);
  $('ss-eta').textContent = fmtSeconds(status.estimated_remaining);
  $('ss-last-scan').textContent = status.last_scan_time ? fmtAge(status.last_scan_time) : '—';

  // running == true covers RUNNING/PAUSED/YIELDING/DEGRADED/RECOVERING
  // (anything not STOPPED) per get_status()'s existing "running" field.
  $('ss-start-btn').hidden = status.running;
  $('ss-pause-btn').hidden = !status.running || status.paused;
  $('ss-resume-btn').hidden = !status.paused;
  $('ss-stop-btn').hidden = !status.running;
}

function ssRenderResults(payload) {
  const slot = $('ss-results-slot');
  if (!slot) return;
  const rows = payload.top_signals || [];
  if (rows.length === 0) {
    slot.innerHTML = '<div class="tab-empty">No qualifying signals yet — the scanner is still working through the asset list, or none currently pass the mandatory filters.</div>';
    return;
  }
  // Phase 8.3: ranking/sorting itself happens server-side in
  // scanner.py's get_results() (filter_score desc, confidence desc,
  // payout desc, freshness desc) — this just renders the already-ranked
  // list plus the rank/payout/age columns the spec asked for.
  const body = rows.map((r) => {
    const conf = r.confluence || {};
    const signal = conf.signal || '—';
    const confidence = (conf.confidence === undefined || conf.confidence === null) ? '—' : `${Number(conf.confidence).toFixed(1)}%`;
    const filterScore = (r.filter_score === undefined || r.filter_score === null) ? '—' : Number(r.filter_score).toFixed(1);
    const passed = (r.passed_filters || []).join(', ') || '—';
    const failed = (r.failed_filters || []).join(', ') || '—';
    const payout = (r.payout_pct === undefined || r.payout_pct === null) ? '—' : `${Number(r.payout_pct).toFixed(0)}%`;
    const dirClass = signal === 'BUY' ? 'buy' : (signal === 'SELL' ? 'sell' : '');
    return `<tr>
      <td>${r.rank ?? '—'}</td>
      <td>${fmtLabel(r.asset || '')}</td>
      <td>${r.timeframe || '—'}</td>
      <td class="${dirClass}">${signal}</td>
      <td>${confidence}</td>
      <td>${filterScore}</td>
      <td>${passed}</td>
      <td>${failed}</td>
      <td>${payout}</td>
      <td>${fmtAge(r.last_update)}</td>
      <td>${r.last_update || '—'}</td>
    </tr>`;
  }).join('');
  slot.innerHTML = `
    <table class="ss-results-table">
      <thead><tr><th>Rank</th><th>Asset</th><th>TF</th><th>Direction</th><th>Confidence</th><th>Filter Score</th><th>Passed Filters</th><th>Failed Filters</th><th>Payout</th><th>Age</th><th>Last Update</th></tr></thead>
      <tbody>${body}</tbody>
    </table>`;
}

async function loadScannerResults() {
  try {
    const res = await fetch('/api/scanner/results');
    const data = await res.json();
    ssRenderResults(data);
  } catch (e) { /* keep whatever results are already shown on a transient failure */ }
}

/* ── M1: Full Scanner Diagnostics ─────────────────────────────────────────
 * Read-only view of /api/scanner/diagnostics — EVERY configured asset/
 * timeframe, not just the ones Ranked Signals surfaces, each labeled
 * SIGNAL/WAIT/FAILED/SKIPPED with the actual reason. No signal/gate logic
 * lives here; this only renders what the backend already computed. */
let _ssDiagVisible = false;

function ssRenderDiagnostics(payload) {
  const slot = $('ss-diag-slot');
  if (!slot) return;
  const entries = payload.entries || [];
  const counts = payload.counts || {};
  $('ss-diag-count-signal').textContent = counts.signal ?? 0;
  $('ss-diag-count-wait').textContent = counts.wait ?? 0;
  $('ss-diag-count-failed').textContent = counts.failed ?? 0;
  $('ss-diag-count-skipped').textContent = counts.skipped ?? 0;

  if (entries.length === 0) {
    slot.innerHTML = '<div class="tab-empty">No diagnostics yet — start the scanner to populate this view.</div>';
    return;
  }

  const statusClass = { SIGNAL: 'buy', WAIT: '', FAILED: 'sell', SKIPPED: '' };
  const body = entries.map((e) => {
    const confidence = (e.confidence === undefined || e.confidence === null) ? '—' : `${Number(e.confidence).toFixed(1)}%`;
    const filterScore = (e.filter_score === undefined || e.filter_score === null) ? '—' : Number(e.filter_score).toFixed(1);
    const failed = (e.failed_filters || []).join(', ') || '—';
    const candles = (e.candle_count === undefined || e.candle_count === null) ? '—' : e.candle_count;
    const reason = e.reason || '—';
    return `<tr>
      <td>${fmtLabel(e.asset || '')}</td>
      <td>${e.timeframe || '—'}</td>
      <td class="${statusClass[e.status] || ''}">${e.status}</td>
      <td>${candles}</td>
      <td>${e.signal || '—'}</td>
      <td>${confidence}</td>
      <td>${filterScore}</td>
      <td>${failed}</td>
      <td>${reason}</td>
      <td>${e.last_update ? fmtAge(e.last_update) : '—'}</td>
    </tr>`;
  }).join('');

  slot.innerHTML = `
    <table class="ss-results-table">
      <thead><tr><th>Asset</th><th>TF</th><th>Status</th><th>Candles</th><th>Signal</th><th>Confidence</th><th>Filter Score</th><th>Failed Filters</th><th>Reason</th><th>Age</th></tr></thead>
      <tbody>${body}</tbody>
    </table>`;
}

async function loadScannerDiagnostics() {
  const slot = $('ss-diag-slot');
  try {
    const res = await fetch('/api/scanner/diagnostics');
    const data = await res.json();
    ssRenderDiagnostics(data);
  } catch (e) {
    if (slot) slot.innerHTML = `<div class="tab-error">⚠ Failed to load diagnostics: ${e.message}</div>`;
  }
}

function ssInitDiagnostics() {
  const toggleBtn = $('ss-diag-toggle-btn');
  const refreshBtn = $('ss-diag-refresh-btn');
  const countsEl = $('ss-diag-counts');
  const slot = $('ss-diag-slot');
  if (!toggleBtn) return;

  toggleBtn.addEventListener('click', () => {
    _ssDiagVisible = !_ssDiagVisible;
    toggleBtn.textContent = _ssDiagVisible ? 'Hide Full Diagnostics' : 'Show Full Diagnostics';
    refreshBtn.hidden = !_ssDiagVisible;
    countsEl.hidden = !_ssDiagVisible;
    slot.hidden = !_ssDiagVisible;
    if (_ssDiagVisible) loadScannerDiagnostics();
  });

  refreshBtn.addEventListener('click', loadScannerDiagnostics);
}

// Phase 8.2: self-managing poll — a single call both refreshes the UI AND
// decides whether the interval should be running at all, based on the
// server's actual state. This means "only poll while running, stop
// polling when stopped" holds even if the scanner was stopped from a
// DIFFERENT tab/device, not just via this page's own Stop button.
async function pollScannerStatus() {
  try {
    const res = await fetch('/api/scanner/status');
    const data = await res.json();
    ssRenderStatus(data);
    if (data.running) {
      loadScannerResults();
      if (_ssDiagVisible) loadScannerDiagnostics();
      if (!_ssPollTimer) _ssPollTimer = setInterval(pollScannerStatus, 2000);
    } else if (_ssPollTimer) {
      clearInterval(_ssPollTimer);
      _ssPollTimer = null;
    }
  } catch (e) { /* a single failed poll shouldn't tear down an active interval */ }
}

function stopScannerPolling() {
  clearInterval(_ssPollTimer);
  _ssPollTimer = null;
}

/* ── Phase 8.3: Scanner Settings (load / save / reset / reload) ─────────── */

function ssBuildSettingsAssetGrid() {
  const grid = $('ss-set-asset-grid');
  if (!grid) return;
  // Live Asset Availability: this is a LIVE selector (feeds the Scanner's
  // custom asset subset) — it must only ever offer currently-available OTC
  // assets, never the static known universe. Falls back to the static list
  // only until the first live fetch resolves; rebuilt automatically after
  // every live refresh via the listener registered below.
  const all = _liveOtcState.loaded ? _liveOtcState.assets : (window._OTC_ASSETS || []);
  grid.innerHTML = all.length
    ? all.map((sym) => `<button type="button" class="asset-chip${_ssSelectedAssets.includes(sym) ? ' active' : ''}" data-sym="${sym}">${fmtLabel(sym)}</button>`).join('')
    : '<div class="asset-empty">No OTC assets currently available from Quotex.</div>';
  grid.querySelectorAll('.asset-chip').forEach((chip) => {
    chip.addEventListener('click', () => {
      const sym = chip.dataset.sym;
      const idx = _ssSelectedAssets.indexOf(sym);
      if (idx === -1) { _ssSelectedAssets.push(sym); chip.classList.add('active'); }
      else { _ssSelectedAssets.splice(idx, 1); chip.classList.remove('active'); }
    });
  });
}

function ssSetAssetMode(custom) {
  $('ss-set-asset-mode-all').classList.toggle('active', !custom);
  $('ss-set-asset-mode-custom').classList.toggle('active', custom);
  $('ss-set-asset-grid').hidden = !custom;
}

function ssSetTfMode(custom) {
  $('ss-set-tf-mode-default').classList.toggle('active', !custom);
  $('ss-set-tf-mode-custom').classList.toggle('active', custom);
  $('ss-set-tf-grid').hidden = !custom;
}

function ssApplySettingsToForm(scannerSettings) {
  const s = scannerSettings || {};
  const assets = s.enabled_assets || [];
  const tfs = s.enabled_timeframes || [];

  _ssSelectedAssets = [...assets];
  ssSetAssetMode(assets.length > 0);
  $('ss-set-asset-grid').querySelectorAll('.asset-chip').forEach((chip) => {
    chip.classList.toggle('active', _ssSelectedAssets.includes(chip.dataset.sym));
  });

  _ssSelectedTimeframes = [...tfs];
  ssSetTfMode(tfs.length > 0);
  $('ss-set-tf-grid').querySelectorAll('.chip-toggle').forEach((chip) => {
    chip.classList.toggle('active', _ssSelectedTimeframes.includes(chip.dataset.tf));
  });

  const minScore = s.minimum_filter_score ?? 0;
  $('ss-set-min-filter-score').value = minScore;
  $('ss-set-min-filter-score-val').textContent = minScore;

  $('ss-set-top-signals').value = (s.top_signals === null || s.top_signals === undefined) ? '' : String(s.top_signals);
  $('ss-set-scan-interval').value = (s.scan_interval === null || s.scan_interval === undefined) ? '' : String(s.scan_interval);

  const enabled = s.scanner_enabled !== false; // default true
  $('ss-set-enabled-toggle').setAttribute('aria-checked', String(enabled));
}

async function ssLoadSettings() {
  try {
    const res = await fetch('/api/settings');
    const data = await res.json();
    ssApplySettingsToForm(data.scanner);
    ssSetMsg('');
  } catch (e) {
    ssSetMsg('Failed to load settings: ' + e.message);
  }
}

function ssReadFormAsPatch() {
  const assetMode = $('ss-set-asset-mode-custom').classList.contains('active');
  const tfMode = $('ss-set-tf-mode-custom').classList.contains('active');
  const topSignalsRaw = $('ss-set-top-signals').value;
  const scanIntervalRaw = $('ss-set-scan-interval').value;
  return {
    scanner: {
      scanner_enabled: $('ss-set-enabled-toggle').getAttribute('aria-checked') === 'true',
      enabled_assets: assetMode ? _ssSelectedAssets : [],
      enabled_timeframes: tfMode ? _ssSelectedTimeframes : [],
      minimum_filter_score: Number($('ss-set-min-filter-score').value) || 0,
      top_signals: topSignalsRaw === '' ? null : Number(topSignalsRaw),
      scan_interval: scanIntervalRaw === '' ? null : Number(scanIntervalRaw),
    },
  };
}

function initSmartScannerPage() {
  if (_ssInited) {
    // Re-visiting the tab: just refresh from the current server state —
    // never re-triggers a start.
    pollScannerStatus();
    return;
  }
  _ssInited = true;

  ssBuildSettingsAssetGrid();
  ssInitDiagnostics();
  onLiveOtcAssetsChanged(ssBuildSettingsAssetGrid);
  refreshLiveOtcAssets();

  $('ss-set-asset-mode-all').addEventListener('click', () => ssSetAssetMode(false));
  $('ss-set-asset-mode-custom').addEventListener('click', () => ssSetAssetMode(true));
  $('ss-set-tf-mode-default').addEventListener('click', () => ssSetTfMode(false));
  $('ss-set-tf-mode-custom').addEventListener('click', () => ssSetTfMode(true));

  $('ss-set-tf-grid').querySelectorAll('.chip-toggle').forEach((chip) => {
    chip.addEventListener('click', () => {
      const tf = chip.dataset.tf;
      const idx = _ssSelectedTimeframes.indexOf(tf);
      if (idx === -1) { _ssSelectedTimeframes.push(tf); chip.classList.add('active'); }
      else { _ssSelectedTimeframes.splice(idx, 1); chip.classList.remove('active'); }
    });
  });

  $('ss-set-min-filter-score').addEventListener('input', () => {
    $('ss-set-min-filter-score-val').textContent = $('ss-set-min-filter-score').value;
  });

  $('ss-set-enabled-toggle').addEventListener('click', () => {
    const btn = $('ss-set-enabled-toggle');
    const on = btn.getAttribute('aria-checked') === 'true';
    btn.setAttribute('aria-checked', String(!on));
  });

  // Phase 8.3: Save/Reset/Reload never touch the running scanner — they
  // only call /api/settings*. ScannerEngine only reads these values at
  // its own start() time (Phase 8.1), so a Save here never restarts or
  // reconfigures an already-running scan.
  $('ss-set-save-btn').addEventListener('click', async () => {
    const btn = $('ss-set-save-btn');
    btn.disabled = true;
    ssSetMsg('');
    try {
      const res = await fetch('/api/settings', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(ssReadFormAsPatch()),
      });
      const data = await res.json();
      if (!res.ok || data.error) throw new Error(data.error || 'Failed to save settings');
      ssSetMsg('✓ Saved — applies the next time you press Start.');
    } catch (e) {
      ssSetMsg('Save failed: ' + e.message);
    } finally {
      btn.disabled = false;
    }
  });

  $('ss-set-reload-btn').addEventListener('click', async () => {
    await ssLoadSettings();
    ssSetMsg('Reloaded from saved settings.');
  });

  $('ss-set-reset-btn').addEventListener('click', async () => {
    if (!confirm('Reset Scanner settings to defaults? Other settings (Indicators, Backtest, etc.) are not affected.')) return;
    const btn = $('ss-set-reset-btn');
    btn.disabled = true;
    try {
      const res = await fetch('/api/scanner/settings/reset', { method: 'POST' });
      const data = await res.json();
      if (!res.ok || data.error) throw new Error(data.error || 'Failed to reset scanner settings');
      ssApplySettingsToForm(data.scanner);
      ssSetMsg('✓ Scanner settings reset to defaults.');
    } catch (e) {
      ssSetMsg('Reset failed: ' + e.message);
    } finally {
      btn.disabled = false;
    }
  });

  $('ss-start-btn').addEventListener('click', async () => {
    try {
      const res = await fetch('/api/scanner/start', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}),
      });
      const data = await res.json();
      if (!res.ok || !data.ok) throw new Error(data.message || data.error || 'Failed to start scanner');
      pollScannerStatus();
    } catch (e) { ssShowError(e.message); }
  });

  $('ss-pause-btn').addEventListener('click', async () => {
    try {
      const res = await fetch('/api/scanner/pause', { method: 'POST' });
      const data = await res.json();
      if (!res.ok || !data.ok) throw new Error(data.message || 'Failed to pause scanner');
    } catch (e) { ssShowError(e.message); }
    pollScannerStatus();
  });

  $('ss-resume-btn').addEventListener('click', async () => {
    try {
      const res = await fetch('/api/scanner/resume', { method: 'POST' });
      const data = await res.json();
      if (!res.ok || !data.ok) throw new Error(data.message || 'Failed to resume scanner');
    } catch (e) { ssShowError(e.message); }
    pollScannerStatus();
  });

  $('ss-stop-btn').addEventListener('click', async () => {
    if (!confirm('Stop the running scanner?')) return;
    try {
      await fetch('/api/scanner/stop', { method: 'POST' });
    } catch (e) { ssShowError(e.message); }
    stopScannerPolling();
    pollScannerStatus();
  });

  // Load Scanner Settings from SettingsStore automatically on first visit.
  ssLoadSettings();

  // One status check on first visit — reflects current server state (in
  // case the scanner is already running from an earlier action this
  // session) without itself starting anything. See pollScannerStatus()'s
  // comment for why this is safe.
  pollScannerStatus();
}
/* ────────────────────────────────────────────────────────────────────────────
 * PHASE 8.4.3 — VALIDATION PAGE: full frontend integration
 * Talks ONLY to the existing, already-tested /api/validation/* routes
 * (ValidationEngine) — no new backend route, no backend file touched.
 * Mirrors the exact patterns already established by the Smart Scanner
 * page: a self-managing poll (pollScannerStatus's pattern), reused
 * fmtLabel()/fmtSeconds() helpers, the Smart Scanner / Backtest asset-grid
 * conventions, and the existing .history-row component for the read-only
 * history.
 * ──────────────────────────────────────────────────────────────────────────── */
let _vInited = false;
let _vPollTimer = null;
let _vSelectedAssets = [];        // [] == "All OTC" (same convention as Smart Scanner settings)
let _vSelectedTimeframes = ['5m']; // matches the '5m'-active-by-default chip rendered in Step 2
// B1 fix: all 13 supported indicators, not hardcoded to the original 3.
// window._VALIDATION_INDICATORS is server-rendered from indicator_registry.get_registry()
// (see app.py index() and templates/index.html) — [{id, name}, ...], 13 entries.
const _vAllIndicators = window._VALIDATION_INDICATORS || [];
let _vSelectedIndicators = _vAllIndicators.map((i) => i.id); // all active by default
let _vHistory = [];                // session-only, in-memory — never persisted, per spec
let _vLastHistorizedFinishedAt = null; // dedupe key so one completed run adds history once

function vShowError(msg) {
  const box = $('v-error-box');
  if (!box) return;
  box.textContent = msg;
  box.hidden = false;
  setTimeout(() => { box.hidden = true; }, 6000);
}

// Sets button visibility/disabled state from the ACTUAL polled server
// status (state/running/paused) — supersedes Step 8A's simpler
// "guess from the last click" version now that real polling exists.
function vSetButtonsForState(uiState) {
  const runBtn = $('v-run-btn');
  const pauseBtn = $('v-pause-btn');
  const resumeBtn = $('v-resume-btn');
  const stopBtn = $('v-stop-btn');

  runBtn.hidden = uiState !== 'stopped';
  runBtn.disabled = uiState !== 'stopped';

  pauseBtn.hidden = uiState !== 'running';
  pauseBtn.disabled = uiState !== 'running';

  resumeBtn.hidden = uiState !== 'paused';
  resumeBtn.disabled = uiState !== 'paused';

  stopBtn.hidden = uiState === 'stopped';
  stopBtn.disabled = uiState === 'stopped';
}

function vSyncButtonsFromStatus(status) {
  const uiState = status.paused ? 'paused' : (status.running ? 'running' : 'stopped');
  vSetButtonsForState(uiState);
}

/* ── Settings panel wiring (asset/timeframe/indicator selection) ────────── */
function vBuildSettingsAssetGrid() {
  const grid = $('v-set-asset-grid');
  if (!grid) return;
  // Live Asset Availability: this is a LIVE selector (Validation fetches
  // real live candles) — it must only ever offer currently-available OTC
  // assets, never the static known universe. Falls back to the static list
  // only until the first live fetch resolves; rebuilt automatically after
  // every live refresh via the listener registered in initValidationPage().
  const all = _liveOtcState.loaded ? _liveOtcState.assets : (window._OTC_ASSETS || []);
  grid.innerHTML = all.length
    ? all.map((sym) => `<button type="button" class="asset-chip${_vSelectedAssets.includes(sym) ? ' active' : ''}" data-sym="${sym}">${fmtLabel(sym)}</button>`).join('')
    : '<div class="asset-empty">No OTC assets currently available from Quotex.</div>';
  grid.querySelectorAll('.asset-chip').forEach((chip) => {
    chip.addEventListener('click', () => {
      const sym = chip.dataset.sym;
      const idx = _vSelectedAssets.indexOf(sym);
      if (idx === -1) { _vSelectedAssets.push(sym); chip.classList.add('active'); }
      else { _vSelectedAssets.splice(idx, 1); chip.classList.remove('active'); }
    });
  });
}

function vSetAssetMode(custom) {
  $('v-set-asset-mode-all').classList.toggle('active', !custom);
  $('v-set-asset-mode-custom').classList.toggle('active', custom);
  $('v-set-asset-grid').hidden = !custom;
}

// B1 fix: builds the indicator chip picker for ALL supported indicators
// (13, from _vAllIndicators) instead of a hardcoded 3-button grid. Same
// chip-toggle markup/behavior as before — all active by default, click
// toggles membership in _vSelectedIndicators.
function vBuildIndicatorGrid() {
  const grid = $('v-set-indicator-grid');
  if (!grid) return;
  grid.innerHTML = _vAllIndicators.map((ind) =>
    `<button class="chip-toggle active" type="button" data-indicator="${ind.id}">${ind.name}</button>`
  ).join('');
  grid.querySelectorAll('.chip-toggle').forEach((chip) => {
    chip.addEventListener('click', () => {
      const name = chip.dataset.indicator;
      const idx = _vSelectedIndicators.indexOf(name);
      if (idx === -1) { _vSelectedIndicators.push(name); chip.classList.add('active'); }
      else { _vSelectedIndicators.splice(idx, 1); chip.classList.remove('active'); }
    });
  });
}

// B1 fix: builds one summary card per indicator in _vAllIndicators (13
// total) instead of 3 hardcoded cards. Markup/classes/id-naming
// (v-summary-<id>, v-sum-<id>-accuracy, etc.) exactly match the previous
// hardcoded blocks so vRenderSummaryCards()/vAppendToHistory() need no
// further changes beyond looping over _vAllIndicators.
function vBuildSummaryCards() {
  const slot = $('v-summary-slot');
  if (!slot) return;
  slot.innerHTML = _vAllIndicators.map((ind, i) => `
    <div class="glass-card v-summary-card" id="v-summary-${ind.id}"${i > 0 ? ' style="margin-top:14px;"' : ''}>
      <div class="section-head"><h3 class="section-title">${ind.name}</h3></div>
      <div class="small-metrics-grid">
        <div class="metric-card"><span class="k">Accuracy</span><span class="v" id="v-sum-${ind.id}-accuracy">—</span></div>
        <div class="metric-card"><span class="k">BUY Accuracy</span><span class="v" id="v-sum-${ind.id}-buy-accuracy">—</span></div>
        <div class="metric-card"><span class="k">SELL Accuracy</span><span class="v" id="v-sum-${ind.id}-sell-accuracy">—</span></div>
        <div class="metric-card"><span class="k">Reliability</span><span class="v" id="v-sum-${ind.id}-reliability">—</span></div>
        <div class="metric-card"><span class="k">Total Samples</span><span class="v" id="v-sum-${ind.id}-total-samples">0</span></div>
        <div class="metric-card"><span class="k">BUY Samples</span><span class="v" id="v-sum-${ind.id}-buy-samples">0</span></div>
        <div class="metric-card"><span class="k">SELL Samples</span><span class="v" id="v-sum-${ind.id}-sell-samples">0</span></div>
        <div class="metric-card"><span class="k">Validation Status</span><span class="v" id="v-sum-${ind.id}-status">Not run</span></div>
      </div>
    </div>
  `).join('');
}

/* ── Progress rendering ──────────────────────────────────────────────────── */
function vRenderProgress(status) {
  $('v-state').textContent = status.state;
  $('v-progress-pct').textContent = `${status.percent_complete ?? 0}%`;
  $('v-progress-bar-fill').style.width = `${status.percent_complete ?? 0}%`;
  $('v-current-asset').textContent = status.current_asset ? fmtLabel(status.current_asset) : '—';
  $('v-current-timeframe').textContent = status.current_timeframe || '—';
  $('v-current-indicator').textContent = status.current_indicator || '—';
  $('v-combinations-processed').textContent = status.combinations_processed ?? 0;
  $('v-combinations-total').textContent = status.total_combinations ?? 0;
  $('v-elapsed').textContent = fmtSeconds(status.elapsed_time);
  $('v-eta').textContent = fmtSeconds(status.estimated_remaining);

  // Status badge (Step 3's markup, wired for the first time here).
  const dot = $('v-status-dot');
  const text = $('v-status-text');
  const dotClass = status.paused ? 'paused' : (status.stopping ? 'stopping' : (status.running ? 'running' : 'stopped'));
  dot.className = `v-status-dot ${dotClass}`;
  text.textContent = status.state;
}

/* ── Summary card rendering ──────────────────────────────────────────────── */
// NOTE (real, honest limitation — flagged since Step 5): validation_engine.py's
// summary does not split accuracy by BUY vs. SELL direction (validate_indicator()
// itself only tracks overall wins/losses, not per-direction). Rather than
// fabricate a number, "BUY Accuracy"/"SELL Accuracy" are rendered as "N/A"
// until/unless a future backend change (out of scope here — validation_engine.py
// is off-limits without explicit approval) adds that breakdown.
function vDeriveStatusLabel(sufficientSample, samples) {
  if (samples > 0 && sufficientSample) return 'Validated';
  if (samples > 0) return 'Insufficient Data';
  return 'No Signal';
}

function vRenderSummaryCards(resultsPayload) {
  const summary = resultsPayload.summary;
  const perIndicator = (summary && summary.per_indicator) || {};

  for (const ind of _vAllIndicators) {
    const name = ind.id;
    const s = perIndicator[name];
    const prefix = `v-sum-${name}`;
    if (!s) {
      $(`${prefix}-accuracy`).textContent = '—';
      $(`${prefix}-buy-accuracy`).textContent = 'N/A';
      $(`${prefix}-sell-accuracy`).textContent = 'N/A';
      $(`${prefix}-reliability`).textContent = '—';
      $(`${prefix}-total-samples`).textContent = '0';
      $(`${prefix}-buy-samples`).textContent = '0';
      $(`${prefix}-sell-samples`).textContent = '0';
      $(`${prefix}-status`).textContent = 'Not run';
      continue;
    }
    $(`${prefix}-accuracy`).textContent = (s.average_win_rate_where_sufficient == null) ? '—' : `${s.average_win_rate_where_sufficient}%`;
    $(`${prefix}-buy-accuracy`).textContent = 'N/A'; // see note above
    $(`${prefix}-sell-accuracy`).textContent = 'N/A'; // see note above
    $(`${prefix}-reliability`).textContent = (s.average_reliability == null) ? '—' : s.average_reliability;
    $(`${prefix}-total-samples`).textContent = s.total_samples ?? 0;
    $(`${prefix}-buy-samples`).textContent = s.total_buy_signals ?? 0;
    $(`${prefix}-sell-samples`).textContent = s.total_sell_signals ?? 0;
    $(`${prefix}-status`).textContent = vDeriveStatusLabel(s.combinations_with_sufficient_sample > 0, s.total_samples || 0);
  }
}

/* ── Results table rendering ──────────────────────────────────────────────── */
function vRenderResultsTable(resultsPayload) {
  const tbody = $('v-results-tbody');
  if (!tbody) return;
  const combos = Object.values(resultsPayload.results || {});
  if (combos.length === 0) {
    tbody.innerHTML = '<tr><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>Not run</td></tr>';
    return;
  }

  const rows = [];
  combos.forEach((combo) => {
    if (combo.error) {
      rows.push(`<tr><td>${fmtLabel(combo.asset || '')}</td><td>${combo.timeframe || '—'}</td><td>—</td><td>—</td><td>N/A</td><td>N/A</td><td>—</td><td>0</td><td>0</td><td>0</td><td>Error</td></tr>`);
      return;
    }
    const indicators = combo.indicators || {};
    for (const [name, r] of Object.entries(indicators)) {
      const accuracy = (r.win_rate == null) ? '—' : `${r.win_rate}%`;
      const reliability = (r.reliability == null) ? '—' : r.reliability;
      const status = vDeriveStatusLabel(r.sufficient_sample, r.samples || 0);
      rows.push(`<tr>
        <td>${fmtLabel(combo.asset || '')}</td>
        <td>${combo.timeframe || '—'}</td>
        <td>${name}</td>
        <td>${accuracy}</td>
        <td>N/A</td>
        <td>N/A</td>
        <td>${reliability}</td>
        <td>${r.samples ?? 0}</td>
        <td>${r.buy_signals ?? 0}</td>
        <td>${r.sell_signals ?? 0}</td>
        <td>${status}</td>
      </tr>`);
    }
  });
  tbody.innerHTML = rows.join('');
}

/* ── History rendering (session-only, in-memory, read-only) ─────────────── */
function vAppendToHistory(status, resultsPayload) {
  const summary = resultsPayload.summary;
  const perIndicator = (summary && summary.per_indicator) || {};
  const timestamp = status.finished_at || new Date().toISOString();

  for (const ind of _vAllIndicators) {
    const name = ind.id;
    const s = perIndicator[name];
    _vHistory.unshift({
      timestamp,
      label: name.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase()),
      timeframeSummary: _vSelectedTimeframes.length ? _vSelectedTimeframes.join(', ') : 'default',
      accuracy: s && s.average_win_rate_where_sufficient != null ? `${s.average_win_rate_where_sufficient}%` : '—',
      reliability: s && s.average_reliability != null ? s.average_reliability : '—',
      status: s ? vDeriveStatusLabel(s.combinations_with_sufficient_sample > 0, s.total_samples || 0) : 'No Signal',
    });
  }
  // Keep history bounded (session-only, in-memory — no persistence, no growth without limit).
  _vHistory = _vHistory.slice(0, 30);
  vRenderHistory();
}

function vRenderHistory() {
  const list = $('v-history-list');
  if (!list) return;
  if (_vHistory.length === 0) {
    list.innerHTML = '<div class="tab-empty">No prior validation runs this session.</div>';
    return;
  }
  list.innerHTML = _vHistory.map((h) => `
    <div class="history-row">
      <div class="history-left">
        <span class="history-asset">Run — ${h.label}</span>
        <span class="history-meta">Timeframe(s): ${h.timeframeSummary}</span>
      </div>
      <div class="history-right">
        <span class="history-conf">Status: ${h.status}</span>
        <span class="history-conf">Accuracy: ${h.accuracy}</span>
        <span class="history-conf">Reliability: ${h.reliability}</span>
        <span class="history-time">${h.timestamp}</span>
      </div>
    </div>`).join('');
}

/* ── Results fetch (summary + table) ──────────────────────────────────────── */
async function vLoadResults() {
  try {
    const res = await fetch('/api/validation/results');
    const data = await res.json();
    vRenderSummaryCards(data);
    vRenderResultsTable(data);
    return data;
  } catch (e) {
    return { results: {}, summary: null };
  }
}

/* ── Self-managing status poll (same pattern as pollScannerStatus) ───────── */
async function pollValidationStatus() {
  try {
    const res = await fetch('/api/validation/status');
    const data = await res.json();
    vRenderProgress(data);
    vSyncButtonsFromStatus(data);

    if (data.running) {
      if (!_vPollTimer) _vPollTimer = setInterval(pollValidationStatus, 2000);
      vLoadResults(); // keep summary/table live while running
    } else {
      if (_vPollTimer) { clearInterval(_vPollTimer); _vPollTimer = null; }
      // On a genuinely new completion (dedup by finished_at), do one final
      // results fetch and add a history entry.
      if (data.finished_at && data.finished_at !== _vLastHistorizedFinishedAt) {
        _vLastHistorizedFinishedAt = data.finished_at;
        const finalResults = await vLoadResults();
        vAppendToHistory(data, finalResults);
      }
    }
  } catch (e) { /* a single failed poll shouldn't tear down an active interval */ }
}

function vStopPolling() {
  if (_vPollTimer) { clearInterval(_vPollTimer); _vPollTimer = null; }
}

/* ── Request body assembly from the Settings panel ───────────────────────── */
function vBuildRunRequestBody() {
  return {
    assets: _vSelectedAssets.length ? _vSelectedAssets : undefined, // omitted -> backend default (all OTC)
    timeframes: _vSelectedTimeframes.length ? _vSelectedTimeframes : undefined,
    indicators: _vSelectedIndicators.length ? _vSelectedIndicators : undefined,
    candle_count: Number($('v-set-candle-count').value),
    lookahead: Number($('v-set-lookahead').value),
  };
}

function initValidationPage() {
  if (_vInited) {
    // Re-visiting the tab: just refresh from current server state, same
    // "never re-triggers a start" guarantee as the Smart Scanner page.
    pollValidationStatus();
    return;
  }
  _vInited = true;

  vBuildSettingsAssetGrid();
  onLiveOtcAssetsChanged(vBuildSettingsAssetGrid);
  refreshLiveOtcAssets();
  $('v-set-asset-mode-all').addEventListener('click', () => vSetAssetMode(false));
  $('v-set-asset-mode-custom').addEventListener('click', () => vSetAssetMode(true));

  $('v-set-tf-grid').querySelectorAll('.chip-toggle').forEach((chip) => {
    chip.addEventListener('click', () => {
      const tf = chip.dataset.tf;
      const idx = _vSelectedTimeframes.indexOf(tf);
      if (idx === -1) { _vSelectedTimeframes.push(tf); chip.classList.add('active'); }
      else { _vSelectedTimeframes.splice(idx, 1); chip.classList.remove('active'); }
    });
  });

  // B1 fix: build the indicator picker + summary cards for all 13
  // supported indicators (was a hardcoded 3-button grid / 3 static cards).
  vBuildIndicatorGrid();
  vBuildSummaryCards();

  $('v-run-btn').addEventListener('click', async () => {
    $('v-run-btn').disabled = true;
    try {
      if (!_vSelectedTimeframes.length) throw new Error('Select at least one timeframe.');
      if (!_vSelectedIndicators.length) throw new Error('Select at least one indicator.');
      const res = await fetch('/api/validation/run', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(vBuildRunRequestBody()),
      });
      const data = await res.json();
      if (!res.ok || !data.ok) throw new Error(data.error || data.message || 'Failed to start validation');
      pollValidationStatus();
    } catch (e) {
      vShowError(e.message);
      $('v-run-btn').disabled = false;
    }
  });

  $('v-pause-btn').addEventListener('click', async () => {
    try {
      const res = await fetch('/api/validation/pause', { method: 'POST' });
      const data = await res.json();
      if (!res.ok || !data.ok) throw new Error(data.message || 'Failed to pause validation');
    } catch (e) { vShowError(e.message); }
    pollValidationStatus();
  });

  $('v-resume-btn').addEventListener('click', async () => {
    try {
      const res = await fetch('/api/validation/resume', { method: 'POST' });
      const data = await res.json();
      if (!res.ok || !data.ok) throw new Error(data.message || 'Failed to resume validation');
    } catch (e) { vShowError(e.message); }
    pollValidationStatus();
  });

  $('v-stop-btn').addEventListener('click', async () => {
    if (!confirm('Stop the running validation?')) return;
    try {
      const res = await fetch('/api/validation/stop', { method: 'POST' });
      const data = await res.json();
      if (!res.ok || !data.ok) throw new Error(data.message || 'Failed to stop validation');
    } catch (e) { vShowError(e.message); }
    pollValidationStatus();
  });

  // One status check on first visit — reflects current server state
  // without itself starting anything (same guarantee as the Smart
  // Scanner page's own first-visit check).
  pollValidationStatus();
}

/* ────────────────────────────────────────────────────────────────────────────
 * Phase 9 — Smart Learning page
 * Talks ONLY to /api/learning/* (status, generate, history, apply, reset).
 * Advisory only: never places trades, never touches Confluence/Hard Gates
 * directly — Apply just calls the existing settings-weights write path.
 * ──────────────────────────────────────────────────────────────────────────── */

let _lInited = false;
let _lPollTimer = null;

const TREND_ICON = { improving: '📈', degrading: '📉', declining: '📉', stable: '➡️', no_data: '—' };
const CONFIDENCE_LABEL = { none: 'None', low: 'Low', medium: 'Medium', high: 'High' };

function lIndicatorLabel(key) {
  return key
    .split('_')
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
}

function lShowError(msg) {
  const box = $('l-error-box');
  if (!box) return;
  box.textContent = msg;
  box.hidden = false;
  $('l-success-box').hidden = true;
}

function lShowSuccess(msg) {
  const box = $('l-success-box');
  if (!box) return;
  box.textContent = msg;
  box.hidden = false;
  $('l-error-box').hidden = true;
}

function lClearMessages() {
  $('l-error-box').hidden = true;
  $('l-success-box').hidden = true;
}

/* ── Weights table rendering ──────────────────────────────────────────────── */
function lRenderWeightsTable(data) {
  const tbody = $('l-weights-tbody');
  if (!tbody) return;
  const perIndicator = data.per_indicator || {};
  const names = Object.keys(perIndicator);

  $('l-status-loading').hidden = true;
  const table = $('l-weights-table');

  if (!names.length) {
    table.hidden = true;
    $('l-status-loading').hidden = false;
    $('l-status-loading').innerHTML = '<div class="tab-empty small">No confluence weights available yet.</div>';
    return;
  }

  table.hidden = false;
  const rows = names.map((name) => {
    const row = perIndicator[name];
    const icon = TREND_ICON[row.trend] || '—';
    const trendLabel = row.trend === 'no_data' ? 'No data' : row.trend.charAt(0).toUpperCase() + row.trend.slice(1);
    const confLabel = CONFIDENCE_LABEL[row.confidence] || row.confidence;
    const changed = Math.abs(row.recommended_weight - row.current_weight) > 0.005;
    return `<tr>
      <td>${lIndicatorLabel(name)}</td>
      <td>${row.current_weight.toFixed(2)}</td>
      <td${changed ? ' class="buy"' : ''}>${row.recommended_weight.toFixed(2)}</td>
      <td>${icon} ${trendLabel}</td>
      <td>${confLabel}</td>
      <td>${row.sample_count}</td>
    </tr>`;
  });
  tbody.innerHTML = rows.join('');
}

/* ── Recommendation History rendering ─────────────────────────────────────── */
function lRenderHistory(historyData) {
  const log = (historyData && historyData.recommendation_log) || [];
  const empty = $('l-history-empty');
  const list = $('l-history-list');
  if (!list) return;

  if (!log.length) {
    empty.hidden = false;
    list.innerHTML = '';
    return;
  }
  empty.hidden = true;

  const rows = log.slice(0, 20).map((rec) => {
    const changedCount = Object.values(rec.per_indicator || {}).filter(
      (r) => Math.abs(r.recommended_weight - r.current_weight) > 0.005
    ).length;
    const genAt = rec.generated_at || '—';
    return `<div class="history-row">
      <div class="history-left">
        <span class="history-asset">Recommendation</span>
        <span class="history-meta">${changedCount} indicator(s) adjusted</span>
      </div>
      <div class="history-right">
        <span class="history-conf">${Object.keys(rec.recommended_weights || {}).length} factors</span>
        <span class="history-time">${genAt}</span>
      </div>
    </div>`;
  });
  list.innerHTML = rows.join('');
}

/* ── API calls ─────────────────────────────────────────────────────────────── */
async function lLoadStatus() {
  try {
    const res = await fetch('/api/learning/status');
    if (!res.ok) throw new Error(`Status ${res.status}`);
    const data = await res.json();
    lRenderWeightsTable(data);
    return data;
  } catch (e) {
    $('l-status-loading').hidden = false;
    $('l-status-loading').innerHTML = '<div class="tab-empty small">Could not load current weights — try again shortly.</div>';
    $('l-weights-table').hidden = true;
    return null;
  }
}

async function lLoadHistory() {
  try {
    const res = await fetch('/api/learning/history');
    if (!res.ok) throw new Error(`Status ${res.status}`);
    const data = await res.json();
    lRenderHistory(data);
  } catch (e) { /* a single failed poll shouldn't clear an already-rendered list */ }
}

async function lGenerateRecommendation() {
  lClearMessages();
  const btn = $('l-generate-btn');
  btn.disabled = true;
  try {
    const res = await fetch('/api/learning/generate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || `Status ${res.status}`);
    lRenderWeightsTable(data);
    await lLoadHistory();
    lShowSuccess('Recommendation generated and added to history below.');
  } catch (e) {
    lShowError(`Could not generate a recommendation: ${e.message}`);
  } finally {
    btn.disabled = false;
  }
}

async function lApplyRecommendation() {
  lClearMessages();
  const btn = $('l-apply-btn');
  btn.disabled = true;
  try {
    const res = await fetch('/api/learning/apply', { method: 'POST' });
    const data = await res.json();
    if (!res.ok || data.ok === false) {
      const reasons = (data.reasons || [data.error || 'Unknown error']).join(' ');
      throw new Error(reasons);
    }
    lShowSuccess('Recommendation applied — Settings now use the updated weights.');
    await lLoadStatus();
  } catch (e) {
    lShowError(`Could not apply recommendation: ${e.message}`);
  } finally {
    btn.disabled = false;
  }
}

async function lResetLearning() {
  lClearMessages();
  const btn = $('l-reset-btn');
  btn.disabled = true;
  try {
    const res = await fetch('/api/learning/reset', { method: 'POST' });
    if (!res.ok) throw new Error(`Status ${res.status}`);
    await lLoadHistory();
    lShowSuccess('Recommendation history cleared. Validation History itself was not touched.');
  } catch (e) {
    lShowError(`Could not reset learning history: ${e.message}`);
  } finally {
    btn.disabled = false;
  }
}

/* ════════════════════════════════════════════════════════════════════════
 * Phase 10.2 — Asset Intelligence + Timeframe Intelligence (additive;
 * everything above this comment is Phase 9's unchanged Smart Learning JS).
 * Read-only renderers for GET /api/learning/{assets,timeframes,
 * top-indicators,recommendations} — no write calls, nothing here is ever
 * POSTed anywhere.
 * ════════════════════════════════════════════════════════════════════════ */

function aiTrendIcon(trend) {
  return TREND_ICON[trend] || '—';
}

function aiMetricCard(label, value, sub) {
  return `<div class="metric-card"><span class="k">${label}</span><span class="v">${value}</span>${sub ? `<span class="sub">${sub}</span>` : ''}</div>`;
}

/* ── Best Assets / Best Timeframes (top 4 by ranking, as metric cards) ──── */
function aiRenderBestGroup(rankingData, keyField, groupsKey, gridId, emptyId) {
  const groups = rankingData[groupsKey] || {};
  const ranking = rankingData.ranking || [];
  const grid = $(gridId);
  const empty = $(emptyId);
  if (!grid || !empty) return;

  if (!ranking.length) {
    empty.hidden = false;
    grid.hidden = true;
    return;
  }
  empty.hidden = true;
  grid.hidden = false;

  const top = ranking.slice(0, 4);
  grid.innerHTML = top.map((key) => {
    const g = groups[key];
    const acc = g.accuracy != null ? `${g.accuracy.toFixed(1)}%` : '—';
    const best = g.best_indicator ? lIndicatorLabel(g.best_indicator) : '—';
    return aiMetricCard(key, acc, `${g.total_validations} samples · best: ${best}`);
  }).join('');
}

/* ── Asset / Timeframe ranking tables (every ranked group, full detail) ── */
function aiRenderRankingTable(rankingData, groupsKey, tbodyId, tableId, emptyId) {
  const groups = rankingData[groupsKey] || {};
  const ranking = rankingData.ranking || [];
  const keys = Object.keys(groups);
  const table = $(tableId);
  const empty = $(emptyId);
  const tbody = $(tbodyId);
  if (!table || !empty || !tbody) return;

  if (!keys.length) {
    empty.hidden = false;
    table.hidden = true;
    return;
  }
  empty.hidden = true;
  table.hidden = false;

  // Ranked (confident) groups first, in best-to-worst order; any remaining
  // groups with too little data for the ranking are appended after, so
  // nothing recorded is silently hidden from this table.
  const ordered = ranking.concat(keys.filter((k) => !ranking.includes(k)));
  tbody.innerHTML = ordered.map((key) => {
    const g = groups[key];
    const acc = g.accuracy != null ? `${g.accuracy.toFixed(1)}%` : '—';
    const best = g.best_indicator ? lIndicatorLabel(g.best_indicator) : '—';
    return `<tr>
      <td>${key}</td>
      <td>${g.total_validations}</td>
      <td>${acc}</td>
      <td>${aiTrendIcon(g.trend)} ${g.trend === 'no_data' ? 'No data' : g.trend}</td>
      <td>${CONFIDENCE_LABEL[g.confidence] || g.confidence}</td>
      <td>${best}</td>
    </tr>`;
  }).join('');
}

/* ── Top / Weak Indicators ────────────────────────────────────────────── */
function aiRenderTopIndicators(data) {
  const grid = $('ai-top-indicators-grid');
  const empty = $('ai-top-indicators-empty');
  if (!grid || !empty) return;
  const top = data.top || [];
  const weakest = data.weakest || [];
  if (!top.length && !weakest.length) {
    empty.hidden = false;
    grid.hidden = true;
    return;
  }
  empty.hidden = true;
  grid.hidden = false;
  const topCards = top.map((e) => aiMetricCard(`⭐ ${lIndicatorLabel(e.indicator)}`, `${e.average_win_rate.toFixed(1)}%`, `${e.total_samples} samples`));
  const weakCards = weakest.map((e) => aiMetricCard(`⚠️ ${lIndicatorLabel(e.indicator)}`, `${e.average_win_rate.toFixed(1)}%`, `${e.total_samples} samples`));
  grid.innerHTML = topCards.concat(weakCards).join('');
}

/* ── Indicator Trends (improving / declining lists) ──────────────────── */
function aiRenderTrends(rec) {
  const listWrap = $('ai-trends-list');
  const empty = $('ai-trends-empty');
  const improving = rec.improving_indicators || [];
  const declining = rec.declining_indicators || [];
  if (!listWrap || !empty) return;

  if (!improving.length && !declining.length) {
    empty.hidden = false;
    listWrap.hidden = true;
    return;
  }
  empty.hidden = true;
  listWrap.hidden = false;

  const rowHtml = (e) => `<div class="history-row">
      <div class="history-left">
        <span class="history-asset">${lIndicatorLabel(e.indicator)}</span>
        <span class="history-meta">${e.total_samples} samples · ${CONFIDENCE_LABEL[e.confidence] || e.confidence} confidence</span>
      </div>
      <div class="history-right">
        <span class="history-conf">${e.average_win_rate.toFixed(1)}% → ${e.last_win_rate.toFixed(1)}%</span>
        <span class="history-time">${e.difference > 0 ? '+' : ''}${e.difference.toFixed(1)}pp</span>
      </div>
    </div>`;

  $('ai-improving-list').innerHTML = improving.length
    ? improving.map(rowHtml).join('')
    : '<div class="tab-empty small">None currently improving.</div>';
  $('ai-declining-list').innerHTML = declining.length
    ? declining.map(rowHtml).join('')
    : '<div class="tab-empty small">None currently declining.</div>';
}

/* ── Recommendation Cards (best indicator per asset / per timeframe) ───── */
function aiRenderRecommendationCards(rec) {
  const wrap = $('ai-rec-cards');
  const empty = $('ai-rec-cards-empty');
  const perAsset = rec.best_indicators_per_asset || {};
  const perTimeframe = rec.best_indicators_per_timeframe || {};
  const assetKeys = Object.keys(perAsset);
  const tfKeys = Object.keys(perTimeframe);
  if (!wrap || !empty) return;

  if (!assetKeys.length && !tfKeys.length) {
    empty.hidden = false;
    wrap.hidden = true;
    return;
  }
  empty.hidden = true;
  wrap.hidden = false;

  $('ai-rec-per-asset').innerHTML = assetKeys.length
    ? assetKeys.map((a) => aiMetricCard(a, perAsset[a] ? lIndicatorLabel(perAsset[a]) : '—')).join('')
    : '<div class="tab-empty small">No data yet.</div>';
  $('ai-rec-per-timeframe').innerHTML = tfKeys.length
    ? tfKeys.map((t) => aiMetricCard(t, perTimeframe[t] ? lIndicatorLabel(perTimeframe[t]) : '—')).join('')
    : '<div class="tab-empty small">No data yet.</div>';
}

/* ── API calls ─────────────────────────────────────────────────────────── */
async function aiLoadAssets() {
  try {
    const res = await fetch('/api/learning/assets');
    if (!res.ok) throw new Error(`Status ${res.status}`);
    const data = await res.json();
    aiRenderBestGroup(data, 'asset', 'assets', 'ai-best-assets-grid', 'ai-best-assets-empty');
    aiRenderRankingTable(data, 'assets', 'ai-asset-ranking-tbody', 'ai-asset-ranking-table', 'ai-asset-ranking-empty');
  } catch (e) { /* a single failed poll shouldn't clear an already-rendered view */ }
}

async function aiLoadTimeframes() {
  try {
    const res = await fetch('/api/learning/timeframes');
    if (!res.ok) throw new Error(`Status ${res.status}`);
    const data = await res.json();
    aiRenderBestGroup(data, 'timeframe', 'timeframes', 'ai-best-timeframes-grid', 'ai-best-timeframes-empty');
    aiRenderRankingTable(data, 'timeframes', 'ai-tf-ranking-tbody', 'ai-tf-ranking-table', 'ai-tf-ranking-empty');
  } catch (e) { /* same as above */ }
}

async function aiLoadTopIndicators() {
  try {
    const res = await fetch('/api/learning/top-indicators');
    if (!res.ok) throw new Error(`Status ${res.status}`);
    aiRenderTopIndicators(await res.json());
  } catch (e) { /* same as above */ }
}

async function aiLoadRecommendations() {
  try {
    const res = await fetch('/api/learning/recommendations');
    if (!res.ok) throw new Error(`Status ${res.status}`);
    const data = await res.json();
    aiRenderTrends(data);
    aiRenderRecommendationCards(data);
  } catch (e) { /* same as above */ }
}

function aiLoadAll() {
  aiLoadAssets();
  aiLoadTimeframes();
  aiLoadTopIndicators();
  aiLoadRecommendations();
}

function lStopPolling() {
  if (_lPollTimer) { clearInterval(_lPollTimer); _lPollTimer = null; }
}

function initLearningPage() {
  if (_lInited) {
    // Re-visiting the tab: just refresh, same "never re-triggers an action
    // on its own" guarantee as every other page here.
    lLoadStatus();
    lLoadHistory();
    aiLoadAll();  // Phase 10.2 — Asset/Timeframe Intelligence, read-only refresh
    if (!_lPollTimer) _lPollTimer = setInterval(lLoadStatus, 15000);
    return;
  }
  _lInited = true;

  $('l-generate-btn').addEventListener('click', lGenerateRecommendation);
  $('l-apply-btn').addEventListener('click', lApplyRecommendation);
  $('l-reset-btn').addEventListener('click', lResetLearning);

  lLoadStatus();
  lLoadHistory();
  aiLoadAll();  // Phase 10.2 — Asset/Timeframe Intelligence, initial load
  _lPollTimer = setInterval(lLoadStatus, 15000);
}

/* ────────────────────────────────────────────────────────────────────────────
 * INIT
 * ──────────────────────────────────────────────────────────────────────────── */

initAssetGrid('man-asset-grid', 'man-asset-search', 'man-asset');
loadSignalsToday();
$('stat-assets').textContent = SCAN_LIST.length;
syncAutoScanToggleUI();
navigateTo('scanner');

// Step 7 — initial page/app-load live OTC availability refresh. Registers
// the shared banner renderer against every page that has one, then kicks
// off the first live fetch; onLiveOtcAssetsChanged() re-renders all of
// them (plus every asset-grid listener registered above) the moment it
// resolves — no separate polling loop, this is a one-shot fetch reused by
// every live selector already listening for state changes.
for (const bannerId of ['man-live-otc-banner', 'ss-live-otc-banner', 'v-live-otc-banner']) {
  if ($(bannerId)) onLiveOtcAssetsChanged(() => renderLiveOtcBanner(bannerId));
}
refreshLiveOtcAssets();

/* ════════════════════════════════════════════════════════════════════════
 * Phase 10.3 Part-2 — AI Health Dashboard + Explainable Signal System.
 * Additive only — nothing above this block is modified. Distinct 'aih'
 * prefix throughout (functions + element ids) to avoid any collision with
 * Phase 10.2's 'ai' prefix (Asset Intelligence, inside the Learning page).
 * ════════════════════════════════════════════════════════════════════════ */

const AIH_HEALTH_BADGE_CLASS = {
  Excellent: 'badge-health-excellent', Good: 'badge-health-good', Fair: 'badge-health-fair',
  Poor: 'badge-health-poor', Critical: 'badge-health-critical',
};

function aihSetBadge(elId, label) {
  const el = $(elId);
  if (!el) return;
  el.textContent = label || '—';
  el.className = 'badge ' + (AIH_HEALTH_BADGE_CLASS[label] || '');
}

function aihMetricCard(label, value, sub) {
  return `<div class="metric-card"><span class="k">${label}</span><span class="v">${value}</span>${sub ? `<span class="sub">${sub}</span>` : ''}</div>`;
}

function aihPct(v) { return (v === null || v === undefined) ? '—' : `${v}%`; }

function aihLoadHealth() {
  fetch('/api/ai/health').then((r) => r.json()).then((data) => {
    aihSetBadge('aih-overall-badge', data.overall_health && data.overall_health.status);
    $('aih-current-regime').textContent = (data.scanner_health && data.scanner_health.current_regime) || 'Unknown';

    aihSetBadge('aih-indicator-badge', data.indicator_health && data.indicator_health.status);
    aihSetBadge('aih-scanner-badge', data.scanner_health && data.scanner_health.status);
    aihSetBadge('aih-validation-badge', data.validation_health && data.validation_health.status);
    aihSetBadge('aih-learning-badge', data.learning_health && data.learning_health.status);
    aihSetBadge('aih-regime-badge', data.regime_health && data.regime_health.status);

    const statsGrid = $('aih-stats-grid');
    if (statsGrid) {
      statsGrid.innerHTML = [
        aihMetricCard('Avg Confidence', data.average_confidence === null ? '—' : `${data.average_confidence}%`),
        aihMetricCard('Avg Filter Score', data.average_filter_score === null ? '—' : data.average_filter_score),
        aihMetricCard('Recent Accuracy', data.recent_accuracy === null ? '—' : `${data.recent_accuracy}%`),
        aihMetricCard('Recent Signals', data.recent_signal_count ?? 0),
        aihMetricCard('Recent WAIT %', data.recent_wait_pct === null ? 'n/a' : `${data.recent_wait_pct}%`,
                       data.recent_wait_pct === null ? 'not tracked by scanner' : ''),
        aihMetricCard('BUY %', aihPct(data.buy_pct)),
        aihMetricCard('SELL %', aihPct(data.sell_pct)),
        aihMetricCard('Data Quality', data.data_quality || '—'),
        aihMetricCard('History Coverage', aihPct(data.history_coverage)),
      ].join('');
    }

    const v = data.validation_health || {};
    $('aih-validation-status-text').textContent = v.total_samples
      ? `${v.total_samples} samples across ${v.runs_recorded || 0} run(s)` + (v.last_run_at ? `, last run ${v.last_run_at}` : '')
      : 'No validation runs recorded yet.';
    const l = data.learning_health || {};
    $('aih-learning-status-text').textContent = l.indicators_total
      ? `${l.indicators_with_confidence || 0}/${l.indicators_total} indicators have a learning confidence opinion`
      : 'No learning data available yet.';
  }).catch(() => {});
}

function aihLoadTopIndicators() {
  fetch('/api/learning/top-indicators').then((r) => r.json()).then((data) => {
    const empty = $('aih-top-indicators-empty');
    const grid = $('aih-top-indicators-grid');
    const top = (data.top || []).slice(0, 3);
    const weakest = (data.weakest || []).slice(0, 3);
    if (!top.length && !weakest.length) {
      empty.hidden = false; grid.hidden = true;
      return;
    }
    empty.hidden = true; grid.hidden = false;
    grid.innerHTML = [
      ...top.map((e) => aihMetricCard('⭐ ' + lIndicatorLabel(e.indicator), `${e.average_win_rate}%`, `${e.total_samples} samples`)),
      ...weakest.map((e) => aihMetricCard('⚠️ ' + lIndicatorLabel(e.indicator), `${e.average_win_rate}%`, `${e.total_samples} samples`)),
    ].join('');
  }).catch(() => {});
}

function aihLoadBestGroups() {
  Promise.all([
    fetch('/api/learning/assets').then((r) => r.json()).catch(() => null),
    fetch('/api/learning/timeframes').then((r) => r.json()).catch(() => null),
  ]).then(([assetData, tfData]) => {
    const empty = $('aih-best-groups-empty');
    const wrap = $('aih-best-groups');
    const assets = (assetData && assetData.assets) || {};
    const timeframes = (tfData && tfData.timeframes) || {};
    const assetRanking = (assetData && assetData.ranking) || [];
    const tfRanking = (tfData && tfData.ranking) || [];
    const assetTop = assetRanking.slice(0, 4).map((name) => [name, assets[name]]);
    const tfTop = tfRanking.slice(0, 4).map((name) => [name, timeframes[name]]);
    if (!assetTop.length && !tfTop.length) {
      empty.hidden = false; wrap.hidden = true;
      return;
    }
    empty.hidden = true; wrap.hidden = false;
    $('aih-best-assets-grid').innerHTML = assetTop.map(([name, g]) =>
      aihMetricCard(fmtLabel(name), g && g.accuracy !== null && g.accuracy !== undefined ? `${g.accuracy}%` : '—',
                    g && g.best_indicator ? `best: ${lIndicatorLabel(g.best_indicator)}` : '')).join('');
    $('aih-best-timeframes-grid').innerHTML = tfTop.map(([name, g]) =>
      aihMetricCard(name, g && g.accuracy !== null && g.accuracy !== undefined ? `${g.accuracy}%` : '—',
                    g && g.best_indicator ? `best: ${lIndicatorLabel(g.best_indicator)}` : '')).join('');
  }).catch(() => {});
}

function aihPopulateExplainSelectors() {
  const assetSel = $('aih-explain-asset');
  const tfSel = $('aih-explain-timeframe');
  if (assetSel) {
    const selectedValue = assetSel.value;
    // Live Asset Availability: this is a LIVE selector — only currently-
    // available OTC assets, never the static known universe. Falls back to
    // the static list only until the first live fetch resolves.
    const otcSyms = _liveOtcState.loaded ? _liveOtcState.assets : (window._OTC_ASSETS || []);
    const otc = otcSyms.map((s) => `<option value="${s}">${fmtLabel(s)} (OTC)</option>`);
    const live = (window._LIVE_ASSETS || []).map((s) => `<option value="${s}">${fmtLabel(s)}</option>`);
    assetSel.innerHTML = '<option value="">— scanner\'s top signal —</option>' + otc.join('') + live.join('');
    if (otcSyms.includes(selectedValue) || (window._LIVE_ASSETS || []).includes(selectedValue)) {
      assetSel.value = selectedValue;
    }
    assetSel.dataset.populated = '1';
  }
  if (tfSel && !tfSel.dataset.populated) {
    tfSel.innerHTML = (window._TIMEFRAMES || []).map((tf) => `<option value="${tf}">${tf}</option>`).join('');
    tfSel.value = window._DEFAULT_TF || '';
    tfSel.dataset.populated = '1';
  }
}
onLiveOtcAssetsChanged(aihPopulateExplainSelectors);

function aihCheckRow(label, check) {
  const passed = !!(check && check.passed);
  const icon = passed ? '<span class="aih-check-icon-pass">✔</span>' : '<span class="aih-check-icon-fail">✖</span>';
  const detail = check && check.detail ? check.detail : '';
  return `<div class="aih-check-row">${icon}<div><strong>${label}</strong><div style="color:var(--text-muted);font-size:11px;">${detail}</div></div></div>`;
}

function aihExplainSignal() {
  aihPopulateExplainSelectors();
  const asset = $('aih-explain-asset').value;
  const timeframe = $('aih-explain-timeframe').value;
  const body = $('aih-explain-body');
  body.innerHTML = '<div class="tab-loading">Analyzing…</div>';
  $('aih-explain-modal-overlay').hidden = false;

  const qs = asset ? `?asset=${encodeURIComponent(asset)}&timeframe=${encodeURIComponent(timeframe)}` : '';
  fetch('/api/ai/explain' + qs).then((r) => r.json()).then((data) => {
    if (data.error) {
      body.innerHTML = `<div class="tab-error">${data.error}</div>`;
      return;
    }
    const checks = data.checks || {};
    const checkLabels = {
      trend: 'Trend', momentum: 'Momentum', volatility: 'Volatility', volume: 'Volume',
      price_action: 'Price Action', support_resistance: 'Support/Resistance', adx: 'ADX',
      mtf: 'MTF', regime: 'Regime',
    };
    const checksHtml = Object.entries(checkLabels).map(([key, label]) => aihCheckRow(label, checks[key])).join('');

    const gatesPassed = (data.hard_gates_passed || []).map((g) => `<span class="badge badge-readonly" style="margin:2px;">${g.label}</span>`).join('') || '<span style="color:var(--text-muted);font-size:11px;">None</span>';
    const gatesFailed = (data.hard_gates_failed || []).map((g) => `<span class="badge badge-live" style="margin:2px;">${g.label}</span>`).join('') || '<span style="color:var(--text-muted);font-size:11px;">None</span>';

    const reasonsHtml = (data.reasons || []).map((r) => `<div class="history-row" style="border:none;padding:3px 0;font-size:11px;">• ${r}</div>`).join('') || '<div style="color:var(--text-muted);font-size:11px;">No reasons available.</div>';
    const warningsHtml = (data.warnings || []).map((w) => `<div class="history-row" style="border:none;padding:3px 0;font-size:11px;color:var(--wait);">⚠ ${w}</div>`).join('') || '<div style="color:var(--text-muted);font-size:11px;">No warnings.</div>';

    body.innerHTML = `
      <div class="small-metrics-grid" style="margin-bottom:14px;">
        ${aihMetricCard('Signal', data.signal)}
        ${aihMetricCard('Confidence', `${data.confidence}%`)}
        ${aihMetricCard('Filter Score', data.final_filter_score)}
      </div>
      <div style="margin-bottom:6px;font-size:11px;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.05em;">Indicator Contributions</div>
      <div style="margin-bottom:14px;">${checksHtml}</div>
      <div style="margin-bottom:6px;font-size:11px;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.05em;">Hard Gates Passed</div>
      <div style="margin-bottom:10px;">${gatesPassed}</div>
      <div style="margin-bottom:6px;font-size:11px;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.05em;">Hard Gates Failed</div>
      <div style="margin-bottom:14px;">${gatesFailed}</div>
      <div style="margin-bottom:6px;font-size:11px;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.05em;">Reasons</div>
      <div style="margin-bottom:14px;">${reasonsHtml}</div>
      <div style="margin-bottom:6px;font-size:11px;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.05em;">Warnings</div>
      <div>${warningsHtml}</div>
    `;
  }).catch(() => {
    body.innerHTML = '<div class="tab-error">Failed to load explanation.</div>';
  });
}

function aihCloseExplainModal() {
  $('aih-explain-modal-overlay').hidden = true;
}

function aihLoadAll() {
  aihLoadHealth();
  aihLoadTopIndicators();
  aihLoadBestGroups();
  aihPopulateExplainSelectors();
}

function initAiHealthPage() {
  aihLoadAll();
}
