#!/usr/bin/env python3
"""
Quotex Market Analyzer — Web Dashboard
========================================
Flask front-end for the analysis-only pipeline in market_analyzer/.
Runs the exact same fetch -> indicators -> backtest -> confluence-signal
flow as run_analysis.py and renders the result in a browser.

There is NO trade-placement button or code path anywhere in this app —
only signal generation and display, same as the rest of the project.
"""

from __future__ import annotations

import os
import sys
import json
import asyncio
import threading
import time
import calendar
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ── Path bootstrap (mirrors run_analysis.py) ───────────────────────────────────
_WEBAPP_DIR = Path(__file__).resolve().parent      # market_analyzer/webapp/
_MARKET_DIR = _WEBAPP_DIR.parent                    # market_analyzer/
_ROOT       = _MARKET_DIR.parent                    # workspace root (Quotex/)
_QUOTEX_DIR = _ROOT / "quotex"                      # quotex/

for _p in (_MARKET_DIR, _QUOTEX_DIR):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from flask import Flask, render_template, request, jsonify

import config as cfg
from fetch_data import QuotexDataFetcher
from indicators import calculate_all
from analyzer import (
    generate_signal, generate_confluence_signal, load_precomputed_weights,
    calculate_filter_score, DEFAULT_CONFLUENCE_WEIGHTS,
)
from backtest import backtest_factor_accuracy, compute_dynamic_weights
from regime_pipeline import compute_regime_adjusted_weights
from explainable_signal import explain_signal
from ai_health_engine import compute_ai_health
from ai_health_history_store import AIHealthHistoryStore
import ai_health_trends
import ai_performance_reports
import analytics_dashboard
from adaptive_calibration import (
    build_calibration_report, generate_calibration_recommendations,
)
from api_quotex.constants import ASSETS, TIMEFRAMES as _TF_SECONDS_MAP
from scanner import ScannerEngine, ScannerConfig

# Phase 7.4 — Settings Store / Backtest Engine / Indicator Registry.
# Purely additive infrastructure: none of these modules touch analyzer.py,
# scanner.py, backtest.py's internals, or the Quotex API/WebSocket/login code.
from settings_store import SettingsStore
import live_assets
from backtest_engine import (
    BacktestEngine, evaluate_apply_conditions,
    STOPPED as _BT_STOPPED, CANDLE_OPTIONS as BACKTEST_CANDLE_OPTIONS,
)
import indicator_registry as ind_registry

# Phase 8.4.3 — ValidationEngine. Same additive-infrastructure guarantee as
# the Phase 7.4 imports above: does not touch analyzer.py, backtest.py,
# scanner.py, indicator_registry.py, indicators.py, or the Quotex API.
from validation_engine import (
    ValidationEngine,
    STOPPED as _VAL_STOPPED, VALIDATION_CANDLE_OPTIONS,
)
from indicator_validation import INDICATOR_NAMES as VALIDATION_INDICATOR_NAMES
# Phase 10.1 — Universal Validation: the full 13-factor confluence set,
# additive alongside VALIDATION_INDICATOR_NAMES above (kept, unused-but-
# available for backward compatibility with anything importing it from
# this module).
from indicator_validation import UNIVERSAL_INDICATOR_NAMES as VALIDATION_UNIVERSAL_INDICATOR_NAMES
from indicator_validation import summarize_by_asset as _summarize_validation_by_asset
from indicator_validation import summarize_by_timeframe as _summarize_validation_by_timeframe

# Phase 8.5 — Validation History Store. Same additive-infrastructure
# guarantee: does not touch validation_engine.py, indicator_validation.py,
# analyzer.py, backtest.py, scanner.py, settings_store.py, or
# indicator_registry.py. Persistence is app.py-polling-driven only (see
# _maybe_persist_validation_history() below) — no callback was added
# inside ValidationEngine.
from validation_history_store import ValidationHistoryStore

# Phase 9 — Smart Learning & Adaptive Weight System. Same additive-only
# guarantee as Phase 8.5 above: learning_engine.py does not import or
# modify analyzer.py, backtest.py, validation_engine.py,
# indicator_validation.py, scanner.py, or indicator_registry.py. It only
# reads _validation_history_store.get_history() (read-only) and produces a
# recommendation dict; applying one reuses the existing, unmodified
# settings_store.apply_suggested_weights() — no new write path into
# settings.json is introduced here.
from learning_engine import LearningHistoryStore, compute_recommendation
import learning_engine as _learning_engine

# Phase 10.2 — Asset Intelligence + Timeframe Intelligence. Same
# decoupling guarantee: asset_timeframe_learning.py does not import or
# modify analyzer.py, backtest.py, validation_engine.py,
# indicator_validation.py, scanner.py, indicator_registry.py, or
# learning_engine.py. It only reads _validation_history_store.get_history()
# (read-only, same store instance Phase 9's Learning routes already use)
# and returns computed rankings/recommendations — never writes anywhere,
# never applies anything automatically. Purely additive routes below.
import asset_timeframe_learning as _asset_timeframe_learning

app = Flask(__name__)

TIMEFRAMES = ["30s", "1m", "2m", "3m", "5m", "10m", "15m", "30m", "45m", "1h"]

_FACTOR_LABELS = {
    "bb": "BB Bounce",
    "rsi_div": "RSI Divergence",
    "stoch": "Stochastic Cross",
    "cci": "CCI Extreme",
    "candle": "Candlestick",
    "mean_reversion": "Mean Reversion",
    "exhaustion": "Candle Exhaustion",
    "round_number": "Round Number",
    "obv": "OBV Divergence",  # Step 4: new factor, additive only
    "sr": "Support/Resistance Zone",  # Phase 6: new factor, additive only
    # Phase 7.3 Part 3, connected to confluence in Phase 8.6 — labels match
    # webapp/indicator_registry.py's _INDICATOR_META names exactly. Reporting
    # only: no change to vote calculation, weighting, or confluence logic.
    "wick_rejection": "Wick Rejection",
    "liquidity_sweep": "Liquidity Sweep",
    "false_breakout": "False Breakout",
}
_VOTE_LABELS = {1: "BULLISH", -1: "BEARISH", 0: "NEUTRAL"}


def _asset_choices() -> Dict[str, list]:
    """Split the asset table into OTC / Live groups for the dropdown."""
    otc = sorted(a for a in ASSETS if a.lower().endswith("_otc"))
    live = sorted(a for a in ASSETS if not a.lower().endswith("_otc"))
    return {"otc": otc, "live": live}


# ─── P0 fix (Unknown Asset / live asset source of truth) ────────────────────
# The static ASSETS dict (api_quotex/constants.py) is a hardcoded reference
# snapshot that is never refreshed from the live Quotex feed. Live-operation
# routes (Manual Analyzer /api/signal, /api/ai/explain) must never reject an
# asset Quotex is currently trading merely because it's absent from that
# stale dict — but they also must never fabricate availability if the live
# session/discovery call itself fails. This helper is the single place that
# implements that rule for those two routes; it does not change how
# backtest/validation gate assets (they keep the static ASSETS dict as a
# valid historical/reference universe — see their own inline comments below).
def _check_live_asset(asset: str, timeframe: Optional[str] = None) -> Dict[str, Any]:
    """
    Live-operation asset gate. Source of truth is the CURRENT live Quotex
    snapshot (live_assets.get_live_assets(), full — OTC and regular pairs),
    never the static ASSETS dict. Returns one of these real states:
      - live discovery/session failure -> ok=False, status=503 (a session
        problem, not an "unknown asset" — never silently treated as known
        via the static list)
      - asset absent from the live snapshot -> ok=False, status=400
        (genuinely not available from Quotex right now)
      - asset present, but the broker's OWN asset row explicitly confirmed a
        timeframe list that does not include the requested `timeframe`
        -> ok=False, status=400, kind="unsupported_timeframe" (fails fast,
        no wasted candle-fetch retries on a combination the broker itself
        already told us it doesn't support)
      - asset present, and timeframe support is unknown/unconfirmed (missing
        metadata, or `timeframe` not passed) -> ok=True — we do NOT block on
        incomplete information; the existing candle-fetch retry path is the
        source of truth in that case
      - asset present and timeframe confirmed supported (or no timeframe
        given) -> ok=True
    No fabrication in any branch.
    """
    try:
        fetcher = _run_bg(_get_shared_fetcher(), timeout=30.0)
    except Exception as exc:
        return {"ok": False, "status": 503,
                "error": f"Cannot verify asset '{asset}' — no active Quotex session ({exc})."}
    try:
        live_snapshot = _run_bg(live_assets.get_live_assets(fetcher), timeout=30.0)
    except Exception as exc:
        return {"ok": False, "status": 503,
                "error": f"Cannot verify asset '{asset}' — live asset discovery failed ({exc})."}
    if not live_snapshot:
        return {"ok": False, "status": 503,
                "error": f"Cannot verify asset '{asset}' — live asset discovery returned no data "
                         f"(Quotex session issue)."}
    asset_info = live_snapshot.get(asset)
    if asset_info is None:
        return {"ok": False, "status": 400,
                "error": f"Asset '{asset}' is not currently available from Quotex."}

    if timeframe:
        tf_secs = _TF_SECONDS_MAP.get(timeframe)
        tf_confirmed = bool(asset_info.get("available_timeframes_confirmed"))
        tf_list = asset_info.get("available_timeframes") or []
        # Only block when the broker's OWN data explicitly confirmed a
        # timeframe list AND the requested timeframe is not in it. Missing/
        # unconfirmed metadata (tf_confirmed False) is never treated as
        # proof of anything — the request is allowed through to the
        # existing retry-based candle fetch, unchanged.
        if tf_confirmed and tf_secs is not None and tf_secs not in tf_list:
            return {
                "ok": False, "status": 400,
                "kind": "unsupported_timeframe",
                "error": f"Asset '{asset}' does not support the '{timeframe}' timeframe on Quotex "
                         f"right now (broker-confirmed available timeframes: {tf_list}).",
            }

    return {"ok": True, "status": 200, "error": None}


# ── Closed-candle helper ───────────────────────────────────────────────────────
# Analysis (indicators/backtest/confluence/MTF) must only ever see fully
# CLOSED candles. Quotex's most-recently-returned candle for a timeframe is
# frequently still forming (its own period boundary hasn't passed yet), and
# its OHLC can keep changing on every poll — using it as the analysis candle
# would make repeated Analyze calls flip signal/entry-window data mid-candle
# for no market reason. This does not touch /api/diagnostic (which reads
# fetcher/transport state directly, never calls _run_pipeline()) and never
# fabricates a timestamp — it only ever drops a real, already-fetched row.
def _drop_forming_candle(df, timeframe: str):
    """Return df with its last row removed if that row is still forming
    (its period boundary has not yet passed). If df has only one row and
    that row is still forming, it is kept as-is rather than emptying the
    frame entirely — an analysis run on the best available (if slightly
    forming) data beats no data at all in that edge case."""
    if df is None or df.empty:
        return df
    tf_secs = _TF_SECONDS_MAP.get(timeframe)
    if not tf_secs:
        return df
    try:
        last_ts = df.index[-1]
        last_ts = last_ts.to_pydatetime() if hasattr(last_ts, "to_pydatetime") else last_ts
        if last_ts.tzinfo is None:
            # Quotex candle timestamps are epoch-seconds derived (UTC).
            last_ts = last_ts.replace(tzinfo=timezone.utc)
    except Exception:
        return df  # can't determine boundary — leave df untouched, never guess
    boundary = last_ts + timedelta(seconds=tf_secs)
    if boundary > datetime.now(timezone.utc) and len(df) > 1:
        return df.iloc[:-1]
    return df


# ── Timing helpers ────────────────────────────────────────────────────────────

def _ts() -> float:
    return time.monotonic()

def _log_step(label: str, t0: float, t_prev: float) -> float:
    now = _ts()
    print(f"  [PERF] {label:<40} step={now - t_prev:6.2f}s  total={now - t0:6.2f}s")
    return now


# ── Fix 3: single shared asyncio event loop running in a background thread ───
#
# Flask sync workers call asyncio.run_coroutine_threadsafe() to submit work to
# this loop.  Because all WebSocket I/O runs cooperatively inside one loop,
# only one /api/signal request is ever in flight at the asyncio level — no
# thread-level locking needed.  The loop also owns the persistent fetcher, so
# there is no per-request connect/disconnect overhead.

_BG_LOOP: asyncio.AbstractEventLoop | None = None
_BG_THREAD: threading.Thread | None = None
_BG_LOCK = threading.Lock()


def _get_bg_loop() -> asyncio.AbstractEventLoop:
    """Return the shared background asyncio loop, starting it if needed."""
    global _BG_LOOP, _BG_THREAD
    with _BG_LOCK:
        if _BG_LOOP is not None and _BG_LOOP.is_running():
            return _BG_LOOP
        _BG_LOOP = asyncio.new_event_loop()
        _BG_THREAD = threading.Thread(
            target=_BG_LOOP.run_forever,
            daemon=True,
            name="quotex-ws-loop",
        )
        _BG_THREAD.start()
    return _BG_LOOP


def _run_bg(coro, timeout: float = 110.0):
    """Submit a coroutine to the background loop; block until it finishes."""
    loop = _get_bg_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=timeout)


# ── Persistent shared fetcher (lives on _BG_LOOP) ────────────────────────────

_shared_fetcher: "QuotexDataFetcher | None" = None
# asyncio.Lock created lazily inside the bg loop to avoid event-loop binding
# issues at import time.
_fetcher_alock: asyncio.Lock | None = None


def _fetcher_lock() -> asyncio.Lock:
    global _fetcher_alock
    if _fetcher_alock is None:
        _fetcher_alock = asyncio.Lock()
    return _fetcher_alock


# Part 3 approved fix — Analyzer priority over Scanner.
# Single shared lock serializing the actual live candle-fetch call
# (_run_pipeline()) across BOTH the Scanner's per-asset/timeframe loop and
# manual /api/signal requests, so a Scanner fetch and a manual Analyzer
# fetch are never simultaneously in-flight on the shared Quotex session —
# closing the gap identified in Part 2 (the Scanner's prior yield-between-
# assets check didn't cover a manual request starting mid-fetch).
# Manual requests take priority: api_signal() marks
# _scanner.manual_request_started() BEFORE attempting to acquire this
# lock (see api_signal() below), and the Scanner checks that same flag
# both before AND immediately after acquiring the lock (see scanner.py's
# _acquire_live_data_lock_with_priority()), backing off if it's set so it
# never starts a new fetch while a manual request is active or waiting.
# Created lazily inside the bg loop, same pattern as _fetcher_lock().
# This does not change _run_pipeline() itself — it only wraps calls to it.
_live_data_alock: asyncio.Lock | None = None


def _live_data_lock() -> asyncio.Lock:
    global _live_data_alock
    if _live_data_alock is None:
        _live_data_alock = asyncio.Lock()
    return _live_data_alock


async def _run_pipeline_with_priority(asset: str, timeframe: str) -> Dict[str, Any]:
    """
    Thin wrapper used ONLY by manual /api/signal requests: acquires the
    shared live-data lock (see _live_data_lock() above) and then calls
    the existing, unmodified _run_pipeline() exactly as before. Exists
    so manual requests and the Scanner serialize through the same lock
    without altering _run_pipeline()'s own logic at all.
    """
    async with _live_data_lock():
        return await _run_pipeline(asset, timeframe)


def _fetcher_is_alive() -> bool:
    """Quick probe — no I/O, just check the client flag."""
    f = _shared_fetcher
    try:
        return (
            f is not None
            and f._client is not None
            and bool(getattr(f._client, "websocket_is_connected", False))
        )
    except Exception:
        return False


async def _get_shared_fetcher() -> "QuotexDataFetcher":
    """
    Return the persistent fetcher, (re-)connecting only when the WebSocket
    is not alive.  Must only be awaited from inside _BG_LOOP.
    NEVER places orders — connection is for data retrieval only.
    """
    global _shared_fetcher
    async with _fetcher_lock():
        if not _fetcher_is_alive():
            if _shared_fetcher is not None:
                try:
                    await _shared_fetcher.disconnect()
                except Exception:
                    pass
            print("[CONN] (Re-)connecting persistent fetcher …")
            t_c = _ts()
            _shared_fetcher = QuotexDataFetcher(asset=cfg.DEFAULT_ASSET, is_demo=cfg.IS_DEMO)
            await _shared_fetcher.connect()
            print(f"[CONN] Persistent fetcher ready — connect took {_ts() - t_c:.2f}s")
    return _shared_fetcher


def _invalidate_shared_fetcher() -> None:
    """
    Mark the persistent fetcher as dead so the next call reconnects, and
    explicitly close the outgoing WebSocket connection first (P0 fix —
    SSID/session replacement). Previously this only dropped the Python
    reference: the old fetcher's `disconnect()` was never called, so the
    prior SSID's authenticated WebSocket was left open/orphaned instead of
    being cleanly closed before the next session takes over.

    Disconnect is scheduled fire-and-forget on the shared background loop
    (never awaited here) so this stays safely callable both from a Flask
    request thread (sync) and from inside the background loop itself
    (e.g. scanner.py's own invalidate-on-failure path) without risking a
    deadlock from blocking the loop thread on its own future.
    """
    global _shared_fetcher
    old_fetcher = _shared_fetcher
    _shared_fetcher = None
    if old_fetcher is not None:
        async def _close_old_fetcher() -> None:
            try:
                await old_fetcher.disconnect()
            except Exception:
                pass
        try:
            asyncio.run_coroutine_threadsafe(_close_old_fetcher(), _get_bg_loop())
        except Exception:
            # Best-effort only — never let cleanup scheduling raise into
            # the caller (session update, scanner failure handling, etc.)
            pass


async def _get_live_asset_status(asset: str) -> str:
    """
    Shared, honest live-open/closed check — used by BOTH the Manual Analyzer
    (/api/signal, below) and the Auto Scanner (scanner.py's ScannerEngine,
    wired in via its optional check_asset_status= constructor callable) so
    an actionable BUY/SELL result gets the exact same freshness guarantee
    regardless of which path produced it. Never fabricates: any failure to
    reach Quotex or resolve the asset returns "unknown", never "open".
    Purely additive metadata — does not touch _run_pipeline(),
    generate_confluence_signal(), or any signal/confidence computation.
    """
    try:
        fetcher = await _get_shared_fetcher()
        live_now = await live_assets.get_live_assets(fetcher)
        info = (live_now or {}).get(asset)
        return "open" if (info and info.get("is_open")) else "closed" if info else "unknown"
    except Exception:
        return "unknown"  # never guess/fabricate on failure


async def _get_live_scanner_assets() -> List[str]:
    """
    Phase 15 — the SAME authoritative live snapshot + settings.enabled_assets
    narrowing api_scanner_start() applies at start time, reused here so
    ScannerEngine's cycle-boundary refresh (refresh_assets=) stays
    consistent with what a fresh scanner start would produce — one
    authoritative source, not a second independent asset list. Deliberately
    raises on any failure rather than swallowing it: scanner.py's own
    refresh_assets caller is responsible for deciding what happens on
    failure (keep the previous list, log it — see _scan_loop()), so this
    function never itself substitutes a static/fabricated list.
    """
    fetcher = await _get_shared_fetcher()
    live_otc = await live_assets.get_live_otc_assets(fetcher, force_refresh=True)
    live_symbols = set(live_otc.keys())
    saved_selection = None
    try:
        saved_selection = (settings_store.get().get("scanner", {}) or {}).get("enabled_assets") or None
    except Exception:
        saved_selection = None
    if saved_selection:
        return [a for a in saved_selection if a in live_symbols]
    return sorted(live_symbols)


async def _run_pipeline(asset: str, timeframe: str) -> Dict[str, Any]:
    """
    Full analysis-only pipeline for one asset/timeframe:
      connect -> fetch candles -> indicators (OTC-tuned if applicable)
      -> backtest factor accuracy -> dynamic weights -> confluence signal.
    NEVER places an order — read-only market data + local computation only.
    Runs on the shared background event loop; reuses the persistent fetcher.
    """
    t0 = t = _ts()
    print(f"\n[PERF] ── pipeline start  asset={asset}  tf={timeframe} ──")

    fetcher = await _get_shared_fetcher()
    t = _log_step("connect() [shared — reused or reconnected]", t0, t)

    try:

        df = await fetcher.get_candles_df(asset=asset, timeframe=timeframe, count=cfg.CANDLE_COUNT)
        t = _log_step(f"get_candles_df({timeframe})", t0, t)
        if df.empty:
            # Approved general fix — surface fetcher.last_fetch_diagnostics
            # instead of always returning the same generic string,
            # regardless of the ACTUAL reason (protocol error, timeout,
            # genuinely unavailable, empty-but-valid response, or a
            # connection/reconnect failure). "error" stays a plain string
            # (app.js renders it directly in several places, e.g. line
            # ~1051/3145 `${data.error}` / `.join('; ')`) so the existing
            # UI keeps working unchanged — its CONTENT is just more
            # specific now. "diagnostics" is a new, additive key with the
            # full structured object for anything that wants it
            # programmatically; nothing currently reads it, so this can't
            # break existing behavior. NEVER includes QUOTEX_SSID or any
            # credential — last_fetch_diagnostics never captures that.
            diag = getattr(fetcher, "last_fetch_diagnostics", None) or {}
            category = diag.get("failure_category")
            reason = diag.get("failure_reason") or (
                f"No candles received for {asset} [{timeframe}]. "
                "The asset may be closed or unavailable right now."
            )
            return {
                "error": reason,
                "failure_category": category,
                "diagnostics": diag,
            }

        # CLOSED CANDLE ONLY — drop the currently-forming candle before any
        # indicator/backtest/confluence computation touches df. See
        # _drop_forming_candle() docstring above (never empties a
        # single-row frame, so df here is guaranteed non-empty). Captured
        # BEFORE trimming so the API can honestly report the distinction
        # between what Quotex actually returned and what was usable —
        # requested vs. returned-raw vs. closed/usable are three different
        # numbers and must not be collapsed into one.
        _candles_returned_raw = len(df)
        df = _drop_forming_candle(df, timeframe)

        otc_settings = cfg.get_indicator_settings(asset)
        indicators = calculate_all(
            df,
            ema_periods=cfg.EMA_PERIODS,
            rsi_period=otc_settings["rsi_period"],
            macd_fast=cfg.MACD_FAST,
            macd_slow=cfg.MACD_SLOW,
            macd_signal=cfg.MACD_SIGNAL,
            bb_period=otc_settings["bb_period"],
            bb_std=otc_settings["bb_std"],
            atr_period=cfg.ATR_PERIOD,
            adx_period=cfg.ADX_PERIOD,
            stoch_period=cfg.STOCH_RSI_PERIOD,
            sr_lookback=cfg.SR_LOOKBACK,
            cci_period=otc_settings["cci_period"],
            stoch_k_period=otc_settings["stoch_k_period"],
            stoch_d_period=otc_settings["stoch_d_period"],
        )
        t = _log_step("calculate_all(indicators)", t0, t)

        # Task 3: Fetch payout % (best-effort — 0.0 = unavailable / closed)
        payout_pct = await fetcher.get_payout(asset, timeframe)
        t = _log_step("get_payout()", t0, t)

        legacy_signal = generate_signal(indicators)
        accuracies = backtest_factor_accuracy(df, indicators, lookahead=4)
        dynamic_weights = compute_dynamic_weights(accuracies)
        t = _log_step("backtest + dynamic_weights", t0, t)

        # Task 5: Use precomputed deep-backtest weights when available (more stable
        # cross-asset averages vs. per-run single-asset weights)
        _precomputed = load_precomputed_weights()
        if _precomputed:
            dynamic_weights = _precomputed

        # Phase 10.3 Part-1: Market Regime Detection + Adaptive Weight Engine +
        # Dynamic Indicator Selection — another scaling layer on top of the
        # already-computed dynamic_weights, same pattern as the Settings
        # layer just below (which still runs last and keeps final say over
        # enable/disable + explicit weight scale). Never touches analyzer.py,
        # backtest.py, or the confluence vote logic itself.
        if getattr(cfg, "ENABLE_REGIME_ADAPTIVE_WEIGHTS", True):
            regime_pack = compute_regime_adjusted_weights(dynamic_weights, indicators)
            dynamic_weights = regime_pack["final_weights"]
        else:
            regime_pack = {
                "regime": {"regime": "Unknown", "confidence": 0.0,
                           "reasons": ["Phase 10.3 regime engine disabled via config."]},
                "adaptive_weight_step": {"applied": False},
                "selection_step": {"applied": False, "primary": [], "low_relevance": []},
            }

        # Phase 7.4: apply Settings' indicator enable/disable + weight-scale
        # on top of the dynamic_weights computed above — see
        # _apply_settings_weight_overrides()'s docstring for why this is a
        # scaling layer rather than a direct replacement. No-op at defaults.
        _settings_snapshot = settings_store.get()
        dynamic_weights = _apply_settings_weight_overrides(dynamic_weights, _settings_snapshot)

        confluence = generate_confluence_signal(df, indicators, dynamic_weights)
        t = _log_step("confluence_signal", t0, t)

        # ── TASK 4: Multi-timeframe confirmation (auto-compare with 1m) ────────
        multi_tf_result: Dict[str, Any] | None = None
        compare_tf = "1m" if timeframe != "1m" else "5m"

        # Step 2 (transparency only): always-populated explicit status,
        # separate from `multi_tf_result` above. Does not affect signal,
        # confidence, or the WAIT-override logic below — it only reports on
        # what happened. Default assumes unavailable until proven otherwise.
        multi_tf_status: Dict[str, Any] = {
            "status": "UNAVAILABLE",
            "reason": "Multi-timeframe check did not run",
            "timeframe_checked": compare_tf,
        }
        try:
            df_cmp = await fetcher.get_candles_df(
                asset=asset, timeframe=compare_tf, count=cfg.CANDLE_COUNT
            )
            # CLOSED CANDLE ONLY — same rule as the primary timeframe above.
            df_cmp = _drop_forming_candle(df_cmp, compare_tf)
            if not df_cmp.empty:
                try:
                    ind_cmp = calculate_all(
                        df_cmp,
                        ema_periods=cfg.EMA_PERIODS,
                        rsi_period=otc_settings["rsi_period"],
                        macd_fast=cfg.MACD_FAST,
                        macd_slow=cfg.MACD_SLOW,
                        macd_signal=cfg.MACD_SIGNAL,
                        bb_period=otc_settings["bb_period"],
                        bb_std=otc_settings["bb_std"],
                        atr_period=cfg.ATR_PERIOD,
                        adx_period=cfg.ADX_PERIOD,
                        stoch_period=cfg.STOCH_RSI_PERIOD,
                        sr_lookback=cfg.SR_LOOKBACK,
                        cci_period=otc_settings["cci_period"],
                        stoch_k_period=otc_settings["stoch_k_period"],
                        stoch_d_period=otc_settings["stoch_d_period"],
                    )
                    acc_cmp = backtest_factor_accuracy(df_cmp, ind_cmp, lookahead=4)
                    dw_cmp = compute_dynamic_weights(acc_cmp)
                    conf_cmp = generate_confluence_signal(df_cmp, ind_cmp, dw_cmp)
                except Exception as calc_exc:
                    multi_tf_status = {
                        "status": "UNAVAILABLE",
                        "reason": f"Indicator calculation failed: {calc_exc}",
                        "timeframe_checked": compare_tf,
                    }
                    raise

                s_primary = confluence["signal"]
                s_compare = conf_cmp["signal"]

                # MTF is a CONFIDENCE MODIFIER, not a signal gate: the primary
                # timeframe's signal/direction is never changed here (no more
                # forcing WAIT on disagreement). Only the confidence value is
                # adjusted, and only when primary itself is not already WAIT.
                if s_primary != "WAIT" and s_compare != "WAIT" and s_primary == s_compare:
                    mt_status = "CONFIRMED"
                    # Same-direction confirmation: boost confidence by 10%, capped at 95%
                    confluence = dict(confluence, confidence=min(95, confluence["confidence"] + 10))
                elif s_primary == "WAIT" or s_compare == "WAIT":
                    mt_status = "PARTIAL"
                    # Comparison timeframe had no confirmation to offer —
                    # neutral: no bonus, no penalty, primary stands as-is.
                else:
                    mt_status = "CONFLICTING"
                    # Comparison timeframe disagrees: apply a moderate
                    # confidence penalty, but the primary signal/direction is
                    # preserved — it is never overridden to WAIT.
                    confluence = dict(confluence, confidence=max(0, confluence["confidence"] - 15))

                multi_tf_result = {
                    "status": mt_status,
                    "primary_tf": timeframe,
                    "primary_signal": s_primary,
                    "compare_tf": compare_tf,
                    "compare_signal": s_compare,
                }

                # Step 2: map the existing 3-way internal status onto the
                # explicit CONFIRMED / DISAGREED / UNAVAILABLE vocabulary,
                # purely for reporting — multi_tf_result above is untouched.
                # PARTIAL (one side was WAIT) is a neutral "no confirmation
                # available" outcome, not a genuine disagreement — CONFLICTING
                # is the only case that actually disagreed, so only that one
                # maps to DISAGREED. This does not change mt_status, the
                # confidence math, or the primary signal in any way.
                if mt_status == "CONFIRMED":
                    multi_tf_status = {
                        "status": "CONFIRMED",
                        "reason": "Higher timeframe signal agrees with primary timeframe",
                        "timeframe_checked": compare_tf,
                    }
                elif mt_status == "CONFLICTING":
                    multi_tf_status = {
                        "status": "DISAGREED",
                        "reason": "Higher timeframe signal conflict",
                        "timeframe_checked": compare_tf,
                    }
                else:  # PARTIAL — one side had insufficient confirmation
                    multi_tf_status = {
                        "status": "UNAVAILABLE",
                        "reason": "Higher timeframe did not provide confirmation (WAIT)",
                        "timeframe_checked": compare_tf,
                    }
            else:
                multi_tf_status = {
                    "status": "UNAVAILABLE",
                    "reason": "Comparison timeframe data unavailable",
                    "timeframe_checked": compare_tf,
                }
        except Exception as exc:
            # Multi-TF is best-effort; primary analysis always returned.
            # Only update the reason if the inner calc-failure handler above
            # did not already set a more specific one.
            if multi_tf_status.get("reason") in (
                "Multi-timeframe check did not run",
            ):
                multi_tf_status = {
                    "status": "UNAVAILABLE",
                    "reason": f"Exception during MTF analysis: {exc}",
                    "timeframe_checked": compare_tf,
                }
        t = _log_step(f"multi_tf get_candles_df({compare_tf})+analysis", t0, t)



        factors_out = []
        for key, label in _FACTOR_LABELS.items():
            vote = confluence["factors"].get(key, 0)
            acc = accuracies.get(key)
            factors_out.append({
                "key": key,
                "label": label,
                "vote": _VOTE_LABELS.get(vote, "NEUTRAL"),
                "weight": round(dynamic_weights.get(key, 0), 1),
                "accuracy": acc,
            })

        result = {
            "asset": asset,
            "timeframe": timeframe,
            "is_otc": "_otc" in asset.lower(),
            "price": indicators.get("price"),
            "candle_count": len(df),
            # Honest data-count transparency (see _drop_forming_candle()
            # comment above): "requested" is what we asked the fetcher for;
            # Quotex's own server decides how many it actually returns in a
            # single response (our client has no working way to request
            # more — see fetch_data.py/client.py for why), so
            # "candles_returned_raw" can legitimately be less than
            # requested; "candle_count" above is what's left after removing
            # the still-forming candle, i.e. the usable/closed count. These
            # are three different numbers and must not be conflated.
            "candles_requested": cfg.CANDLE_COUNT,
            "candles_returned_raw": _candles_returned_raw,
            "trend": indicators.get("direction"),
            "confluence": {
                "signal": confluence["signal"],
                "confidence": confluence["confidence"],
            },
            "legacy": {
                "signal": legacy_signal["signal"],
                "confidence": legacy_signal["confidence"],
            },
            "agree": confluence["signal"] == legacy_signal["signal"],
            "factors": factors_out,
            "indicators": {
                "rsi": indicators.get("rsi"),
                "macd_histogram": indicators.get("macd_histogram"),
                "cci": indicators.get("cci"),
                "adx": indicators.get("adx"),
                "candlestick_pattern": indicators.get("candlestick_pattern"),
                # Phase 5: new, additive — richer structure with direction/
                # strength/reliability. None for older cached data paths.
                "candlestick_pattern_detail": indicators.get("candlestick_pattern_detail"),
                "support": indicators.get("support"),
                "resistance": indicators.get("resistance"),
                # Phase 6: new, additive — richer zone-based structure. None
                # for older cached data paths.
                "support_resistance_detail": indicators.get("support_resistance_detail"),
                "atr_pct": indicators.get("atr_pct"),
                "volatility_level": indicators.get("level"),
                # EMA-200 filter fix: `calculate_all()` already computes this
                # (cfg.EMA_PERIODS includes 200) but it was never exposed to
                # the frontend, so the "EMA 200" filter pill had no real data
                # to use and was reusing the EMA 50 pill's trend-word check
                # instead. Smallest additive field — real EMA-200 value,
                # nothing derived/faked. Signal/confluence calculation is
                # untouched; this is a read-only reporting addition.
                "ema_200": indicators.get("ema_200"),
            },
            # Task 3: payout % (None = unavailable or market closed)
            "payout_pct": round(payout_pct, 1) if payout_pct and payout_pct > 0 else None,
            # Task 7: price history for frontend Chart.js chart (last 50 closes)
            "price_history": [round(float(x), 5) for x in df["close"].tail(50).tolist()],
            # TASK 4: multi-timeframe confirmation result (None if unavailable)
            "multi_tf": multi_tf_result,
            # Step 2: always-populated explicit status — CONFIRMED / DISAGREED /
            # UNAVAILABLE — purely additive, does not affect signal/confidence.
            "multi_tf_status": multi_tf_status,
            # Phase 10.3 Part-1: additive, read-only — market regime
            # classification + explainability. Does not affect signal,
            # confidence, or any existing field (the weight adjustment it
            # drove already happened above, before generate_confluence_signal).
            "regime": {
                "name": regime_pack["regime"]["regime"],
                "confidence": regime_pack["regime"]["confidence"],
                "reasons": regime_pack["regime"]["reasons"],
                "adaptive_weight_applied": regime_pack["adaptive_weight_step"]["applied"],
                "selection_applied": regime_pack["selection_step"]["applied"],
                "primary_indicators": regime_pack["selection_step"]["primary"],
                "low_relevance_indicators": regime_pack["selection_step"]["low_relevance"],
            },
        }

        # Phase 7: Filter Score — computed ONCE here (single source of truth),
        # reusing only fields already produced above. Does not affect
        # confidence, signal, or any existing field. scanner.py reads these
        # same fields straight from this result rather than recomputing them.
        # Phase 7.4: settings-driven overrides (adx_trending, atr_extreme_pct,
        # min_payout) — at settings.json's shipped defaults these are
        # identical to cfg.ADX_TRENDING / analyzer.VOLATILITY_EXTREME_ATR_PCT
        # / cfg.MIN_PAYOUT, so behavior is byte-identical until a user
        # actually edits Settings.
        _filter_overrides = settings_store.get_filter_score_config_overrides()
        filter_result = calculate_filter_score(
            indicators=indicators,
            signal_data={"multi_tf_status": multi_tf_status, "payout_pct": payout_pct},
            config=_filter_overrides,
        )
        result["filter_score"] = filter_result["filter_score"]
        result["mandatory_pass"] = filter_result["mandatory_pass"]
        result["passed_filters"] = filter_result["passed_filters"]
        result["failed_filters"] = filter_result["failed_filters"]
        result["filter_breakdown"] = filter_result["filter_breakdown"]

        # ── Part 2-4: signal actionable countdown metadata ──────────────────
        # Additive only — never used for order placement (analysis/scanner
        # only, no trade execution anywhere in this project). Gives the
        # frontend everything it needs to compute a live countdown to the
        # NEXT candle boundary for the selected timeframe, without the
        # frontend inventing its own timing logic or using page-load time.
        _tf_secs = _TF_SECONDS_MAP.get(timeframe)
        _now_utc = datetime.now(timezone.utc)
        result["generated_at"] = _now_utc.isoformat()
        result["timeframe_seconds"] = _tf_secs

        _candle_start = None
        try:
            if not df.empty:
                _last_ts = df.index[-1]
                _candle_start = _last_ts.to_pydatetime() if hasattr(_last_ts, "to_pydatetime") else _last_ts
                if _candle_start is not None and _candle_start.tzinfo is None:
                    # Quotex candle timestamps are epoch-seconds derived (UTC)
                    _candle_start = _candle_start.replace(tzinfo=timezone.utc)
        except Exception:
            _candle_start = None

        _next_boundary = None
        _entry_window_unavailable_reason = None
        # Max whole intervals we'll advance past the candle's own boundary
        # before treating the data as too stale to trust for a real expiry
        # projection. One interval covers the normal case (this closed
        # candle's own boundary has just passed, project to the next one).
        # A small allowance beyond that covers an unusually slow
        # request/fetch round-trip. Anything beyond this is no longer
        # "the actual candle timing", it's guessing how many intervals
        # might have silently gone by — which CRITICAL CORRECTION #2
        # explicitly forbids treating as a trustworthy real boundary.
        _MAX_TRUSTWORTHY_ADVANCE_INTERVALS = 3
        if _tf_secs and _candle_start is not None:
            # Preferred source: actual latest CLOSED candle's start time.
            # BUG FIX (proven root cause of "Signal expired" appearing
            # immediately after a fresh analysis): df's last row is now
            # always the latest CLOSED candle (see _drop_forming_candle()
            # above) — its own boundary (_candle_start + one interval) has,
            # by definition, already passed by the time this code runs, so
            # using it directly as entry_window_end produced a timestamp
            # already in the past. The actionable entry window is really
            # "until the CURRENTLY-FORMING candle closes" — i.e. the next
            # boundary strictly AFTER now, not the boundary that already
            # closed. Advancing by whole timeframe intervals until the
            # result is genuinely in the future keeps this 100% derived
            # from the real candle timestamp (never fabricated) — but only
            # up to _MAX_TRUSTWORTHY_ADVANCE_INTERVALS; beyond that we no
            # longer have sufficiently current/trustworthy candle data to
            # honestly claim a real broker boundary (CORRECTION #2), so we
            # report unavailable instead of extrapolating indefinitely.
            _candidate = _candle_start + timedelta(seconds=_tf_secs)
            _advances = 1
            while _candidate <= _now_utc:
                if _advances >= _MAX_TRUSTWORTHY_ADVANCE_INTERVALS:
                    _candidate = None
                    _entry_window_unavailable_reason = "stale_candle_timing"
                    break
                _candidate += timedelta(seconds=_tf_secs)
                _advances += 1
            _next_boundary = _candidate
        # CORRECTION #1: no current-time-grid fallback. If the candle
        # timestamp itself is unavailable, entry_window_end must be None
        # and entry_window_source must be "unavailable" — never a boundary
        # derived from datetime.now() / page-load time / request time.
        # There is no other source of truth for a real broker candle
        # boundary than the actual candle timestamp; without it, honesty
        # requires reporting "unavailable", not guessing.

        result["candle_start"] = _candle_start.isoformat() if _candle_start else None
        result["entry_window_end"] = _next_boundary.isoformat() if _next_boundary else None
        if _next_boundary is not None:
            result["entry_window_source"] = "candle_timestamp"
            result["entry_window_unavailable_reason"] = None
        else:
            result["entry_window_source"] = "unavailable"
            # Honest, additive diagnostic — why entry_window_end is None:
            # "stale_candle_timing" (candle too old to trust, CORRECTION #2),
            # "no_candle_timestamp" (df empty/unparsable), or
            # "unknown_timeframe" (timeframe not in _TF_SECONDS_MAP at all).
            # Never affects signal generation; purely for transparency.
            if not _tf_secs:
                result["entry_window_unavailable_reason"] = "unknown_timeframe"
            elif _entry_window_unavailable_reason:
                result["entry_window_unavailable_reason"] = _entry_window_unavailable_reason
            else:
                result["entry_window_unavailable_reason"] = "no_candle_timestamp"

        print(f"[PERF] ── pipeline done  total={_ts() - t0:.2f}s ──\n")
        return result
    except (ConnectionError, OSError, RuntimeError) as e:
        # Connection died mid-request — invalidate so the next call reconnects.
        _invalidate_shared_fetcher()
        raise


# ─── Phase 7.4: Settings Store ──────────────────────────────────────────────
# Fixed path, gitignored (see repo-root .gitignore's Security section).
# One store instance per process — every read/write goes straight to disk
# (see settings_store.py docstring), so this is safe under Gunicorn workers.
# NOTE: moved above the Smart Scanner block (Phase 8.1) so `settings_store`
# exists in time to be passed into ScannerEngine's now-optional
# `settings_store=` constructor arg below. Nothing about SettingsStore
# itself changed — only its position in this file.
_SETTINGS_PATH = str(_WEBAPP_DIR / "settings.json")
settings_store = SettingsStore(_SETTINGS_PATH)


# ─── Phase 7: Smart Scanner ────────────────────────────────────────────────
# Pure orchestration around _run_pipeline() above — reused exactly, never
# duplicated. Runs as one coroutine on the SAME shared _BG_LOOP, so it never
# introduces a concurrent Quotex fetch alongside a manual /api/signal call.
# Phase 8.1: `settings_store=settings_store` is the only change here — it's
# read defensively inside ScannerEngine (see scanner.py's
# _read_scanner_settings()), so this remains safe even if settings.json is
# ever missing/corrupt.
_scanner = ScannerEngine(
    run_pipeline=_run_pipeline,
    assets=_asset_choices()["otc"],
    invalidate_fetcher=_invalidate_shared_fetcher,
    adx_trending=cfg.ADX_TRENDING,
    config=ScannerConfig(min_payout=cfg.MIN_PAYOUT),
    settings_store=settings_store,
    # Part 3 approved fix — Analyzer priority over Scanner. Passing the
    # lock GETTER (not the lock itself) mirrors _get_shared_fetcher()'s
    # own lazy-creation pattern, so the Lock is only ever constructed
    # from inside the running _BG_LOOP, never at import time.
    live_data_lock=_live_data_lock,
    # Phase 15 (Part 1) — same live-open/closed check Manual Analyzer's
    # /api/signal route applies to its own actionable results (see
    # _get_live_asset_status() above). Passing the coroutine function
    # itself (not calling it) — scanner.py awaits it directly since it
    # already runs on the same shared background loop.
    check_asset_status=_get_live_asset_status,
    # Phase 15 — cycle-boundary refresh (see ScannerEngine._scan_loop()):
    # reuses the exact same authoritative live-snapshot helper
    # /api/scanner/start already uses, so a long-running scan re-syncs
    # against Quotex's current live availability once per cycle rather
    # than only ever using the snapshot taken at start() time.
    refresh_assets=_get_live_scanner_assets,
)


# ─── Phase 7.4: Backtest Engine ─────────────────────────────────────────────
# fetch_candles wraps the SAME shared fetcher machinery _run_pipeline() uses
# — this does not open a second Quotex connection path.
async def _backtest_fetch_candles(asset: str, timeframe: str, count: int):
    fetcher = await _get_shared_fetcher()
    df = await fetcher.get_candles_df(asset=asset, timeframe=timeframe, count=count)
    # CLOSED CANDLE ONLY — same rule _run_pipeline() applies to primary/MTF
    # analysis (see _drop_forming_candle() above). Previously this backtest
    # fetch path did not apply it, so candles_fetched/candle_count_met in
    # backtest_engine.py could include a still-forming candle, inconsistent
    # with the closed-candle guarantee everywhere else. This can only ever
    # remove at most the single already-forming last row (same guarantees:
    # never empties a one-row frame, never touches df on an unknown
    # timeframe or unparsable timestamp) — it does not change what counts
    # as a "requested" or "returned" candle, only what counts as "usable".
    candles_returned_raw = len(df)
    df = _drop_forming_candle(df, timeframe)
    # Diagnostics only — never load-bearing. df.attrs is a best-effort
    # pandas side-channel; if it's ever unavailable for any reason,
    # backtest_engine.py falls back to len(df) and the requested count,
    # which is exactly what it already did before this change.
    try:
        df.attrs["candles_requested"] = count
        df.attrs["candles_returned_raw"] = candles_returned_raw
    except Exception:
        pass
    return df


_backtest_engine = BacktestEngine(fetch_candles=_backtest_fetch_candles)


# ─── Phase 8.4.3: Validation Engine ─────────────────────────────────────────
# Reuses the SAME _backtest_fetch_candles() closure defined immediately
# above — no new fetch/session logic, no second Quotex connection path.
# Thin wrapper (validation_engine.py) around indicator_validation.py's
# existing validate_indicator() (Phase 8.4.2, unmodified). Runs on the SAME
# shared _BG_LOOP as every other engine in this file.
_validation_engine = ValidationEngine(fetch_candles=_backtest_fetch_candles)


# ─── Phase 8.5: Validation History Store ────────────────────────────────────
# Fixed path, gitignored (same convention as settings.json). Auto-creates
# on first use — see validation_history_store.py's own docstring for the
# full I/O contract.
_VALIDATION_HISTORY_PATH = str(_WEBAPP_DIR / "validation_history.json")
_validation_history_store = ValidationHistoryStore(_VALIDATION_HISTORY_PATH)

# ─── Phase 9: Smart Learning History Store ──────────────────────────────────
# Fixed path, gitignored (same convention as settings.json/
# validation_history.json). Auto-creates on first use. Stores only this
# module's own recommendation snapshots — never validation_history.json's
# data, which stays exclusively owned by _validation_history_store above.
_LEARNING_HISTORY_PATH = str(_WEBAPP_DIR / "learning_history.json")
_learning_history_store = LearningHistoryStore(_LEARNING_HISTORY_PATH)

# ─── Phase 10.4 Goal 3: AI Health History Store ─────────────────────────────
# Fixed path, gitignored (same convention as the two stores above).
# Auto-creates on first use. Stores only rolling compute_ai_health()
# snapshots — never validation_history.json's or learning_history.json's
# data, which stay exclusively owned by the stores above.
_AI_HEALTH_HISTORY_PATH = str(_WEBAPP_DIR / "ai_health_history.json")
_ai_health_history_store = AIHealthHistoryStore(_AI_HEALTH_HISTORY_PATH)

# Throttle so polling /api/ai/history/* can't flood the snapshot log —
# same "quiet persistence" spirit as _maybe_persist_validation_history()
# below, applied to a fixed wall-clock interval instead of a run-completion
# event (health can be sampled at any time, unlike a validation run).
_AI_HEALTH_SNAPSHOT_MIN_INTERVAL_SECONDS = 900  # 15 minutes

# In-memory guard (this process only) against re-persisting the same
# completed run on every poll — the store's own record_run() ALSO
# deduplicates by timestamp internally, so this is purely a cheap
# short-circuit to skip a redundant file read/write, not a correctness
# requirement.
_last_persisted_validation_run_at: Optional[str] = None


def _maybe_persist_validation_history() -> None:
    """
    Phase 8.5 persistence strategy: app.py-polling-driven ONLY — no
    callback or hook was added inside validation_engine.py (explicit
    decision, see PROJECT_STATUS.md/NEXT_PHASE.md). This function is
    called opportunistically from the existing /api/validation/status
    route (already polled by the UI every ~2s while a run is active) and
    from the new /api/validation/history* routes — whichever happens to
    be hit first after a run completes is the one that persists it.
    Deliberately does nothing until ValidationEngine reports STOPPED with
    a `finished_at` timestamp — never reads mid-run state, never
    interferes with an active run.
    """
    global _last_persisted_validation_run_at
    status = _validation_engine.status()
    if status.get("state") != _VAL_STOPPED:
        return
    finished_at = status.get("finished_at")
    if not finished_at or finished_at == _last_persisted_validation_run_at:
        return
    results = _validation_engine.get_results()
    summary = results.get("summary")
    if summary is not None:
        _validation_history_store.record_run(summary, timestamp=finished_at)
    # Phase 10.2 — Asset Intelligence + Timeframe Intelligence. Additive,
    # independent call: record_run() (above, unmodified) persists the
    # global per-indicator rolling stats from `summary`; this persists
    # the same run's per-asset and per-timeframe breakdown from the RAW
    # per-combination `results["results"]` dict (previously computed and
    # then discarded once `summary` was built — now also persisted).
    # Same short-circuit-by-timestamp guard above covers both calls.
    raw_results = results.get("results")
    if raw_results:
        _validation_history_store.record_asset_timeframe_stats(raw_results, timestamp=finished_at)
    _last_persisted_validation_run_at = finished_at


# ─── Phase 7.4: Quotex session.json path (session mgmt routes, below) ──────
# Unchanged from the pre-existing session-loading convention in
# quotex/api_quotex — QUOTEX_SSID env var still takes priority when set;
# this path is only where the UI-driven "Update SSID" action writes to.
_SESSION_JSON_PATH = _QUOTEX_DIR / "session.json"


def _apply_settings_weight_overrides(dynamic_weights: Dict[str, float],
                                     settings: Dict[str, Any]) -> Dict[str, float]:
    """
    Phase 7.4 — layers Settings' indicator enable/disable + weight-scale on
    top of the ALREADY-COMPUTED per-run dynamic_weights (live backtest
    accuracy, or precomputed weights — untouched, computed exactly as
    before this function is called).

    Deliberate design choice (see PROJECT_MEMORY.md / IMPLEMENTATION_REPORT):
    NEXT_PHASE.md's literal instruction was to pass
    settings_store.get_effective_dynamic_weights() directly as
    generate_confluence_signal()'s dynamic_weights argument. That would
    silently DISCARD the existing live/precomputed dynamic-weight
    computation on every request (replacing it with settings' flat
    per-indicator weights), which conflicts with two harder constraints
    from this project's rules: (1) never modify the Dynamic Weight
    algorithm, and (2) default settings.json / no settings.json must be
    byte-identical to current behavior. A blind replace is NOT
    byte-identical, because the live-computed dynamic_weights are almost
    never uniform 10.0-each in practice.

    Instead: each factor's computed weight is scaled by
    (settings_weight / 10.0) — a pure multiplier, 1.0 for every factor at
    settings' shipped defaults (all weights default to 10.0), which makes
    this a no-op when settings are untouched. Disabled indicators are
    forced to weight 0 regardless of scale, matching the settings_store
    docstring's documented "disabled = weight 0" contract exactly. If
    settings.normalize_weights is on, the result is rescaled to sum to
    100 (same invariant DEFAULT_CONFLUENCE_WEIGHTS already guarantees).
    Never touches analyzer.py, DEFAULT_CONFLUENCE_WEIGHTS, or the vote
    logic itself.
    """
    indicators_cfg = settings.get("indicators", {})
    out: Dict[str, float] = {}
    for key, base_weight in dynamic_weights.items():
        ind_cfg = indicators_cfg.get(key)
        if ind_cfg is None:
            out[key] = base_weight
            continue
        if not ind_cfg.get("enabled", True):
            out[key] = 0.0
            continue
        scale = float(ind_cfg.get("weight", 10.0)) / 10.0
        out[key] = base_weight * scale

    if settings.get("backtest", {}).get("normalize_weights", True):
        total = sum(out.values())
        if total > 0:
            out = {k: round(v * 100.0 / total, 4) for k, v in out.items()}
            # Same rounding-drift fix as settings_store.get_effective_dynamic_weights():
            # assign the residual to the largest weight so the sum is exactly 100.
            drift = round(100.0 - sum(out.values()), 4)
            if drift != 0.0:
                top_key = max(out, key=out.get)
                out[top_key] = round(out[top_key] + drift, 4)
    return out


# ─── Routes ─────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    choices = _asset_choices()
    # B1 fix: pass the full indicator registry (id + display name, 13 total)
    # to the template so the Validation page can build its indicator picker
    # and summary cards dynamically instead of a hardcoded 3-indicator list.
    # Read-only use of the existing, unmodified indicator_registry.get_registry().
    validation_indicators = [
        {"id": e["id"], "name": e["name"]} for e in ind_registry.get_registry()
    ]
    return render_template(
        "index.html",
        otc_assets=choices["otc"],
        live_assets=choices["live"],
        timeframes=TIMEFRAMES,
        default_asset=cfg.DEFAULT_ASSET,
        default_timeframe=cfg.PRIMARY_TIMEFRAME,
        is_demo=cfg.IS_DEMO,
        validation_indicators=validation_indicators,
    )


@app.route("/api/signal", methods=["POST"])
def api_signal():
    """
    Run the analysis pipeline for the requested asset/timeframe and return
    the confluence + legacy signal as JSON. Analysis only — no order is
    placed by this endpoint or anything it calls.
    """
    data = request.get_json(silent=True) or {}
    asset = str(data.get("asset", cfg.DEFAULT_ASSET))
    timeframe = str(data.get("timeframe", cfg.PRIMARY_TIMEFRAME))

    if timeframe not in TIMEFRAMES:
        return jsonify({"error": f"Invalid timeframe '{timeframe}'. Must be one of {TIMEFRAMES}."}), 400
    # P0 fix — Unknown Asset: gate against the live Quotex snapshot, not the
    # stale static ASSETS dict (see _check_live_asset() docstring). Also
    # passes `timeframe` so a broker-confirmed unsupported combination fails
    # fast instead of burning candle-fetch retries.
    asset_check = _check_live_asset(asset, timeframe)
    if not asset_check["ok"]:
        return jsonify({"error": asset_check["error"], "kind": asset_check.get("kind")}), asset_check["status"]

    try:
        # Fix 3: submit to the shared background loop so the persistent
        # WebSocket connection is reused across requests.
        # Phase 7: mark this as an in-flight manual request so the scanner
        # (if running) yields between its own asset steps until this finishes.
        _scanner.manual_request_started()
        try:
            # Part 3 approved fix: goes through _run_pipeline_with_priority()
            # (shared live-data lock) instead of calling _run_pipeline()
            # directly, so this manual request and any in-progress/upcoming
            # Scanner fetch are never simultaneously in-flight. _run_pipeline()
            # itself is unchanged; only how it's invoked here changed.
            result = _run_bg(_run_pipeline_with_priority(asset, timeframe))
        finally:
            _scanner.manual_request_finished()
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 500
    except ValueError as e:
        return jsonify({"error": str(e)}), 500
    except ConnectionError as e:
        # TASK 5: clearer session-expiry guidance for the user
        err_str = str(e).replace("\n", " ")
        if "session" in err_str.lower() or "failed to connect" in err_str.lower() or "ssid" in err_str.lower():
            user_msg = (
                "Session expired or connection failed. "
                "To renew: open browser DevTools (F12) → Network tab → "
                "WS filter → connect to quotex.io → copy 'ssid' from WebSocket URL → "
                "update quotex/session.json. "
                "Or re-run: bash quotex/run_login.sh"
            )
        else:
            user_msg = f"Connection error: {err_str}"
        return jsonify({"error": user_msg}), 502
    except Exception as e:  # noqa: BLE001 — surface unexpected errors to the UI
        return jsonify({"error": f"Analysis failed: {e}"}), 500

    if "error" in result:
        return jsonify(result), 502

    # Phase 15 (Part 1) — Manual Analyzer safety foundation: re-verify
    # current live open/closed state right before returning an actionable
    # result. This is purely additive metadata (result["asset_status"]) —
    # _run_pipeline()/generate_confluence_signal()/MTF are completely
    # untouched, and the signal/confidence/direction already computed are
    # never altered here. The frontend is responsible for using this field
    # to withhold an actionable TRADE presentation for a closed asset
    # (Part 2) — this endpoint only supplies the honest, freshly-checked
    # fact, never fabricates or guesses it. Uses the SAME helper the
    # scanner now uses (see _get_live_asset_status() above and
    # ScannerEngine's check_asset_status= wiring below), so Manual Analyzer
    # and Auto Scanner apply identical live-freshness logic.
    sig = (result.get("confluence") or {}).get("signal")
    if sig in ("BUY", "SELL"):
        try:
            result["asset_status"] = _run_bg(_get_live_asset_status(asset), timeout=10.0)
        except Exception:
            result["asset_status"] = "unknown"  # never guess/fabricate on failure
    return jsonify(result)



@app.route("/api/diagnostic", methods=["GET"])
def api_diagnostic():
    """Read-only backend candle diagnostic. Never places trades."""
    asset = (request.args.get("asset") or getattr(cfg, "DEFAULT_ASSET", "EURUSD_otc")).strip()
    timeframe = (request.args.get("timeframe") or getattr(cfg, "PRIMARY_TIMEFRAME", "1m")).strip()
    try:
        count = int(request.args.get("count") or getattr(cfg, "CANDLE_COUNT", 50))
    except (TypeError, ValueError):
        count = getattr(cfg, "CANDLE_COUNT", 50)
    count = max(1, min(count, 500))

    started = time.time()
    stages = []

    def add_stage(name, status="ok", detail=None):
        item = {"stage": name, "status": status}
        if detail is not None:
            item["detail"] = detail
        stages.append(item)
        print(f"[DIAGNOSTIC] {name} status={status}"
              + (f" detail={detail}" if detail is not None else ""), flush=True)

    async def diagnose():
        add_stage("request_received", detail={
            "asset": asset, "timeframe": timeframe, "count": count
        })

        try:
            fetcher = await _get_shared_fetcher()
            connected = _fetcher_is_alive()
            add_stage("quotex_connection", "ok" if connected else "error", {
                "connected": connected,
                "session": "available" if fetcher is not None else "missing",
            })

            add_stage("candle_fetch_started", detail={
                "asset": asset, "timeframe": timeframe, "count": count
            })

            # Capture transport metadata around this exact request.
            transport_started_at = time.time()
            transport_before = fetcher.get_candle_transport_diagnostics()

            try:
                df = await fetcher.get_candles_df(
                    asset=asset, timeframe=timeframe, count=count
                )
            except Exception as exc:
                add_stage("candle_fetch", "error", {
                    "exception_type": type(exc).__name__,
                    "message": str(exc),
                    "fetch_diagnostics": dict(
                        getattr(fetcher, "last_fetch_diagnostics", {}) or {}
                    ),
                    "traceback": traceback.format_exc(limit=12),
                })
                return

            diag = dict(getattr(fetcher, "last_fetch_diagnostics", {}) or {})
            rows = int(len(df)) if df is not None else 0

            transport_after = fetcher.get_candle_transport_diagnostics()
            trace = list(transport_after.get("recent_trace") or [])
            transport_events = [
                e for e in trace
                if isinstance(e, dict) and float(e.get("ts", 0) or 0) >= transport_started_at - 0.25
            ]
            unmatched = fetcher.get_recent_unmatched_candle_responses(transport_started_at - 0.25)
            uncorrelated = fetcher.get_recent_uncorrelated_candle_errors(transport_started_at - 0.25)
            sent = [e for e in transport_events if e.get("direction") == "send"]
            received = [e for e in transport_events if e.get("direction") == "recv"]
            history_received = [e for e in received if e.get("event") in (
                "history/list", "history/list/v2", "chart_notification/get", "loadHistoryPeriod", "candles_received"
            )]
            transport = {
                "events": transport_events[-50:],
                "event_count": len(transport_events),
                "pending_binary_events_before": transport_before.get("pending_binary_events", []),
                "pending_binary_events_after": transport_after.get("pending_binary_events", []),
                "unmatched_candle_responses": unmatched[-20:],
                "uncorrelated_candle_errors": uncorrelated[-20:],
                "interpretation": {
                    "request_send_events": len(sent),
                    "receive_events": len(received),
                    "history_or_candle_events": len(history_received),
                    "server_frame_observed": bool(received),
                    "history_frame_observed": bool(history_received),
                    "parser_or_correlation_issue_suspected": bool(history_received) and rows == 0,
                    "transport_no_response_suspected": not received and rows == 0,
                    "unmatched_response_observed": bool(unmatched),
                },
            }
            diag["transport"] = transport
            add_stage("transport_diagnostic", "ok", transport)

            if rows:
                add_stage("candle_fetch", "ok", {
                    "rows": rows,
                    "diagnostics": diag,
                    "columns": list(df.columns),
                })
                add_stage("analysis", "ready", {
                    "rows": rows,
                    "message": "Candles reached the analysis stage."
                })
            else:
                add_stage("candle_fetch", "empty", {
                    "rows": 0, "diagnostics": diag
                })
                add_stage("analysis", "skipped",
                          "No candles received; indicators were not run.")

        except Exception as exc:
            add_stage("diagnostic_exception", "error", {
                "exception_type": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(limit=12),
            })

    try:
        _run_bg(diagnose(), timeout=125.0)
    except Exception as exc:
        add_stage("request_runner", "error", {
            "exception_type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(limit=12),
        })

    return jsonify({
        "ok": not any(s.get("status") == "error" for s in stages),
        "read_only": True,
        "asset": asset,
        "timeframe": timeframe,
        "elapsed_seconds": round(time.time() - started, 3),
        "stages": stages,
        "note": "Diagnostic only. No order/trade action is performed.",
    })

@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


# ── Task 7: Live prices — polling-based (no persistent WebSocket to browser) ──

_LIVE_PRICE_CACHE: dict = {"prices": {}, "ts": 0.0}
_TOP_OTC = ["EURUSD_otc", "GBPUSD_otc", "USDJPY_otc", "AUDUSD_otc", "USDCAD_otc", "EURJPY_otc"]

# Phase 3 / Part 3 approved fix: bounded concurrency for per-asset candle
# fetches below. AsyncQuotexClient._request_candles() keys in-flight
# requests by "{asset}_{timeframe}", and _on_candles_received() (Part 3
# fix) now ONLY ever resolves a request from a response exactly matching
# its own (asset, period) — never from an unrelated pending request — so
# concurrent fetches for different assets on the same connection are safe.
# The semaphore just caps how many fetches run at once, to stay a
# well-behaved client of the upstream server rather than firing all assets
# simultaneously.
_LIVE_PRICES_MAX_CONCURRENCY = 5


async def _fetch_current_prices(assets: list) -> dict:
    """
    Pull the latest 1-minute candle for each asset concurrently (bounded by
    a semaphore), using the existing persistent shared fetcher/session
    (Part 3 approved fix) instead of opening a second, independent Quotex
    connection. Returns { asset_sym: { price: float, change_pct: float } }.
    NEVER places orders — read-only candle data only. Never connects or
    disconnects the shared fetcher itself — that lifecycle belongs solely
    to _get_shared_fetcher()/_invalidate_shared_fetcher(); this function
    only borrows the existing connection.
    """
    fetcher = await _get_shared_fetcher()
    result: dict = {}
    semaphore = asyncio.Semaphore(_LIVE_PRICES_MAX_CONCURRENCY)

    async def _fetch_one(a: str) -> None:
        async with semaphore:
            try:
                df = await fetcher.get_candles_df(asset=a, timeframe="1m", count=3)
                if not df.empty and len(df) >= 2:
                    close = float(df["close"].iloc[-1])
                    prev  = float(df["close"].iloc[-2])
                    chg   = round((close - prev) / prev * 100, 3) if prev else 0.0
                    result[a] = {"price": close, "change_pct": chg}
            except Exception:
                pass  # Skip individual asset errors — each asset writes to its own key

    await asyncio.gather(*(_fetch_one(a) for a in assets))
    return result


@app.route("/api/live-prices")
def api_live_prices():
    """
    Return current prices for the top OTC pairs.
    Server-side 30-second cache avoids hammering Quotex with new connections.
    Frontend polls this endpoint every 15 s for the Markets tab.
    Analysis-only — no order placement.
    """
    import time
    now   = time.time()
    cache = _LIVE_PRICE_CACHE

    # Return cached data if it is fresh enough
    if now - cache["ts"] < 30 and cache["prices"]:
        return jsonify({"prices": cache["prices"], "cached": True})

    try:
        prices = _run_bg(_fetch_current_prices(_TOP_OTC))
        cache["prices"] = prices
        cache["ts"]     = now
        return jsonify({"prices": prices, "cached": False})
    except Exception as exc:
        # Serve stale cache rather than an empty error on transient failures
        if cache["prices"]:
            return jsonify({"prices": cache["prices"], "cached": True, "stale": True})
        return jsonify({"error": f"Price fetch failed: {exc}"}), 500


@app.route("/api/assets/live", methods=["GET"])
def api_assets_live():
    """
    Live Quotex Asset Availability (Step 3) — single source of truth for
    which OTC assets are currently tradeable right now. Thin read-only
    wrapper around live_assets.py, which itself wraps the EXISTING shared
    fetcher/session (_get_shared_fetcher(), same connection every other
    route already uses) — no second Quotex login, no new connection logic.

    api_quotex/constants.py's static ASSETS dict (imported below as
    `known_universe`) is exposed ONLY as a reference/known count for
    comparison — it is never substituted for live availability, and never
    used as a fallback when the live call fails.

    Query params:
      ?refresh=1   bypass live_assets.py's short in-process cache and force
                   a fresh read from the live session.

    On any session/connection failure, returns success=false with a clear
    reason and zero counts — never a fabricated/static asset list.
    """
    force_refresh = request.args.get("refresh", "").strip().lower() in ("1", "true", "yes")
    known_universe = list(ASSETS.keys())

    try:
        fetcher = _run_bg(_get_shared_fetcher(), timeout=30.0)
    except Exception as exc:
        return jsonify({
            "success": False,
            "error": f"No active Quotex session — {exc}",
            "known_otc_count": sum(1 for s in known_universe if s.lower().endswith("_otc")),
            "live_otc_total_count": 0,
            "available_otc_count": 0,
            "available_otc_assets": [],
            "entries": [],
        }), 503

    try:
        live = _run_bg(live_assets.get_live_assets(fetcher, force_refresh=force_refresh), timeout=30.0)
    except Exception as exc:
        return jsonify({
            "success": False,
            "error": f"Failed to fetch live asset list from Quotex — {exc}",
            "known_otc_count": sum(1 for s in known_universe if s.lower().endswith("_otc")),
            "live_otc_total_count": 0,
            "available_otc_count": 0,
            "available_otc_assets": [],
            "entries": [],
        }), 503

    if not live:
        return jsonify({
            "success": False,
            "error": "Quotex returned no asset data (empty snapshot).",
            "known_otc_count": sum(1 for s in known_universe if s.lower().endswith("_otc")),
            "live_otc_total_count": 0,
            "available_otc_count": 0,
            "available_otc_assets": [],
            "entries": [],
        }), 503

    snapshot = live_assets.build_snapshot(known_universe=known_universe, live_assets=live)
    return jsonify({
        "success": True,
        "discovered_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        **snapshot,
    })


@app.route("/api/assets/live/catalog", methods=["GET"])
def api_assets_live_catalog():
    """
    Phase 15 (Part 1) — unified live asset catalog, ACROSS ALL categories
    Quotex's live session returns (not just OTC). New, additive endpoint —
    does not touch /api/assets/live's existing contract or behavior, so
    nothing that currently depends on that route can regress. Same shared
    fetcher/session as every other route (no second Quotex login).

    Query params:
      ?refresh=1   bypass live_assets.py's short in-process cache.

    On any session/connection failure, returns success=false with zero
    counts — never a fabricated/static catalog.
    """
    force_refresh = request.args.get("refresh", "").strip().lower() in ("1", "true", "yes")

    try:
        fetcher = _run_bg(_get_shared_fetcher(), timeout=30.0)
    except Exception as exc:
        return jsonify({
            "success": False, "error": f"No active Quotex session — {exc}",
            "total_available": 0, "category_counts": {}, "assets": [],
        }), 503

    try:
        live = _run_bg(live_assets.get_live_assets(fetcher, force_refresh=force_refresh), timeout=30.0)
    except Exception as exc:
        return jsonify({
            "success": False, "error": f"Failed to fetch live asset list from Quotex — {exc}",
            "total_available": 0, "category_counts": {}, "assets": [],
        }), 503

    if not live:
        return jsonify({
            "success": False, "error": "Quotex returned no asset data (empty snapshot).",
            "total_available": 0, "category_counts": {}, "assets": [],
        }), 503

    catalog = live_assets.build_catalog_snapshot(live_assets=live)
    return jsonify({
        "success": True,
        "discovered_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        **catalog,
    })


@app.route("/api/scanner/status")
def api_scanner_status():
    """Scanner lifecycle state, metrics, history, and recent events."""
    return jsonify(_scanner.get_status())


@app.route("/api/scanner/results")
def api_scanner_results():
    """
    Ranked top signals from the scanner's cache. Only assets that passed
    ALL hard gates and have a non-WAIT signal are included — no partial
    credit scoring, per the approved architecture.
    """
    min_confidence = request.args.get("min_confidence", type=float)
    timeframe = request.args.get("timeframe", type=str)
    limit = request.args.get("limit", type=int)
    return jsonify(_scanner.get_results(min_confidence=min_confidence, timeframe=timeframe, limit=limit))


@app.route("/api/scanner/recommendations")
def api_scanner_recommendations():
    """
    Phase 12 — one dynamically-chosen best timeframe per asset, derived
    entirely from data the scanner already computes every cycle for every
    (asset, timeframe) it scans (no new scan path, no second signal
    engine). See ScannerEngine.get_recommendations() for the exact
    TRADE/WATCH/NOT_TRADE derivation — it only reuses existing fields
    (mandatory_pass, confidence, filter_score, payout) and the scanner's
    own existing confidence threshold; nothing is fabricated.
    """
    min_confidence = request.args.get("min_confidence", type=float)
    limit = request.args.get("limit", type=int)
    return jsonify(_scanner.get_recommendations(min_confidence=min_confidence, limit=limit))


@app.route("/api/scanner/diagnostics")
def api_scanner_diagnostics():
    """
    M1 — Full Scanner Diagnostics (read-only). Unlike /api/scanner/results
    (which only returns assets that cleared every hard gate and confidence
    threshold), this returns EVERY asset/timeframe the scanner is currently
    configured to cover, each labeled SIGNAL / WAIT / FAILED / SKIPPED with
    the actual reason — so it's possible to see why an asset did or didn't
    produce a signal. Thin passthrough to ScannerEngine.get_diagnostics();
    does not filter, rank, or alter anything the scanner already computed.
    """
    return jsonify(_scanner.get_diagnostics())


@app.route("/api/scanner/start", methods=["POST"])
def api_scanner_start():
    data = request.get_json(silent=True) or {}
    timeframes = data.get("timeframes")
    if timeframes:
        invalid = [tf for tf in timeframes if tf not in TIMEFRAMES]
        if invalid:
            return jsonify({"error": f"Invalid timeframe(s): {invalid}"}), 400

    # Step 4/6 + Finding B fix — Live Asset Availability: refresh live OTC
    # availability from Quotex immediately before starting, ALWAYS — the
    # static api_quotex/constants.py list is never used as the scan target,
    # and an explicit "assets" list in the request body no longer bypasses
    # this check (previously it did — see Finding B in the no-fake-data
    # audit). Precedence, highest to lowest, all filtered through the SAME
    # live snapshot:
    #   1. an explicit "assets" list in this request's JSON body — OTC-
    #      suffixed entries are kept only if currently live-available; a
    #      non-OTC entry passes through unfiltered (outside this system's
    #      OTC-only scope, same as live_assets.py generally)
    #   2. a saved custom selection (Settings > Scanner > enabled_assets),
    #      intersected with live availability — a previously-selected asset
    #      that's no longer live-available is dropped, never force-scanned
    #   3. the full live-available OTC snapshot
    # In all three cases the final list passed to ScannerEngine.start() is
    # always a subset of what Quotex reports available *right now*.
    try:
        fetcher = _run_bg(_get_shared_fetcher(), timeout=30.0)
    except Exception as exc:
        return jsonify({
            "ok": False,
            "message": f"Cannot start scanner — no active Quotex session ({exc}).",
        }), 503
    try:
        live_otc = _run_bg(live_assets.get_live_otc_assets(fetcher, force_refresh=True), timeout=30.0)
    except Exception as exc:
        return jsonify({
            "ok": False,
            "message": f"Cannot start scanner — failed to fetch live asset availability ({exc}).",
        }), 503
    live_symbols = set(live_otc.keys())

    explicit_assets = data.get("assets")
    dropped_unavailable: List[str] = []
    if explicit_assets:
        filtered = live_assets.filter_requested_against_live(explicit_assets, live_symbols)
        assets_for_run = filtered["kept"]
        dropped_unavailable = filtered["dropped_otc_unavailable"]
        if not assets_for_run:
            return jsonify({
                "ok": False,
                "message": "No requested OTC assets are currently available from Quotex.",
                "requested": explicit_assets,
                "dropped_otc_unavailable": dropped_unavailable,
                "live_otc_discovered": len(live_symbols),
            }), 503
    else:
        saved_selection = None
        try:
            saved_selection = (settings_store.get().get("scanner", {}) or {}).get("enabled_assets") or None
        except Exception:
            saved_selection = None

        if saved_selection:
            assets_for_run = [a for a in saved_selection if a in live_symbols]
        else:
            assets_for_run = sorted(live_symbols)

    live_snapshot_meta: Dict[str, Any] = {
        "live_otc_discovered": len(live_symbols),
        "scanner_target": len(assets_for_run),
    }
    if dropped_unavailable:
        live_snapshot_meta["dropped_otc_unavailable"] = dropped_unavailable

    # Finding C guard — defense in depth: assets_for_run must always be a
    # concrete list by this point (possibly empty, e.g. 0 live OTC assets
    # with no explicit request and no saved selection — that's a legitimate
    # "scan 0 assets" state, not an error). This route must NEVER call
    # ScannerEngine.start() with assets left as None, since that would
    # silently fall back to the static constructor asset list inside
    # ScannerEngine itself. This is a pure sanity assertion over code that
    # already always sets assets_for_run above; it exists to catch any
    # future edit to this function that accidentally removes that guarantee.
    if assets_for_run is None:
        return jsonify({
            "ok": False,
            "message": "Internal error: no live asset snapshot was resolved before starting the scanner.",
        }), 500

    result = _scanner.start(
        loop=_get_bg_loop(),
        assets=assets_for_run,
        timeframes=timeframes,
        top_n=data.get("top_n"),
        min_confidence=data.get("min_confidence"),
        refresh_seconds=data.get("refresh_seconds"),
    )
    result.update(live_snapshot_meta)
    return jsonify(result), (200 if result["ok"] else 409)


@app.route("/api/scanner/stop", methods=["POST"])
def api_scanner_stop():
    result = _scanner.stop()
    return jsonify(result), (200 if result["ok"] else 409)


@app.route("/api/scanner/pause", methods=["POST"])
def api_scanner_pause():
    result = _scanner.pause()
    return jsonify(result), (200 if result["ok"] else 409)


@app.route("/api/scanner/resume", methods=["POST"])
def api_scanner_resume():
    result = _scanner.resume()
    return jsonify(result), (200 if result["ok"] else 409)


@app.route("/api/scanner/settings/reset", methods=["POST"])
def api_scanner_settings_reset():
    """
    Phase 8.3 — resets ONLY the `scanner` settings section to its
    defaults (scanner_enabled/enabled_assets/enabled_timeframes/
    scan_interval/minimum_filter_score/top_signals), leaving every other
    settings section (indicators, backtest, filters, etc.) untouched.
    Thin wrapper around settings_store.reset_section("scanner") — same
    pattern as every other route in this section. Does NOT touch the
    running scanner in any way (per spec: settings changes only apply on
    the next start()), so no ScannerEngine call happens here at all.
    """
    try:
        return jsonify(settings_store.reset_section("scanner"))
    except KeyError as e:
        return jsonify({"error": str(e)}), 400


# ═══════════════════════════════════════════════════════════════════════════
# Phase 7.4 — Settings Routes
# Thin HTTP wrappers around settings_store.py's already-tested methods.
# Mirrors the exact pattern of the scanner routes above. No new business
# logic lives here — every route is a direct pass-through.
# ═══════════════════════════════════════════════════════════════════════════

@app.route("/api/settings", methods=["GET", "POST"])
def api_settings():
    if request.method == "GET":
        return jsonify(settings_store.get())
    patch = request.get_json(silent=True) or {}
    try:
        return jsonify(settings_store.update(patch))
    except Exception as e:  # noqa: BLE001
        return jsonify({"error": f"Settings update failed: {e}"}), 400


@app.route("/api/settings/reset", methods=["POST"])
def api_settings_reset():
    return jsonify(settings_store.reset())


@app.route("/api/settings/backups", methods=["GET"])
def api_settings_backups():
    return jsonify({"backups": settings_store.list_backups()})


@app.route("/api/settings/backups/restore", methods=["POST"])
def api_settings_backups_restore():
    data = request.get_json(silent=True) or {}
    index = data.get("index")
    if index is None:
        return jsonify({"error": "Missing required field 'index'"}), 400
    try:
        return jsonify(settings_store.restore_backup(int(index)))
    except IndexError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:  # noqa: BLE001
        return jsonify({"error": f"Restore failed: {e}"}), 400


@app.route("/api/settings/export", methods=["GET"])
def api_settings_export():
    return jsonify(settings_store.export_settings())


@app.route("/api/settings/import", methods=["POST"])
def api_settings_import():
    data = request.get_json(silent=True) or {}
    try:
        return jsonify(settings_store.import_settings(data))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:  # noqa: BLE001
        return jsonify({"error": f"Import failed: {e}"}), 400


@app.route("/api/settings/backup", methods=["POST"])
def api_settings_backup():
    """Manual 'create a backup now' action (separate from the automatic
    backup apply_suggested_weights() already creates on every apply)."""
    data = request.get_json(silent=True) or {}
    reason = str(data.get("reason", "manual"))
    return jsonify(settings_store.create_backup(reason=reason))


# ═══════════════════════════════════════════════════════════════════════════
# Phase 7.4 — Indicator Registry Route
# Read-only metadata view: the 13-entry registry (10 confluence-connected +
# 3 not-yet-connected, per indicator_registry.py), overlaid with the user's
# Settings enable/disable + weight overrides. Does not compute or duplicate
# any indicator math.
# ═══════════════════════════════════════════════════════════════════════════

@app.route("/api/indicators", methods=["GET"])
def api_indicators():
    entries = ind_registry.get_registry()
    settings = settings_store.get()
    entries = ind_registry.apply_settings_overrides(entries, settings.get("indicators", {}))

    # If a backtest has completed, layer real accuracy/dynamic_weight data on
    # top — pure read of _backtest_engine's already-computed public
    # attributes (.results / .summary), no re-computation, no
    # backtest_engine.py changes. Averages each factor's accuracy across all
    # successfully-backtested assets (same "simple averaging" approach
    # _build_summary() already uses for suggested_weights).
    if _backtest_engine.summary and _backtest_engine.summary.get("assets_backtested", 0) > 0:
        valid = {a: r for a, r in _backtest_engine.results.items() if "error" not in r}
        if valid:
            keys = next(iter(valid.values()))["accuracies"].keys()
            avg_accuracies = {
                k: {
                    "accuracy": round(sum(r["accuracies"][k]["accuracy"] for r in valid.values()) / len(valid), 4),
                    "sample_size": min(r["accuracies"][k]["sample_size"] for r in valid.values()),
                }
                for k in keys
            }
            timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(_backtest_engine.finished_at or time.time()))
            entries = ind_registry.populate_from_backtest(
                entries, avg_accuracies, _backtest_engine.summary["suggested_weights"], timestamp
            )

    return jsonify({
        "indicators": entries,
        "filter_gate_criteria": ind_registry.FILTER_GATE_CRITERIA,
        "new_indicator_ids": ind_registry.NEW_INDICATOR_IDS,
    })


# ═══════════════════════════════════════════════════════════════════════════
# Phase 7.4 — Backtest Routes
# Thin HTTP wrappers around backtest_engine.py's already-tested lifecycle.
# _backtest_engine is a single module-level instance (one backtest at a
# time — start() itself already rejects a second concurrent run).
# ═══════════════════════════════════════════════════════════════════════════

@app.route("/api/backtest/run", methods=["POST"])
def api_backtest_run():
    data = request.get_json(silent=True) or {}

    # 400 — malformed client input (bad types), caught before touching the engine.
    try:
        assets = data.get("assets") or _asset_choices()["otc"]
        if not isinstance(assets, list) or not all(isinstance(a, str) for a in assets):
            return jsonify({"ok": False, "error": "'assets' must be a list of asset symbol strings."}), 400
        timeframe = str(data.get("timeframe", cfg.PRIMARY_TIMEFRAME))
        candle_count = int(data.get("candle_count", settings_store.get()["backtest"]["min_candles"]))
        lookahead = int(data.get("lookahead", 4))
    except (TypeError, ValueError) as e:
        return jsonify({"ok": False, "error": f"Invalid request body: {e}"}), 400

    # 400 — semantically invalid but well-typed input.
    if timeframe not in TIMEFRAMES:
        return jsonify({"ok": False, "error": f"Invalid timeframe '{timeframe}'. Must be one of {TIMEFRAMES}."}), 400
    # P0 fix — Unknown Asset: Backtest is historical work, so the static
    # ASSETS reference dict remains a valid universe on its own (an asset
    # need not be currently open/live to have historical candles). Fix is
    # additive only — a live-only asset (present in the current Quotex
    # snapshot but not yet in the static reference dict) is now ALSO
    # accepted rather than rejected. Best-effort only: if no session is
    # available to check live availability, this silently falls back to
    # the static-only check (identical to pre-fix behavior) rather than
    # blocking backtests from being queued — per-asset fetch failures are
    # already handled (and, after the P0 progress fix below, no longer
    # abort the batch) once the run actually starts.
    live_snapshot: Dict[str, Any] = {}
    try:
        _fetcher = _run_bg(_get_shared_fetcher(), timeout=10.0)
        live_snapshot = _run_bg(live_assets.get_live_assets(_fetcher), timeout=10.0) or {}
    except Exception:
        live_snapshot = {}
    invalid_assets = [a for a in assets if a not in ASSETS and a not in live_snapshot]
    if invalid_assets:
        return jsonify({"ok": False, "error": f"Unknown asset(s): {invalid_assets}"}), 400
    if candle_count not in BACKTEST_CANDLE_OPTIONS:
        return jsonify({"ok": False, "error": f"candle_count must be one of {BACKTEST_CANDLE_OPTIONS}, got {candle_count}"}), 400
    if not assets:
        return jsonify({"ok": False, "error": "No assets specified"}), 400

    # 409 — genuine conflict: a backtest is already in progress.
    # Bug fix (found in Phase 7.4 API regression testing): BacktestEngine.start()
    # returns the same {"ok": False, "message": ...} shape whether the reason
    # is "already running" (a real 409 conflict) or "invalid candle_count" /
    # "no assets" (a 400 client input error, both pre-checked above already)
    # — see backtest_engine.py's start() docstring/body, unmodified. Checking
    # the conflict condition ourselves first means every case now gets the
    # right HTTP status without needing to touch backtest_engine.py at all.
    if _backtest_engine.state != _BT_STOPPED:
        return jsonify({"ok": False, "error": f"Backtest already {_backtest_engine.state} — ignoring duplicate start request"}), 409

    # 500 — something the checks above didn't anticipate (e.g. the shared
    # background event loop isn't available). Genuinely unexpected only.
    try:
        result = _backtest_engine.start(
            loop=_get_bg_loop(), assets=assets, timeframe=timeframe,
            candle_count=candle_count, lookahead=lookahead,
        )
    except Exception as e:  # noqa: BLE001
        return jsonify({"ok": False, "error": f"Internal error starting backtest: {e}"}), 500

    if not result["ok"]:
        # Defensive fallback only — every documented start() failure reason
        # is already intercepted by the checks above, so this path should be
        # unreachable in practice; kept as a safety net rather than a 500.
        return jsonify(result), 400
    return jsonify(result), 200


@app.route("/api/backtest/pause", methods=["POST"])
def api_backtest_pause():
    result = _backtest_engine.pause()
    return jsonify(result), (200 if result["ok"] else 409)


@app.route("/api/backtest/resume", methods=["POST"])
def api_backtest_resume():
    result = _backtest_engine.resume()
    return jsonify(result), (200 if result["ok"] else 409)


@app.route("/api/backtest/stop", methods=["POST"])
def api_backtest_stop():
    result = _backtest_engine.stop()
    return jsonify(result), (200 if result["ok"] else 409)


@app.route("/api/backtest/status", methods=["GET"])
def api_backtest_status():
    return jsonify(_backtest_engine.status())


@app.route("/api/backtest/results", methods=["GET"])
def api_backtest_results():
    return jsonify(_backtest_engine.get_results())


@app.route("/api/backtest/apply-weights", methods=["POST"])
def api_backtest_apply_weights():
    """
    Explicit, user-triggered only (this route IS that explicit action —
    never called by anything automatic/scheduled in this codebase).
    Gate-checks via evaluate_apply_conditions() first; only writes to
    settings.json if can_apply=True. apply_suggested_weights() itself
    already creates a backup — never duplicated here.
    """
    backtest_cfg = settings_store.get()["backtest"]
    check = evaluate_apply_conditions(
        _backtest_engine,
        min_candles=backtest_cfg["min_candles"],
        min_indicator_sample_size=backtest_cfg["min_indicator_sample_size"],
    )
    if not check["can_apply"]:
        return jsonify({"ok": False, "reasons": check["reasons"]}), 409

    updated = settings_store.apply_suggested_weights(
        check["summary"]["suggested_weights"], reason="backtest"
    )
    return jsonify({"ok": True, "settings": updated})


@app.route("/api/backtest/save-weights", methods=["POST"])
def api_backtest_save_weights():
    """
    Phase 17 Step 1 — manual weight editor Save. Deliberately independent
    of evaluate_apply_conditions()/the 2000-candle & 100-sample gates above:
    per the explicit requirement, "weight editing and backtest-data
    eligibility are separate concerns" — a user hand-tuning weights is not
    claiming a fresh backtest satisfied those statistical minimums, so this
    route never checks them. What IS still enforced, server-side (never
    trust the client alone): exactly the 13 real confluence factor keys —
    no more, no fewer, nothing renamed — and the weights must sum to
    100 (within a tiny float tolerance) with no negative value, mirroring
    the same total-must-equal-100 invariant DEFAULT_CONFLUENCE_WEIGHTS
    itself already guarantees. Reuses the exact same, already-existing
    settings_store.apply_suggested_weights() persistence path the
    Apply-Suggested-Weights route above uses — no second settings system,
    no duplicated write logic — just a different (manual) source for the
    weights dict and a different `reason` tag in the backup.
    """
    data = request.get_json(silent=True) or {}
    weights = data.get("weights")
    if not isinstance(weights, dict):
        return jsonify({"ok": False, "reasons": ["Request body must include a 'weights' object."]}), 400

    expected_keys = set(DEFAULT_CONFLUENCE_WEIGHTS.keys())  # the authoritative 13 — never redefined here
    got_keys = set(weights.keys())
    if got_keys != expected_keys:
        missing = sorted(expected_keys - got_keys)
        extra = sorted(got_keys - expected_keys)
        reasons = []
        if missing:
            reasons.append(f"Missing factor(s): {', '.join(missing)}")
        if extra:
            reasons.append(f"Unknown factor(s): {', '.join(extra)}")
        return jsonify({"ok": False, "reasons": reasons}), 400

    parsed: Dict[str, float] = {}
    for k, v in weights.items():
        try:
            fv = float(v)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "reasons": [f"Weight for '{k}' is not a number."]}), 400
        if fv < 0:
            return jsonify({"ok": False, "reasons": [f"Weight for '{k}' cannot be negative."]}), 400
        parsed[k] = fv

    total = sum(parsed.values())
    if abs(total - 100.0) > 0.05:
        return jsonify({
            "ok": False,
            "reasons": [f"Total weight must equal 100 (got {total:.2f})."],
        }), 400

    updated = settings_store.apply_suggested_weights(parsed, reason="manual_edit")
    # Read back from the store (not just the in-memory `parsed`) so the
    # response honestly reflects what was actually persisted to disk.
    saved_weights = {
        k: updated["indicators"][k]["weight"]
        for k in expected_keys
        if k in updated.get("indicators", {})
    }
    return jsonify({"ok": True, "settings": updated, "weights": saved_weights})


# ═══════════════════════════════════════════════════════════════════════════
# Phase 8.4.3 — Validation Routes
# Thin HTTP wrappers around validation_engine.py's ValidationEngine, mirroring
# the exact request-validation/status-code pattern the backtest routes above
# already use (400 for malformed/invalid input, 409 for a genuine lifecycle
# conflict, 500 only for a genuinely unexpected internal error). Analysis
# only — never connects the 3 new indicators to Confluence, never computes
# or writes a dynamic weight, never touches voting.
# ═══════════════════════════════════════════════════════════════════════════

@app.route("/api/validation/run", methods=["POST"])
def api_validation_run():
    data = request.get_json(silent=True) or {}

    # 400 — malformed client input (bad types), caught before touching the engine.
    try:
        explicit_assets = data.get("assets")
        if explicit_assets is not None and (
            not isinstance(explicit_assets, list) or not all(isinstance(a, str) for a in explicit_assets)
        ):
            return jsonify({"ok": False, "error": "'assets' must be a list of asset symbol strings."}), 400
        timeframes = data.get("timeframes") or [cfg.PRIMARY_TIMEFRAME]
        if not isinstance(timeframes, list) or not all(isinstance(t, str) for t in timeframes):
            return jsonify({"ok": False, "error": "'timeframes' must be a list of timeframe strings."}), 400
        # Phase 10.1: default (when 'indicators' is omitted) is now the
        # full 13-factor universal set — see validation_engine.py's start()
        # for the same default. Explicitly passing the original 3 names
        # still works exactly as before.
        indicators = data.get("indicators") or list(VALIDATION_UNIVERSAL_INDICATOR_NAMES)
        if not isinstance(indicators, list) or not all(isinstance(i, str) for i in indicators):
            return jsonify({"ok": False, "error": "'indicators' must be a list of indicator name strings."}), 400
        candle_count = int(data.get("candle_count", 2000))
        lookahead = int(data.get("lookahead", 4))
    except (TypeError, ValueError) as e:
        return jsonify({"ok": False, "error": f"Invalid request body: {e}"}), 400

    # Finding B fix (no-fake-data audit) — Live Asset Availability: ALWAYS
    # fetch the live OTC snapshot now, regardless of whether the caller
    # supplied an explicit "assets" list. An explicit list no longer
    # bypasses live availability (it previously did). OTC-suffixed
    # requested assets are kept only if currently live-available and
    # therefore actually fetchable; non-OTC requested assets pass through
    # unfiltered — outside this live-availability system's OTC-only scope,
    # same as live_assets.py generally.
    try:
        fetcher = _run_bg(_get_shared_fetcher(), timeout=30.0)
        live_otc = _run_bg(live_assets.get_live_otc_assets(fetcher, force_refresh=True), timeout=30.0)
    except Exception as exc:
        return jsonify({"ok": False, "error": f"No active Quotex session — cannot determine live asset availability ({exc})."}), 503
    live_symbols = set(live_otc.keys())

    dropped_unavailable: List[str] = []
    if explicit_assets:
        filtered = live_assets.filter_requested_against_live(explicit_assets, live_symbols)
        assets = filtered["kept"]
        dropped_unavailable = filtered["dropped_otc_unavailable"]
        if not assets:
            return jsonify({
                "ok": False,
                "error": "No requested OTC assets are currently available from Quotex.",
                "requested": explicit_assets,
                "dropped_otc_unavailable": dropped_unavailable,
                "live_otc_discovered": len(live_symbols),
            }), 503
    else:
        assets = sorted(live_symbols)

    # 400 — semantically invalid but well-typed input.
    invalid_timeframes = [t for t in timeframes if t not in TIMEFRAMES]
    if invalid_timeframes:
        return jsonify({"ok": False, "error": f"Invalid timeframe(s): {invalid_timeframes}. Must be from {TIMEFRAMES}."}), 400
    # P0 fix — Unknown Asset: `assets` at this point has ALREADY been
    # filtered against the live Quotex snapshot fetched above (either via
    # filter_requested_against_live() or as the live snapshot itself) — it
    # is by construction a subset of what Quotex reports available right
    # now (for OTC symbols) or an unfiltered explicit non-OTC symbol. Re-
    # rejecting those live-verified assets against the stale static ASSETS
    # dict undid that live-availability fix; the static dict remains a
    # valid additional (not exclusive) reference universe.
    invalid_assets = [a for a in assets if a not in ASSETS and a not in live_symbols]
    if invalid_assets:
        return jsonify({"ok": False, "error": f"Unknown asset(s): {invalid_assets}"}), 400
    invalid_indicators = [i for i in indicators if i not in VALIDATION_UNIVERSAL_INDICATOR_NAMES]
    if invalid_indicators:
        return jsonify({"ok": False, "error": f"Unknown indicator(s): {invalid_indicators}. Must be from {list(VALIDATION_UNIVERSAL_INDICATOR_NAMES)}."}), 400
    if candle_count not in VALIDATION_CANDLE_OPTIONS:
        return jsonify({"ok": False, "error": f"candle_count must be one of {VALIDATION_CANDLE_OPTIONS}, got {candle_count}"}), 400
    if not assets:
        msg = ("No OTC assets currently available from Quotex." if not explicit_assets
               else "No assets specified")
        return jsonify({"ok": False, "error": msg}), (503 if not explicit_assets else 400)
    if not timeframes:
        return jsonify({"ok": False, "error": "No timeframes specified"}), 400

    # 409 — genuine conflict: a validation run is already in progress.
    if _validation_engine.state != _VAL_STOPPED:
        return jsonify({"ok": False, "error": f"Validation already {_validation_engine.state} — ignoring duplicate start request"}), 409

    # 500 — something the checks above didn't anticipate.
    try:
        result = _validation_engine.start(
            loop=_get_bg_loop(), assets=assets, timeframes=timeframes,
            indicators=indicators, candle_count=candle_count, lookahead=lookahead,
        )
    except Exception as e:  # noqa: BLE001
        return jsonify({"ok": False, "error": f"Internal error starting validation: {e}"}), 500

    if not result["ok"]:
        # Defensive fallback only — every documented start() failure reason
        # is already intercepted by the checks above.
        return jsonify(result), 400
    return jsonify(result), 200


@app.route("/api/validation/pause", methods=["POST"])
def api_validation_pause():
    result = _validation_engine.pause()
    return jsonify(result), (200 if result["ok"] else 409)


@app.route("/api/validation/resume", methods=["POST"])
def api_validation_resume():
    result = _validation_engine.resume()
    return jsonify(result), (200 if result["ok"] else 409)


@app.route("/api/validation/stop", methods=["POST"])
def api_validation_stop():
    result = _validation_engine.stop()
    return jsonify(result), (200 if result["ok"] else 409)


@app.route("/api/validation/status", methods=["GET"])
def api_validation_status():
    return jsonify(_validation_engine.status())


@app.route("/api/validation/results", methods=["GET"])
def api_validation_results():
    return jsonify(_validation_engine.get_results())


# ═══════════════════════════════════════════════════════════════════════════
# Phase 8.5 — Validation History Routes (additive; the 6 routes above are
# UNCHANGED — persistence is triggered only from these 3 new routes, per
# the "app.py-polling-and-persist, no ValidationEngine callback" decision).
# ═══════════════════════════════════════════════════════════════════════════

@app.route("/api/validation/history", methods=["GET"])
def api_validation_history():
    _maybe_persist_validation_history()
    return jsonify(_validation_history_store.get_history())


@app.route("/api/validation/history/<indicator>", methods=["GET"])
def api_validation_history_indicator(indicator):
    _maybe_persist_validation_history()
    if indicator not in VALIDATION_UNIVERSAL_INDICATOR_NAMES:
        return jsonify({
            "error": f"Unknown indicator: {indicator!r}. Must be from {list(VALIDATION_UNIVERSAL_INDICATOR_NAMES)}"
        }), 400
    return jsonify(_validation_history_store.get_indicator_history(indicator))


@app.route("/api/validation/history/reset", methods=["POST"])
def api_validation_history_reset():
    return jsonify(_validation_history_store.reset())


# ═══════════════════════════════════════════════════════════════════════════
# Phase 10.1 — Asset-wise / Timeframe-wise Validation summary routes
# (additive; every route above this block is UNCHANGED). Pure, read-only
# aggregation of _validation_engine.get_results() via
# indicator_validation.summarize_by_asset()/summarize_by_timeframe() — no
# new measurement, no re-fetching, no write anywhere. Available at any
# time; returns empty summaries if no validation run has completed yet.
# ═══════════════════════════════════════════════════════════════════════════

def _flatten_validation_results() -> list:
    """Turns ValidationEngine.results (keyed "asset|timeframe" -> {indicators:
    {name: result_dict}}) into the flat list-of-result-dicts shape
    indicator_validation.summarize_by_asset()/summarize_by_timeframe()
    expect. Skips failed (asset, timeframe) combinations (those only have
    an "error" key, no "indicators")."""
    flat = []
    for combo in _validation_engine.results.values():
        for result in (combo.get("indicators") or {}).values():
            flat.append(result)
    return flat


@app.route("/api/validation/summary/by-asset", methods=["GET"])
def api_validation_summary_by_asset():
    return jsonify(_summarize_validation_by_asset(_flatten_validation_results()))


@app.route("/api/validation/summary/by-timeframe", methods=["GET"])
def api_validation_summary_by_timeframe():
    return jsonify(_summarize_validation_by_timeframe(_flatten_validation_results()))


# ═══════════════════════════════════════════════════════════════════════════
# Phase 9 — Smart Learning Routes (additive; every route above this block is
# UNCHANGED). Read-only consumer of _validation_history_store — never writes
# to validation_history.json. "Apply" reuses the existing, unmodified
# settings_store.apply_suggested_weights() (same function backtest's
# "Apply Suggested Weights" already uses) with reason="learning" — no new
# settings-write path is introduced.
# ═══════════════════════════════════════════════════════════════════════════

@app.route("/api/learning/status", methods=["GET"])
def api_learning_status():
    """Computes a fresh recommendation from the current Validation History —
    does NOT persist it (see /api/learning/generate for the explicit,
    user-triggered persist action). Cheap, pure computation; safe to poll."""
    history = _validation_history_store.get_history()
    rec = compute_recommendation(DEFAULT_CONFLUENCE_WEIGHTS, history)
    return jsonify(rec)


@app.route("/api/learning/generate", methods=["POST"])
def api_learning_generate():
    """
    Explicit, user-triggered only (this route IS that explicit action —
    never called automatically). Computes a fresh recommendation and
    appends it to the persisted recommendation_log — this is what builds
    "Recommendation History" over time. Optional JSON body may override
    min_weight/max_weight/learning_rate/min_samples for this one
    computation; omitted fields fall back to learning_engine's defaults.
    """
    body = request.get_json(silent=True) or {}
    kwargs: Dict[str, Any] = {}
    float_fields = ("min_weight", "max_weight", "learning_rate")
    int_fields = ("min_samples",)
    for key in float_fields:
        if key in body:
            try:
                kwargs[key] = float(body[key])
            except (TypeError, ValueError):
                return jsonify({"error": f"Invalid value for {key!r}: must be a number"}), 400
    for key in int_fields:
        if key in body:
            try:
                kwargs[key] = int(body[key])
            except (TypeError, ValueError):
                return jsonify({"error": f"Invalid value for {key!r}: must be an integer"}), 400

    history = _validation_history_store.get_history()
    rec = compute_recommendation(DEFAULT_CONFLUENCE_WEIGHTS, history, **kwargs)
    _learning_history_store.record_recommendation(rec)
    return jsonify(rec)


@app.route("/api/learning/history", methods=["GET"])
def api_learning_history():
    return jsonify(_learning_history_store.get_history())


@app.route("/api/learning/apply", methods=["POST"])
def api_learning_apply():
    """
    Explicit, user-triggered only. Gate-checks first (at least one
    indicator must have real Validation History data — i.e. confidence
    != "none" — otherwise there is nothing for Learning to actually
    contribute and "applying" would just rewrite existing weights back
    onto themselves); only writes to settings.json if that passes.
    apply_suggested_weights() itself already creates a backup — never
    duplicated here (same pattern as /api/backtest/apply-weights).
    """
    history = _validation_history_store.get_history()
    rec = compute_recommendation(DEFAULT_CONFLUENCE_WEIGHTS, history)
    has_data = any(v["confidence"] != "none" for v in rec["per_indicator"].values())
    if not has_data:
        return jsonify({
            "ok": False,
            "reasons": ["No indicator has enough accumulated Validation History yet "
                        f"(needs >= {_learning_engine.DEFAULT_MIN_SAMPLES} samples)."],
        }), 409

    updated = settings_store.apply_suggested_weights(rec["recommended_weights"], reason="learning")
    return jsonify({"ok": True, "settings": updated, "applied_recommendation": rec})


@app.route("/api/learning/reset", methods=["POST"])
def api_learning_reset():
    """Resets ONLY this module's own recommendation_log — never touches
    validation_history.json (Validation History is never edited by this
    module, structurally, see learning_engine.py's module docstring)."""
    return jsonify(_learning_history_store.reset())


# ═══════════════════════════════════════════════════════════════════════════
# Phase 10.2 — Asset Intelligence + Timeframe Intelligence Routes (additive;
# every route above this block is UNCHANGED). Read-only consumers of
# _validation_history_store's Phase 10.2 asset_stats/timeframe_stats +
# pre-existing rolling_stats. Cheap, pure computation via
# asset_timeframe_learning.py; never persist anything themselves — there is
# no /reset or /generate-and-persist route in this phase's scope, unlike
# Phase 9's Learning routes (see asset_timeframe_learning.py's own
# docstring for why no history-of-recommendations log exists here).
# ═══════════════════════════════════════════════════════════════════════════

@app.route("/api/learning/assets", methods=["GET"])
def api_learning_assets():
    """Step 3 — Asset Learning: total validations/wins/losses/accuracy/
    trend/confidence + best/weakest indicator, per OTC asset, plus a
    best-to-worst ranking. Optional ?min_samples= query param overrides
    the confidence-gating threshold used for the ranking list."""
    min_samples = request.args.get("min_samples", _asset_timeframe_learning.DEFAULT_MIN_SAMPLES, type=int)
    history = _validation_history_store.get_history()
    return jsonify(_asset_timeframe_learning.compute_asset_rankings(history, min_samples=min_samples))


@app.route("/api/learning/timeframes", methods=["GET"])
def api_learning_timeframes():
    """Step 4 — Timeframe Learning: identical shape to /api/learning/assets,
    grouped by timeframe instead (1m/5m/15m/any other timeframe actually
    present in Validation History — no fixed timeframe list is assumed)."""
    min_samples = request.args.get("min_samples", _asset_timeframe_learning.DEFAULT_MIN_SAMPLES, type=int)
    history = _validation_history_store.get_history()
    return jsonify(_asset_timeframe_learning.compute_timeframe_rankings(history, min_samples=min_samples))


@app.route("/api/learning/top-indicators", methods=["GET"])
def api_learning_top_indicators():
    """Global (not asset/timeframe-scoped) indicator ranking from the
    existing rolling_stats: strongest + weakest indicators system-wide.
    Optional ?min_samples= and ?top_n= query params."""
    min_samples = request.args.get("min_samples", _asset_timeframe_learning.DEFAULT_MIN_SAMPLES, type=int)
    top_n = request.args.get("top_n", _asset_timeframe_learning.DEFAULT_TOP_N, type=int)
    if not top_n or top_n < 1:
        return jsonify({"error": "'top_n' must be a positive integer"}), 400
    history = _validation_history_store.get_history()
    return jsonify(_asset_timeframe_learning.compute_top_indicators(history, min_samples=min_samples, top_n=top_n))


@app.route("/api/learning/recommendations", methods=["GET"])
def api_learning_recommendations():
    """Step 5 — Learning Recommendations: best indicator per asset, best
    indicator per timeframe, lowest-performing indicators overall, and
    improving/declining indicators overall. Advisory only — this route
    never writes anywhere and nothing here is applied automatically."""
    min_samples = request.args.get("min_samples", _asset_timeframe_learning.DEFAULT_MIN_SAMPLES, type=int)
    top_n = request.args.get("top_n", _asset_timeframe_learning.DEFAULT_TOP_N, type=int)
    if not top_n or top_n < 1:
        return jsonify({"error": "'top_n' must be a positive integer"}), 400
    history = _validation_history_store.get_history()
    return jsonify(_asset_timeframe_learning.compute_recommendations(history, min_samples=min_samples, top_n=top_n))


# ═══════════════════════════════════════════════════════════════════════════
# Phase 10.3 Part-2 — AI Health Dashboard + Explainable Signal System Routes
# (additive; every route above this block is UNCHANGED). Read-only — none
# of these routes write to any file or mutate any store. `/api/ai/health`,
# `/api/ai/status`, and `/api/ai/statistics` all reuse the same input-
# gathering helper (`_build_ai_health()` below) so the three stay
# consistent by construction; `/api/ai/explain` reuses `_run_pipeline()`
# exactly like `/api/signal` does (same error handling, same manual-
# request bookkeeping) when an asset/timeframe is given, or falls back to
# explaining the scanner's current top-ranked cached signal when not.
# ═══════════════════════════════════════════════════════════════════════════

def _build_ai_health() -> Dict[str, Any]:
    """Gathers every input compute_ai_health() needs from the stores/
    engines app.py already owns, then delegates entirely to
    ai_health_engine.compute_ai_health() — no health-scoring logic lives
    in this file itself."""
    history = _validation_history_store.get_history()
    recommendation = compute_recommendation(DEFAULT_CONFLUENCE_WEIGHTS, history)
    scanner_status = _scanner.get_status()
    scanner_results = _scanner.get_results()
    top_signals = scanner_results.get("top_signals") or []
    current_regime = top_signals[0].get("regime") if top_signals else None
    return compute_ai_health(
        validation_history=history,
        recommendation=recommendation,
        scanner_status=scanner_status,
        scanner_results=scanner_results,
        current_regime=current_regime,
    )


@app.route("/api/ai/health", methods=["GET"])
def api_ai_health():
    """Full AI Health snapshot — see ai_health_engine.compute_ai_health()'s
    docstring for the exact shape. Read-only; safe to poll."""
    return jsonify(_build_ai_health())


@app.route("/api/ai/status", methods=["GET"])
def api_ai_status():
    """Condensed, glance-friendly view of /api/ai/health — just the 6
    status labels + current regime + whether the scanner is running.
    Cheaper for a UI element that only needs to show a badge, not the
    full breakdown."""
    health = _build_ai_health()
    return jsonify({
        "overall_health": health["overall_health"]["status"],
        "indicator_health": health["indicator_health"]["status"],
        "scanner_health": health["scanner_health"]["status"],
        "validation_health": health["validation_health"]["status"],
        "learning_health": health["learning_health"]["status"],
        "regime_health": health["regime_health"]["status"],
        "current_regime": health["scanner_health"]["current_regime"],
        "scanner_running": health["scanner_health"]["running"],
    })


@app.route("/api/ai/statistics", methods=["GET"])
def api_ai_statistics():
    """Just the flat numeric/statistical fields from /api/ai/health
    (average confidence, average filter score, recent accuracy, recent
    signal count, recent WAIT %, BUY %, SELL %, data quality, history
    coverage) — no nested component-health breakdowns."""
    health = _build_ai_health()
    return jsonify({
        "average_confidence": health["average_confidence"],
        "average_filter_score": health["average_filter_score"],
        "recent_accuracy": health["recent_accuracy"],
        "recent_signal_count": health["recent_signal_count"],
        "recent_wait_pct": health["recent_wait_pct"],
        "buy_pct": health["buy_pct"],
        "sell_pct": health["sell_pct"],
        "data_quality": health["data_quality"],
        "history_coverage": health["history_coverage"],
    })


@app.route("/api/ai/explain", methods=["GET"])
def api_ai_explain():
    """
    Explains a BUY/SELL/WAIT signal — see explainable_signal.explain_
    signal()'s docstring for the exact shape (checks/hard gates/reasons/
    warnings). With ?asset=&timeframe= query params, runs the same
    analysis-only _run_pipeline() /api/signal uses (same error handling,
    same manual-request bookkeeping — never places an order). Without
    them, explains the scanner's current top-ranked cached signal instead
    (empty explanation, not an error, if the scanner has nothing cached
    yet — same "return Unknown/empty rather than guess" convention used
    throughout this project).
    """
    asset = request.args.get("asset")
    timeframe = request.args.get("timeframe", cfg.PRIMARY_TIMEFRAME)

    if asset:
        if timeframe not in TIMEFRAMES:
            return jsonify({"error": f"Invalid timeframe '{timeframe}'. Must be one of {TIMEFRAMES}."}), 400
        # P0 fix — Unknown Asset: same live-snapshot gate as /api/signal
        # (see _check_live_asset() docstring); no longer the stale static dict.
        asset_check = _check_live_asset(asset, timeframe)
        if not asset_check["ok"]:
            return jsonify({"error": asset_check["error"], "kind": asset_check.get("kind")}), asset_check["status"]
        try:
            _scanner.manual_request_started()
            try:
                # Part 3 approved fix — same priority-lock wrapper as
                # /api/signal (see _run_pipeline_with_priority() above).
                result = _run_bg(_run_pipeline_with_priority(asset, timeframe))
            finally:
                _scanner.manual_request_finished()
        except FileNotFoundError as e:
            return jsonify({"error": str(e)}), 500
        except ValueError as e:
            return jsonify({"error": str(e)}), 500
        except ConnectionError as e:
            return jsonify({"error": f"Connection error: {e}"}), 502
        except Exception as e:  # noqa: BLE001 — surface unexpected errors to the UI
            return jsonify({"error": f"Analysis failed: {e}"}), 500
        if "error" in result:
            return jsonify(result), 502
    else:
        top_signals = _scanner.get_results().get("top_signals") or []
        result = top_signals[0] if top_signals else {}

    return jsonify(explain_signal(result))


# ═══════════════════════════════════════════════════════════════════════════
# Phase 10.4 Goal 3 — Historical AI Health Routes (additive; every route
# above this block is UNCHANGED, including /api/ai/health itself — this
# block never modifies what that route returns, it only separately records
# a copy of it over time). All snapshot recording goes through
# _maybe_record_ai_health_snapshot() below, throttled to at most once per
# _AI_HEALTH_SNAPSHOT_MIN_INTERVAL_SECONDS so polling can't flood the log.
# ═══════════════════════════════════════════════════════════════════════════

def _maybe_record_ai_health_snapshot() -> None:
    """Records one compute_ai_health() snapshot via _build_ai_health()
    (the exact same helper /api/ai/health already uses — no separate
    health-computation path exists), at most once per
    _AI_HEALTH_SNAPSHOT_MIN_INTERVAL_SECONDS. Never called from any
    pre-existing /api/ai/* route — only from the new routes below, so
    existing route behavior is provably unchanged."""
    history = _ai_health_history_store.get_history()
    snapshots = history.get("snapshots") or []
    now = time.time()
    if snapshots:
        last_ts = snapshots[0].get("timestamp")
        try:
            last_epoch = calendar.timegm(time.strptime(last_ts, "%Y-%m-%dT%H:%M:%SZ"))
        except (TypeError, ValueError):
            last_epoch = 0
        if now - last_epoch < _AI_HEALTH_SNAPSHOT_MIN_INTERVAL_SECONDS:
            return
    _ai_health_history_store.record_snapshot(_build_ai_health())


@app.route("/api/ai/history/health", methods=["GET"])
def api_ai_history_health():
    """Health History — the full stored snapshot log (most-recent-first).
    Records a new snapshot first (throttled — see
    _maybe_record_ai_health_snapshot()), so the log fills in just from
    being polled, same as validation history's own persistence
    convention. Optional ?limit= caps how many snapshots are returned
    (the store itself still keeps up to
    ai_health_history_store.MAX_SNAPSHOT_ENTRIES regardless)."""
    _maybe_record_ai_health_snapshot()
    limit = request.args.get("limit", type=int)
    return jsonify(ai_health_trends.compute_health_history(_ai_health_history_store.get_history(), limit=limit))


@app.route("/api/ai/history/trends", methods=["GET"])
def api_ai_history_trends():
    """Daily / Weekly / Monthly / Confidence / Validation / Learning /
    Regime trend, bundled — see
    ai_health_trends.build_health_trends_report()'s docstring for the
    exact shape of each. Records a new snapshot first (same throttle as
    /api/ai/history/health)."""
    _maybe_record_ai_health_snapshot()
    history_limit = request.args.get("history_limit", 50, type=int)
    return jsonify(ai_health_trends.build_health_trends_report(
        _ai_health_history_store.get_history(), history_limit=history_limit
    ))


@app.route("/api/ai/history/reset", methods=["POST"])
def api_ai_history_reset():
    """Explicit, user-initiated only — wipes the AI Health snapshot log.
    Does not touch validation_history.json or learning_history.json."""
    return jsonify(_ai_health_history_store.reset())


# ═══════════════════════════════════════════════════════════════════════════
# Phase 10.4 Goal 2 — Adaptive AI Calibration Routes (additive; every route
# above this block is UNCHANGED). Read-only — none of these routes write to
# any file or mutate any store. All four reuse the same
# _build_calibration_report() helper below, sourced entirely from stores
# app.py already owns (_validation_history_store, _learning_history_store).
# Deliberate scope boundary: none of these routes fetch fresh candle data,
# so the df-based sections of adaptive_calibration.py (confidence
# calibration / confidence scaling / threshold optimization) are not
# reachable from these routes yet and report as null — see
# adaptive_calibration.py's module docstring for why, and
# docs/NEXT_PHASE.md for this being tracked as a follow-up, not an
# oversight.
# ═══════════════════════════════════════════════════════════════════════════

def _build_calibration_report() -> Dict[str, Any]:
    """Gathers every input build_calibration_report() needs from the stores
    app.py already owns, then delegates entirely to
    adaptive_calibration.build_calibration_report() — no calibration logic
    lives in this file itself."""
    return build_calibration_report(
        validation_history=_validation_history_store.get_history(),
        learning_history=_learning_history_store.get_history(),
        asset_stats=_validation_history_store.get_asset_stats(),
        timeframe_stats=_validation_history_store.get_timeframe_stats(),
    )


@app.route("/api/calibration/report", methods=["GET"])
def api_calibration_report():
    """Full calibration report — see
    adaptive_calibration.build_calibration_report()'s docstring for the
    exact shape. Read-only; safe to poll."""
    return jsonify(_build_calibration_report())


@app.route("/api/calibration/status", methods=["GET"])
def api_calibration_status():
    """Condensed, glance-friendly view of /api/calibration/report — just
    the per-indicator stability/trend labels and the current best
    asset/timeframe, not the full evidence payload."""
    report = _build_calibration_report()
    asset_ranking = (report.get("asset_calibration") or {}).get("ranking") or []
    tf_ranking = (report.get("timeframe_calibration") or {}).get("ranking") or []
    return jsonify({
        "generated_at": report["generated_at"],
        "indicator_stability_summary": {
            name: info.get("stability") for name, info in (report.get("indicator_stability") or {}).items()
        },
        "validation_trend_summary": {
            name: info.get("trend") for name, info in (report.get("validation_trend") or {}).items()
        },
        "best_asset": asset_ranking[0] if asset_ranking else None,
        "best_timeframe": tf_ranking[0] if tf_ranking else None,
    })


@app.route("/api/calibration/history", methods=["GET"])
def api_calibration_history():
    """Raw inputs the calibration report is built from
    (validation history, learning history, asset stats, timeframe stats) —
    for a caller that wants the underlying data rather than the derived
    report."""
    return jsonify({
        "validation_history": _validation_history_store.get_history(),
        "learning_history": _learning_history_store.get_history(),
        "asset_stats": _validation_history_store.get_asset_stats(),
        "timeframe_stats": _validation_history_store.get_timeframe_stats(),
    })


@app.route("/api/calibration/recommendations", methods=["GET"])
def api_calibration_recommendations():
    """Calibration Recommendations — see
    adaptive_calibration.generate_calibration_recommendations()'s
    docstring. Advisory only; this route never writes anywhere and nothing
    here is applied automatically."""
    report = _build_calibration_report()
    return jsonify({
        "generated_at": report["generated_at"],
        "recommendations": generate_calibration_recommendations(report),
    })


# ═══════════════════════════════════════════════════════════════════════════
# Phase 10.4 Goal 4 — AI Performance Reports Routes (additive; every route
# above this block is UNCHANGED). Read-only — none of these routes write to
# any file or mutate any store. Every route delegates entirely to
# ai_performance_reports.py, sourced from the same stores app.py already
# owns. Deliberate scope boundary, consistent with Goal 2's: no route here
# triggers a live Quotex fetch, so /api/reports/export's "walk_forward"
# section is always {"available": False} — walk_forward.run_walk_forward()
# needs a df this sandbox has no way to fetch-and-verify end-to-end (same
# reasoning as adaptive_calibration's df-dependent functions).
# ═══════════════════════════════════════════════════════════════════════════

@app.route("/api/reports/daily", methods=["GET"])
def api_reports_daily():
    """Daily Report — see ai_performance_reports.compute_daily_report()'s docstring."""
    return jsonify(ai_performance_reports.compute_daily_report(_ai_health_history_store.get_history()))


@app.route("/api/reports/weekly", methods=["GET"])
def api_reports_weekly():
    """Weekly Report — see ai_performance_reports.compute_weekly_report()'s docstring."""
    return jsonify(ai_performance_reports.compute_weekly_report(_ai_health_history_store.get_history()))


@app.route("/api/reports/monthly", methods=["GET"])
def api_reports_monthly():
    """Monthly Report — see ai_performance_reports.compute_monthly_report()'s docstring."""
    return jsonify(ai_performance_reports.compute_monthly_report(_ai_health_history_store.get_history()))


@app.route("/api/reports/assets", methods=["GET"])
def api_reports_assets():
    """Asset Report — best/worst asset + full ranking. Optional ?min_samples=."""
    min_samples = request.args.get("min_samples", ai_performance_reports.DEFAULT_MIN_SAMPLES, type=int)
    return jsonify(ai_performance_reports.compute_asset_report(
        _validation_history_store.get_history(), min_samples=min_samples
    ))


@app.route("/api/reports/timeframes", methods=["GET"])
def api_reports_timeframes():
    """Timeframe Report — best/worst timeframe + full ranking. Optional ?min_samples=."""
    min_samples = request.args.get("min_samples", ai_performance_reports.DEFAULT_MIN_SAMPLES, type=int)
    return jsonify(ai_performance_reports.compute_timeframe_report(
        _validation_history_store.get_history(), min_samples=min_samples
    ))


@app.route("/api/reports/indicators", methods=["GET"])
def api_reports_indicators():
    """Indicator Report — best/worst indicator + top/weakest lists. Optional ?min_samples=&top_n=."""
    min_samples = request.args.get("min_samples", ai_performance_reports.DEFAULT_MIN_SAMPLES, type=int)
    top_n = request.args.get("top_n", ai_performance_reports.DEFAULT_TOP_N, type=int)
    return jsonify(ai_performance_reports.compute_indicator_report(
        _validation_history_store.get_history(), min_samples=min_samples, top_n=top_n
    ))


@app.route("/api/reports/validation", methods=["GET"])
def api_reports_validation():
    """Validation Report — system-wide accuracy/coverage/trend. Optional ?min_samples=."""
    min_samples = request.args.get("min_samples", ai_performance_reports.DEFAULT_MIN_SAMPLES, type=int)
    return jsonify(ai_performance_reports.compute_validation_report(
        _validation_history_store.get_history(), min_samples=min_samples
    ))


@app.route("/api/reports/learning", methods=["GET"])
def api_reports_learning():
    """Learning Report — improving/degrading/stable indicators + recommended weights."""
    history = _validation_history_store.get_history()
    rec = compute_recommendation(DEFAULT_CONFLUENCE_WEIGHTS, history)
    return jsonify(ai_performance_reports.compute_learning_report(rec))


@app.route("/api/reports/ai-health", methods=["GET"])
def api_reports_ai_health():
    """AI Health Report — current snapshot + trends + Regime Distribution."""
    return jsonify(ai_performance_reports.compute_ai_health_report(
        _build_ai_health(), _ai_health_history_store.get_history()
    ))


@app.route("/api/reports/calibration", methods=["GET"])
def api_reports_calibration():
    """Calibration Summary — see ai_performance_reports.compute_calibration_summary()'s docstring."""
    return jsonify(ai_performance_reports.compute_calibration_summary(_build_calibration_report()))


@app.route("/api/reports/export", methods=["GET"])
def api_reports_export():
    """Export-ready bundle of every report section — see
    ai_performance_reports.build_full_performance_report()'s docstring.
    Optional ?min_samples=&top_n=."""
    min_samples = request.args.get("min_samples", ai_performance_reports.DEFAULT_MIN_SAMPLES, type=int)
    top_n = request.args.get("top_n", ai_performance_reports.DEFAULT_TOP_N, type=int)
    history = _validation_history_store.get_history()
    rec = compute_recommendation(DEFAULT_CONFLUENCE_WEIGHTS, history)
    return jsonify(ai_performance_reports.build_full_performance_report(
        validation_history=history,
        recommendation=rec,
        ai_health_snapshot=_build_ai_health(),
        ai_health_history=_ai_health_history_store.get_history(),
        calibration_report=_build_calibration_report(),
        walk_forward_result=None,
        min_samples=min_samples,
        top_n=top_n,
    ))


@app.route("/reports", methods=["GET"])
def reports_page():
    """Minimal additive dashboard page for the reports above. Self-
    contained (its own inline script), reuses the existing style.css —
    no new CSS framework, no change to templates/index.html or
    static/app.js."""
    return render_template("reports.html")


# ═══════════════════════════════════════════════════════════════════════════
# Phase 10.4 Goal 5 — Advanced Analytics Dashboard Routes (additive; every
# route above this block is UNCHANGED, including every /api/reports/*
# route — this block only ADDS one bundling endpoint on top of them, it
# never changes what they individually return). Read-only.
# ═══════════════════════════════════════════════════════════════════════════

@app.route("/api/analytics/dashboard", methods=["GET"])
def api_analytics_dashboard():
    """Single bundled payload for the /analytics dashboard page — see
    analytics_dashboard.build_analytics_dashboard()'s docstring. Optional
    ?min_samples=&top_n=."""
    min_samples = request.args.get("min_samples", ai_performance_reports.DEFAULT_MIN_SAMPLES, type=int)
    top_n = request.args.get("top_n", ai_performance_reports.DEFAULT_TOP_N, type=int)
    history = _validation_history_store.get_history()
    rec = compute_recommendation(DEFAULT_CONFLUENCE_WEIGHTS, history)
    return jsonify(analytics_dashboard.build_analytics_dashboard(
        validation_history=history,
        recommendation=rec,
        ai_health_snapshot=_build_ai_health(),
        ai_health_history=_ai_health_history_store.get_history(),
        calibration_report=_build_calibration_report(),
        walk_forward_result=None,
        min_samples=min_samples,
        top_n=top_n,
    ))


@app.route("/analytics", methods=["GET"])
def analytics_page():
    """Advanced Analytics Dashboard page. Self-contained (its own inline
    script + the same Chart.js CDN build templates/index.html already
    loads — not a new dependency), reuses style.css — no new CSS
    framework, no change to templates/index.html, static/app.js, or
    templates/reports.html."""
    return render_template("analytics.html")


# ═══════════════════════════════════════════════════════════════════════════
# Phase 7.4 — Quotex Session Management Routes
# Reuses the EXISTING connect()/load_session_ssid() path exactly as-is —

# nothing in quotex/api_quotex/ (WebSocket, login, config) is touched.
# "Update SSID" writes only to session.json, never to an env var.
# ═══════════════════════════════════════════════════════════════════════════

@app.route("/api/session/status", methods=["GET"])
def api_session_status():
    env_ssid_set = bool(os.environ.get("QUOTEX_SSID", "").strip())
    file_ssid_present = False
    file_ssid_length = None
    if _SESSION_JSON_PATH.exists():
        try:
            data = json.loads(_SESSION_JSON_PATH.read_text(encoding="utf-8"))
            ssid = str(data.get("ssid", "")).strip()
            file_ssid_present = bool(ssid)
            file_ssid_length = len(ssid) if ssid else None
        except Exception:
            pass
    quotex_settings = settings_store.get()["quotex"]
    return jsonify({
        "env_ssid_set": env_ssid_set,
        "session_file_exists": _SESSION_JSON_PATH.exists(),
        "session_file_ssid_present": file_ssid_present,
        "session_file_ssid_length": file_ssid_length,  # length only — value never exposed
        "active_source": "env" if env_ssid_set else ("session.json" if file_ssid_present else "none"),
        "last_validated_at": quotex_settings.get("last_validated_at"),
        "last_validation_status": quotex_settings.get("last_validation_status"),
        "fetcher_connected": _fetcher_is_alive(),
    })


@app.route("/api/session/update", methods=["POST"])
def api_session_update():
    """Writes the pasted SSID to quotex/session.json (never an env var).
    Preserves any other keys already in the file. Does not connect —
    call /api/session/validate afterwards to test it."""
    data = request.get_json(silent=True) or {}
    ssid = str(data.get("ssid", "")).strip()
    if not ssid or len(ssid) < 10:
        return jsonify({"error": "SSID looks too short/invalid — paste the full value."}), 400

    existing: Dict[str, Any] = {}
    if _SESSION_JSON_PATH.exists():
        try:
            existing = json.loads(_SESSION_JSON_PATH.read_text(encoding="utf-8"))
            if not isinstance(existing, dict):
                existing = {}
        except Exception:
            existing = {}

    existing["ssid"] = ssid
    _SESSION_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = str(_SESSION_JSON_PATH) + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(existing, f, indent=2)
    os.replace(tmp_path, _SESSION_JSON_PATH)  # atomic — same safety pattern as settings_store

    # A newly-written SSID means the old session must stop being used —
    # this now also explicitly disconnects the outgoing WebSocket (see
    # _invalidate_shared_fetcher()'s updated docstring; P0 fix).
    _invalidate_shared_fetcher()

    # P0 fix — SSID/session replacement: load_session_ssid() (fetch_data.py)
    # prefers the QUOTEX_SSID environment variable over session.json
    # whenever it's set. That means the SSID just saved above can be
    # silently ignored on the next connect() if an env var is configured
    # (e.g. Replit Secrets) — previously this was a silent no-op from the
    # user's point of view. Report it explicitly instead of hiding it.
    env_ssid_set = bool(os.environ.get("QUOTEX_SSID", "").strip())
    response: Dict[str, Any] = {
        "ok": True,
        "message": "SSID saved to session.json.",
        "ssid_length": len(ssid),
        "env_var_override_active": env_ssid_set,
    }
    if env_ssid_set:
        response["warning"] = (
            "QUOTEX_SSID environment variable is currently set and takes "
            "priority over session.json — the SSID you just saved will NOT "
            "be used for connections until that environment variable is "
            "unset. Call /api/session/validate to see exactly which source "
            "('env' or 'session.json') is actually used."
        )
    return jsonify(response)


@app.route("/api/session/validate", methods=["POST"])
def api_session_validate():
    """
    Forces a fresh connection attempt via the EXISTING connect() path
    (same code _run_pipeline() uses) and reports success/failure.

    Honesty note: AsyncQuotexClient's connect() performs authentication and
    WebSocket-gateway attachment as a single handshake — it does not expose
    separate "server" vs "gateway" stage events. So "Connected to Server" /
    "Connected to Gateway" / "Session Valid" are reported together as one
    outcome here, not as independently-verified stages; this endpoint does
    not fabricate finer-grained status than the underlying client provides.
    """
    # P0 fix — SSID/session replacement: explicitly disconnect/clear the
    # old session BEFORE attempting the new one, so a failed new SSID can
    # never leave the old session's connection active or be reported as
    # still-connected. _invalidate_shared_fetcher() now also closes the
    # outgoing WebSocket instead of just dropping the reference.
    _invalidate_shared_fetcher()  # force a real reconnect, not a cached "alive" check

    # Report which source connect() will actually read the SSID from —
    # same precedence load_session_ssid() (fetch_data.py) uses. Surfacing
    # this here means a user who just saved a new SSID via
    # /api/session/update can immediately see whether it was the one
    # actually used, instead of it being silently overridden by an env var.
    ssid_source = "env" if os.environ.get("QUOTEX_SSID", "").strip() else "session.json"

    try:
        _run_bg(_get_shared_fetcher(), timeout=30.0)
        settings_store.update({"quotex": {
            "last_validated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "last_validation_status": "valid",
        }})
        return jsonify({
            "ok": True,
            "connected_to_server": True,
            "connected_to_gateway": True,
            "session_valid": True,
            "ssid_source": ssid_source,
            "message": f"Session validated — connected successfully using SSID from {ssid_source}.",
        })
    except Exception as e:  # noqa: BLE001
        # Connection genuinely failed — _shared_fetcher stays None (never
        # silently reverts to the prior session; see _get_shared_fetcher()),
        # so no subsequent request can be served by the old, now-disconnected
        # session either.
        settings_store.update({"quotex": {
            "last_validated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "last_validation_status": "invalid",
        }})
        return jsonify({
            "ok": False,
            "connected_to_server": False,
            "connected_to_gateway": False,
            "session_valid": False,
            "ssid_source": ssid_source,
            "error": str(e),
        }), 502


# ═══════════════════════════════════════════════════════════════════════════
# Phase 7.4 — Generic API error handler
# Safety net only: every route added this phase already handles its own
# expected failure modes with a specific status code (400/404/409, and the
# backtest/run route's own explicit 500 wrapper) — this exists purely to
# turn any genuinely unanticipated exception into a structured JSON 500
# instead of Flask's default HTML error page, for every /api/* route.
# Does not change behavior for any request that was already handled.
# ═══════════════════════════════════════════════════════════════════════════

@app.errorhandler(500)
def _handle_internal_error(e):
    return jsonify({"ok": False, "error": "Internal server error", "detail": str(e)}), 500


@app.errorhandler(Exception)
def _handle_unexpected_exception(e):
    from werkzeug.exceptions import HTTPException
    if isinstance(e, HTTPException):
        return e  # let Flask's normal handling of 404/405/etc. through unchanged
    return jsonify({"ok": False, "error": "Internal server error", "detail": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
