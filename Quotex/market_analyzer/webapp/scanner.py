"""
Phase 7 — Smart Scanner Engine
================================
Pure orchestration layer around the existing analysis pipeline. This module
NEVER computes an indicator, NEVER runs confluence logic, and NEVER talks to
Quotex directly — it only calls the `run_pipeline(asset, timeframe)` async
callable supplied by app.py (i.e. the existing `_run_pipeline`, unchanged),
and reads fields that pipeline already produces.

Analysis only. No order is placed by this module or anything it calls.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from api_quotex.constants import TIMEFRAMES as _TF_SECONDS

try:
    from analyzer import calculate_filter_score
except ImportError:
    from market_analyzer.analyzer import calculate_filter_score

SCANNER_SCHEMA_VERSION = "1.0"
SCANNER_ENGINE_VERSION = "phase8.1"  # Phase 8.1: SettingsStore integration + progress tracking + status()/results() aliases (additive only — see CHANGELOG.md)

# ── States ──────────────────────────────────────────────────────────────────
STOPPED = "STOPPED"
RUNNING = "RUNNING"
PAUSED = "PAUSED"
YIELDING = "YIELDING"      # momentarily deferring to an in-flight manual request
DEGRADED = "DEGRADED"      # too many consecutive failures — cooling down
RECOVERING = "RECOVERING"  # cooldown elapsed, probing with one asset before resuming


def _now() -> float:
    return time.time()


def _iso(ts: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


@dataclass
class ScannerConfig:
    timeframes: List[str] = field(default_factory=lambda: ["5m"])
    asset_gap_seconds: float = 0.7          # pacing delay between assets
    cycle_interval_seconds: float = 120.0   # target time between cycle starts
    asset_timeout_seconds: float = 20.0     # per-asset _run_pipeline timeout
    min_payout: float = 80.0
    top_n: int = 10
    min_confidence: float = 70.0
    cache_ttl_seconds: float = 300.0        # entries older than this are purged
    degrade_after_consecutive_failures: int = 5
    degrade_cooldown_seconds: float = 30.0
    manual_priority_max_wait_seconds: float = 5.0


class ScannerEngine:
    """
    Sequential, single-flight scanner. Designed to run as ONE coroutine on
    the app's existing shared background event loop (_BG_LOOP) so it never
    introduces concurrent Quotex fetches — the same serialization guarantee
    /api/signal already relies on.
    """

    def __init__(
        self,
        run_pipeline: Callable[[str, str], Awaitable[Dict[str, Any]]],
        assets: List[str],
        invalidate_fetcher: Callable[[], None],
        adx_trending: float,
        config: Optional[ScannerConfig] = None,
        settings_store: Optional[Any] = None,
        live_data_lock: Optional[Callable[[], "asyncio.Lock"]] = None,
    ) -> None:
        self._run_pipeline = run_pipeline
        self._assets = list(assets)
        self._invalidate_fetcher = invalidate_fetcher
        self._adx_trending = adx_trending
        self.cfg = config or ScannerConfig()

        # Part 3 approved fix — Analyzer priority over Scanner. Optional
        # getter (mirrors app.py's _get_shared_fetcher()/_fetcher_lock()
        # lazy-creation pattern) for the lock shared with manual /api/signal
        # requests, so a Scanner fetch and a manual Analyzer fetch are never
        # simultaneously in-flight. If None (e.g. an older caller/test that
        # doesn't pass one), the scanner behaves exactly as it did before
        # this fix — no lock is used, matching prior behavior exactly.
        self._live_data_lock_getter = live_data_lock

        # Phase 8.1 — SettingsStore integration (fully optional/additive).
        # `settings_store` is duck-typed (anything with a .get() -> dict
        # method, matching webapp/settings_store.py's SettingsStore) rather
        # than imported by class, to avoid any import coupling between
        # scanner.py and settings_store.py. If None (or if reading it ever
        # fails for any reason), the scanner behaves EXACTLY as it did
        # before Phase 8.1 — every settings read below is wrapped so a
        # missing/broken settings_store can never break the scan loop.
        self._settings_store = settings_store
        # Effective per-run asset list — defaults to the full asset list
        # passed in, exactly like Phase 7 behavior. Only narrowed if
        # settings.scanner.enabled_assets is a non-empty list at start()
        # time.
        self._effective_assets: List[str] = list(assets)
        # Effective per-run minimum filter score gate for get_results()/
        # results() — 0.0 means "no additional gate", i.e. byte-identical
        # to pre-Phase-8.1 behavior (only the existing mandatory_pass gate
        # applies).
        self._minimum_filter_score: float = 0.0
        # Last settings snapshot actually applied at start() — exposed via
        # get_status() purely for operator visibility/debugging; not used
        # for any control-flow decision itself.
        self._settings_applied: Optional[Dict[str, Any]] = None

        self.state = STOPPED
        self._loop_task: Optional[asyncio.Task] = None
        self._stop_requested = False
        self._pause_event = asyncio.Event()
        self._pause_event.set()  # not paused by default

        # Phase 8.1 — progress tracking (reset every cycle in _scan_loop).
        self._assets_completed: int = 0

        # Manual-request priority (§8/§9 of the approved architecture)
        self.manual_requests_in_flight = 0

        # Cache: (asset, timeframe) -> result dict (already JSON-safe)
        self._cache: Dict[Tuple[str, str], Dict[str, Any]] = {}

        # M1 (diagnostics) — (asset, timeframe) -> last failure reason.
        # Purely additive bookkeeping alongside _cache above: _cache already
        # holds every SUCCESSFUL attempt (including WAIT), but a failed
        # attempt (candle error, timeout, exception) previously left no
        # per-asset trace anywhere — only aggregate counters/last_error.
        # This dict is written in the exact same try/except _scan_loop()
        # already has (no new control flow, no change to what is retried,
        # skipped, or how hard gates/signals are computed) and is read only
        # by the new get_diagnostics() accessor below.
        self._failures: Dict[Tuple[str, str], Dict[str, Any]] = {}

        # Metrics
        self._asset_durations: deque = deque(maxlen=200)  # rolling window, seconds
        self._success_count = 0
        self._failure_count = 0
        self.last_error: Optional[str] = None

        # History: last 10 completed cycles
        self.history: deque = deque(maxlen=10)

        # Event log: last 100 events
        self.events: deque = deque(maxlen=100)

        self.current_cycle = 0
        self.current_asset: Optional[str] = None
        # Phase 8.3 — additional status-panel fields, additive only.
        self.current_timeframe: Optional[str] = None
        self._last_scan_time: Optional[float] = None  # epoch of the most recent per-asset scan attempt (success or failure)
        self._cycle_started_at: Optional[float] = None
        self._consecutive_failures = 0
        self._last_full_cycle_duration: Optional[float] = None

    # ── Event log helper ─────────────────────────────────────────────────────
    def _log_event(self, kind: str, detail: str = "") -> None:
        self.events.append({"ts": _iso(_now()), "event": kind, "detail": detail})

    def _set_state(self, new_state: str, detail: str = "") -> None:
        if new_state != self.state:
            self._log_event("state_change", f"{self.state} -> {new_state}" + (f" ({detail})" if detail else ""))
        self.state = new_state

    # ── Phase 8.1: SettingsStore integration (additive, best-effort) ────────
    def _read_scanner_settings(self) -> Optional[Dict[str, Any]]:
        """
        Best-effort read of the `scanner` section of settings_store.get().
        Returns None if no settings_store was supplied, or if reading it
        fails for ANY reason (malformed file, missing key, wrong type,
        etc.) — a broken/absent settings_store must never be able to break
        scanner startup or the scan loop, so every caller of this method
        already treats None as "fall back to prior, pre-Phase-8.1 behavior".
        """
        if self._settings_store is None:
            return None
        try:
            data = self._settings_store.get()
            scanner_settings = data.get("scanner", {}) if isinstance(data, dict) else {}
            return scanner_settings if isinstance(scanner_settings, dict) else {}
        except Exception as exc:  # noqa: BLE001 — never let a settings read crash the scanner
            self._log_event("settings_read_failed", str(exc))
            return None

    # ── Lifecycle control (called from Flask routes, NOT from the loop itself) ─
    def start(self, loop: asyncio.AbstractEventLoop, timeframes: Optional[List[str]] = None,
              top_n: Optional[int] = None, min_confidence: Optional[float] = None,
              refresh_seconds: Optional[float] = None,
              assets: Optional[List[str]] = None) -> Dict[str, Any]:
        if self.state != STOPPED:
            return {"ok": False, "message": f"Scanner already {self.state}"}

        # Phase 8.1 — SettingsStore integration. `settings` is None whenever
        # no settings_store was supplied (or a read failed), in which case
        # every line below is a no-op and start() behaves exactly as it did
        # before Phase 8.1 (explicit args only, same defaults as ScannerConfig).
        settings = self._read_scanner_settings()

        if settings is not None and settings.get("scanner_enabled", True) is False:
            self._log_event("scanner_start_blocked", "settings.scanner.scanner_enabled=false")
            return {"ok": False, "message": "Scanner is disabled in Settings (scanner.scanner_enabled=false)"}

        settings = settings or {}
        # Explicit start()-call args always win over settings (matches the
        # existing Settings-vs-request-override precedence already used
        # elsewhere in this project, e.g. app.py's filter-score overrides).
        # An empty/falsy settings value (missing key, empty list, 0, None)
        # is treated as "not set" so pre-Phase-8.1 defaults are preserved.
        effective_timeframes = timeframes or settings.get("enabled_timeframes") or None
        effective_top_n = top_n or settings.get("top_signals") or None
        effective_refresh = refresh_seconds or settings.get("scan_interval") or None
        settings_assets = settings.get("enabled_assets") or None
        settings_min_filter_score = settings.get("minimum_filter_score")

        if effective_timeframes:
            self.cfg.timeframes = effective_timeframes
        if effective_top_n:
            self.cfg.top_n = effective_top_n
        if min_confidence is not None:
            self.cfg.min_confidence = min_confidence
        if effective_refresh:
            self.cfg.cycle_interval_seconds = effective_refresh

        # Narrow the per-run asset list only if settings name a non-empty
        # subset that actually intersects the known asset list; otherwise
        # fall back to the full list exactly as before Phase 8.1.
        #
        # Live Asset Availability (additive): an explicit `assets` list
        # passed to this call (e.g. app.py's live Quotex-availability
        # snapshot) takes top precedence over both of the above — the
        # scanner then scans EXACTLY that snapshot, not the static
        # self._assets universe. Omitting `assets` (the default) leaves
        # this whole block byte-identical to pre-existing behavior.
        if assets is not None:
            self._effective_assets = list(assets)
        elif settings_assets:
            narrowed = [a for a in settings_assets if a in self._assets]
            self._effective_assets = narrowed or list(self._assets)
        else:
            self._effective_assets = list(self._assets)

        try:
            self._minimum_filter_score = float(settings_min_filter_score) if settings_min_filter_score else 0.0
        except (TypeError, ValueError):
            self._minimum_filter_score = 0.0

        self._settings_applied = dict(settings) if settings else None

        self._stop_requested = False
        self._pause_event.set()
        self._assets_completed = 0
        self._set_state(RUNNING, "start requested")
        self._log_event(
            "scanner_start",
            f"timeframes={self.cfg.timeframes} top_n={self.cfg.top_n} "
            f"assets={len(self._effective_assets)} min_filter_score={self._minimum_filter_score}",
        )
        self._loop_task = asyncio.run_coroutine_threadsafe(self._scan_loop(), loop)
        return {"ok": True, "message": "Scanner started"}

    def stop(self) -> Dict[str, Any]:
        if self.state == STOPPED:
            return {"ok": False, "message": "Scanner already stopped"}
        self._stop_requested = True
        self._pause_event.set()  # wake it if paused, so it can see the stop flag
        self._log_event("scanner_stop", "stop requested")
        return {"ok": True, "message": "Stop requested — scanner will halt at the next asset boundary"}

    def pause(self) -> Dict[str, Any]:
        if self.state == STOPPED:
            return {"ok": False, "message": "Scanner is not running"}
        self._pause_event.clear()
        self._set_state(PAUSED, "pause requested")
        self._log_event("scanner_pause", "")
        return {"ok": True, "message": "Scanner paused"}

    def resume(self) -> Dict[str, Any]:
        if self.state != PAUSED:
            return {"ok": False, "message": f"Scanner is not paused (state={self.state})"}
        self._pause_event.set()
        self._set_state(RUNNING, "resume requested")
        self._log_event("scanner_resume", "")
        return {"ok": True, "message": "Scanner resumed"}

    # ── Manual-request priority (§8/§9) ─────────────────────────────────────
    def manual_request_started(self) -> None:
        self.manual_requests_in_flight += 1

    def manual_request_finished(self) -> None:
        self.manual_requests_in_flight = max(0, self.manual_requests_in_flight - 1)

    async def _yield_to_manual_requests(self) -> None:
        if self.manual_requests_in_flight <= 0:
            return
        prev_state = self.state
        self._set_state(YIELDING, f"{self.manual_requests_in_flight} manual request(s) in flight")
        waited = 0.0
        step = 0.1
        while self.manual_requests_in_flight > 0 and waited < self.cfg.manual_priority_max_wait_seconds:
            await asyncio.sleep(step)
            waited += step
        self._set_state(prev_state if prev_state != YIELDING else RUNNING)

    async def _acquire_live_data_lock_with_priority(self) -> Optional["asyncio.Lock"]:
        """
        Part 3 approved fix. Called immediately before EVERY single
        _run_pipeline() call the scan loop makes (not just once per asset),
        closing the gap where a manual request starting mid-fetch could
        previously overlap with a Scanner fetch on the same shared Quotex
        session. Always defers to an in-flight manual (Analyzer) request:
          1. Yields (via _yield_to_manual_requests(), unchanged) while a
             manual request is currently active.
          2. Acquires the shared live-data lock.
          3. Re-checks manual_requests_in_flight immediately after
             acquiring — closes the race where a manual request starts in
             the instant between the check in step 1 and the acquire in
             step 2. If one slipped in, releases and loops back to step 1.
          4. Returns the acquired, held lock (caller MUST release it in a
             finally block) once neither race applies.
        Returns None (no-op) if no lock getter was configured, so the
        Scanner still runs exactly as before if it's ever constructed
        without one (e.g. an older test).
        """
        if self._live_data_lock_getter is None:
            return None
        while True:
            await self._yield_to_manual_requests()
            lock = self._live_data_lock_getter()
            await lock.acquire()
            if self.manual_requests_in_flight > 0:
                lock.release()
                continue
            return lock

    # ── Cache ────────────────────────────────────────────────────────────────
    def _store_result(self, asset: str, timeframe: str, result: Dict[str, Any],
                       duration_seconds: float) -> None:
        """
        result already contains filter_score/passed_filters/failed_filters/
        filter_breakdown — computed ONCE by _run_pipeline() via
        analyzer.calculate_filter_score(). Phase 7 STEP 1: scanner.py no
        longer computes its own gates; this is purely a passthrough + cache
        bookkeeping step.
        """
        entry = dict(result)  # shallow copy — result is already the JSON-safe dict _run_pipeline returns
        entry["schema_version"] = SCANNER_SCHEMA_VERSION
        entry["engine_version"] = SCANNER_ENGINE_VERSION
        entry["last_update"] = _iso(_now())
        entry["scan_duration_ms"] = round(duration_seconds * 1000, 1)
        entry["scanner_cycle"] = self.current_cycle
        self._cache[(asset, timeframe)] = entry

    @staticmethod
    def _parse_iso_epoch(ts: str) -> float:
        """
        Best-effort ISO-8601 (`%Y-%m-%dT%H:%M:%SZ`, as produced by `_iso()`)
        -> epoch seconds, for numeric freshness comparisons. Returns 0.0 (the
        oldest possible value) on any parse failure so a malformed/missing
        timestamp sorts last, never crashes ranking. Phase 8.3 — extracted
        so both `_cleanup_stale_cache()` and `get_results()`'s ranking can
        share one implementation instead of duplicating the parse.
        """
        try:
            return time.mktime(time.strptime(ts, "%Y-%m-%dT%H:%M:%SZ"))
        except Exception:
            return 0.0

    def _cleanup_stale_cache(self) -> int:
        """TTL-based automatic cleanup. Returns number of entries purged."""
        cutoff = _now() - self.cfg.cache_ttl_seconds
        stale_keys = []
        for key, entry in self._cache.items():
            entry_ts = self._parse_iso_epoch(entry.get("last_update", ""))
            if entry_ts == 0.0:
                continue  # unparseable — leave it, matches prior behavior of skipping on parse failure
            if entry_ts < cutoff:
                stale_keys.append(key)
        for key in stale_keys:
            del self._cache[key]
        if stale_keys:
            self._log_event("cache_cleanup", f"purged {len(stale_keys)} stale entries")
        return len(stale_keys)

    # ── The scan loop itself ────────────────────────────────────────────────
    async def _scan_loop(self) -> None:
        try:
            while not self._stop_requested:
                await self._pause_event.wait()
                if self._stop_requested:
                    break

                self.current_cycle += 1
                self._cycle_started_at = _now()
                self._assets_completed = 0  # Phase 8.1 — reset progress counter each cycle
                self._log_event("cycle_start", f"cycle={self.current_cycle}")
                cycle_success = 0
                cycle_failure = 0

                for asset in self._effective_assets:
                    if self._stop_requested:
                        break
                    await self._pause_event.wait()
                    if self._stop_requested:
                        break

                    await self._yield_to_manual_requests()

                    if self.state == DEGRADED:
                        remaining = self.cfg.degrade_cooldown_seconds
                        chunk = 0.2
                        while remaining > 0 and not self._stop_requested:
                            await asyncio.sleep(min(chunk, remaining))
                            remaining -= chunk
                        if self._stop_requested:
                            break
                        self._set_state(RECOVERING, "cooldown elapsed, probing")

                    self.current_asset = asset
                    for tf in self.cfg.timeframes:
                        if self._stop_requested:
                            break
                        self.current_timeframe = tf  # Phase 8.3 — status-panel field
                        # Part 3 approved fix — acquire the shared live-data
                        # lock (with manual-request priority) immediately
                        # before THIS specific fetch, not just once per
                        # asset. Ensures this fetch and any manual /api/signal
                        # request are never simultaneously in-flight.
                        _live_lock = await self._acquire_live_data_lock_with_priority()
                        t_start = _now()
                        try:
                            try:
                                result = await asyncio.wait_for(
                                    self._run_pipeline(asset, tf),
                                    timeout=self.cfg.asset_timeout_seconds,
                                )
                            finally:
                                if _live_lock is not None:
                                    _live_lock.release()
                            duration = _now() - t_start
                            self._asset_durations.append(duration)

                            if "error" in result:
                                raise RuntimeError(result["error"])

                            self._store_result(asset, tf, result, duration)
                            # M1 — this attempt succeeded, so any earlier
                            # failure record for this asset/timeframe is
                            # stale; drop it so diagnostics reflects the
                            # latest attempt only (mirrors _cache's own
                            # latest-wins semantics for successes).
                            self._failures.pop((asset, tf), None)

                            self._success_count += 1
                            cycle_success += 1
                            self._consecutive_failures = 0
                            if self.state in (DEGRADED, RECOVERING):
                                self._set_state(RUNNING, "recovered")

                        except Exception as exc:  # noqa: BLE001 — logged, never crashes the loop
                            duration = _now() - t_start
                            self._asset_durations.append(duration)
                            self._failure_count += 1
                            cycle_failure += 1
                            self._consecutive_failures += 1
                            self.last_error = f"{asset}[{tf}]: {exc}"
                            self._log_event("asset_failed", self.last_error)
                            # M1 — per-asset diagnostic record of WHY this
                            # asset/timeframe produced no result this
                            # attempt (candle error, timeout, exception from
                            # the pipeline, etc.) — read-only bookkeeping,
                            # does not change retry/skip behavior below.
                            self._failures[(asset, tf)] = {
                                "asset": asset,
                                "timeframe": tf,
                                "error": str(exc),
                                "error_type": type(exc).__name__,
                                "timestamp": _iso(_now()),
                                "scanner_cycle": self.current_cycle,
                            }

                            if isinstance(exc, (ConnectionError, OSError, RuntimeError, asyncio.TimeoutError)):
                                self._invalidate_fetcher()

                            if self._consecutive_failures >= self.cfg.degrade_after_consecutive_failures:
                                self._set_state(DEGRADED, f"{self._consecutive_failures} consecutive failures")
                                self._log_event("degraded", self.last_error or "")

                        await asyncio.sleep(self.cfg.asset_gap_seconds)
                        self._last_scan_time = _now()  # Phase 8.3 — every attempt, success or failure

                    # Phase 8.1 — progress tracking: count this asset as
                    # "completed" once all its configured timeframes have
                    # been attempted (success or failure both count, same
                    # as backtest_engine.py's candles_processed semantics —
                    # it tracks work attempted, not just successful work).
                    self._assets_completed += 1

                self.current_asset = None
                self.current_timeframe = None  # Phase 8.3
                # Cycle-finalization work (cache cleanup, history bookkeeping)
                # is wrapped so an unexpected exception here can never kill
                # the outer scan loop — it would otherwise propagate past
                # this function's try/finally and silently end continuous
                # scanning after just one cycle. Per-asset/per-timeframe
                # fetch errors already have their own handling above; this
                # is specifically for anything after that inner loop.
                try:
                    self._cleanup_stale_cache()

                    cycle_duration = _now() - self._cycle_started_at
                    self._last_full_cycle_duration = cycle_duration
                    self.history.append({
                        "cycle": self.current_cycle,
                        "started_at": _iso(self._cycle_started_at),
                        "ended_at": _iso(_now()),
                        "duration_seconds": round(cycle_duration, 2),
                        "assets_scanned": cycle_success + cycle_failure,
                        "assets_succeeded": cycle_success,
                        "assets_failed": cycle_failure,
                    })
                    self._log_event("cycle_complete", f"cycle={self.current_cycle} duration={cycle_duration:.1f}s "
                                                       f"success={cycle_success} failed={cycle_failure}")
                except Exception as exc:  # noqa: BLE001 — logged, never kills continuous scanning
                    cycle_duration = _now() - self._cycle_started_at
                    self._log_event("cycle_finalize_error", f"cycle={self.current_cycle} error={exc}")

                if self._stop_requested:
                    break
                sleep_for = max(0.0, self.cfg.cycle_interval_seconds - cycle_duration)
                # Chunked sleep so stop()/pause() take effect promptly instead
                # of waiting out the full inter-cycle interval (bug found and
                # fixed during Phase 7 validation testing).
                remaining = sleep_for
                chunk = 0.2
                while remaining > 0 and not self._stop_requested:
                    await asyncio.sleep(min(chunk, remaining))
                    remaining -= chunk
        finally:
            self._set_state(STOPPED, "loop exited")
            self._log_event("scanner_stopped", "")
            self.current_asset = None

    # ── M1: Full Scanner Diagnostics (read-only, additive) ──────────────────
    def get_diagnostics(self) -> Dict[str, Any]:
        """
        Returns ONE entry per (asset, timeframe) in the scanner's current
        configured universe (self._effective_assets x self.cfg.timeframes) —
        unlike get_results(), nothing here is filtered out. Built entirely
        from data _scan_loop() already computes and stores (_cache /
        _failures above); this method performs no new analysis, calls no
        pipeline, and cannot alter or weaken any hard gate, filter score, or
        signal — it only classifies and surfaces what already happened.

        Per-entry `status` is one of:
          "SIGNAL"  — mandatory_pass True AND signal is BUY/SELL (this is
                      the subset get_results() surfaces, subject also to
                      its own confidence/min_filter_score thresholds there)
          "WAIT"    — a completed attempt exists but produced no actionable
                      signal — either a hard gate failed (mandatory_pass is
                      False; see `failed_filters`) or the confluence vote
                      itself came back WAIT
          "FAILED"  — the most recent attempt raised an exception (candle
                      fetch error, timeout, connection issue, indicator
                      calc error, etc.) — see `error`/`error_type`
          "SKIPPED" — no attempt has been recorded yet for this asset/
                      timeframe: the scanner hasn't reached it this cycle
                      yet, or no run has started. This is an honest "no
                      data yet" state, never a fabricated result.
        """
        entries: List[Dict[str, Any]] = []
        for asset in self._effective_assets:
            for tf in self.cfg.timeframes:
                key = (asset, tf)
                cached = self._cache.get(key)
                failure = self._failures.get(key)

                if cached is not None:
                    confluence = cached.get("confluence") or {}
                    signal = confluence.get("signal")
                    mandatory_pass = cached.get("mandatory_pass")
                    failed_filters = cached.get("failed_filters")
                    if mandatory_pass and signal in ("BUY", "SELL"):
                        status = "SIGNAL"
                        reason = None
                    elif not mandatory_pass:
                        status = "WAIT"
                        reason = (f"Hard gate failed: {', '.join(failed_filters)}"
                                  if failed_filters else "Hard gate failed")
                    else:
                        status = "WAIT"
                        reason = "No qualifying signal (confluence vote did not reach BUY/SELL)"
                    entries.append({
                        "asset": asset, "timeframe": tf, "status": status,
                        "signal": signal, "confidence": confluence.get("confidence"),
                        "filter_score": cached.get("filter_score"),
                        "mandatory_pass": mandatory_pass,
                        "failed_filters": failed_filters,
                        "passed_filters": cached.get("passed_filters"),
                        "filter_breakdown": cached.get("filter_breakdown"),
                        "candle_count": cached.get("candle_count"),
                        "payout_pct": cached.get("payout_pct"),
                        "reason": reason, "error": None, "error_type": None,
                        "last_update": cached.get("last_update"),
                    })
                elif failure is not None:
                    entries.append({
                        "asset": asset, "timeframe": tf, "status": "FAILED",
                        "signal": None, "confidence": None, "filter_score": None,
                        "mandatory_pass": None, "failed_filters": None,
                        "passed_filters": None, "filter_breakdown": None,
                        "candle_count": None, "payout_pct": None,
                        "reason": failure.get("error"), "error": failure.get("error"),
                        "error_type": failure.get("error_type"),
                        "last_update": failure.get("timestamp"),
                    })
                else:
                    entries.append({
                        "asset": asset, "timeframe": tf, "status": "SKIPPED",
                        "signal": None, "confidence": None, "filter_score": None,
                        "mandatory_pass": None, "failed_filters": None,
                        "passed_filters": None, "filter_breakdown": None,
                        "candle_count": None, "payout_pct": None,
                        "reason": "Not scanned yet — scanner hasn't reached this asset/timeframe, or no run has started",
                        "error": None, "error_type": None, "last_update": None,
                    })

        counts = {
            "signal": sum(1 for e in entries if e["status"] == "SIGNAL"),
            "wait": sum(1 for e in entries if e["status"] == "WAIT"),
            "failed": sum(1 for e in entries if e["status"] == "FAILED"),
            "skipped": sum(1 for e in entries if e["status"] == "SKIPPED"),
        }
        return {
            "generated_at": _iso(_now()),
            "cycle": self.current_cycle,
            "total": len(entries),
            "counts": counts,
            "entries": entries,
        }

    # ── Metrics / status / results (read-only, safe from Flask WSGI threads) ─
    def get_metrics(self) -> Dict[str, Any]:
        total = self._success_count + self._failure_count
        avg_asset = (sum(self._asset_durations) / len(self._asset_durations)
                     if self._asset_durations else None)
        avg_cycle = (sum(h["duration_seconds"] for h in self.history) / len(self.history)
                     if self.history else None)
        return {
            "average_asset_scan_time_seconds": round(avg_asset, 3) if avg_asset is not None else None,
            "average_cycle_time_seconds": round(avg_cycle, 2) if avg_cycle is not None else None,
            "success_rate": round(self._success_count / total, 4) if total else None,
            "failure_rate": round(self._failure_count / total, 4) if total else None,
            "last_error": self.last_error,
        }

    def get_status(self) -> Dict[str, Any]:
        # Phase 8.1 — progress tracking. `total_assets` now reflects the
        # EFFECTIVE per-run asset list (narrowed by settings.enabled_assets
        # if that was set at start() time); with no settings_store, or with
        # enabled_assets unset/empty, self._effective_assets always equals
        # self._assets — so this is byte-identical to pre-Phase-8.1 output
        # in the default case.
        total_assets = len(self._effective_assets)
        elapsed_time = None
        percent_complete = 0.0
        estimated_remaining = None
        if self._cycle_started_at is not None and self.state != STOPPED:
            elapsed_time = round(_now() - self._cycle_started_at, 1)
            if total_assets:
                percent_complete = round(min(100.0, (self._assets_completed / total_assets) * 100), 1)
            if percent_complete > 0 and elapsed_time is not None:
                estimated_remaining = round(elapsed_time / (percent_complete / 100.0) - elapsed_time, 1)

        return {
            "state": self.state,
            "running": self.state not in (STOPPED,),
            "paused": self.state == PAUSED,
            "degraded": self.state in (DEGRADED, RECOVERING),
            "total_assets": total_assets,
            "current_asset": self.current_asset,
            "current_timeframe": self.current_timeframe,  # Phase 8.3
            "last_scan_time": _iso(self._last_scan_time) if self._last_scan_time else None,  # Phase 8.3
            "assets_completed": self._assets_completed,
            "percent_complete": percent_complete,
            "elapsed_time": elapsed_time,
            "estimated_remaining": estimated_remaining,
            "current_cycle": self.current_cycle,
            "cycle_started_at": _iso(self._cycle_started_at) if self._cycle_started_at else None,
            "last_full_cycle_duration_seconds": (
                round(self._last_full_cycle_duration, 2) if self._last_full_cycle_duration else None
            ),
            "refresh_seconds": self.cfg.cycle_interval_seconds,
            "asset_gap_seconds": self.cfg.asset_gap_seconds,
            "timeframes": self.cfg.timeframes,
            "cached_results": len(self._cache),
            "manual_requests_in_flight": self.manual_requests_in_flight,
            "metrics": self.get_metrics(),
            "history": list(self.history),
            "events": list(self.events)[-25:],  # recent slice; full log via a dedicated field if needed
            "settings_enabled": self._settings_store is not None,  # Phase 8.1 — visibility only
            "minimum_filter_score": self._minimum_filter_score,     # Phase 8.1
            "schema_version": SCANNER_SCHEMA_VERSION,
            "engine_version": SCANNER_ENGINE_VERSION,
        }

    def get_results(self, min_confidence: Optional[float] = None,
                     timeframe: Optional[str] = None, limit: Optional[int] = None) -> Dict[str, Any]:
        min_conf = min_confidence if min_confidence is not None else self.cfg.min_confidence
        limit = limit or self.cfg.top_n

        candidates = []
        for (asset, tf), entry in self._cache.items():
            if timeframe and tf != timeframe:
                continue
            # Phase 7.1 STEP 3: filter_score is now a GRADED quality metric
            # and is no longer forced to 0 on a mandatory gate failure — so
            # it can no longer be used as a visibility proxy. Use the new
            # mandatory_pass field instead (same original binary hard-gate
            # semantics as Phase 7.0's gate check). A hidden signal still
            # keeps its filter_score cached for ranking/analysis purposes —
            # it's just excluded from the scanner's surfaced results here.
            if not entry.get("mandatory_pass"):
                continue  # a mandatory gate failed -> hidden from results, per spec
            # Phase 8.1 — optional additional quality gate from Settings.
            # self._minimum_filter_score defaults to 0.0 (no settings_store,
            # or minimum_filter_score unset/0 in settings), which never
            # excludes anything beyond the existing mandatory_pass gate —
            # byte-identical to pre-Phase-8.1 behavior in that case.
            if self._minimum_filter_score and (entry.get("filter_score", 0) or 0) < self._minimum_filter_score:
                continue
            confluence = entry.get("confluence", {})
            signal = confluence.get("signal")
            confidence = confluence.get("confidence", 0) or 0
            if signal in (None, "WAIT"):
                continue
            if confidence < min_conf:
                continue
            candidates.append(entry)

        # Ranking (Phase 7 STEP 4/5): filter_score desc (primary), confidence
        # desc, payout desc, last_update (freshness) desc.
        # Phase 8.3 bug fix: freshness was previously compared as a raw ISO
        # string with no negation, which sorts ascending (oldest-first) on
        # ties — the opposite of the "freshness desc" (most-recent-first)
        # this comment always said it implemented. Parsing to epoch and
        # negating fixes the tie-break direction; filter_score/confidence/
        # payout ordering (unaffected by this bug) is unchanged.
        def _rank_key(e: Dict[str, Any]):
            filter_score = e.get("filter_score", 0) or 0
            confidence = e.get("confluence", {}).get("confidence", 0) or 0
            payout = e.get("payout_pct") or 0
            freshness_epoch = self._parse_iso_epoch(e.get("last_update", ""))
            return (-filter_score, -confidence, -payout, -freshness_epoch)

        candidates.sort(key=_rank_key)
        top = candidates[:limit]
        for i, entry in enumerate(top, start=1):
            entry["rank"] = i

        return {
            "top_signals": top,
            "generated_at": _iso(_now()),
            "cycle": self.current_cycle,
            "schema_version": SCANNER_SCHEMA_VERSION,
            "engine_version": SCANNER_ENGINE_VERSION,
        }

    def get_recommendations(self, min_confidence: Optional[float] = None,
                             limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Phase 12 — dynamic multi-timeframe recommendation, built ONLY from
        data _run_pipeline() already computes for every cached (asset, tf)
        cell (self._cache, filled once per cycle by the SAME loop that
        already scans every configured timeframe for every asset — no new
        scan path, no second signal engine).

        For each asset, considers every timeframe currently cached for it
        and picks the single best candidate using the EXACT SAME ranking
        key already used by get_results() (filter_score desc, confidence
        desc, payout desc, freshness desc) — so "best" here means exactly
        what the rest of the scanner already means by "best", not a new
        invented score.

        Status derivation (uses only fields that already exist — no
        fabricated AI/indicator votes):
          TRADE       — best candidate passes mandatory_pass (all existing
                        hard gates: ema_trend/adx/atr/support_resistance/
                        payout) AND confidence >= min_conf (the scanner's
                        own existing confidence bar) — i.e. it would
                        already qualify for get_results() today.
          WATCH       — best candidate is a real BUY/SELL and passes
                        mandatory_pass, but confidence is below min_conf —
                        directionally real per existing gates, just not
                        strong enough to clear the existing bar.
          NOT_TRADE   — best candidate is a real BUY/SELL but fails
                        mandatory_pass (an existing hard gate rejected it).
          (asset omitted) — every cached timeframe for that asset is WAIT
                        or missing — "NO RECOMMENDATION" per spec; this
                        method never fabricates a recommendation for an
                        asset with no real candidate.
        """
        min_conf = min_confidence if min_confidence is not None else self.cfg.min_confidence
        limit = limit or self.cfg.top_n

        by_asset: Dict[str, List[Dict[str, Any]]] = {}
        for (asset, tf), entry in self._cache.items():
            by_asset.setdefault(asset, []).append(dict(entry, timeframe=tf))

        def _rank_key(e: Dict[str, Any]):
            filter_score = e.get("filter_score", 0) or 0
            confidence = e.get("confluence", {}).get("confidence", 0) or 0
            payout = e.get("payout_pct") or 0
            freshness_epoch = self._parse_iso_epoch(e.get("last_update", ""))
            return (-filter_score, -confidence, -payout, -freshness_epoch)

        recommendations = []
        for asset, entries in by_asset.items():
            # Full per-timeframe comparison table (Section G) — includes
            # WAIT rows too, so the UI can show e.g. "15M WAIT" alongside
            # the actionable timeframes, exactly as the spec's example
            # shows. Never used to produce a recommendation by itself.
            timeframe_table = sorted(
                [
                    {
                        "timeframe": e["timeframe"],
                        "signal": e.get("confluence", {}).get("signal"),
                        "confidence": e.get("confluence", {}).get("confidence", 0) or 0,
                        "filter_score": e.get("filter_score", 0) or 0,
                        "mandatory_pass": bool(e.get("mandatory_pass")),
                        "entry_window_end": e.get("entry_window_end"),
                    }
                    for e in entries
                ],
                key=lambda row: _TF_SECONDS.get(row["timeframe"], 0),
            )

            candidates = [e for e in entries if (e.get("confluence", {}).get("signal") in ("BUY", "SELL"))]
            if not candidates:
                continue  # every cached timeframe is WAIT/missing -> NO RECOMMENDATION for this asset

            # Ranking happens ONLY among valid (mandatory_pass) candidates
            # first — a timeframe that fails an existing hard gate must
            # never shadow/outrank a different timeframe on the SAME asset
            # that actually clears every gate. Only when literally none of
            # the asset's candidates pass do we fall back to the best of
            # the failing ones, purely so it can still be surfaced as
            # NOT_TRADE (never as TRADE/WATCH).
            passing = [e for e in candidates if e.get("mandatory_pass")]
            if passing:
                best = min(passing, key=_rank_key)
            else:
                best = min(candidates, key=_rank_key)
            best_conf = best.get("confluence", {})
            best_confidence = best_conf.get("confidence", 0) or 0

            if best.get("mandatory_pass") and best_confidence >= min_conf:
                status = "TRADE"
            elif best.get("mandatory_pass"):
                status = "WATCH"
            else:
                status = "NOT_TRADE"

            recommendations.append({
                "asset": asset,
                "status": status,
                "recommended_timeframe": best["timeframe"],
                "signal": best_conf.get("signal"),
                "confidence": best_confidence,
                "filter_score": best.get("filter_score", 0) or 0,
                "payout_pct": best.get("payout_pct"),
                "regime": best.get("regime"),
                "multi_tf_status": best.get("multi_tf_status"),
                "indicators": best.get("indicators"),
                "entry_window_end": best.get("entry_window_end"),
                "candle_start": best.get("candle_start"),
                "generated_at": best.get("generated_at"),
                "timeframes": timeframe_table,
            })

        recommendations.sort(key=lambda r: (
            {"TRADE": 0, "WATCH": 1, "NOT_TRADE": 2}.get(r["status"], 3),
            -(r["filter_score"] or 0), -(r["confidence"] or 0),
        ))
        return {
            "recommendations": recommendations[:limit],
            "generated_at": _iso(_now()),
            "cycle": self.current_cycle,
            "schema_version": SCANNER_SCHEMA_VERSION,
            "engine_version": SCANNER_ENGINE_VERSION,
        }

    # ── Phase 8.1: compatibility aliases ────────────────────────────────────
    # Pure aliases, added ONLY because the Phase 8.1 spec asked for a
    # `status()`/`results()` name alongside the existing `get_status()`/
    # `get_results()` — no new behavior, no parameter changes, nothing else
    # calls these internally (the loop and existing routes still use the
    # original get_status()/get_results() names, unchanged).
    def status(self) -> Dict[str, Any]:
        """Alias for get_status() — see that method for the full contract."""
        return self.get_status()

    def results(self, min_confidence: Optional[float] = None,
                timeframe: Optional[str] = None, limit: Optional[int] = None) -> Dict[str, Any]:
        """Alias for get_results() — see that method for the full contract."""
        return self.get_results(min_confidence=min_confidence, timeframe=timeframe, limit=limit)
