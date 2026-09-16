"""
Phase 7.2 — Backtest Engine
============================
A standalone module (deliberately NOT merged into scanner.py, per Phase 7.2's
explicit requirement that the Smart Scanner architecture stay untouched).
Mirrors scanner.py's proven async lifecycle pattern (STOPPED/RUNNING/PAUSED,
pause/resume via asyncio.Event, one instance submitted onto the existing
shared _BG_LOOP — this module never creates its own event loop).

Reuses ONLY existing, unmodified functions:
  - backtest.backtest_factor_accuracy()
  - backtest.compute_dynamic_weights()
  - backtest.backtest_filter_score_report()
No indicator, confluence, or filter-score math is duplicated here. Does not
import or touch analyzer.py, scanner.py, the Quotex API, or WebSocket code.

Analysis only. Never places an order. Never mutates indicator weights on its
own — results (including suggested weights) are kept IN MEMORY ONLY on this
engine instance. Writing them into settings.json is a separate, explicit,
user-triggered action (Settings page / a future route), not performed here.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional

try:
    from backtest import backtest_factor_accuracy, compute_dynamic_weights, backtest_filter_score_report
except ImportError:
    from market_analyzer.backtest import (
        backtest_factor_accuracy, compute_dynamic_weights, backtest_filter_score_report,
    )

STOPPED = "STOPPED"
RUNNING = "RUNNING"
PAUSED = "PAUSED"

# Phase 7.4: expanded per explicit spec (500/1000/1500/2000/3000/5000,
# default 2000). This is a config constant only — the run loop / gate /
# summary logic above and below is untouched.
CANDLE_OPTIONS = (500, 1000, 1500, 2000, 3000, 5000)
DEFAULT_CANDLE_COUNT = 2000


def _now() -> float:
    return time.time()


def _iso(ts: Optional[float]) -> Optional[str]:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts)) if ts else None


class BacktestEngine:
    """
    fetch_candles: async callable(asset, timeframe, count) -> DataFrame,
    supplied by app.py — reuses the SAME fetcher/session machinery
    _run_pipeline already uses. This module never talks to Quotex itself,
    so it cannot introduce a second concurrent Quotex connection.
    """

    def __init__(self, fetch_candles: Callable[[str, str, int], Awaitable[Any]]):
        self._fetch_candles = fetch_candles

        self.state = STOPPED
        self._stop_requested = False
        self._cancelled = False          # True if stop() interrupted a run; False if it finished naturally
        self._pause_event = asyncio.Event()
        self._pause_event.set()

        # Progress tracking (exact field names per spec)
        self.current_asset: Optional[str] = None
        self.current_indicator: Optional[str] = None
        self.candles_processed = 0
        self.total_candles = 0
        # P0 fix — asset-count-based progress (see start()'s comment)
        self.total_assets = 0
        self.assets_processed = 0
        self.started_at: Optional[float] = None
        self.finished_at: Optional[float] = None
        self.last_error: Optional[str] = None

        self.results: Dict[str, Any] = {}     # per-asset: accuracy/weights/filter-score-report/timestamp
        self.summary: Optional[Dict[str, Any]] = None

    # ── Lifecycle: start / stop / pause / resume ────────────────────────────
    def start(self, loop: asyncio.AbstractEventLoop, assets: List[str], timeframe: str,
              candle_count: int = DEFAULT_CANDLE_COUNT, lookahead: int = 4) -> Dict[str, Any]:
        """
        Submits the backtest coroutine onto the CALLER-SUPPLIED loop (the
        app's existing shared _BG_LOOP) — this method does not create or
        manage any event loop itself.

        Safety: one backtest at a time. A start() call while already
        RUNNING/PAUSED is ignored (returns ok=False) rather than queuing or
        interrupting the in-progress run.
        """
        if self.state != STOPPED:
            return {"ok": False, "message": f"Backtest already {self.state} — ignoring duplicate start request"}

        if candle_count not in CANDLE_OPTIONS:
            return {"ok": False, "message": f"candle_count must be one of {CANDLE_OPTIONS}, got {candle_count}"}
        if not assets:
            return {"ok": False, "message": "No assets specified"}

        self._stop_requested = False
        self._cancelled = False
        self._pause_event.set()
        self.state = RUNNING
        self.results = {}
        self.summary = None
        self.candles_processed = 0
        self.total_candles = candle_count * len(assets)
        # P0 fix — progress model: percent_complete is now based on ASSETS
        # actually processed (attempted, regardless of outcome), not on
        # candles fetched. total_candles/candles_processed are kept as
        # informational secondary stats only (back-compat for anything
        # already reading them) — see status()'s updated docstring.
        self.total_assets = len(assets)
        self.assets_processed = 0
        self.started_at = _now()
        self.finished_at = None
        self.last_error = None
        self.current_asset = None
        self.current_indicator = None

        asyncio.run_coroutine_threadsafe(
            self._run_loop(assets, timeframe, candle_count, lookahead), loop
        )
        return {"ok": True, "message": "Backtest started", "total_candles": self.total_candles}

    def stop(self) -> Dict[str, Any]:
        """
        Requests a stop. Per spec: "Stop immediately after the current work
        unit" — the in-progress asset's fetch/compute is allowed to finish
        (never interrupting an active Quotex request), then the loop halts
        at the next checkpoint rather than starting another asset.
        """
        if self.state == STOPPED:
            return {"ok": False, "message": "Backtest is not running"}
        self._stop_requested = True
        self._cancelled = True
        self._pause_event.set()   # wake it if paused, so it can observe the stop flag
        return {"ok": True, "message": "Stop requested — will halt after the current work unit completes"}

    def pause(self) -> Dict[str, Any]:
        """Pause only takes effect between work units (asset boundaries),
        never mid-fetch — same guarantee as stop()."""
        if self.state != RUNNING:
            return {"ok": False, "message": f"Cannot pause from state={self.state}"}
        self._pause_event.clear()
        self.state = PAUSED
        return {"ok": True, "message": "Backtest paused"}

    def resume(self) -> Dict[str, Any]:
        if self.state != PAUSED:
            return {"ok": False, "message": f"Backtest is not paused (state={self.state})"}
        self._pause_event.set()
        self.state = RUNNING
        return {"ok": True, "message": "Backtest resumed"}

    # ── The backtest loop ────────────────────────────────────────────────────
    async def _run_loop(self, assets: List[str], timeframe: str, candle_count: int, lookahead: int) -> None:
        try:
            for asset in assets:
                # Checkpoint 1: honor stop/pause BEFORE starting the next
                # asset's Quotex fetch — never mid-fetch.
                if self._stop_requested:
                    break
                await self._pause_event.wait()
                if self._stop_requested:
                    break

                self.current_asset = asset
                self.current_indicator = "fetching_candles"
                t_asset_start = _now()

                # P0 fix — every asset increments assets_processed exactly
                # once, on every exit path (fetch failure, no data, compute
                # failure, or full success). This is what makes progress
                # reach 100% once every requested asset has been attempted,
                # regardless of how many succeeded — see status().
                try:
                    df = await self._fetch_candles(asset, timeframe, candle_count)
                except Exception as exc:  # noqa: BLE001 — logged, loop continues to next asset
                    self.last_error = f"{asset}: fetch failed: {exc}"
                    self.results[asset] = {
                        "status": "FAILED", "error": self.last_error, "timestamp": _iso(_now()),
                    }
                    self.assets_processed += 1
                    continue

                if df is None or len(df) == 0:
                    # P0 fix — a missing/empty history is a real, honest
                    # NO_DATA outcome, not a fabricated result and not a
                    # generic "error" that looks identical to a fetch
                    # exception or a compute failure.
                    self.last_error = f"{asset}: no candle data returned"
                    self.results[asset] = {
                        "status": "NO_DATA", "error": self.last_error, "timestamp": _iso(_now()),
                    }
                    self.assets_processed += 1
                    continue

                candles_fetched = len(df)
                # Honest data-count transparency, additive/diagnostic only —
                # does not change candle_count_met or any gate. See
                # _backtest_fetch_candles() in app.py for where these come
                # from. df.attrs is best-effort; absence just falls back to
                # the already-existing candles_fetched/candle_count values,
                # i.e. exactly today's behavior.
                _attrs = getattr(df, "attrs", {}) or {}
                candles_requested_actual = _attrs.get("candles_requested", candle_count)
                candles_returned_raw = _attrs.get("candles_returned_raw", candles_fetched)

                # P0 fix — the compute stage (factor accuracy, dynamic
                # weights, filter-score report) was previously unprotected:
                # any exception here propagated out of _run_loop entirely,
                # silently killing the WHOLE batch instead of just this one
                # asset. Isolated per-asset the same way the fetch stage
                # already was.
                try:
                    # ── Reuse existing backtest functions exactly — no duplicated math ──
                    self.current_indicator = "computing_factor_accuracy"
                    accuracies = backtest_factor_accuracy(df, lookahead=lookahead)

                    # Per-indicator progress visibility only — the accuracy math
                    # itself already ran as one vectorized call above; this just
                    # walks the ALREADY-COMPUTED dict so current_indicator is
                    # meaningful for a status poller, without re-running anything.
                    for name in accuracies:
                        if self._stop_requested:
                            break
                        self.current_indicator = name
                        await asyncio.sleep(0)  # yield to the event loop

                    self.current_indicator = "computing_dynamic_weights"
                    suggested_weights = compute_dynamic_weights(accuracies)

                    self.current_indicator = "filter_score_report"
                    fs_report = backtest_filter_score_report(df, lookahead=lookahead)
                except Exception as exc:  # noqa: BLE001 — logged, loop continues to next asset
                    self.last_error = f"{asset}: compute failed: {exc}"
                    self.results[asset] = {
                        "status": "FAILED", "error": self.last_error, "timestamp": _iso(_now()),
                    }
                    self.assets_processed += 1
                    del df
                    continue

                # Only counted toward the informational candle stat on the
                # success path — matches what was actually usable.
                self.candles_processed += candles_fetched

                # Deliberately NOT storing `df` itself — only derived,
                # already-small result dicts, per the memory-usage requirement.
                self.results[asset] = {
                    "status": "SUCCESS",
                    "candles_used": candles_fetched,
                    "candles_requested": candles_requested_actual,
                    "candles_returned_raw": candles_returned_raw,
                    "accuracies": accuracies,
                    "suggested_weights": suggested_weights,
                    "filter_score_report": fs_report,
                    "runtime_seconds": round(_now() - t_asset_start, 2),
                    "timestamp": _iso(_now()),
                    "candle_count_met": candles_fetched >= candle_count,
                }
                self.assets_processed += 1
                del df  # explicit: drop the reference as soon as we're done with it

                # Checkpoint 2: after finishing this asset's work unit,
                # honor a stop/pause request before moving to the next one.
                if self._stop_requested:
                    break

            self.current_asset = None
            self.current_indicator = None
            # P0 fix (defense-in-depth) — _build_summary() aggregates
            # ACROSS all assets after the per-asset loop finishes; it is
            # itself a compute step and was found to be capable of raising
            # (e.g. a per-asset accuracies dict missing a key another
            # asset's dict has — a pre-existing edge case in the underlying
            # aggregation, reproduced during P0 testing on synthetic data).
            # Previously that exception was unprotected here too: it would
            # propagate out of _run_loop exactly like the per-asset compute
            # exceptions did, leaving self.summary silently None even
            # though every asset had already been genuinely processed and
            # percent_complete/completed would otherwise correctly show
            # 100%/True. Isolated the same way, with an honest fallback
            # summary instead of a silent None.
            try:
                self._build_summary(candle_count)
            except Exception as exc:  # noqa: BLE001
                self.last_error = f"summary aggregation failed: {exc}"
                self.summary = {
                    "assets_backtested": 0,
                    "assets_succeeded": sum(1 for r in self.results.values() if r.get("status") == "SUCCESS"),
                    "assets_no_data": sum(1 for r in self.results.values() if r.get("status") == "NO_DATA"),
                    "assets_failed": sum(1 for r in self.results.values() if r.get("status") == "FAILED"),
                    "assets_skipped": max(0, self.total_assets - len(self.results)),
                    "suggested_weights": {}, "min_sample_sizes": {},
                    "candle_count_target": candle_count, "all_assets_met_candle_count": False,
                    "aggregation_error": self.last_error,
                }
        finally:
            self.finished_at = _now()
            self.state = STOPPED

    def _build_summary(self, candle_count: int) -> None:
        """Aggregate suggested weights across all successfully-backtested
        assets by simple averaging — pure arithmetic on the existing
        per-asset compute_dynamic_weights() outputs, no new scoring system,
        no automatic write anywhere.

        P0 fix: also reports the real per-status breakdown (SUCCESS /
        NO_DATA / FAILED / SKIPPED) instead of collapsing every non-success
        outcome into one "failed" bucket — NO_DATA (empty/missing history)
        and FAILED (fetch or compute exception) are different, honest
        states, never fabricated into each other.
        """
        valid = {a: r for a, r in self.results.items() if r.get("status") == "SUCCESS"}
        no_data = {a: r for a, r in self.results.items() if r.get("status") == "NO_DATA"}
        failed = {a: r for a, r in self.results.items() if r.get("status") == "FAILED"}
        # Assets in the run's asset list that never got attempted at all —
        # only non-empty when the run was stopped/cancelled early.
        skipped_count = max(0, self.total_assets - len(self.results))

        if not valid:
            self.summary = {
                "assets_backtested": 0,
                "assets_succeeded": 0,
                "assets_no_data": len(no_data),
                "assets_failed": len(failed),
                "assets_skipped": skipped_count,
                "suggested_weights": {}, "min_sample_sizes": {},
                "candle_count_target": candle_count, "all_assets_met_candle_count": False,
            }
            return

        keys = next(iter(valid.values()))["suggested_weights"].keys()
        avg_weights = {k: round(sum(r["suggested_weights"][k] for r in valid.values()) / len(valid), 4)
                       for k in keys}
        min_sample_sizes = {k: min(r["accuracies"][k]["sample_size"] for r in valid.values())
                             for k in keys}
        all_met_candle_count = all(r["candle_count_met"] for r in valid.values())

        self.summary = {
            "assets_backtested": len(valid),
            "assets_succeeded": len(valid),
            "assets_no_data": len(no_data),
            "assets_failed": len(failed),
            "assets_skipped": skipped_count,
            "suggested_weights": avg_weights,     # in-memory only — see module docstring
            "min_sample_sizes": min_sample_sizes,  # worst case across assets
            "candle_count_target": candle_count,
            "all_assets_met_candle_count": all_met_candle_count,
        }

    # ── status() ─────────────────────────────────────────────────────────────
    def status(self) -> Dict[str, Any]:
        """
        P0 fix — percent_complete is now based on assets_processed /
        total_assets (work UNITS attempted, success or failure both count —
        same convention scanner.py's _assets_completed already uses), not
        on candles_processed / total_candles. The candle-based fields
        previously undercounted whenever any asset failed or returned
        fewer candles than requested, which permanently capped
        percent_complete below 100 even on a run that had genuinely
        finished. candles_processed/total_candles are kept in the response
        as informational secondary stats only.
        """
        elapsed = ((self.finished_at or _now()) - self.started_at) if self.started_at else 0.0
        percent_complete = (
            round(min(100.0, self.assets_processed / self.total_assets * 100), 1)
            if self.total_assets else 0.0
        )
        estimated_remaining = None
        if self.state == RUNNING and percent_complete > 0:
            estimated_remaining = round(elapsed / (percent_complete / 100.0) - elapsed, 1)

        # completed only means "the loop actually reached the end of its
        # work queue" — finished_at being set is necessary but was never
        # sufficient on its own (an uncaught compute exception used to hit
        # the same finally-block outcome as a real finish; that path is now
        # closed per-asset above, but this flag is defined correctly
        # regardless).
        completed = (
            self.state == STOPPED and self.finished_at is not None
            and not self._cancelled and self.assets_processed >= self.total_assets
        )
        cancelled = self.state == STOPPED and self.finished_at is not None and self._cancelled

        return {
            "state": self.state,
            "running": self.state == RUNNING,
            "paused": self.state == PAUSED,
            "completed": completed,
            "cancelled": cancelled,
            "current_asset": self.current_asset,
            "current_indicator": self.current_indicator,
            "assets_processed": self.assets_processed,
            "total_assets": self.total_assets,
            "candles_processed": self.candles_processed,
            "total_candles": self.total_candles,
            "percent_complete": percent_complete,
            "elapsed_time": round(elapsed, 1),
            "estimated_remaining": estimated_remaining,
            "last_error": self.last_error,
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.finished_at),
            "summary": self.summary,
        }

    def get_results(self) -> Dict[str, Any]:
        """Full per-asset results + summary — separate from status() so a
        lightweight progress poller doesn't have to pull the full payload."""
        return {"results": self.results, "summary": self.summary}


def evaluate_apply_conditions(engine: "BacktestEngine", min_candles: int,
                              min_indicator_sample_size: int) -> Dict[str, Any]:
    """
    Evaluation only — never applies anything itself. Weights must NEVER be
    applied automatically; this is read by a future Settings-page route to
    decide whether to enable an explicit "Apply Suggested Weights" action.
    """
    reasons: List[str] = []
    if engine.state != STOPPED or engine.summary is None:
        reasons.append("Backtest has not completed successfully.")
        return {"can_apply": False, "reasons": reasons}

    summary = engine.summary
    if summary.get("assets_backtested", 0) == 0:
        reasons.append("No assets were successfully backtested.")

    # Bug fix (found during Phase 7.2 testing): all_assets_met_candle_count
    # only confirms each asset actually received as many candles as the RUN
    # requested — it says nothing about whether that run's candle_count
    # itself met the caller's policy minimum. A backtest run started with
    # candle_count=500 would otherwise silently pass this gate even when
    # min_candles=2000 is required. Check both explicitly.
    candle_count_target = summary.get("candle_count_target", 0)
    if candle_count_target < min_candles:
        reasons.append(
            f"Backtest was run with {candle_count_target} candles, below the required minimum ({min_candles})."
        )
    elif not summary.get("all_assets_met_candle_count", False):
        reasons.append(f"Not all assets met the minimum candle requirement ({min_candles}).")

    min_samples = summary.get("min_sample_sizes", {})
    insufficient = [k for k, v in min_samples.items() if v < min_indicator_sample_size]
    if insufficient:
        reasons.append(
            f"Indicators below minimum sample size ({min_indicator_sample_size}): {insufficient}"
        )

    return {"can_apply": len(reasons) == 0, "reasons": reasons, "summary": summary}
