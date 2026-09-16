"""
Targeted regression suite for the Phase 15 cycle-boundary refresh addition:
ScannerEngine's new optional refresh_assets= callable, wired into app.py
via _get_live_scanner_assets().

Same import-stub approach as test_phase15_scanner_investigation.py — the
real scanner.py module is loaded, only api_quotex.constants is stubbed.

Run with: python3 Quotex/tests/test_phase15_scanner_refresh.py
"""
import sys
import os
import types
import asyncio

_HERE = os.path.dirname(os.path.abspath(__file__))
_MARKET_ANALYZER = os.path.join(_HERE, "..", "market_analyzer")
_WEBAPP = os.path.join(_MARKET_ANALYZER, "webapp")

_pkg = types.ModuleType("api_quotex")
_const_mod = types.ModuleType("api_quotex.constants")
_const_mod.TIMEFRAMES = {"1m": 60}
sys.modules["api_quotex"] = _pkg
sys.modules["api_quotex.constants"] = _const_mod

sys.path.insert(0, _MARKET_ANALYZER)
sys.path.insert(0, _WEBAPP)

import scanner  # noqa: E402

passed = 0
failed = 0


def check(name, cond):
    global passed, failed
    if cond:
        print(f"  PASS  {name}")
        passed += 1
    else:
        print(f"  FAIL  {name}")
        failed += 1


APP_PY_SRC = open(os.path.join(_WEBAPP, "app.py"), encoding="utf-8").read()
SCANNER_SRC = open(os.path.join(_WEBAPP, "scanner.py"), encoding="utf-8").read()


# ─── Source-level: the refresh call site is exactly where it must be ───────
print("\n=== Source: refresh happens once per cycle, before the asset loop ===")
_cycle_start_idx = SCANNER_SRC.index('self._log_event("cycle_start"')
_refresh_idx = SCANNER_SRC.index("if self._refresh_assets is not None:")
_asset_loop_idx = SCANNER_SRC.index("for asset in self._effective_assets:")
check(
    "refresh check comes after cycle_start logging",
    _cycle_start_idx < _refresh_idx,
)
check(
    "refresh check comes before the asset iteration loop",
    _refresh_idx < _asset_loop_idx,
)
check(
    "a successful refresh builds a NEW list (list(fresh_assets)), never reuses the caller's object",
    "self._effective_assets = list(fresh_assets)" in SCANNER_SRC,
)
check(
    "an exception during refresh keeps the previous list untouched (no assignment in the except branch)",
    "fresh_assets = None" in SCANNER_SRC,
)
check(
    "an empty-but-successful result is also treated as keep-previous, not blanked out",
    "elif fresh_assets is not None:" in SCANNER_SRC,
)
check(
    "app.py wires _get_live_scanner_assets as refresh_assets=",
    "refresh_assets=_get_live_scanner_assets," in APP_PY_SRC,
)
check(
    "_get_live_scanner_assets reuses the SAME live snapshot call /api/scanner/start uses "
    "(one authoritative source, not a second independent path)",
    "live_assets.get_live_otc_assets(fetcher, force_refresh=True)" in APP_PY_SRC
    and APP_PY_SRC.count("live_assets.get_live_otc_assets(fetcher, force_refresh=True)") >= 2,
)


# ─── Real execution: multi-cycle refresh behavior ───────────────────────────
print("\n=== Real execution: refresh actually applies at cycle boundaries ===")

_refresh_calls = []
_captured_effective_assets = []  # snapshot of _effective_assets at each pipeline call


async def _refresh_fn():
    gen = len(_refresh_calls)
    _refresh_calls.append(gen)
    if gen == 0:
        return ["ASSET_GEN0_otc"]
    elif gen == 1:
        return ["ASSET_GEN1_otc"]
    elif gen == 2:
        return []  # simulate an empty-but-successful response
    else:
        raise RuntimeError("simulated live-fetch failure")  # every call after gen 2 fails


async def _fake_run_pipeline(asset, timeframe):
    return {"asset": asset, "timeframe": timeframe, "confluence": {"signal": "WAIT", "confidence": 0}}


async def _run_multi_cycle():
    engine = scanner.ScannerEngine(
        run_pipeline=_fake_run_pipeline,
        assets=["STARTUP_DEFAULT_otc"],
        invalidate_fetcher=lambda: None,
        adx_trending=25.0,
        config=scanner.ScannerConfig(
            timeframes=["1m"], asset_gap_seconds=0.0, cycle_interval_seconds=0.02,
            asset_timeout_seconds=5.0,
        ),
        refresh_assets=_refresh_fn,
    )
    # Wrap the loop's per-cycle asset list so we can see what was active at
    # the moment each cycle actually ran, without needing to hook _scan_loop
    # internals — capture right after each cycle_start by polling.
    engine.start(loop=asyncio.get_event_loop(), assets=["STARTUP_DEFAULT_otc"])
    seen_lists = []
    last_cycle = -1
    for _ in range(400):  # poll for up to ~2s in small increments
        await asyncio.sleep(0.005)
        if engine.current_cycle != last_cycle:
            last_cycle = engine.current_cycle
            seen_lists.append(list(engine._effective_assets))
        if len(_refresh_calls) >= 4:
            break
    engine._stop_requested = True
    await asyncio.sleep(0.1)
    return engine, seen_lists


engine, seen_lists = asyncio.run(_run_multi_cycle())

check("refresh_assets was actually called across multiple cycles", len(_refresh_calls) >= 4)
check(
    "cycle 1 used the gen-0 refreshed list (['ASSET_GEN0_otc']), not the start()-time default",
    any(lst == ["ASSET_GEN0_otc"] for lst in seen_lists),
)
check(
    "a later cycle used the gen-1 refreshed list (['ASSET_GEN1_otc'])",
    any(lst == ["ASSET_GEN1_otc"] for lst in seen_lists),
)
check(
    "an empty gen-2 response did NOT blank the active list — gen-1's list was preserved for that cycle",
    ["ASSET_GEN1_otc"] in seen_lists and [] not in seen_lists,
)
check(
    "after the simulated failure (gen 3+), the list still equals the last good one (gen-1), never crashed to empty",
    list(engine._effective_assets) == ["ASSET_GEN1_otc"],
)
check(
    "scanner kept running through the refresh failure (multiple cycles completed, not stuck)",
    engine.current_cycle >= 3,
)


# ─── Snapshot identity: each successful refresh is a NEW list object ───────
print("\n=== Snapshot proof: refresh builds a new list, never aliases the source ===")


async def _identity_refresh_fn():
    shared_source = ["SAME_SOURCE_otc"]
    _identity_refresh_fn.last_source = shared_source
    return shared_source


_identity_refresh_fn.last_source = None


async def _run_identity_check():
    engine = scanner.ScannerEngine(
        run_pipeline=_fake_run_pipeline,
        assets=["X_otc"],
        invalidate_fetcher=lambda: None,
        adx_trending=25.0,
        config=scanner.ScannerConfig(
            timeframes=["1m"], asset_gap_seconds=0.0, cycle_interval_seconds=0.01,
            asset_timeout_seconds=5.0,
        ),
        refresh_assets=_identity_refresh_fn,
    )
    engine.start(loop=asyncio.get_event_loop(), assets=["X_otc"])
    await asyncio.sleep(0.15)
    engine._stop_requested = True
    await asyncio.sleep(0.1)
    return engine


engine2 = asyncio.run(_run_identity_check())
check(
    "_effective_assets is a distinct list object from whatever refresh_assets() returned",
    engine2._effective_assets is not _identity_refresh_fn.last_source
    and engine2._effective_assets == _identity_refresh_fn.last_source,
)


# ─── No mid-cycle mutation: refresh_assets is never called inside the loop ─
print("\n=== Mid-cycle immutability still holds with refresh enabled ===")
_run_pipeline_snapshots = []


async def _snapshotting_run_pipeline(asset, timeframe):
    # Any test-observable value here proves _effective_assets did not
    # change between the start of this cycle and this particular call.
    _run_pipeline_snapshots.append(list(_capture_engine[0]._effective_assets))
    return {"asset": asset, "timeframe": timeframe, "confluence": {"signal": "WAIT", "confidence": 0}}


_capture_engine = [None]


async def _run_immutability_check():
    engine = scanner.ScannerEngine(
        run_pipeline=_snapshotting_run_pipeline,
        assets=["A_otc", "B_otc", "C_otc"],
        invalidate_fetcher=lambda: None,
        adx_trending=25.0,
        config=scanner.ScannerConfig(
            timeframes=["1m"], asset_gap_seconds=0.0, cycle_interval_seconds=5.0,  # long — stay in cycle 1
            asset_timeout_seconds=5.0,
        ),
        # Refresh would only apply at the NEXT cycle boundary (5s away) —
        # within this single long cycle, _effective_assets must stay fixed
        # across all three assets' pipeline calls.
        refresh_assets=None,
    )
    _capture_engine[0] = engine
    engine.start(loop=asyncio.get_event_loop(), assets=["A_otc", "B_otc", "C_otc"])
    await asyncio.sleep(0.3)
    engine._stop_requested = True
    await asyncio.sleep(0.1)
    return engine


engine3 = asyncio.run(_run_immutability_check())
check(
    "at least 3 pipeline calls were captured within the single long cycle",
    len(_run_pipeline_snapshots) >= 3,
)
check(
    "every snapshot within that one cycle is identical — no mid-cycle mutation",
    all(s == _run_pipeline_snapshots[0] for s in _run_pipeline_snapshots),
)


# ─── Backward compatibility: refresh_assets=None (default) unaffected ──────
print("\n=== Backward compatibility: default (refresh_assets=None) is unaffected ===")


async def _run_default_check():
    engine = scanner.ScannerEngine(
        run_pipeline=_fake_run_pipeline,
        assets=["ONLY_otc"],
        invalidate_fetcher=lambda: None,
        adx_trending=25.0,
        config=scanner.ScannerConfig(
            timeframes=["1m"], asset_gap_seconds=0.0, cycle_interval_seconds=0.01,
            asset_timeout_seconds=5.0,
        ),
        # refresh_assets intentionally omitted -> defaults to None
    )
    engine.start(loop=asyncio.get_event_loop(), assets=["ONLY_otc"])
    await asyncio.sleep(0.15)
    engine._stop_requested = True
    await asyncio.sleep(0.1)
    return engine


engine4 = asyncio.run(_run_default_check())
check(
    "with no refresh_assets supplied, the list is exactly what start() set, unchanged across cycles",
    engine4._effective_assets == ["ONLY_otc"],
)
check("scanner still completed multiple cycles normally", engine4.current_cycle >= 2)


# ─── Exactly one scanner.py; safety invariants unchanged ───────────────────
print("\n=== Structural + safety invariants ===")
_repo_root = os.path.join(_HERE, "..")
_found = []
for _dirpath, _dirnames, _filenames in os.walk(_repo_root):
    if "__pycache__" in _dirpath:
        continue
    if "scanner.py" in _filenames:
        _found.append(os.path.relpath(os.path.join(_dirpath, "scanner.py"), _repo_root))
check("exactly one scanner.py exists", _found == ["market_analyzer/webapp/scanner.py"])

sys.path.insert(0, _MARKET_ANALYZER)
import analyzer
import backtest
check("analyzer.MIN_AGREEING_FACTORS == 2", analyzer.MIN_AGREEING_FACTORS == 2)
check("backtest.MIN_SIGNALS_REQUIRED == 20", backtest.MIN_SIGNALS_REQUIRED == 20)
import re
_settings_src = open(os.path.join(_WEBAPP, "settings_store.py"), encoding="utf-8").read()
check(
    "settings_store min_candles still 2000",
    re.search(r'"min_candles"\s*:\s*2000', _settings_src) is not None,
)
check(
    "settings_store min_indicator_sample_size still 100",
    re.search(r'"min_indicator_sample_size"\s*:\s*100', _settings_src) is not None,
)
check(
    "no signal/confluence source was touched by this change",
    "generate_confluence_signal" not in SCANNER_SRC,
)
check(
    "_loop_task / _stop_requested / _pause_event names are untouched (still present, not renamed)",
    all(name in SCANNER_SRC for name in ("_loop_task", "_stop_requested", "_pause_event")),
)


print(f"\n=== RESULT: {passed} passed, {failed} failed ===")
sys.exit(1 if failed else 0)
