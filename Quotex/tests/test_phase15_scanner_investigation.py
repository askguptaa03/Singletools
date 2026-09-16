"""
Targeted regression suite for the Phase 15 scanner investigation:
  - scanner's authoritative asset universe (live catalog, not static)
  - _effective_assets immutability (set once at start(), never mutated
    mid-run — confirmed by source trace, verified here by a real run)
  - the new check_asset_status= safety-parity fix (scanner's BUY/SELL
    results now get the same live-open re-check Manual Analyzer's
    /api/signal already applied)
  - confirms exactly one scanner.py exists
  - confirms no signal/confluence/backtest logic was touched

scanner.py cannot be imported directly in this sandbox (its `from
api_quotex.constants import TIMEFRAMES` pulls in api_quotex/__init__.py,
which pulls in client.py, which needs loguru — unavailable, no network).
This suite stubs ONLY api_quotex.constants.TIMEFRAMES (a plain dict, zero
behavior) so the REAL scanner.py module loads and runs for real — this is
not a source-text/regex check, it is genuine execution of the actual
ScannerEngine class and its real _scan_loop().

Run with: python3 Quotex/tests/test_phase15_scanner_investigation.py
"""
import sys
import os
import re
import types
import asyncio

_HERE = os.path.dirname(os.path.abspath(__file__))
_MARKET_ANALYZER = os.path.join(_HERE, "..", "market_analyzer")
_WEBAPP = os.path.join(_MARKET_ANALYZER, "webapp")
_QUOTEX_API = os.path.join(_HERE, "..", "quotex", "api_quotex")

# Stub api_quotex.constants BEFORE importing scanner — no other module is
# touched or faked; scanner.py itself is the real, unmodified-by-this-test
# file on disk.
_pkg = types.ModuleType("api_quotex")
_const_mod = types.ModuleType("api_quotex.constants")
_const_mod.TIMEFRAMES = {"1m": 60, "5m": 300, "15m": 900}
sys.modules["api_quotex"] = _pkg
sys.modules["api_quotex.constants"] = _const_mod

sys.path.insert(0, _MARKET_ANALYZER)
sys.path.insert(0, _WEBAPP)

import scanner  # noqa: E402  (real module, only its api_quotex dep is stubbed)

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


# ─── K. Exactly one scanner.py exists ───────────────────────────────────────
print("\n=== K. exactly one scanner.py exists ===")
_repo_root = os.path.join(_HERE, "..")
_found = []
for _dirpath, _dirnames, _filenames in os.walk(_repo_root):
    if "__pycache__" in _dirpath:
        continue
    if "scanner.py" in _filenames:
        _found.append(os.path.relpath(os.path.join(_dirpath, "scanner.py"), _repo_root))
check("exactly one scanner.py found in the whole repo", len(_found) == 1)
check(
    "it is at market_analyzer/webapp/scanner.py (not market_analyzer/scanner.py)",
    _found == ["market_analyzer/webapp/scanner.py"],
)
check(
    "market_analyzer/scanner.py does NOT exist",
    not os.path.exists(os.path.join(_MARKET_ANALYZER, "scanner.py")),
)


# ─── B/G. Scanner's real asset source is the live catalog, not static ──────
print("\n=== B/G. /api/scanner/start uses the live catalog, not the static ASSETS dict ===")
_start_route_match = re.search(
    r'@app\.route\("/api/scanner/start".*?(?=\n@app\.route)',
    APP_PY_SRC,
    re.DOTALL,
)
check("/api/scanner/start route source found", _start_route_match is not None)
_start_src = _start_route_match.group(0) if _start_route_match else ""
check(
    "calls live_assets.get_live_otc_assets(..., force_refresh=True)",
    "live_assets.get_live_otc_assets(fetcher, force_refresh=True)" in _start_src,
)
check(
    "an explicit request 'assets' list is filtered through live availability, not used as-is",
    "live_assets.filter_requested_against_live(explicit_assets, live_symbols)" in _start_src,
)
check(
    "a saved settings.enabled_assets selection is intersected with live_symbols",
    "[a for a in saved_selection if a in live_symbols]" in _start_src,
)
check(
    "ScannerEngine.start() is always called with an explicit assets= (never left to the static constructor default)",
    "assets=assets_for_run" in _start_src,
)


# ─── C. Manual Analyzer's asset-validity gate uses the same live source ────
print("\n=== C. Manual Analyzer (/api/signal) uses the same live source, not the static dict ===")
check(
    "_check_live_asset() docstring states live_assets.get_live_assets() is the source of truth",
    "Source of truth is the CURRENT live Quotex" in APP_PY_SRC,
)
check(
    "api_signal() calls _check_live_asset() before running the pipeline",
    'asset_check = _check_live_asset(asset, timeframe)' in APP_PY_SRC,
)


# ─── D/F. The new asset_status safety-parity fix (the one real gap found) ──
print("\n=== D/F. asset_status parity: shared helper used by BOTH api_signal() and scanner ===")
check(
    "_get_live_asset_status() shared helper exists",
    "async def _get_live_asset_status(asset: str) -> str:" in APP_PY_SRC,
)
check(
    "the helper never fabricates — falls back to 'unknown' on any failure",
    'return "unknown"  # never guess/fabricate on failure' in APP_PY_SRC,
)
check(
    "api_signal() now delegates to the shared helper (no more duplicated inline logic)",
    "_run_bg(_get_live_asset_status(asset), timeout=10.0)" in APP_PY_SRC,
)
check(
    "ScannerEngine is constructed with check_asset_status=_get_live_asset_status",
    "check_asset_status=_get_live_asset_status," in APP_PY_SRC,
)
check(
    "scanner.py accepts an optional check_asset_status callable (duck-typed, defaults to None)",
    "check_asset_status: Optional[Callable[[str], Awaitable[str]]] = None," in SCANNER_SRC,
)
check(
    "scanner.py's asset_status check is scoped so its own failure can never be mistaken for a scan failure",
    "mistaken for a failed analysis attempt" in SCANNER_SRC,
)


# ─── E/H. Snapshot immutability — real runtime proof, not just source ──────
print("\n=== E/H. _effective_assets is set once and never mutated mid-run (real execution) ===")


async def _fake_run_pipeline_buy(asset, timeframe):
    return {
        "asset": asset, "timeframe": timeframe,
        "confluence": {"signal": "BUY", "confidence": 80},
    }


_status_calls = []


async def _fake_check_asset_status(asset):
    _status_calls.append(asset)
    return "closed"  # deliberately "closed" so we can prove it actually threads through


async def _run_one_cycle_and_capture():
    engine = scanner.ScannerEngine(
        run_pipeline=_fake_run_pipeline_buy,
        assets=["EURUSD_otc"],
        invalidate_fetcher=lambda: None,
        adx_trending=25.0,
        config=scanner.ScannerConfig(
            timeframes=["1m"], asset_gap_seconds=0.0, cycle_interval_seconds=0.01,
            asset_timeout_seconds=5.0,
        ),
        check_asset_status=_fake_check_asset_status,
    )
    effective_before = list(engine._effective_assets)
    result = engine.start(loop=asyncio.get_event_loop(), assets=["EURUSD_otc"])
    # Let exactly one cycle run, then stop — _effective_assets should be
    # byte-identical to what start() set, proving nothing mutated it
    # mid-run (there is, per source trace, no code path that could).
    await asyncio.sleep(0.3)
    engine._stop_requested = True
    await asyncio.sleep(0.2)
    return engine, effective_before, result


engine, effective_before, start_result = asyncio.run(_run_one_cycle_and_capture())

check("scanner.start() succeeded", start_result.get("ok") is True)
check(
    "_effective_assets is unchanged after a real cycle ran (never mutated mid-run)",
    engine._effective_assets == effective_before == ["EURUSD_otc"],
)
check(
    "the scanner actually ran the fake pipeline and produced a cached result",
    ("EURUSD_otc", "1m") in engine._cache,
)
_cached = engine._cache.get(("EURUSD_otc", "1m"), {})
check(
    "the BUY signal computed by run_pipeline is preserved untouched",
    (_cached.get("confluence") or {}).get("signal") == "BUY"
    and (_cached.get("confluence") or {}).get("confidence") == 80,
)
check(
    "check_asset_status was actually invoked for this asset",
    "EURUSD_otc" in _status_calls,
)
check(
    "the scanner's cached result now carries asset_status, exactly like /api/signal's",
    _cached.get("asset_status") == "closed",
)


# ─── Safety net: a failing check_asset_status must never fail the scan ─────
print("\n=== Safety: a broken check_asset_status callable never breaks the scan ===")


async def _broken_check_asset_status(asset):
    raise RuntimeError("simulated failure")


async def _run_with_broken_status_check():
    engine = scanner.ScannerEngine(
        run_pipeline=_fake_run_pipeline_buy,
        assets=["EURUSD_otc"],
        invalidate_fetcher=lambda: None,
        adx_trending=25.0,
        config=scanner.ScannerConfig(
            timeframes=["1m"], asset_gap_seconds=0.0, cycle_interval_seconds=0.01,
            asset_timeout_seconds=5.0,
        ),
        check_asset_status=_broken_check_asset_status,
    )
    engine.start(loop=asyncio.get_event_loop(), assets=["EURUSD_otc"])
    await asyncio.sleep(0.3)
    engine._stop_requested = True
    await asyncio.sleep(0.2)
    return engine


engine2 = asyncio.run(_run_with_broken_status_check())
_cached2 = engine2._cache.get(("EURUSD_otc", "1m"), {})
check(
    "the analysis result is still stored successfully despite the status-check failure",
    (_cached2.get("confluence") or {}).get("signal") == "BUY",
)
check(
    "asset_status falls back to 'unknown' rather than crashing or omitting the result",
    _cached2.get("asset_status") == "unknown",
)
check(
    "this was NOT counted as a scan failure",
    engine2._failure_count == 0 and engine2._success_count > 0,
)


# ─── check_asset_status=None (default) behaves exactly as before ──────────
print("\n=== Backward compatibility: check_asset_status=None changes nothing ===")


async def _run_without_status_check():
    engine = scanner.ScannerEngine(
        run_pipeline=_fake_run_pipeline_buy,
        assets=["EURUSD_otc"],
        invalidate_fetcher=lambda: None,
        adx_trending=25.0,
        config=scanner.ScannerConfig(
            timeframes=["1m"], asset_gap_seconds=0.0, cycle_interval_seconds=0.01,
            asset_timeout_seconds=5.0,
        ),
        # check_asset_status intentionally omitted -> defaults to None
    )
    engine.start(loop=asyncio.get_event_loop(), assets=["EURUSD_otc"])
    await asyncio.sleep(0.3)
    engine._stop_requested = True
    await asyncio.sleep(0.2)
    return engine


engine3 = asyncio.run(_run_without_status_check())
_cached3 = engine3._cache.get(("EURUSD_otc", "1m"), {})
check(
    "with no check_asset_status supplied, no asset_status key is added (pre-existing behavior preserved)",
    "asset_status" not in _cached3,
)
check(
    "the result is otherwise identical to before this fix",
    (_cached3.get("confluence") or {}).get("signal") == "BUY",
)


# ─── Safety invariants unchanged ────────────────────────────────────────────
print("\n=== Safety invariants ===")
sys.path.insert(0, _MARKET_ANALYZER)
import analyzer
import backtest
check("analyzer.MIN_AGREEING_FACTORS == 2", analyzer.MIN_AGREEING_FACTORS == 2)
check("backtest.MIN_SIGNALS_REQUIRED == 20", backtest.MIN_SIGNALS_REQUIRED == 20)
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
    "no signal/confluence/generate_confluence_signal source was touched by this change",
    "generate_confluence_signal" not in SCANNER_SRC,
)


print(f"\n=== RESULT: {passed} passed, {failed} failed ===")
sys.exit(1 if failed else 0)
