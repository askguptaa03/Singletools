"""
Targeted regression suite for the 250 -> ~199 candle investigation.

Covers, with evidence-based (not guessed) coverage of what was actually
changed:

  1. Quotex protocol: documents (does not re-derive) that no outgoing
     pagination/backfill message exists anywhere in api_quotex/*.py — see
     PATCH_SUMMARY_CANDLE_INVESTIGATION.txt for the full trace. This suite
     does not attempt to fabricate or test a guessed protocol, per the
     explicit instruction not to invent one.
  2. _drop_forming_candle() still removes at most one forming candle and
     reproduces the exact 200 -> 199 arithmetic you observed.
  3. app.py's _backtest_fetch_candles() (source-extracted, since app.py
     itself can't be imported here — same loguru/websockets/cloudscraper
     sandbox limitation as the previous patch round) now applies
     _drop_forming_candle() and records candles_requested /
     candles_returned_raw as DataFrame.attrs diagnostics.
  4. backtest_engine.py's real _run_loop() (imported and run for real —
     this module has no heavy external dependencies) correctly surfaces
     candles_requested / candles_returned_raw / candles_used in each
     asset's result, WITHOUT changing candle_count_met, MIN_SIGNALS_REQUIRED,
     or any other gate.
  5. Safety invariants (MIN_AGREEING_FACTORS=2, MIN_SIGNALS_REQUIRED=20,
     min_candles=2000, min_indicator_sample_size=100) remain unchanged.
  6. A zero-sample factor (e.g. round_number=0) is never silently upgraded
     to a fake/default sample size or accuracy.

Run with:  python3 Quotex/tests/test_candle_count_investigation.py
"""
import sys
import os
import re
import asyncio

_HERE = os.path.dirname(os.path.abspath(__file__))
_MARKET_ANALYZER = os.path.join(_HERE, "..", "market_analyzer")
_WEBAPP = os.path.join(_MARKET_ANALYZER, "webapp")
sys.path.insert(0, _MARKET_ANALYZER)
sys.path.insert(0, _WEBAPP)

import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone

import backtest
import backtest_engine

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


APP_PY_PATH = os.path.join(_WEBAPP, "app.py")
APP_PY_SRC = open(APP_PY_PATH, encoding="utf-8").read()

QUOTEX_API_DIR = os.path.join(_HERE, "..", "quotex", "api_quotex")


# ─── 1. Quotex protocol — confirm no outgoing pagination message exists ─────
print("\n=== 1. Quotex outgoing-protocol trace (source-level, no guessing) ===")
_client_src = open(os.path.join(QUOTEX_API_DIR, "client.py"), encoding="utf-8").read()
_ws_src = open(os.path.join(QUOTEX_API_DIR, "websocket_client.py"), encoding="utf-8").read()
_ka_src = open(os.path.join(QUOTEX_API_DIR, "connection_keep_alive.py"), encoding="utf-8").read()
_all_protocol_src = _client_src + _ws_src + _ka_src

check(
    "instruments/update carries only asset+period (no count/offset)",
    'f\'42["instruments/update",{{"asset":"{asset}","period":{int(timeframe)}}}]\''
    in _client_src,
)
check(
    "chart_notification/get is sent with no payload at all",
    "'42[\"chart_notification/get\"]'" in _client_src,
)
check(
    "end_time is accepted by _request_candles but never used in its body",
    len(re.findall(r"end_time", _client_src.split("async def _request_candles")[1].split(
        "def _record_unmatched_candle_response")[0])) == 1,
    # exactly 1 = only the signature itself references it
)
for _term in ("loadHistoryPeriod", "history/list", "history/list/v2"):
    _send_pattern = re.compile(
        r"send_message(?:_immediate)?\([^)]*" + re.escape(_term)
    )
    check(
        f"'{_term}' is never constructed as an outgoing send anywhere",
        _send_pattern.search(_all_protocol_src) is None,
    )


# ─── 2. _drop_forming_candle() reproduces the exact 200 -> 199 arithmetic ───
print("\n=== 2. _drop_forming_candle() reproduces the observed 200 -> 199 ===")
_func_match = re.search(
    r"def _drop_forming_candle\(.*?\n(?:.*\n)*?    return df\n",
    APP_PY_SRC,
)
check("_drop_forming_candle source extracted from app.py", _func_match is not None)
_ns = {
    "_TF_SECONDS_MAP": {"1m": 60, "5m": 300},
    "datetime": datetime, "timedelta": timedelta, "timezone": timezone,
}
exec(_func_match.group(0), _ns)
_drop_forming_candle = _ns["_drop_forming_candle"]


def _make_indexed_df(n, last_start, freq_seconds):
    idx = pd.date_range(end=last_start, periods=n, freq=f"{freq_seconds}s", tz="UTC")
    return pd.DataFrame({"open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0}, index=idx)


now = datetime.now(timezone.utc)
forming_last_start = now - timedelta(seconds=10)  # 1m candle still forming
df_200 = _make_indexed_df(200, forming_last_start, 60)
df_199 = _drop_forming_candle(df_200, "1m")
check(
    "200 raw candles with a forming tail -> exactly 199 usable (the observed case)",
    len(df_200) == 200 and len(df_199) == 199,
)
check(
    "no second candle is accidentally removed",
    len(df_199) == len(df_200) - 1,
)


# ─── 3. app.py's _backtest_fetch_candles applies the closed-candle rule ─────
print("\n=== 3. _backtest_fetch_candles() applies _drop_forming_candle + attrs (source check) ===")
_bt_fetch_match = re.search(
    r"async def _backtest_fetch_candles\(.*?\n(?:.*\n)*?    return df\n",
    APP_PY_SRC,
)
check("_backtest_fetch_candles source found in app.py", _bt_fetch_match is not None)
if _bt_fetch_match:
    _src = _bt_fetch_match.group(0)
    check(
        "_backtest_fetch_candles calls _drop_forming_candle",
        "_drop_forming_candle(df, timeframe)" in _src,
    )
    check(
        "raw count is captured BEFORE trimming, not after",
        _src.index("candles_returned_raw = len(df)") < _src.index("_drop_forming_candle(df, timeframe)"),
    )
    check(
        "candles_requested / candles_returned_raw are recorded as df.attrs",
        'df.attrs["candles_requested"] = count' in _src
        and 'df.attrs["candles_returned_raw"] = candles_returned_raw' in _src,
    )
    check(
        "attrs assignment is wrapped so it can never crash the fetch (best-effort only)",
        "except Exception" in _src,
    )

print("\n=== 3b. _run_pipeline's primary analysis exposes the same transparency fields ===")
check(
    "candles_requested is exposed in the /api/signal result payload",
    '"candles_requested": cfg.CANDLE_COUNT' in APP_PY_SRC,
)
check(
    "candles_returned_raw is exposed in the /api/signal result payload",
    '"candles_returned_raw": _candles_returned_raw' in APP_PY_SRC,
)
check(
    "_candles_returned_raw is captured before the closed-candle trim",
    APP_PY_SRC.index("_candles_returned_raw = len(df)")
    < APP_PY_SRC.index('df = _drop_forming_candle(df, timeframe)\n\n        otc_settings'),
)


# ─── 4. backtest_engine.py — real runtime test of the full loop ────────────
print("\n=== 4. backtest_engine.py real _run_loop(): transparency fields + gates ===")


def _make_plain_df(n):
    """Same synthetic-data pattern the project's own test_phase_8_6.py uses
    (plain RangeIndex — analyzer/backtest math doesn't require a
    DatetimeIndex; only _drop_forming_candle(), tested separately above,
    does)."""
    np.random.seed(7)
    close = 100 + np.cumsum(np.random.randn(n) * 0.3)
    high = close + np.abs(np.random.randn(n) * 0.2)
    low = close - np.abs(np.random.randn(n) * 0.2)
    open_ = close + np.random.randn(n) * 0.1
    return pd.DataFrame({
        "open": open_, "high": high, "low": low, "close": close,
        "volume": np.random.randint(100, 1000, n),
    })


async def _fake_fetch_candles_undersized(asset, timeframe, count):
    """Simulates the real-world Quotex ceiling: caller asks for `count`
    (e.g. 2000), but only ~200 raw candles are actually available/returned
    — exactly the scenario traced in section 1 above — already run through
    the closed-candle trim (199), with .attrs set exactly as
    _backtest_fetch_candles() now sets them."""
    df = _make_plain_df(199)
    df.attrs["candles_requested"] = count
    df.attrs["candles_returned_raw"] = 200
    return df


async def _run_one_asset(fetch_fn, candle_count):
    engine = backtest_engine.BacktestEngine(fetch_candles=fetch_fn)
    # Minimal manual setup mirroring what start() does, without needing the
    # threaded-loop submission machinery (_run_loop is a plain coroutine).
    engine.total_assets = 1
    engine.assets_processed = 0
    engine.results = {}
    engine.candles_processed = 0
    engine.total_candles = candle_count
    engine.started_at = backtest_engine._now()
    await engine._run_loop(["EURUSD_otc"], "1m", candle_count, 4)
    return engine


engine_undersized = asyncio.run(_run_one_asset(_fake_fetch_candles_undersized, 2000))
_result = engine_undersized.results.get("EURUSD_otc", {})

check("asset result status is SUCCESS (fetch/compute did not error)", _result.get("status") == "SUCCESS")
check("candles_used reflects the real (trimmed) count, 199", _result.get("candles_used") == 199)
check("candles_requested is surfaced and equals what was actually asked for (2000)",
      _result.get("candles_requested") == 2000)
check("candles_returned_raw is surfaced and equals the pre-trim count (200)",
      _result.get("candles_returned_raw") == 200)
check(
    "candle_count_met is honestly False (199 < 2000) — gate NOT weakened",
    _result.get("candle_count_met") is False,
)

engine_undersized._build_summary(2000)
check(
    "summary.all_assets_met_candle_count is False for an undersized batch",
    engine_undersized.summary.get("all_assets_met_candle_count") is False,
)
check(
    "summary.candle_count_target still reports the real requested value (2000)",
    engine_undersized.summary.get("candle_count_target") == 2000,
)


# Sanity check: when enough real candles ARE available, the gate correctly
# passes — proves the gate still WORKS, not just that it still fails.
async def _fake_fetch_candles_sufficient(asset, timeframe, count):
    df = _make_plain_df(count)
    df.attrs["candles_requested"] = count
    df.attrs["candles_returned_raw"] = count
    return df


engine_sufficient = asyncio.run(_run_one_asset(_fake_fetch_candles_sufficient, 2000))
_result2 = engine_sufficient.results.get("EURUSD_otc", {})
check(
    "candle_count_met is True when the real requirement genuinely is met",
    _result2.get("candle_count_met") is True,
)


# ─── 5. Safety invariants unchanged ─────────────────────────────────────────
print("\n=== 5. Safety invariants ===")
check("backtest.MIN_SIGNALS_REQUIRED == 20", backtest.MIN_SIGNALS_REQUIRED == 20)
check("backtest_engine.DEFAULT_CANDLE_COUNT == 2000", backtest_engine.DEFAULT_CANDLE_COUNT == 2000)
check(
    "backtest_engine.CANDLE_OPTIONS unchanged (500,1000,1500,2000,3000,5000)",
    backtest_engine.CANDLE_OPTIONS == (500, 1000, 1500, 2000, 3000, 5000),
)
_settings_src = open(os.path.join(_WEBAPP, "settings_store.py"), encoding="utf-8").read()
check(
    "settings_store min_candles still 2000",
    re.search(r'"min_candles"\s*:\s*2000', _settings_src) is not None,
)
check(
    "settings_store min_indicator_sample_size still 100",
    re.search(r'"min_indicator_sample_size"\s*:\s*100', _settings_src) is not None,
)
sys.path.insert(0, _MARKET_ANALYZER)
import analyzer
check("analyzer.MIN_AGREEING_FACTORS == 2", analyzer.MIN_AGREEING_FACTORS == 2)


# ─── 6. A zero-sample factor is never faked ─────────────────────────────────
print("\n=== 6. Zero-sample factors are reported honestly, never faked ===")
# A flat/constant-price series makes several oscillator-style factors fire
# zero real votes over a short window — proves sample_size=0 survives
# untouched rather than being defaulted to something non-zero.
flat_df = pd.DataFrame({
    "open": [100.0] * 30, "high": [100.0] * 30, "low": [100.0] * 30,
    "close": [100.0] * 30, "volume": [500] * 30,
})
_acc = backtest.backtest_factor_accuracy(flat_df, lookahead=4)
_zero_factors = [k for k, v in _acc.items() if v["sample_size"] == 0]
check("at least one factor genuinely produced sample_size == 0 on flat data", len(_zero_factors) > 0)
for k in _zero_factors:
    check(f"'{k}' sample_size=0 has accuracy=None (not faked/defaulted)", _acc[k]["accuracy"] is None)


print(f"\n=== RESULT: {passed} passed, {failed} failed ===")
sys.exit(1 if failed else 0)
