"""
Targeted regression suite for the "EMA200 pill / 13-factor API / closed-candle /
MTF PARTIAL reporting" patch.

Run with:  python3 Quotex/tests/test_patch_ema200_closedcandle_mtf.py
(from the repo root, or anywhere — path setup below is self-contained).

Covers:
  1. MIN_AGREEING_FACTORS is still exactly 2 (unchanged by this patch).
  2. Backtest safety gates (MIN_SIGNALS_REQUIRED=20, settings_store's
     min_candles=2000 / min_indicator_sample_size=100) are unchanged.
  3. webapp/app.py's _FACTOR_LABELS now contains exactly the 13 keys
     analyzer.DEFAULT_CONFLUENCE_WEIGHTS uses — no more, no less.
  4. webapp/app.py's _drop_forming_candle() (extracted and exec'd standalone,
     since app.py itself cannot be imported in a sandbox without loguru/
     websockets/cloudscraper — same limitation the project's own
     TEST_REPORT.md notes) correctly drops a still-forming last candle,
     keeps an already-closed last candle, and never empties a single-row
     frame.
  5. The MTF PARTIAL branch reports status "UNAVAILABLE" (not "DISAGREED"),
     and the CONFLICTING branch still reports "DISAGREED" — source-level
     check, for the same import-constraint reason as #4.
  6. static/app.js's "EMA 200" filter pill expression is no longer identical
     to the "EMA 50" pill's expression, and references the new ema_200
     backend field rather than trend direction alone.
  7. webapp/app.py's indicators payload now exposes "ema_200".

No network access, no live Quotex connection, no Flask/loguru/websockets
import required — matches this project's established sandbox-testing
convention (see docs/TEST_REPORT.md) plus a source-level fallback for the
handful of checks that live inside app.py, which cannot be imported here.
"""
import sys
import os
import re
import ast

_HERE = os.path.dirname(os.path.abspath(__file__))
_MARKET_ANALYZER = os.path.join(_HERE, "..", "market_analyzer")
_WEBAPP = os.path.join(_MARKET_ANALYZER, "webapp")
sys.path.insert(0, _MARKET_ANALYZER)

import pandas as pd
from datetime import datetime, timedelta, timezone

import analyzer
import backtest

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

APP_JS_PATH = os.path.join(_WEBAPP, "static", "app.js")
APP_JS_SRC = open(APP_JS_PATH, encoding="utf-8").read()


# ─── 1 & 2. Safety gates untouched by this patch ────────────────────────────
print("\n=== 1. MIN_AGREEING_FACTORS untouched ===")
check("analyzer.MIN_AGREEING_FACTORS == 2", analyzer.MIN_AGREEING_FACTORS == 2)

print("\n=== 2. Backtest safety gates untouched ===")
check("backtest.MIN_SIGNALS_REQUIRED == 20", backtest.MIN_SIGNALS_REQUIRED == 20)
_settings_src = open(
    os.path.join(_WEBAPP, "settings_store.py"), encoding="utf-8"
).read()
check(
    "settings_store still has min_candles: 2000",
    re.search(r'"min_candles"\s*:\s*2000', _settings_src) is not None,
)
check(
    "settings_store still has min_indicator_sample_size: 100",
    re.search(r'"min_indicator_sample_size"\s*:\s*100', _settings_src) is not None,
)


# ─── 3. _FACTOR_LABELS == the real 13 confluence factor keys ────────────────
print("\n=== 3. _FACTOR_LABELS has exactly the 13 real confluence factors ===")
tree = ast.parse(APP_PY_SRC, filename=APP_PY_PATH)
factor_labels = None
for node in ast.walk(tree):
    if isinstance(node, ast.Assign) and any(
        getattr(t, "id", None) == "_FACTOR_LABELS" for t in node.targets
    ):
        factor_labels = ast.literal_eval(node.value)
        break
check("_FACTOR_LABELS was found in app.py", factor_labels is not None)
check(
    "_FACTOR_LABELS has exactly 13 keys",
    factor_labels is not None and len(factor_labels) == 13,
)
check(
    "_FACTOR_LABELS keys == analyzer.DEFAULT_CONFLUENCE_WEIGHTS keys",
    factor_labels is not None
    and set(factor_labels.keys()) == set(analyzer.DEFAULT_CONFLUENCE_WEIGHTS.keys()),
)
for k in ("wick_rejection", "liquidity_sweep", "false_breakout",
          "mean_reversion", "exhaustion", "round_number"):
    check(
        f"_FACTOR_LABELS contains previously-missing factor '{k}'",
        factor_labels is not None and k in factor_labels,
    )


# ─── 4. _drop_forming_candle() behavior ─────────────────────────────────────
print("\n=== 4. _drop_forming_candle() (extracted from app.py, exec'd standalone) ===")
_func_match = re.search(
    r"def _drop_forming_candle\(.*?\n(?:.*\n)*?    return df\n",
    APP_PY_SRC,
)
check("_drop_forming_candle source was extracted from app.py", _func_match is not None)

_ns = {
    "_TF_SECONDS_MAP": {"1m": 60, "5m": 300, "15m": 900},
    "datetime": datetime,
    "timedelta": timedelta,
    "timezone": timezone,
}
exec(_func_match.group(0), _ns)
_drop_forming_candle = _ns["_drop_forming_candle"]


def _make_df(n, last_start, freq_seconds):
    idx = pd.date_range(
        end=last_start, periods=n, freq=f"{freq_seconds}s", tz="UTC"
    )
    return pd.DataFrame(
        {"open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0}, index=idx
    )


now = datetime.now(timezone.utc)

# Case A: last candle's period has already fully closed -> keep it.
closed_last_start = now - timedelta(seconds=120)  # 1m candle closed ~60s ago
df_closed = _make_df(5, closed_last_start, 60)
out_closed = _drop_forming_candle(df_closed, "1m")
check(
    "already-closed last candle is kept",
    len(out_closed) == len(df_closed) and out_closed.index[-1] == df_closed.index[-1],
)

# Case B: last candle is still forming (its boundary is in the future) -> drop it.
forming_last_start = now - timedelta(seconds=10)  # 1m candle still forming
df_forming = _make_df(5, forming_last_start, 60)
out_forming = _drop_forming_candle(df_forming, "1m")
check(
    "still-forming last candle is dropped",
    len(out_forming) == len(df_forming) - 1
    and out_forming.index[-1] == df_forming.index[-2],
)

# Case C: single-row frame, still forming -> kept (never emptied).
df_single = _make_df(1, forming_last_start, 60)
out_single = _drop_forming_candle(df_single, "1m")
check(
    "single-row still-forming frame is never emptied",
    len(out_single) == 1,
)

# Case D: unknown timeframe key -> returned untouched, no guessing.
out_unknown_tf = _drop_forming_candle(df_forming, "not_a_real_tf")
check(
    "unknown timeframe leaves df untouched",
    len(out_unknown_tf) == len(df_forming),
)

# Case E: empty frame -> returned untouched.
out_empty = _drop_forming_candle(df_forming.iloc[0:0], "1m")
check("empty df stays empty, no exception", out_empty.empty)

# Case F: repeated calls within the same forming period return the same
# latest CLOSED candle (no flip from forming OHLC).
out_forming_2 = _drop_forming_candle(df_forming, "1m")
check(
    "repeated calls on the same forming snapshot return the same closed candle",
    out_forming.index[-1] == out_forming_2.index[-1],
)


# ─── 5. MTF PARTIAL -> UNAVAILABLE (not DISAGREED), CONFLICTING -> DISAGREED ─
print("\n=== 5. MTF PARTIAL/CONFLICTING status-mapping (source-level check) ===")
# The mapping is inside _run_pipeline(); app.py itself can't be imported in
# this sandbox (loguru/websockets/cloudscraper unavailable, same limitation
# noted in docs/TEST_REPORT.md), so this is verified directly against the
# shipped source text rather than at runtime.
_mapping_block_match = re.search(
    r'if mt_status == "CONFIRMED":.*?else:\s*# PARTIAL.*?\n\s*\}',
    APP_PY_SRC,
    re.DOTALL,
)
check("MTF status-mapping block was found in app.py", _mapping_block_match is not None)
_mapping_block = _mapping_block_match.group(0) if _mapping_block_match else ""

_confirmed_block = re.search(r'"CONFIRMED":.*?\}', _mapping_block, re.DOTALL)
_conflicting_block = re.search(
    r'elif mt_status == "CONFLICTING":.*?\}', _mapping_block, re.DOTALL
)
_partial_block = re.search(r"else:\s*# PARTIAL.*?\}", _mapping_block, re.DOTALL)

check(
    "CONFLICTING branch still maps to status DISAGREED",
    _conflicting_block is not None
    and '"status": "DISAGREED"' in _conflicting_block.group(0),
)
check(
    "PARTIAL branch maps to status UNAVAILABLE (fixed)",
    _partial_block is not None
    and '"status": "UNAVAILABLE"' in _partial_block.group(0),
)
check(
    "PARTIAL branch no longer maps to DISAGREED",
    _partial_block is not None
    and '"status": "DISAGREED"' not in _partial_block.group(0),
)
check(
    "PARTIAL branch keeps the honest WAIT reason text",
    _partial_block is not None
    and "did not provide confirmation (WAIT)" in _partial_block.group(0),
)

# The +10 / -15 confidence math and "never forced to WAIT" comment must be
# byte-for-byte untouched by this patch.
check(
    "MTF confidence math (+10 confirmed) is unchanged",
    "confluence[\"confidence\"] + 10" in APP_PY_SRC,
)
check(
    "MTF confidence math (-15 conflicting) is unchanged",
    "confluence[\"confidence\"] - 15" in APP_PY_SRC,
)
check(
    "MTF still never forces the primary signal to WAIT",
    "no more\n                # forcing WAIT on disagreement" in APP_PY_SRC
    or "no more forcing WAIT on disagreement" in APP_PY_SRC,
)


# ─── 6. EMA 200 pill no longer duplicates EMA 50 ────────────────────────────
print("\n=== 6. app.js EMA 200 pill fix ===")
_ema50_line_match = re.search(r"label: 'EMA 50'.*", APP_JS_SRC)
_ema200_block_match = re.search(
    r"label: 'EMA 200'.*?(?:\},|\}\s*\n\s*\?)", APP_JS_SRC, re.DOTALL
)
check("EMA 50 pill line found", _ema50_line_match is not None)
check("EMA 200 pill block found", _ema200_block_match is not None)
if _ema50_line_match and _ema200_block_match:
    ema50_expr = _ema50_line_match.group(0)
    ema200_expr = _ema200_block_match.group(0)
    check(
        "EMA 200 pill expression is not identical to EMA 50's",
        ema200_expr.replace("200", "50") != ema50_expr
        and "trendWord(data.trend) !== 'Neutral' ? 'pass' : 'fail'" not in ema200_expr,
    )
    check(
        "EMA 200 pill references the real ema_200 backend field",
        "ind.ema_200" in ema200_expr,
    )
    check(
        "EMA 200 pill does not silently default to 'pass'/'fail' with no data check",
        "'na'" in ema200_expr,
    )


# ─── 7. Backend exposes ema_200 in the indicators payload ───────────────────
print("\n=== 7. app.py exposes ema_200 in the API response ===")
check(
    '"ema_200": indicators.get("ema_200") is present in the indicators payload',
    '"ema_200": indicators.get("ema_200")' in APP_PY_SRC,
)


print(f"\n=== RESULT: {passed} passed, {failed} failed ===")
sys.exit(1 if failed else 0)
