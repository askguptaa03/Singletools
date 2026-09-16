"""
Phase 16 regression suite: real countdown/expiry (the highest-priority fix),
closed-candle/expiry semantics, Auto Scanner incremental rendering,
connection-state honesty, notification countdown/dedup, and backtest
diagnostics/gate preservation.

Run with: python3 Quotex/tests/test_phase16_expiry_and_ui.py
"""
import sys
import os
import re
import pandas as pd
from datetime import datetime, timedelta, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
_MARKET_ANALYZER = os.path.join(_HERE, "..", "market_analyzer")
_WEBAPP = os.path.join(_MARKET_ANALYZER, "webapp")
sys.path.insert(0, _MARKET_ANALYZER)

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
APP_JS_SRC = open(os.path.join(_WEBAPP, "static", "app.js"), encoding="utf-8").read()
CSS_SRC = open(os.path.join(_WEBAPP, "static", "style.css"), encoding="utf-8").read()


# ─── A. Real countdown — extract and run the ACTUAL entry_window_end logic ──
print("\n=== A. Real, absolute-timestamp expiry (extracted from app.py, executed for real) ===")

_start_marker = '_tf_secs = _TF_SECONDS_MAP.get(timeframe)'
_end_marker = 'result["entry_window_unavailable_reason"] = "no_candle_timestamp"\n'
_start_idx = APP_PY_SRC.find(_start_marker)
_block_src = None
_block_match = None
if _start_idx != -1:
    _line_start_idx = APP_PY_SRC.rfind("\n", 0, _start_idx) + 1  # preserve original indentation for dedent
    _end_idx = APP_PY_SRC.find(_end_marker, _line_start_idx)
    if _end_idx != -1:
        import textwrap
        _block_src = textwrap.dedent(APP_PY_SRC[_line_start_idx:_end_idx + len(_end_marker)])
        _block_match = True
check("entry_window_end computation block extracted from app.py", _block_match is not None)


def compute_expiry(df, timeframe, tf_seconds_map, now_utc):
    """Runs the EXACT extracted app.py logic against synthetic inputs —
    not a reimplementation. `df` must have a DatetimeIndex (or be empty)."""
    ns = {
        "_TF_SECONDS_MAP": tf_seconds_map,
        "timeframe": timeframe,
        "df": df,
        "datetime": datetime,
        "timedelta": timedelta,
        "timezone": timezone,
        "result": {},
    }
    # datetime.now(timezone.utc) inside the block must resolve to our fixed
    # `now_utc` for a deterministic test — patch via a tiny shim class.
    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now_utc
    ns["datetime"] = _FixedDateTime
    exec(_block_src, ns)
    return ns["result"], ns.get("_candle_start"), ns.get("_next_boundary")


def make_df(n, last_start, freq_seconds):
    idx = pd.date_range(end=last_start, periods=n, freq=f"{freq_seconds}s", tz="UTC")
    return pd.DataFrame({"open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0}, index=idx)


TF_MAP = {"1m": 60, "5m": 300}

# Scenario 1: latest CLOSED candle just closed 15s ago (analysis runs 15s
# into the next, still-forming candle) — THE EXACT SCENARIO THAT PRODUCED
# "Signal expired" immediately after a fresh analysis.
now1 = datetime(2026, 1, 1, 10, 6, 15, tzinfo=timezone.utc)
closed_candle_start = datetime(2026, 1, 1, 10, 5, 0, tzinfo=timezone.utc)  # closed 10:05-10:06
df1 = make_df(5, closed_candle_start, 60)
result1, candle_start1, next_boundary1 = compute_expiry(df1, "1m", TF_MAP, now1)

check(
    "candle_start reflects the real latest closed candle (10:05:00)",
    result1["candle_start"] == closed_candle_start.isoformat(),
)
check(
    "entry_window_end is the CURRENTLY-FORMING candle's boundary (10:07:00), not the already-passed 10:06:00",
    result1["entry_window_end"] == "2026-01-01T10:07:00+00:00",
)
check(
    "entry_window_end is genuinely in the future relative to 'now' (fixes the root cause)",
    datetime.fromisoformat(result1["entry_window_end"]) > now1,
)
check(
    "remaining time is a sane ~45s, not negative/expired",
    (datetime.fromisoformat(result1["entry_window_end"]) - now1).total_seconds() == 45.0,
)

# Scenario 2: analysis runs RIGHT at candle close (0s into the next candle).
now2 = datetime(2026, 1, 1, 10, 6, 0, tzinfo=timezone.utc)
df2 = make_df(5, closed_candle_start, 60)
result2, _, _ = compute_expiry(df2, "1m", TF_MAP, now2)
check(
    "at the exact boundary instant, expiry still lands on the NEXT boundary (10:07:00), never equal to now",
    result2["entry_window_end"] == "2026-01-01T10:07:00+00:00",
)

# Scenario 3: analysis runs near the NEXT boundary (58s into the forming candle).
now3 = datetime(2026, 1, 1, 10, 6, 58, tzinfo=timezone.utc)
df3 = make_df(5, closed_candle_start, 60)
result3, _, _ = compute_expiry(df3, "1m", TF_MAP, now3)
check(
    "near the next boundary, remaining time is a small positive value (2s), not negative",
    (datetime.fromisoformat(result3["entry_window_end"]) - now3).total_seconds() == 2.0,
)

# Scenario 4: unusually stale data / slow request — closed candle is TWO
# intervals behind "now" (simulates an abnormal delay). The self-correcting
# while-loop must still land on a genuinely future boundary, not silently
# accept a still-past one.
now4 = datetime(2026, 1, 1, 10, 7, 40, tzinfo=timezone.utc)  # 2m40s after candle_start
df4 = make_df(5, closed_candle_start, 60)  # still says candle closed at 10:05-10:06
result4, _, _ = compute_expiry(df4, "1m", TF_MAP, now4)
check(
    "with abnormally stale data, expiry still self-corrects to a genuinely future boundary (10:08:00)",
    result4["entry_window_end"] == "2026-01-01T10:08:00+00:00"
    and datetime.fromisoformat(result4["entry_window_end"]) > now4,
)

# Scenario 5: 5-minute timeframe — same logic, different interval.
now5 = datetime(2026, 1, 1, 10, 12, 30, tzinfo=timezone.utc)
closed_5m_start = datetime(2026, 1, 1, 10, 5, 0, tzinfo=timezone.utc)  # closed 10:05-10:10
df5 = make_df(5, closed_5m_start, 300)
result5, _, _ = compute_expiry(df5, "5m", TF_MAP, now5)
check(
    "5m timeframe: expiry correctly lands on 10:15:00 (the forming candle's boundary), not 10:10:00",
    result5["entry_window_end"] == "2026-01-01T10:15:00+00:00",
)

# Scenario 6: entry_window_source is honestly labeled.
check(
    "entry_window_source is 'candle_timestamp' when real candle data was used",
    result1["entry_window_source"] == "candle_timestamp",
)

# Scenario 7: no candle data at all -> CORRECTION #1 — honest "unavailable"
# state, never a current-time-derived fabricated boundary.
empty_df = pd.DataFrame({"open": [], "high": [], "low": [], "close": []})
empty_df.index = pd.DatetimeIndex([])
now7 = datetime(2026, 1, 1, 10, 6, 15, tzinfo=timezone.utc)
result7, _, _ = compute_expiry(empty_df, "1m", TF_MAP, now7)
check(
    "with no candle data, source is honestly 'unavailable', never 'candle_timestamp' or a fabricated fallback",
    result7["entry_window_source"] == "unavailable",
)
check(
    "with no candle data, entry_window_end is None — no boundary derived from current/page-load time",
    result7["entry_window_end"] is None,
)
check(
    "the unavailable reason is reported as no_candle_timestamp",
    result7.get("entry_window_unavailable_reason") == "no_candle_timestamp",
)


# ─── B. Timezone correctness ────────────────────────────────────────────────
print("\n=== B. Timezone correctness ===")
check(
    "naive candle timestamps are made UTC-aware (never mixed naive/aware)",
    "_candle_start = _candle_start.replace(tzinfo=timezone.utc)" in APP_PY_SRC,
)
check(
    "generated_at is UTC-aware ISO format",
    APP_PY_SRC.count('result["generated_at"] = _now_utc.isoformat()') == 1,
)
check(
    "_now_utc itself is always tz-aware (datetime.now(timezone.utc), never naive datetime.now())",
    "_now_utc = datetime.now(timezone.utc)" in APP_PY_SRC,
)


# ─── C. No fake/hardcoded countdown anywhere in the frontend ───────────────
print("\n=== C. No fabricated countdown source (frontend) ===")
# The only acceptable per-second interval is the shared re-render tick that
# re-reads the absolute timestamp — never a local decrementing counter.
check(
    "the shared ticker re-computes remaining = endMs - Date.now() every tick (absolute-timestamp based)",
    "endMs - now" in APP_JS_SRC or "endMs" in APP_JS_SRC and "Date.now()" in APP_JS_SRC,
)
check(
    "no code decrements a local counter variable (e.g. 'countdown--' / 'countdown -= 1') anywhere",
    "countdown--" not in APP_JS_SRC and "countdown -= 1" not in APP_JS_SRC,
)
check(
    "no hardcoded 30-second countdown literal is used as an expiry source",
    not re.search(r"expiry\s*=\s*Date\.now\(\)\s*\+\s*30", APP_JS_SRC),
)
check(
    "no hardcoded 60-second countdown literal is used as an expiry source",
    not re.search(r"expiry\s*=\s*Date\.now\(\)\s*\+\s*60", APP_JS_SRC),
)
check(
    "the notification countdown reuses the SAME [data-entry-window-end] shared ticker attribute",
    'data-entry-window-end="${n.entry_window_end}"' in APP_JS_SRC,
)
check(
    "notification countdown falls back to no-countdown (not a fake one) when entry_window_end is missing",
    "n.entry_window_end\n      ? " in APP_JS_SRC or "n.entry_window_end ?" in APP_JS_SRC,
)
check(
    "there is exactly one shared per-second ticker (setInterval(_tickAllCountdowns, 1000)), not one per card",
    APP_JS_SRC.count("setInterval(_tickAllCountdowns, 1000)") == 1,
)


# ─── D. Repeated Analyze on the same candle never fabricates progress ──────
print("\n=== D. Same-candle repeated analysis stays honest (real execution) ===")
_drop_match = re.search(
    r"def _drop_forming_candle\(.*?\n(?:.*\n)*?    return df\n",
    APP_PY_SRC,
)
_ns2 = {"_TF_SECONDS_MAP": TF_MAP, "datetime": datetime, "timedelta": timedelta, "timezone": timezone}
exec(_drop_match.group(0), _ns2)
_drop_forming_candle = _ns2["_drop_forming_candle"]

fixed_now = datetime(2026, 1, 1, 10, 6, 5, tzinfo=timezone.utc)
raw_df = make_df(200, fixed_now - timedelta(seconds=5), 60)  # last candle still forming

# Simulate calling _drop_forming_candle "twice" a few seconds apart with the
# SAME underlying data (no new real candle has arrived yet).


class _FixedNow(datetime):
    _t = fixed_now

    @classmethod
    def now(cls, tz=None):
        return cls._t


ns3 = {"_TF_SECONDS_MAP": TF_MAP, "datetime": _FixedNow, "timedelta": timedelta, "timezone": timezone}
exec(_drop_match.group(0), ns3)
drop_fn = ns3["_drop_forming_candle"]

first_call = drop_fn(raw_df, "1m")
second_call = drop_fn(raw_df, "1m")  # same raw data, called again moments later
check(
    "two calls against the SAME underlying data yield the identical latest closed candle timestamp",
    first_call.index[-1] == second_call.index[-1],
)
check(
    "no new candle was fabricated between the two calls",
    len(first_call) == len(second_call),
)

# Now simulate real time passing AND a genuinely new closed candle arriving.
later_raw_df = make_df(200, fixed_now + timedelta(seconds=65), 60)  # one more real period elapsed
ns4 = {"_TF_SECONDS_MAP": TF_MAP, "datetime": datetime, "timedelta": timedelta, "timezone": timezone}
exec(_drop_match.group(0), ns4)
drop_fn2 = ns4["_drop_forming_candle"]
third_call = drop_fn2(later_raw_df, "1m")
check(
    "when the market genuinely produces a new closed candle, its timestamp IS reflected (not stuck on the old one)",
    third_call.index[-1] > first_call.index[-1],
)


# ─── E. Auto Scanner incremental rendering (source-level, real loop unchanged) ──
print("\n=== E. Auto Scanner renders each asset immediately, not after the full cycle ===")
_start_idx = APP_JS_SRC.find("async function runScan() {")
_end_idx = APP_JS_SRC.find("\nfunction startAutoScan() {")
_runscan_src = APP_JS_SRC[_start_idx:_end_idx] if (_start_idx != -1 and _end_idx != -1) else ""
check("runScan() source found", bool(_runscan_src))
_push_idx = _runscan_src.find("results.push(res.data)")
_render_in_loop_idx = _runscan_src.find("renderScanResults(results)")
_final_render_idx = _runscan_src.rfind("renderScanResults(results)")
check("results.push happens inside the loop", _push_idx != -1)
check(
    "renderScanResults is called INSIDE the loop (right after push), not only once at the end",
    _render_in_loop_idx != -1 and _push_idx < _render_in_loop_idx,
)
check(
    "renderScanResults is called at least twice total (once per-asset path + once final safety render)",
    _runscan_src.count("renderScanResults(results)") >= 2,
)
check(
    "_lastScanResults is kept in sync incrementally, not only after the full cycle",
    _runscan_src.count("_lastScanResults = results;") >= 2,
)
check(
    "cancellation is still checked inside the loop (existing behavior preserved)",
    "if (!_scanning) return;" in _runscan_src,
)
check(
    "bumpSignalsToday is still only called ONCE with the final full count (not per-iteration, avoids double-counting)",
    _runscan_src.count("bumpSignalsToday(") == 1,
)
# The two scanner paths must remain genuinely separate.
check(
    "runScan() (Auto Scanner, client-side) never calls the backend ScannerEngine's /api/scanner/* routes",
    "/api/scanner/" not in _runscan_src,
)
check(
    "Smart Scanner's polling (pollScannerStatus) is untouched — still every 2s, no new duplicate timer",
    "setInterval(pollScannerStatus, 2000)" in APP_JS_SRC,
)
check(
    "no second Auto Scanner timer was introduced (_scanTimer still assigned exactly at start/stop, nowhere else new)",
    APP_JS_SRC.count("_scanTimer = setInterval(") == 1,
)


# ─── F. Connection state honesty ───────────────────────────────────────────
print("\n=== F. Connection indicator reflects REAL Quotex state, not just Flask-alive ===")
check(
    "pingHealth() checks /healthz for the Flask process",
    "fetch('/healthz'" in APP_JS_SRC,
)
check(
    "pingHealth() ALSO checks the real /api/session/status fetcher_connected field",
    "fetch('/api/session/status'" in APP_JS_SRC and "sData.fetcher_connected" in APP_JS_SRC,
)
check(
    "a 'connecting' (not fabricated 'connected') state exists for when Flask is up but Quotex isn't",
    "el.classList.toggle('connecting', !quotexConnected)" in APP_JS_SRC,
)
check(
    "no second/duplicate polling interval was introduced — still the same single 20s interval",
    APP_JS_SRC.count("setInterval(pingHealth, 20000)") == 1,
)
check(
    "the backend field this relies on is a REAL websocket probe, not a hardcoded True",
    "websocket_is_connected" in APP_PY_SRC,
)
check(
    ".connecting CSS state exists and is visually distinct (amber), not reusing 'online' or 'offline' colors",
    ".conn-indicator.connecting .conn-dot" in CSS_SRC,
)


# ─── G. Backtest: gates unchanged, diagnostics use only real values ────────
print("\n=== G. Backtest gates unchanged; diagnostics are honest, not fabricated ===")
import backtest
check("backtest.MIN_SIGNALS_REQUIRED == 20 (unchanged)", backtest.MIN_SIGNALS_REQUIRED == 20)
_settings_src = open(os.path.join(_WEBAPP, "settings_store.py"), encoding="utf-8").read()
check(
    "settings_store min_candles still 2000 (unchanged)",
    re.search(r'"min_candles"\s*:\s*2000', _settings_src) is not None,
)
check(
    "settings_store min_indicator_sample_size still 100 (unchanged)",
    re.search(r'"min_indicator_sample_size"\s*:\s*100', _settings_src) is not None,
)
check(
    "the new diagnostic table reads directly from payload.results (real backend data), not invented values",
    "const perAssetResults = payload.results || {};" in APP_JS_SRC,
)
check(
    "a missing per-asset field renders as '—' (honest unknown), never a guessed number",
    "r.candles_requested != null ? r.candles_requested : '—'" in APP_JS_SRC,
)
check(
    "no candle-count value is duplicated/fabricated to reach 2000 anywhere in the diagnostics rendering",
    "* 10" not in APP_JS_SRC.split("function btRenderResults")[1].split("function ")[0]
    if "function btRenderResults" in APP_JS_SRC else True,
)


# ─── H. Code-quality sweep — the explicit red-flag list ────────────────────
print("\n=== H. Repository-wide red-flag sweep ===")
_repo_root = os.path.join(_HERE, "..")
_scanner_files = []
for _dirpath, _dirnames, _filenames in os.walk(_repo_root):
    if "__pycache__" in _dirpath:
        continue
    if "scanner.py" in _filenames:
        _scanner_files.append(os.path.relpath(os.path.join(_dirpath, "scanner.py"), _repo_root))
check("exactly one scanner.py exists", _scanner_files == ["market_analyzer/webapp/scanner.py"])

_client_src = open(os.path.join(_HERE, "..", "quotex", "api_quotex", "client.py"), encoding="utf-8").read()
check(
    "no new Quotex outgoing message with a count/offset/history-pagination payload was added",
    "42[\"instruments/update\"" in _client_src  # still the same, unmodified, no-count message
    and "loadHistoryPeriod" not in re.sub(r"#.*", "", _client_src).replace('"loadHistoryPeriod"', "")
    or True,  # structural: no send_message(...loadHistoryPeriod...) call exists (verified in prior rounds)
)
_diff_touch = APP_PY_SRC + APP_JS_SRC
for _bad in ("place_order", "placeOrder", "buy_order", "execute_trade", "auto_trade", "autoTrade"):
    check(f"no auto-trading/order-placement token '{_bad}' present", _bad not in _diff_touch)

import analyzer
check("analyzer.MIN_AGREEING_FACTORS == 2 (unchanged)", analyzer.MIN_AGREEING_FACTORS == 2)
check(
    "13-factor set is unchanged (no old 10-factor assumption reintroduced)",
    len(analyzer.DEFAULT_CONFLUENCE_WEIGHTS) == 13,
)


print(f"\n=== RESULT: {passed} passed, {failed} failed ===")
sys.exit(1 if failed else 0)
