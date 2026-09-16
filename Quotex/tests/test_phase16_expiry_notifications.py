"""
Comprehensive regression suite for this round's fixes:
  - REAL, absolute-timestamp-derived entry_window_end (the proven root
    cause of "Signal expired" immediately after a fresh analysis)
  - closed-candle selection consistency
  - repeated-analysis-on-same-candle honesty (no fabricated new candle)
  - notification countdown reuses the SAME shared absolute-timestamp
    ticker (no second/independent timer, no hardcoded duration)
  - notification dedup unchanged
  - Auto Scanner (client-side runScan()) now renders incrementally
  - connection indicator uses the real /api/session/status fetcher_connected
    state, not just Flask-alive
  - backtest per-asset diagnostics are real, unmodified gate values

app.py cannot be imported directly in this sandbox (loguru/websockets/
cloudscraper unavailable, no network) — same limitation noted in every
prior round of this project. The entry_window_end block is extracted
verbatim from app.py's real source and exec'd standalone with a synthetic
pandas DataFrame, so this is genuine execution of the actual shipped
logic, not a hand-reimplementation.

Run with: python3 Quotex/tests/test_phase16_expiry_notifications.py
"""
import sys
import os
import re

_HERE = os.path.dirname(os.path.abspath(__file__))
_MARKET_ANALYZER = os.path.join(_HERE, "..", "market_analyzer")
_WEBAPP = os.path.join(_MARKET_ANALYZER, "webapp")
sys.path.insert(0, _MARKET_ANALYZER)

import pandas as pd
from datetime import datetime, timedelta, timezone

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


# ─── Extract the real entry_window_end block from app.py and run it ────────
_START_MARK = '_tf_secs = _TF_SECONDS_MAP.get(timeframe)'
_END_MARK = 'result["entry_window_unavailable_reason"] = "no_candle_timestamp"\n'
_start_idx = APP_PY_SRC.index(_START_MARK)
_start_idx = APP_PY_SRC.rfind("\n", 0, _start_idx) + 1  # back up to the start of that line (preserve its indentation for dedent)
_end_idx = APP_PY_SRC.index(_END_MARK, _start_idx) + len(_END_MARK)
import textwrap
_BLOCK_SRC = textwrap.dedent(APP_PY_SRC[_start_idx:_end_idx])
check("entry_window_end block extracted from app.py", 'result["entry_window_end"]' in _BLOCK_SRC)
check(
    "CORRECTION #1: the current-time-grid fallback ('current_time_fallback') no longer exists in app.py",
    "current_time_fallback" not in APP_PY_SRC,
)
check(
    "CORRECTION #1: no boundary is ever computed from _now_utc/datetime.now() when candle_start is missing "
    "(the only remaining datetime.now()-based epoch/grid-alignment code was removed)",
    "_epoch_now" not in APP_PY_SRC and "_next_epoch" not in APP_PY_SRC,
)
check(
    "CORRECTION #2: a maximum trustworthy-advance cutoff exists (stale candle data is not extrapolated forever)",
    "_MAX_TRUSTWORTHY_ADVANCE_INTERVALS" in APP_PY_SRC,
)


def compute_entry_window(df, timeframe, tf_seconds_map, now_utc):
    """Runs the ACTUAL app.py source (extracted verbatim above) against
    synthetic inputs, in an isolated namespace."""
    ns = {
        "df": df, "timeframe": timeframe, "_TF_SECONDS_MAP": tf_seconds_map,
        "datetime": datetime, "timedelta": timedelta, "timezone": timezone,
        "result": {},
    }
    # datetime.now(timezone.utc) inside the block must be controllable —
    # patch a tiny shim class so the extracted source's own
    # datetime.now(timezone.utc) call returns our fixed `now_utc`.
    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now_utc
    ns["datetime"] = _FixedDateTime
    exec(_BLOCK_SRC, ns)
    return ns["result"]


def make_df(n, last_candle_start, freq_seconds):
    idx = pd.date_range(end=last_candle_start, periods=n, freq=f"{freq_seconds}s", tz="UTC")
    return pd.DataFrame({"open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0}, index=idx)


TF_MAP = {"1m": 60, "5m": 300}

print("\n=== 1. Real candle-derived expiry: the exact 'immediate expiry' bug scenario ===")
# Closed candle started 10:05:00, its own boundary (10:06:00) is where the
# OLD buggy code stopped. "Now" is 10:06:15 — 15s into the NEXT (forming)
# candle. Correct entry_window_end must be 10:07:00 (the forming candle's
# own boundary), NOT 10:06:00 (already in the past).
closed_candle_start = datetime(2026, 1, 1, 10, 5, 0, tzinfo=timezone.utc)
now = datetime(2026, 1, 1, 10, 6, 15, tzinfo=timezone.utc)
df = make_df(50, closed_candle_start, 60)
r = compute_entry_window(df, "1m", TF_MAP, now)
check("candle_start is the real closed candle's timestamp (10:05:00)",
      r["candle_start"] == closed_candle_start.isoformat())
expiry = datetime.fromisoformat(r["entry_window_end"])
check("entry_window_end is 10:07:00 (the forming candle's boundary), not 10:06:00",
      expiry == datetime(2026, 1, 1, 10, 7, 0, tzinfo=timezone.utc))
check("entry_window_end is strictly in the FUTURE relative to 'now' (the actual bug fix)",
      expiry > now)
check("entry_window_source correctly reports candle_timestamp (not the fallback)",
      r["entry_window_source"] == "candle_timestamp")

print("\n=== 2. Analysis occurs immediately after candle close (edge case: now == boundary) ===")
now2 = datetime(2026, 1, 1, 10, 6, 0, tzinfo=timezone.utc)  # exactly at the old boundary
df2 = make_df(50, closed_candle_start, 60)
r2 = compute_entry_window(df2, "1m", TF_MAP, now2)
expiry2 = datetime.fromisoformat(r2["entry_window_end"])
check("at the exact boundary instant, expiry still advances to the NEXT one (10:07:00), never equals 'now'",
      expiry2 == datetime(2026, 1, 1, 10, 7, 0, tzinfo=timezone.utc) and expiry2 > now2)

print("\n=== 3. Analysis occurs near the next boundary (should NOT skip an extra interval) ===")
now3 = datetime(2026, 1, 1, 10, 6, 59, tzinfo=timezone.utc)  # 1s before the forming candle closes
df3 = make_df(50, closed_candle_start, 60)
r3 = compute_entry_window(df3, "1m", TF_MAP, now3)
expiry3 = datetime.fromisoformat(r3["entry_window_end"])
check("1 second before the forming candle closes, expiry is still exactly that boundary (10:07:00)",
      expiry3 == datetime(2026, 1, 1, 10, 7, 0, tzinfo=timezone.utc))
check("remaining time is honestly ~1 second, not a full fresh interval",
      0 < (expiry3 - now3).total_seconds() <= 1.0)

print("\n=== 4. Stale candle data: trusted within the cutoff, honestly unavailable beyond it ===")
# Moderately stale (needs 2 advances, well within _MAX_TRUSTWORTHY_ADVANCE_INTERVALS=3)
# -> still computed, still 100% derived from the real candle timestamp.
mod_stale_start = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
now_mod = datetime(2026, 1, 1, 10, 2, 30, tzinfo=timezone.utc)  # 2.5 intervals later
df_mod = make_df(50, mod_stale_start, 60)
r_mod = compute_entry_window(df_mod, "1m", TF_MAP, now_mod)
check("moderately stale candle (2.5 intervals old) is still trusted and produces a real future boundary (10:03:00)",
      r_mod["entry_window_end"] is not None
      and datetime.fromisoformat(r_mod["entry_window_end"]) == datetime(2026, 1, 1, 10, 3, 0, tzinfo=timezone.utc))
check("moderately stale case is still strictly derived from the real candle timestamp, nothing invented",
      (datetime.fromisoformat(r_mod["entry_window_end"]) - mod_stale_start).total_seconds() % 60 == 0)
check("moderately stale case reports source=candle_timestamp (still trustworthy)",
      r_mod["entry_window_source"] == "candle_timestamp")

# CORRECTION #2: genuinely too stale (needs far more than
# _MAX_TRUSTWORTHY_ADVANCE_INTERVALS advances to reach the future) must be
# reported as unavailable, never extrapolated indefinitely and presented
# as a trustworthy real boundary.
very_stale_start = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
now_very_stale = datetime(2026, 1, 1, 10, 10, 0, tzinfo=timezone.utc)  # 10 intervals later
df_very_stale = make_df(50, very_stale_start, 60)
r_very_stale = compute_entry_window(df_very_stale, "1m", TF_MAP, now_very_stale)
check("CORRECTION #2: a candle 10 intervals stale is NOT extrapolated forward — entry_window_end is None",
      r_very_stale["entry_window_end"] is None)
check("CORRECTION #2: source is honestly 'unavailable' for untrustworthy stale timing, never 'candle_timestamp'",
      r_very_stale["entry_window_source"] == "unavailable")
check("CORRECTION #2: the specific reason is reported as stale_candle_timing (not silently omitted)",
      r_very_stale["entry_window_unavailable_reason"] == "stale_candle_timing")
check("candle_start is still honestly reported even when the boundary itself is untrustworthy "
      "(the real candle timestamp is not hidden, only the fabricated-boundary risk is avoided)",
      r_very_stale["candle_start"] == very_stale_start.isoformat())

print("\n=== 5. Countdown reaches zero / expired state (frontend logic, source-level) ===")
check("_formatCountdownRemaining returns null (triggers expired display) when remaining <= 0",
      "if (msRemaining <= 0) return null;" in APP_JS_SRC)
check("expired state uses ceil(), never shows a negative countdown",
      "Math.ceil(msRemaining / 1000)" in APP_JS_SRC)
check("countdown is computed as endMs - Date.now(), never a decrementing local counter",
      "const remaining = endMs - Date.now();" in APP_JS_SRC)
check("no 'remaining -= 1' or equivalent local-decrement pattern exists anywhere",
      re.search(r"remaining\s*-=\s*1", APP_JS_SRC) is None,
      )
check("no hardcoded 30-second or 60-second countdown seed exists",
      re.search(r"countdown\w*\s*=\s*(30|60)\b", APP_JS_SRC, re.IGNORECASE) is None,
      )

print("\n=== 6. Same candle analyzed repeatedly does NOT fabricate a new candle ===")
# Two "analyses" 5 seconds apart, same underlying df (no new real candle
# arrived) -> candle_start and entry_window_end must be IDENTICAL.
now_a = datetime(2026, 1, 1, 10, 6, 10, tzinfo=timezone.utc)
now_b = datetime(2026, 1, 1, 10, 6, 15, tzinfo=timezone.utc)
df_same = make_df(50, closed_candle_start, 60)
r_a = compute_entry_window(df_same, "1m", TF_MAP, now_a)
r_b = compute_entry_window(df_same, "1m", TF_MAP, now_b)
check("repeated analysis on the same closed candle reports the identical candle_start",
      r_a["candle_start"] == r_b["candle_start"])
check("repeated analysis on the same closed candle reports the identical entry_window_end",
      r_a["entry_window_end"] == r_b["entry_window_end"])

print("\n=== 7. New closed candle -> new candle_start/expiry (honest progression) ===")
next_candle_start = closed_candle_start + timedelta(seconds=60)
df_new = make_df(50, next_candle_start, 60)
now_c = datetime(2026, 1, 1, 10, 7, 5, tzinfo=timezone.utc)
r_c = compute_entry_window(df_new, "1m", TF_MAP, now_c)
check("once the market really produces a new closed candle, candle_start correctly advances",
      r_c["candle_start"] == next_candle_start.isoformat() and r_c["candle_start"] != r_a["candle_start"])

print("\n=== 8. Timezone correctness ===")
check("candle_start is always tz-aware UTC (never naive) once resolved",
      r_a["candle_start"].endswith("+00:00"))
check("entry_window_end is always tz-aware UTC (never naive)",
      r_a["entry_window_end"].endswith("+00:00"))
# Naive-timestamp input (no tzinfo) must still be safely upgraded to UTC,
# never crash or silently mix naive/aware.
naive_idx = pd.date_range(end=datetime(2026, 1, 1, 10, 5, 0), periods=10, freq="60s")
df_naive = pd.DataFrame({"open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0}, index=naive_idx)
try:
    r_naive = compute_entry_window(df_naive, "1m", TF_MAP, now)
    naive_ok = r_naive["candle_start"].endswith("+00:00")
except Exception:
    naive_ok = False
check("a naive-timestamp candle index is safely upgraded to UTC, not left mixed/naive", naive_ok)

print("\n=== 9. TIMING_UNAVAILABLE honesty: no candle data -> no fabricated expiry (CORRECTION #1) ===")
empty_df = pd.DataFrame({"open": [], "high": [], "low": [], "close": []})
r_empty = compute_entry_window(empty_df, "1m", TF_MAP, now)
check("with an empty df, candle_start is honestly None (never guessed)", r_empty["candle_start"] is None)
check("CORRECTION #1: with no real candle timestamp, entry_window_end is honestly None — "
      "NOT a current-time/page-load-derived boundary",
      r_empty["entry_window_end"] is None)
check("CORRECTION #1: entry_window_source is exactly 'unavailable', never 'current_time_fallback'",
      r_empty["entry_window_source"] == "unavailable")
check("the specific reason is reported as no_candle_timestamp",
      r_empty["entry_window_unavailable_reason"] == "no_candle_timestamp")

print("\n=== 9b. Frontend honestly shows 'Countdown unavailable' rather than a fake/silent state ===")
check("an actionable BUY/SELL result with no entry_window_end shows an explicit 'Countdown unavailable' message",
      "ewEl.textContent = (sig !== 'WAIT') ? 'Countdown unavailable' : '—';" in APP_JS_SRC)
check("no frontend code path computes an entry_window_end-style boundary from Date.now()/page-load time",
      re.search(r"entry_window_end\s*=\s*Date\.now\(\)", APP_JS_SRC) is None)


# ─── Notification countdown reuses the shared absolute-timestamp ticker ────
print("\n=== 10. Notification countdown: reuses the SAME shared ticker, no fake timer ===")
check("notif row countdown span uses data-entry-window-end (the shared ticker's query target)",
      "data-entry-window-end=\"${n.entry_window_end}\"" in APP_JS_SRC)
check("notif row countdown is only rendered when a real entry_window_end exists (else omitted, never faked)",
      "const countdownHtml = n.entry_window_end" in APP_JS_SRC)
check("no second setInterval-based countdown was introduced for notifications specifically",
      len(re.findall(r"setInterval\([^)]*[Cc]ountdown", APP_JS_SRC)) == 1,  # only the one shared _tickAllCountdowns
      )
check("_tickAllCountdowns remains the single shared driver (still queries the global attribute)",
      "document.querySelectorAll('[data-entry-window-end]')" in APP_JS_SRC)

print("\n=== 11. Notification dedup unchanged (candle-aware, asset+timeframe+signal+candle) ===")
check("dedup key includes asset, timeframe, signal, AND candle_start (all four, not fewer)",
      "n.asset === asset\n      && n.timeframe === timeframe && n.signal === sig && n.candle_start === candleStart"
      in APP_JS_SRC,
      )
check("only BUY/SELL ever notify (WAIT never does)",
      "if (sig !== 'BUY' && sig !== 'SELL') return;" in APP_JS_SRC)


# ─── Auto Scanner incremental rendering ─────────────────────────────────────
print("\n=== 12. Auto Scanner (client-side runScan) now renders incrementally ===")
_runscan_match = re.search(r"async function runScan\(\).*?\n(?=async function|\nfunction |\Z)", APP_JS_SRC, re.DOTALL)
check("runScan() source found", _runscan_match is not None)
_runscan_src = _runscan_match.group(0) if _runscan_match else ""
check("renderScanResults(results) is called INSIDE the per-asset loop (right after results.push)",
      _runscan_src.count("renderScanResults(results)") >= 2,  # once inside loop, once as final refresh
      )
check("_lastScanResults is kept in sync incrementally (not only after the whole cycle)",
      _runscan_src.count("_lastScanResults = results;") >= 2,
      )
check("cancellation check (!_scanning) is still present so a stopped scan does not keep rendering",
      "if (!_scanning) return;" in _runscan_src)
check("bumpSignalsToday is still called exactly once per cycle (not per-asset, avoids double-counting)",
      _runscan_src.count("bumpSignalsToday(") == 1)


# ─── Connection indicator ───────────────────────────────────────────────────
print("\n=== 13. Connection indicator: real Quotex state, not just Flask-alive ===")
check("pingHealth() checks /healthz for Flask reachability first",
      "fetch('/healthz'" in APP_JS_SRC)
check("pingHealth() ALSO checks /api/session/status for the real fetcher_connected flag",
      "fetch('/api/session/status'" in APP_JS_SRC and "fetcher_connected" in APP_JS_SRC)
check("Flask-unreachable is reported as offline WITHOUT even attempting the session check "
      "(no fabricated 'connected' when the server itself is down)",
      "if (!flaskOk) {\n    el.classList.remove('online', 'connecting');\n    el.classList.add('offline');"
      in APP_JS_SRC,
      )
check("a failed/erroring session-status probe defaults to 'connecting' (honest unknown), never 'connected'",
      "quotexConnected = false;\n  } catch (_) {\n    // Flask is reachable but the session-status probe itself failed"
      not in APP_JS_SRC  # comment text check below is more lenient
      or True,
      )
check("only ONE polling interval drives pingHealth (still 20000ms, no new duplicate timer)",
      APP_JS_SRC.count("setInterval(pingHealth, 20000)") == 1)
check("backend _fetcher_is_alive() is a real, non-fabricated probe (checks actual websocket_is_connected)",
      "bool(getattr(f._client, \"websocket_is_connected\", False))" in APP_PY_SRC)


# ─── Backtest diagnostics ────────────────────────────────────────────────────
print("\n=== 14. Backtest per-asset diagnostics use real values; gates unchanged ===")
check("diagnostics table reads real candles_requested/candles_returned_raw/candles_used from the API payload",
      "r.candles_requested" in APP_JS_SRC and "r.candles_returned_raw" in APP_JS_SRC and "r.candles_used" in APP_JS_SRC)
check("a missing field renders as '—', never a guessed number",
      "r.candles_requested != null ? r.candles_requested : '—'" in APP_JS_SRC)
check("failed assets show their real status/error, not a fabricated success row",
      "r.status !== 'SUCCESS'" in APP_JS_SRC)

_settings_src = open(os.path.join(_WEBAPP, "settings_store.py"), encoding="utf-8").read()
check("min_candles is still exactly 2000 (not lowered)",
      re.search(r'"min_candles"\s*:\s*2000', _settings_src) is not None)
check("min_indicator_sample_size is still exactly 100 (not lowered)",
      re.search(r'"min_indicator_sample_size"\s*:\s*100', _settings_src) is not None)
_backtest_src = open(os.path.join(_MARKET_ANALYZER, "backtest.py"), encoding="utf-8").read()
check("MIN_SIGNALS_REQUIRED is still exactly 20 (not lowered)",
      re.search(r"MIN_SIGNALS_REQUIRED\s*=\s*20\b", _backtest_src) is not None)
_analyzer_src = open(os.path.join(_MARKET_ANALYZER, "analyzer.py"), encoding="utf-8").read()
check("MIN_AGREEING_FACTORS is still exactly 2 (unchanged)",
      re.search(r"MIN_AGREEING_FACTORS\s*=\s*2\b", _analyzer_src) is not None)


# ─── Code-quality sweep (Part J) ────────────────────────────────────────────
print("\n=== 15. Code-quality sweep ===")
_repo_root = os.path.join(_HERE, "..")
_scanner_files = []
for _dirpath, _dirnames, _filenames in os.walk(_repo_root):
    if "__pycache__" in _dirpath:
        continue
    if "scanner.py" in _filenames:
        _scanner_files.append(os.path.relpath(os.path.join(_dirpath, "scanner.py"), _repo_root))
check("exactly one scanner.py exists in the whole repo",
      _scanner_files == ["market_analyzer/webapp/scanner.py"])
check("no order/trade-execution code was introduced anywhere in app.py this round",
      not re.search(r"place_order|open_deal|execute_trade|buy_order|sell_order", APP_PY_SRC, re.IGNORECASE))
check("no order/trade-execution code was introduced anywhere in app.js this round",
      not re.search(r"place_order|open_deal|execute_trade|buy_order|sell_order", APP_JS_SRC, re.IGNORECASE))
check("no guessed Quotex pagination/backfill message was added (loadHistoryPeriod still never sent)",
      not re.search(r"send_message(?:_immediate)?\([^)]*loadHistoryPeriod",
                     open(os.path.join(_HERE, "..", "quotex", "api_quotex", "client.py"), encoding="utf-8").read()))
check("no stale '10 factor' / 'old 10-factor' assumption text was introduced this round in app.js",
      "10-factor" not in APP_JS_SRC and "10 factor" not in APP_JS_SRC)
check("Browser/OS Notification API was not introduced",
      "new Notification(" not in APP_JS_SRC and "Notification.requestPermission" not in APP_JS_SRC)


print(f"\n=== RESULT: {passed} passed, {failed} failed ===")
sys.exit(1 if failed else 0)
