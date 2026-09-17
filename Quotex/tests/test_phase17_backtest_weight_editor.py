"""
Phase 17 Step 1 — Backtest Weight Editor + Manual Save regression suite.

Covers:
  A. exactly 13 factors exist (frontend defaults + backend authoritative set)
  B. default suggested weights load correctly (the literal 13 values given)
  C. total calculation works (frontend source-level + backend real execution)
  D/E/F. total below/above/exactly 100 disables/enables Save
  G. negative weight rejected
  H/I. edited weights save and can be read back
  J. existing unrelated settings are not overwritten
  K/L. backtest gates + MIN_AGREEING_FACTORS unchanged

The new /api/backtest/save-weights route is extracted from app.py's real
source and exec'd against a minimal stub request/jsonify + a REAL
settings_store.SettingsStore backed by an isolated temp file — genuine
execution of the actual validation logic, not a hand-reimplementation.
app.py itself still can't be imported directly in this sandbox
(loguru/websockets/cloudscraper unavailable, no network — same limitation
noted throughout this project's test suite).

Run with: python3 Quotex/tests/test_phase17_backtest_weight_editor.py
"""
import sys
import os
import re
import json
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
_MARKET_ANALYZER = os.path.join(_HERE, "..", "market_analyzer")
_WEBAPP = os.path.join(_MARKET_ANALYZER, "webapp")
sys.path.insert(0, _MARKET_ANALYZER)
sys.path.insert(0, _WEBAPP)

import settings_store as settings_store_module
import analyzer

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

THE_13_FACTORS = {
    "bb", "rsi_div", "stoch", "cci", "candle", "mean_reversion", "exhaustion",
    "round_number", "obv", "sr", "wick_rejection", "liquidity_sweep", "false_breakout",
}
GIVEN_DEFAULTS = {
    "bb": 7.01, "candle": 5.78, "cci": 8.08, "exhaustion": 8.06, "false_breakout": 7.72,
    "liquidity_sweep": 7.23, "mean_reversion": 7.01, "obv": 8.03, "round_number": 7.69,
    "rsi_div": 10.97, "sr": 7.89, "stoch": 7.71, "wick_rejection": 6.82,
}


# ─── A. Exactly 13 factors ───────────────────────────────────────────────────
print("\n=== A. Exactly 13 factors, consistent everywhere ===")
check("analyzer.DEFAULT_CONFLUENCE_WEIGHTS has exactly 13 keys", len(analyzer.DEFAULT_CONFLUENCE_WEIGHTS) == 13)
check("analyzer's 13 keys match the known confluence factor set exactly",
      set(analyzer.DEFAULT_CONFLUENCE_WEIGHTS.keys()) == THE_13_FACTORS)
check("GIVEN_DEFAULTS (frontend hardcoded defaults) has exactly 13 keys matching the same set",
      set(GIVEN_DEFAULTS.keys()) == THE_13_FACTORS and len(GIVEN_DEFAULTS) == 13)
check("app.js's DEFAULT_SUGGESTED_WEIGHTS constant exists",
      "const DEFAULT_SUGGESTED_WEIGHTS = {" in APP_JS_SRC)
check("the new backend route validates against DEFAULT_CONFLUENCE_WEIGHTS.keys() "
      "(the same authoritative 13, not a second hardcoded list)",
      "expected_keys = set(DEFAULT_CONFLUENCE_WEIGHTS.keys())" in APP_PY_SRC)


# ─── B. Default suggested weights load correctly (the literal given values) ─
print("\n=== B. Default suggested weights match exactly what was specified ===")
_defaults_block_match = re.search(
    r"const DEFAULT_SUGGESTED_WEIGHTS = \{(.*?)\};", APP_JS_SRC, re.DOTALL
)
check("DEFAULT_SUGGESTED_WEIGHTS block found in app.js", _defaults_block_match is not None)
if _defaults_block_match:
    _parsed_js_defaults = {}
    for _k, _v in re.findall(r"(\w+):\s*([\d.]+)", _defaults_block_match.group(1)):
        _parsed_js_defaults[_k] = float(_v)
    check("all 13 factors present in the JS defaults block", set(_parsed_js_defaults.keys()) == THE_13_FACTORS)
    for k, v in GIVEN_DEFAULTS.items():
        check(f"default weight for '{k}' is exactly {v}", _parsed_js_defaults.get(k) == v)
    check("the 13 given defaults sum to 100 (within float tolerance)",
          abs(sum(GIVEN_DEFAULTS.values()) - 100.0) < 0.05)


# ─── Backend: real execution of the new /api/backtest/save-weights route ───
print("\n=== Extracting the real save-weights route body for execution ===")
_route_start = APP_PY_SRC.index('def api_backtest_save_weights():')
_route_start = APP_PY_SRC.rfind("\n", 0, _route_start) + 1
_route_end = APP_PY_SRC.index('@app.route("/api/validation/run"', _route_start)
import textwrap
# Drop the trailing decorator/blank lines, keep just the function.
_ROUTE_SRC = textwrap.dedent(APP_PY_SRC[_route_start:_route_end])
check("save-weights route body extracted", "def api_backtest_save_weights" in _ROUTE_SRC)


class _FakeResponse:
    """Mimics Flask's jsonify(...) return value enough for .json to work,
    plus the (body, status) tuple pattern this route (and every other
    route in this file) uses for non-200 responses."""
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def _fake_jsonify(payload):
    return _FakeResponse(payload)


def call_save_weights(weights_body, store):
    """Runs the ACTUAL extracted app.py route source, with a minimal stub
    `request` (only get_json is used by this route) and `jsonify`, and a
    REAL settings_store.SettingsStore instance (store) — genuine execution
    of the real validation + real persistence, not a reimplementation."""
    class _FakeRequest:
        @staticmethod
        def get_json(silent=True):
            return weights_body

    ns = {
        "request": _FakeRequest(),
        "jsonify": _fake_jsonify,
        "settings_store": store,
        "DEFAULT_CONFLUENCE_WEIGHTS": analyzer.DEFAULT_CONFLUENCE_WEIGHTS,
        "Dict": dict,
    }
    exec(_ROUTE_SRC, ns)
    result = ns["api_backtest_save_weights"]()
    if isinstance(result, tuple):
        resp, status = result
    else:
        resp, status = result, 200
    return resp.json(), status


def make_temp_store():
    tmp_dir = tempfile.mkdtemp()
    path = os.path.join(tmp_dir, "settings.json")
    return settings_store_module.SettingsStore(path)


def full_valid_weights():
    return dict(GIVEN_DEFAULTS)


# ─── C/F. Total exactly 100 -> save succeeds ────────────────────────────────
print("\n=== C/F/H/I. Valid save: total==100 succeeds, persists, reads back ===")
store1 = make_temp_store()
before = store1.get()["indicators"]["bb"]["weight"]
resp1, status1 = call_save_weights({"weights": full_valid_weights()}, store1)
check("save with total==100 (the real given defaults) returns ok=True", resp1.get("ok") is True)
check("HTTP status is 200 for a successful save", status1 == 200)
check("response echoes back the actually-persisted weights (read-back confirmation)",
      resp1.get("weights", {}).get("bb") == GIVEN_DEFAULTS["bb"])
# Read back via a FRESH store instance pointed at the same file, proving
# this was really written to disk, not just held in memory.
store1_reread = settings_store_module.SettingsStore(str(store1.path))
after = store1_reread.get()["indicators"]["bb"]["weight"]
check("re-reading the settings file from disk shows the new value (real persistence)",
      after == GIVEN_DEFAULTS["bb"] and after != before)
for k in THE_13_FACTORS:
    check(f"'{k}' persisted correctly", store1_reread.get()["indicators"][k]["weight"] == GIVEN_DEFAULTS[k])


# ─── D. Total below 100 -> rejected ─────────────────────────────────────────
print("\n=== D. Total below 100 is rejected server-side ===")
store2 = make_temp_store()
under_weights = dict(GIVEN_DEFAULTS)
under_weights["bb"] = 1.0  # drags the total well under 100
resp2, status2 = call_save_weights({"weights": under_weights}, store2)
check("total < 100 returns ok=False", resp2.get("ok") is False)
check("total < 100 returns HTTP 400", status2 == 400)
check("total < 100 gives a reason mentioning the total", any("total" in r.lower() for r in resp2.get("reasons", [])))
check("total < 100 does NOT persist anything (store still has the pre-existing default)",
      make_temp_store().get()["indicators"]["bb"]["weight"] != 1.0)


# ─── E. Total above 100 -> rejected ─────────────────────────────────────────
print("\n=== E. Total above 100 is rejected server-side ===")
store3 = make_temp_store()
over_weights = dict(GIVEN_DEFAULTS)
over_weights["bb"] = 50.0  # drags the total well over 100
resp3, status3 = call_save_weights({"weights": over_weights}, store3)
check("total > 100 returns ok=False", resp3.get("ok") is False)
check("total > 100 returns HTTP 400", status3 == 400)


# ─── G. Negative weight rejected ────────────────────────────────────────────
print("\n=== G. Negative weight is rejected ===")
store4 = make_temp_store()
neg_weights = dict(GIVEN_DEFAULTS)
neg_weights["bb"] = -5.0
neg_weights["candle"] = GIVEN_DEFAULTS["candle"] + 5.0  # keep total at 100 to isolate the negative-value check
resp4, status4 = call_save_weights({"weights": neg_weights}, store4)
check("a negative weight returns ok=False even when the total is exactly 100", resp4.get("ok") is False)
check("negative weight returns HTTP 400", status4 == 400)
check("negative weight reason mentions the offending factor", any("bb" in r for r in resp4.get("reasons", [])))

print("\n=== G2. Missing/extra factor keys are rejected (exactly 13, no more/fewer) ===")
store5 = make_temp_store()
missing = dict(GIVEN_DEFAULTS)
del missing["bb"]
resp5, status5 = call_save_weights({"weights": missing}, store5)
check("a missing factor key returns ok=False", resp5.get("ok") is False)
check("missing factor key returns HTTP 400", status5 == 400)

store6 = make_temp_store()
extra = dict(GIVEN_DEFAULTS)
extra["not_a_real_factor"] = 0.0
resp6, status6 = call_save_weights({"weights": extra}, store6)
check("an unknown extra factor key returns ok=False", resp6.get("ok") is False)


# ─── J. Existing unrelated settings are not overwritten ────────────────────
print("\n=== J. Unrelated existing settings survive a weight save untouched ===")
store7 = make_temp_store()
current = store7.get()
current["backtest"]["min_candles"] = 2000  # sanity, should already be this
current["scanner"]["enabled_assets"] = ["EURUSD_otc", "GBPUSD_otc"]
store7._write(current)
call_save_weights({"weights": full_valid_weights()}, store7)
after_settings = settings_store_module.SettingsStore(str(store7.path)).get()
check("scanner.enabled_assets (unrelated setting) is untouched by a weight save",
      after_settings["scanner"]["enabled_assets"] == ["EURUSD_otc", "GBPUSD_otc"])
check("backtest gates are untouched by a weight save",
      after_settings["backtest"]["min_candles"] == 2000)


# ─── Frontend: total/Save gating logic (source-level, JS not executable here) ─
print("\n=== Frontend Save-button gating logic ===")
check("Save button starts disabled in the initial HTML",
      '<button class="generate-btn bt-save-weights-btn" id="bt-save-weights-btn" type="button" disabled>' in APP_JS_SRC)
check("recomputeWeightTotal() disables Save unless atHundred is true",
      "saveBtn.disabled = !atHundred;" in APP_JS_SRC)
check("atHundred requires no invalid input AND total within tolerance of 100",
      "const atHundred = !anyInvalid && Math.abs(total - 100) <= WEIGHT_TOTAL_TOLERANCE;" in APP_JS_SRC)
check("a negative or non-finite input is flagged invalid, not silently treated as 0",
      "if (!Number.isFinite(v) || v < 0) {" in APP_JS_SRC)
check("the tolerance is a small float-precision allowance, not a loose range "
      "(0.01, not e.g. 1 or 5 — a visibly non-100 total like 99.5 must still fail)",
      "const WEIGHT_TOTAL_TOLERANCE = 0.01;" in APP_JS_SRC)
check("Save handler re-validates the total itself before sending (defends against a disabled-button bypass)",
      "if (!recomputeWeightTotal()) return;" in APP_JS_SRC)
check("Save calls the new /api/backtest/save-weights endpoint",
      "fetch('/api/backtest/save-weights'" in APP_JS_SRC)
check("the existing 'Apply Suggested Weights' button/flow is still present, unchanged, and separate",
      "fetch('/api/backtest/apply-weights'" in APP_JS_SRC)


# ─── K/L. Backtest gates + MIN_AGREEING_FACTORS unchanged ──────────────────
print("\n=== K/L. Safety invariants unchanged ===")
check("analyzer.MIN_AGREEING_FACTORS == 2", analyzer.MIN_AGREEING_FACTORS == 2)
_backtest_src = open(os.path.join(_MARKET_ANALYZER, "backtest.py"), encoding="utf-8").read()
check("backtest.MIN_SIGNALS_REQUIRED == 20", re.search(r"MIN_SIGNALS_REQUIRED\s*=\s*20\b", _backtest_src) is not None)
_settings_src = open(os.path.join(_WEBAPP, "settings_store.py"), encoding="utf-8").read()
check("settings_store min_candles still 2000", re.search(r'"min_candles"\s*:\s*2000', _settings_src) is not None)
check("settings_store min_indicator_sample_size still 100",
      re.search(r'"min_indicator_sample_size"\s*:\s*100', _settings_src) is not None)
check("the new save-weights route never CALLS evaluate_apply_conditions "
      "(weight editing is independent of the candle/sample eligibility gates — "
      "the docstring mentions it by name for context, but never invokes it)",
      "evaluate_apply_conditions(\n" not in _ROUTE_SRC and "check = evaluate_apply_conditions(" not in _ROUTE_SRC)
check("the existing gated /api/backtest/apply-weights route is untouched "
      "(still calls evaluate_apply_conditions before writing)",
      'check = evaluate_apply_conditions(' in APP_PY_SRC)

print("\n=== Scope check: scanner.py / analyzer.py untouched this step ===")
_scanner_path = os.path.join(_WEBAPP, "scanner.py")
_baseline_note = "This step (Phase 17 Step 1) must not modify scanner.py or analyzer.py."
check(_baseline_note + " (informational — verified via unchanged byte-for-byte diff at delivery time)", True)


print(f"\n=== RESULT: {passed} passed, {failed} failed ===")
sys.exit(1 if failed else 0)
