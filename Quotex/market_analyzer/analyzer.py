"""
Market signal analyzer for Quotex.
Reads indicator results, produces BUY / SELL / WAIT signal with confidence score.
NEVER places orders — analysis only.
"""

from __future__ import annotations
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd

try:
    from config import RSI_OVERSOLD, RSI_OVERBOUGHT, ADX_TRENDING, STOCH_RSI_LO, STOCH_RSI_HI, MIN_PAYOUT
except ImportError:
    from market_analyzer.config import RSI_OVERSOLD, RSI_OVERBOUGHT, ADX_TRENDING, STOCH_RSI_LO, STOCH_RSI_HI, MIN_PAYOUT


# ─── Signal scoring ───────────────────────────────────────────────────────────

def _score_indicators(ind: Dict[str, Any]) -> Tuple[int, int, List[str]]:
    """
    Score each indicator for bullish (+1) or bearish (−1).
    Returns (bullish_count, bearish_count, reasoning_lines).
    """
    bullish = 0
    bearish = 0
    reasons: List[str] = []

    price = ind.get("price", 0)

    # ── RSI ──────────────────────────────────────────────────────────────────
    rsi_val = ind.get("rsi", 50)
    if rsi_val < RSI_OVERSOLD:
        bullish += 1
        reasons.append(f"  RSI {rsi_val:.1f} → oversold (bullish)")
    elif rsi_val > RSI_OVERBOUGHT:
        bearish += 1
        reasons.append(f"  RSI {rsi_val:.1f} → overbought (bearish)")
    else:
        reasons.append(f"  RSI {rsi_val:.1f} → neutral")

    # ── MACD ─────────────────────────────────────────────────────────────────
    hist = ind.get("macd_histogram", 0)
    macd_line  = ind.get("macd", 0)
    macd_signal= ind.get("macd_signal", 0)
    if hist > 0 and macd_line > macd_signal:
        bullish += 1
        reasons.append(f"  MACD histogram +{hist:.5f} above signal (bullish)")
    elif hist < 0 and macd_line < macd_signal:
        bearish += 1
        reasons.append(f"  MACD histogram {hist:.5f} below signal (bearish)")
    else:
        reasons.append(f"  MACD histogram {hist:.5f} → neutral / crossing")

    # ── EMA crossovers ───────────────────────────────────────────────────────
    e9  = ind.get("ema_9",  price)
    e21 = ind.get("ema_21", price)
    e50 = ind.get("ema_50", price)
    if e9 > e21 and price > e50:
        bullish += 1
        reasons.append(f"  EMA9 > EMA21 & price > EMA50 (bullish)")
    elif e9 < e21 and price < e50:
        bearish += 1
        reasons.append(f"  EMA9 < EMA21 & price < EMA50 (bearish)")
    else:
        reasons.append(f"  EMA alignment: mixed")

    # ── Bollinger Bands ───────────────────────────────────────────────────────
    bb_lower = ind.get("bb_lower", 0)
    bb_upper = ind.get("bb_upper", 0)
    bb_mid   = ind.get("bb_middle", 0)
    if price < bb_lower:
        bullish += 1
        reasons.append(f"  Price below BB lower → potential bounce (bullish)")
    elif price > bb_upper:
        bearish += 1
        reasons.append(f"  Price above BB upper → potential reversal (bearish)")
    elif price < bb_mid:
        bearish += 1
        reasons.append(f"  Price below BB middle band (mild bearish)")
    else:
        bullish += 1
        reasons.append(f"  Price above BB middle band (mild bullish)")

    # ── Stochastic RSI ────────────────────────────────────────────────────────
    stoch_k = ind.get("stoch_rsi_k", 50)
    stoch_d = ind.get("stoch_rsi_d", 50)
    if stoch_k < STOCH_RSI_LO and stoch_k > stoch_d:
        bullish += 1
        reasons.append(f"  Stoch RSI K {stoch_k:.1f} oversold, K>D (bullish)")
    elif stoch_k > STOCH_RSI_HI and stoch_k < stoch_d:
        bearish += 1
        reasons.append(f"  Stoch RSI K {stoch_k:.1f} overbought, K<D (bearish)")
    else:
        reasons.append(f"  Stoch RSI K {stoch_k:.1f} → neutral")

    # ── ADX / trend strength ──────────────────────────────────────────────────
    adx_val  = ind.get("adx", 0)
    di_plus  = ind.get("di_plus",  0)
    di_minus = ind.get("di_minus", 0)
    trend_dir = ind.get("direction", "SIDEWAYS")
    if adx_val >= ADX_TRENDING:
        if di_plus > di_minus:
            bullish += 1
            reasons.append(f"  ADX {adx_val:.1f} trending, +DI > -DI (bullish momentum)")
        else:
            bearish += 1
            reasons.append(f"  ADX {adx_val:.1f} trending, -DI > +DI (bearish momentum)")
    else:
        reasons.append(f"  ADX {adx_val:.1f} → weak trend / ranging (reduce weight)")

    # ── VWAP ─────────────────────────────────────────────────────────────────
    vwap_val = ind.get("vwap", price)
    if price > vwap_val:
        bullish += 1
        reasons.append(f"  Price > VWAP {vwap_val:.5f} (bullish)")
    else:
        bearish += 1
        reasons.append(f"  Price < VWAP {vwap_val:.5f} (bearish)")

    # ── Overall EMA trend ─────────────────────────────────────────────────────
    if trend_dir == "BULLISH":
        bullish += 1
        reasons.append(f"  EMA trend: BULLISH")
    elif trend_dir == "BEARISH":
        bearish += 1
        reasons.append(f"  EMA trend: BEARISH")
    else:
        reasons.append(f"  EMA trend: SIDEWAYS / mixed")

    return bullish, bearish, reasons


def generate_signal(ind: Dict[str, Any]) -> Dict[str, Any]:
    """
    Aggregate indicator scores into a BUY / SELL / WAIT signal.
    Returns dict with: signal, confidence (0–100), breakdown.
    """
    bullish, bearish, reasons = _score_indicators(ind)
    total = bullish + bearish
    if total == 0:
        return {"signal": "WAIT", "confidence": 0, "reasons": reasons,
                "bullish_count": 0, "bearish_count": 0}

    if bullish > bearish:
        dominant = bullish
        signal = "BUY"
    elif bearish > bullish:
        dominant = bearish
        signal = "SELL"
    else:
        dominant = bullish
        signal = "WAIT"

    confidence = round((dominant / total) * 100)

    # Boost/dampen by ADX strength
    adx_val = ind.get("adx", 0)
    vol_level = ind.get("level", "MEDIUM")

    if adx_val < ADX_TRENDING:
        confidence = max(0, confidence - 10)   # weak trend → less confident
    if vol_level == "HIGH":
        confidence = max(0, confidence - 5)    # high volatility → riskier

    if signal == "WAIT":
        confidence = max(0, confidence - 15)

    return {
        "signal":        signal,
        "confidence":    min(100, confidence),
        "bullish_count": bullish,
        "bearish_count": bearish,
        "reasons":       reasons,
    }


# ─── Confluence signal engine ─────────────────────────────────────────────────

DEFAULT_CONFLUENCE_WEIGHTS: Dict[str, float] = {
    # Phase 8.6: rebalanced from 10 factors x 10.0 to 13 factors x ~7.6923
    # (100/13, non-exact) to connect the three Phase 7.3 Part 3 indicators
    # (Wick Rejection, Liquidity Sweep, False Breakout) to the confluence
    # vote, keeping every factor's default share equal, same precedent as
    # Phase 6's rebalance when SR was added. 100/13 doesn't divide evenly at
    # 2 decimals (12 x 7.69 = 92.28), so the rounding residual (0.04) is
    # assigned to one factor (false_breakout, last one added) so the sum is
    # exactly 100.0 — same "assign residual to a single weight" convention
    # already used in settings_store.get_effective_dynamic_weights(). Dynamic
    # (backtest-derived) weights, when available, still override these
    # defaults per-factor based on real accuracy — this only changes what
    # "no backtest data yet" falls back to.
    "bb":               7.69,
    "rsi_div":          7.69,
    "stoch":            7.69,
    "cci":              7.69,
    "candle":           7.69,
    "mean_reversion":   7.69,
    "exhaustion":       7.69,
    "round_number":     7.69,
    "obv":              7.69,
    "sr":               7.69,
    "wick_rejection":   7.69,
    "liquidity_sweep":  7.69,
    "false_breakout":   7.72,
}
assert abs(sum(DEFAULT_CONFLUENCE_WEIGHTS.values()) - 100.0) < 1e-9  # 12*7.69 + 7.72 = 100.00
assert len(DEFAULT_CONFLUENCE_WEIGHTS) == 13

# Phase 5: minimum geometry-derived reliability_score (0-100) required before
# a candlestick_pattern_detail's direction is allowed to cast a non-zero
# confluence vote. Prevents weak/marginal pattern shapes from flipping the
# candle factor's vote — consistent with Step 1's "do not create false
# signals" principle. Not yet backtest-calibrated (see Phase 5.1 candidate).
CANDLE_RELIABILITY_VOTE_THRESHOLD = 40.0

# Phase 6: minimum zone_reliability (0-100, geometry/frequency-derived — see
# indicators.detect_support_resistance_zones) required before the SR factor's
# proximity-based direction is allowed to cast a non-zero confluence vote.
# Configurable (module-level, easy to tune/expose later) — same "don't create
# false signals from weak evidence" principle as CANDLE_RELIABILITY_VOTE_THRESHOLD.
SR_RELIABILITY_VOTE_THRESHOLD = 40.0

# Phase 8.6: minimum reliability_score (0-100, geometry-derived — see
# indicators.detect_wick_rejection/detect_liquidity_sweep/
# detect_false_breakout) required before each of the 3 newly-connected
# factors' direction is allowed to cast a non-zero confluence vote. Same
# "don't create false signals from weak evidence" principle and same
# threshold value as CANDLE_RELIABILITY_VOTE_THRESHOLD/
# SR_RELIABILITY_VOTE_THRESHOLD above — not yet backtest-calibrated
# separately, consistent with those two.
WICK_REJECTION_VOTE_THRESHOLD = 40.0
LIQUIDITY_SWEEP_VOTE_THRESHOLD = 40.0
FALSE_BREAKOUT_VOTE_THRESHOLD = 40.0



def load_precomputed_weights(backtest_json_path=None):
    """
    Task 5: Load averaged factor weights produced by deep_backtest.py.
    Returns a weights dict if backtest_results.json exists and is valid,
    otherwise returns None (caller falls back to per-run weights).
    Never raises — all errors are silently swallowed.
    """
    import json as _json
    from pathlib import Path as _Path
    try:
        if backtest_json_path is None:
            backtest_json_path = _Path(__file__).resolve().parent / "backtest_results.json"
        data = _json.loads(_Path(backtest_json_path).read_text())
        avg = data.get("average_weights")
        if avg and isinstance(avg, dict) and sum(avg.values()) > 0:
            return avg
    except Exception:
        pass
    return None


def _confluence_factor_votes(df: pd.DataFrame, ind: Dict[str, Any]) -> Dict[str, int]:
    """
    Score the 5 confluence factors for the *latest* bar only (+1 bullish,
    -1 bearish, 0 neutral). Mirrors backtest._factor_votes()'s per-bar logic
    exactly, so live signals and backtested accuracies are measuring the
    same thing.
    """
    votes: Dict[str, int] = {}

    # ── BB Bounce ────────────────────────────────────────────────────────────
    price = ind.get("price", 0)
    bb_lower = ind.get("bb_lower", 0)
    bb_upper = ind.get("bb_upper", 0)
    if price <= bb_lower:
        votes["bb"] = 1
    elif price >= bb_upper:
        votes["bb"] = -1
    else:
        votes["bb"] = 0

    # ── RSI Divergence (last 5 candles: price direction vs RSI direction) ────
    rsi_series = ind.get("_rsi_series")
    close = df["close"]
    if rsi_series is not None and len(close) > 5 and len(rsi_series) > 5:
        price_change_5 = close.iloc[-1] - close.iloc[-6]
        rsi_change_5 = rsi_series.iloc[-1] - rsi_series.iloc[-6]
        if price_change_5 < 0 and rsi_change_5 > 0:
            votes["rsi_div"] = 1
        elif price_change_5 > 0 and rsi_change_5 < 0:
            votes["rsi_div"] = -1
        else:
            votes["rsi_div"] = 0
    else:
        votes["rsi_div"] = 0

    # ── Stochastic Cross (classic price-based oscillator) ────────────────────
    k_series = ind.get("_stoch_k_series")
    d_series = ind.get("_stoch_d_series")
    if k_series is not None and d_series is not None and len(k_series) > 1:
        k, d = k_series.iloc[-1], d_series.iloc[-1]
        k_prev, d_prev = k_series.iloc[-2], d_series.iloc[-2]
        cross_up = (k_prev <= d_prev) and (k > d)
        cross_down = (k_prev >= d_prev) and (k < d)
        if k < 20 and d < 20 and cross_up:
            votes["stoch"] = 1
        elif k > 80 and d > 80 and cross_down:
            votes["stoch"] = -1
        else:
            votes["stoch"] = 0
    else:
        votes["stoch"] = 0

    # ── CCI Extreme + Reversal ────────────────────────────────────────────────
    cci_series = ind.get("_cci_series")
    cci_val = ind.get("cci", 0)
    if cci_series is not None and len(cci_series) > 1:
        cci_rising = cci_series.iloc[-1] > cci_series.iloc[-2]
        cci_falling = cci_series.iloc[-1] < cci_series.iloc[-2]
        if cci_val < -100 and cci_rising:
            votes["cci"] = 1
        elif cci_val > 100 and cci_falling:
            votes["cci"] = -1
        else:
            votes["cci"] = 0
    else:
        votes["cci"] = 0

    # ── Candlestick pattern ────────────────────────────────────────────────────
    # Phase 5: prefer the richer candlestick_pattern_detail structure
    # (direction + geometry-derived reliability_score) when present. Only
    # falls back to the legacy plain-name check if that key is entirely
    # absent — i.e. an older/incomplete indicators dict — preserving full
    # backward compatibility for any caller still supplying only a name.
    detail = ind.get("candlestick_pattern_detail")
    if detail is not None:
        direction = detail.get("direction", "NEUTRAL")
        reliability = detail.get("reliability_score", 0)
        if direction == "BUY" and reliability >= CANDLE_RELIABILITY_VOTE_THRESHOLD:
            votes["candle"] = 1
        elif direction == "SELL" and reliability >= CANDLE_RELIABILITY_VOTE_THRESHOLD:
            votes["candle"] = -1
        else:
            votes["candle"] = 0
    else:
        pattern = ind.get("candlestick_pattern")
        if pattern in ("hammer", "bullish_engulfing"):
            votes["candle"] = 1
        elif pattern == "bearish_engulfing":
            votes["candle"] = -1
        else:
            votes["candle"] = 0

    # ── Mean Reversion (factor 6) ─────────────────────────────────────────────
    mr_strength = ind.get("mr_strength", 0.0)
    mr_upper    = ind.get("mr_upper", False)
    mr_lower    = ind.get("mr_lower", False)
    if mr_upper and mr_strength >= 2.0:
        votes["mean_reversion"] = -1    # price above upper BB → bearish reversal
    elif mr_lower and mr_strength <= -2.0:
        votes["mean_reversion"] = 1     # price below lower BB → bullish reversal
    else:
        votes["mean_reversion"] = 0

    # ── Consecutive Candle Exhaustion (factor 7) ──────────────────────────────
    exh_exhaustion = ind.get("exh_exhaustion", False)
    exh_direction  = ind.get("exh_direction")
    if exh_exhaustion and exh_direction == "bullish":
        votes["exhaustion"] = -1        # 5+ bull candles → bearish reversal
    elif exh_exhaustion and exh_direction == "bearish":
        votes["exhaustion"] = 1         # 5+ bear candles → bullish reversal
    else:
        votes["exhaustion"] = 0

    # ── Round Number Proximity (factor 8) ─────────────────────────────────────
    # Purely a discretion/confluence zone marker — vote stays neutral.
    votes["round_number"] = 0

    # ── OBV Divergence (factor 9, Step 4) ─────────────────────────────────────
    # Same 5-bar comparison pattern as RSI Divergence above: price vs. OBV
    # moving in opposite directions signals accumulation/distribution that
    # isn't yet reflected in price.
    obv_series = ind.get("_obv_series")
    if obv_series is not None and len(close) > 5 and len(obv_series) > 5:
        price_change_5 = close.iloc[-1] - close.iloc[-6]
        obv_change_5 = obv_series.iloc[-1] - obv_series.iloc[-6]
        if price_change_5 < 0 and obv_change_5 > 0:
            votes["obv"] = 1     # price down, OBV up -> bullish divergence
        elif price_change_5 > 0 and obv_change_5 < 0:
            votes["obv"] = -1    # price up, OBV down -> bearish divergence
        else:
            votes["obv"] = 0
    else:
        votes["obv"] = 0

    # ── Support/Resistance Zone (factor 10, Phase 6) ──────────────────────────
    # BUY when price is safely positioned near a strong support zone; SELL
    # when near a strong resistance zone; otherwise neutral. Gated by
    # zone_reliability (geometry/frequency-derived, see
    # indicators.detect_support_resistance_zones) so weak/thin zones can't
    # flip the vote — same principle as the candlestick reliability gate.
    sr_detail = ind.get("support_resistance_detail")
    if sr_detail is not None:
        reliability = sr_detail.get("zone_reliability", 0) or 0
        safe_entry = sr_detail.get("safe_entry", True)
        support_dist_atr = sr_detail.get("support_distance_atr")
        resistance_dist_atr = sr_detail.get("resistance_distance_atr")
        near_support = support_dist_atr is not None and support_dist_atr < 1.0
        near_resistance = resistance_dist_atr is not None and resistance_dist_atr < 1.0
        if (near_support and safe_entry and reliability >= SR_RELIABILITY_VOTE_THRESHOLD
                and not near_resistance):
            votes["sr"] = 1
        elif (near_resistance and safe_entry and reliability >= SR_RELIABILITY_VOTE_THRESHOLD
                and not near_support):
            votes["sr"] = -1
        else:
            votes["sr"] = 0
    else:
        votes["sr"] = 0

    # ── Wick Rejection (factor 11, Phase 8.6) ─────────────────────────────────
    # Single-candle long-wick rejection (indicators.detect_wick_rejection).
    # BUY when the rejection wick is on the downside (buyers defended),
    # SELL when it's on the upside — gated by reliability_score, same
    # pattern as the candle/sr factors above.
    wr_detail = ind.get("wick_rejection_detail")
    if wr_detail is not None:
        direction = wr_detail.get("direction")
        reliability = wr_detail.get("reliability_score", 0) or 0
        if direction == "BUY" and reliability >= WICK_REJECTION_VOTE_THRESHOLD:
            votes["wick_rejection"] = 1
        elif direction == "SELL" and reliability >= WICK_REJECTION_VOTE_THRESHOLD:
            votes["wick_rejection"] = -1
        else:
            votes["wick_rejection"] = 0
    else:
        votes["wick_rejection"] = 0

    # ── Liquidity Sweep (factor 12, Phase 8.6) ────────────────────────────────
    # Stop-hunt reversal past a recent swing extreme
    # (indicators.detect_liquidity_sweep). BUY on a sell-side sweep (swept
    # low, closed back above it), SELL on a buy-side sweep — gated by
    # reliability_score, same pattern as above.
    ls_detail = ind.get("liquidity_sweep_detail")
    if ls_detail is not None:
        direction = ls_detail.get("direction")
        reliability = ls_detail.get("reliability_score", 0) or 0
        if direction == "BUY" and reliability >= LIQUIDITY_SWEEP_VOTE_THRESHOLD:
            votes["liquidity_sweep"] = 1
        elif direction == "SELL" and reliability >= LIQUIDITY_SWEEP_VOTE_THRESHOLD:
            votes["liquidity_sweep"] = -1
        else:
            votes["liquidity_sweep"] = 0
    else:
        votes["liquidity_sweep"] = 0

    # ── False Breakout (factor 13, Phase 8.6) ─────────────────────────────────
    # False break of a Phase 6 S/R zone (indicators.detect_false_breakout).
    # BUY on a false support break (dipped below support, closed back
    # above), SELL on a false resistance break — gated by reliability_score,
    # same pattern as above.
    fb_detail = ind.get("false_breakout_detail")
    if fb_detail is not None:
        direction = fb_detail.get("direction")
        reliability = fb_detail.get("reliability_score", 0) or 0
        if direction == "BUY" and reliability >= FALSE_BREAKOUT_VOTE_THRESHOLD:
            votes["false_breakout"] = 1
        elif direction == "SELL" and reliability >= FALSE_BREAKOUT_VOTE_THRESHOLD:
            votes["false_breakout"] = -1
        else:
            votes["false_breakout"] = 0
    else:
        votes["false_breakout"] = 0

    return votes


# ─── Market condition dampener (Step 1 — safety/reliability) ─────────────────
# Reduces confluence confidence when the market is weakly trending (low ADX)
# or volatility is elevated/extreme. This NEVER changes which side won the
# weighted vote and NEVER increases confidence — it only scales the reported
# confidence down to reflect lower-quality market conditions. The existing
# backtest-derived dynamic weights (bullish_weight / bearish_weight / the
# >=3-agreeing-factors gate) are completely untouched by this step.
ADX_WEAK_DAMPEN            = 0.85   # confidence x= this when ADX < ADX_TRENDING
VOLATILITY_HIGH_DAMPEN     = 0.90   # confidence x= this when volatility level == HIGH
VOLATILITY_EXTREME_ATR_PCT = 1.5    # ATR% above this counts as "extreme" volatility
VOLATILITY_EXTREME_DAMPEN  = 0.80   # confidence x= this when volatility is extreme

# Minimum number of independently-agreeing (non-zero) confluence factors
# required before a BUY/SELL is committed to; below this, generate_confluence_signal()
# returns WAIT. Single source of truth — every caller (Manual Analyzer, Scanner,
# Backtest) goes through generate_confluence_signal() below, so changing this
# constant changes the requirement everywhere consistently.
MIN_AGREEING_FACTORS = 2


def _apply_market_condition_dampener(confidence: float, ind: Dict[str, Any]) -> Tuple[float, List[str]]:
    """
    Multiplicatively dampens an already-computed confidence score based on
    ADX trend strength and ATR-based volatility. Returns (new_confidence,
    notes) where notes explains which dampeners fired (for transparency in
    the API response / UI, not required for the calculation itself).
    """
    adjusted = float(confidence)
    notes: List[str] = []

    adx_val = ind.get("adx", 0) or 0
    if adx_val < ADX_TRENDING:
        adjusted *= ADX_WEAK_DAMPEN
        notes.append(f"weak trend (ADX {adx_val:.1f} < {ADX_TRENDING}) -> x{ADX_WEAK_DAMPEN}")

    atr_pct = ind.get("atr_pct", 0) or 0
    vol_level = ind.get("level", "MEDIUM")
    if atr_pct > VOLATILITY_EXTREME_ATR_PCT:
        adjusted *= VOLATILITY_EXTREME_DAMPEN
        notes.append(f"extreme volatility (ATR% {atr_pct:.2f} > {VOLATILITY_EXTREME_ATR_PCT}) -> x{VOLATILITY_EXTREME_DAMPEN}")
    elif vol_level == "HIGH":
        adjusted *= VOLATILITY_HIGH_DAMPEN
        notes.append(f"high volatility (level=HIGH) -> x{VOLATILITY_HIGH_DAMPEN}")

    return round(max(0.0, adjusted), 1), notes


def generate_confluence_signal(df: pd.DataFrame, indicators_result: Dict[str, Any],
                               dynamic_weights: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
    """
    Multi-factor confluence signal: votes from independent factors (BB Bounce,
    RSI Divergence, Stochastic Cross, CCI Extreme+Reversal, Candlestick
    pattern, Mean Reversion, Consecutive Candle Exhaustion, Round Number
    proximity, OBV Divergence, Support/Resistance proximity, Wick Rejection,
    Liquidity Sweep, False Breakout), combined via weighted voting.

    Requires >= MIN_AGREEING_FACTORS agreeing (non-zero) factors AND a
    non-tied weighted result before committing to BUY/SELL; otherwise
    returns WAIT. Analysis only — never places an order.

    Step 1: after the signal/confidence is determined from the vote, a
    market-condition dampener (weak ADX / elevated volatility) reduces the
    reported confidence. It cannot flip WAIT into a signal or change which
    side won, and it never raises confidence above the raw voted value.
    """
    weights = dict(dynamic_weights) if dynamic_weights else dict(DEFAULT_CONFLUENCE_WEIGHTS)
    votes = _confluence_factor_votes(df, indicators_result)

    bullish_weight = sum(weights.get(name, 0) for name, v in votes.items() if v == 1)
    bearish_weight = sum(weights.get(name, 0) for name, v in votes.items() if v == -1)
    agreeing_factors = sum(1 for v in votes.values() if v != 0)

    if agreeing_factors >= MIN_AGREEING_FACTORS and bullish_weight != bearish_weight:
        signal = "BUY" if bullish_weight > bearish_weight else "SELL"
        confidence_raw = round(max(bullish_weight, bearish_weight), 1)
    else:
        signal = "WAIT"
        confidence_raw = 0

    if signal == "WAIT":
        # Nothing to dampen — WAIT is already confidence 0, and dampening
        # must never be able to turn a WAIT into a signal.
        confidence = confidence_raw
        market_condition_notes: List[str] = []
    else:
        confidence, market_condition_notes = _apply_market_condition_dampener(
            confidence_raw, indicators_result
        )

    return {
        "signal": signal,
        "confidence": confidence,
        "confidence_raw": confidence_raw,           # new, additive: pre-dampener value
        "market_condition": market_condition_notes,  # new, additive: why confidence was reduced (if at all)
        "factors": votes,
        "weights_used": weights,
    }


# ─── Filter Score system (Phase 7, graded in Phase 7.1) ────────────────────────
# Separate from, and does NOT modify, the confluence engine above or its
# DEFAULT_CONFLUENCE_WEIGHTS. This is a distinct signal-QUALITY score — "how
# good is this already-computed confluence signal" — layered on top, never a
# replacement for or generator of BUY/SELL itself.
# This is the SINGLE SOURCE OF TRUTH for hard-gate logic: scanner.py calls
# this function instead of maintaining its own duplicate gate checks.
#
# Phase 7.1 change: filter_score is now a GRADED 0-100 quality metric that is
# ALWAYS computed via the rubric below — it no longer collapses to 0 just
# because a mandatory gate failed (Phase 7.0's all-or-nothing design made the
# score too binary — almost every result landed at either 0 or 93-100,
# leaving the 20-80 range almost never used). The original binary hard-gate
# pass/fail semantics are preserved separately in the new `mandatory_pass`
# field (and in passed_filters/failed_filters, unchanged in meaning) — the
# scanner still hides a signal when mandatory_pass is False, but a hidden
# signal now still gets a meaningful, informative score for ranking/analysis
# rather than a flat, uninformative 0.

FILTER_SCORE_WEIGHTS: Dict[str, float] = {
    "ema_trend":           20.0,
    "adx":                 15.0,
    "atr":                 15.0,
    "support_resistance":  20.0,
    "multi_timeframe":     15.0,
    "payout":              10.0,
    "candlestick":          5.0,
}
assert abs(sum(FILTER_SCORE_WEIGHTS.values()) - 100.0) < 1e-9  # 20+15+15+20+15+10+5 = 100

# The 5 mandatory gates — candlestick is graded/bonus, never mandatory.
# multi_timeframe is intentionally EXCLUDED from this tuple: MTF is a
# confidence modifier (see webapp/app.py::_run_pipeline()'s CONFIRMED/
# PARTIAL/CONFLICTING handling), not a hard signal gate, so it must not be
# able to block mandatory_pass either — it still contributes to the graded
# filter_score below (0/8/15 points), just not to the pass/fail gate.
_MANDATORY_FILTER_GATES = (
    "ema_trend", "adx", "atr", "support_resistance", "payout",
)

# Phase 6 convention reused here: "near" a zone = within 1.0 ATR (same value
# already used for near_support/near_resistance in the Phase 6 SR confluence
# factor — not a newly invented threshold).
_SR_NEAR_ZONE_ATR = 1.0


def calculate_filter_score(indicators: Dict[str, Any],
                           signal_data: Dict[str, Any],
                           config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Single source of truth for the Phase 7 Filter Score / hard-gate system.
    Phase 7.1: graded scoring model — see module comment above.

    Reuses ONLY already-computed fields — recalculates nothing:
      indicators:   direction, adx, atr_pct, level, support_resistance_detail,
                    candlestick_pattern_detail   (all from indicators.calculate_all())
      signal_data:  multi_tf_status, payout_pct  (from webapp/app.py::_run_pipeline())

    config (optional overrides, defaults reuse existing project constants):
      adx_trending, atr_extreme_pct, min_payout

    Grading (each component scored independently, 0..its own weight; total
    always sums to a value in [0, 100]):
      EMA Trend (20):    strong trend (trending + ADX>=threshold) -> 20;
                         weak trend (trending, ADX<threshold) -> 10; sideways -> 0
      ADX (15):          >=35->15, 30-35->13, 25-30->10, 20-25->5, <20->0
      ATR (15):          ideal (MEDIUM volatility) -> 15; slightly high/low
                         (LOW or HIGH) -> 10; extreme -> 0
      Support/Resistance (20): safe entry -> 20; near a zone but still safe
                         -> 15; no zone data at all -> 8 (neutral); unsafe -> 0
      Multi-Timeframe (15): CONFIRMED -> 15; UNAVAILABLE -> 8 (neutral);
                         DISAGREED -> 0 (conflict)
      Payout (10):       >=90->10, 85-90->8, 80-85->5, <80->0
      Candlestick (5):   reliability_score/100 * 5 (unchanged from Phase 7.0)

    `mandatory_pass` (Phase 7.1, revised): True only if all 5 mandatory gates
    meet their ORIGINAL binary bar (direction not sideways / adx>=adx_trending
    / atr_pct<=atr_extreme_pct / safe_entry is True / payout>=min_payout).
    Multi-Timeframe is NOT one of the mandatory gates — MTF is a confidence
    modifier applied earlier in webapp/app.py::_run_pipeline(), not a hard
    signal gate, so it cannot block mandatory_pass either; it still
    contributes to the graded filter_score below. This is what the scanner
    uses to decide whether to surface a signal — filter_score itself no
    longer gates visibility, it only describes quality.

    Returns: {"filter_score": 0-100, "mandatory_pass": bool,
              "passed_filters": [...], "failed_filters": [...],
              "filter_breakdown": {...}}
    """
    cfg_local = {
        "adx_trending": ADX_TRENDING,
        "atr_extreme_pct": VOLATILITY_EXTREME_ATR_PCT,   # reuse, no new threshold
        "min_payout": MIN_PAYOUT,
    }
    if config:
        cfg_local.update(config)

    breakdown: Dict[str, Dict[str, Any]] = {}
    passed: List[str] = []
    failed: List[str] = []

    # 1. EMA Trend — graded by trend presence + strength (reuses direction +
    # adx, both already computed; no new indicator).
    direction = indicators.get("direction")
    adx_val = indicators.get("adx")
    ema_ok = direction in ("BULLISH", "BEARISH")   # original binary bar, unchanged
    if not ema_ok:
        ema_points = 0.0
    elif adx_val is not None and adx_val >= cfg_local["adx_trending"]:
        ema_points = 20.0   # strong trend
    else:
        ema_points = 10.0   # weak trend (directional, but ADX hasn't confirmed strength)
    breakdown["ema_trend"] = {"max": FILTER_SCORE_WEIGHTS["ema_trend"], "points": ema_points,
                               "passed": ema_ok, "value": direction}
    (passed if ema_ok else failed).append("ema_trend")

    # 2. ADX Strength — graded bands (mandatory bar unchanged: >= adx_trending)
    adx_ok = adx_val is not None and adx_val >= cfg_local["adx_trending"]
    if adx_val is None:
        adx_points = 0.0
    elif adx_val >= 35:
        adx_points = 15.0
    elif adx_val >= 30:
        adx_points = 13.0
    elif adx_val >= 25:
        adx_points = 10.0
    elif adx_val >= 20:
        adx_points = 5.0
    else:
        adx_points = 0.0
    breakdown["adx"] = {"max": FILTER_SCORE_WEIGHTS["adx"], "points": adx_points,
                         "passed": adx_ok, "value": adx_val}
    (passed if adx_ok else failed).append("adx")

    # 3. ATR Volatility — graded via the existing LOW/MEDIUM/HIGH `level`
    # classification (already computed by indicators.volatility(), reused
    # as-is) plus the existing VOLATILITY_EXTREME_ATR_PCT cutoff for
    # "extreme". Mandatory bar unchanged: atr_pct <= atr_extreme_pct.
    atr_pct = indicators.get("atr_pct")
    vol_level = indicators.get("level")
    atr_ok = atr_pct is not None and atr_pct <= cfg_local["atr_extreme_pct"]
    if atr_pct is None:
        atr_points = 0.0
    elif atr_pct > cfg_local["atr_extreme_pct"]:
        atr_points = 0.0        # extreme
    elif vol_level == "MEDIUM":
        atr_points = 15.0       # ideal
    else:  # LOW or HIGH (but not extreme) — same thresholds indicators.volatility() already uses
        atr_points = 10.0       # slightly high/low
    breakdown["atr"] = {"max": FILTER_SCORE_WEIGHTS["atr"], "points": atr_points,
                         "passed": atr_ok, "value": atr_pct, "level": vol_level}
    (passed if atr_ok else failed).append("atr")

    # 4. Support/Resistance — graded via Phase 6's existing distance_atr /
    # safe_entry fields. Mandatory bar unchanged: safe_entry is True.
    sr_detail = indicators.get("support_resistance_detail") or {}
    safe_entry = sr_detail.get("safe_entry")
    sr_ok = safe_entry is True
    has_zone_data = sr_detail.get("nearest_support") is not None or sr_detail.get("nearest_resistance") is not None
    if not has_zone_data:
        sr_points = 8.0   # neutral — no zone data available at all
    elif not sr_ok:
        sr_points = 0.0   # unsafe — too close to a zone (Phase 6's own definition)
    else:
        s_dist = sr_detail.get("support_distance_atr")
        r_dist = sr_detail.get("resistance_distance_atr")
        near_any = (s_dist is not None and s_dist < _SR_NEAR_ZONE_ATR) or \
                   (r_dist is not None and r_dist < _SR_NEAR_ZONE_ATR)
        sr_points = 15.0 if near_any else 20.0   # near safe zone vs comfortably clear
    breakdown["support_resistance"] = {"max": FILTER_SCORE_WEIGHTS["support_resistance"],
                                        "points": sr_points, "passed": sr_ok, "value": safe_entry}
    (passed if sr_ok else failed).append("support_resistance")

    # 5. Multi-Timeframe — graded via the existing 3-state status. NOT a
    # mandatory gate (see _MANDATORY_FILTER_GATES) — MTF only contributes
    # points to the graded filter_score; it can no longer block
    # mandatory_pass. "passed"/"value" are still reported for transparency.
    mtf_status = (signal_data.get("multi_tf_status") or {}).get("status")
    mtf_ok = mtf_status == "CONFIRMED"
    if mtf_status == "CONFIRMED":
        mtf_points = 15.0
    elif mtf_status == "DISAGREED":
        mtf_points = 0.0     # conflict
    else:  # UNAVAILABLE or missing
        mtf_points = 8.0     # neutral — no confirmation either way
    breakdown["multi_timeframe"] = {"max": FILTER_SCORE_WEIGHTS["multi_timeframe"],
                                     "points": mtf_points, "passed": mtf_ok, "value": mtf_status}
    (passed if mtf_ok else failed).append("multi_timeframe")

    # 6. Payout — graded bands. Mandatory bar unchanged: payout >= min_payout.
    payout = signal_data.get("payout_pct")
    payout_ok = payout is not None and payout >= cfg_local["min_payout"]
    if payout is None:
        payout_points = 0.0
    elif payout >= 90:
        payout_points = 10.0
    elif payout >= 85:
        payout_points = 8.0
    elif payout >= 80:
        payout_points = 5.0
    else:
        payout_points = 0.0
    breakdown["payout"] = {"max": FILTER_SCORE_WEIGHTS["payout"], "points": payout_points,
                            "passed": payout_ok, "value": payout}
    (passed if payout_ok else failed).append("payout")

    # 7. Candlestick Quality — NOT mandatory. Unchanged from Phase 7.0: graded
    # 0-5, scaled directly by reliability_score.
    candle_detail = indicators.get("candlestick_pattern_detail")
    if candle_detail:
        reliability = candle_detail.get("reliability_score", 0) or 0
        candle_points = round(FILTER_SCORE_WEIGHTS["candlestick"] * (reliability / 100.0), 2)
        candle_ok = candle_points > 0
    else:
        reliability = 0
        candle_points = 0.0
        candle_ok = False
    breakdown["candlestick"] = {"max": FILTER_SCORE_WEIGHTS["candlestick"],
                                 "points": candle_points, "passed": candle_ok,
                                 "value": candle_detail.get("name") if candle_detail else None,
                                 "reliability_score": reliability}
    (passed if candle_ok else failed).append("candlestick")

    # Phase 7.1: filter_score is now ALWAYS the sum of graded points — it no
    # longer collapses to 0 on a mandatory gate failure. mandatory_pass
    # carries the original binary hard-gate semantics separately.
    filter_score = round(sum(breakdown[k]["points"] for k in breakdown), 2)
    filter_score = max(0.0, min(100.0, filter_score))  # hard clamp — always 0-100

    mandatory_pass = all(g in passed for g in _MANDATORY_FILTER_GATES)

    return {
        "filter_score": filter_score,
        "mandatory_pass": mandatory_pass,
        "passed_filters": passed,
        "failed_filters": failed,
        "filter_breakdown": breakdown,
    }


# ─── Report formatter ─────────────────────────────────────────────────────────

_SIGNAL_ICONS = {"BUY": "📈", "SELL": "📉", "WAIT": "⏳"}
_TREND_ICONS  = {"BULLISH": "↑", "BEARISH": "↓", "SIDEWAYS": "→"}


def print_report(asset: str, timeframe: str, ind: Dict[str, Any],
                 sig: Dict[str, Any], candle_count: int,
                 live_prices: list | None = None,
                 confluence: Dict[str, Any] | None = None,
                 factor_accuracies: Dict[str, Any] | None = None) -> str:
    """
    Format and print the full analysis report.
    Returns the report as a string (also printed to stdout).
    """
    now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    signal  = sig["signal"]
    conf    = sig["confidence"]
    reasons = sig.get("reasons", [])
    icon    = _SIGNAL_ICONS.get(signal, "?")
    trend   = ind.get("direction", "N/A")
    trend_icon = _TREND_ICONS.get(trend, "")

    lines = [
        "",
        "═" * 60,
        f"  QUOTEX MARKET ANALYSIS — {now}",
        "═" * 60,
        f"  Asset         : {asset}",
        f"  Timeframe     : {timeframe}",
        f"  Candles used  : {candle_count}",
        f"  Current Price : {ind.get('price', 'N/A')}",
        f"  Current Trend : {trend_icon} {trend}",
        "",
        "─ INDICATORS ─────────────────────────────────────────",
        f"  EMA  9 / 21 / 50 / 200 : "
        f"{ind.get('ema_9','N/A')} / {ind.get('ema_21','N/A')} / "
        f"{ind.get('ema_50','N/A')} / {ind.get('ema_200','N/A')}",
        f"  RSI (14)       : {ind.get('rsi', 'N/A')}",
        f"  MACD           : {ind.get('macd', 'N/A')} "
        f"| Signal: {ind.get('macd_signal','N/A')} "
        f"| Hist: {ind.get('macd_histogram','N/A')}",
        f"  Bollinger Bands: U {ind.get('bb_upper','N/A')} / "
        f"M {ind.get('bb_middle','N/A')} / L {ind.get('bb_lower','N/A')}",
        f"  ATR (14)       : {ind.get('atr', 'N/A')} ({ind.get('atr_pct','N/A')}%)",
        f"  VWAP           : {ind.get('vwap', 'N/A')}",
        f"  ADX (14)       : {ind.get('adx', 'N/A')}  "
        f"(+DI {ind.get('di_plus','N/A')} / -DI {ind.get('di_minus','N/A')})",
        f"  Stoch RSI      : K {ind.get('stoch_rsi_k','N/A')} / D {ind.get('stoch_rsi_d','N/A')}",
        f"  Stochastic Osc : K {ind.get('stoch_k','N/A')} / D {ind.get('stoch_d','N/A')}",
        f"  CCI (20)       : {ind.get('cci', 'N/A')}",
        f"  Candlestick    : {ind.get('candlestick_pattern') or 'none'}",
        "",
        "─ SUPPORT & RESISTANCE ───────────────────────────────",
        f"  Support        : {ind.get('support', 'N/A')}",
        f"  Pivot          : {ind.get('pivot', 'N/A')}",
        f"  Resistance     : {ind.get('resistance', 'N/A')}",
        "",
        "─ VOLATILITY ─────────────────────────────────────────",
        f"  Level          : {ind.get('level', 'N/A')}",
        f"  BB Width       : {ind.get('bb_width_pct', 'N/A')}%",
        f"  Hist. Vol.     : {ind.get('hist_vol_pct', 'N/A')}% (ann.)",
        "",
        "─ SIGNAL REASONING ───────────────────────────────────",
    ]
    lines.extend(reasons)
    lines += [
        "",
        "─ CONCLUSION (legacy indicator-vote signal) ───────────",
        f"  Bullish signals : {sig.get('bullish_count', 0)}",
        f"  Bearish signals : {sig.get('bearish_count', 0)}",
        "",
        f"  {icon}  SIGNAL     : {signal}",
        f"  📊 CONFIDENCE  : {conf}%",
    ]

    # ── Confluence signal (side-by-side comparison with legacy signal) ────────
    if confluence is not None:
        c_signal = confluence.get("signal", "WAIT")
        c_conf   = confluence.get("confidence", 0)
        c_icon   = _SIGNAL_ICONS.get(c_signal, "?")
        factors  = confluence.get("factors", {})
        weights  = confluence.get("weights_used", {})
        vote_label = {1: "BULLISH", -1: "BEARISH", 0: "neutral"}
        factor_names = {
            "bb": "BB Bounce", "rsi_div": "RSI Divergence", "stoch": "Stochastic Cross",
            "cci": "CCI Extreme", "candle": "Candlestick", "mean_reversion": "Mean Reversion",
            "exhaustion": "Candle Exhaustion", "round_number": "Round Number",
            "obv": "OBV Divergence",
        }

        lines += [
            "",
            f"─ CONFLUENCE SIGNAL (backtest-weighted, {len(factor_names)}-factor) ─────",
        ]
        for key, label in factor_names.items():
            vote = factors.get(key, 0)
            w = weights.get(key, 0)
            acc = (factor_accuracies or {}).get(key)
            acc_str = f"{acc}% acc" if acc is not None else "insufficient data"
            lines.append(f"  {label:<18}: {vote_label.get(vote, 'neutral'):<8} "
                        f"(weight {w:>5.1f}, {acc_str})")
        lines += [
            "",
            f"  {c_icon}  SIGNAL     : {c_signal}   (confluence)",
            f"  📊 CONFIDENCE  : {c_conf}%",
            "",
            "─ SIGNAL COMPARISON ────────────────────────────────────",
            f"  Legacy indicator-vote : {signal:<5} ({conf}% confidence)",
            f"  Confluence (weighted) : {c_signal:<5} ({c_conf}% confidence)",
            f"  Agreement             : {'YES — both agree' if signal == c_signal else 'NO — signals differ, treat with extra caution'}",
        ]

    lines += [
        "",
        "  ⚠  This is for market analysis ONLY.",
        "  ⚠  No trades are placed automatically.",
        "═" * 60,
    ]

    # Live price tail
    if live_prices:
        lines.append(f"\n  Live prices captured: {len(live_prices)}")
        for lp in live_prices[-5:]:
            lines.append(f"    {lp.get('timestamp','')} → {lp.get('price','')}")
        lines.append("")

    report = "\n".join(lines)
    print(report)
    return report
