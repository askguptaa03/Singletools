"""
Backtest-based dynamic factor weighting for the Quotex Market Analyzer.

Replays each of the 13 confluence factors (bb, rsi_div, stoch, cci, candle,
mean_reversion, exhaustion, round_number, obv, sr, wick_rejection,
liquidity_sweep, false_breakout) bar-by-bar across
already-fetched historical candles, and measures how often each factor's
vote correctly predicted price direction `lookahead` candles later. Purely
historical/statistical analysis on data already in memory — NEVER places
orders, NEVER calls any network/API function.
"""

from __future__ import annotations
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

try:
    from indicators import (
        bollinger_bands, rsi, stochastic_oscillator, cci, detect_candlestick_pattern_detailed, obv,
        _swing_high_low_prices, _cluster_zones, _SR_TOO_CLOSE_ATR, ema, adx as _adx_fn, atr as _atr_fn,
        detect_wick_rejection, detect_liquidity_sweep, detect_false_breakout,
    )
except ImportError:
    from market_analyzer.indicators import (
        bollinger_bands, rsi, stochastic_oscillator, cci, detect_candlestick_pattern_detailed, obv,
        _swing_high_low_prices, _cluster_zones, _SR_TOO_CLOSE_ATR, ema, adx as _adx_fn, atr as _atr_fn,
        detect_wick_rejection, detect_liquidity_sweep, detect_false_breakout,
    )

# Phase 5: mirrors analyzer.CANDLE_RELIABILITY_VOTE_THRESHOLD. Kept as an
# independent constant (not imported cross-module) to match this file's
# existing pattern of self-contained defaults (see _DEFAULT_8F_WEIGHTS,
# which similarly mirrors analyzer.DEFAULT_CONFLUENCE_WEIGHTS rather than
# importing it) and to avoid introducing a circular import.
CANDLE_RELIABILITY_VOTE_THRESHOLD = 40.0

# Phase 6: mirrors analyzer.SR_RELIABILITY_VOTE_THRESHOLD, same
# independent-constant convention as above.
SR_RELIABILITY_VOTE_THRESHOLD = 40.0

# Phase 5 (backtest 13-factor fix): mirrors analyzer.WICK_REJECTION_VOTE_
# THRESHOLD / LIQUIDITY_SWEEP_VOTE_THRESHOLD / FALSE_BREAKOUT_VOTE_
# THRESHOLD, same independent-constant convention as above.
WICK_REJECTION_VOTE_THRESHOLD = 40.0
LIQUIDITY_SWEEP_VOTE_THRESHOLD = 40.0
FALSE_BREAKOUT_VOTE_THRESHOLD = 40.0

# Phase 7: mirrors config.ADX_TRENDING / analyzer.VOLATILITY_EXTREME_ATR_PCT,
# same independent-constant convention as above — used only by
# backtest_filter_score_report() below.
_FS_ADX_TRENDING = 25.0
_FS_ATR_EXTREME_PCT = 1.5
# Phase 7.1: mirrors analyzer._SR_NEAR_ZONE_ATR — same "near a zone" band
# (1.0 ATR) used for the graded Support/Resistance score below.
_FS_SR_NEAR_ZONE_ATR = 1.0

# A factor needs at least this many historical signals before its backtested
# accuracy is trusted; otherwise its default weight is used instead.
# Single source of truth — used by BOTH backtest_factor_accuracy() (gates
# whether an accuracy is computed at all) and compute_dynamic_weights()
# (gates whether that accuracy is trusted enough to override the default
# weight). Previously these were two separate constants (10 here, and a
# second hardcoded `20` inside compute_dynamic_weights) which meant this
# constant had no real effect on behavior — compute_dynamic_weights' 20 was
# always the binding gate. Set to 20 here (unifying at the value that was
# already effectively in control) so behavior is unchanged and there is now
# exactly one place to adjust this threshold in the future.
MIN_SIGNALS_REQUIRED = 20

# Default weights for the 13-factor confluence system. Mirrors
# analyzer.DEFAULT_CONFLUENCE_WEIGHTS's values exactly (independent
# constant, not imported — same convention as the threshold constants
# above), so this file's fallback defaults never drift from the live
# confluence engine's.
#
# Bug fix, prerequisite for Phase 9 (found auditing the live pipeline):
# this dict previously had only the original 10 factors at their
# pre-Phase-8.6 value (10.0 each). Since compute_dynamic_weights() below
# builds its ENTIRE output from this dict's keys, wick_rejection/
# liquidity_sweep/false_breakout were never present in its result at
# all — and because generate_confluence_signal() REPLACES (not merges)
# its weights dict when a non-empty dynamic_weights is passed, those 3
# indicators silently received an effective weight of 0 on every real
# request through app.py's live pipeline, despite their votes being
# computed correctly. Confirmed end-to-end with the real functions
# before this fix (see docs/CHANGELOG.md's Phase 9 entry for the
# verification trace). The 10 original factors' *values* also had to
# move from 10.0 to 7.69 here (not just the 3 new keys added) — with 13
# keys now sharing the reserved-weight budget, leaving the old 10.0s in
# place would sum to 123.1 instead of 100 and break
# compute_dynamic_weights()'s "reserved / remaining_budget" math. This
# is a data-only change — no rule in compute_dynamic_weights() below is
# touched, and _factor_votes()/backtest_factor_accuracy() still only
# score the original 10 (extending backtested accuracy scoring itself
# to the 3 new indicators remains the separately-tracked NEXT_PHASE.md
# pending item, intentionally not done here).
_DEFAULT_8F_WEIGHTS: Dict[str, float] = {
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
assert abs(sum(_DEFAULT_8F_WEIGHTS.values()) - 100.0) < 1e-9
assert len(_DEFAULT_8F_WEIGHTS) == 13



def _factor_votes(df: pd.DataFrame,
                  indicators_history: Optional[Dict[str, Any]] = None) -> Dict[str, pd.Series]:
    """
    Build a full-history +1 / 0 / -1 vote Series for each of the 13 confluence
    factors, replaying the same logic used in
    analyzer.generate_confluence_signal() across every bar (not just the
    latest one), so accuracy can be measured historically.

    `indicators_history` (optional) is the dict returned by
    indicators.calculate_all() for this df — if it carries pre-computed
    `_rsi_series` / `_cci_series` / `_obv_series` (and matching periods),
    those are reused instead of recomputing, so the backtest stays
    consistent with whatever settings (default or OTC-tuned) were used for
    the live analysis.
    """
    indicators_history = indicators_history or {}
    close = df["close"]

    # ── BB Bounce ────────────────────────────────────────────────────────────
    bb_period = indicators_history.get("_bb_period", 20)
    bb_std = indicators_history.get("_bb_std", 2.0)
    bb = bollinger_bands(close, bb_period, bb_std)
    bb_vote = pd.Series(0, index=df.index)
    bb_vote[close <= bb["lower"]] = 1
    bb_vote[close >= bb["upper"]] = -1

    # ── RSI Divergence (price vs RSI direction over a 5-candle window) ───────
    rsi_period = indicators_history.get("_rsi_period", 14)
    rsi_s = indicators_history.get("_rsi_series")
    if rsi_s is None:
        rsi_s = rsi(close, rsi_period)
    price_change_5 = close.diff(5)
    rsi_change_5 = rsi_s.diff(5)
    rsi_vote = pd.Series(0, index=df.index)
    rsi_vote[(price_change_5 < 0) & (rsi_change_5 > 0)] = 1   # bullish divergence
    rsi_vote[(price_change_5 > 0) & (rsi_change_5 < 0)] = -1  # bearish divergence

    # ── Stochastic Cross (classic price-based oscillator) ────────────────────
    k_period = indicators_history.get("_stoch_k_period", 14)
    d_period = indicators_history.get("_stoch_d_period", 3)
    stoch = stochastic_oscillator(df, k_period, d_period)
    k, d = stoch["k"], stoch["d"]
    k_prev, d_prev = k.shift(1), d.shift(1)
    cross_up = (k_prev <= d_prev) & (k > d)
    cross_down = (k_prev >= d_prev) & (k < d)
    stoch_vote = pd.Series(0, index=df.index)
    stoch_vote[(k < 20) & (d < 20) & cross_up] = 1
    stoch_vote[(k > 80) & (d > 80) & cross_down] = -1

    # ── CCI Extreme + Reversal ────────────────────────────────────────────────
    cci_period = indicators_history.get("_cci_period", 20)
    cci_s = indicators_history.get("_cci_series")
    if cci_s is None:
        cci_s = cci(df, cci_period)
    cci_rising = cci_s.diff() > 0
    cci_falling = cci_s.diff() < 0
    cci_vote = pd.Series(0, index=df.index)
    cci_vote[(cci_s < -100) & cci_rising] = 1
    cci_vote[(cci_s > 100) & cci_falling] = -1

    # ── Candlestick pattern (per-bar, using a rolling up-to-3-candle window) ──
    # Phase 5: widened from 2 rows to 3 to allow Morning/Evening Star (3-candle
    # patterns) to be measured historically. This does NOT change behavior for
    # doji/hammer/engulfing — those only ever read the last 1-2 rows of
    # whatever window they're given, so giving them an extra leading row (when
    # available) changes nothing about their detection. Uses the same
    # richer-detail-with-reliability-gate logic as the live vote function
    # (analyzer._confluence_factor_votes), so live and backtest measure the
    # same thing.
    candle_vote = pd.Series(0, index=df.index)
    for i in range(1, len(df)):
        window = df.iloc[max(0, i - 2):i + 1]
        detail = detect_candlestick_pattern_detailed(window)
        if detail is not None:
            direction = detail.get("direction", "NEUTRAL")
            reliability = detail.get("reliability_score", 0)
            if direction == "BUY" and reliability >= CANDLE_RELIABILITY_VOTE_THRESHOLD:
                candle_vote.iloc[i] = 1
            elif direction == "SELL" and reliability >= CANDLE_RELIABILITY_VOTE_THRESHOLD:
                candle_vote.iloc[i] = -1

    # ── Mean Reversion (factor 6) ─────────────────────────────────────────────
    # Upper band + strength >= bb_std → bearish reversal
    # Lower band + strength <= -bb_std → bullish reversal
    mr_bb_std = indicators_history.get("_bb_std", 2.0)
    mr_bb     = bollinger_bands(close, bb_period, mr_bb_std)
    std_roll  = close.rolling(bb_period).std(ddof=0).replace(0, np.nan).fillna(0)
    sma_roll  = close.rolling(bb_period).mean().fillna(close)
    strength_s = (close - sma_roll) / std_roll.replace(0, np.nan).fillna(0)
    mr_vote = pd.Series(0, index=df.index)
    mr_vote[(close >= mr_bb["upper"]) & (strength_s >= mr_bb_std)]  = -1  # bearish
    mr_vote[(close <= mr_bb["lower"]) & (strength_s <= -mr_bb_std)] =  1  # bullish

    # ── Consecutive Candle Exhaustion (factor 7) ──────────────────────────────
    # 5+ consecutive bull candles → bearish reversal vote
    # 5+ consecutive bear candles → bullish reversal vote
    body_dir = pd.Series(0, index=df.index)
    body_dir[df["close"] > df["open"]] =  1
    body_dir[df["close"] < df["open"]] = -1
    streak_group = (body_dir != body_dir.shift(1)).cumsum()
    consec       = body_dir.groupby(streak_group).cumcount() + 1
    exh_vote = pd.Series(0, index=df.index)
    exh_vote[(body_dir ==  1) & (consec >= 5)] = -1  # bull exhaustion → bearish
    exh_vote[(body_dir == -1) & (consec >= 5)] =  1  # bear exhaustion → bullish

    # ── Round Number (factor 8) ───────────────────────────────────────────────
    # Psychological level — always neutral (0) in the backtest vote;
    # accuracy will be None (0 signals) → uses default weight.
    rn_vote = pd.Series(0, index=df.index)

    # ── OBV Divergence (factor 9, Step 4) ─────────────────────────────────────
    # Same 5-candle price-vs-indicator divergence pattern as RSI Divergence.
    obv_s = indicators_history.get("_obv_series")
    if obv_s is None:
        obv_s = obv(df)
    obv_change_5 = obv_s.diff(5)
    obv_vote = pd.Series(0, index=df.index)
    obv_vote[(price_change_5 < 0) & (obv_change_5 > 0)] = 1   # bullish divergence
    obv_vote[(price_change_5 > 0) & (obv_change_5 < 0)] = -1  # bearish divergence

    # ── Support/Resistance Zone (factor 10, Phase 6) ──────────────────────────
    # Fully vectorized — no Python loop. Reuses the exact same fractal swing
    # detector as the live engine (indicators._swing_high_low_prices), then
    # shift(k)+ffill() to get, at each bar, the most recently CONFIRMED swing
    # high/low known as of that bar (causal — avoids look-ahead bias from the
    # detector's centered window).
    #
    # Honest simplification vs. the live snapshot engine
    # (indicators.detect_support_resistance_zones): this tracks only the
    # single most-recently-confirmed swing high/low per bar, not the full
    # multi-zone clustering with touch-count strength that the live engine
    # computes. True per-bar zone re-clustering would need either a Python
    # loop or expanding-window re-computation — both explicitly discouraged
    # for this step. Reliability is approximated by requiring a confirmed
    # swing actually exists (known_support/known_resistance not NaN) rather
    # than a touch-count-based score, since a single ffilled point has no
    # "touch count" — this still enforces "don't vote on absent/speculative
    # evidence," the same protective intent as the live reliability gate,
    # just via a simpler mechanism.
    sr_k = 2
    swing_high_price, swing_low_price = _swing_high_low_prices(df, k=sr_k)
    known_resistance = swing_high_price.shift(sr_k).ffill()
    known_support = swing_low_price.shift(sr_k).ffill()

    atr_proxy = (df["high"] - df["low"]).rolling(14, min_periods=1).mean()
    atr_proxy = atr_proxy.where(atr_proxy > 0).bfill().fillna(1e-6)

    support_dist_atr = (close - known_support) / atr_proxy
    resistance_dist_atr = (known_resistance - close) / atr_proxy

    near_support = support_dist_atr < 1.0
    near_resistance = resistance_dist_atr < 1.0
    unsafe = (support_dist_atr < _SR_TOO_CLOSE_ATR) | (resistance_dist_atr < _SR_TOO_CLOSE_ATR)
    safe_entry = ~unsafe.fillna(False)

    sr_vote = pd.Series(0, index=df.index)
    sr_vote[near_support.fillna(False) & safe_entry & ~near_resistance.fillna(False)
            & known_support.notna()] = 1
    sr_vote[near_resistance.fillna(False) & safe_entry & ~near_support.fillna(False)
            & known_resistance.notna()] = -1

    # ── Wick Rejection / Liquidity Sweep / False Breakout (factors 11-13) ────
    # Phase 5 fix (Phase 9's weights-dict expansion to 13 factors was never
    # matched by a corresponding _factor_votes() update — these 3 factors
    # had NO historical vote series at all, so backtest_factor_accuracy()
    # could never compute accuracy for them and compute_dynamic_weights()
    # always fell back to their static default weight, regardless of real
    # historical performance).
    #
    # Same established pattern as the "Candlestick pattern" loop above:
    # replay the exact live detector (indicators.detect_wick_rejection /
    # detect_liquidity_sweep / detect_false_breakout) on a rolling,
    # causal (no look-ahead) window ending at each bar, gated by the same
    # reliability_score thresholds analyzer.py uses. Combined into one loop
    # (instead of three separate loops) purely for efficiency — each
    # function is still called independently, with its own window size
    # matching what that live function actually requires. Reuses the
    # atr_proxy Series already computed above for the S/R factor as the
    # atr_value argument (same "shared, already-computed ATR" convention
    # analyzer.py's calculate_all() -> generate_confluence_signal() uses
    # live, instead of letting each detector silently recompute its own).
    wick_rejection_vote = pd.Series(0, index=df.index)
    liquidity_sweep_vote = pd.Series(0, index=df.index)
    false_breakout_vote = pd.Series(0, index=df.index)

    _ls_lookback = 20   # matches detect_liquidity_sweep()'s own default
    _fb_lookback = 30   # matches detect_false_breakout()'s own default

    for i in range(1, len(df)):
        atr_v = float(atr_proxy.iloc[i]) if atr_proxy.iloc[i] > 0 else None

        wr_window = df.iloc[i:i + 1]
        wr_detail = detect_wick_rejection(wr_window, atr_value=atr_v)
        if wr_detail is not None:
            direction = wr_detail.get("direction")
            reliability = wr_detail.get("reliability_score", 0) or 0
            if direction == "BUY" and reliability >= WICK_REJECTION_VOTE_THRESHOLD:
                wick_rejection_vote.iloc[i] = 1
            elif direction == "SELL" and reliability >= WICK_REJECTION_VOTE_THRESHOLD:
                wick_rejection_vote.iloc[i] = -1

        ls_window = df.iloc[max(0, i - (_ls_lookback + 1)):i + 1]
        ls_detail = detect_liquidity_sweep(ls_window, lookback=_ls_lookback, atr_value=atr_v)
        if ls_detail is not None:
            direction = ls_detail.get("direction")
            reliability = ls_detail.get("reliability_score", 0) or 0
            if direction == "BUY" and reliability >= LIQUIDITY_SWEEP_VOTE_THRESHOLD:
                liquidity_sweep_vote.iloc[i] = 1
            elif direction == "SELL" and reliability >= LIQUIDITY_SWEEP_VOTE_THRESHOLD:
                liquidity_sweep_vote.iloc[i] = -1

        fb_window = df.iloc[max(0, i - _fb_lookback - 1):i + 1]
        fb_detail = detect_false_breakout(fb_window, sr_detail=None, atr_value=atr_v, lookback=_fb_lookback)
        if fb_detail is not None:
            direction = fb_detail.get("direction")
            reliability = fb_detail.get("reliability_score", 0) or 0
            if direction == "BUY" and reliability >= FALSE_BREAKOUT_VOTE_THRESHOLD:
                false_breakout_vote.iloc[i] = 1
            elif direction == "SELL" and reliability >= FALSE_BREAKOUT_VOTE_THRESHOLD:
                false_breakout_vote.iloc[i] = -1

    return {
        "bb":             bb_vote,
        "rsi_div":        rsi_vote,
        "stoch":          stoch_vote,
        "cci":            cci_vote,
        "candle":         candle_vote,
        "mean_reversion": mr_vote,
        "exhaustion":     exh_vote,
        "round_number":   rn_vote,
        "obv":            obv_vote,
        "sr":             sr_vote,
        "wick_rejection":  wick_rejection_vote,
        "liquidity_sweep": liquidity_sweep_vote,
        "false_breakout":  false_breakout_vote,
    }


def backtest_factor_accuracy(df: pd.DataFrame,
                             indicators_history: Optional[Dict[str, Any]] = None,
                             lookahead: int = 4) -> Dict[str, Dict[str, Any]]:
    """
    For each of the 13 confluence factors, replay historical +1/-1 votes and
    check whether price moved in the voted direction `lookahead` candles later.

    Returns:
        {
            factor_name: {
                "accuracy":    float | None,   # None = insufficient signals
                "sample_size": int,            # total non-zero historical votes
            }
        }
    If sample_size < MIN_SIGNALS_REQUIRED the accuracy is None and the caller
    falls back to the default weight for that factor.
    """
    close         = df["close"]
    future_return = close.shift(-lookahead) - close
    votes         = _factor_votes(df, indicators_history)

    result: Dict[str, Dict[str, Any]] = {}
    for name, vote in votes.items():
        valid     = (vote != 0) & future_return.notna()
        n_signals = int(valid.sum())
        if n_signals < MIN_SIGNALS_REQUIRED:
            result[name] = {"accuracy": None, "sample_size": n_signals}
            continue
        correct = (
            ((vote[valid] > 0) & (future_return[valid] > 0))
            | ((vote[valid] < 0) & (future_return[valid] < 0))
        )
        result[name] = {
            "accuracy":    round(float(correct.sum()) / n_signals * 100, 2),
            "sample_size": n_signals,
        }

    return result


def compute_dynamic_weights(accuracies: Dict[str, Any],
                            default_weights: Optional[Dict[str, float]] = None
                            ) -> Dict[str, float]:
    """
    Turn backtested factor detail dicts into a weight set that always sums to
    100.

    Accepts both the new format  {factor: {"accuracy": ..., "sample_size": ...}}
    and the legacy format         {factor: float | None}  (for backward compat).

    Rules (applied in priority order):
      1. accuracy is None OR sample_size < MIN_SIGNALS_REQUIRED → insufficient
         data → default weight
      2. accuracy <= 52 %                       → near-random → fixed score of 5
         (still participates in normalisation but gets a penalty score)
      3. Otherwise                              → score = accuracy value
         (scored factors share the remaining budget proportionally)
    """
    if default_weights is None:
        default_weights = _DEFAULT_8F_WEIGHTS

    weights:  Dict[str, float] = {}
    scored:   Dict[str, float] = {}
    reserved: float            = 0.0

    for name, default_w in default_weights.items():
        detail = accuracies.get(name)

        # Unpack new dict format or legacy float/None
        if isinstance(detail, dict):
            acc         = detail.get("accuracy")
            sample_size = detail.get("sample_size", 0)
        else:
            acc         = detail             # legacy: float or None
            sample_size = 999 if acc is not None else 0  # assume sufficient if float

        if acc is None or sample_size < MIN_SIGNALS_REQUIRED:
            # Insufficient data → use default weight
            weights[name]  = float(default_w)
            reserved       += default_w
        elif acc <= 52.0:
            # Near-random accuracy → penalised score (still proportionally scaled)
            scored[name] = 5.0
        else:
            scored[name] = acc

    if not scored:
        # Every factor fell back to its default
        diff = round(100.0 - sum(weights.values()), 2)
        if abs(diff) >= 0.01 and weights:
            largest = max(weights, key=weights.get)
            weights[largest] = round(weights[largest] + diff, 2)
        return weights

    remaining_budget = 100.0 - reserved
    total_score      = sum(scored.values())

    if total_score <= 0 or remaining_budget <= 0:
        share = max(0.0, remaining_budget / len(scored)) if scored else 0.0
        for name in scored:
            weights[name] = round(share, 2)
    else:
        for name, score in scored.items():
            weights[name] = round(remaining_budget * (score / total_score), 2)

    # Round-off correction so weights always sum to exactly 100.
    diff = round(100.0 - sum(weights.values()), 2)
    if abs(diff) >= 0.01 and weights:
        largest = max(weights, key=weights.get)
        weights[largest] = round(weights[largest] + diff, 2)

    return weights


# ─── Phase 7 STEP 7: Filter Score Range vs Accuracy report ────────────────────

FILTER_SCORE_BUCKETS = [(0, 20), (20, 40), (40, 60), (60, 80), (80, 100)]


def backtest_filter_score_report(df: pd.DataFrame, lookahead: int = 4) -> Dict[str, Any]:
    """
    Buckets historical bars by a REDUCED Filter Score and reports win rate /
    accuracy per bucket (0-20, 20-40, 40-60, 60-80, 80-100).

    HONEST LIMITATION (documented, not fabricated — see Phase 7 audit):
    Only 5 of the 7 live Filter Score criteria are backtestable from
    historical OHLCV data alone: EMA Trend, ADX, ATR, Support/Resistance,
    Candlestick Quality. The other two are structurally excluded:
      - Multi-Timeframe Confirmation: would require a second historical
        timeframe fetched and analyzed in lockstep — not available here.
      - Payout: Quotex has NO historical payout series at all (it's a live,
        current-moment broker value) — this can never be backtested,
        regardless of how much data plumbing is added.
    The 5 available criteria's weights (20/15/15/20/5 = 75 of the live
    system's 100) are proportionally RESCALED to their own 0-100 range, so
    this reduced score is NOT directly comparable in absolute value to
    analyzer.calculate_filter_score()'s live 7-criteria output — it answers
    "does score, from what CAN be measured historically, correlate with
    forward accuracy" rather than reproducing the exact live number.

    Reuses existing vectorized indicator building blocks (ema, adx, atr, the
    Phase 6 SR swing detector, the Phase 5 candlestick detector) — does NOT
    recompute or duplicate any indicator's own math. EMA/ADX/ATR/SR are
    fully vectorized; candlestick detection needs a light per-bar loop (same
    documented trade-off as Phase 5 — a single geometry check per bar, not a
    recomputation of any indicator).
    """
    n = len(df)
    if n < 60:
        return {
            "error": "Insufficient candles for a meaningful backtest (need >= 60)",
            "buckets": {}, "limitation": "See docstring — MTF and Payout are not backtestable.",
        }

    close = df["close"]

    # ── EMA Trend (vectorized) — mirrors indicators.trend_direction()'s
    # comparison logic (EMA9 vs EMA21, price vs EMA50, 5-bar slope), applied
    # to every bar instead of just the latest one. Reuses ema(), not a new
    # indicator.
    ema9, ema21, ema50 = ema(close, 9), ema(close, 21), ema(close, 50)
    slope = ema9.pct_change(5) * 100
    bull_votes = (ema9 > ema21).astype(int) + (close > ema50).astype(int) + (slope > 0).astype(int)
    bear_votes = (ema9 < ema21).astype(int) + (close < ema50).astype(int) + (slope < 0).astype(int)
    direction = pd.Series("SIDEWAYS", index=df.index)
    direction[bull_votes >= 2] = "BULLISH"
    direction[bear_votes >= 2] = "BEARISH"
    ema_ok = direction != "SIDEWAYS"

    # ── ADX (vectorized, reuses adx()) ────────────────────────────────────
    adx_series = _adx_fn(df, 14)["adx"]
    adx_ok = adx_series >= _FS_ADX_TRENDING

    # ── ATR (vectorized, reuses atr(); same atr_pct formula as indicators.volatility()) ─
    atr_series = _atr_fn(df, 14)
    atr_pct_series = (atr_series / close.replace(0, np.nan) * 100)
    atr_ok = (atr_pct_series <= _FS_ATR_EXTREME_PCT).fillna(False)

    # ── Support/Resistance safe_entry (vectorized, same causal shift+ffill
    # pattern as the Phase 6 SR confluence factor) ────────────────────────
    sr_k = 2
    swing_high_price, swing_low_price = _swing_high_low_prices(df, k=sr_k)
    known_resistance = swing_high_price.shift(sr_k).ffill()
    known_support = swing_low_price.shift(sr_k).ffill()
    atr_proxy = atr_series.where(atr_series > 0).bfill().fillna(1e-6)
    support_dist_atr = (close - known_support) / atr_proxy
    resistance_dist_atr = (known_resistance - close) / atr_proxy
    unsafe = (support_dist_atr < _SR_TOO_CLOSE_ATR) | (resistance_dist_atr < _SR_TOO_CLOSE_ATR)
    sr_ok = ~unsafe.fillna(False)
    has_zone_data = known_support.notna() | known_resistance.notna()
    near_any = ((support_dist_atr < _FS_SR_NEAR_ZONE_ATR).fillna(False) |
                (resistance_dist_atr < _FS_SR_NEAR_ZONE_ATR).fillna(False))

    # ── Candlestick Quality (per-bar — same documented trade-off as Phase 5;
    # a lightweight geometry check per bar, not a duplicated indicator) ────
    candle_points_raw = pd.Series(0.0, index=df.index)
    for i in range(2, n):
        window = df.iloc[max(0, i - 2):i + 1]
        detail = detect_candlestick_pattern_detailed(window)
        if detail is not None:
            candle_points_raw.iloc[i] = round(5.0 * (detail.get("reliability_score", 0) / 100.0), 2)

    # ── Rescale the 5 available criteria's weights to their own 0-100 range ─
    # Live weights for these 5: ema=20, adx=15, atr=15, sr=20, candle=5 (sum=75).
    # Phase 7.1: GRADED per-band scoring (vectorized), mirroring
    # analyzer.calculate_filter_score()'s bands exactly, just rescaled —
    # replaces the old binary all-or-nothing-per-criterion version.
    rescale = 100.0 / 75.0

    # EMA: strong (trending + ADX>=threshold) -> 20*rescale; weak (trending,
    # ADX<threshold) -> 10*rescale; sideways -> 0
    ema_points = pd.Series(0.0, index=df.index)
    ema_points[ema_ok & adx_ok] = 20.0 * rescale
    ema_points[ema_ok & ~adx_ok] = 10.0 * rescale

    # ADX: >=35->15, 30-35->13, 25-30->10, 20-25->5, <20->0 (all * rescale)
    adx_points = pd.Series(0.0, index=df.index)
    adx_points[adx_series >= 35] = 15.0 * rescale
    adx_points[(adx_series >= 30) & (adx_series < 35)] = 13.0 * rescale
    adx_points[(adx_series >= 25) & (adx_series < 30)] = 10.0 * rescale
    adx_points[(adx_series >= 20) & (adx_series < 25)] = 5.0 * rescale

    # ATR: ideal (MEDIUM, reusing indicators.volatility()'s own 0.15/0.5
    # thresholds) -> 15*rescale; slightly high/low (LOW/HIGH, not extreme)
    # -> 10*rescale; extreme -> 0
    atr_points = pd.Series(0.0, index=df.index)
    is_extreme = (atr_pct_series > _FS_ATR_EXTREME_PCT).fillna(True)
    is_medium = (atr_pct_series > 0.15) & (atr_pct_series <= 0.5)
    atr_points[~is_extreme & is_medium] = 15.0 * rescale
    atr_points[~is_extreme & ~is_medium] = 10.0 * rescale

    # Support/Resistance: safe -> 20*rescale; near-but-safe -> 15*rescale;
    # no zone data -> 8*rescale (neutral); unsafe -> 0
    sr_points = pd.Series(8.0 * rescale, index=df.index)   # default: neutral (no zone data)
    sr_points[has_zone_data & sr_ok & ~near_any] = 20.0 * rescale
    sr_points[has_zone_data & sr_ok & near_any] = 15.0 * rescale
    sr_points[has_zone_data & ~sr_ok] = 0.0

    mandatory_ok = ema_ok & adx_ok & atr_ok & sr_ok
    filter_score_series = (ema_points + adx_points + atr_points + sr_points
                            + candle_points_raw * rescale).clip(0.0, 100.0)

    # ── Forward outcome + bucket assignment ───────────────────────────────
    future_close = close.shift(-lookahead)
    is_trade = mandatory_ok & future_close.notna()
    win = is_trade & (
        ((direction == "BULLISH") & (future_close > close)) |
        ((direction == "BEARISH") & (future_close < close))
    )
    loss = is_trade & ~win

    buckets_out: Dict[str, Any] = {}
    for lo, hi in FILTER_SCORE_BUCKETS:
        label = f"{lo}-{hi}"
        in_bucket = is_trade & (filter_score_series >= lo) & (filter_score_series <= hi if hi == 100 else filter_score_series < hi)
        trades = int(in_bucket.sum())
        wins = int((in_bucket & win).sum())
        losses = int((in_bucket & loss).sum())
        buckets_out[label] = {
            "trades": trades, "wins": wins, "losses": losses,
            "accuracy": round(wins / trades, 4) if trades else None,
        }

    return {
        "buckets": buckets_out,
        "lookahead": lookahead,
        "total_bars": n,
        "total_trades": int(is_trade.sum()),
        "criteria_used": ["ema_trend", "adx", "atr", "support_resistance", "candlestick"],
        "criteria_excluded": {
            "multi_timeframe": "Requires a second historical timeframe fetched in lockstep — not available.",
            "payout": "Quotex has no historical payout series — never backtestable.",
        },
        "note": "Reduced 5-criteria GRADED score (Phase 7.1), rescaled to 0-100. Not directly "
                "comparable to the live 7-criteria filter_score from analyzer.calculate_filter_score(). "
                "'Trade' bars still require all 4 backtestable mandatory gates (ema/adx/atr/sr) to "
                "individually pass, so the practical minimum score for a counted trade is roughly "
                "50 (weak-but-passing grades across all 4) rather than 0 — the 0-20/20-40 buckets may "
                "still be sparse or empty even after the Phase 7.1 grading improvement, which is an "
                "expected consequence of keeping mandatory-gate trade eligibility, not a bug.",
    }
