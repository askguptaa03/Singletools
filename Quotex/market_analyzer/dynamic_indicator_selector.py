"""
Phase 10.3 Part-1 — Dynamic Indicator Selection.

Distinct in kind from regime_weight_engine.py's CONTINUOUS per-factor
multiplier table: this module makes a CATEGORICAL relevance call per
regime — which confluence factors are "primary" (most trustworthy in this
regime) vs "low relevance" (still computed and still voting, just
discounted) vs neutral (untouched).

Deliberate design choice: a deselected factor's weight is REDUCED, never
zeroed. Zeroing would (a) be equivalent to permanently disabling an
indicator, which is a Settings-level user decision
(_apply_settings_weight_overrides in webapp/app.py), not something an
automatic regime classifier should do, and (b) risks starving
analyzer.generate_confluence_signal()'s "agreeing_factors >= MIN_AGREEING_FACTORS" requirement
of real information — vote counting is based on the factor's vote sign, not
its weight, so a zeroed factor could still count toward "agreeing" while
contributing nothing, which is a worse, more confusing outcome than a mild
discount. "Return Unknown instead of guessing" (Phase 10.3 scope) is
respected the same way: Uncertain/Mixed and Unknown regimes select nothing
and change nothing.

Independently testable: apply_dynamic_indicator_selection() takes a plain
weights dict + a regime_result dict, no other dependency.
"""
from __future__ import annotations
from typing import Any, Dict, List

from regime_detector import (
    REGIME_STRONG_UPTREND, REGIME_STRONG_DOWNTREND, REGIME_SIDEWAYS_RANGE,
    REGIME_HIGH_VOLATILITY, REGIME_LOW_VOLATILITY, REGIME_BREAKOUT,
    REGIME_REVERSAL, REGIME_UNCERTAIN, REGIME_UNKNOWN,
)

# Fixed, documented, non-learned multipliers — not ML.
PRIMARY_BOOST_MULTIPLIER = 1.15
LOW_RELEVANCE_MULTIPLIER = 0.5   # discounted, never zeroed — see module docstring

# regime -> {"primary": [...factor names...], "low_relevance": [...factor names...]}
# Factors not listed in either bucket for a regime are left untouched.
DYNAMIC_SELECTION_TABLE: Dict[str, Dict[str, List[str]]] = {
    REGIME_STRONG_UPTREND: {
        "primary": ["obv", "sr", "candle"],
        "low_relevance": ["mean_reversion", "round_number"],
    },
    REGIME_STRONG_DOWNTREND: {
        "primary": ["obv", "sr", "candle"],
        "low_relevance": ["mean_reversion", "round_number"],
    },
    REGIME_SIDEWAYS_RANGE: {
        "primary": ["bb", "mean_reversion", "stoch", "cci", "round_number"],
        "low_relevance": ["obv"],
    },
    REGIME_LOW_VOLATILITY: {
        "primary": ["bb", "mean_reversion", "round_number"],
        "low_relevance": ["wick_rejection", "liquidity_sweep", "false_breakout"],
    },
    REGIME_HIGH_VOLATILITY: {
        "primary": ["wick_rejection", "liquidity_sweep", "false_breakout", "sr"],
        "low_relevance": ["round_number", "exhaustion"],
    },
    REGIME_BREAKOUT: {
        "primary": ["sr", "obv", "candle"],
        "low_relevance": ["bb", "mean_reversion", "round_number"],
    },
    REGIME_REVERSAL: {
        "primary": ["candle", "wick_rejection", "liquidity_sweep", "false_breakout", "rsi_div"],
        "low_relevance": ["obv"],
    },
    # Uncertain/Mixed and Unknown intentionally absent — no selection is
    # made when the regime itself isn't confidently known.
}


def apply_dynamic_indicator_selection(weights: Dict[str, float],
                                       regime_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply the categorical primary/low-relevance selection for the current
    regime on top of `weights` (typically the already regime-weighted
    output of regime_weight_engine.apply_regime_adaptive_weights()).

    Returns:
        {
          "weights": rescaled dict, summing to 100.0 if the input summed to > 0,
          "regime": the regime name used,
          "primary": [factor names boosted],
          "low_relevance": [factor names discounted],
          "applied": bool — False for Uncertain/Mixed/Unknown/unlisted regimes,
          "notes": [explainability strings],
        }
    """
    regime = regime_result.get("regime", REGIME_UNKNOWN)
    table = DYNAMIC_SELECTION_TABLE.get(regime)

    if not table or regime in (REGIME_UNCERTAIN, REGIME_UNKNOWN):
        return {
            "weights": dict(weights),
            "regime": regime,
            "primary": [],
            "low_relevance": [],
            "applied": False,
            "notes": [
                f"Regime '{regime}' -> no dynamic indicator selection applied; "
                "all indicators kept at their current weight."
            ],
        }

    primary = table.get("primary", [])
    low_relevance = table.get("low_relevance", [])

    adjusted: Dict[str, float] = {}
    notes: List[str] = []
    for factor, w in weights.items():
        if factor in primary:
            adjusted[factor] = w * PRIMARY_BOOST_MULTIPLIER
            notes.append(
                f"{factor}: primary indicator for regime '{regime}' -> "
                f"weight x{PRIMARY_BOOST_MULTIPLIER}"
            )
        elif factor in low_relevance:
            adjusted[factor] = w * LOW_RELEVANCE_MULTIPLIER
            notes.append(
                f"{factor}: low relevance in regime '{regime}' -> weight "
                f"x{LOW_RELEVANCE_MULTIPLIER} (discounted, not zeroed — a "
                "factor is never fully silenced by automatic selection)"
            )
        else:
            adjusted[factor] = w

    total = sum(adjusted.values())
    if total > 0:
        adjusted = {k: round(v * 100.0 / total, 4) for k, v in adjusted.items()}
        drift = round(100.0 - sum(adjusted.values()), 4)
        if drift != 0.0:
            top_key = max(adjusted, key=adjusted.get)
            adjusted[top_key] = round(adjusted[top_key] + drift, 4)

    return {
        "weights": adjusted,
        "regime": regime,
        "primary": primary,
        "low_relevance": low_relevance,
        "applied": True,
        "notes": notes,
    }
