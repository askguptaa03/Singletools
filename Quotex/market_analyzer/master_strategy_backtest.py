"""Offline OOS / walk-forward evaluator for Master Market-Reading playbooks.

Input: OHLCV CSV with a datetime index column (or a `timestamp` column).
Output: JSON-ready per-strategy OOS statistics. This never changes live signals.
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict
import pandas as pd
from master_engine import STRATEGIES, analyze


def _load(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    ts = "timestamp" if "timestamp" in df.columns else df.columns[0]
    df[ts] = pd.to_datetime(df[ts], utc=True, errors="coerce")
    df = df.dropna(subset=[ts]).set_index(ts).sort_index()
    for c in ("open", "high", "low", "close", "volume"):
        if c not in df.columns:
            if c == "volume":
                df[c] = 0.0
            else:
                raise ValueError(f"Missing required OHLC column: {c}")
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna(subset=["open", "high", "low", "close"])


def walk_forward_report(df: pd.DataFrame, timeframe: str, horizon: int = 1,
                        warmup: int = 80, oos_start: float = 0.60,
                        min_samples: int = 30) -> Dict[str, Any]:
    n = len(df)
    start = max(warmup, int(n * oos_start))
    out = {}
    for strategy in STRATEGIES:
        wins = losses = signals = 0
        for i in range(start, n - horizon):
            hist = df.iloc[:i].tail(warmup)
            r = analyze(hist, indicators={}, timeframe=timeframe,
                         mode="selected-strategy", strategy=strategy,
                         quality_mode="balanced")
            sig = r.get("signal")
            if sig not in ("BUY", "SELL"):
                continue
            signals += 1
            now = float(df["close"].iloc[i-1])
            future = float(df["close"].iloc[i+horizon-1])
            ok = (sig == "BUY" and future > now) or (sig == "SELL" and future < now)
            if ok: wins += 1
            else: losses += 1
        acc = (wins / signals * 100.0) if signals else None
        out[strategy] = {
            "strategy": strategy,
            "oos_samples": signals,
            "oos_wins": wins,
            "oos_losses": losses,
            "oos_accuracy": round(acc, 2) if acc is not None else None,
            "eligible_for_adaptive_bonus": bool(signals >= min_samples),
            "timeframe": timeframe,
        }
    return {"schema_version": 1, "timeframe": timeframe,
            "horizon_candles": horizon, "oos_start": oos_start,
            "results": out}


def save_report(report: Dict[str, Any], path: str) -> None:
    Path(path).write_text(json.dumps(report, indent=2), encoding="utf-8")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("csv")
    ap.add_argument("--timeframe", default="1m")
    ap.add_argument("--horizon", type=int, default=1)
    ap.add_argument("--output", default="master_strategy_performance.json")
    args = ap.parse_args()
    report = walk_forward_report(_load(args.csv), args.timeframe, args.horizon)
    # The live engine reads a flat performance map. Keep the raw report too.
    flat = {}
    for sid, row in report["results"].items():
        flat[sid] = row
    save_report(flat, args.output)
    print(json.dumps(report, indent=2))
