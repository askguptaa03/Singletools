
"""Master market-reading engine for Singletools.

Pure analysis layer. It does not fetch data, place orders, mutate settings,
or replace the existing 13-factor analyzer. It converts OHLCV + existing
indicator output into a structured market-reading/playbook decision.

The score returned here is an evidence score, not a calibrated probability.
Only OOS/walk-forward calibration may turn it into a probability estimate.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import json
import math
import numpy as np
import pandas as pd


STRATEGIES = [
    "liquidity_sweep_reversal",
    "breakout_retest_continuation",
    "trend_pullback_continuation",
    "support_resistance_rejection",
    "supply_demand_reaction",
    "false_breakout_trap",
    "compression_expansion",
    "range_extreme_reversal",
    "momentum_continuation",
    "multi_timeframe_alignment",
    "exhaustion_reversal",
    "m_w_reversal",
    "triangle_breakout",
    "flag_pennant_continuation",
    "wedge_channel_break",
    "structure_break_retest",
]

STRATEGY_LABELS = {
    "liquidity_sweep_reversal": "Liquidity Sweep → Reversal",
    "breakout_retest_continuation": "Breakout → Retest → Continuation",
    "trend_pullback_continuation": "Trend Pullback → Continuation",
    "support_resistance_rejection": "Support/Resistance Rejection",
    "supply_demand_reaction": "Supply/Demand Reaction",
    "false_breakout_trap": "False Breakout → Trap",
    "compression_expansion": "Compression → Expansion",
    "range_extreme_reversal": "Range High/Low Reversal",
    "momentum_continuation": "Momentum Continuation",
    "multi_timeframe_alignment": "Multi-Timeframe Alignment",
    "exhaustion_reversal": "Exhaustion → Reversal",
    "m_w_reversal": "M/W Structure Reversal",
    "triangle_breakout": "Triangle Breakout",
    "flag_pennant_continuation": "Flag/Pennant Continuation",
    "wedge_channel_break": "Wedge/Channel Break",
    "structure_break_retest": "Structure Break → Retest",
}

QUALITY_MODES = ("strict", "balanced", "opportunity")
ANALYSIS_MODES = ("13-factor", "selected-strategy", "adaptive")


@dataclass
class PlaybookDecision:
    strategy: str
    direction: str = "WAIT"
    setup_valid: bool = False
    confirmation_a: bool = False
    confirmation_b: bool = False
    confirmation_c: bool = False
    evidence_score: float = 0.0
    reasons: List[str] = None
    blockers: List[str] = None

    def __post_init__(self):
        if self.reasons is None:
            self.reasons = []
        if self.blockers is None:
            self.blockers = []


def _f(v, default=0.0):
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _series(df, col):
    return pd.to_numeric(df[col], errors="coerce") if col in df.columns else pd.Series(np.nan, index=df.index)


def _atr(df, n=14):
    h, l, c = _series(df, "high"), _series(df, "low"), _series(df, "close")
    prev = c.shift(1)
    tr = pd.concat([(h-l), (h-prev).abs(), (l-prev).abs()], axis=1).max(axis=1)
    return tr.rolling(n, min_periods=1).mean()


def _swing_points(df, k=2):
    h, l = _series(df, "high"), _series(df, "low")
    sh = h[(h.shift(k) < h) & (h.shift(-k) < h)]
    sl = l[(l.shift(k) > l) & (l.shift(-k) > l)]
    return sh.dropna(), sl.dropna()


def _structure(df):
    sh, sl = _swing_points(df, 2)
    if len(sh) >= 2 and len(sl) >= 2:
        hh = sh.iloc[-1] > sh.iloc[-2]
        hl = sl.iloc[-1] > sl.iloc[-2]
        lh = sh.iloc[-1] < sh.iloc[-2]
        ll = sl.iloc[-1] < sl.iloc[-2]
        if hh and hl:
            return "BULLISH", ["Higher High + Higher Low"]
        if lh and ll:
            return "BEARISH", ["Lower High + Lower Low"]
    return "RANGE", ["No clean two-leg trend structure"]


def _candle(df):
    o,h,l,c = map(lambda x:_series(df,x), ("open","high","low","close"))
    if len(df) < 2:
        return {"direction":"NEUTRAL","rejection":False,"impulse":False,"exhaustion":False,
                "engulfing":False,"body_ratio":0.0,"close_location":0.5}
    oo,hh,ll,cc = [x.iloc[-1] for x in (o,h,l,c)]
    rng=max(_f(hh-ll),1e-12); body=abs(_f(cc-oo))
    upper=_f(hh-max(oo,cc)); lower=_f(min(oo,cc)-ll)
    loc=_f((cc-ll)/rng,0.5)
    direction="BUY" if cc>oo else "SELL" if cc<oo else "NEUTRAL"
    rejection=(lower/rng>=0.55 and loc>=0.62) or (upper/rng>=0.55 and loc<=0.38)
    impulse=body/rng>=0.65
    prev_o,prev_c=o.iloc[-2],c.iloc[-2]
    engulf=((cc>oo and prev_c<prev_o and cc>=prev_o and oo<=prev_c) or
            (cc<oo and prev_c>prev_o and cc<=prev_o and oo>=prev_c))
    return {"direction":direction,"rejection":rejection,"impulse":impulse,
            "exhaustion":body/rng<0.30 and (upper+lower)/rng>0.60,
            "engulfing":engulf,"body_ratio":round(body/rng,3),
            "close_location":round(loc,3),"upper_wick_ratio":round(upper/rng,3),
            "lower_wick_ratio":round(lower/rng,3)}


def _zones(df):
    c=_series(df,"close"); h=_series(df,"high"); l=_series(df,"low"); atr=_atr(df)
    last=_f(c.iloc[-1])
    window=min(len(df),50)
    lows=l.iloc[-window:]; highs=h.iloc[-window:]
    support=_f(lows.nsmallest(min(3,len(lows))).mean(),last)
    resistance=_f(highs.nlargest(min(3,len(highs))).mean(),last)
    a=max(_f(atr.iloc[-1]),1e-12)
    return {
        "support":support,"resistance":resistance,
        "near_support":abs(last-support)<=1.25*a,
        "near_resistance":abs(last-resistance)<=1.25*a,
        "demand_low":_f(lows.quantile(.15),last),
        "supply_high":_f(highs.quantile(.85),last),
    }


def _liquidity(df):
    if len(df)<8: return {"sweep":False,"direction":"NEUTRAL","level":None}
    h,l,c=_series(df,"high"),_series(df,"low"),_series(df,"close")
    prior_hi=_f(h.iloc[-7:-1].max()); prior_lo=_f(l.iloc[-7:-1].min())
    last_hi,last_lo,last_c=_f(h.iloc[-1]),_f(l.iloc[-1]),_f(c.iloc[-1])
    if last_lo < prior_lo and last_c > prior_lo:
        return {"sweep":True,"direction":"BUY","level":prior_lo}
    if last_hi > prior_hi and last_c < prior_hi:
        return {"sweep":True,"direction":"SELL","level":prior_hi}
    return {"sweep":False,"direction":"NEUTRAL","level":None}


def _range(df):
    h,l,c=_series(df,"high"),_series(df,"low"),_series(df,"close")
    n=min(len(df),30)
    hi=_f(h.iloc[-n:].max()); lo=_f(l.iloc[-n:].min()); last=_f(c.iloc[-1])
    width=max(hi-lo,1e-12)
    pos=(last-lo)/width
    return {"high":hi,"low":lo,"position":pos,"valid":width>0}


def _volatility(df):
    atr=_atr(df); last=max(abs(_f(_series(df,"close").iloc[-1])),1e-12)
    pct=_f(atr.iloc[-1])/last*100
    recent=_f(atr.iloc[-5:].mean()); older=max(_f(atr.iloc[-20:-5].mean()),1e-12)
    return {"atr_pct":pct,"expanding":recent>older*1.12,"compressing":recent<older*.88}


def _pattern(df):
    """Conservative chart-pattern tags. Shape recognition never signals alone."""
    if len(df) < 24:
        return {"name":"none","direction":"NEUTRAL","quality":0,"patterns":[]}
    c=_series(df,"close"); h=_series(df,"high"); l=_series(df,"low")
    x=c.iloc[-24:].to_numpy(float)
    q=max(_f(np.nanmax(x)-np.nanmin(x)),1e-12)
    patterns=[]
    # M / W (double top/bottom) and triple variants.
    left=x[:8]; mid=x[8:16]; right=x[16:]
    top1=float(np.max(left)); top2=float(np.max(right)); bot_mid=float(np.min(mid))
    bot1=float(np.min(left)); bot2=float(np.min(right)); top_mid=float(np.max(mid))
    if abs(top1-top2)/q<.18 and bot_mid < min(top1,top2)-.25*q:
        patterns.append(("M / Double Top","SELL",72))
    if abs(bot1-bot2)/q<.18 and top_mid > max(bot1,bot2)+.25*q:
        patterns.append(("W / Double Bottom","BUY",72))
    if len(x)>=18:
        # Three-touch approximation: comparable extrema in three 6-bar blocks.
        blocks=[x[:8],x[8:16],x[16:]]
        tops=[float(np.max(b)) for b in blocks]; bots=[float(np.min(b)) for b in blocks]
        if max(tops)-min(tops) <= .16*q and min(b for b in bots) < np.mean(tops)-.20*q:
            patterns.append(("Triple Top","SELL",68))
        if max(bots)-min(bots) <= .16*q and max(t for t in tops) > np.mean(bots)+.20*q:
            patterns.append(("Triple Bottom","BUY",68))
    # Head-and-shoulders / inverse: middle extremum dominates shoulders.
    sh,sl=_swing_points(df,2)
    if len(sh)>=3:
        a,b,d=[float(v) for v in sh.iloc[-3:]]
        if b>a*1.002 and b>d*1.002 and abs(a-d)/q<.22:
            patterns.append(("Head & Shoulders","SELL",70))
    if len(sl)>=3:
        a,b,d=[float(v) for v in sl.iloc[-3:]]
        if b<a*.998 and b<d*.998 and abs(a-d)/q<.22:
            patterns.append(("Inverse Head & Shoulders","BUY",70))
    # Triangle / wedge / pennant-like compression.
    h1=_f(h.iloc[-24:-12].max()-h.iloc[-24:-12].min()); h2=_f(h.iloc[-12:].max()-h.iloc[-12:].min())
    l1=_f(l.iloc[-24:-12].max()-l.iloc[-24:-12].min()); l2=_f(l.iloc[-12:].max()-l.iloc[-12:].min())
    if h2<h1*.82 and l2<l1*.82:
        patterns.append(("Triangle / Compression","NEUTRAL",68))
        if abs(h2-l2)/max(h1,l1,1e-12)<.35:
            patterns.append(("Symmetrical Triangle","NEUTRAL",64))
        elif h2<h1*.90:
            patterns.append(("Falling Wedge","BUY",60))
        elif l2<l1*.90:
            patterns.append(("Rising Wedge","SELL",60))
    # Rectangle / channel / flag-like consolidation.
    if h2/h1<1.15 and l2/l1<1.15:
        patterns.append(("Rectangle / Range","NEUTRAL",58))
        if abs(float(np.polyfit(range(12), h.iloc[-12:].to_numpy(float),1)[0])) > 0:
            patterns.append(("Channel / Flag-like Consolidation","NEUTRAL",55))
    # Rounding: monotonic slope changes around a central arc.
    if len(x)>=20:
        d1=float(np.polyfit(range(10),x[:10],1)[0]); d2=float(np.polyfit(range(10),x[-10:],1)[0])
        if d1<0 and d2>0: patterns.append(("Rounding Bottom","BUY",57))
        if d1>0 and d2<0: patterns.append(("Rounding Top","SELL",57))
    if not patterns:
        return {"name":"No dominant chart pattern","direction":"NEUTRAL","quality":35,"patterns":[]}
    primary=max(patterns,key=lambda z:z[2])
    return {"name":primary[0],"direction":primary[1],"quality":primary[2],
            "patterns":[{"name":n,"direction":d,"quality":q} for n,d,q in patterns]}

def _indicator_support(ind, direction):
    vals=[]
    rsi=_f(ind.get("rsi"),50); adx=_f(ind.get("adx"),0)
    ema9=_f(ind.get("ema_9"),0); ema21=_f(ind.get("ema_21"),0); price=_f(ind.get("price"),0)
    if direction=="BUY":
        vals.append(rsi<45 or (ema9>ema21 and price>ema21))
        vals.append(adx>=20)
    elif direction=="SELL":
        vals.append(rsi>55 or (ema9<ema21 and price<ema21))
        vals.append(adx>=20)
    else:
        return 0
    return sum(bool(x) for x in vals)


def _decision(strategy, direction, a, b, c, reasons, blockers):
    if direction not in ("BUY","SELL"): direction="WAIT"
    valid=bool(a and b)
    score=35 + 18*int(a) + 18*int(b) + 10*int(c) + min(12,len(reasons)*2)
    if blockers: score-=min(25,5*len(blockers))
    score=max(0,min(100,score))
    return PlaybookDecision(strategy,direction,valid,bool(a),bool(b),bool(c),score,reasons,blockers)


def _playbook(name, ctx):
    s=ctx["structure"]; zones=ctx["zones"]; liq=ctx["liquidity"]; can=ctx["candle"]
    rng=ctx["range"]; vol=ctx["volatility"]; pat=ctx["pattern"]; ind=ctx["indicators"]
    direction="WAIT"; a=b=c=False; reasons=[]; blockers=[]
    if name=="liquidity_sweep_reversal":
        direction=liq["direction"]; a=liq["sweep"]; b=a and ((can["direction"]==direction) or can["rejection"])
        c=b and bool((zones["near_support"] if direction=="BUY" else zones["near_resistance"]) or pat["name"] in ("W / Double Bottom","M / Double Top"))
        reasons += ["Liquidity sweep/reclaim"] if a else []
        reasons += ["Reversal candle confirms rejection"] if b else []
    elif name=="breakout_retest_continuation":
        h,l,c=_series(ctx["df"],"high"),_series(ctx["df"],"low"),_series(ctx["df"],"close")
        prev_hi=_f(h.iloc[-8:-2].max()); prev_lo=_f(l.iloc[-8:-2].min()); last=_f(c.iloc[-1]); prior=_f(c.iloc[-2])
        direction="BUY" if last>prev_hi else "SELL" if last<prev_lo else "WAIT"
        a=direction!="WAIT"; b=a and ((prior>prev_hi and last>prev_hi) if direction=="BUY" else (prior<prev_lo and last<prev_lo))
        c=b and can["impulse"]
        reasons += ["Structure level broken with a close"] if a else []
        reasons += ["Breakout hold/retest condition"] if b else []
    elif name=="trend_pullback_continuation":
        direction=s; a=s in ("BULLISH","BEARISH")
        b=a and (can["direction"]=="BUY" if s=="BULLISH" else can["direction"]=="SELL")
        c=b and _indicator_support(ind,"BUY" if s=="BULLISH" else "SELL")>=1
        reasons += ["Clear trend structure"] if a else []
        reasons += ["Pullback/continuation candle"] if b else []
    elif name=="support_resistance_rejection":
        direction="BUY" if zones["near_support"] else "SELL" if zones["near_resistance"] else "WAIT"
        a=direction!="WAIT"; b=a and can["rejection"]; c=b and can["close_location"] >= .62 if direction=="BUY" else b and can["close_location"] <= .38
        reasons += ["Price reached a meaningful zone"] if a else []
        reasons += ["Rejection candle"] if b else []
    elif name=="supply_demand_reaction":
        direction="BUY" if zones["near_support"] else "SELL" if zones["near_resistance"] else "WAIT"
        a=direction!="WAIT"; b=a and can["rejection"]; c=b and (pat["quality"]>=55)
        reasons += ["Demand/supply area reached"] if a else []
        reasons += ["Reaction confirmed by candle"] if b else []
    elif name=="false_breakout_trap":
        direction=liq["direction"]; a=liq["sweep"]; b=a and can["rejection"]; c=b and can["engulfing"]
        reasons += ["Failed level break / liquidity trap"] if a else []
        reasons += ["Opposite-side close confirms trap"] if b else []
    elif name=="compression_expansion":
        direction=can["direction"]; a=vol["compressing"]; b=a and vol["expanding"]; c=b and can["impulse"]
        reasons += ["Volatility compression"] if a else []
        reasons += ["Expansion candle"] if b else []
    elif name=="range_extreme_reversal":
        direction="BUY" if rng["position"]<=.18 else "SELL" if rng["position"]>=.82 else "WAIT"
        a=rng["valid"] and direction!="WAIT"; b=a and can["rejection"]; c=b and _indicator_support(ind,direction)>=1
        reasons += ["Established range extreme"] if a else []
        reasons += ["Boundary rejection"] if b else []
    elif name=="momentum_continuation":
        direction="BUY" if s=="BULLISH" else "SELL" if s=="BEARISH" else can["direction"]
        a=direction in ("BUY","SELL") and can["impulse"]; b=a and _indicator_support(ind,direction)>=1; c=b and _f(ind.get("adx"))>=25
        reasons += ["Directional impulse"] if a else []
        reasons += ["Momentum supports continuation"] if b else []
    elif name=="multi_timeframe_alignment":
        direction="BUY" if s=="BULLISH" else "SELL" if s=="BEARISH" else "WAIT"
        mtf=ctx.get("mtf") or {}
        mtf_status=mtf.get("status") if isinstance(mtf,dict) else None
        mtf_signal=mtf.get("primary_signal") if isinstance(mtf,dict) else None
        compare_signal=mtf.get("compare_signal") if isinstance(mtf,dict) else None
        a=direction!="WAIT" and mtf_status=="CONFIRMED"
        b=a and can["direction"]==direction
        c=b and compare_signal==direction and _indicator_support(ind,direction)>=1
        reasons += ["Primary structure bias"] if direction!="WAIT" else []
        reasons += ["Higher timeframe confirms direction"] if a else []
    elif name=="exhaustion_reversal":
        direction="SELL" if s=="BULLISH" else "BUY" if s=="BEARISH" else ("BUY" if rng["position"]>.8 else "SELL" if rng["position"]<.2 else "WAIT")
        a=can["exhaustion"] or (can["body_ratio"]<.35 and can["rejection"]); b=a and can["rejection"]; c=b and (zones["near_support"] if direction=="BUY" else zones["near_resistance"])
        reasons += ["Move shows exhaustion"] if a else []
        reasons += ["Reversal confirmation"] if b else []
    elif name=="m_w_reversal":
        direction=pat["direction"]; a=pat["name"] in ("M / Double Top","W / Double Bottom"); b=a and can["direction"]==direction; c=b and can["rejection"]
        reasons += [pat["name"]] if a else []
        reasons += ["Pattern direction confirmed by candle"] if b else []
    elif name=="triangle_breakout":
        direction=can["direction"]; a=pat["name"]=="Triangle / Compression"; b=a and can["impulse"]; c=b and _indicator_support(ind,direction)>=1
        reasons += ["Triangle/compression detected"] if a else []
        reasons += ["Expansion/breakout candle"] if b else []
    elif name=="flag_pennant_continuation":
        direction="BUY" if s=="BULLISH" else "SELL" if s=="BEARISH" else "WAIT"
        a=direction!="WAIT" and pat["name"]=="Rectangle / Range"; b=a and can["impulse"]; c=b and _indicator_support(ind,direction)>=1
        reasons += ["Trend pause / flag-like consolidation"] if a else []
        reasons += ["Continuation impulse"] if b else []
    elif name=="wedge_channel_break":
        direction=can["direction"]; a=pat["name"]=="Triangle / Compression"; b=a and can["impulse"]; c=b and liq["sweep"]
        reasons += ["Contracting structure"] if a else []
        reasons += ["Break candle"] if b else []
    elif name=="structure_break_retest":
        direction="BUY" if s=="BULLISH" else "SELL" if s=="BEARISH" else "WAIT"
        a=direction!="WAIT"; b=a and can["rejection"]; c=b and (liq["sweep"] or can["engulfing"])
        reasons += ["Structure direction"] if a else []
        reasons += ["Retest/rejection behavior"] if b else []
    return _decision(name,direction,a,b,c,reasons,blockers)


def _performance_bonus(strategy: str, asset: Optional[str], timeframe: str, regime: str = "") -> float:
    """Use only persisted out-of-sample/walk-forward evidence when present.
    Missing evidence gives zero bonus; it never fabricates a win rate.
    """
    candidates = []
    p = Path(__file__).resolve().parent / "master_strategy_performance.json"
    candidates.append(p)
    for key in (f"{asset}|{timeframe}|{regime}", f"*|{timeframe}|{regime}", f"*|{timeframe}|*"):
        try:
            data=json.loads(p.read_text())
            row=data.get(key) or data.get(strategy, {})
            if isinstance(row,dict) and row.get("strategy") not in (None,strategy):
                continue
            n=int(row.get("oos_samples",0) or 0)
            acc=row.get("oos_accuracy")
            if n>=100 and acc is not None:
                # Cap the influence: historical evidence selects among valid setups;
                # it cannot rescue a setup that fails mandatory confirmations.
                return max(-8.0,min(8.0,(_f(acc,50.0)-50.0)*0.16))
        except Exception:
            pass
    return 0.0

def analyze(df: pd.DataFrame, indicators: Optional[Dict[str,Any]]=None,
            timeframe: str="1m", mode: str="adaptive",
            strategy: Optional[str]=None, quality_mode: str="balanced",
            asset: Optional[str]=None, mtf: Optional[Dict[str,Any]]=None) -> Dict[str,Any]:
    """Run chart reading + playbook selection. Existing 13-factor engine remains separate."""
    indicators=indicators or {}
    mode=str(mode or "adaptive").lower()
    mode_map={"13-factor":"13-factor","selected":"selected-strategy","selected-strategy":"selected-strategy",
              "adaptive":"adaptive"}
    mode=mode_map.get(mode, "adaptive")
    quality_mode=str(quality_mode or "balanced").lower()
    if quality_mode not in QUALITY_MODES: quality_mode="balanced"
    if df is None or len(df)<25:
        return {"signal":"WAIT","confidence_score":0,"strategy":None,"mode":mode,
                "quality_mode":quality_mode,"wait_reason":"Insufficient closed candles for master chart reading."}

    # 30s is allowed only if timestamps actually form genuine 30-second candles.
    cadence_ok=True; cadence_reason=None
    try:
        ts=pd.to_datetime(df.index)
        diffs=ts.to_series().diff().dt.total_seconds().dropna()
        if timeframe=="30s" and (len(diffs)<5 or abs(float(diffs.median())-30)>2):
            cadence_ok=False; cadence_reason="30s requested but feed timestamps do not show genuine 30-second candles."
    except Exception:
        pass

    ctx={"df":df,"indicators":indicators,"structure":_structure(df)[0],
         "zones":_zones(df),"liquidity":_liquidity(df),"candle":_candle(df),
         "range":_range(df),"volatility":_volatility(df),"pattern":_pattern(df),
         "mtf": mtf or {}}
    candidates=[]
    names=[strategy] if mode=="selected-strategy" and strategy in STRATEGIES else STRATEGIES
    if mode=="13-factor":
        return {"signal":"WAIT","confidence_score":None,"strategy":None,"mode":mode,
                "quality_mode":quality_mode,"wait_reason":"13-Factor mode selected; Master Playbook layer is bypassed.",
                "chart_reading":_chart_payload(ctx),"playbooks":[]}
    for n in names:
        if n in STRATEGIES:
            candidates.append(_playbook(n,ctx))
    valid=[x for x in candidates if x.setup_valid and x.confirmation_a and x.confirmation_b]
    # Adaptive selection is evidence-based within the current chart context, not random.
    regime_name=ctx["structure"]
    ranked=[]
    for x in valid:
        bonus=_performance_bonus(x.strategy, asset, timeframe, regime_name)
        ranked.append((x,bonus))
    ranked.sort(key=lambda z:(z[0].evidence_score+z[1], int(z[0].confirmation_c)), reverse=True)
    valid=[x for x,_ in ranked]
    chosen=valid[0] if valid else None
    if chosen is None:
        reason="; ".join(sorted(set(sum((x.blockers for x in candidates),[])))) or "No playbook has two aligned mandatory confirmations."
        return {"signal":"WAIT","confidence_score":max([x.evidence_score for x in candidates],default=0),
                "strategy":None,"mode":mode,"quality_mode":quality_mode,"wait_reason":reason,
                "chart_reading":_chart_payload(ctx),
                "playbooks":[asdict(x) for x in candidates]}
    if not cadence_ok:
        return {"signal":"WAIT","confidence_score":chosen.evidence_score,"strategy":chosen.strategy,"mode":mode,
                "quality_mode":quality_mode,"wait_reason":cadence_reason,
                "chart_reading":_chart_payload(ctx),"playbooks":[asdict(x) for x in candidates]}
    # Quality modes alter acceptance only after mandatory confirmations exist.
    threshold={"strict":72,"balanced":58,"opportunity":50}[quality_mode]
    if chosen.evidence_score < threshold:
        return {"signal":"WAIT","confidence_score":chosen.evidence_score,"strategy":chosen.strategy,"mode":mode,
                "quality_mode":quality_mode,"wait_reason":f"Setup evidence score {chosen.evidence_score:.0f} is below {quality_mode} quality threshold {threshold}.",
                "chart_reading":_chart_payload(ctx),"playbooks":[asdict(x) for x in candidates]}
    # A meaningful third confirmation raises evidence; a strong opposite candle blocks.
    opposite = (chosen.direction=="BUY" and ctx["candle"]["direction"]=="SELL" and ctx["candle"]["impulse"]) or \
               (chosen.direction=="SELL" and ctx["candle"]["direction"]=="BUY" and ctx["candle"]["impulse"])
    if opposite:
        return {"signal":"WAIT","confidence_score":max(0,chosen.evidence_score-12),"strategy":chosen.strategy,"mode":mode,
                "quality_mode":quality_mode,"wait_reason":"Strong opposite impulse candle contradicts the playbook.",
                "chart_reading":_chart_payload(ctx),"playbooks":[asdict(x) for x in candidates]}
    score=chosen.evidence_score + (8 if chosen.confirmation_c else 0)
    return {"signal":chosen.direction,"confidence_score":round(min(100,score),1),
            "strategy":chosen.strategy,"strategy_label":STRATEGY_LABELS[chosen.strategy],
            "mode":mode,"quality_mode":quality_mode,
            "confirmations":{"mandatory":[chosen.confirmation_a,chosen.confirmation_b],"optional_third":chosen.confirmation_c},
            "reasons":chosen.reasons,"wait_reason":None,
            "chart_reading":_chart_payload(ctx),"playbooks":[asdict(x) for x in candidates],
            "confidence_basis":"Evidence score only; not a win-probability estimate until OOS calibration.",
            "historical_evidence_used": bool(any(_performance_bonus(x.strategy, asset, timeframe, regime_name) != 0 for x in valid))}


def _chart_payload(ctx):
    can=ctx["candle"]; return {
        "structure":ctx["structure"],"pattern":ctx["pattern"],
        "zones":ctx["zones"],"liquidity":ctx["liquidity"],
        "candle_psychology":can,"volatility":ctx["volatility"],
        "range":ctx["range"],"mtf":ctx.get("mtf") or {},
    }


def strategy_catalog():
    return [{"id":k,"name":STRATEGY_LABELS[k]} for k in STRATEGIES]
