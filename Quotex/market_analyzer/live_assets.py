"""
Live Quotex Asset Availability layer (additive, read-only, new file).

Single source of truth for "which OTC assets are currently tradeable on
Quotex right now". Wraps the EXISTING QuotexDataFetcher.get_available_assets()
(market_analyzer/fetch_data.py) -> AsyncQuotexClient.get_available_assets()
(quotex/api_quotex/client.py, untouched) via the project's EXISTING shared
fetcher/session (app.py's _get_shared_fetcher()) — no second Quotex login,
no new WebSocket/connection logic, no changes to client.py or any other
Quotex API internals.

api_quotex/constants.py's static ASSETS dict remains the KNOWN/REFERENCE
universe only (used for backtest/historical work, which does not depend on
live availability) — this module NEVER treats that static list as
"currently available". Availability always comes from a live client call;
if that call fails or the session is unavailable, functions here return
empty/False rather than falling back to the static list or fabricating a
result.

Public interface:
  get_live_assets(fetcher, force_refresh=False)           -> {symbol: info}
  get_live_otc_assets(fetcher, force_refresh=False)        -> {symbol: info}  (open + OTC only)
  is_asset_available(fetcher, asset, force_refresh=False)  -> bool
  build_snapshot(known_universe, live_assets)               -> reporting dict
"""
from __future__ import annotations

import time
from typing import Any, Dict, List

# Short in-process cache: the underlying AsyncQuotexClient already keeps its
# own asset snapshot current via the live 'assets_list' websocket event, so
# this cache exists only to avoid redundant awaits/dict work across a burst
# of near-simultaneous Flask requests (e.g. several UI panels refreshing at
# once) — not to paper over staleness. force_refresh=True always bypasses it.
_CACHE_TTL_SECONDS = 5.0
_cache: Dict[str, Any] = {"timestamp": 0.0, "assets": {}}


def _now() -> float:
    return time.monotonic()


async def get_live_assets(fetcher, force_refresh: bool = False) -> Dict[str, Dict[str, Any]]:
    """
    Full live snapshot straight from the existing, authenticated Quotex
    session — id/name/type/payout/is_otc/is_open/available_timeframes per
    symbol, exactly as the Quotex API returns it. No fabrication: if the
    underlying call fails, isn't connected, or returns nothing, this
    returns {} — callers must treat that as "unknown / session issue",
    never silently substitute stale or static data.
    """
    global _cache
    if not force_refresh and (_now() - _cache["timestamp"]) < _CACHE_TTL_SECONDS and _cache["assets"]:
        return _cache["assets"]
    try:
        assets = await fetcher.get_available_assets()
    except Exception:
        # QuotexDataFetcher.get_available_assets() already catches its own
        # exceptions and returns {} — this is a second, defensive layer so
        # this function's own "no fabrication on failure" guarantee holds
        # even if it's ever called against a different fetcher/session
        # object that doesn't have that same internal guard.
        assets = {}
    assets = assets if isinstance(assets, dict) else {}
    _cache = {"timestamp": _now(), "assets": assets}
    return assets


def filter_otc(assets: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Every OTC symbol Quotex returned this snapshot, open OR closed."""
    return {sym: info for sym, info in assets.items() if info.get("is_otc")}


def filter_available(assets: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Only symbols Quotex reports as currently open (is_open truthy)."""
    return {sym: info for sym, info in assets.items() if info.get("is_open")}


async def get_live_otc_assets(fetcher, force_refresh: bool = False) -> Dict[str, Dict[str, Any]]:
    """Currently-open OTC assets only — the live scan/selection universe."""
    all_assets = await get_live_assets(fetcher, force_refresh=force_refresh)
    return filter_available(filter_otc(all_assets))


async def is_asset_available(fetcher, asset: str, force_refresh: bool = False) -> bool:
    """True only if Quotex's live snapshot has this exact symbol AND marks it open."""
    all_assets = await get_live_assets(fetcher, force_refresh=force_refresh)
    info = all_assets.get(asset)
    return bool(info and info.get("is_open"))


def filter_requested_against_live(requested: List[str], live_otc_symbols) -> Dict[str, List[str]]:
    """
    Finding B fix — the caller-supplied "assets" override (e.g. an explicit
    request body to /api/scanner/start or /api/validation/run) must not be
    able to bypass live availability. Intersects every OTC-suffixed symbol
    in `requested` against `live_otc_symbols` (the current live snapshot);
    an OTC symbol not currently live-available is dropped, never scanned/
    validated. Non-OTC symbols (e.g. regular "LIVE" market pairs) are
    outside this live-availability system's scope — same OTC-only scope as
    every other function in this module — and pass through unfiltered,
    since there is no live discovery source for them here to check against.

    Returns {"kept": [...], "dropped_otc_unavailable": [...]} — `kept` is
    the actual scan/validation target; `dropped_otc_unavailable` is purely
    informational (surfaced to the caller so a removed/renamed/closed asset
    is never silently vanished without explanation).
    """
    live_set = set(live_otc_symbols)
    kept: List[str] = []
    dropped: List[str] = []
    for asset in requested:
        if asset.lower().endswith("_otc"):
            if asset in live_set:
                kept.append(asset)
            else:
                dropped.append(asset)
        else:
            kept.append(asset)
    return {"kept": kept, "dropped_otc_unavailable": dropped}


def build_snapshot(known_universe: List[str], live_assets: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Read-only reporting structure for /api/assets/live and for the
    scanner's per-run snapshot (Step 6). Classifies every OTC symbol
    Quotex actually returned as available/closed, and separately flags
    known-universe OTC symbols Quotex did NOT return at all this snapshot
    (e.g. delisted/renamed) — nothing is silently dropped in either
    direction, and the static `known_universe` never overrides what
    Quotex actually reported as available.
    """
    otc_all = filter_otc(live_assets)          # every OTC symbol Quotex returned, open or closed
    otc_available = filter_available(otc_all)  # currently tradeable only
    known_otc = [s for s in known_universe if s.lower().endswith("_otc")]
    known_otc_set = set(known_otc)

    entries: List[Dict[str, Any]] = []
    seen = set()
    for sym, info in sorted(otc_all.items()):
        entries.append({
            "asset": sym,
            "name": info.get("name"),
            "is_open": bool(info.get("is_open")),
            "payout": info.get("payout"),
            "in_known_universe": sym in known_otc_set,
        })
        seen.add(sym)
    # Known-universe OTC symbols Quotex didn't return at all this snapshot —
    # explicitly surfaced as unavailable, not silently dropped.
    for sym in known_otc:
        if sym not in seen:
            entries.append({
                "asset": sym, "name": None, "is_open": False,
                "payout": None, "in_known_universe": True,
                "not_returned_by_quotex": True,
            })

    return {
        "known_otc_count": len(known_otc),
        "live_otc_total_count": len(otc_all),
        "available_otc_count": len(otc_available),
        "available_otc_assets": sorted(otc_available.keys()),
        "entries": entries,
    }


# ── Phase 15 (Part 1): unified live catalog — ADDITIVE ONLY ────────────────
# Everything below is new and does not modify any function above. It reuses
# the exact same live data source (get_live_assets(), the existing shared
# fetcher/session — no second login, no new Quotex connection code) and
# adds category classification ACROSS ALL asset types Quotex returns, not
# just OTC. build_snapshot() / filter_otc() / filter_available() / the
# existing /api/assets/live route contract are all left byte-for-byte
# unchanged so nothing that currently depends on them can regress.
#
# IMPORTANT HONESTY NOTE: the raw Quotex "type" string (asset[3] in the
# protocol row, captured by client.py as info["type"]) has not been
# observed from a live session in this environment — there is no network
# access here to confirm its exact values. The keyword-based mapping below
# is a best-effort classification of whatever string Quotex actually
# sends, NOT a verified 1:1 mapping to a documented Quotex taxonomy. Any
# type string that does not match a known keyword pattern is classified as
# "other" rather than being dropped or silently miscategorized — no asset
# is ever discarded because its type wasn't recognized.
_CATEGORY_KEYWORDS: List[tuple] = [
    ("Crypto", ("crypto", "coin", "btc", "eth")),
    ("Commodities", ("commodity", "commodities", "metal", "oil", "gold", "silver", "energy")),
    ("Stocks", ("stock", "share", "equity")),
    ("Indices", ("index", "indices", "indice")),
    ("Forex", ("forex", "currency", "fx")),
]


def classify_category(info: Dict[str, Any]) -> str:
    """
    Best-effort category for one asset's info dict (see honesty note
    above). OTC-ness is a separate, already-reliable flag (info["is_otc"],
    populated directly from the symbol's "_otc" suffix — not a guess) and
    is intentionally NOT one of the mutually-exclusive buckets here: an
    OTC asset still gets its underlying instrument-type category (e.g. an
    OTC forex pair is still "Forex"), so callers that want an "OTC" facet
    should check info.get("is_otc") independently, exactly like the
    existing filter_otc() does.
    """
    raw_type = str(info.get("type") or "").strip().lower()
    for category, keywords in _CATEGORY_KEYWORDS:
        if any(kw in raw_type for kw in keywords):
            return category
    return "Other"


def build_catalog_snapshot(live_assets: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Unified live catalog (Phase 15 Part 1) — covers every asset Quotex's
    live session actually returned this snapshot, not just OTC. Category
    counts are for the CURRENTLY OPEN subset only (matching the existing
    OTC snapshot's "available" semantics) — never a static/fabricated
    count. `is_otc` is preserved per-entry so a caller can further split
    by OTC vs non-OTC within any category if needed.
    """
    open_assets = filter_available(live_assets)

    category_counts: Dict[str, int] = {}
    entries: List[Dict[str, Any]] = []
    for sym, info in sorted(open_assets.items()):
        category = classify_category(info)
        category_counts[category] = category_counts.get(category, 0) + 1
        entries.append({
            "asset": sym,               # raw internal Quotex symbol — never lost/renamed
            "name": info.get("name"),
            "category": category,
            "is_otc": bool(info.get("is_otc")),
            "is_open": True,            # this list is already open-filtered
            "payout": info.get("payout"),
        })

    return {
        "total_available": len(open_assets),
        "category_counts": category_counts,
        "assets": entries,
    }
