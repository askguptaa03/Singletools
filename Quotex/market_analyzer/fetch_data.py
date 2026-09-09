"""
Fetch market data from Quotex using AsyncQuotexClient.
Handles session loading, connection, candle history, and live streaming.
NEVER places orders — data retrieval only.
"""

from __future__ import annotations
import sys
import os
import json
import time
import asyncio
from pathlib import Path
from typing import List, Optional, Callable, Any

import pandas as pd

# ── Path setup ──────────────────────────────────────────────────────────────
_MARKET_DIR = Path(__file__).resolve().parent       # market_analyzer/
_ROOT       = _MARKET_DIR.parent                    # workspace root
_QUOTEX_DIR = _ROOT / "quotex"                      # quotex/

# Insert quotex/ so that `import api_quotex` works
if str(_QUOTEX_DIR) not in sys.path:
    sys.path.insert(0, str(_QUOTEX_DIR))

# Insert market_analyzer/ so sibling modules (config, indicators, analyzer) resolve
if str(_MARKET_DIR) not in sys.path:
    sys.path.insert(0, str(_MARKET_DIR))

# ── Imports that depend on path setup ────────────────────────────────────────
from api_quotex import AsyncQuotexClient
from api_quotex.models import Candle
from api_quotex.utils import sanitize_symbol  # diagnostic-only: mirrors client.py's own
from api_quotex.constants import TIMEFRAMES as _TF_SECONDS_MAP
                                               # normalization so we can SHOW callers what
                                               # identifier will actually be used, without
                                               # changing that logic (which lives in the
                                               # protected client.py and is untouched here).
import config as cfg


# ─── Session loading ─────────────────────────────────────────────────────────

def load_session_ssid() -> str:
    """
    Load the SSID.  Priority order:
      1. QUOTEX_SSID environment variable  (Replit Secrets / deployment env)
      2. quotex/session.json               (written by quotex/login.py)
    Raises FileNotFoundError / ValueError with actionable fix instructions.
    SAFETY: never prints or logs the SSID value — only its length.
    """
    # ── Priority 1: QUOTEX_SSID environment variable ────────────────────────
    env_ssid = os.environ.get("QUOTEX_SSID", "").strip()
    if env_ssid:
        if env_ssid == "[object Storage]" or len(env_ssid) < 10:
            raise ValueError(
                f"QUOTEX_SSID env var is set but looks invalid "
                f"(length={len(env_ssid)}). "
                "Paste the full SSID value from Quotex DevTools."
            )
        print(f"  → SSID loaded from QUOTEX_SSID env var "
              f"(length: {len(env_ssid)})")   # value never printed
        return env_ssid

    # ── Priority 2: session.json fallback ───────────────────────────────────
    session_path = _ROOT / cfg.SESSION_FILE
    if not session_path.exists():
        raise FileNotFoundError(
            f"\n[ERROR] Session file not found: {session_path}\n"
            "  Option A (recommended): Set QUOTEX_SSID in Replit Secrets.\n"
            "  Option B: Run:  python quotex/login.py\n"
            "  Then try again."
        )
    try:
        data = json.loads(session_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"Session file is corrupted ({e}). Re-run login.py.")

    ssid = data.get("ssid", "").strip()
    if not ssid or ssid == "[object Storage]" or len(ssid) < 10:
        raise ValueError(
            f"\n[ERROR] Invalid SSID in {session_path}\n"
            "  Option A (recommended): Set QUOTEX_SSID in Replit Secrets.\n"
            "  Option B: Run:  python quotex/login.py   and log in again."
        )
    print(f"  → SSID loaded from session.json "
          f"(length: {len(ssid)})")          # value never printed
    return ssid


# ─── Data fetcher ────────────────────────────────────────────────────────────

class QuotexDataFetcher:
    """
    Connects to Quotex, downloads historical candles, and optionally streams
    live price updates.  Does NOT place any orders.
    """

    def __init__(self, asset: str = cfg.DEFAULT_ASSET,
                 is_demo: bool = cfg.IS_DEMO):
        self.asset   = asset
        self.is_demo = is_demo
        self._client: Optional[AsyncQuotexClient] = None
        self._live_prices: List[dict] = []
        # Candle-fetch reliability sequence (Phase 3 fix): the diagnostic
        # object from the most recent get_candles() call, populated on
        # both success and failure. Callers (Analyzer/Scanner) can read
        # this after a call to see exactly what was checked. NEVER
        # contains the SSID or any secret value — only status labels.
        self.last_fetch_diagnostics: dict = {}

    # ── Diagnostics ──────────────────────────────────────────────────────────

    def get_candle_transport_diagnostics(self) -> dict:
        """Return secret-free WebSocket transport diagnostics from the client."""
        try:
            if self._client is None:
                return {"pending_binary_events": [], "recent_trace": [], "trace_count": 0, "last_event": None}
            return dict(self._client.get_candle_transport_diagnostics() or {})
        except Exception:
            return {"pending_binary_events": [], "recent_trace": [], "trace_count": 0, "last_event": None}

    def get_recent_unmatched_candle_responses(self, since_ts: float = 0.0) -> list:
        try:
            if self._client is None:
                return []
            return list(self._client.get_recent_unmatched_candle_responses(since_ts))
        except Exception:
            return []

    def get_recent_uncorrelated_candle_errors(self, since_ts: float = 0.0) -> list:
        try:
            if self._client is None:
                return []
            return list(self._client.get_recent_uncorrelated_candle_errors(since_ts))
        except Exception:
            return []

    # ── Connection ───────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Load session SSID and open the WebSocket connection."""
        ssid = load_session_ssid()   # prints source + length (never the value)

        self._client = AsyncQuotexClient(
            ssid=ssid,
            is_demo=self.is_demo,
            enable_logging=True,    # enable existing library candle diagnostics; secrets are not logged
        )
        print("  → Connecting to Quotex WebSocket …")
        connected = await self._client.connect()
        if not connected:
            raise ConnectionError(
                # TASK 5: clear session-expiry guidance
                "Failed to connect to Quotex — session may be expired.\n"
                "  Fix option 1 (DevTools): Open browser → quotex.io → DevTools (F12) → "
                "Network tab → WS filter → find WebSocket request → "
                "copy 'ssid' value from URL or payload → paste into quotex/session.json\n"
                "  Fix option 2 (auto-login): Re-run:  bash quotex/run_login.sh"
            )
        print("  → Connected ✓")
        # Give the server a brief moment to push the initial instruments/list frame.
        # 0.5 s is enough over a normal WebSocket round-trip; the old 2 s was
        # a conservative guess and added unnecessary latency to every request.
        await asyncio.sleep(0.5)

    async def disconnect(self) -> None:
        """Close the WebSocket connection cleanly."""
        if self._client:
            try:
                await self._client.disconnect()
            except Exception:
                pass
            self._client = None

    # ── Candles (history) ────────────────────────────────────────────────────

    async def get_candles(self, asset: str | None = None,
                          timeframe: str | None = None,
                          count: int | None = None,
                          *, max_retries: int = 2) -> List[Candle]:
        """
        Fetch historical OHLCV candles from Quotex.
        Uses AsyncQuotexClient.get_candles() which reuses the existing
        WebSocket protocol (instruments/follow + instruments/update +
        chart_notification/get) and internal _on_candles_received handler.

        Candle-fetch reliability sequence (Phase 3 fix): a single empty
        response no longer immediately gives up. On empty/failed:
          1. re-check current live asset availability (existing
             get_available_assets() — same live registry Analyzer/Scanner
             use; never a static fallback)
          2. verify the requested asset symbol against that live snapshot
          3. verify the requested timeframe (an omitted timeframe is
             backfilled from the existing config default, same as before;
             the diagnostic always reflects the actual resolved value)
          4. retry with a controlled (exponential) backoff
          5. if the WebSocket looks dead, reconnect using the EXISTING
             connect()/disconnect() session architecture — no new auth
             mechanism, no new reconnection logic invented — then retry
          6. only after that full sequence still fails is a genuine,
             structured failure returned

        Never silently substitutes another asset. Never falls back to a
        static asset list. Never pretends candles were received when they
        were not — on failure this returns [] exactly as before; the
        diagnostic detail is additive, in self.last_fetch_diagnostics.
        """
        asset     = asset     or self.asset
        timeframe = timeframe or cfg.PRIMARY_TIMEFRAME
        count     = count     or cfg.CANDLE_COUNT

        # Mirrors the EXACT normalization client.py's own get_candles()
        # and get_available_assets() apply internally — computed here only
        # to make the identifier flow traceable in diagnostics and to fix
        # a real bug: the availability re-check below must compare against
        # this SAME normalized form, not the raw caller-supplied string,
        # or a live asset can be wrongly declared "not available" purely
        # because of an incidental formatting difference (case, whitespace)
        # between what the caller passed and the registry's canonical key.
        normalized_symbol = sanitize_symbol(asset).replace("_OTC", "_otc")

        diag = {
            "requested_symbol":       asset,
            "normalized_symbol":      normalized_symbol,
            "live_registry_symbol":   None,
            "api_request_identifier": normalized_symbol,
            "timeframe":              timeframe,
            "requested_candle_count": count,
            "session_status":         "unknown",
            "websocket_status":       "unknown",
            "availability_status":    "unknown",
            "raw_response_type":      None,
            "raw_response_count":     None,
            "retry_attempts":         0,
            # One of: "protocol_error", "timeout", "unavailable",
            # "empty_response", "connection_error", or None on success.
            # Populated below from the approved client.py error-
            # correlation mechanism wherever that signal is available;
            # never guessed.
            "failure_category":       None,
            "failure_reason":         None,
            # New diagnostics (4D): whether the broker's own asset row
            # confirmed a timeframe list, and what it was, when known.
            "available_timeframes_known":       False,
            "advertised_available_timeframes":  None,
        }
        self.last_fetch_diagnostics = diag

        if not self._client:
            diag["session_status"] = "not_connected"
            diag["websocket_status"] = "not_connected"
            diag["failure_category"] = "connection_error"
            diag["failure_reason"] = "connection_error: not connected. Call connect() first."
            raise RuntimeError("Not connected. Call connect() first.")

        diag["session_status"] = "connected"
        # Timeframe verification (step 3): an omitted timeframe/asset/count
        # is intentionally backfilled from cfg defaults above (pre-existing,
        # unchanged behavior) — that's a valid request, not a failure. What
        # IS verified here is that the diagnostic always reflects the
        # actual, RESOLVED timeframe that will be requested, never a stale
        # or unresolved value — that's what a caller diagnosing a failure
        # actually needs to see.
        diag["timeframe"] = timeframe

        # Step 4A: pre-flight timeframe-support check, at the SAME shared
        # layer both Manual Analyzer and Scanner go through (this function),
        # so the protection applies to both, not just the /api/signal route.
        # Only blocks when the broker's OWN asset row explicitly confirmed a
        # timeframe list that does not include the requested one — missing
        # or unconfirmed metadata is never treated as proof of anything,
        # and the request proceeds to the existing retry logic unchanged.
        try:
            avail_pre = await self.get_available_assets()
        except Exception:
            avail_pre = {}
        asset_info_pre = avail_pre.get(normalized_symbol) if avail_pre else None
        if asset_info_pre:
            tf_confirmed_pre = bool(asset_info_pre.get("available_timeframes_confirmed"))
            tf_list_pre = asset_info_pre.get("available_timeframes") or []
            diag["available_timeframes_known"] = tf_confirmed_pre
            diag["advertised_available_timeframes"] = tf_list_pre if tf_confirmed_pre else None
            tf_secs_pre = _TF_SECONDS_MAP.get(timeframe)
            if tf_confirmed_pre and tf_secs_pre is not None and tf_secs_pre not in tf_list_pre:
                diag["failure_category"] = "unsupported_timeframe"
                diag["failure_reason"] = (
                    f"unsupported_timeframe: asset '{normalized_symbol}' does not support the "
                    f"'{timeframe}' timeframe on Quotex right now "
                    f"(broker-confirmed available timeframes: {tf_list_pre})."
                )
                diag["availability_status"] = "available"  # asset itself is known/available
                print(f"  ⚠  {diag['failure_reason']}")
                return []

        attempt = 0
        while attempt <= max_retries:
            diag["websocket_status"] = (
                "connected" if getattr(self._client, "websocket_is_connected", False)
                else "disconnected"
            )
            print(f"  → Fetching {count} candles [{timeframe}] for {asset} "
                  f"(normalized: {normalized_symbol}, attempt {attempt + 1}/{max_retries + 1}) …")
            attempt_started_at = time.time()
            try:
                # asset (raw) is passed through unchanged — client.py's
                # get_candles() applies the identical sanitize_symbol()
                # normalization internally; normalized_symbol above is
                # computed here purely for diagnostic visibility, not to
                # change what's actually requested.
                candles = await self._client.get_candles(asset, timeframe, count)
            except Exception as exc:  # noqa: BLE001 — recorded, sequence continues
                candles = []
                diag["failure_reason"] = f"exception during candle fetch: {exc}"

            diag["raw_response_type"] = type(candles).__name__
            diag["raw_response_count"] = len(candles) if hasattr(candles, "__len__") else None

            if candles:
                print(f"  → Received {len(candles)} candles ✓")
                diag["failure_category"] = None
                diag["failure_reason"] = None
                diag["availability_status"] = "available"
                return candles

            # Approved general fix — consume the correlated outcome
            # client.py's _on_error()/_request_candles() recorded for THIS
            # exact (asset, timeframe) request, if any. Distinguishes a
            # real server-side protocol error from a plain timeout from a
            # genuinely-empty-but-valid response — never guessed, only
            # ever what the server/client actually reported for this
            # specific request.
            client_outcome = None
            try:
                if hasattr(self._client, "get_last_candle_error"):
                    client_outcome = self._client.get_last_candle_error(asset, timeframe)
            except Exception:
                client_outcome = None

            if client_outcome:
                kind = client_outcome.get("kind")
                if kind == "protocol_error":
                    diag["failure_category"] = "protocol_error"
                    err_detail = client_outcome.get("error") or client_outcome.get("message") or "unspecified"
                    diag["failure_reason"] = f"protocol_error: Quotex reported an error for this request: {err_detail}"
                elif kind == "timeout":
                    diag["failure_category"] = "timeout"
                    diag["failure_reason"] = "timeout: no response received from Quotex within the request window."
                elif kind == "empty_response":
                    diag["failure_category"] = "empty_response"
                    diag["failure_reason"] = "empty_response: Quotex responded successfully but returned zero candles."
            else:
                # No directly-correlated outcome available (older client,
                # or nothing recorded yet) — check for a recent protocol
                # error that couldn't be tied to a specific request, as a
                # soft, clearly-labeled hint only. Never used to short-
                # circuit retries or override a more specific category.
                try:
                    if hasattr(self._client, "get_recent_uncorrelated_candle_errors"):
                        uncorrelated = self._client.get_recent_uncorrelated_candle_errors(attempt_started_at)
                        if uncorrelated and not diag["failure_reason"]:
                            latest = uncorrelated[-1]
                            diag["failure_category"] = "protocol_error"
                            diag["failure_reason"] = (
                                "protocol_error (uncorrelated — could not be tied to this exact request): "
                                f"{latest.get('error') or latest.get('message') or 'unspecified'}"
                            )
                except Exception:
                    pass

            diag["retry_attempts"] = attempt + 1

            # Step 1 + 2: re-check live availability, verify the NORMALIZED
            # asset symbol against it (bug fix — previously compared the
            # raw, un-normalized string, which could falsely read as
            # "not available" for a genuinely live asset whenever the
            # caller's string didn't already exactly match the registry's
            # own canonical key form). Same live registry Analyzer/Scanner
            # use (get_available_assets() — never a static list).
            try:
                avail = await self.get_available_assets()
            except Exception:
                avail = {}
            if avail:
                asset_info_retry = avail.get(normalized_symbol)
                is_live = asset_info_retry is not None
                diag["availability_status"] = "available" if is_live else "not_available"
                diag["live_registry_symbol"] = normalized_symbol if is_live else None
                if is_live:
                    diag["is_open"] = bool(asset_info_retry.get("is_open"))
            else:
                diag["availability_status"] = "unknown (live discovery unavailable)"

            if diag["availability_status"] == "not_available":
                # Genuinely not a live asset right now (checked against the
                # NORMALIZED form) — retrying won't help, and retrying
                # would waste time without changing the outcome. This is a
                # definitive, retry-pointless terminal state — takes
                # priority over any softer protocol_error/timeout guess
                # recorded above for this same attempt.
                diag["failure_category"] = "unavailable"
                diag["failure_reason"] = f"unavailable: asset '{normalized_symbol}' is not currently available from Quotex."
                break

            if diag["availability_status"] == "available" and diag.get("is_open") is False:
                # Asset is known to Quotex but the broker itself currently
                # reports it as closed (is_open=False) — distinct from the
                # generic "unavailable" (unknown symbol) and from a plain
                # "timeout" (symbol open, response just didn't arrive).
                # Retrying a genuinely-closed asset is pointless, same
                # reasoning as the not_available branch above.
                diag["failure_category"] = "asset_closed"
                diag["failure_reason"] = f"asset_closed: asset '{normalized_symbol}' is currently closed on Quotex (is_open=False)."
                break

            if attempt >= max_retries:
                break

            # Step 4/5: controlled backoff, reconnect via the EXISTING
            # session architecture only if the WebSocket looks dead.
            backoff = 0.5 * (2 ** attempt)
            print(f"  ⚠  No candles on attempt {attempt + 1} — retrying in {backoff:.1f}s …")
            if not getattr(self._client, "websocket_is_connected", False):
                print("  → WebSocket appears disconnected — reconnecting …")
                try:
                    await self.disconnect()
                    await self.connect()
                    diag["session_status"] = "reconnected"
                except Exception as exc:  # noqa: BLE001
                    diag["session_status"] = "reconnect_failed"
                    diag["websocket_status"] = "disconnected"
                    diag["failure_category"] = "connection_error"
                    diag["failure_reason"] = f"connection_error: reconnect failed: {exc}"
                    break
            await asyncio.sleep(backoff)
            attempt += 1

        if not diag["failure_reason"]:
            diag["failure_category"] = diag["failure_category"] or "timeout"
            diag["failure_reason"] = (
                f"timeout: no candles received for {normalized_symbol} [{timeframe}] after "
                f"{diag['retry_attempts']} attempt(s) — asset may be closed or unavailable."
            )
        print(f"  ⚠  {diag['failure_reason']}")
        return []

    async def get_candles_df(self, asset: str | None = None,
                             timeframe: str | None = None,
                             count: int | None = None) -> pd.DataFrame:
        """
        Fetch historical candles and return them as a pandas DataFrame.
        Index = timestamp (datetime).  Columns: open, high, low, close, volume.
        """
        candles = await self.get_candles(asset, timeframe, count)
        if not candles:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

        rows = [{
            "timestamp": c.timestamp,
            "open":   c.open,
            "high":   c.high,
            "low":    c.low,
            "close":  c.close,
            "volume": float(c.volume) if c.volume is not None else 0.0,
        } for c in candles]

        df = pd.DataFrame(rows)
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)
        return df

    # ── Live streaming ────────────────────────────────────────────────────────

    async def subscribe_live(self, asset: str | None = None,
                             timeframe: str | None = None,
                             duration: float = cfg.LIVE_STREAM_SECONDS,
                             on_price: Optional[Callable[[dict], None]] = None,
                             ) -> List[dict]:
        """
        Collect live price ticks for `duration` seconds using the Quotex
        instruments/history protocol that AsyncQuotexClient already implements.

        How it works
        ────────────
        get_candles() (called before this method) sends:
          42["instruments/follow","<asset>"]
          42["instruments/update",{"asset":"<asset>","period":<tf>}]
          42["chart_notification/get",{"asset":"<asset>","version":"1.0.0"}]

        After those messages the server keeps streaming live price ticks via
        quote_stream events → _on_quote_stream() → price_update events.
        This method registers callbacks on those two events, calls
        request_chart_notifications() to re-activate the stream, waits
        `duration` seconds, then unregisters.

        subscribe_symbol / unsubscribe_symbol are NOT used — they are not
        part of the Quotex WebSocket protocol.

        Returns a list of {"symbol", "timestamp", "price"} dicts.
        NEVER places orders.
        """
        if not self._client:
            raise RuntimeError("Not connected. Call connect() first.")

        asset     = asset     or self.asset
        timeframe = timeframe or cfg.PRIMARY_TIMEFRAME

        self._live_prices.clear()
        seen_timestamps: set = set()

        def _on_price_update(data: Any) -> None:
            """Collect normalised price_update dicts for this asset only."""
            if not isinstance(data, dict):
                return
            if data.get("symbol") != asset:
                return
            ts = data.get("timestamp")
            if ts in seen_timestamps:
                return
            seen_timestamps.add(ts)
            entry = {
                "symbol":    asset,
                "timestamp": ts,
                "price":     data.get("price"),
            }
            self._live_prices.append(entry)
            if on_price:
                try:
                    on_price(entry)
                except Exception:
                    pass

        def _on_quote_stream(data: Any) -> None:
            """
            Belt-and-suspenders: catch raw quote_stream payloads in case
            price_update hasn't fired for a tick yet.
            quote_stream payload is a list of [symbol, timestamp, price, …].
            """
            if not isinstance(data, list):
                return
            for quote in data:
                if (not isinstance(quote, (list, tuple))
                        or len(quote) < 3
                        or quote[0] != asset):
                    continue
                ts = quote[1]
                if ts in seen_timestamps:
                    continue
                seen_timestamps.add(ts)
                try:
                    price = float(quote[2])
                except (TypeError, ValueError):
                    continue
                entry = {"symbol": asset, "timestamp": ts, "price": price}
                self._live_prices.append(entry)
                if on_price:
                    try:
                        on_price(entry)
                    except Exception:
                        pass

        self._client.add_event_callback("price_update",  _on_price_update)
        self._client.add_event_callback("quote_stream",  _on_quote_stream)

        # Re-send chart_notification/get to ensure the server keeps the stream
        # active.  instruments/follow was already sent by get_candles().
        try:
            await self._client.request_chart_notifications(asset)
        except Exception as exc:
            print(f"  ⚠  chart_notification/get failed (non-fatal): {exc}")

        print(f"  → Listening for live prices [{timeframe}] ({duration}s) …")
        await asyncio.sleep(duration)

        try:
            self._client.remove_event_callback("price_update", _on_price_update)
        except Exception:
            pass
        try:
            self._client.remove_event_callback("quote_stream", _on_quote_stream)
        except Exception:
            pass

        print(f"  → Captured {len(self._live_prices)} live price ticks ✓")
        return list(self._live_prices)

    # ── Payout ───────────────────────────────────────────────────────────────

    async def get_payout(self, asset: str, timeframe: str) -> float:
        """
        Task 3: Fetch payout % for asset/timeframe via the Quotex client.
        Returns 0.0 if the client is not connected or the call fails.
        NEVER places orders — read-only metadata query.
        """
        if not self._client:
            return 0.0
        try:
            result = await self._client.get_payout(asset, timeframe)
            return float(result) if result is not None else 0.0
        except Exception:
            return 0.0

    async def get_available_assets(self) -> dict:
        """
        Live Asset Discovery layer support — thin passthrough to the
        existing AsyncQuotexClient.get_available_assets() (api_quotex/
        client.py, untouched), which is already kept current via the
        live 'assets_list' websocket event on this same connection.
        Returns {} if not connected or the call fails — callers must
        treat that as "unknown", never as "0 assets confirmed available".
        NEVER places orders — read-only metadata query.
        """
        if not self._client:
            return {}
        try:
            result = await self._client.get_available_assets()
            return result if isinstance(result, dict) else {}
        except Exception:
            return {}

    # ── CSV export ────────────────────────────────────────────────────────────

    @staticmethod
    def save_candles_csv(df: pd.DataFrame, path: str | None = None) -> str:
        """
        Save a candle DataFrame to CSV.
        Default path: market_analyzer/output/candles.csv
        Returns the absolute path that was written.
        """
        if path is None:
            out_dir = _ROOT / cfg.OUTPUT_DIR
            out_dir.mkdir(parents=True, exist_ok=True)
            path = str(out_dir / cfg.CSV_FILENAME)
        df.to_csv(path)
        return path
