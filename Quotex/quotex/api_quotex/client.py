"""Professional Async Quotex API Client – Unified & Cleaned."""
import asyncio
import os
import json
import time
import requests
import uuid
import base64
from typing import Dict, List, Any, Callable, Optional, Union, Tuple
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
import pandas as pd
from loguru import logger

from .monitoring import error_monitor, health_checker, ErrorCategory, ErrorSeverity
from .websocket_client import AsyncWebSocketClient
from .models import Balance, Candle, Order, OrderResult, OrderStatus, OrderDirection, ConnectionStatus, ServerTime
from .constants import ASSETS, REGIONS, TIMEFRAMES, API_LIMITS, CONNECTION_SETTINGS
from .exceptions import QuotexError, ConnectionError, AuthenticationError, OrderError, InvalidParameterError, WebSocketError, Base64DecodeError
from .utils import sanitize_symbol, format_session_id, retry_async, candles_to_dataframe
from .login import get_ssid
from .config import Config
from .connection_keep_alive import ConnectionKeepAlive

# Logging
logger.remove()
log_filename = f"log-{time.strftime('%Y-%m-%d')}.txt"
logger.add(log_filename, level="INFO", encoding="utf-8", backtrace=True, diagnose=True)

# Fast path: ultra-low-latency candle store
class FastCandleStore:
    """Lock-free ring buffer per (asset, period) for near-instant candle reads."""
    def __init__(self, maxlen: int = 4096):
        self._bufs: Dict[Tuple[str, int], deque] = {}
        self._last_ts: Dict[Tuple[str, int], int] = {}
        self._maxlen = maxlen

    def _key(self, asset: str, period: int) -> Tuple[str, int]:
        return (asset, int(period))

    def add_many(self, asset: str, period: int, candles: List["Candle"]) -> None:
        if not candles:
            return
        k = self._key(asset, period)
        buf = self._bufs.get(k)
        if buf is None:
            buf = deque(maxlen=self._maxlen)
            self._bufs[k] = buf

        # De-duplicate while preserving order (oldest -> newest)
        last_ts = self._last_ts.get(k, 0)
        appended_any = False
        for c in sorted(candles, key=lambda x: x.timestamp):
            ts_i = int(c.timestamp.timestamp())
            if ts_i <= last_ts:
                continue
            buf.append(c)
            last_ts = ts_i
            appended_any = True

        if appended_any:
            self._last_ts[k] = last_ts

    def get_tail(self, asset: str, period: int, count: int) -> List["Candle"]:
        k = self._key(asset, period)
        buf = self._bufs.get(k)
        if not buf:
            return []
        if count <= 0 or count >= len(buf):
            return list(buf)
        return list(buf)[-count:]

    def size(self, asset: str, period: int) -> int:
        k = self._key(asset, period)
        buf = self._bufs.get(k)
        return len(buf) if buf else 0

class AsyncQuotexClient:
    """Professional async Quotex client with modern Python practices"""
    # region Initialization and Setup
    def __init__(self, ssid: str, is_demo: bool = True, uid: int = 0, region: Optional[str] = None,
                 is_fast_history: bool = True, persistent_connection: bool = False, auto_reconnect: bool = True,
                 enable_logging: bool = True, max_reconnect_attempts: int = CONNECTION_SETTINGS["max_reconnect_attempts"]):
        from .config import Config

        self.raw_ssid = ssid
        self.is_demo = is_demo
        self.preferred_region = region
        self.uid = uid
        self.is_fast_history = is_fast_history
        self.persistent_connection = persistent_connection
        self.auto_reconnect = auto_reconnect
        self.enable_logging = enable_logging
        self.max_reconnect_attempts = max_reconnect_attempts
        self._config = Config()
        self._reconnect_attempts = 0
        self.balance_id: Optional[int] = None
        self.websocket_is_connected: bool = False
        self.ssl_mutual_exclusion: bool = False
        self.ssl_mutual_exclusion_write: bool = False
        if not enable_logging:
            logger.remove()
            logger.add(lambda msg: None, level="CRITICAL")
        self._original_demo = None
        if ssid.startswith('42["authorization",'):
            self._parse_complete_ssid(ssid)
        else:
            self.session_id = ssid
            self._complete_ssid = None
        self._websocket = AsyncWebSocketClient()
        # --- runtime state ---
        self._balance: Optional[Balance] = None
        self._orders: Dict[str, OrderResult] = {}
        self._active_orders: Dict[str, OrderResult] = {}          # key = requestId
        self._order_results: Dict[str, OrderResult] = {}          # key = requestId
        self._pending_order_requests: Dict[str, Order] = {}       # key = requestId
        self._candles_cache: Dict[str, List[Candle]] = {}
        self._assets_data: Dict[str, Dict[str, Any]] = {}
        self._assets_requests: Dict[str, asyncio.Future] = {}
        self._candle_requests: Dict[str, asyncio.Future] = {}
        # Approved general fix — protocol errors during an in-flight candle
        # request were previously silently discarded by _on_error(),
        # leaving the pending future to sit until the full timeout. These
        # two structures let a genuinely-correlated error resolve its
        # exact (asset, timeframe) request immediately, and let an
        # error that can't be reliably correlated still be surfaced as a
        # soft diagnostic hint — without ever guessing which request it
        # belongs to (see _on_error() and get_last_candle_error() below).
        self._candle_request_errors: Dict[str, Dict[str, Any]] = {}
        self._recent_uncorrelated_candle_errors: List[Dict[str, Any]] = []
        # Part 3 approved fix — candle responses that could not be safely
        # correlated to any currently-pending request (see
        # _on_candles_received() / _record_unmatched_candle_response()).
        # Diagnostics only; never used to resolve a request.
        self._unmatched_candle_responses: List[Dict[str, Any]] = []
        self._balance_requests: Dict[str, asyncio.Future] = {}
        self._payout_data: Dict[str, float] = {}
        self._server_time: Optional[ServerTime] = None
        self._event_callbacks: Dict[str, List[Callable]] = defaultdict(list)
        # Mapping between requestId and server order id; plus reverse index
        self._request_id_to_server_id: Dict[str, str] = {}        # requestId -> server_order_id
        self._server_order_index: Dict[str, str] = {}             # server_order_id -> requestId
        self._setup_event_handlers()
        self._error_monitor = error_monitor
        self._health_checker = health_checker
        self._operation_metrics: Dict[str, List[float]] = defaultdict(list)
        self._last_health_check = time.time()
        self._last_assets_update = 0
        self._keep_alive_manager = None
        self._ping_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._assets_update_task: Optional[asyncio.Task] = None
        self._is_persistent = False
        self._connection_stats = {
            "total_connections": 0,
            "successful_connections": 0,
            "total_reconnects": 0,
            "last_ping_time": None,
            "messages_sent": 0,
            "messages_received": 0,
            "connection_start_time": None,
        }
        # Fast store for instant candle reads
        self._fast_store = FastCandleStore()

        logger.info(
            f"Initialized Quotex client (demo={is_demo}, persistent={persistent_connection}) with enhanced monitoring"
            if enable_logging else ""
        )

    def _setup_event_handlers(self):
        self._websocket.add_event_handler('authenticated', self._on_authenticated)
        self._websocket.add_event_handler('s_authorization', self._on_authenticated)
        self._websocket.add_event_handler('balance_updated', self._on_balance_updated)
        self._websocket.add_event_handler('balance_data', self._on_balance_data)
        self._websocket.add_event_handler('balance_list', self._on_balance_list)
        self._websocket.add_event_handler('settings_list', self._on_settings_list)
        self._websocket.add_event_handler('orders_opened_list', self._on_orders_opened_list)
        self._websocket.add_event_handler('orders_closed_list', self._on_orders_closed_list)
        self._websocket.add_event_handler('order_opened', self._on_order_opened)
        self._websocket.add_event_handler('order_closed', self._on_order_closed)
        self._websocket.add_event_handler('drawing_load', self._on_drawing_load)
        self._websocket.add_event_handler('stream_update', self._on_stream_update)
        self._websocket.add_event_handler('candles_received', self._on_candles_received)
        self._websocket.add_event_handler('assets_list', self._on_assets_updated)
        self._websocket.add_event_handler('quote_stream', self._on_quote_stream)
        self._websocket.add_event_handler('depth_change', self._on_depth_change)
        self._websocket.add_event_handler('error', self._on_error)
        self._websocket.add_event_handler('json_data', self._on_json_data)
        self._websocket.add_event_handler('unknown_event', self._on_unknown_event)
        self._websocket.add_event_handler('auth_error', self._on_auth_error)
    # endregion

    # region Connection Management
    async def connect(self, regions: Optional[List[str]] = None, persistent: bool = None) -> bool:
        logger.info("Connecting to Quotex...")

        if persistent is not None:
            self.persistent_connection = persistent
        try:
            self.ssl_mutual_exclusion = False
            self.ssl_mutual_exclusion_write = False
            self.websocket_is_connected = False

            if self.persistent_connection:
                return await self._start_persistent_connection(regions)
            else:
                return await self._start_regular_connection(regions)
        except Exception as e:
            logger.error(f"Connection failed: {str(e)}")
            await self._error_monitor.record_error(
                error_type="connection_failed",
                severity=ErrorSeverity.HIGH,
                category=ErrorCategory.CONNECTION,
                message=f"Connection failed: {str(e)}",
                context={"exception": str(e)}
            )
            return False

    async def _start_regular_connection(self, regions: Optional[List[str]] = None) -> bool:
        logger.info("Starting regular connection...")

        if not regions:
            if self.is_demo:
                demo_urls = REGIONS.get_demo_regions()
                regions = [name for name, url in REGIONS.get_all_regions().items() if url in demo_urls]
                logger.info(f"Demo mode: Using demo regions: {regions}")
            else:
                regions = [name for name, url in REGIONS.get_all_regions().items() if "DEMO" not in name.upper()]
                logger.info(f"Live mode: Using non-demo regions: {regions}")

        self._connection_stats["total_connections"] += 1
        self._connection_stats["connection_start_time"] = time.time()

        for region in regions:
            if self._reconnect_attempts >= self.max_reconnect_attempts:
                logger.error(f"Max reconnect attempts ({self.max_reconnect_attempts}) reached. Aborting connection.")
                raise ConnectionError(f"Failed to connect after {self.max_reconnect_attempts} attempts")
            try:
                region_url = REGIONS.get_region(region)
                if not region_url:
                    logger.warning(f"No URL found for region {region}")
                    continue

                import socket
                host = region_url.split("//")[1].split("/")[0]
                socket.getaddrinfo(host, 443)  # DNS resolution check

                urls = [region_url]
                logger.info(f"Trying region: {region} with URL: {region_url}")

                ssid_message = self._format_session_message()
                success = await self._websocket.connect(urls, ssid_message)
                if success:
                    logger.info(f"Connected to region: {region}")
                    await self._wait_for_authentication(timeout=1.0)
                    await self._initialize_data()
                    await self._start_keep_alive_tasks()

                    self._connection_stats["successful_connections"] += 1
                    self.websocket_is_connected = True
                    logger.info("Successfully connected and authenticated")

                    self._reconnect_attempts = 0
                    return True

            except socket.gaierror as e:
                logger.warning(f"DNS resolution failed for {region_url}: {str(e)}")
                await self._error_monitor.record_error(
                    error_type="dns_resolution_failed",
                    severity=ErrorSeverity.HIGH,
                    category=ErrorCategory.CONNECTION,
                    message=f"DNS resolution failed for {region_url}: {str(e)}",
                    context={"region": region, "url": region_url}
                )
                self._reconnect_attempts += 1
                continue

            except Exception as e:
                logger.warning(f"Failed to connect to region {region}: {str(e)}")
                self._reconnect_attempts += 1
                await self._error_monitor.record_error(
                    error_type="connection_failed",
                    severity=ErrorSeverity.HIGH,
                    category=ErrorCategory.CONNECTION,
                    message=f"Failed to connect to {region}: {str(e)}",
                    context={"region": region, "exception": str(e)}
                )

                delay = min(
                    CONNECTION_SETTINGS["reconnect_initial_delay"] * (CONNECTION_SETTINGS["reconnect_factor"] ** self._reconnect_attempts),
                    CONNECTION_SETTINGS["reconnect_max_delay"]
                )
                logger.info(f"Waiting {delay:.2f} seconds before next reconnect attempt")
                await asyncio.sleep(delay)
                continue

        raise ConnectionError(f"Failed to connect to any region after {self._reconnect_attempts} attempts")

    async def _start_persistent_connection(self, regions: Optional[List[str]] = None) -> bool:
        logger.info("Starting persistent connection with automatic keep-alive...")
        from .connection_keep_alive import ConnectionKeepAlive
        complete_ssid = self.raw_ssid
        self._keep_alive_manager = ConnectionKeepAlive(complete_ssid, self.is_demo)
        self._keep_alive_manager.add_event_handler('connected', self._on_keep_alive_connected)
        self._keep_alive_manager.add_event_handler('reconnected', self._on_keep_alive_reconnected)
        self._keep_alive_manager.add_event_handler('message_received', self._on_keep_alive_message)
        self._keep_alive_manager.add_event_handler('balance_data', self._on_balance_data)
        self._keep_alive_manager.add_event_handler('balance_updated', self._on_balance_updated)
        self._keep_alive_manager.add_event_handler('authenticated', self._on_authenticated)
        self._keep_alive_manager.add_event_handler('order_opened', self._on_order_opened)
        self._keep_alive_manager.add_event_handler('order_closed', self._on_order_closed)
        self._keep_alive_manager.add_event_handler('candles_received', self._on_candles_received)
        self._keep_alive_manager.add_event_handler('assets_list', self._on_assets_updated)
        self._keep_alive_manager.add_event_handler('quote_stream', self._on_quote_stream)
        self._keep_alive_manager.add_event_handler('depth_change', self._on_depth_change)
        self._keep_alive_manager.add_event_handler('error', self._on_error)
        self._keep_alive_manager.add_event_handler('auth_error', self._on_auth_error)
        success = await self._keep_alive_manager.connect_with_keep_alive(regions)
        if success:
            self._is_persistent = True
            self.websocket_is_connected = True
            logger.info("Persistent connection established successfully")
            return True
        else:
            logger.error("Failed to establish persistent connection")
            return False

    async def disconnect(self) -> None:
        logger.info("Disconnecting from Quotex...")

        if self._ping_task:
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass

        if self._reconnect_task:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass

        if self._assets_update_task:
            self._assets_update_task.cancel()
            try:
                await self._assets_update_task
            except asyncio.CancelledError:
                pass

        if self._is_persistent and self._keep_alive_manager:
            await self._keep_alive_manager.disconnect()
        else:
            await self._websocket.disconnect()

        self._is_persistent = False
        self.websocket_is_connected = False

        self._balance = None
        self._active_orders.clear()
        self._order_results.clear()
        self._pending_order_requests.clear()

        logger.info("Disconnected successfully")

    async def _start_keep_alive_tasks(self):
        logger.info("Starting keep-alive tasks for regular connection...")

        self._ping_task = asyncio.create_task(self._ping_loop())
        if self.auto_reconnect:
            self._reconnect_task = asyncio.create_task(self._reconnection_monitor())
        self._assets_update_task = asyncio.create_task(self._update_raw_assets())

    async def _ping_loop(self):
        while self.is_connected and not self._is_persistent:
            try:
                await self._websocket.send_message('2')
                self._connection_stats["last_ping_time"] = time.time()
                await asyncio.sleep(25)
            except Exception as e:
                logger.warning(f"Ping failed: {str(e)}")
                await self._error_monitor.record_error(
                    error_type="ping_failed",
                    severity=ErrorSeverity.MEDIUM,
                    category=ErrorCategory.CONNECTION,
                    message=f"Ping failed: {str(e)}",
                    context={}
                )
                break

    async def _reconnection_monitor(self):
        while not self._is_persistent:
            await asyncio.sleep(1)
            if not self.is_connected:
                logger.info("Connection lost, attempting reconnection...")
                self._connection_stats["total_reconnects"] += 1
                try:
                    await self._start_regular_connection()
                    self.websocket_is_connected = True
                    logger.info("Reconnection successful")
                except Exception as e:
                    logger.error(f"Reconnection error: {str(e)}")
                    delay = min(
                        CONNECTION_SETTINGS["reconnect_initial_delay"] * (CONNECTION_SETTINGS["reconnect_factor"] ** self._connection_stats["total_reconnects"]),
                        CONNECTION_SETTINGS["reconnect_max_delay"]
                    )
                    logger.info(f"Waiting {delay:.2f} seconds before next reconnect attempt")
                    await asyncio.sleep(delay)
                    await self._error_monitor.record_error(
                        error_type="reconnection_failed",
                        severity=ErrorSeverity.HIGH,
                        category=ErrorCategory.CONNECTION,
                        message=f"Reconnection error: {str(e)}",
                        context={"attempt": self._connection_stats["total_reconnects"]}
                    )

    @property
    def is_connected(self) -> bool:
        if self._is_persistent and self._keep_alive_manager:
            return self._keep_alive_manager.is_connected
        else:
            return self._websocket.is_connected and self.websocket_is_connected

    @property
    def connection_info(self):
        if self._is_persistent and self._keep_alive_manager:
            return self._keep_alive_manager.connection_info
        else:
            return self._websocket.connection_info

    async def send_message(self, message: str) -> bool:
        try:
            while self.ssl_mutual_exclusion or self.ssl_mutual_exclusion_write:
                await asyncio.sleep(0.1)
            self.ssl_mutual_exclusion_write = True

            if self._is_persistent and self._keep_alive_manager:
                success = await self._keep_alive_manager.send_message(message)
            else:
                await self._websocket.send_message(message)
                success = True

            self._connection_stats["messages_sent"] += 1
            self.ssl_mutual_exclusion_write = False
            return success

        except Exception as e:
            logger.error(f"Failed to send message: {str(e)}")
            self.ssl_mutual_exclusion_write = False
            await self._error_monitor.record_error(
                error_type="send_message_failed",
                severity=ErrorSeverity.MEDIUM,
                category=ErrorCategory.CONNECTION,
                message=f"Failed to send message: {str(e)}",
                context={"message": message[:100]}
            )
            return False

    def get_connection_stats(self) -> Dict[str, Any]:
        stats = self._connection_stats.copy()
        if self._is_persistent and self._keep_alive_manager:
            stats.update(self._keep_alive_manager.get_stats())
        else:
            stats.update(
                {
                    "websocket_connected": self._websocket.is_connected,
                    "connection_info": self._websocket.connection_info,
                }
            )
        return stats
    # endregion

    # region Balance Management
    async def get_balance(self) -> Balance:
        if not self.is_connected:
            raise ConnectionError("Not connected to Quotex")

        try:
            if not self._balance or (datetime.now() - self._balance.last_updated).seconds > 60:
                await self._request_balance_update()
                start_time = time.time()
                timeout = 10.0
                while not self._balance and (time.time() - start_time) < timeout:
                    await asyncio.sleep(0.1)

            if self._balance:
                return self._balance

            logger.warning("Balance not available after request, continuing to wait...")
            await asyncio.sleep(1)
            return await self.get_balance()

        except Exception as e:
            logger.error(f"Error getting balance: {str(e)}")
            await self._error_monitor.record_error(
                error_type="balance_retrieval_failed",
                severity=ErrorSeverity.HIGH,
                category=ErrorCategory.DATA,
                message=f"Error getting balance: {str(e)}",
                context={}
            )
            raise

    async def _request_balance_update(self) -> None:
        message = '42["balance",{}]'
        await self.send_message(message)
    # endregion

    # region Order Management
    async def place_order(self, asset: str, amount: float, direction: OrderDirection, duration: Union[int, str]) -> Order:
        raise RuntimeError("Trading is disabled in this project — analysis-only. place_order()/_send_order() must never be called.")
        if not self.is_connected:
            raise ConnectionError("Not connected to Quotex")

        parsed_secs = self._parse_seconds(duration)
        self._validate_order_parameters(asset, amount, direction, int(parsed_secs))

        req_id = str(int(time.time() * 1000))
        order = Order(asset=asset, amount=amount, direction=direction, duration=int(parsed_secs), request_id=req_id)
        self._pending_order_requests[req_id] = order

        try:
            await self._send_order(order)
            result = await self._wait_for_order_result(req_id, order, timeout=180.0)  # Increased timeout
            logger.info(f"Order placed: {result.order_id} - {result.status}")
            return result

        except Exception as e:
            logger.error(f"Order placement failed: {str(e)}")
            self._pending_order_requests.pop(req_id, None)
            await self._error_monitor.record_error(
                error_type="order_placement_failed",
                severity=ErrorSeverity.HIGH,
                category=ErrorCategory.TRADING,
                message=f"Order placement failed: {str(e)}",
                context={"asset": asset, "amount": amount, "direction": direction.value, "duration": str(duration)}
            )
            raise OrderError(f"Failed to place order: {str(e)}")

    async def check_order_result(self, order_id: str) -> Optional[OrderResult]:
        """
        Return OrderResult by requestId (preferred) or broker_order_id (fallback).
        Keeps existing keys, adds a robust reverse lookup to prevent timeouts.
        """
        # Exact requestId hit
        if order_id in self._active_orders:
            return self._active_orders[order_id]
        if order_id in self._order_results:
            return self._order_results[order_id]

        # broker_order_id hit (server id)
        for res in self._order_results.values():
            if res.broker_order_id and str(res.broker_order_id) == str(order_id):
                return res
        for res in self._active_orders.values():
            if res.broker_order_id and str(res.broker_order_id) == str(order_id):
                return res

        # requestId mapping
        if order_id in self._request_id_to_server_id:
            srv = self._request_id_to_server_id[order_id]
            for res in self._order_results.values():
                if res.broker_order_id and str(res.broker_order_id) == str(srv):
                    return res

        return None

    async def get_active_orders(self) -> List[OrderResult]:
        return list(self._active_orders.values())

    async def _send_order(self, order: Order) -> None:
        raise RuntimeError("Trading is disabled in this project — analysis-only. place_order()/_send_order() must never be called.")
        action = "call" if order.direction == OrderDirection.CALL else "put"
        option_type, time_field = self._compute_order_time_and_type(order.asset, order.duration)

        payload = {
            "asset": order.asset,
            "amount": float(order.amount),
            "time": int(time_field),
            "action": action,
            "isDemo": 1 if self.is_demo else 0,
            "tournamentId": 0,
            "requestId": int(order.request_id),
            "optionType": int(option_type)
        }

        try:
            await self.send_message('42["tick"]')
            await self.send_message(f'42["instruments/follow","{order.asset}"]')
        except Exception:
            pass

        message = f'42["orders/open",{json.dumps(payload, separators=(",",":"))}]'
        await self.send_message(message)
        if self.enable_logging:
            logger.debug(f"Sent order: {message}")

    async def _wait_for_order_result(self, request_id: str, order: "Order", timeout: float = 180.0) -> "OrderResult":
        start_time = time.time()
        warned = False
        while time.time() - start_time < timeout:
            if request_id in self._active_orders:
                if self.enable_logging:
                    logger.info(f"Order {request_id} found in active tracking (opened)")
                if request_id in self._pending_order_requests:
                    del self._pending_order_requests[request_id]
                return self._active_orders[request_id]

            if request_id in self._order_results:
                if self.enable_logging:
                    logger.info(f"Order {request_id} found in completed results")
                if request_id in self._pending_order_requests:
                    del self._pending_order_requests[request_id]
                return self._order_results[request_id]

            if not warned and (time.time() - start_time) >= timeout * 0.8:
                warned = True
                logger.warning(f"Order {request_id} approaching timeout ({timeout:.1f}s)")

            await asyncio.sleep(0.05)

        logger.error(f"Order {request_id} timed out waiting for server response")
        if request_id in self._pending_order_requests:
            del self._pending_order_requests[request_id]

        await self._error_monitor.record_error(
            error_type="order_timeout",
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.TRADING,
            message=f"Order {request_id} timed out waiting for server confirmation",
            context={"request_id": request_id}
        )
        raise OrderError(f"Order {request_id} timed out waiting for server confirmation")

    async def check_win(self, order_id: str) -> tuple[Optional[float], Optional[str]]:
        logger.info(f"check_win: Waiting for order {order_id} result.")
        timeout = 180.0
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                result = await self.check_order_result(order_id)
                if result and result.status in [OrderStatus.WIN, OrderStatus.LOSS, OrderStatus.DRAW]:
                    profit = result.profit if result.profit is not None else 0.0
                    status = result.status.value if result.status else "unknown"
                    logger.info(
                        f"[RESULT] Order {order_id} - {status.upper()} - Profit: ${profit:.2f} | "
                        f"Open: {result.open_price or 0:.5f}, Close: {result.close_price or 0:.5f}"
                    )
                    return profit, status

                await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"Error in check_win for {order_id}: {str(e)}")
                await self._error_monitor.record_error(
                    error_type="check_win_error",
                    severity=ErrorSeverity.MEDIUM,
                    category=ErrorCategory.TRADING,
                    message=f"Error checking win for order {order_id}: {str(e)}",
                    context={"order_id": order_id}
                )
                await asyncio.sleep(1)

        logger.warning(f"No result found for order {order_id} within timeout")
        return None, None
    # endregion

    # region Asset Management
    async def get_available_assets(self) -> Dict[str, Dict[str, Any]]:
        while True:
            try:
                if isinstance(getattr(self, "_assets_data", None), dict) and self._assets_data:
                    return self._assets_data.copy()

                raw_assets = await self._get_raw_asset()
                assets: Dict[str, Dict[str, Any]] = {}

                for asset in raw_assets or []:
                    try:
                        symbol = str(asset[1])
                        # Normalize the same way get_candles() and
                        # get_assets_and_payouts() already do, so the key
                        # this registry exposes is guaranteed to be the
                        # exact string a subsequent get_candles() call will
                        # end up using — no mismatch between what's
                        # advertised as available and what's requested.
                        symbol = sanitize_symbol(symbol).replace("_OTC", "_otc")

                        _tf_raw_present = (len(asset) > 12 and isinstance(asset[12], list)) or \
                                          (len(asset) > 15 and isinstance(asset[15], list))
                        _tf_parsed = sorted(set(
                            [
                                int(x[0]) for x in asset[12]
                                if isinstance(asset[12], list)
                                for x in ([x] if isinstance(x, (int, str)) else [x])
                                if isinstance(x, (list, tuple)) and x
                            ] + [
                                int(x) for x in asset[12]
                                if isinstance(asset[12], list) and isinstance(x, (int, str))
                            ] + [
                                int(obj.get("time"))
                                for obj in (asset[15] or [])
                                if len(asset) > 15 and isinstance(asset[15], list)
                                for obj in ([obj] if isinstance(obj, dict) else [])
                                if isinstance(obj.get("time", None), (int, str))
                            ]
                        )) if _tf_raw_present else []
                        # "Confirmed" means the broker's raw asset row actually
                        # carried a non-empty timeframe list we could parse —
                        # NOT merely that the row had a list-shaped field.
                        # Anything else (missing field, empty list, parse
                        # yielded nothing) falls back to the full generic
                        # list and is marked unconfirmed, so callers can tell
                        # "broker said no" apart from "we don't actually know".
                        _tf_confirmed = bool(_tf_parsed)
                        _tf_list = _tf_parsed if _tf_confirmed else \
                            [30, 60, 120, 180, 300, 600, 900, 1800, 2700, 3600, 7200, 10800, 14400]

                        assets[symbol] = {
                            "id": int(asset[0]) if len(asset) > 0 and asset[0] is not None else 0,
                            "name": str(asset[2]) if len(asset) > 2 else symbol,
                            "type": str(asset[3]) if len(asset) > 3 else "unknown",
                            "payout": int(asset[5]) if len(asset) > 5 and asset[5] is not None else 0,
                            "is_otc": "_otc" in symbol.lower(),
                            "is_open": bool(asset[14]) if len(asset) > 14 and asset[14] is not None else False,
                            "available_timeframes": _tf_list,
                            "available_timeframes_confirmed": _tf_confirmed,
                        }
                    except Exception:
                        continue

                if assets:
                    self._assets_data = assets.copy()

                return assets

            except Exception as e:
                if self.enable_logging:
                    logger.error(f"Error fetching available assets: {e}")
                await asyncio.sleep(1)

    async def get_assets_and_payouts(self) -> Dict[str, int]:
        """
        Fast path: use the latest instruments snapshot (kept in _assets_data) when available,
        otherwise pull a fresh snapshot via _get_raw_asset(). Only open assets with positive payout are returned.
        """
        if not self.is_connected:
            raise ConnectionError("Not connected to Quotex")

        try:
            # Prefer cached snapshot if present
            if getattr(self, "_assets_data", None):
                payouts: Dict[str, int] = {}
                for symbol, info in self._assets_data.items():
                    try:
                        payout = int(info.get("payout", 0) or 0)
                        is_open = bool(info.get("is_open", False))
                        if is_open and payout > 0:
                            payouts[sanitize_symbol(symbol)] = payout
                    except Exception:
                        continue
                return payouts

            # Fallback: pull fresh snapshot
            raw_assets = await self._get_raw_asset()
            payouts: Dict[str, int] = {}
            for row in raw_assets or []:
                try:
                    symbol = str(row[1])
                    payout = int(row[5]) if len(row) > 5 and row[5] is not None else 0
                    is_open = bool(row[14]) if len(row) > 14 and row[14] is not None else False
                    if "_OTC" in symbol:
                        symbol = symbol.replace("_OTC", "_otc")
                    if is_open and payout > 0:
                        payouts[sanitize_symbol(symbol)] = payout
                except Exception:
                    continue
            return payouts

        except Exception:
            await asyncio.sleep(1)
            return {}

    async def get_payout(self, asset: str, timeframe: str) -> float:
        """
        Pocket-Option style speed: answer from instruments snapshot first (source of truth),
        then fallback to a direct instruments/list pull if stale/missing.
        """
        if asset not in ASSETS:
            raise InvalidParameterError(f"Invalid asset: {asset}")
        if timeframe not in TIMEFRAMES:
            raise InvalidParameterError(f"Invalid timeframe: {timeframe}")

        # Fast snapshot path
        if getattr(self, "_assets_data", None):
            info = self._assets_data.get(asset) or self._assets_data.get(sanitize_symbol(asset))
            if info:
                if not info.get("is_open", False):
                    logger.warning(f"Asset {asset} is closed")
                    return 0.0
                try:
                    return float(int(info.get("payout", 0) or 0))
                except Exception:
                    pass

        # Fallback to raw pull (keeps original logic)
        while True:
            try:
                raw_assets = await self._get_raw_asset()
                for row in raw_assets or []:
                    try:
                        if str(row[1]) == asset:
                            payout = int(row[5]) if len(row) > 5 and row[5] is not None else 0
                            is_open = bool(row[14]) if len(row) > 14 and row[14] is not None else False

                            if not is_open:
                                logger.warning(f"Asset {asset} is closed")
                                return 0.0
                            return float(payout)
                    except Exception:
                        continue
                await asyncio.sleep(1)
            except Exception:
                await asyncio.sleep(1)

    async def _get_raw_asset(self) -> List[Any]:
        request_id = f"assets_{int(time.time()*1000)}"
        timeout = getattr(self._config.trading, "default_timeout", 10) + 20

        future = asyncio.get_event_loop().create_future()
        self._assets_requests[request_id] = future

        try:
            msg = '451-["instruments/list",{"_placeholder":true,"num":0}]'
            await self.send_message(msg)
            if self.enable_logging:
                logger.debug(f"Sent instruments/list (request_id={request_id})")

            raw_assets = await asyncio.wait_for(future, timeout=timeout)
            return raw_assets or []

        except asyncio.TimeoutError:
            await self._error_monitor.record_error(
                error_type="assets_timeout",
                severity=ErrorSeverity.MEDIUM,
                category=ErrorCategory.DATA,
                message="Timeout waiting for assets data",
            )
            raise

        except Exception as e:
            await self._error_monitor.record_error(
                error_type="assets_error",
                severity=ErrorSeverity.HIGH,
                category=ErrorCategory.DATA,
                message=f"Error getting assets: {e}",
            )
            raise

        finally:
            self._assets_requests.pop(request_id, None)

    async def _on_assets_updated(self, data: Any) -> None:
        """
        Normalize and store assets snapshot pushed by the server.
        Also resolves any pending futures created by _get_raw_asset().
        Now updates self._last_assets_update for the refresh scheduler.
        """
        try:
            parsed: Dict[str, Dict[str, Any]] = {}
            if isinstance(data, list):
                for row in data:
                    try:
                        if not (isinstance(row, (list, tuple)) and len(row) >= 15):
                            continue

                        symbol = str(row[1] or "").strip()
                        if not symbol:
                            continue

                        if "_OTC" in symbol:
                            symbol = symbol.replace("_OTC", "_otc")

                        payout = int(row[5]) if len(row) > 5 and row[5] is not None else 0
                        is_open = bool(row[14]) if len(row) > 14 and row[14] is not None else False

                        tfs: List[int] = []
                        if len(row) > 12 and isinstance(row[12], list):
                            for x in row[12]:
                                if isinstance(x, (list, tuple)) and x:
                                    tfs.append(int(x[0]))
                                elif isinstance(x, (int, str)):
                                    tfs.append(int(x))
                        if len(row) > 15 and isinstance(row[15], list):
                            for obj in row[15]:
                                if isinstance(obj, dict) and "time" in obj:
                                    tfs.append(int(obj["time"]))

                        tfs = sorted(set(tfs))
                        # "Confirmed" = the broker row actually yielded a
                        # non-empty parsed timeframe list. Empty/missing
                        # falls back to the generic list and is marked
                        # unconfirmed (see get_available_assets() docstring
                        # for why this distinction matters before gating).
                        tfs_confirmed = bool(tfs)
                        if not tfs_confirmed:
                            tfs = [30, 60, 120, 180, 300, 600, 900, 1800, 2700, 3600, 7200, 10800, 14400]

                        parsed[symbol] = {
                            "id": int(row[0] or 0),
                            "name": str(row[2] or symbol),
                            "type": str(row[3] or "unknown"),
                            "payout": payout,
                            "is_otc": "_otc" in symbol.lower(),
                            "is_open": is_open,
                            "available_timeframes": tfs,
                            "available_timeframes_confirmed": tfs_confirmed,
                        }
                    except Exception:
                        continue

            if parsed:
                self._assets_data = parsed
                self._last_assets_update = time.time()  # <-- track last refresh moment
                logger.info(f"Stored {len(parsed)} assets from instruments/list")

            # Resolve any pending futures waiting for instruments/list
            for _, fut in list(self._assets_requests.items()):
                if not fut.done():
                    try:
                        fut.set_result(data if isinstance(data, list) else [])
                    except Exception:
                        pass

            await self._emit_event("assets_list", parsed if parsed else data)

        except Exception as e:
            logger.error(f"Error processing assets update: {e}")
            await self._error_monitor.record_error(
                error_type="assets_update_failed",
                severity=ErrorSeverity.MEDIUM,
                category=ErrorCategory.DATA,
                message=f"Error processing assets update: {e}",
            )

    async def _update_raw_assets(self) -> None:
        """
        Periodically refresh instruments list, but only when the last snapshot is stale.
        Debounces immediate refresh after connect/auth (server usually pushes one already),
        and avoids overlapping requests.
        """
        # Prefer config override if present, else a sensible default.
        min_refresh = getattr(self._config.trading, "assets_refresh_interval", 120)
        jitter_floor = 5  # small wait to avoid tight loops when skipping

        while self.is_connected:
            try:
                # If we very recently received assets_list (pushed by server or previous pull), skip.
                now = time.time()
                last = float(self._last_assets_update or 0.0)
                age = now - last if last > 0 else 1e9

                # If a request is already in-flight, don't start another one.
                has_inflight = any(not fut.done() for fut in list(self._assets_requests.values()))
                if has_inflight:
                    await asyncio.sleep(jitter_floor)
                    continue

                if age < min_refresh:
                    # Sleep just enough to respect the refresh window (with a floor).
                    wait_for = max(jitter_floor, min_refresh - age)
                    await asyncio.sleep(wait_for)
                    continue

                # Request a fresh snapshot now.
                await self._get_raw_asset()
                logger.debug("Updated assets list periodically")

                # Sleep to next refresh cycle.
                await asyncio.sleep(min_refresh)

            except asyncio.CancelledError:
                # Task is being cancelled on disconnect; exit cleanly.
                break
            except asyncio.TimeoutError:
                # _get_raw_asset already records "assets_timeout"; back off a bit quietly.
                await asyncio.sleep(max(jitter_floor, min_refresh // 2))
            except Exception as e:
                logger.error(f"Failed to update assets: {e}")
                await asyncio.sleep(max(jitter_floor, min_refresh // 2))
    # endregion

    # region Candle Management
    async def request_chart_notifications(self, asset: str, version: str = "1.0.0") -> None:
        """
        Send the chart-notification command through the active websocket.

        The command itself has no payload; instruments/update selects the
        asset/period.  We deliberately use the client's active websocket
        object rather than the persistent keep-alive wrapper so the command
        and the receiver share exactly the same transport and diagnostics.
        """
        msg = '42["chart_notification/get"]'
        try:
            # This is part of the candle request/response transaction. It must
            # reach the active socket immediately; queueing it can leave the
            # candle future waiting while the diagnostic incorrectly reports
            # only the earlier instruments/update frame.
            await self._websocket.send_message_immediate(msg)
            try:
                self._websocket.record_candle_transport_event(
                    "socketio-text", event="chart_notification/get", direction="send",
                    asset=asset, request_id=f"{sanitize_symbol(asset).replace('_OTC', '_otc')}", status="sent"
                )
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Failed to request chart notifications: {str(e)}")
            await self._error_monitor.record_error(
                error_type="chart_notification_request_failed",
                severity=ErrorSeverity.HIGH,
                category=ErrorCategory.DATA,
                message=f"Failed to request chart notifications: {str(e)}",
                context={"asset": asset, "version": version}
            )
            # Do not hide a transport failure behind the generic candle timeout.
            raise

    async def get_candles(self, asset: str, timeframe: Union[str, int], count: int = 100,
                          end_time: Optional[datetime] = None) -> List["Candle"]:
        """
        1) Serve immediately from FastCandleStore if enough data is already cached.
        2) Otherwise fire a single consolidated request and merge the response into the store.
        """
        tf_secs = TIMEFRAMES.get(timeframe, 60) if isinstance(timeframe, str) else int(timeframe)
        sym = sanitize_symbol(asset).replace("_OTC", "_otc")

        # Lazy-init fast store
        if not hasattr(self, "_fast_store"):
            self._fast_store = FastCandleStore()

        # Instant answer if we already have enough candles cached — but only
        # if the latest cached candle's own boundary hasn't already passed
        # (Patch 2, Phase 2.8). Reuses the existing FastCandleStore API
        # unchanged (add_many/get_tail/size not modified) — this only
        # changes the decision of whether the cache-hit branch is taken.
        cached_n = self._fast_store.size(sym, tf_secs)
        if cached_n >= max(1, count):
            is_fresh = False
            try:
                _tail = self._fast_store.get_tail(sym, tf_secs, 1)
                if _tail:
                    _boundary = _tail[-1].timestamp + timedelta(seconds=tf_secs)
                    is_fresh = _boundary > datetime.now(timezone.utc)
                # else: unexpected empty tail despite cached_n > 0 — do not
                # crash, do not fabricate anything; is_fresh stays False so
                # this falls through to the existing fetch path below,
                # exactly like a stale-cache result.
            except Exception:
                # Any unexpected error while checking freshness must never
                # block or crash the request — fall through to the existing
                # fetch path, same as a stale-cache result.
                is_fresh = False
            if is_fresh:
                return self._fast_store.get_tail(sym, tf_secs, count)
            # else: stale — deliberately fall through to the existing
            # cache-miss / broker-fetch path below. No new retry system,
            # no new locks, no background refresh — same _request_candles()
            # call, same exception handling, same failure_category logic.

        # Not enough in cache → fetch and then merge
        try:
            candles = await self._request_candles(sym, tf_secs, count, end_time)
            if candles:
                self._fast_store.add_many(sym, tf_secs, candles)
                # Also keep old cache for backward compatibility (if anything else uses it)
                self._candles_cache[f"{sym}_{tf_secs}"] = self._fast_store.get_tail(sym, tf_secs, 999_999)
            return self._fast_store.get_tail(sym, tf_secs, count)
        except Exception as e:
            logger.error(f"Error fetching candles for {sym}: {e}")
            return []

    def get_last_candle_error(self, asset: str, timeframe: Union[str, int]) -> Optional[Dict[str, Any]]:
        """
        Approved general fix — returns (and consumes) the most recent
        outcome recorded for this exact (asset, timeframe) candle
        request, if any. Shape: {"error", "message", "timestamp", "kind"}
        where kind is one of "protocol_error" (a real, correlated server
        error — never a guess), "timeout" (no response at all within the
        window), or "empty_response" (a response arrived, resolved
        successfully, but was empty — no error). Only ever returns
        information for THIS exact request; never another asset/timeframe's
        outcome. Safe to call right after get_candles() returns an empty
        result, to distinguish these cases from each other.
        """
        tf_secs = TIMEFRAMES.get(timeframe, 60) if isinstance(timeframe, str) else int(timeframe)
        sym = sanitize_symbol(asset).replace("_OTC", "_otc")
        rid = f"{sym}_{tf_secs}"
        return self._candle_request_errors.pop(rid, None)

    def get_recent_uncorrelated_candle_errors(self, since_ts: float) -> List[Dict[str, Any]]:
        """
        Protocol errors observed that could NOT be reliably tied to a
        specific candle request (the server's error payload lacked
        asset/period context). Returned only as a soft, clearly-labeled
        hint for diagnostics — never used to resolve/short-circuit any
        specific request, since which request (if any) it belongs to
        can't be proven.
        """
        return [e for e in self._recent_uncorrelated_candle_errors if e.get("timestamp", 0) >= since_ts]

    async def get_candles_dataframe(self, asset: str, timeframe: Union[str, int],
                                    count: int = 100, end_time: Optional[datetime] = None) -> pd.DataFrame:
        candles = await self.get_candles(asset, timeframe, count, end_time)
        if not candles:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        rows = [{
            "timestamp": c.timestamp,
            "open": c.open, "high": c.high, "low": c.low, "close": c.close, "volume": c.volume
        } for c in candles]
        df = pd.DataFrame(rows)
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)
        return df

    async def _request_candles(self, asset: str, timeframe: int, count: int,
                               end_time: Optional[datetime]) -> List["Candle"]:
        """
        Consolidates concurrent requests per (asset,timeframe) and resolves all waiters together.
        Sends follow/update + chart_notification in one burst. Keeps original behavior & timeouts.
        """
        request_id = f"{asset}_{timeframe}"
        if self.enable_logging:
            logger.warning(f"[CANDLE-DEBUG] REQUEST start asset={asset} period={timeframe} count={count}")
        # Reuse an in-flight future for the same rid (like Pocket Option).
        # The shared future resolves with the FULL response for the first
        # caller's request — each caller (this one included) still trims to
        # its OWN requested `count`, so two callers sharing one in-flight
        # request for the same (asset, timeframe) but different counts each
        # get back the number of candles they actually asked for.
        fut = self._candle_requests.get(request_id)
        if fut and not fut.done():
            try:
                shared_candles = await asyncio.wait_for(fut, timeout=max(10.0, float(getattr(self._config.trading, "default_timeout", 10))))
            except asyncio.TimeoutError:
                return []
            return shared_candles[-count:] if isinstance(shared_candles, list) and count else shared_candles
        candle_future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._candle_requests[request_id] = candle_future
        try:
            # Canonical Quotex chart sequence: select the asset/timeframe,
            # then request chart notifications. The historical response is
            # announced by a 451-[...history/list/v2...] header followed by a
            # binary JSON frame. Avoid the unsupported/legacy
            # instruments/follow command here; depth/follow is unrelated to
            # candle history.
            upd_msg = f'42["instruments/update",{{"asset":"{asset}","period":{int(timeframe)}}}]'
            if self.enable_logging:
                logger.warning(f"[CANDLE-DEBUG] SEND update asset={asset} period={timeframe}")
            # Candle history must use a confirmed, immediate frame send.
            # Do not route this request through the batching/keep-alive queue.
            await self._websocket.send_message_immediate(upd_msg)
            try:
                self._websocket.record_candle_transport_event(
                    "socketio-text", event="instruments/update", direction="send",
                    asset=asset, period=int(timeframe), request_id=request_id, status="sent"
                )
            except Exception:
                pass

            # Send the chart-notification command on the SAME websocket object
            # that owns the candle receiver.  In persistent mode the old path
            # routed this through the keep-alive wrapper, which could make the
            # transport trace misleading and, on a stale manager, silently
            # omit the command from the active socket.
            await self.request_chart_notifications(asset)
            if self.enable_logging:
                logger.warning(f"[CANDLE-DEBUG] SEND chart_notification asset={asset}")
            timeout = max(10.0, float(getattr(self._config.trading, "default_timeout", 10)))
            if self.enable_logging:
                logger.warning(f"[CANDLE-DEBUG] WAIT response asset={asset} period={timeframe} timeout={timeout}s")
            candles = await asyncio.wait_for(candle_future, timeout=timeout)
            if self.enable_logging:
                logger.warning(f"[CANDLE-DEBUG] FUTURE resolved asset={asset} period={timeframe} candles={len(candles) if isinstance(candles, list) else type(candles).__name__}")
            try:
                self._websocket.record_candle_transport_event(
                    "candle-future", event="candles_received", direction="internal",
                    asset=asset, period=int(timeframe), request_id=request_id,
                    payload_count=len(candles) if isinstance(candles, list) else None, status="resolved"
                )
            except Exception:
                pass
            if not candles:
                # Minimal extension of the same approved error-propagation
                # mechanism: record that this request resolved successfully
                # but empty (no protocol error occurred) — distinct from a
                # true timeout below. setdefault() so this NEVER overwrites
                # a genuine correlated protocol error _on_error() may have
                # already recorded for this exact rid (that takes priority).
                self._candle_request_errors.setdefault(request_id, {
                    "error": None, "message": None, "timestamp": time.time(), "kind": "empty_response",
                })
            return candles[-count:] if isinstance(candles, list) and count else candles
        except asyncio.TimeoutError:
            if self.enable_logging:
                logger.warning(f"[CANDLE-DEBUG] TIMEOUT asset={asset} period={timeframe}")
                logger.warning(f"Candle request timed out for {asset} / {timeframe}")
            try:
                self._websocket.record_candle_transport_event(
                    "candle-future", event="candles_received", direction="internal",
                    asset=asset, period=int(timeframe), request_id=request_id, status="timeout"
                )
            except Exception:
                pass
            self._candle_request_errors.setdefault(request_id, {
                "error": None, "message": "timeout: no response received", "timestamp": time.time(), "kind": "timeout",
            })
            return []
        finally:
            # Let _on_candles_received own completion; just cleanup mapping here
            self._candle_requests.pop(request_id, None)

    def _record_unmatched_candle_response(self, asset: Optional[str], period: Optional[int],
                                          reason: str) -> None:
        """
        Part 3 approved fix. A candle response that cannot be safely
        correlated to a currently-pending request is recorded here for
        diagnostics ONLY. It is never used to resolve or short-circuit
        any pending future and is never merged into any asset's cache —
        see _on_candles_received() for the enforcement of that rule.
        Bounded to the most recent 50 entries to avoid unbounded growth.
        """
        self._unmatched_candle_responses.append({
            "asset": asset, "period": period, "reason": reason, "timestamp": time.time(),
        })
        if len(self._unmatched_candle_responses) > 50:
            self._unmatched_candle_responses = self._unmatched_candle_responses[-50:]

    def get_recent_unmatched_candle_responses(self, since_ts: float) -> List[Dict[str, Any]]:
        """
        Diagnostics-only accessor (Part 3 approved fix), symmetric with
        get_recent_uncorrelated_candle_errors(). Returns candle responses
        that arrived but could not be safely correlated to any pending
        request — e.g. a late response for an already-timed-out request,
        or a response missing asset/period entirely. Never used to
        resolve any request; informational only.
        """
        return [e for e in self._unmatched_candle_responses if e.get("timestamp", 0) >= since_ts]


    def get_candle_transport_diagnostics(self) -> Dict[str, Any]:
        """Return secret-free WebSocket transport trace for the last diagnostics."""
        try:
            return self._websocket.get_candle_transport_diagnostics()
        except Exception:
            return {"pending_binary_events": [], "recent_trace": []}

    async def _on_candles_received(self, data: Dict[str, Any]) -> None:
        """
        Normalize Quotex candle-history payloads and correlate them safely.

        Correlation rule (Part 3 approved fix): the server may omit
        asset/period from the binary history payload because those values are
        already implied by the request. If exactly one candle request is
        pending, that request is the only safe correlation target. If multiple
        requests are pending, metadata-less payloads are recorded as unmatched
        rather than guessed.
        """
        try:
            if self.enable_logging:
                if isinstance(data, dict):
                    safe_keys = list(data.keys())[:30]
                    logger.warning(f"[CANDLE-DEBUG] HANDLER event data_type=dict keys={safe_keys}")
                elif isinstance(data, list):
                    logger.warning(f"[CANDLE-DEBUG] HANDLER event data_type=list length={len(data)} first_type={type(data[0]).__name__ if data else 'empty'}")
                else:
                    logger.warning(f"[CANDLE-DEBUG] HANDLER event data_type={type(data).__name__}")
            payload: Any = data

            # Unwrap common envelopes.
            if isinstance(payload, dict) and isinstance(payload.get("data"), (dict, list)):
                nested = payload.get("data")
                if isinstance(nested, dict) and any(k in nested for k in ("asset", "period", "history", "candles")):
                    payload = nested
                elif isinstance(nested, list):
                    payload = nested

            asset = payload.get("asset") if isinstance(payload, dict) else None
            period = payload.get("period") if isinstance(payload, dict) else None

            candles_raw: List[Any] = []
            if isinstance(payload, dict):
                if isinstance(payload.get("candles"), list):
                    candles_raw = payload["candles"]
                elif isinstance(payload.get("history"), list):
                    candles_raw = payload["history"]
                elif isinstance(payload.get("history"), dict):
                    candles_raw = list(payload["history"].values())
                elif isinstance(payload.get("data"), dict):
                    nested = payload["data"]
                    if isinstance(nested.get("candles"), list):
                        candles_raw = nested["candles"]
                    elif isinstance(nested.get("history"), list):
                        candles_raw = nested["history"]
                    elif isinstance(nested.get("history"), dict):
                        candles_raw = list(nested["history"].values())
            elif isinstance(payload, list):
                candles_raw = payload

            # If metadata is absent, correlate only when exactly one request is pending.
            if asset is None or period is None:
                pending = [
                    (rid, fut) for rid, fut in self._candle_requests.items()
                    if fut is not None and not fut.done()
                ]
                if len(pending) == 1:
                    rid, _ = pending[0]
                    if "_" in rid:
                        asset, period_text = rid.rsplit("_", 1)
                        try:
                            period = int(period_text)
                        except ValueError:
                            period = None
                if asset is None or period is None:
                    self._record_unmatched_candle_response(asset, period, reason="missing_asset_or_period")
                    if self.enable_logging:
                        logger.warning(f"[CANDLE-DEBUG] UNMATCHED missing_asset_or_period pending={list(self._candle_requests.keys())}")
                    return

            period_int = int(period)
            if self.enable_logging:
                logger.warning(f"[CANDLE-DEBUG] PARSE asset={asset} period={period_int} raw_count={len(candles_raw)}")
            parsed = self._parse_candles_data(candles_raw, str(asset), period_int)
            if self.enable_logging:
                logger.warning(f"[CANDLE-DEBUG] PARSED asset={asset} period={period_int} candle_count={len(parsed)}")

            if parsed:
                if not hasattr(self, "_fast_store"):
                    self._fast_store = FastCandleStore()
                self._fast_store.add_many(str(asset), period_int, parsed)

            rid = f"{asset}_{period_int}"
            waiter = self._candle_requests.get(rid)
            if waiter and not waiter.done():
                waiter.set_result(parsed)
            else:
                self._record_unmatched_candle_response(
                    str(asset), period_int, reason="no_matching_pending_request"
                )

        except Exception as e:
            if self.enable_logging:
                logger.error(f"Error in candles handler: {e}")
            # Do not resolve unrelated requests on a parsing/correlation failure.
        finally:
            await self._emit_event("candles_received", data)

    def _parse_candles_data(self, candles_data: List[Any], asset: str, timeframe: int) -> List["Candle"]:
        """
        Robust parser for Quotex history rows. Known gateways have emitted
        both [ts, open, close, high, low, ticks] and
        [ts, open, low, high, close, volume] layouts. For list rows, infer
        the layout from OHLC consistency instead of silently swapping close
        with low. Dict rows remain field-name based.
        """
        candles: List["Candle"] = []
        try:
            if isinstance(candles_data, list):
                for row in candles_data:
                    # Standard Quotex history rows are arrays. Accept the
                    # equivalent dict form as well because some gateways
                    # serialize OHLCV rows as named fields.
                    if isinstance(row, dict):
                        ts = row.get("timestamp", row.get("time", row.get("at")))
                        o = row.get("open", row.get("o"))
                        raw_low = row.get("low", row.get("l"))
                        raw_high = row.get("high", row.get("h"))
                        c = row.get("close", row.get("c"))
                        vol = row.get("volume", row.get("v", 0.0))
                        if any(v is None for v in (ts, o, raw_low, raw_high, c)):
                            continue
                    elif isinstance(row, (list, tuple)) and len(row) >= 5:
                        ts = row[0]
                        o = row[1]
                        a = row[2]
                        b = row[3]
                        d = row[4]
                        vol = row[5] if len(row) > 5 and row[5] is not None else 0.0
                    else:
                        continue
                    try:
                        ts = float(ts)
                        # Accept both Unix seconds and Unix milliseconds.
                        if ts > 2_000_000_000:
                            ts /= 1000.0
                        o = float(o)
                        if isinstance(row, (list, tuple)):
                            a = float(a)
                            b = float(b)
                            d = float(d)

                            # Candidate 1: [open, low, high, close]
                            c1 = d
                            lo1, hi1 = min(a, b), max(a, b)
                            valid1 = lo1 <= min(o, c1) <= max(o, c1) <= hi1

                            # Candidate 2: [open, close, high, low]
                            c2 = a
                            lo2, hi2 = min(b, d), max(b, d)
                            valid2 = lo2 <= min(o, c2) <= max(o, c2) <= hi2

                            if valid2 and not valid1:
                                c, lo, hi = c2, lo2, hi2
                            elif valid1 and not valid2:
                                c, lo, hi = c1, lo1, hi1
                            elif valid2 and valid1:
                                # Ambiguous but both geometries are valid;
                                # prefer the current Quotex history layout
                                # [open, close, high, low].
                                c, lo, hi = c2, lo2, hi2
                            else:
                                # Malformed OHLC row; do not fabricate data.
                                continue
                        else:
                            o = float(o)
                            raw_low = float(raw_low)
                            raw_high = float(raw_high)
                            c = float(c)
                            hi = max(raw_high, raw_low)
                            lo = min(raw_high, raw_low)

                        vol = float(vol) if vol is not None else 0.0
                        candle = Candle(
                            # Patch 1 (Phase 2.8) — explicit UTC. Was previously
                            # datetime.fromtimestamp(ts) with no tz=, which
                            # interprets epoch seconds in the SERVER's local
                            # system timezone rather than UTC — ambiguous and
                            # environment-dependent. This is the only candle-
                            # timestamp conversion site; order/deal timestamps
                            # (_parse_timestamp()) and _qx_expiration_epoch()
                            # are unrelated and intentionally left untouched.
                            timestamp=datetime.fromtimestamp(ts, tz=timezone.utc),
                            open=o, high=hi, low=lo, close=c, volume=vol,
                            asset=asset, timeframe=int(timeframe)
                        )
                        candles.append(candle)
                    except (ValueError, TypeError):
                        continue
        except Exception as e:
            if self.enable_logging:
                logger.error(f"Error parsing candles data: {e}")
        return candles
    # endregion

    # region Event Handling
    async def _on_authenticated(self, data: Any) -> None:
        try:
            logger.info("Authenticated with server")
            await self.send_message('451-["instruments/list",{"_placeholder":true,"num":0}]')
        except Exception as e:
            logger.error(f"_on_authenticated error: {e}")

    async def _on_auth_error(self, data: Dict[str, Any]) -> None:
        logger.error(f"Authentication error: {data.get('message', 'Unknown error')}")
        await self._error_monitor.record_error(
            error_type="authentication_error",
            severity=ErrorSeverity.CRITICAL,
            category=ErrorCategory.AUTHENTICATION,
            message=f"Authentication error: {data.get('message', 'Unknown error')}",
            context={"data": str(data)[:100]}
        )
        await self._emit_event("auth_error", data)

        creds = self._config.load_config()
        email = creds.get("email")
        password = creds.get("password")
        email_pass = creds.get("email_pass")

        if email and password:
            logger.info("Attempting to refresh SSID due to authentication error")
            success, session_data = await get_ssid(email, password, email_pass, is_demo=self.is_demo)
            if success:
                self.raw_ssid = session_data["ssid"]
                self.session_id = session_data["token"]
                logger.info("New SSID obtained, attempting reconnection")
                await self.disconnect()
                await self.connect()
            else:
                logger.error("Failed to refresh SSID")
                await self._error_monitor.record_error(
                    error_type="ssid_refresh_failed",
                    severity=ErrorSeverity.CRITICAL,
                    category=ErrorCategory.AUTHENTICATION,
                    message="Failed to refresh SSID after authentication error",
                    context={"email": email}
                )

    async def _on_balance_updated(self, data: Dict[str, Any]) -> None:
        if data.get("balance") is None:
            logger.debug(f"Ignoring empty balance_updated event: {data}")
            return

        try:
            balance_value = float(data.get("balance"))
            balance = Balance(
                balance=balance_value,
                currency=data.get("currency", "USD"),
                is_demo=self.is_demo,
            )
            self._balance = balance

            if self.enable_logging:
                logger.info(f"Balance updated: ${balance.balance:.2f}")

            await self._emit_event("balance_updated", data)

        except Exception as e:
            logger.error(f"Failed to update balance: {str(e)}")
            await self._error_monitor.record_error(
                error_type="balance_update_failed",
                severity=ErrorSeverity.MEDIUM,
                category=ErrorCategory.DATA,
                message=f"Failed to update balance: {str(e)}",
                context={"data": str(data)[:100]}
            )

    async def _on_balance_data(self, payload: dict) -> None:
        try:
            self.uid = int(payload.get("uid") or self.uid or 0)
            if "isDemo" in payload:
                try:
                    self.is_demo = bool(int(payload["isDemo"]))
                except Exception:
                    self.is_demo = bool(payload["isDemo"])

            balance_value = float(payload.get("balance") or 0.0)
            self._balance = Balance(
                balance=balance_value,
                currency="USD",
                is_demo=self.is_demo,
                last_updated=datetime.now(),
            )
            await self._emit_event("balance_updated", self._balance)
            logger.info(f"Balance updated: uid={self.uid}, balance={self._balance.balance}")

        except Exception as e:
            logger.error(f"_on_balance_data error: {e}")

    async def _parse_timestamp(self, ts: Union[str, int, float], default: datetime = None) -> Optional[datetime]:
        """Parse timestamp which could be a number (Unix timestamp) or a string in 'YYYY-MM-DD HH:MM:SS' format."""
        if ts is None:
            return default
        try:
            if isinstance(ts, (int, float)):
                return datetime.fromtimestamp(ts / 1000 if ts > 2_000_000_000 else ts)
            elif isinstance(ts, str):
                try:
                    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    return datetime.fromtimestamp(float(ts) / 1000 if float(ts) > 2_000_000_000 else float(ts))
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse timestamp {ts}: {str(e)}")
            return default
        return default

    async def _on_order_opened(self, payload: Any) -> None:
        try:
            # Ignore textual ACKs like "OPEN" that arrive via successopenOrder
            if isinstance(payload, str):
                if payload.strip().upper() == "OPEN":
                    logger.debug("Ignoring textual ACK 'OPEN' for order_opened")
                    return
                logger.debug(f"Ignoring string payload in _on_order_opened: {payload}")
                return

            if not isinstance(payload, dict):
                logger.warning(f"Unexpected payload for _on_order_opened: {type(payload)}")
                return

            logger.debug(f"Order opened payload: {json.dumps(payload)}")

            server_order_id = str(payload.get("id") or payload.get("orderId") or "")
            req_id = str(payload.get("requestId") or "")
            uid = int(payload.get("uid") or (self.uid or 0))
            acc_bal = float(payload.get("accountBalance") or payload.get("balance") or 0.0)

            # Map both directions
            if req_id and server_order_id:
                self._request_id_to_server_id[req_id] = server_order_id
                self._server_order_index[server_order_id] = req_id

            pending_order = self._pending_order_requests.get(req_id)
            cmd = payload.get("command")
            direction = None
            if cmd is not None:
                try:
                    direction = OrderDirection.CALL if int(cmd) == 0 else OrderDirection.PUT
                except (ValueError, TypeError):
                    direction = pending_order.direction if pending_order else None

            opened_at = await self._parse_timestamp(payload.get("openTime") or payload.get("openTimestamp"), default=datetime.now())
            closed_at = await self._parse_timestamp(payload.get("closeTime") or payload.get("closeTimestamp"))

            item = OrderResult(
                order_id=req_id or server_order_id or str(uuid.uuid4()),
                broker_order_id=server_order_id or None,
                status=OrderStatus.OPEN,       # requires OPEN in models.OrderStatus
                profit=None,
                open_price=float(payload.get("openPrice") or 0.0),
                close_price=None,
                asset=str(payload.get("asset") or (pending_order.asset if pending_order else "")),
                amount=float(payload.get("amount") or (pending_order.amount if pending_order else 0.0)),
                direction=direction,
                duration=int(payload.get("duration") or (pending_order.duration if pending_order else 0)),
                placed_at=opened_at,
                expires_at=closed_at,
                uid=uid,
            )

            # Track OPEN state by requestId (waiter relies on this)
            key = req_id or server_order_id
            self._active_orders[key] = item

            # Update balance if present
            if acc_bal > 0:
                self._balance = Balance(
                    balance=acc_bal,
                    currency="USD",
                    is_demo=self.is_demo,
                    last_updated=datetime.now(),
                )
                await self._emit_event("balance_updated", self._balance)

            logger.info(f"Order OPENED: req={req_id}, id={server_order_id}, uid={uid}, asset={item.asset}, amount={item.amount}")
            await self._emit_event("order_opened", item)

        except Exception as e:
            logger.error(f"_on_order_opened error: {str(e)}")
            await self._error_monitor.record_error(
                error_type="order_opened_error",
                severity=ErrorSeverity.MEDIUM,
                category=ErrorCategory.TRADING,
                message=f"Error processing order opened: {str(e)}",
                context={"payload": (json.dumps(payload)[:100] if isinstance(payload, dict) else str(payload)[:100])}
            )

    async def _on_order_closed(self, payload: Dict[str, Any]) -> None:
        """
        Handle a single closed order 'deal' payload coming from:
          - s_orders/close -> deals[N]
          - orders/closed/list -> each item
        Keeps original logic/formatting; only fixes logger var & result mapping.
        """
        try:
            deal = payload or {}

            # Required primary keys
            server_id = str(deal.get("id") or "").strip()
            asset = str(deal.get("asset") or "")
            amount = float(deal.get("amount") or 0.0)
            profit_val = float(deal.get("profit") or 0.0)

            # Try direct map: requestId -> server_id (filled on _on_order_opened)
            req_key = None
            if server_id and server_id in self._server_order_index:
                req_key = self._server_order_index.get(server_id)

            # Fallback: infer by time proximity & asset
            if not req_key and asset and ("openTime" in deal or "openTimestamp" in deal):
                try:
                    open_epoch = await self._parse_timestamp(deal.get("openTime") or deal.get("openTimestamp"))
                    for k, v in list(self._active_orders.items()):
                        if v.asset == asset and v.placed_at:
                            if abs(int(v.placed_at.timestamp()) - open_epoch) <= 5:
                                req_key = k
                                break
                    else:
                        # last resort: first match by asset
                        for k, v in list(self._active_orders.items()):
                            if v.asset == asset:
                                req_key = k
                                break
                except Exception:
                    pass

            # Last fallback: invert map request->server (slower, safe)
            if not req_key and self._request_id_to_server_id:
                for k, v in list(self._request_id_to_server_id.items()):
                    if v == server_id:
                        req_key = k
                        break

            # Pull timing/prices
            item_open_ts = await self._parse_timestamp(deal.get("openTime") or deal.get("openTimestamp"))
            item_close_ts = await self._parse_timestamp(deal.get("closeTime") or deal.get("closeTimestamp"))
            open_price = float(deal.get("openPrice") or 0.0)
            close_price = float(deal.get("closePrice") or 0.0)

            base_item = self._active_orders.get(req_key or server_id)

            # Direction: 0=CALL, 1=PUT per server (same as your existing logic)
            direction = (
                OrderDirection.CALL if deal.get("command") == 0 else
                OrderDirection.PUT if deal.get("command") == 1 else
                (base_item.direction if base_item else None)
            )

            # Classify result
            if profit_val > 0:
                status = OrderStatus.WIN
            elif profit_val < 0:
                status = OrderStatus.LOSS
            else:
                status = OrderStatus.DRAW

            duration_val = int(deal.get("duration") or (base_item.duration if base_item else 0))
            uid_val = int(deal.get("uid") or self.uid or 0)

            final = OrderResult(
                order_id=(req_key or server_id or str(uuid.uuid4())),
                broker_order_id=server_id or (base_item.broker_order_id if base_item else None),
                status=status,
                profit=profit_val,
                open_price=open_price if open_price else (base_item.open_price if base_item else 0.0),
                close_price=close_price if close_price else (base_item.close_price if base_item else 0.0),
                asset=asset or (base_item.asset if base_item else ""),
                amount=amount if amount else (base_item.amount if base_item else 0.0),
                direction=direction,
                duration=duration_val,
                placed_at=item_open_ts or (base_item.placed_at if base_item else datetime.now()),
                expires_at=item_close_ts or (base_item.expires_at if base_item else None),
                uid=uid_val
            )

            # Optional: carry payout if present
            try:
                payout = float(deal.get("percentProfit") or deal.get("payout") or 0.0)
                final = final.copy(update={"payout": payout})
            except Exception:
                pass

            # Move active -> results
            if (req_key or server_id) in self._active_orders:
                self._active_orders.pop(req_key or server_id, None)
            self._order_results[final.order_id] = final

            # Clean pending if any
            if final.order_id in self._pending_order_requests:
                self._pending_order_requests.pop(final.order_id, None)

            logger.info(f"Order CLOSED: req={req_key} id={server_id} status={final.status} profit={final.profit}")
            await self._emit_event("order_closed", final)

        except Exception as e:
            logger.error(f"_on_order_closed error: {str(e)}")
            await self._error_monitor.record_error(
                error_type="order_closed_error",
                severity=ErrorSeverity.MEDIUM,
                category=ErrorCategory.TRADING,
                message=str(e),
                context={"payload_keys": list(payload.keys()) if isinstance(payload, dict) else type(payload).__name__}
            )

    async def _on_stream_update(self, data: Dict[str, Any]) -> None:
        if self.enable_logging:
            logger.debug(f"Stream update received: {data}")
        await self._emit_event("stream_update", data)

    async def _on_price_update(self, data: Dict[str, Any]) -> None:
        logger.debug(f"Price update received: {data}")
        await self._emit_event("price_update", data)

    async def _on_quote_stream(self, data: Dict[str, Any]) -> None:
        if self.enable_logging:
            logger.debug(f"Quote stream received: {data}")
        try:
            if isinstance(data, list) and len(data) > 0:
                for quote in data:
                    if isinstance(quote, list) and len(quote) >= 3 and isinstance(quote[0], str):
                        asset = quote[0]
                        timestamp = quote[1]
                        price = float(quote[2])
                        try:
                            self._payout_data[asset] = price
                        except Exception:
                            pass
                        await self._on_price_update({
                            "symbol": asset,
                            "timestamp": timestamp,
                            "price": price
                        })

            await self._emit_event("quote_stream", data)

        except Exception as e:
            logger.error(f"Failed to process quote stream: {str(e)}")
            await self._error_monitor.record_error(
                error_type="quote_stream_failed",
                severity=ErrorSeverity.MEDIUM,
                category=ErrorCategory.DATA,
                message=f"Failed to process quote stream: {str(e)}",
                context={"data": str(data)[:100]}
            )

    async def _on_depth_change(self, data: Dict[str, Any]) -> None:
        if self.enable_logging:
            logger.debug(f"Depth change received: {data}")
        try:
            asset = data.get("asset", "N/A")
            depth_data = data.get("depth", {})
            if asset != "N/A" and depth_data:
                logger.info(f"Processed depth change for asset: {asset}")
                self._payout_data[asset] = depth_data.get("payout", self._payout_data.get(asset, 0.0))
            await self._emit_event("depth_change", data)
        except Exception as e:
            logger.error(f"Failed to process depth change: {str(e)}")
            await self._error_monitor.record_error(
                error_type="depth_change_failed",
                severity=ErrorSeverity.MEDIUM,
                category=ErrorCategory.DATA,
                message=f"Failed to process depth change: {str(e)}",
                context={"data": str(data)[:100]}
            )

    async def _on_keep_alive_connected(self, data: Dict[str, Any]) -> None:
        if self.enable_logging:
            logger.info("Keep-alive connection established")
        self.websocket_is_connected = True
        await self._emit_event("connected", data)

    async def _on_keep_alive_reconnected(self, data: Dict[str, Any]) -> None:
        if self.enable_logging:
            logger.info("Keep-alive reconnected")
        self.websocket_is_connected = True
        await self._emit_event("reconnected", data)

    async def _on_keep_alive_message(self, data: Dict[str, Any]) -> None:
        if self.enable_logging:
            logger.debug(f"Keep-alive message received: {data}")
        await self._emit_event("message_received", data)

    async def _on_balance_list(self, data: Dict[str, Any]) -> None:
        logger.info(f"Balance list received: {data}")
        await self._emit_event("balance_list_updated", data)

    async def _on_settings_list(self, data: Dict[str, Any]) -> None:
        logger.info(f"Settings list received: {data}")
        await self._emit_event("settings_list_updated", data)

    async def _on_orders_opened_list(self, data: Dict[str, Any]) -> None:
        logger.info(f"Opened orders list received: {data}")
        try:
            if isinstance(data, list):
                for order in data:
                    try:
                        order_id = str(order.get("id") or order.get("orderId") or "")
                        req_id = str(order.get("requestId") or "")
                        pending_order = self._pending_order_requests.get(req_id)

                        cmd = order.get("command")
                        direction = OrderDirection.CALL if cmd == 0 else OrderDirection.PUT if cmd == 1 else (pending_order.direction if pending_order else None)

                        opened_at = await self._parse_timestamp(order.get("openTime") or order.get("openTimestamp"), default=datetime.now())
                        closed_at = await self._parse_timestamp(order.get("closeTime") or order.get("closeTimestamp"))

                        item = OrderResult(
                            order_id=req_id or order_id or str(uuid.uuid4()),
                            broker_order_id=order_id or None,
                            status=OrderStatus.OPEN,
                            profit=None,
                            open_price=float(order.get("openPrice") or 0.0),
                            close_price=None,
                            asset=str(order.get("asset") or (pending_order.asset if pending_order else "")),
                            amount=float(order.get("amount") or (pending_order.amount if pending_order else 0.0)),
                            direction=direction,
                            duration=int(order.get("duration") or (pending_order.duration if pending_order else 0)),
                            placed_at=opened_at,
                            expires_at=closed_at,
                            uid=int(order.get("uid") or self.uid or 0),
                        )

                        self._active_orders[item.order_id] = item
                        if req_id and order_id:
                            self._request_id_to_server_id[req_id] = order_id
                            self._server_order_index[order_id] = req_id
                    except Exception:
                        continue

            await self._emit_event("orders_opened_updated", data)

        except Exception as e:
            logger.error(f"Error processing opened orders list: {str(e)}")

    async def _on_drawing_load(self, data: Dict[str, Any]) -> None:
        logger.info(f"Drawing load data received: {data}")
        await self._emit_event("drawing_load_updated", data)

    async def _on_orders_closed_list(self, data: List[Dict[str, Any]]) -> None:
        """
        Handle 'orders/closed/list' bulk payload.
        The server may push this list; process each item via the single-deal handler.
        """
        try:
            if not isinstance(data, list):
                return
            for item in data:
                await self._on_order_closed(item)
        except Exception as e:
            logger.error(f"_on_orders_closed_list error: {str(e)}")
            await self._error_monitor.record_error(
                error_type="orders_closed_list_error",
                severity=ErrorSeverity.MEDIUM,
                category=ErrorCategory.TRADING,
                message=str(e),
                context={"items": len(data) if isinstance(data, list) else 0}
            )

    async def _on_error(self, data: Dict[str, Any]) -> None:
        logger.error(f"Received error: {data}")

        if data.get("error") == "not_money":
            logger.warning("Order placement failed due to insufficient funds")
            await self._error_monitor.record_error(
                error_type="insufficient_funds",
                severity=ErrorSeverity.HIGH,
                category=ErrorCategory.INSUFFICIENT_FUNDS,
                message="Order placement failed due to insufficient funds",
                context={"data": str(data)[:100]}
            )

        try:
            req_id = str(data.get("requestId") or "")
            if req_id and req_id in self._pending_order_requests:
                self._pending_order_requests.pop(req_id, None)
                logger.warning(f"Order error received for requestId={req_id}: {data.get('message') or data.get('error')}")
        except Exception:
            pass

        # Approved general fix — candle-request protocol error correlation.
        # Order-error handling above is untouched. This is additive and
        # only concerns self._candle_requests / self._candle_request_errors,
        # never self._pending_order_requests or order futures.
        try:
            err_asset = data.get("asset") if isinstance(data, dict) else None
            err_period = data.get("period") if isinstance(data, dict) else None
            # Deliberately excludes QUOTEX_SSID/credentials/cookies/headers —
            # only the server's own error/message text and timing, same
            # shape as what's already logged above.
            safe_error = {
                "error": data.get("error") if isinstance(data, dict) else None,
                "message": data.get("message") if isinstance(data, dict) else None,
                "timestamp": time.time(),
                "kind": "protocol_error",
            }
            if err_asset is not None and err_period is not None:
                # Reliable correlation: the server echoed asset+period,
                # exactly like a real candle response does — safe to tie
                # to that EXACT pending request only (never a different
                # one), matching self._candle_requests' own keying.
                rid = f"{err_asset}_{int(err_period)}"
                self._candle_request_errors[rid] = safe_error
                waiter = self._candle_requests.get(rid)
                if waiter and not waiter.done():
                    # Resolve immediately (empty result) instead of letting
                    # the caller wait out the full timeout for a request
                    # the server already told us failed. _request_candles()
                    # still returns [] on this path (unchanged contract);
                    # get_last_candle_error() lets the caller find out why.
                    waiter.set_result([])
            else:
                # Cannot reliably tell which in-flight request (if any)
                # this belongs to — never guess/attach it to one. Recorded
                # only as a soft, timestamped, clearly-uncorrelated hint;
                # bounded so it can't grow unboundedly.
                self._recent_uncorrelated_candle_errors.append(safe_error)
                self._recent_uncorrelated_candle_errors = self._recent_uncorrelated_candle_errors[-5:]
        except Exception:
            # Never let this diagnostic bookkeeping break error handling.
            pass

        await self._emit_event("error", data)

    async def _on_json_data(self, data: Any) -> None:
        try:
            # Handle dictionary-style events (canonical JSON)
            if isinstance(data, dict):
                event_type = data.get("event")
                event_data = data.get("data")

                # Handle raw balance structure
                if "uid" in data and "balance" in data and isinstance(data.get("balance"), (int, float, str)):
                    await self._on_balance_data(data)

                if event_type:
                    logger.info(f"Received JSON event: type={event_type}, data={str(event_data)[:100]}")

                    if event_type == "s_balance":
                        await self._on_balance_data(event_data)
                        return

                    if event_type == "instruments/list":
                        await self._on_assets_updated(event_data)
                        return

                    if event_type == "quotes/stream":
                        await self._on_quote_stream(event_data)
                        return

                    if event_type in ("history/list", "history/list/v2", "loadHistoryPeriod"):
                        await self._on_candles_received(event_data)
                        return

                    if event_type == "chart_notification/get":
                        await self._on_candles_received(event_data)
                        return

                    if event_type == "s_orders/open":
                        await self._on_order_opened(event_data)
                        return

                    if event_type == "successopenOrder":
                        if isinstance(event_data, str):
                            logger.debug(f"Ignoring textual ACK for open: {event_data}")
                            return
                        if isinstance(event_data, dict):
                            await self._on_order_opened(event_data)
                            return

                    if event_type == "s_orders/close":
                        await self._on_order_closed(event_data)
                        return

                    if event_type == "successcloseOrder":
                        await self._on_order_closed(event_data)
                        return

                    if event_type == "orders/closed/list":
                        await self._on_orders_closed_list(event_data)
                        return

            # Handle list-style events: ["event_name", payload]
            elif isinstance(data, list) and len(data) > 1:
                event_type = data[0]
                event_data = data[1]

                logger.info(f"Received JSON event: type={event_type}, data={str(event_data)[:100]}")

                if event_type == "s_authorization":
                    await self._emit_event("authenticated", event_data)
                    self.websocket_is_connected = True
                    self.ssl_mutual_exclusion = False
                    return

                if event_type == "instruments/list":
                    await self._on_assets_updated(event_data)
                    return

                if event_type == "quotes/stream":
                    await self._on_quote_stream(event_data)
                    return

                if event_type in ("history/list", "history/list/v2", "loadHistoryPeriod"):
                    await self._on_candles_received(event_data)
                    return

                if event_type == "chart_notification/get":
                    await self._on_candles_received(event_data)
                    return

                if event_type == "error":
                    await self._on_error(event_data)
                    return

            # Fallback logger
            logger.debug(f"Received JSON data (unclassified): {str(data)[:100]}")
            await self._emit_event("json_data", data)

        except Exception as e:
            logger.error(f"_on_json_data error: {e}")

    async def _on_unknown_event(self, data: Dict[str, Any]) -> None:
        try:
            event_type = data.get('type', 'unknown')
            event_data = data.get('data', {})
            data_str = str(event_data)[:100] if event_data else ""
            logger.warning(f"Unknown event received: type={event_type}, data={data_str}...")
            await self._error_monitor.record_error(
                error_type="unknown_event",
                severity=ErrorSeverity.MEDIUM,
                category=ErrorCategory.DATA,
                message=f"Received unknown event: {event_type}",
                context={"event_type": event_type, "event_data": data_str}
            )
        except Exception as e:
            logger.error(f"Error processing unknown event: {str(e)}")
            await self._error_monitor.record_error(
                error_type="unknown_event_processing_error",
                severity=ErrorSeverity.MEDIUM,
                category=ErrorCategory.SYSTEM,
                message=f"Error processing unknown event: {str(e)}",
                context={"event_type": str(data.get('type', 'unknown'))}
            )

    async def _emit_event(self, event: str, data: Any) -> None:
        if event in self._event_callbacks:
            for callback in self._event_callbacks[event]:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(data)
                    else:
                        callback(data)
                except Exception as e:
                    if self.enable_logging:
                        logger.error(f'Error in event callback for {event}: {str(e)}')
                    await self._error_monitor.record_error(
                        error_type="event_callback_error",
                        severity=ErrorSeverity.MEDIUM,
                        category=ErrorCategory.SYSTEM,
                        message=f"Error in event callback for {event}: {str(e)}",
                        context={"event": event, "data": str(data)[:100]}
                    )

    def add_event_callback(self, event: str, callback: Callable) -> None:
        if event not in self._event_callbacks:
            self._event_callbacks[event] = []
        self._event_callbacks[event].append(callback)

    def remove_event_callback(self, event: str, callback: Callable) -> None:
        if event in self._event_callbacks:
            try:
                self._event_callbacks[event].remove(callback)
            except ValueError:
                pass
    # endregion

    # region Helper Methods
    def _parse_complete_ssid(self, ssid: str) -> None:
        try:
            json_start = ssid.find("{")
            json_end = ssid.rfind("}") + 1
            if json_start != -1 and json_end > json_start:
                json_part = ssid[json_start:json_end]
                data = json.loads(json_part)
                self.session_id = data.get("session", "")
                self._original_demo = bool(data.get("isDemo", 1))
                self._complete_ssid = None
        except Exception as e:
            logger.warning(f"Failed to parse SSID: {str(e)}")
            self.session_id = ssid
            self._complete_ssid = None

    def _format_session_message(self) -> str:
        auth_data = {
            "session": self.session_id,
            "isDemo": 1 if self.is_demo else 0,
            "tournamentId": 0,
        }
        if self.is_fast_history:
            auth_data["isFastHistory"] = True
        return f'42["authorization",{json.dumps(auth_data)}]'

    async def _wait_for_authentication(self, timeout: float = 30.0) -> None:

        # Early exit if handshake already completed before we started waiting
        try:
            if getattr(self._websocket, 'is_authenticated', False):
                return
        except Exception:
            pass
        auth_received = False

        def on_auth(data):
            nonlocal auth_received
            auth_received = True

        def on_auth_error(data):
            nonlocal auth_received
            auth_received = False
            raise AuthenticationError(f"Authentication rejected: {data.get('message', 'Unknown error')}")

        self._websocket.add_event_handler("authenticated", on_auth)
        self._websocket.add_event_handler("auth_error", on_auth_error)

        try:
            start_time = time.time()
            while not auth_received and (time.time() - start_time) < timeout:
                if getattr(self._websocket, 'is_authenticated', False):
                    break
                await asyncio.sleep(0.1)
            if not auth_received and not getattr(self._websocket, 'is_authenticated', False):
                raise AuthenticationError("Authentication timeout")
        finally:
            self._websocket.remove_event_handler("authenticated", on_auth)
            self._websocket.remove_event_handler("auth_error", on_auth_error)

    async def _on_auth_reject(self, data: dict) -> None:
        """Handle explicit authorization rejection by server."""
        if self.enable_logging:
            logger.error("Server rejected authorization; closing connection.")
        try:
            await self.disconnect()
        finally:
            raise QuotexError("Authorization rejected by server")

    async def _initialize_data(self) -> None:
        await self._request_balance_update()
        await self._setup_time_sync()

    async def _setup_time_sync(self) -> None:
        local_time = datetime.now().timestamp()
        self._server_time = ServerTime(server_timestamp=local_time, local_timestamp=local_time, offset=0.0)

    def _is_otc(self, asset: str) -> bool:
        return "_otc" in (asset or "").lower()

    def _parse_seconds(self, duration) -> int:
        if isinstance(duration, (int, float)):
            return max(int(duration), 1)

        s = str(duration).strip().lower()
        try:
            if ":" in s or "-" in s:
                from datetime import datetime, date
                if "-" in s:
                    fmt = "%Y-%m-%d %H:%M:%S" if s.count(":") == 2 else "%Y-%m-%d %H:%M"
                else:
                    fmt = "%H:%M:%S" if s.count(":") == 2 else "%H:%M"
                dt = datetime.strptime(s, fmt)
                if "-" not in s:
                    today = date.today()
                    dt = dt.replace(year=today.year, month=today.month, day=today.day)
                diff = int(dt.timestamp() - time.time())
                return max(diff, 1)

            if s.endswith("ms"):
                return max(int(float(s[:-2]) / 1000.0), 1)
            if s.endswith("s"):
                return max(int(float(s[:-1])), 1)
            if s.endswith("m"):
                return max(int(float(s[:-1]) * 60), 1)
            if s.endswith("h"):
                return max(int(float(s[:-1]) * 3600), 1)
            return max(int(float(s)), 1)
        except Exception:
            raise InvalidParameterError(f"Unrecognized duration: {duration}")

    def _qx_expiration_epoch(self, now_ts: int, minutes: int) -> int:
        from datetime import datetime, timedelta
        now = datetime.fromtimestamp(now_ts)
        shift = 1 if now.second >= 30 else 0
        base = now.replace(second=0, microsecond=0) + timedelta(minutes=shift + int(minutes))
        return int(time.mktime(base.timetuple()))

    def _compute_order_time_and_type(self, asset: str, duration) -> tuple[int, int]:
        is_otc = self._is_otc(asset)
        secs = int(self._parse_seconds(duration))
        if is_otc:
            return 100, secs
        mins = max(int((secs + 59) // 60), 1)
        return 1, self._qx_expiration_epoch(int(time.time()), mins)

    def _validate_order_parameters(self, asset: str, amount: float, direction: OrderDirection, duration: int) -> None:
        if asset not in ASSETS:
            raise InvalidParameterError(f"Invalid asset: {asset}")
        if amount < API_LIMITS["min_order_amount"] or amount > API_LIMITS["max_order_amount"]:
            raise InvalidParameterError(f"Amount must be between {API_LIMITS['min_order_amount']} and {API_LIMITS['max_order_amount']}")
        if duration < API_LIMITS["min_duration"] or duration > API_LIMITS["max_duration"]:
            raise InvalidParameterError(f"Duration must be between {API_LIMITS['min_duration']} and {API_LIMITS['max_duration']} seconds")
        if len(self._active_orders) >= API_LIMITS["max_concurrent_orders"]:
            raise OrderError("Maximum concurrent orders reached")
    # endregion
