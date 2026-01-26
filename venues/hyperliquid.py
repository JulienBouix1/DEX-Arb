
# -*- coding: utf-8 -*-
"""Hyperliquid venue adapter: robust WS (anti-429), BBO/L2 streaming, IOC fallback and equity probe."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Deque

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed

from core.orderbook import Book
from venues.hl_client import HLClient
from venues.base import VenueBase
from core.dns_utils import get_connector

log = logging.getLogger(__name__)

# ---- Rate limiting / WS tuning ----
# Default 4 Hz = 0.25s between subs to avoid Aster 3002 rate limit
SUB_RATE_LIMIT_HZ: float = float((__import__("os").environ.get("HL_SUB_RATE_LIMIT_HZ") or 4.0))
MAX_SUB_BURST: int = 5  # do at most 5 subs per second regardless
RESUB_MIN_PERIOD_S: float = 30.0  # do not resub the same coin more than every 30s

@dataclass
class OrderResult:
    status: str
    filled: float
    reason: str = ""
    dry_run: bool = False

class Hyperliquid(VenueBase):
    def __init__(self, cfg: Dict[str, Any]) -> None:
        import os
        self.rest_url: str = cfg["rest_url"].rstrip("/")
        self.ws_url: str = cfg["ws_url"].rstrip("/")
        self.fees_bps = cfg.get("fees", {})
        self.min_qty_usd: float = float(cfg.get("min_qty_usd", 10.0))
        self.ioc_guard_bps: float = float(cfg.get("ioc_guard_bps", 10.0))

        # Load user_address from config or environment variable
        user_addr_env = cfg.get("user_address_env", "HL_USER_ADDRESS")
        self.user_address: str = cfg.get("user_address") or os.environ.get(user_addr_env, "")

        self.private_key_env: str = str(cfg.get("private_key_env", "HL_PRIVATE_KEY"))
        self.label = 'HL'
        self._hl = HLClient(self.rest_url, self.user_address, self.private_key_env)



        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

        self._send_queue: asyncio.Queue = asyncio.Queue()
        self._subs: set[str] = set()
        self._last_sub_ts: Dict[str, float] = {}
        self._confirmed_subs: set[str] = set()  # Track confirmed subscriptions for diagnostics
        self._books: Dict[str, Book] = {}
        self._bbo: Dict[str, Tuple[float, float]] = {}  # symbol->(bid,ask)
        self._bbo_ts: Dict[str, float] = {}  # symbol->timestamp of last BBO update
        self._l2: Dict[str, Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]] = {}
        self._connected = asyncio.Event()

        # Volume Cache for liquidity-aware strategies
        self._cached_vols: Dict[str, float] = {}

        # tasks
        self._reader_task: Optional[asyncio.Task] = None
        self._writer_task: Optional[asyncio.Task] = None
        self._sub_task: Optional[asyncio.Task] = None
        self._staleness_task: Optional[asyncio.Task] = None

        # stats
        self._last_hb: float = 0.0
        self._messages_rx: int = 0
        self._messages_tx: int = 0
        self._last_error: str = ""

    def check_credentials(self) -> bool:
        """Verify that API keys/address are present and SDK is ready (does not validate against network)."""
        if not self.user_address:
            return False
        # HLClient checks PRIVATE_KEY env var internally during init
        return self._hl._sdk_ok

    # ---------- Lifecycle ----------
    async def start(self) -> None:
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=10)
            self._session = aiohttp.ClientSession(connector=get_connector(), timeout=timeout)
        if self._reader_task:
            return
        self._reader_task = asyncio.create_task(self._ws_reader_loop(), name="hl_ws_reader")
        self._writer_task = asyncio.create_task(self._ws_writer_loop(), name="hl_ws_writer")
        self._sub_task = asyncio.create_task(self._sub_throttle_loop(), name="hl_ws_sub_throttle")
        # DISABLED: Staleness monitor was causing connection instability by forcing reconnects
        # self._staleness_task = asyncio.create_task(self._staleness_monitor_loop(), name="hl_staleness_monitor")
        # self._ping_task = asyncio.create_task(self._ping_loop(), name="hl_ws_ping")
        await asyncio.wait_for(self._connected.wait(), timeout=15)

    async def close(self) -> None:
        for t in (self._reader_task, self._writer_task, self._sub_task, self._staleness_task, getattr(self, "_ping_task", None)):
            if t:
                t.cancel()
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._session:
            await self._session.close()
            self._session = None

    async def _ping_loop(self) -> None:
        """Send application-level pings to keep HL connection alive."""
        while True:
            await asyncio.sleep(40.0)
            try:
                await self._send_queue.put({"method": "ping"})
            except Exception:
                pass

    # ---------- Public API ----------
    async def subscribe_orderbook(self, coin: str) -> None:
        """Subscribe to BBO and L2 for a specific HL coin. Rate-limited to avoid 429."""
        coin = coin.upper()
        if coin in self._subs:
            return
        self._subs.add(coin)
        await self._enqueue_subscribe(coin)

    async def subscribe_tickers(self, symbols: List[str]) -> None:
        """Subscribe to real-time ticker/BBO updates for symbols."""
        for symbol in symbols:
            await self.subscribe_orderbook(symbol)

    def is_ws_healthy(self, max_age_ms: float = 5000.0) -> bool:
        """
        Check if WebSocket is connected and receiving data.
        FIX 2026-01-22: Added to detect WS disconnects that leave stale prices.

        Returns True only if:
        1. _connected event is set (WS appears connected)
        2. At least one subscribed symbol has fresh data (< max_age_ms)
        """
        if not self._connected.is_set():
            return False

        if not self._subs:
            # No subscriptions yet, consider healthy
            return True

        # Check if ANY symbol has fresh data
        now = time.time()
        for coin in self._subs:
            ts = self._bbo_ts.get(coin.upper(), 0.0)
            if ts > 0 and (now - ts) * 1000 < max_age_ms:
                return True

        # All subscribed symbols have stale data = unhealthy
        return False

    def get_bbo(self, coin: str) -> Optional[Tuple[float, float]]:
        return self._bbo.get(coin.upper())
    
    def get_bbo_with_depth(self, coin: str) -> Optional[Tuple[Tuple[float, float], Tuple[float, float]]]:
        """Return BBO with sizes: ((bid_price, bid_size), (ask_price, ask_size))"""
        coin = coin.upper()
        book = self._books.get(coin)
        if not book or not book.bids or not book.asks:
            # Fallback to BBO without size
            bbo = self._bbo.get(coin)
            if bbo:
                return ((bbo[0], 0.0), (bbo[1], 0.0))
            return None
        return ((book.bids[0][0], book.bids[0][1]), (book.asks[0][0], book.asks[0][1]))

    def get_aggregated_depth_usd(self, coin: str, levels: int = 5) -> float:
        """Sum depth across multiple orderbook levels for more accurate liquidity estimate."""
        coin = coin.upper()
        book = self._books.get(coin)
        if not book or not book.bids or not book.asks:
            return 0.0
        mid = (book.bids[0][0] + book.asks[0][0]) / 2
        bid_depth = sum(sz for _, sz in book.bids[:levels])
        ask_depth = sum(sz for _, sz in book.asks[:levels])
        return min(bid_depth, ask_depth) * mid

    def get_bbo_age_ms(self, coin: str) -> float:
        """Return age of BBO in milliseconds. Returns large value if no BBO."""
        ts = self._bbo_ts.get(coin.upper(), 0.0)
        if ts <= 0:
            return 999999.0
        return (time.time() - ts) * 1000.0
    
    async def fetch_bbo_rest(self, coin: str) -> Optional[Tuple[float, float]]:
        """Fetch BBO via REST API (fallback when WS is stale)."""
        url = f"{self.rest_url}/info"
        payload = {"type": "l2Book", "coin": coin.upper()}
        try:
            if not self._session or self._session.closed:
                self._session = aiohttp.ClientSession(connector=get_connector(), timeout=aiohttp.ClientTimeout(total=5))
            
            async with self._session.post(url, json=payload) as r:
                if r.status != 200:
                    log.warning(f"[HL] REST BBO fetch failed: status={r.status} text={await r.text()}")
                    return None
                data = await r.json()
                # FIX: Check if data is None before calling .get()
                if not data or not isinstance(data, dict):
                    log.debug(f"[HL] REST BBO fetch returned invalid data: {type(data)}")  # Reduced from WARNING - handled gracefully
                    return None
                levels = data.get("levels") or []
                # FIX: Robust null checking to prevent NoneType.get() errors
                if (len(levels) >= 2 and
                    levels[0] and isinstance(levels[0], list) and len(levels[0]) > 0 and
                    levels[1] and isinstance(levels[1], list) and len(levels[1]) > 0 and
                    levels[0][0] and isinstance(levels[0][0], dict) and
                    levels[1][0] and isinstance(levels[1][0], dict)):
                    best_bid = float(levels[0][0].get("px", 0) or 0)
                    best_ask = float(levels[1][0].get("px", 0) or 0)
                    if best_bid > 0 and best_ask > 0:
                        # Update cached BBO
                        self._bbo[coin.upper()] = (best_bid, best_ask)
                        self._bbo_ts[coin.upper()] = time.time()
                        return (best_bid, best_ask)
        except Exception as e:
            log.warning("[HL] REST BBO fetch exception: %s", e)
            # If session is broken, close it so next time we make a new one
            if self._session:
                try:
                    await self._session.close()
                except: pass
                self._session = None
        return None
    
    async def get_fresh_bbo(self, coin: str, max_age_ms: float = 2000.0) -> Optional[Tuple[float, float]]:
        """Get BBO, fetching via REST if WS data is stale."""
        age = self.get_bbo_age_ms(coin)
        if age <= max_age_ms:
            return self._bbo.get(coin.upper())
        # WS data is stale, try REST fallback
        return await self.fetch_bbo_rest(coin)
    
    def is_connected(self) -> bool:
        """Check if WS is currently connected and ready for orders."""
        return self._connected.is_set() and self._ws is not None
    
    async def get_funding_rates(self) -> Dict[str, float]:
        """Fetch current funding rates for all assets.
        Returns dict of coin -> funding rate (as decimal, e.g. 0.0001 = 0.01% = 1 bps).
        """
        url = f"{self.rest_url}/info"
        payload = {"type": "metaAndAssetCtxs"}
        try:
            if not self._session or self._session.closed:
                self._session = aiohttp.ClientSession(connector=get_connector(), timeout=aiohttp.ClientTimeout(total=10))
            
            async with self._session.post(url, json=payload) as r:
                if r.status != 200:
                    log.warning(f"[HL] Funding rates fetch failed: status={r.status}")
                    return {}
                data = await r.json()
                # Structure: [meta, assetCtxs]
                if not isinstance(data, list) or len(data) != 2:
                    return {}
                
                universe = data[0].get("universe", [])
                assetCtxs = data[1]
                
                rates: Dict[str, float] = {}
                # Universe and assetCtxs are aligned by index
                for i, item in enumerate(universe):
                    if i >= len(assetCtxs):
                        break
                    coin = str(item.get("name", "")).upper()
                    ctx = assetCtxs[i]
                    funding = ctx.get("funding")
                    
                    if coin and funding is not None:
                        try:
                            rates[coin] = float(funding)
                            # Also cache 24h volume (dayNtlVlm)
                            vol = ctx.get("dayNtlVlm")
                            if vol is not None:
                                self._cached_vols[coin] = float(vol)
                        except (ValueError, TypeError):
                            pass
                return rates
        except Exception as e:
            log.warning("[HL] Funding rates fetch exception: %s", e)
            return {}

    def get_book(self, coin: str) -> Optional[Book]:
        return self._books.get(coin.upper())

    async def equity(self) -> Optional[float]:
        return await self._hl.equity(self._session)

    async def get_positions(self) -> List[Dict[str, Any]]:
        """Fetch all open positions from Hyperliquid.
        Returns list of dicts with standardized keys: symbol, size, size_usd, entry.
        FIX 2026-01-22: Added size_usd from API's positionValue field for accurate USD comparison.
        """
        if not self.user_address:
            return []
        try:
            if not self._session or self._session.closed:
                self._session = aiohttp.ClientSession(connector=get_connector(), timeout=aiohttp.ClientTimeout(total=10))

            url = f"{self.rest_url}/info"
            payload = {"type": "clearinghouseState", "user": self.user_address}

            async with self._session.post(url, json=payload) as r:
                if r.status != 200:
                    log.warning(f"[HL] get_positions failed: status={r.status}")
                    return []
                data = await r.json()
                # Structure: {"marginSummary": {...}, "assetPositions": [...]}
                asset_positions = data.get("assetPositions", [])

                positions = []
                for ap in asset_positions:
                    pos_data = ap.get("position", {})
                    coin = str(pos_data.get("coin", "")).upper()
                    szi = float(pos_data.get("szi", 0) or 0)  # Signed size in coins
                    entry_px = float(pos_data.get("entryPx", 0) or 0)
                    # FIX 2026-01-22: Use positionValue for USD value (more accurate)
                    position_value = float(pos_data.get("positionValue", 0) or 0)
                    # Fallback: calculate from size * entry if positionValue not available
                    size_usd = abs(position_value) if position_value != 0 else abs(szi * entry_px)

                    # FIX 2026-01-25: Add USD threshold to filter dust positions at source
                    # Prevents orphan alerts for tiny residuals from partial fills
                    MIN_POSITION_USD = 10.0
                    if coin and abs(szi) > 0:
                        if size_usd < MIN_POSITION_USD:
                            log.debug(f"[HL] Ignoring dust position {coin}: ${size_usd:.2f} < ${MIN_POSITION_USD}")
                            continue
                        positions.append({
                            "symbol": coin,
                            "size": szi,  # Coins (for order calculations)
                            "size_usd": size_usd,  # USD value (for reconciliation)
                            "entry": entry_px
                        })
                return positions
        except Exception as e:
            log.warning("[HL] get_positions error: %s", e)
            return []

    async def set_isolated_margin(self, coin: str, leverage: int = 2) -> bool:
        """Set isolated margin mode for a coin before opening positions.
        P0 FIX: Default leverage reduced from 3x to 2x for risk management.
        """
        return await self._hl.set_isolated_margin(coin, leverage)

    async def set_leverage(self, coin: str, leverage: int = 2) -> bool:
        """Set leverage for a coin. Delegates to set_isolated_margin.
        P0 FIX: Default leverage reduced from 3x to 2x for risk management.
        """
        return await self.set_isolated_margin(coin, leverage)
    
    async def get_funding_rate(self, coin: str) -> float:
        """Fetch current funding rate for a single symbol."""
        rates = await self.get_funding_rates()
        return rates.get(coin.upper(), 0.0)

    def round_price(self, coin: str, price: float) -> float:
        """Standardize price rounding for HL."""
        return self._hl.round_price(coin, price)
    
    def round_qty(self, coin: str, qty: float) -> float:
        """Standardize quantity rounding for HL."""
        return self._hl.round_qty(coin, qty)

    async def place_order(self, coin: str, side: str, qty: float, price: Optional[float] = None,
                          ioc: bool = True, reduce_only: bool = False, client_id: Optional[str] = None) -> Dict[str, Any]:
        """Place an order using HL SDK with IOC fallback. Returns dict for consistency with Aster."""
        is_buy = side.upper().startswith("B")
        resp = await self._hl.order(coin, is_buy, qty, price, ioc=ioc, reduce_only=reduce_only, client_id=client_id)
        # Return dict format for consistency with Aster and flatten code
        return {
            "status": resp.get("status", "filled"),
            "filled": float(resp.get("filled", 0.0)),
            "reason": resp.get("reason", ""),
            "dry_run": bool(resp.get("dry_run", False)),
            "order_id": resp.get("oid"),
            "avg_price": resp.get("avgPx")
        }

    async def get_order_fill_price(self, oid: Any) -> Optional[float]:
        """Fetch average fill price for an order ID from HL history/status."""
        if not oid: return None
        try:
            # Fetch order status from HL API
            res = await self._hl.get_order_status(oid)
            if isinstance(res, dict):
                # Standardize: SDK might return direct 'data' or wrapped in 'response'
                data = res.get("response", {}).get("data", {}) if "response" in res else res.get("data", res)
                
                if isinstance(data, dict):
                    # Optimized check: order status 'filled' block
                    fill_data = data.get("filled")
                    if isinstance(fill_data, dict) and fill_data.get("avgPx"):
                        return float(fill_data["avgPx"])
                    
                    # Alternative check: inside order object
                    order = data.get("order", {})
                    if isinstance(order, dict):
                        st = order.get("status")
                        if isinstance(st, dict) and "filled" in st:
                            avg_px = st["filled"].get("avgPx")
                            if avg_px: return float(avg_px)
                        # Fallback for some SDK versions returning avgPx directly in order
                        if order.get("avgPx"): return float(order["avgPx"])
            
            # LAST RESORT: Check recent fills via userFills
            # (Only do this if orderStatus failed to provide avgPx)
            try:
                # Need to know the coin, which we don't have here. 
                # This is why passing coin to get_order_fill_price is better.
                pass 
            except Exception: pass
            
            return None
        except Exception as e:
            log.warning(f"[HL] get_order_fill_price error for {oid}: {e}")
        return None
        
    async def fetch_order_result(self, oid: int) -> Dict[str, Any]:
        """Fetch full order result."""
        return await self._hl.get_order_status(oid)

    async def cancel_order(self, order_id: Any, symbol: str) -> Dict[str, Any]:
        """
        Cancel an open order on Hyperliquid.

        Args:
            order_id: The order ID (int or string) to cancel
            symbol: The coin symbol (e.g., "BTC", "ETH")

        Returns:
            Dict with 'status' key: 'cancelled' on success, 'error' on failure
        """
        if not symbol:
            return {"status": "error", "reason": "Symbol required for HL cancel"}
        try:
            oid = int(order_id) if order_id else 0
            if oid <= 0:
                return {"status": "error", "reason": "Invalid order_id"}
            return await self._hl.cancel_order(symbol, oid)
        except ValueError:
            return {"status": "error", "reason": f"Cannot convert order_id to int: {order_id}"}
        except Exception as e:
            log.error(f"[HL] cancel_order error: {e}")
            return {"status": "error", "reason": str(e)}

    async def wait_for_fill(
        self,
        order_id: Any,
        symbol: str,
        expected_qty: float,
        timeout_s: float = 60.0,
        poll_interval: float = 2.0
    ) -> Dict[str, Any]:
        """
        Wait for a limit order to fill, polling order status.

        Args:
            order_id: The order ID to monitor
            symbol: The coin symbol (e.g., "BTC")
            expected_qty: Expected fill quantity
            timeout_s: Maximum wait time (default 60s)
            poll_interval: Polling frequency (default 2s)

        Returns:
            Dict with keys: filled, avg_price, is_maker, status
            status is one of: 'filled', 'partial', 'timeout', 'cancelled'
        """
        if not order_id:
            return {"filled": 0.0, "avg_price": 0.0, "is_maker": False, "status": "error"}

        try:
            oid = int(order_id)
        except (ValueError, TypeError):
            return {"filled": 0.0, "avg_price": 0.0, "is_maker": False, "status": "error"}

        start_time = time.time()
        last_filled = 0.0

        while (time.time() - start_time) < timeout_s:
            try:
                status = await self._hl.get_order_status(oid)
                if not status:
                    await asyncio.sleep(poll_interval)
                    continue

                # Parse order status from HL API response
                # Response format: {"order": {...}, "status": "filled"/"open"/"cancelled"}
                order_info = status.get("order", status)
                filled = 0.0
                avg_price = 0.0

                # Try to extract fill info from various response formats
                if isinstance(order_info, dict):
                    filled = float(order_info.get("totalSz", 0.0) or
                                   order_info.get("filledSz", 0.0) or
                                   order_info.get("filled", 0.0) or 0.0)
                    avg_price = float(order_info.get("avgPx", 0.0) or
                                      order_info.get("avgPrice", 0.0) or 0.0)

                    order_status = order_info.get("status", "").lower()
                    if order_status in ("filled", "closed") or filled >= expected_qty * 0.95:
                        log.info(f"[HL] Order {oid} filled: {filled:.6f} @ ${avg_price:.4f}")
                        return {
                            "filled": filled,
                            "avg_price": avg_price,
                            "is_maker": True,
                            "status": "filled"
                        }

                    if order_status == "cancelled":
                        return {
                            "filled": filled,
                            "avg_price": avg_price,
                            "is_maker": filled > 0,
                            "status": "cancelled"
                        }

                if filled > last_filled:
                    log.debug(f"[HL] Order {oid} partial fill: {filled:.6f}/{expected_qty:.6f}")
                    last_filled = filled

            except Exception as e:
                log.warning(f"[HL] wait_for_fill poll error: {e}")

            await asyncio.sleep(poll_interval)

        # Timeout - return partial fill info
        elapsed = time.time() - start_time
        log.info(f"[HL] Order {oid} timeout after {elapsed:.1f}s, filled={last_filled:.6f}")
        return {
            "filled": last_filled,
            "avg_price": 0.0,  # Unknown if partial
            "is_maker": last_filled > 0,
            "status": "timeout"
        }

    # ---------- Internal WS ----------
    async def _ws_reader_loop(self) -> None:
        backoff = 0.3  # Start with 300ms backoff for fast recovery
        while True:
            try:
                # Pre-resolve DNS to avoid system DNS issues
                # Extract host from ws_url
                from urllib.parse import urlparse
                parsed = urlparse(self.ws_url)
                host = parsed.hostname
                port = parsed.port or (443 if parsed.scheme == "wss" else 80)
                
                # Resolve DNS (force IPv4 to avoid IPv6 issues)
                import socket
                try:
                    addr_info = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
                    if addr_info:
                        resolved_ip = addr_info[0][4][0]
                        log.debug(f"[HL] Resolved {host} to {resolved_ip}")
                except Exception as dns_err:
                    log.warning(f"[HL] DNS resolution failed for {host}: {dns_err}")
                    resolved_ip = host  # Fallback to hostname
                
                # Build connection URL with resolved IP (but keep host header for SSL)
                # Note: websockets library handles SNI properly with 'host' parameter
                async with websockets.connect(
                    self.ws_url, 
                    ping_interval=5,    # Ping every 5s (faster dead connection detection)
                    ping_timeout=10,    # Wait 10s for pong
                    close_timeout=5,    # Give more time for graceful close
                    open_timeout=30     # Longer timeout for initial connection
                ) as ws:
                    self._ws = ws
                    self._connected.set()
                    backoff = 0.3  # Reset backoff on successful connect
                    
                    # Trigger resubscription (log only on first connect)
                    if len(self._subs) > 0:
                        log.debug("[HL] WS connected - resubscribing %d coins", len(self._subs))
                    self._last_sub_ts.clear()
                    for coin in list(self._subs):
                        await self._enqueue_subscribe(coin)
                    
                    try:
                        while True:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30.0)  # Increased from 15s
                            self._messages_rx += 1
                            await self._handle_msg(msg)
                    except asyncio.TimeoutError:
                        log.debug("[HL] WS recv timeout, sending ping")
                        await self._ws.send('{"method":"ping"}')
                    except Exception as e:
                        if not isinstance(e, asyncio.CancelledError):
                            log.debug("[HL] WS iter error: %s", e)
                        raise e
            except asyncio.CancelledError:
                return
            except Exception as e:
                self._last_error = str(e)
                log.debug("[HL] WS error: %s - reconnecting in %.1fs", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 10.0)  # Max 10s backoff
            finally:
                self._connected.clear()
                self._ws = None


    async def _ws_writer_loop(self) -> None:
        # Smooth outgoing subscribe messages to avoid 429
        min_interval = 1.0 / max(SUB_RATE_LIMIT_HZ, 1.0)
        burst = MAX_SUB_BURST
        last_send = 0.0
        tokens = burst
        while True:
            try:
                # Wait for connection context
                # Use safe attribute check for openness (handles varied websockets versions)
                is_open = self._ws and getattr(self._ws, "open", False)
                if not self._connected.is_set() or not is_open:
                    await self._connected.wait()

                payload = await self._send_queue.get()
                now = time.time()
                elapsed = now - last_send
                tokens = min(burst, tokens + elapsed * SUB_RATE_LIMIT_HZ)
                if tokens < 1.0:
                    # log.debug(f"[HL-WRITER] Rate limited, sleeping for {min_interval:.2f}s")
                    await asyncio.sleep(min_interval)
                    # Re-check connection after sleep
                    if not self._connected.is_set():
                        await self._send_queue.put(payload)
                        continue
                
                try:
                    await self._ws.send(json.dumps(payload))
                    self._messages_tx += 1
                    tokens -= 1.0
                    last_send = time.time()
                except Exception as e:
                    # Connection lost during send - suppress noise
                    log.debug("[HL-WRITER] Send error: %s. Re-queueing and waiting...", e)
                    await self._send_queue.put(payload)
                    self._connected.clear() # Force wait
                    await asyncio.sleep(1.0)

            except asyncio.CancelledError:
                return
            except Exception as e:
                log.error("[HL-WRITER] Loop error: %s", e)
                await asyncio.sleep(1.0)

    async def _staleness_monitor_loop(self) -> None:
        """Monitor for stale price data and force reconnect if needed."""
        STALE_THRESHOLD_MS = 5000.0   # 5 seconds (reduced for faster staleness detection)
        CHECK_INTERVAL_S = 2.0        # Check every 2s for quicker response

        while True:
            try:
                await asyncio.sleep(CHECK_INTERVAL_S)
                if not self._subs or not self._connected.is_set():
                    continue

                stale_symbols = []
                for coin in list(self._subs):
                    age_ms = self.get_bbo_age_ms(coin)
                    if age_ms > STALE_THRESHOLD_MS:
                        stale_symbols.append((coin, age_ms))

                if stale_symbols:
                    # FIX 2026-01-22: Reduced to debug - staleness handled silently in background
                    log.debug("[HL] STALENESS: %d symbols stale (>%.0fs): %s",
                        len(stale_symbols), STALE_THRESHOLD_MS / 1000,
                        ", ".join(f"{s}:{int(age)}ms" for s, age in stale_symbols[:5]))

                    # If >50% of symbols are stale, force reconnect
                    if len(stale_symbols) > len(self._subs) * 0.5:
                        log.debug("[HL] >50%% symbols stale - forcing WS reconnect")
                        if self._ws:
                            try:
                                await self._ws.close()
                            except Exception:
                                pass
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.debug("[HL] Staleness monitor error: %s", e)
                await asyncio.sleep(5.0)

    async def _sub_throttle_loop(self) -> None:
        # Periodically re-enqueue subscriptions (once per RESUB_MIN_PERIOD_S)
        while True:
            try:
                await asyncio.sleep(RESUB_MIN_PERIOD_S / 2)
                now = time.time()
                for c in list(self._subs):
                    last = self._last_sub_ts.get(c, 0.0)
                    if (now - last) >= RESUB_MIN_PERIOD_S:
                        await self._enqueue_subscribe(c)
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.warning("[HL] sub throttle error: %s", e)

    async def _enqueue_subscribe(self, coin: str) -> None:
        now = time.time()
        last = float(self._last_sub_ts.get(coin, 0.0))
        if (now - last) < RESUB_MIN_PERIOD_S:
            return
        self._last_sub_ts[coin] = now
        # BBO and L2 book subscriptions
        await self._send_queue.put({"method": "subscribe", "subscription": {"type": "bbo", "coin": coin}})
        await self._send_queue.put({"method": "subscribe", "subscription": {"type": "l2Book", "coin": coin}})

    async def _handle_msg(self, raw: Any) -> None:
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            data = json.loads(raw)
        except Exception as e:
            log.warning("[HL] handle_msg parse error: %s", e)
            return
        
        # HL message format: {"channel": "l2Book", "data": {"coin": "BTC", "levels": [[bids], [asks]], "time": ...}}
        # levels[0] = bids (list of {"px": price, "sz": size, "n": count})
        # levels[1] = asks (list of {"px": price, "sz": size, "n": count})
        ch = data.get("channel")
        d = data.get("data") or {}
        
        if ch == "subscriptionResponse":
            # Track confirmed subscriptions for diagnostics
            method = d.get("method", "")
            sub = d.get("subscription", {})
            coin = sub.get("coin")
            if method == "subscribe" and coin:
                self._confirmed_subs.add(coin.upper())
                log.debug("[HL] Subscription confirmed for %s", coin)
            return
        elif ch == "l2Book":
            coin = str(d.get("coin", "")).upper()
            levels = d.get("levels") or []
            if len(levels) >= 2:
                # Parse bids (levels[0]) and asks (levels[1])
                raw_bids = levels[0] if levels[0] else []
                raw_asks = levels[1] if len(levels) > 1 else []
                
                bids = []
                for item in raw_bids:
                    try:
                        px = float(item.get("px", 0) or item[0] if isinstance(item, list) else item.get("px", 0))
                        sz = float(item.get("sz", 0) or item[1] if isinstance(item, list) else item.get("sz", 0))
                        if px > 0 and sz > 0:
                            bids.append((px, sz))
                    except Exception:
                        continue
                
                asks = []
                for item in raw_asks:
                    try:
                        px = float(item.get("px", 0) or item[0] if isinstance(item, list) else item.get("px", 0))
                        sz = float(item.get("sz", 0) or item[1] if isinstance(item, list) else item.get("sz", 0))
                        if px > 0 and sz > 0:
                            asks.append((px, sz))
                    except Exception:
                        continue
                
                # Sort and keep top 25
                bids = sorted(bids, key=lambda x: x[0], reverse=True)[:25]
                asks = sorted(asks, key=lambda x: x[0])[:25]
                
                self._l2[coin] = (bids, asks)
                self._books[coin] = Book(bids=bids, asks=asks)
                
                # Update BBO from L2
                if bids and asks:
                    self._bbo[coin] = (bids[0][0], asks[0][0])
                    self._bbo_ts[coin] = time.time()  # Track freshness
        elif ch == "bbo":
            # BBO channel (if subscribed) - backup
            coin = str(d.get("coin", "")).upper()
            bid = float(d.get("bid", 0.0) or 0.0)
            ask = float(d.get("ask", 0.0) or 0.0)
            if bid > 0 and ask > 0:
                self._bbo[coin] = (bid, ask)
                self._bbo_ts[coin] = time.time()  # Track freshness

    # ---- Helpers ----
    def fees_taker_bps(self) -> float:
        return float(self.fees_bps.get("taker_bps", 5.0))

    def fees_maker_bps(self) -> float:
        return float(self.fees_bps.get("maker_bps", 1.5))
