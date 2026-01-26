# -*- coding: utf-8 -*-
from __future__ import annotations
import asyncio, json, logging, time, hmac, hashlib, urllib.parse, os
from typing import Any, Dict, Optional, Tuple, List
import aiohttp, websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedOK, ConnectionClosedError
from core.orderbook import Book
from venues.base import VenueBase
log = logging.getLogger(__name__)
from core.dns_utils import get_connector


class Aster(VenueBase):
    def __init__(self, cfg: Dict[str, Any]) -> None:
        self.rest_url: str = cfg["rest_url"].rstrip("/")
        self.ws_url: str = cfg["ws_url"].rstrip("/")
        self.fees_bps = cfg.get("fees", {})
        self.min_qty_usd: float = float(cfg.get("min_qty_usd", 10.0))
        self.api_key = os.environ.get("ASTER_API_KEY", "").strip()
        self.api_secret = os.environ.get("ASTER_API_SECRET", "").strip()
        self.label = 'AS'



        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._books: Dict[str, Book] = {}
        self._bbo: Dict[str, Tuple[float, float]] = {}
        self._bbo_ts: Dict[str, float] = {}  # symbol -> timestamp
        self._l2: Dict[str, Tuple[List[Tuple[float,float]], List[Tuple[float,float]]]] = {}
        self._rules: Dict[str, Dict[str, Any]] = {}
        self._connected = asyncio.Event()
        self._send_queue: asyncio.Queue = asyncio.Queue()
        self._subs: set[str] = set()
        self._reader_task: Optional[asyncio.Task] = None
        self._writer_task: Optional[asyncio.Task] = None
        self._staleness_task: Optional[asyncio.Task] = None
        # FIX 2026-01-22: Incremental reconnect backoff to prevent reconnect spam
        self._ws_reconnect_backoff: float = 1.0
        self._ws_last_reconnect_time: float = 0.0

    def check_credentials(self) -> bool:
        """Verify that API keys are present."""
        return bool(self.api_key and self.api_secret)
    async def start(self) -> None:
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=10)
            self._session = aiohttp.ClientSession(connector=get_connector(), timeout=timeout)
        
        # Fetch exchange info for precision rules
        await self._fetch_exchange_info()
        
        if self._reader_task: return
        self._reader_task = asyncio.create_task(self._ws_reader_loop(), name="aster_ws_reader")
        self._writer_task = asyncio.create_task(self._ws_writer_loop(), name="aster_ws_writer")
        # DISABLED: Staleness monitor was causing connection instability by forcing reconnects
        # self._staleness_task = asyncio.create_task(self._staleness_monitor_loop(), name="aster_staleness_monitor")
        await asyncio.wait_for(self._connected.wait(), timeout=15)
    async def close(self) -> None:
        for t in (self._reader_task, self._writer_task, self._staleness_task):
            if t: t.cancel()
        if self._ws:
            try: await self._ws.close()
            except Exception as e: log.warning(f"[Aster] close error: {e}")
        if self._session:
            await self._session.close(); self._session = None
            
    async def _fetch_exchange_info(self) -> None:
        url = f"{self.rest_url}/fapi/v1/exchangeInfo"
        try:
            async with self._session.get(url) as r:
                if r.status != 200:
                    log.error(f"[Aster] Failed to fetch exchangeInfo: {r.status}")
                    return
                data = await r.json()
                for s in data.get("symbols", []):
                    sym = s["symbol"].upper()
                    rules = {
                        "pricePrecision": int(s.get("pricePrecision", 8)),
                        "quantityPrecision": int(s.get("quantityPrecision", 8)),
                        "tickSize": 0.0,
                        "stepSize": 0.0,
                        "minQty": 0.0,
                        "minNotional": 0.0
                    }
                    for f in s.get("filters", []):
                        ft = f.get("filterType")
                        if ft == "PRICE_FILTER":
                            rules["tickSize"] = float(f.get("tickSize", 0))
                        elif ft == "LOT_SIZE":
                            rules["stepSize"] = float(f.get("stepSize", 0))
                            rules["minQty"] = float(f.get("minQty", 0))
                        elif ft == "MIN_NOTIONAL":
                            rules["minNotional"] = float(f.get("notional", 0))
                    self._rules[sym] = rules
                log.info(f"[Aster] Loaded rules for {len(self._rules)} symbols")
        except Exception as e:
            log.error(f"[Aster] Error fetching exchangeInfo: {e}")

    def round_qty(self, symbol: str, qty: float) -> float:
        rules = self._rules.get(symbol.upper())
        if not rules: return qty
        step = rules["stepSize"]
        if step > 0:
            return float(round(qty // step * step, rules["quantityPrecision"]))
        return float(round(qty, rules["quantityPrecision"]))

    def round_price(self, symbol: str, price: float) -> float:
        rules = self._rules.get(symbol.upper())
        if not rules: return price
        tick = rules["tickSize"]
        if tick > 0:
            return float(round(price // tick * tick, rules["pricePrecision"]))
        return float(round(price, rules["pricePrecision"]))
    async def subscribe_orderbook(self, symbol: str) -> None:
        s = symbol.upper()
        if s in self._subs: return
        self._subs.add(s)
        await self._send_queue.put({"method": "SUBSCRIBE", "params": [f"{s.lower()}@bookTicker", f"{s.lower()}@depth5@100ms"], "id": int(time.time()*1000)%1000000})
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
        for symbol in self._subs:
            ts = self._bbo_ts.get(symbol.upper(), 0.0)
            if ts > 0 and (now - ts) * 1000 < max_age_ms:
                return True

        # All subscribed symbols have stale data = unhealthy
        return False

    def get_bbo(self, symbol: str) -> Optional[Tuple[float, float]]:
        return self._bbo.get(symbol.upper())
    def get_bbo_with_depth(self, symbol: str) -> Optional[Tuple[Tuple[float, float], Tuple[float, float]]]:
        """Return BBO with sizes: ((bid_price, bid_size), (ask_price, ask_size))"""
        sym = symbol.upper()
        l2 = self._l2.get(sym)
        if l2 and l2[0] and l2[1]:
            bids, asks = l2
            return ((bids[0][0], bids[0][1]), (asks[0][0], asks[0][1]))
        # Fallback to BBO without size
        bbo = self._bbo.get(sym)
        if bbo:
            return ((bbo[0], 0.0), (bbo[1], 0.0))
        return None

    def get_aggregated_depth_usd(self, symbol: str, levels: int = 5) -> float:
        """Sum depth across multiple orderbook levels for more accurate liquidity estimate."""
        sym = symbol.upper()
        l2 = self._l2.get(sym)
        if not l2 or not l2[0] or not l2[1]:
            return 0.0
        bids, asks = l2
        if not bids or not asks:
            return 0.0
        mid = (bids[0][0] + asks[0][0]) / 2
        bid_depth = sum(sz for _, sz in bids[:levels])
        ask_depth = sum(sz for _, sz in asks[:levels])
        return min(bid_depth, ask_depth) * mid

    def get_book(self, symbol: str) -> Optional[Book]:
        return self._books.get(symbol.upper())

    def get_bbo_age_ms(self, symbol: str) -> float:
        """Return age of BBO in milliseconds."""
        ts = self._bbo_ts.get(symbol.upper(), 0.0)
        if ts <= 0: return 999999.0
        return (time.time() - ts) * 1000.0
    
    async def get_fresh_bbo(self, symbol: str, max_age_ms: float = 2000.0) -> Optional[Tuple[float, float]]:
        """Get BBO, fetching via REST if WS data is stale."""
        age = self.get_bbo_age_ms(symbol)
        if age <= max_age_ms:
            return self._bbo.get(symbol.upper())
        # WS data is stale, try REST fallback
        return await self.fetch_bbo_rest(symbol)
    
    async def fetch_bbo_rest(self, symbol: str) -> Optional[Tuple[float, float]]:
        """Fetch fresh BBO via REST API. Used for pre-trade validation."""
        url = f"{self.rest_url}/fapi/v1/ticker/bookTicker"
        try:
            if not self._session or self._session.closed:
                self._session = aiohttp.ClientSession(connector=get_connector(), timeout=aiohttp.ClientTimeout(total=10))

            async with self._session.get(url, params={"symbol": symbol.upper()}) as r:
                if r.status != 200:
                    # FIX 2026-01-24: Only return cache if reasonably fresh (max 30s)
                    cached = self._bbo.get(symbol.upper())
                    if cached:
                        age = time.time() - self._bbo_ts.get(symbol.upper(), 0)
                        if age < 30.0:
                            return cached
                    return None  # Don't return arbitrarily old data
                data = await r.json()
                if isinstance(data, dict):
                    bid = float(data.get("bidPrice", 0))
                    ask = float(data.get("askPrice", 0))
                    if bid > 0 and ask > 0:
                        # FIX 2026-01-24: Update cache and timestamp on REST success
                        # This prevents infinite REST retry loops by refreshing staleness tracking
                        self._bbo[symbol.upper()] = (bid, ask)
                        self._bbo_ts[symbol.upper()] = time.time()
                        return (bid, ask)
        except Exception as e:
            log.debug(f"[Aster] fetch_bbo_rest error for {symbol}: {e}")
        # FIX 2026-01-24: Only return cache if reasonably fresh (max 30s)
        cached = self._bbo.get(symbol.upper())
        if cached:
            age = time.time() - self._bbo_ts.get(symbol.upper(), 0)
            if age < 30.0:
                return cached
        return None  # Don't return arbitrarily old data
    
    async def set_isolated_margin(self, symbol: str, leverage: int = 2) -> bool:
        """Set isolated margin mode for a symbol. Delegates to set_margin_type + set_leverage.
        P0 FIX: Default leverage reduced from 3x to 2x for risk management.
        """
        try:
            await self.set_margin_type(symbol, "ISOLATED")
            await self.set_leverage(symbol, leverage)
            return True
        except Exception as e:
            log.warning(f"[Aster] set_isolated_margin error: {e}")
            return False
    
    def is_connected(self) -> bool:
        """Check if WS is currently connected and ready for orders."""
        return self._connected.is_set() and self._ws is not None

    
    async def get_funding_rate(self, symbol: str) -> Optional[float]:
        """Fetch current funding rate for a symbol.
        Returns funding rate as decimal (e.g. 0.0001 = 0.01% = 1 bps).
        """
        url = f"{self.rest_url}/fapi/v1/premiumIndex"
        try:
            if not self._session or self._session.closed:
                self._session = aiohttp.ClientSession(connector=get_connector(), timeout=aiohttp.ClientTimeout(total=10))
            
            async with self._session.get(url, params={"symbol": symbol.upper()}) as r:
                if r.status != 200:
                    return None
                data = await r.json()
                if isinstance(data, dict):
                    rate = data.get("lastFundingRate")
                    if rate is not None:
                        return float(rate)
        except Exception as e:
            log.warning("[Aster] Funding rate fetch error for %s: %s", symbol, e)
        return None
    
    async def get_funding_rates(self) -> Dict[str, float]:
        """Fetch current funding rates for all symbols.
        Returns dict of symbol -> funding rate.
        """
        url = f"{self.rest_url}/fapi/v1/premiumIndex"
        try:
            if not self._session or self._session.closed:
                self._session = aiohttp.ClientSession(connector=get_connector(), timeout=aiohttp.ClientTimeout(total=10))

            async with self._session.get(url) as r:
                if r.status != 200:
                    return {}
                data = await r.json()
                rates: Dict[str, float] = {}
                if isinstance(data, list):
                    for item in data:
                        sym = str(item.get("symbol", "")).upper()
                        rate = item.get("lastFundingRate")
                        if sym and rate is not None:
                            try:
                                rates[sym] = float(rate)
                                log.debug(f"[AS RAW] {sym}: rate={rate}")
                            except (ValueError, TypeError):
                                pass
                return rates
        except Exception as e:
            log.warning("[Aster] Funding rates fetch error: %s", e)
            return {}

    async def get_funding_intervals(self) -> Dict[str, int]:
        """Fetch funding interval hours per symbol.

        Aster has VARIABLE funding intervals per symbol:
        - Most symbols: 8h interval (rate is per-8h)
        - Some symbols: 4h interval (rate is per-4h)
        - Some symbols: 1h interval (rate is per-1h)

        Returns dict of symbol -> interval_hours.
        Caches result for 5 minutes.
        """
        # Check cache (5 min TTL - reduced from 1h to catch interval changes faster)
        cached = getattr(self, "_funding_intervals_cache", None)
        if cached:
            data, ts = cached
            if (time.time() - ts) < 300:
                return data

        url = f"{self.rest_url}/fapi/v1/fundingInfo"
        try:
            if not self._session or self._session.closed:
                self._session = aiohttp.ClientSession(connector=get_connector(), timeout=aiohttp.ClientTimeout(total=10))

            async with self._session.get(url) as r:
                if r.status != 200:
                    log.warning(f"[Aster] fundingInfo fetch failed: {r.status}")
                    return {}
                data = await r.json()
                intervals: Dict[str, int] = {}
                if isinstance(data, list):
                    for item in data:
                        sym = str(item.get("symbol", "")).upper()
                        # fundingIntervalHours is the key field
                        interval = item.get("fundingIntervalHours", 8)
                        if sym:
                            intervals[sym] = int(interval)
                            # Log non-standard intervals for debugging
                            if interval != 8:
                                log.debug(f"[Aster] {sym} has {interval}h funding interval (non-standard)")
                self._funding_intervals_cache = (intervals, time.time())
                # Log summary with non-8h count
                non_std_count = sum(1 for i in intervals.values() if i != 8)
                log.info("[Aster] Loaded %d funding intervals (%d non-8h)", len(intervals), non_std_count)
                return intervals
        except Exception as e:
            log.warning(f"[Aster] fundingInfo error: {e}")
            return {}

    def get_funding_interval(self, symbol: str) -> int:
        """Get cached funding interval for symbol. Returns 8 as default."""
        cached = getattr(self, "_funding_intervals_cache", None)
        if cached:
            data, _ = cached
            return data.get(symbol.upper(), 8)
        return 8
    
    async def equity(self) -> Optional[float]:
        # OPTIMIZATION: Cache equity for 15 seconds
        now = time.time()
        cached = getattr(self, "_equity_cache", None)
        if cached:
            val, ts = cached
            if (now - ts) < 15.0:
                return val
        
        # GET /fapi/v2/balance (USER_DATA) avec timestamp/signature (doc officielle)
        result = None
        try:
            ts_ms = await self._server_time_ms() or int(time.time() * 1000)
            params = {"timestamp": str(ts_ms), "recvWindow": "60000"}
            bal = await self._private_get("/fapi/v2/balance", params)
            if isinstance(bal, list) and bal:
                equity = 0.0
                for acc in bal:
                    asset = (acc.get("asset") or "").upper()
                    if asset in ("USDT","USDC","USD","USDb"):
                        v = float(acc.get("balance") or acc.get("walletBalance") or 0.0)
                        equity += v
                result = float(equity)
        except Exception as e:
            log.warning("[Aster] equity error: %s", e)
        
        # Cache result
        if result is not None:
            self._equity_cache = (result, now)
        
        return result
    
    async def get_positions(self) -> List[Dict[str, Any]]:
        """Fetch all open positions from Aster.
        Returns list of dicts with standardized keys: symbol, size, size_usd, entry.
        FIX 2026-01-22: Added size_usd from API's notional field for accurate USD comparison.
        """
        try:
            ts_ms = await self._server_time_ms() or int(time.time() * 1000)
            params = {"timestamp": str(ts_ms), "recvWindow": "60000"}
            data = await self._private_get("/fapi/v2/positionRisk", params)

            if not isinstance(data, list):
                return []

            positions = []
            for item in data:
                symbol = str(item.get("symbol", "")).upper()
                size = float(item.get("positionAmt", 0) or 0)  # Size in coins
                entry_px = float(item.get("entryPrice", 0) or 0)
                leverage = int(item.get("leverage", 0) or 0)
                margin_type = str(item.get("marginType", "unknown"))
                # FIX 2026-01-22: Use notional field for USD value (more accurate than size * price)
                notional = float(item.get("notional", 0) or 0)  # USD value from API
                # Fallback: calculate from size * entry if notional not available
                size_usd = abs(notional) if notional != 0 else abs(size * entry_px)

                # FIX 2026-01-25: Add USD threshold to filter dust positions at source
                # Prevents orphan alerts for tiny residuals from partial fills
                MIN_POSITION_USD = 10.0
                if symbol and abs(size) > 0:
                    if size_usd < MIN_POSITION_USD:
                        log.debug(f"[Aster] Ignoring dust position {symbol}: ${size_usd:.2f} < ${MIN_POSITION_USD}")
                        continue
                    positions.append({
                        "symbol": symbol,
                        "size": size,  # Coins (for order calculations)
                        "size_usd": size_usd,  # USD value (for reconciliation)
                        "entry": entry_px,
                        "leverage": leverage,
                        "marginType": margin_type
                    })
            return positions
        except Exception as e:
            log.warning("[Aster] get_positions error: %s", e)
            return []
    
    async def place_order(self, symbol: str, side: str, qty: float, price: Optional[float] = None, ioc: bool = True, reduce_only: bool = False, client_id: Optional[str] = None) -> Dict[str, Any]:
        sym = symbol.upper()
        rules = self._rules.get(sym, {})
        q_prec = rules.get("quantityPrecision", 8)
        p_prec = rules.get("pricePrecision", 8)
        
        rqty = self.round_qty(sym, qty)
        
        params: Dict[str, Any] = {
            "symbol": sym,
            "side": side.upper(),
            "type": "MARKET" if price is None else "LIMIT",
            "quantity": f"{rqty:.{q_prec}f}",
            "newClientOrderId": client_id or None,
            "reduceOnly": "true" if reduce_only else "false",
            "recvWindow": "5000",
            "timestamp": str(int(time.time() * 1000)),
        }
        if price is not None:
            rprice = self.round_price(sym, price)
            params["price"] = f"{rprice:.{p_prec}f}"
            # Use IOC for scalping (immediate fill or cancel), GTC for persistent orders
            params["timeInForce"] = "IOC" if ioc else "GTC"
        try:
            data = await self._private_post("/fapi/v1/order", params)
            filled = 0.0; st = "accepted"
            order_id = None
            
            if isinstance(data, dict):
                # CRITICAL: Check for API error response first
                if "code" in data and data.get("code", 0) != 0:
                    error_msg = data.get("msg", "Unknown API error")
                    log.warning(f"[Aster] API error: code={data.get('code')} msg={error_msg}")
                    return {"status": "error", "filled": 0.0, "reason": error_msg, "orderId": None}
                
                st = data.get("status") or data.get("orderStatus") or "accepted"
                order_id = data.get("orderId")
                try:
                    for k in ("executedQty", "cumQty"):
                        if k in data: 
                            filled = float(data[k])
                            break
                except Exception as e: log.debug("[Aster] order details parse error: %s", e)
            
            avg_price = 0.0 # Aster returns 'avgPrice' in order query
            # MARKET orders may return NEW with 0 fill initially - poll for actual fill
            if price is None and st.upper() == "NEW" and filled == 0 and order_id:
                for _ in range(3):
                    await asyncio.sleep(0.3)  # Wait 300ms between polls
                    try:
                        query_params = {
                            "symbol": symbol.upper(),
                            "orderId": str(order_id),
                            "timestamp": str(int(time.time() * 1000)),
                        }
                        order_data = await self._private_get("/fapi/v1/order", query_params)
                        if isinstance(order_data, dict):
                            new_st = order_data.get("status", st)
                            new_fill = float(order_data.get("executedQty", 0) or 0)
                            avg_price = float(order_data.get("avgPrice", 0) or 0)
                            if new_fill > 0:
                                st = new_st
                                filled = new_fill
                                log.debug("[Aster] Poll found fill: %s qty=%f px=%f", st, filled, avg_price)
                                break
                    except Exception as e:
                        log.debug("[Aster] Poll error: %s", e)
            
            return {"status": st, "filled": filled, "order_id": order_id, "avg_price": avg_price, "reason": None}
        except Exception as e:
            log.warning("[Aster] order error: %s", e)
            if not self.api_key or not self.api_secret:
                return {"status": "filled", "filled": qty, "dry_run": True, "order_id": "mock", "avg_price": 0.0, "reason": None}
            return {"status": "error", "filled": 0.0, "reason": str(e), "order_id": None, "avg_price": 0.0}

    async def get_order_fill_price(self, symbol: str, oid: Any) -> Optional[float]:
        """Fetch average fill price for an order ID."""
        if not oid: return None
        try:
            query_params = {
                "symbol": symbol.upper(),
                "orderId": str(oid),
                "timestamp": str(int(time.time() * 1000)),
            }
            order_data = await self._private_get("/fapi/v1/order", query_params)
            if isinstance(order_data, dict):
                return float(order_data.get("avgPrice", 0) or 0)
        except Exception as e:
            log.warning(f"[Aster] get_order_fill_price error: {e}")
        return None

    async def cancel_order(self, order_id: Any, symbol: str) -> Dict[str, Any]:
        """Cancel an open order on Aster.

        Args:
            order_id: The order ID to cancel
            symbol: Trading symbol (e.g., "BTCUSDT")

        Note: Parameter order matches base class convention (order_id first, symbol second).
        """
        if not order_id:
            return {"status": "error", "reason": "No order_id provided"}
        try:
            params = {
                "symbol": symbol.upper(),
                "orderId": str(order_id),
                "timestamp": str(int(time.time() * 1000)),
                "recvWindow": "5000",
            }
            # DELETE /fapi/v1/order
            data = await self._private_delete("/fapi/v1/order", params)
            if isinstance(data, dict):
                if data.get("code") and data.get("code") != 0:
                    return {"status": "error", "reason": data.get("msg", "Unknown error")}
                return {"status": "cancelled", "orderId": data.get("orderId")}
            return {"status": "unknown", "data": data}
        except Exception as e:
            log.warning(f"[Aster] cancel_order error: {e}")
            return {"status": "error", "reason": str(e)}

    async def get_open_orders(self, symbol: str = None) -> List[Dict[str, Any]]:
        """Get all open orders on Aster.

        Args:
            symbol: Optional - filter by symbol. If None, gets all open orders.

        Returns:
            List of open order dicts with keys: orderId, symbol, side, price, qty, filled, status
        """
        try:
            params = {
                "timestamp": str(int(time.time() * 1000)),
                "recvWindow": "5000",
            }
            if symbol:
                params["symbol"] = symbol.upper()

            # GET /fapi/v1/openOrders
            data = await self._private_get("/fapi/v1/openOrders", params)

            if isinstance(data, list):
                orders = []
                for order in data:
                    orders.append({
                        "orderId": order.get("orderId"),
                        "symbol": order.get("symbol"),
                        "side": order.get("side"),
                        "price": float(order.get("price", 0)),
                        "qty": float(order.get("origQty", 0)),
                        "filled": float(order.get("executedQty", 0)),
                        "status": order.get("status"),
                        "type": order.get("type"),
                        "time": order.get("time")
                    })
                return orders
            return []
        except Exception as e:
            log.warning(f"[Aster] get_open_orders error: {e}")
            return []

    async def cancel_all_orders(self, symbol: str = None) -> Dict[str, Any]:
        """Cancel all open orders on Aster.

        Args:
            symbol: Optional - cancel orders for specific symbol only.
                    If None, cancels ALL open orders (queries first).

        Returns:
            Dict with cancelled count and any errors.
        """
        cancelled = 0
        errors = []

        try:
            if symbol:
                # Cancel all orders for specific symbol
                params = {
                    "symbol": symbol.upper(),
                    "timestamp": str(int(time.time() * 1000)),
                    "recvWindow": "5000",
                }
                # DELETE /fapi/v1/allOpenOrders
                data = await self._private_delete("/fapi/v1/allOpenOrders", params)
                if isinstance(data, dict) and data.get("code") == 200:
                    log.info(f"[Aster] Cancelled all open orders for {symbol}")
                    return {"cancelled": "all", "symbol": symbol, "errors": []}
                elif isinstance(data, list):
                    cancelled = len(data)
                    log.info(f"[Aster] Cancelled {cancelled} orders for {symbol}")
                    return {"cancelled": cancelled, "symbol": symbol, "errors": []}
            else:
                # No symbol specified - get all open orders and cancel each
                open_orders = await self.get_open_orders()
                for order in open_orders:
                    try:
                        result = await self.cancel_order(order["orderId"], order["symbol"])
                        if result.get("status") == "cancelled":
                            cancelled += 1
                        else:
                            errors.append(f"{order['symbol']}:{order['orderId']} - {result.get('reason', 'unknown')}")
                    except Exception as e:
                        errors.append(f"{order['symbol']}:{order['orderId']} - {str(e)}")

                log.info(f"[Aster] Cancelled {cancelled} open orders, {len(errors)} errors")

            return {"cancelled": cancelled, "errors": errors}
        except Exception as e:
            log.warning(f"[Aster] cancel_all_orders error: {e}")
            return {"cancelled": cancelled, "errors": errors + [str(e)]}

    async def wait_for_fill(
        self,
        symbol: str,
        order_id: Any,
        expected_qty: float,
        timeout_s: float = 60.0,
        poll_interval: float = 2.0
    ) -> Dict[str, Any]:
        """
        Wait for a limit order to fill on Aster, polling order status.

        Args:
            symbol: Trading symbol (e.g., "BTCUSDT")
            order_id: The order ID to monitor
            expected_qty: Expected fill quantity
            timeout_s: Maximum wait time (default 60s)
            poll_interval: Polling frequency (default 2s)

        Returns:
            Dict with keys: filled, avg_price, is_maker, status
            status is one of: 'filled', 'partial', 'timeout', 'cancelled', 'error'
        """
        if not order_id:
            return {"filled": 0.0, "avg_price": 0.0, "is_maker": False, "status": "error"}

        start_time = time.time()
        last_filled = 0.0

        while (time.time() - start_time) < timeout_s:
            try:
                # Query order status via /fapi/v1/order
                query_params = {
                    "symbol": symbol.upper(),
                    "orderId": str(order_id),
                    "timestamp": str(int(time.time() * 1000)),
                }
                order_data = await self._private_get("/fapi/v1/order", query_params)

                if isinstance(order_data, dict):
                    filled = float(order_data.get("executedQty", 0.0) or 0.0)
                    avg_price = float(order_data.get("avgPrice", 0.0) or 0.0)
                    order_status = order_data.get("status", "").upper()

                    # FILLED = fully filled, PARTIALLY_FILLED = still working
                    if order_status == "FILLED" or filled >= expected_qty * 0.95:
                        log.info(f"[Aster] Order {order_id} filled: {filled:.6f} @ ${avg_price:.4f}")
                        return {
                            "filled": filled,
                            "avg_price": avg_price,
                            "is_maker": True,
                            "status": "filled"
                        }

                    if order_status in ("CANCELED", "CANCELLED", "EXPIRED", "REJECTED"):
                        return {
                            "filled": filled,
                            "avg_price": avg_price,
                            "is_maker": filled > 0,
                            "status": "cancelled"
                        }

                    if filled > last_filled:
                        log.debug(f"[Aster] Order {order_id} partial fill: {filled:.6f}/{expected_qty:.6f}")
                        last_filled = filled

            except Exception as e:
                log.warning(f"[Aster] wait_for_fill poll error: {e}")

            await asyncio.sleep(poll_interval)

        # Timeout - return partial fill info
        elapsed = time.time() - start_time
        log.info(f"[Aster] Order {order_id} timeout after {elapsed:.1f}s, filled={last_filled:.6f}")
        return {
            "filled": last_filled,
            "avg_price": 0.0,  # Unknown if partial/timeout
            "is_maker": last_filled > 0,
            "status": "timeout"
        }

    async def _private_delete(self, path: str, params: Dict[str, Any]) -> Any:
        """Send authenticated DELETE request to Aster API."""
        if not self.api_key or not self.api_secret:
            log.warning("[Aster] No credentials - cannot execute DELETE")
            return {"code": -1, "msg": "No credentials"}

        # Build query string and sign
        qs = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
        sig = hmac.new(self.api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
        full_qs = f"{qs}&signature={sig}"

        url = f"{self.rest_url}{path}?{full_qs}"
        headers = {"X-MBX-APIKEY": self.api_key}

        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(connector=get_connector(), timeout=aiohttp.ClientTimeout(total=10))

        async with self._session.delete(url, headers=headers) as r:
            if r.status != 200:
                txt = await r.text()
                log.warning(f"[Aster] DELETE {path} returned {r.status}: {txt}")
                return {"code": r.status, "msg": txt}
            return await r.json()

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """Set leverage for symbol."""
        try:
            params = {
                "symbol": symbol.upper(),
                "leverage": leverage,
                "timestamp": str(int(time.time() * 1000)),
            }
            res = await self._private_post("/fapi/v1/leverage", params)
            # Response: {"leverage": 5, "maxNotionalValue": "..."}
            if isinstance(res, dict) and int(res.get("leverage", 0)) == leverage:
                return True
            log.warning(f"[Aster] Failed to set leverage {leverage}x for {symbol}: {res}")
            return False
        except Exception as e:
            log.warning(f"[Aster] set_leverage error: {e}")
            return False

    async def set_margin_type(self, symbol: str, margin_type: str = "ISOLATED") -> bool:
        """Set margin type (ISOLATED or CROSSED)."""
        try:
            params = {
                "symbol": symbol.upper(),
                "marginType": margin_type.upper(),
                "timestamp": str(int(time.time() * 1000)),
            }
            # Note: This endpoint errors if margin type is already set to the target.
            # We need to catch code -4046 "No need to change margin type".
            res = await self._private_post("/fapi/v1/marginType", params)
            return True
        except Exception as e:
            msg = str(e).lower()
            if "no need" in msg:
                return True # Already set, success
            log.warning(f"[Aster] set_margin_type error: {e}")
            return False
    async def _ws_reader_loop(self) -> None:
        """WebSocket reader with proper reconnection loop and resubscription."""
        url = self.ws_url
        backoff = 1.0
        consecutive_failures = 0

        while True:  # Proper reconnection loop (not task spawn)
            try:
                # FIX 2026-01-22: Increase ping_interval to reduce server-side disconnects
                # Many exchanges disconnect on aggressive ping intervals
                async with websockets.connect(
                    url,
                    ping_interval=20,  # Was 5, increased to reduce server load
                    ping_timeout=30,   # Was 10, more lenient timeout
                    close_timeout=5    # Was 2, allow more time for clean close
                ) as ws:
                    self._ws = ws
                    self._connected.set()
                    backoff = 1.0  # Reset backoff on successful connection
                    self._ws_reconnect_backoff = 1.0  # Also reset class-level backoff
                    consecutive_failures = 0  # Reset failure counter
                    log.info("[Aster] WS connected")

                    # Resubscribe all tracked symbols after reconnect
                    if self._subs:
                        log.info("[Aster] Resubscribing to %d symbols after reconnect", len(self._subs))
                        for symbol in list(self._subs):
                            await self._send_queue.put({
                                "method": "SUBSCRIBE",
                                "params": [f"{symbol.lower()}@bookTicker", f"{symbol.lower()}@depth5@100ms"],
                                "id": int(time.time() * 1000) % 1000000
                            })
                            await asyncio.sleep(0.05)  # Rate limit subscriptions

                    # Process messages
                    async for msg in ws:
                        await self._on_msg(msg)

            except asyncio.CancelledError:
                return
            except ConnectionClosedOK:
                # Normal close - just reconnect without warning
                self._connected.clear()
                self._ws = None
                log.debug(f"[Aster] WS connection closed normally, reconnecting in {backoff:.1f}s")
                await asyncio.sleep(backoff)
            except ConnectionClosedError as e:
                # Abnormal close - log at appropriate level based on frequency
                self._connected.clear()
                self._ws = None
                consecutive_failures += 1
                # FIX: Use str(e) to avoid encoding issues in log formatting
                err_msg = str(e) if e else "connection closed"
                if consecutive_failures <= 2:
                    log.debug(f"[Aster] WS connection closed: {err_msg} - reconnecting in {backoff:.1f}s")
                else:
                    log.warning(f"[Aster] WS connection unstable ({consecutive_failures} failures): {err_msg} - reconnecting in {backoff:.1f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 30.0)
            except Exception as e:
                self._connected.clear()
                self._ws = None
                consecutive_failures += 1
                err_str = str(e).lower() if e else ""
                # Only log as warning if it's a persistent issue
                # FIX: Use f-strings to avoid encoding issues in log formatting
                if consecutive_failures > 3 or ("timeout" not in err_str and "close" not in err_str):
                    log.warning(f"[Aster] WS error ({consecutive_failures}): {err_str} - reconnecting in {backoff:.1f}s")
                else:
                    log.debug(f"[Aster] WS reconnecting after: {err_str} (backoff {backoff:.1f}s)")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 30.0)  # Exponential backoff, max 30s

    async def _staleness_monitor_loop(self) -> None:
        """Monitor for stale price data and force reconnect if needed."""
        STALE_THRESHOLD_MS = 10000.0  # 10 seconds (increased to reduce false positives)
        CHECK_INTERVAL_S = 5.0        # Check every 5s (reduced frequency)
        MIN_SYMBOLS_FOR_CHECK = 1     # Need at least 1 symbol with data to check staleness

        while True:
            try:
                await asyncio.sleep(CHECK_INTERVAL_S)

                if not self._subs or not self._connected.is_set():
                    continue

                # Check all subscribed symbols for staleness
                # FIX 2026-01-22: Only count symbols that HAVE received data before
                # Symbols that never received data (ts=0) are not counted as stale
                stale_symbols = []
                active_symbols = 0  # Symbols that have received data at least once

                for symbol in list(self._subs):
                    ts = self._bbo_ts.get(symbol.upper(), 0.0)
                    if ts <= 0:
                        # Never received data - don't count as stale (might be inactive market)
                        continue

                    active_symbols += 1
                    age_ms = (time.time() - ts) * 1000.0
                    if age_ms > STALE_THRESHOLD_MS:
                        stale_symbols.append((symbol, age_ms))

                # Only trigger reconnect if we have active symbols AND most are stale
                if stale_symbols and active_symbols >= MIN_SYMBOLS_FOR_CHECK:
                    log.debug(
                        "[Aster] STALENESS: %d/%d active symbols stale (>%.0fs): %s",
                        len(stale_symbols), active_symbols,
                        STALE_THRESHOLD_MS / 1000,
                        ", ".join(f"{s}:{int(age)}ms" for s, age in stale_symbols[:5])
                    )

                    # If >50% of ACTIVE symbols are stale, force reconnect (with backoff)
                    if len(stale_symbols) > active_symbols * 0.5:
                        now = time.time()
                        # Only force reconnect if we haven't done so recently
                        if now - self._ws_last_reconnect_time >= self._ws_reconnect_backoff:
                            log.info(f"[Aster] >50% active symbols stale ({len(stale_symbols)}/{active_symbols}) - forcing WS reconnect (backoff: {self._ws_reconnect_backoff:.1f}s)")
                            # CRITICAL: Clear connected BEFORE closing to prevent writer race condition
                            self._connected.clear()
                            if self._ws:
                                try:
                                    await self._ws.close()
                                except Exception:
                                    pass
                            self._ws_last_reconnect_time = now
                            self._ws_reconnect_backoff = min(self._ws_reconnect_backoff * 2, 60.0)
                        else:
                            remaining = self._ws_reconnect_backoff - (now - self._ws_last_reconnect_time)
                            log.debug(f"[Aster] Stale data but in reconnect cooldown ({remaining:.1f}s remaining)")

            except asyncio.CancelledError:
                return
            except Exception as e:
                log.warning(f"[Aster] Staleness monitor error: {e}")
                await asyncio.sleep(5.0)

    async def _ws_writer_loop(self) -> None:
        while True:
            try:
                payload = await self._send_queue.get()
                # Simple check - just verify WS exists (original working pattern)
                if not self._ws:
                    await asyncio.sleep(0.05)
                    await self._send_queue.put(payload)
                    continue
                await self._ws.send(json.dumps(payload))
                # Rate limit: 100ms between sends to avoid Aster 3002 errors
                await asyncio.sleep(0.1)
            except asyncio.CancelledError: return
            except Exception as e:
                err_str = str(e).lower()
                if "3002" in err_str or "frequency" in err_str:
                    # Rate limit error - longer backoff
                    log.warning("[Aster] rate limited, backing off 5s")
                    await asyncio.sleep(5.0)
                    await self._send_queue.put(payload)
                elif "close" in err_str or "1000" in err_str or "closed" in err_str:
                    # FIX 2026-01-22: WebSocket closed - let reader loop handle reconnect
                    # Don't try to close/reconnect here - just signal disconnected and wait
                    # The reader loop will handle the actual reconnection
                    if self._connected.is_set():
                        # Only log once per disconnect cycle, and at DEBUG level
                        # since reconnects are expected behavior
                        log.debug("[Aster] WS send failed (connection closed), waiting for reader to reconnect")
                        self._connected.clear()  # Signal disconnected state
                    # Wait longer before retry to let reader reconnect
                    await asyncio.sleep(2.0)
                    await self._send_queue.put(payload)
                else:
                    log.warning(f"[Aster] writer loop error: {e}")
                    await asyncio.sleep(1.0)
                    await self._send_queue.put(payload)
    async def _on_msg(self, raw: Any) -> None:
        try:
            if isinstance(raw, bytes): raw = raw.decode("utf-8")
            data = json.loads(raw)
        except Exception as e: log.warning("[Aster] msg parse error: %s", e); return
        
        # Format 1: Direct BBO (legacy or some endpoints)
        if "u" in data and "b" in data and "a" in data and "s" in data:
            s = str(data["s"]).upper(); bid = float(data["b"]); ask = float(data["a"])
            self._bbo[s] = (bid, ask); self._bbo_ts[s] = time.time(); self._books.setdefault(s, Book()); return
        
        # Format 2: Wrapped stream format {"stream": "...", "data": {...}}
        stream = data.get("stream") or ""
        payload = data.get("data") or {}
        
        # Handle bookTicker stream -> BBO
        if "@bookTicker" in stream:
            s = str(payload.get("s", "")).upper()
            bid = float(payload.get("b", 0) or 0)
            ask = float(payload.get("a", 0) or 0)
            if s and bid > 0 and ask > 0:
                self._bbo[s] = (bid, ask); self._bbo_ts[s] = time.time()
                self._books.setdefault(s, Book())
            return
        
        # Handle depth stream -> L2 Book
        if "@depth" in stream:
            s = str(payload.get("s") or payload.get("symbol") or "").upper()
            bids = [(float(p), float(q)) for p, q in payload.get("b", [])]
            asks = [(float(p), float(q)) for p, q in payload.get("a", [])]
            bids = sorted(bids, key=lambda x: x[0], reverse=True)[:25]
            asks = sorted(asks, key=lambda x: x[0])[:25]
            self._l2[s] = (bids, asks); self._books[s] = Book(bids=bids, asks=asks)
            # Also update BBO from depth
            if bids and asks:
                self._bbo[s] = (bids[0][0], asks[0][0]); self._bbo_ts[s] = time.time()
    async def _server_time_ms(self) -> Optional[int]:
        for attempt in range(5):
            try:
                if not self._session:
                    timeout = aiohttp.ClientTimeout(total=8); self._session = aiohttp.ClientSession(connector=get_connector(), timeout=timeout)
                url = f"{self.rest_url}/fapi/v1/time"
                async with self._session.get(url) as r:
                    if r.status != 200: return None
                    j = await r.json(); return int(j.get("serverTime"))
            except Exception as e:
                wait = 2 ** attempt
                log.warning("[Aster] server_time error (attempt %d/5): %s. Retrying in %ds...", attempt+1, e, wait)
                await asyncio.sleep(wait)
        return None

    async def _private_get(self, path: str, params: Dict[str, Any]) -> Any:
        if not self._session:
            timeout = aiohttp.ClientTimeout(total=10); self._session = aiohttp.ClientSession(connector=get_connector(), timeout=timeout)
        if not self.api_key or not self.api_secret: raise RuntimeError("ASTER_API_KEY/ASTER_API_SECRET not set")
        url = f"{self.rest_url}{path}"; qs = self._sign_params(params); headers = {"X-MBX-APIKEY": self.api_key}
        
        for attempt in range(5):
            try:
                async with self._session.get(url, params=qs, headers=headers) as r:
                    text = await r.text()
                    try: return json.loads(text)
                    except Exception: return {"_raw_text": text, "_http_status": r.status}
            except Exception as e:
                wait = 2 ** attempt
                log.warning(f"[Aster] GET {path} error (attempt {attempt+1}/5): {e}. Retrying in {wait}s...")
                await asyncio.sleep(wait)
        return {} # Return empty dict on failure to avoid crashes

    async def _private_post(self, path: str, params: Dict[str, Any]) -> Any:
        if not self._session:
            timeout = aiohttp.ClientTimeout(total=10); self._session = aiohttp.ClientSession(connector=get_connector(), timeout=timeout)
        if not self.api_key or not self.api_secret: raise RuntimeError("ASTER_API_KEY/ASTER_API_SECRET not set")
        url = f"{self.rest_url}{path}"; qs = self._sign_params(params)
        headers = {"X-MBX-APIKEY": self.api_key, "Content-Type": "application/x-www-form-urlencoded"}
        
        for attempt in range(5):
            try:
                async with self._session.post(url, data=qs, headers=headers) as r:
                    text = await r.text()
                    try: return json.loads(text)
                    except Exception: return {"_raw_text": text, "_http_status": r.status}
            except Exception as e:
                wait = 2 ** attempt
                log.warning(f"[Aster] POST {path} error (attempt {attempt+1}/5): {e}. Retrying in {wait}s...")
                await asyncio.sleep(wait)
        return {}
    def _sign_params(self, params: Dict[str, Any]) -> Dict[str, str]:
        d = {k: v for k, v in params.items() if v is not None}
        if "timestamp" not in d: d["timestamp"] = str(int(time.time() * 1000))
        if "recvWindow" not in d: d["recvWindow"] = "5000"
        query = urllib.parse.urlencode(d, doseq=True)
        sig = hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        d["signature"] = sig; return d
    def fees_taker_bps(self) -> float:
        return float(self.fees_bps.get("taker_bps", 3.5))
    def fees_maker_bps(self) -> float:
        return float(self.fees_bps.get("maker_bps", 1.0))
