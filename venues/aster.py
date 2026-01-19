# -*- coding: utf-8 -*-
from __future__ import annotations
import asyncio, json, logging, time, hmac, hashlib, urllib.parse, os
from typing import Any, Dict, Optional, Tuple, List
import aiohttp, websockets
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
        await asyncio.wait_for(self._connected.wait(), timeout=15)
    async def close(self) -> None:
        for t in (self._reader_task, self._writer_task):
            if t: t.cancel()
        if self._ws:
            try: await self._ws.close()
            except Exception as e: log.warning("[Aster] close error: %s", e)
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
    def get_book(self, symbol: str) -> Optional[Book]:
        return self._books.get(symbol.upper())
    
    def get_bbo_age_ms(self, symbol: str) -> float:
        """Return age of BBO in milliseconds."""
        ts = self._bbo_ts.get(symbol.upper(), 0.0)
        if ts <= 0: return 999999.0
        return (time.time() - ts) * 1000.0
    
    async def get_fresh_bbo(self, symbol: str, max_age_ms: float = 2000.0) -> Optional[Tuple[float, float]]:
        """Get BBO, using cached value if fresh enough."""
        age = self.get_bbo_age_ms(symbol)
        if age <= max_age_ms:
            return self._bbo.get(symbol.upper())
        # Aster primarily uses WS, so just return cached value
        return self._bbo.get(symbol.upper())
    
    async def fetch_bbo_rest(self, symbol: str) -> Optional[Tuple[float, float]]:
        """Fetch fresh BBO via REST API. Used for pre-trade validation."""
        url = f"{self.rest_url}/fapi/v1/ticker/bookTicker"
        try:
            if not self._session or self._session.closed:
                self._session = aiohttp.ClientSession(connector=get_connector(), timeout=aiohttp.ClientTimeout(total=10))
            
            async with self._session.get(url, params={"symbol": symbol.upper()}) as r:
                if r.status != 200:
                    return self._bbo.get(symbol.upper())  # Fallback to cache
                data = await r.json()
                if isinstance(data, dict):
                    bid = float(data.get("bidPrice", 0))
                    ask = float(data.get("askPrice", 0))
                    if bid > 0 and ask > 0:
                        return (bid, ask)
        except Exception as e:
            log.debug(f"[Aster] fetch_bbo_rest error for {symbol}: {e}")
        return self._bbo.get(symbol.upper())  # Fallback to cache
    
    async def set_isolated_margin(self, symbol: str, leverage: int = 5) -> bool:
        """Set isolated margin mode for a symbol. Delegates to set_margin_type + set_leverage."""
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
                            except (ValueError, TypeError):
                                pass
                return rates
        except Exception as e:
            log.warning("[Aster] Funding rates fetch error: %s", e)
            return {}
    
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
        Returns list of dicts with standardized keys: symbol, size, entry.
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
                size = float(item.get("positionAmt", 0) or 0)
                entry_px = float(item.get("entryPrice", 0) or 0)
                
                if symbol and abs(size) > 0:
                    positions.append({
                        "symbol": symbol,
                        "size": size,
                        "entry": entry_px
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

    async def cancel_order(self, symbol: str, order_id: Any) -> Dict[str, Any]:
        """Cancel an open order on Aster."""
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
        url = self.ws_url
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=2) as ws:
                self._ws = ws; self._connected.set(); log.info("[Aster] WS connected")
                async for msg in ws: await self._on_msg(msg)
        except asyncio.CancelledError: return
        except Exception as e:
            log.warning("[Aster] WS error: %s", e); await asyncio.sleep(1.5); self._connected.clear(); asyncio.create_task(self._ws_reader_loop())
    async def _ws_writer_loop(self) -> None:
        while True:
            try:
                payload = await self._send_queue.get()
                if not self._ws:
                    await asyncio.sleep(0.05); await self._send_queue.put(payload); continue
                await self._ws.send(json.dumps(payload))
            except asyncio.CancelledError: return
            except Exception as e:
                log.warning("[Aster] writer loop error: %s", e)
                await asyncio.sleep(0.5)
                await self._send_queue.put(payload)
    async def _on_msg(self, raw: Any) -> None:
        try:
            if isinstance(raw, bytes): raw = raw.decode("utf-8")
            data = json.loads(raw)
        except Exception as e: log.warning("[Aster] msg parse error: %s", e); return
        
        # Format 1: Direct BBO (legacy or some endpoints)
        if "u" in data and "b" in data and "a" in data and "s" in data:
            s = str(data["s"]).upper(); bid = float(data["b"]); ask = float(data["a"])
            self._bbo[s] = (bid, ask); self._bbo_ts[s] = time.time(); self._books[s] = self._books.get(s) or Book(); return
        
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
                self._books[s] = self._books.get(s) or Book()
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
