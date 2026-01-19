# -*- coding: utf-8 -*-
# Hyperliquid lightweight client (hotfix v7)
from __future__ import annotations
import os, json, logging
import asyncio
import time
from typing import Any, Dict, Optional
log = logging.getLogger("venues.hl_client")
_DEFAULT_REST = "https://api.hyperliquid.xyz"
_DEFAULT_WS   = "wss://api.hyperliquid.xyz/ws"
def _looks_url(s: Optional[str]) -> bool:
    return isinstance(s, str) and "://" in s
def _sanitize_url(u: Optional[str], fallback: str) -> str:
    if not isinstance(u, str):
        return fallback
    s = u.strip()
    if not s or s.lower().startswith("0x") or "://" not in s:
        return fallback
    return s
def _deep_find_equity(obj: Any) -> Optional[float]:
    try:
        if isinstance(obj, dict):
            for k in ("equity","accountValue","totalAccountValue","totalEquity","nav","totalWalletBalance","totalMarginBalance"):
                if k in obj:
                    try: return float(obj[k])
                    except Exception: pass
            for v in obj.values():
                r = _deep_find_equity(v)
                if r is not None: return r
        elif isinstance(obj, (list, tuple)):
            for it in obj:
                r = _deep_find_equity(it)
                if r is not None: return r
    except Exception: pass
    return None
class HLClient:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        rest_url = kwargs.pop("rest_url", None)
        ws_url   = kwargs.pop("ws_url", None)
        user_address = kwargs.pop("user_address", None) or os.getenv("HL_ACCOUNT_ADDRESS") or os.getenv("HL_ADDRESS")
        private_key_env = kwargs.pop("private_key_env", None) or "HL_PRIVATE_KEY"
        if len(args) >= 1 and isinstance(args[0], str):
            a = args[0]
            if "://" in a: rest_url = a
            elif a.lower().startswith("0x"): user_address = a
            else: private_key_env = a
        if len(args) >= 2 and isinstance(args[1], str):
            b = args[1]
            if b.lower().startswith("0x"): user_address = b
            elif "://" in b and not rest_url: rest_url = b
        if len(args) >= 3 and isinstance(args[2], str):
            c = args[2]
            if "://" not in c: private_key_env = c
        self.rest_url = _sanitize_url(rest_url or os.getenv("HL_REST_URL"), _DEFAULT_REST)
        self.ws_url   = _sanitize_url(ws_url   or os.getenv("HL_WS_URL"),   _DEFAULT_WS)
        self.user_address: Optional[str] = user_address
        self._privkey: Optional[str] = os.getenv(private_key_env)
        self._sdk = None
        self._sdk_ok = False
        self._api_ok = False
        self._meta: Dict[str, Any] = {} # UPPER -> {szDecimals, ...}
        self._asset_map: Dict[str, str] = {} # UPPER -> originalName
        try:
            self._init_sdk()
        except Exception as e:
            log.error("Failed to init HL SDK: %s", e, exc_info=True)
    async def equity(self, session, address: Optional[str] = None) -> Optional[float]:
        addr = address or self.user_address or os.getenv("HL_ACCOUNT_ADDRESS") or os.getenv("HL_ADDRESS")
        if not addr: return None
        
        # OPTIMIZATION: Cache equity for 15 seconds to reduce pre-trade latency
        now = time.time()
        cache_key = f"equity_{addr}"
        cached = getattr(self, "_equity_cache", {})
        if cache_key in cached:
            val, ts = cached[cache_key]
            if (now - ts) < 15.0:  # 15 second cache TTL
                return val
        
        # Fetch fresh value
        result = None
        try:
            info = getattr(self._sdk, "info", None)
            if info and hasattr(info, "user_state"):
                loop = asyncio.get_running_loop()
                st = await loop.run_in_executor(None, lambda: info.user_state(addr))
                val = _deep_find_equity(st)
                if isinstance(val, (int,float)): 
                    result = float(val)
        except Exception: pass
        
        if result is None:
            try:
                url = self.rest_url.rstrip("/") + "/info"
                for payload in ({"type":"userState","user":addr},{"type":"clearinghouseState","user":addr},{"type":"balance","user":addr}):
                    r = await session.post(url, json=payload)
                    if r.status != 200: continue
                    j = await r.json()
                    val = _deep_find_equity(j)
                    if isinstance(val, (int,float)): 
                        result = float(val)
                        break
            except Exception: pass
        
        # Cache the result
        if result is not None:
            if not hasattr(self, "_equity_cache"):
                self._equity_cache = {}
            self._equity_cache[cache_key] = (result, now)
        
        return result

    def _init_sdk(self) -> None:
        try:
            from hyperliquid.exchange import Exchange  # type: ignore
            from eth_account import Account  # type: ignore
        except Exception as e:
            log.warning("hyperliquid SDK not installed or eth_account missing: %s", e)
            self._sdk_ok = False; self._api_ok = False; return
        base = self.rest_url
        if not _looks_url(base) or base.lower().startswith("0x"):
            base = _DEFAULT_REST; self.rest_url = base
        wallet = None
        try:
            if self._privkey and self._privkey.lower().startswith("0x"):
                wallet = Account.from_key(self._privkey)
        except Exception: wallet = None
        try:
            # ORDRE CORRECT (wallet, base_url, account_address=...) cf. SDK
            self._sdk = Exchange(wallet, base, account_address=self.user_address)
            info = getattr(self._sdk, "info", None)
            if info:
                # Cache perp meta for rounding
                if hasattr(info, "meta"):
                    try:
                        m = info.meta()
                        if m and "universe" in m:
                            for asset in m["universe"]:
                                orig_name = asset.get("name", "")
                                upper_name = orig_name.upper()
                                if orig_name:
                                    self._meta[upper_name] = asset
                                    self._asset_map[upper_name] = orig_name
                    except Exception as e:
                        log.debug(f"[HL] SDK meta fetch failed: {e}")
                if hasattr(info, "spot_meta"): _ = info.spot_meta()
            
            self._sdk_ok = True; self._api_ok = True
            log.info("[HL] SDK ready (base=%s, %d assets in meta)", base, len(self._meta))
        except Exception:
            self._sdk_ok = False; self._api_ok = False; raise
        
        # Always attempt REST meta fetch as fallback or to ensure data
        asyncio.create_task(self._fetch_meta_rest())
    
    async def _fetch_meta_rest(self) -> None:
        """Fetch perpetual metadata via REST for rounding and precision."""
        url = f"{self.rest_url}/info"
        payload = {"type": "meta"}
        try:
            # We need a session, if not started yet, make a temporary one
            session = self._session
            close_session = False
            if not session or session.closed:
                session = aiohttp.ClientSession(connector=get_connector(), timeout=aiohttp.ClientTimeout(total=10))
                close_session = True
            
            async with session.post(url, json=payload) as r:
                if r.status == 200:
                    m = await r.json()
                    if m and "universe" in m:
                        for asset in m["universe"]:
                            orig_name = asset.get("name", "")
                            upper_name = orig_name.upper()
                            if orig_name:
                                self._meta[upper_name] = asset
                                self._asset_map[upper_name] = orig_name
                        # log.info(f"[HL] REST meta updated: {len(self._meta)} assets")
            
            if close_session:
                await session.close()
        except Exception as e:
            log.debug(f"[HL] REST meta fetch failed: {e}")

    async def set_isolated_margin(self, coin: str, leverage: int = 5) -> bool:
        """Set isolated margin mode for a specific coin with given leverage. Cached per session.
        Implements fallback: tries requested leverage, then 3x, then 2x if rejected."""
        if not self._sdk:
            return False

        if not hasattr(self, "_margin_cache"):
            self._margin_cache = set()

        # Check if already set for this coin (any leverage)
        coin_upper = coin.upper()
        if any(k.startswith(f"{coin_upper}_") for k in self._margin_cache):
            return True

        loop = asyncio.get_running_loop()
        sdk_coin = self._asset_map.get(coin_upper, coin)

        # Try leverage levels with fallback
        leverage_levels = [leverage]
        if leverage > 3:
            leverage_levels.append(3)
        if leverage > 2:
            leverage_levels.append(2)

        for lev in leverage_levels:
            try:
                res = await loop.run_in_executor(
                    None,
                    lambda l=lev: self._sdk.update_leverage(l, sdk_coin, is_cross=False)
                )

                if isinstance(res, dict) and res.get("status") == "err":
                    err_msg = str(res.get("response", "")).lower()
                    if "invalid leverage" in err_msg or "leverage" in err_msg:
                        if lev > 2:
                            log.debug(f"[HL] {sdk_coin} rejected {lev}x, trying lower...")
                            continue
                    log.warning(f"[HL] Failed to set isolated margin for {sdk_coin} @ {lev}x: {res.get('response')}")
                    return False

                log.info(f"[HL] Set isolated margin for {sdk_coin} @ {lev}x")
                self._margin_cache.add(f"{coin_upper}_{lev}")
                return True

            except Exception as e:
                log.error(f"[HL] Exception setting margin for {coin} @ {lev}x: {e}")
                return False

        log.warning(f"[HL] Could not set leverage for {sdk_coin} after all fallbacks")
        return False

    def round_price(self, coin: str, price: float) -> float:
        """
        Hyperliquid dynamic rounding:
        - Price must be at most 5 significant digits.
        - Price must also respect minimum tick sizes for very low prices.
        """
        if price <= 0: return price

        # 1. HL Rule: Max 5 significant digits
        import math
        sig = 5
        try:
            # Calculate significant digit rounding
            res = round(price, sig - int(math.floor(math.log10(abs(price)))) - 1)
        except ValueError:
            # Handle edge case where price is too small for log10
            res = round(price, 8)

        # 2. CRITICAL: Clean up floating point representation noise
        # Convert to string and back to remove precision artifacts like 0.01230000000001
        res_str = f"{res:.10g}"  # 10 significant figures, strip trailing zeros
        res = float(res_str)

        # 3. Additional tick size validation for very small prices
        # HL typically requires prices to be on tick boundaries (e.g., 0.0001 for some assets)
        # If price is below 1, round to at most 6 decimal places
        if res < 1.0:
            res = round(res, 6)
        elif res < 10.0:
            res = round(res, 5)
        elif res < 100.0:
            res = round(res, 4)

        return float(res)

    def round_qty(self, coin: str, qty: float) -> float:
        """Round quantity based on szDecimals from meta."""
        meta = self._meta.get(coin.upper(), {})
        sz_decimals = int(meta.get("szDecimals", 4))
        return float(round(qty, sz_decimals))

    async def order(self, coin: str, is_buy: bool, sz: float, limit_px: Optional[float], ioc: bool = False, reduce_only: bool = False, client_id: Optional[str] = None) -> Dict[str, Any]:
        if not self._sdk: return {"status": "error", "reason": "SDK not init", "filled": 0.0}
        
        # 1. Resolve Price for Market Orders
        if limit_px is None:
            try:
                loop = asyncio.get_running_loop()
                info = getattr(self._sdk, "info", None)
                if not info: return {"status": "error", "reason": "No SDK info", "filled": 0.0}
                
                mids = await loop.run_in_executor(None, info.all_mids)
                mid = float(mids.get(coin, 0.0))
                if mid <= 0: return {"status": "error", "reason": f"No mid for {coin}", "filled": 0.0}
                
                # Aggressive limit for Market: +/- 0.5% to cross the spread
                limit_px = mid * 1.005 if is_buy else mid * 0.995
                ioc = True
            except Exception as e:
                return {"status": "error", "reason": f"Market px err: {e}", "filled": 0.0}

        # 2. CRITICAL: Round price and quantity to match HL standards
        limit_px = self.round_price(coin, limit_px)
        sz = self.round_qty(coin, sz)
        
        if sz <= 0:
            return {"status": "error", "reason": f"Qty rounded to 0 for {coin}", "filled": 0.0}
        
        # 3. Build Order Type
        order_type = {"limit": {"tif": "Ioc" if ioc else "Gtc"}}
    
        # 4. Execute via SDK
        loop = asyncio.get_running_loop()
        sdk_coin = self._asset_map.get(coin.upper(), coin)
        try:
            res = await loop.run_in_executor(None, lambda: self._sdk.order(
                sdk_coin, is_buy, sz, limit_px, order_type, reduce_only
            ))
            # Parse response to extract fill info
            # SDK returns: {"status": "ok", "response": {"type": "order", "data": {"statuses": [...]}}}
            filled = 0.0
            status = "ok"
            reason = ""
            oid = None
            avg_px = None
            
            if isinstance(res, dict):
                status = res.get("status", "ok")
                resp = res.get("response", {})
                if isinstance(resp, dict):
                    data = resp.get("data", {})
                    if isinstance(data, dict):
                        statuses = data.get("statuses", [])
                        if statuses and isinstance(statuses, list):
                            first = statuses[0]
                            if isinstance(first, dict):
                                if "filled" in first:
                                    fill_data = first["filled"]
                                    if isinstance(fill_data, dict):
                                        # {"filled": {"totalSz": "0.001", "avgPx": "90733.0", "oid": 123}}
                                        filled = float(fill_data.get("totalSz") or 0.0)
                                        avg_px = float(fill_data.get("avgPx") or 0.0)
                                        oid = fill_data.get("oid")
                                elif "resting" in first:
                                    # {"resting": {"oid": 123}}
                                    rest_data = first["resting"]
                                    oid = rest_data.get("oid")
                                elif "error" in first:
                                    status = "error"
                                    reason = str(first.get("error", ""))
                                    # DEBUG: Log price validation failures
                                    if "invalid price" in reason.lower():
                                        log.error(f"[HL] PRICE VALIDATION FAILED: coin={coin}, px={limit_px}, sz={sz}, is_buy={is_buy}, error={reason}")
            
            # Debug unusual status
            if status not in ("ok", "filled"):
                 log.warning(f"[HL] Order status '{status}': {res}")
                 
            return {"status": status, "filled": filled, "reason": reason, "oid": oid, "avgPx": avg_px}
        except Exception as e:
            return {"status": "error", "reason": str(e), "filled": 0.0}

    async def get_order_status(self, oid: int) -> Dict[str, Any]:
        """Fetch order status by OID using API."""
        if not self.user_address or not oid: return {}
        try:
            url = self.rest_url.rstrip("/") + "/info"
            payload = {"type": "orderStatus", "user": self.user_address, "oid": oid}
            if not getattr(self, "_session", None):
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, json=payload) as r:
                        if r.status == 200: return await r.json()
            else:
                # Reuse session if available (TODO: passing session would be better architecture)
                pass
            
            # Fallback naive one-off request
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as r:
                    if r.status == 200: return await r.json()
        except Exception as e:
            log.warning(f"[HL] get_order_status failed: {e}")
        return {}

Hyperliquid = HLClient
