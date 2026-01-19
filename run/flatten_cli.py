# -*- coding: utf-8 -*-
"""Flatten all open positions on Hyperliquid and Aster."""
import asyncio
import os
import sys
import yaml
import logging

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from venues.hyperliquid import Hyperliquid
from venues.aster import Aster

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("flat")


async def get_hl_positions(hl: Hyperliquid) -> list:
    """Get open positions from Hyperliquid via SDK."""
    try:
        if hl._hl._sdk and hasattr(hl._hl._sdk, "info"):
            addr = hl._hl.user_address or os.getenv("HL_ADDRESS")
            if not addr:
                log.error("No HL address configured")
                return []
            loop = asyncio.get_running_loop()
            state = await loop.run_in_executor(None, lambda: hl._hl._sdk.info.user_state(addr))
            positions = []
            if isinstance(state, dict):
                asset_positions = state.get("assetPositions", [])
                for ap in asset_positions:
                    pos = ap.get("position", {})
                    coin = pos.get("coin", "")
                    szi = float(pos.get("szi", 0) or 0)
                    if coin and abs(szi) > 0:
                        positions.append({"coin": coin, "size": szi})
            return positions
    except Exception as e:
        log.error(f"Failed to get HL positions: {e}")
    return []


async def get_aster_positions(asr: Aster) -> list:
    """Get open positions from Aster via REST API."""
    try:
        import time
        params = {"timestamp": str(int(time.time() * 1000))}
        data = await asr._private_get("/fapi/v2/positionRisk", params)
        positions = []
        if isinstance(data, list):
            for pos in data:
                symbol = pos.get("symbol", "")
                amt = float(pos.get("positionAmt", 0) or 0)
                if symbol and abs(amt) > 0:
                    positions.append({"symbol": symbol, "size": amt})
        return positions
    except Exception as e:
        log.error(f"Failed to get Aster positions: {e}")
    return []


async def flatten_hl_position(hl: Hyperliquid, coin: str, size: float) -> bool:
    """Close a single HL position with aggressive pricing for guaranteed fill."""
    side = "SELL" if size > 0 else "BUY"
    qty = abs(size)
    log.info(f"[HL] Closing {coin}: {side} {qty}")
    try:
        # Get current mid price for aggressive limit
        mid = None
        try:
            info = getattr(hl._hl._sdk, "info", None)
            if info:
                loop = asyncio.get_running_loop()
                mids = await loop.run_in_executor(None, info.all_mids)
                mid = float(mids.get(coin, 0.0))
        except Exception:
            pass
        
        # Use aggressive price: 2% slippage for guaranteed fill
        if mid and mid > 0:
            if side == "SELL":
                price = mid * 0.98  # Sell 2% below mid
            else:
                price = mid * 1.02  # Buy 2% above mid
        else:
            price = None  # Fallback to market
        
        res = await hl.place_order(coin, side, qty, price=price, ioc=True, reduce_only=True)
        filled = float(res.get("filled", 0) or 0)
        status = res.get("status", "")
        log.info(f"[HL] {coin} close result: status={status} filled={filled}/{qty} @price={price}")
        return filled >= qty * 0.95
    except Exception as e:
        log.error(f"[HL] Failed to close {coin}: {e}")
        return False


async def flatten_aster_position(asr: Aster, symbol: str, size: float) -> bool:
    """Close a single Aster position."""
    side = "SELL" if size > 0 else "BUY"
    qty = abs(size)
    log.info(f"[AS] Closing {symbol}: {side} {qty}")
    try:
        res = await asr.place_order(symbol, side, qty, price=None, ioc=False, reduce_only=True)
        filled = float(res.get("filled", 0) or 0)
        status = res.get("status", "")
        log.info(f"[AS] {symbol} close result: status={status} filled={filled}/{qty}")
        return filled >= qty * 0.95
    except Exception as e:
        log.error(f"[AS] Failed to close {symbol}: {e}")
        return False


async def main():
    log.info("=== FLATTEN ALL POSITIONS ===")
    
    # Load config
    with open("config/venues.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    
    hl_cfg = cfg.get("hyperliquid", {})
    as_cfg = cfg.get("aster", {})
    
    # Initialize venues
    hl = Hyperliquid(hl_cfg)
    asr = Aster(as_cfg)
    
    await hl.start()
    await asr.start()
    
    # Get positions
    hl_positions = await get_hl_positions(hl)
    as_positions = await get_aster_positions(asr)
    
    log.info(f"Found {len(hl_positions)} HL positions, {len(as_positions)} Aster positions")
    
    # Flatten HL
    hl_results = []
    for pos in hl_positions:
        ok = await flatten_hl_position(hl, pos["coin"], pos["size"])
        hl_results.append((pos["coin"], ok))
    
    # Flatten Aster
    as_results = []
    for pos in as_positions:
        ok = await flatten_aster_position(asr, pos["symbol"], pos["size"])
        as_results.append((pos["symbol"], ok))
    
    # Summary
    log.info("=== FLATTEN SUMMARY ===")
    for coin, ok in hl_results:
        log.info(f"HL {coin}: {'OK' if ok else 'FAILED'}")
    for sym, ok in as_results:
        log.info(f"AS {sym}: {'OK' if ok else 'FAILED'}")
    
    await hl.close()
    await asr.close()
    log.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
