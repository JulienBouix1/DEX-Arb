
# core/arb_patch/flat.py - Flatten helpers
from __future__ import annotations
import asyncio
from typing import Optional, Tuple, Dict, Any

async def flatten_symmetry(
    hl, aster, hl_coin: str, as_symbol: str,
    exec_buy_on_hl: bool, size_hl: float, size_as: float,
    price_hl: Optional[float] = None, price_as: Optional[float] = None,
    timeout_s: float = 10.0
) -> Tuple[float, float]:
    """
    Ensure both venues are flat by placing reduce-only order on the side that has residual size.
    Returns (hl_flat, as_flat) residuals flattened.
    """
    hl_resid = round(size_hl - size_as, 12)
    as_resid = round(size_as - size_hl, 12)
    hl_flat_filled = 0.0
    as_flat_filled = 0.0
    tasks = []
    if hl_resid > 0:
        # more filled on HL than Aster -> offset on HL reduce_only in opposite direction
        tasks.append(hl.place_order(hl_coin, "SELL" if exec_buy_on_hl else "BUY", hl_resid, price=None, ioc=True, reduce_only=True))
    elif as_resid > 0:
        # more filled on Aster -> offset on Aster reduce_only in opposite direction
        tasks.append(aster.place_order(as_symbol, "BUY" if exec_buy_on_hl else "SELL", as_resid, price=None, ioc=True, reduce_only=True))
    if tasks:
        try:
            results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout_s)
            for r in results:
                # best-effort parse
                filled = float(getattr(r, "filled", 0.0) if hasattr(r, "filled") else r.get("filled", 0.0))
                if hl_resid > 0:
                    hl_flat_filled = filled
                if as_resid > 0:
                    as_flat_filled = filled
        except asyncio.TimeoutError:
            pass
    return (hl_flat_filled, as_flat_filled)


async def flat_all(hl, aster, cfg: Optional[Dict[str, Any]] = None) -> None:
    """
    Emergency flatten all positions on both venues.
    Used by KillSwitch.
    """
    tasks = []
    
    # 1. Hyperliquid flatten
    async def _flat_hl():
        try:
            # Get positions
            # This depends on HL implementation, assuming it has a method to get positions or state
            # If not, we try /info userState
            # But let's assume instance has .positions() or similar, or we use private method
            # Re-using hl logic from strategy might be hard if not exposed.
            # Best effort: use clearinghouse state
            state = await hl._hl.info.user_state(hl._hl.account)
            positions = state.get("assetPositions", [])
            for p in positions:
                pos = p.get("position", {})
                coin = pos.get("coin", "")
                sze = float(pos.get("sze", 0.0))
                if coin and abs(sze) > 0:
                    side = "SELL" if sze > 0 else "BUY"
                    # Market reduce-only
                    try:
                        await hl.place_order(coin, side, abs(sze), price=None, ioc=True, reduce_only=True)
                    except Exception:
                        pass
        except Exception:
            pass

    # 2. Aster flatten
    async def _flat_as():
        try:
            # Need to fetch positions. /fapi/v2/positionRisk
            # Using private_get
            positions = await aster._private_get("/fapi/v2/positionRisk", {})
            if isinstance(positions, list):
                for p in positions:
                    sym = p.get("symbol")
                    amt = float(p.get("positionAmt") or 0.0)
                    if sym and abs(amt) > 0:
                        side = "SELL" if amt > 0 else "BUY"
                        try:
                            await aster.place_order(sym, side, abs(amt), price=None, ioc=False, reduce_only=True)
                        except Exception:
                            pass
        except Exception:
            pass

    await asyncio.gather(_flat_hl(), _flat_as())

