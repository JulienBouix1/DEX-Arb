# core/arb_patch/executor.py - dual leg executor with IOC and symmetry
from __future__ import annotations
import asyncio
from typing import Tuple, Dict, Any, Optional
from dataclasses import asdict, is_dataclass

def _normalize_result(res: Any) -> Dict[str, Any]:
    """Convert OrderResult dataclass to dict if needed."""
    if res is None:
        return {"status": "error", "filled": 0.0, "reason": "None result"}
    if is_dataclass(res) and not isinstance(res, type):
        return asdict(res)
    if isinstance(res, dict):
        return res
    return {"status": "error", "filled": 0.0, "reason": f"Unknown type: {type(res)}"}

async def dual_ioc(
    hl, aster,
    hl_coin: str, as_symbol: str,
    side_hl: str, side_as: str,
    qty_hl: float, qty_as: float,
    price_hl: Optional[float] = None, price_as: Optional[float] = None,
    timeout_s: float = 4.0
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Fire both IOC orders concurrently and return their results as dicts.
    """
    t1 = asyncio.create_task(hl.place_order(hl_coin, side_hl, qty_hl, price=price_hl, ioc=True, reduce_only=False))
    t2 = asyncio.create_task(aster.place_order(as_symbol, side_as, qty_as, price=price_as, ioc=True, reduce_only=False))
    done, pending = await asyncio.wait({t1, t2}, return_when=asyncio.ALL_COMPLETED, timeout=timeout_s)
    for p in pending:
        p.cancel()
    
    res_hl = _normalize_result(t1.result()) if not t1.cancelled() else {"status": "timeout", "filled": 0.0}
    res_as = _normalize_result(t2.result()) if not t2.cancelled() else {"status": "timeout", "filled": 0.0}
    
    return (res_hl, res_as)

