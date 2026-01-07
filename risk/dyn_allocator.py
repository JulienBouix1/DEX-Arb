
# risk/dyn_allocator.py - Dynamic allocator with retries and min notionals
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple

@dataclass
class AllocPlan:
    notional_usd: float
    reason: str

async def compute_notional(equity_usd: Optional[float],
                           alloc_frac: float,
                           min_notional: float,
                           max_notional: float) -> AllocPlan:
    """
    Compute a per-trade notional in USD using equity and guardrails.
    Returns 0 when equity is None or below min.
    Clamps result to [min_notional, max_notional].
    """
    if equity_usd is None or equity_usd <= 0:
        return AllocPlan(0.0, "no_equity")
    
    raw = float(equity_usd) * float(max(min(alloc_frac, 1.0), 0.0))
    
    # Clamp: if raw < min_notional, reject. Otherwise cap at max_notional.
    if raw < min_notional:
        return AllocPlan(0.0, "below_min_notional")
    
    notional = min(raw, max_notional)  # Cap at max, never exceed
    return AllocPlan(notional, "ok")

