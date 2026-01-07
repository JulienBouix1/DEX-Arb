# core/orderbook.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Tuple, Optional

@dataclass
class Book:
    bids: List[Tuple[float, float]] = field(default_factory=list)  # (price, qty)
    asks: List[Tuple[float, float]] = field(default_factory=list)

    def best_bid(self) -> Optional[float]:
        if not self.bids:
            return None
        return max(self.bids, key=lambda x: x[0])[0]

    def best_ask(self) -> Optional[float]:
        if not self.asks:
            return None
        return min(self.asks, key=lambda x: x[0])[0]

    def mid(self) -> Optional[float]:
        bb = self.best_bid()
        ba = self.best_ask()
        if bb is None or ba is None or bb <= 0 or ba <= 0:
            return None
        return (bb + ba) / 2.0
