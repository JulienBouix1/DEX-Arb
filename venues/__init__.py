# -*- coding: utf-8 -*-
"""
Venue connectors for trading across decentralized exchanges.

Supported venues:
- Hyperliquid (HL) - Native DEX with spot and perp markets
- Aster (AS) - Binance Futures-compatible protocol
- Lighter (LT) - zkSync L2 DEX

All venues inherit from VenueBase to ensure consistent interface.
"""
from venues.base import VenueBase, OrderResult, Position

__all__ = [
    "VenueBase",
    "OrderResult",
    "Position",
]
