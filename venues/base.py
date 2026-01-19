# -*- coding: utf-8 -*-
"""
Abstract base class for venue connectors.

All venue implementations (Hyperliquid, Aster, Lighter) should inherit from this
base class to ensure a consistent interface for the trading strategies.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class OrderResult:
    """Standardized order result across all venues."""
    status: str          # "filled", "partial", "rejected", "error"
    filled: float        # Amount filled
    reason: str = ""     # Error/rejection reason if any
    order_id: str = ""   # Venue-specific order ID
    price: float = 0.0   # Execution price (average if partial)
    dry_run: bool = False  # True if this was a simulated order


@dataclass
class Position:
    """Standardized position representation across all venues."""
    symbol: str          # Symbol/coin identifier
    size: float          # Signed position size (positive=long, negative=short)
    entry_price: float   # Average entry price
    unrealized_pnl: float = 0.0  # Unrealized P&L
    leverage: int = 1    # Position leverage
    margin: float = 0.0  # Margin used


class VenueBase(ABC):
    """
    Abstract base class for all venue connectors.

    Each venue implementation must provide these core methods to integrate
    with the trading strategies (Carry, Scalp) and Position Manager.
    """

    # Label for logging (e.g., "HL", "AS", "LT")
    label: str = "VENUE"

    # ==================== Lifecycle ====================

    @abstractmethod
    async def start(self) -> None:
        """
        Initialize connections (REST session, WebSocket).
        Must be called before any other methods.
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """
        Clean up connections and resources.
        Should be called on shutdown.
        """
        pass

    @abstractmethod
    def check_credentials(self) -> bool:
        """
        Verify that API credentials are present and valid.
        Does NOT make network calls - just checks if keys exist.

        Returns:
            True if credentials are configured, False otherwise.
        """
        pass

    # ==================== Account ====================

    @abstractmethod
    async def equity(self) -> Optional[float]:
        """
        Get total account equity in USD.

        Returns:
            Account equity in USD, or None if unavailable.
        """
        pass

    @abstractmethod
    async def get_positions(self) -> List[Dict[str, Any]]:
        """
        Get all open positions.

        Returns:
            List of position dicts with at minimum:
            - 'symbol' or 'coin': str
            - 'size': float (signed, positive=long)
            - 'entryPrice' or 'entry_price': float
        """
        pass

    # ==================== Market Data ====================

    @abstractmethod
    def get_bbo(self, symbol: str) -> Optional[Tuple[float, float]]:
        """
        Get best bid/offer for a symbol.

        Args:
            symbol: The trading symbol (venue-specific format)

        Returns:
            Tuple of (best_bid, best_ask), or None if unavailable.
        """
        pass

    @abstractmethod
    def get_bbo_age_ms(self, symbol: str) -> float:
        """
        Get age of BBO data in milliseconds.

        Args:
            symbol: The trading symbol

        Returns:
            Age in milliseconds since last BBO update.
        """
        pass

    @abstractmethod
    async def subscribe_tickers(self, symbols: List[str]) -> None:
        """
        Subscribe to real-time ticker/BBO updates for symbols.

        Args:
            symbols: List of symbols to subscribe to.
        """
        pass

    @abstractmethod
    async def get_funding_rates(self) -> Dict[str, float]:
        """
        Get current funding rates for all symbols.

        Returns:
            Dict mapping symbol -> funding rate (as decimal, e.g., 0.0001 = 1 bps)
        """
        pass

    # ==================== Trading ====================

    @abstractmethod
    async def place_order(
        self,
        symbol: str,
        side: str,  # "BUY" or "SELL"
        qty: float,
        price: Optional[float] = None,  # None = market order
        ioc: bool = False,  # Immediate-or-cancel
        reduce_only: bool = False,
        client_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Place an order on the venue.

        Args:
            symbol: Trading symbol
            side: "BUY" or "SELL"
            qty: Order quantity (in base asset)
            price: Limit price (None for market order)
            ioc: If True, cancel unfilled portion immediately
            reduce_only: If True, only reduce existing position
            client_id: Optional client order ID

        Returns:
            Dict with at minimum:
            - 'status': str ("filled", "partial", "rejected", etc.)
            - 'filled': float (amount filled)
            - 'order_id': str (venue order ID)
        """
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        """
        Cancel an open order.

        Args:
            order_id: The order ID to cancel
            symbol: Symbol (required by some venues)

        Returns:
            True if cancellation succeeded, False otherwise.
        """
        pass

    # ==================== Leverage (Optional) ====================

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """
        Set leverage for a symbol. Override if venue supports it.

        Args:
            symbol: Trading symbol
            leverage: Desired leverage (e.g., 3 for 3x)

        Returns:
            True if successful, False otherwise.
        """
        return True  # Default: no-op for venues without leverage setting

    # ==================== Helpers ====================

    def round_qty(self, symbol: str, qty: float) -> float:
        """
        Round quantity to venue-specific precision.
        Override if venue has specific rules.

        Args:
            symbol: Trading symbol
            qty: Raw quantity

        Returns:
            Rounded quantity.
        """
        return round(qty, 8)  # Default: 8 decimal places

    def round_price(self, symbol: str, price: float) -> float:
        """
        Round price to venue-specific precision.
        Override if venue has specific rules.

        Args:
            symbol: Trading symbol
            price: Raw price

        Returns:
            Rounded price.
        """
        return round(price, 8)  # Default: 8 decimal places
