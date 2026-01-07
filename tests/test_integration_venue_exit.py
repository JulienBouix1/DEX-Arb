# -*- coding: utf-8 -*-
"""
Integration test: Verify exit symbol resolution works for all venue combinations.
This test creates mock positions and verifies the exit logic resolves symbols correctly.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from unittest.mock import MagicMock, AsyncMock, patch
import asyncio


class TestExitSymbolResolutionIntegration:
    """Integration tests for exit symbol resolution with mock venues."""

    @pytest.fixture
    def mock_venues(self):
        """Create mock venues that track what symbols are called."""
        # Track calls to place_order
        hl_calls = []
        as_calls = []
        lt_calls = []

        hl = MagicMock()
        hl.name = "HL"
        hl.get_bbo = MagicMock(return_value=(40000.0, 40010.0))
        hl.round_qty = MagicMock(side_effect=lambda s, q: round(q, 6))

        async def hl_place_order(symbol, side, qty, **kwargs):
            hl_calls.append({"symbol": symbol, "side": side, "qty": qty})
            return {"filled": qty, "avg_price": 40005.0, "status": "filled"}
        hl.place_order = hl_place_order

        asr = MagicMock()
        asr.name = "AS"
        asr.get_bbo = MagicMock(return_value=(40005.0, 40015.0))
        asr.round_qty = MagicMock(side_effect=lambda s, q: round(q, 6))

        async def as_place_order(symbol, side, qty, **kwargs):
            as_calls.append({"symbol": symbol, "side": side, "qty": qty})
            return {"filled": qty, "avg_price": 40010.0, "status": "filled"}
        asr.place_order = as_place_order

        lighter = MagicMock()
        lighter.name = "LT"
        lighter.get_bbo = MagicMock(return_value=(40002.0, 40012.0))
        lighter.round_qty = MagicMock(side_effect=lambda s, q: round(q, 6))

        async def lt_place_order(symbol, side, qty, **kwargs):
            lt_calls.append({"symbol": symbol, "side": side, "qty": qty})
            return {"filled": qty, "avg_price": 40007.0, "status": "filled"}
        lighter.place_order = lt_place_order

        return {
            "hl": hl,
            "asr": asr,
            "lighter": lighter,
            "hl_calls": hl_calls,
            "as_calls": as_calls,
            "lt_calls": lt_calls,
        }

    @pytest.mark.asyncio
    async def test_hl_as_exit_places_orders_with_correct_symbols(self, mock_venues):
        """Test that HL-AS exit calls place_order with correct symbols."""
        from strategies.carry_strategy import CarryStrategy, CarryPosition
        import time

        # Create minimal risk config
        risk = {
            "carry": {
                "enabled": True,
                "paper_mode": False,
                "dry_run_live": False,  # Actually call place_order
            }
        }
        cfg = {}
        notifier = MagicMock()

        # Create strategy with mocks
        strategy = CarryStrategy(
            hl=mock_venues["hl"],
            asr=mock_venues["asr"],
            cfg=cfg,
            pairs_map={"BTCUSDT": "BTC"},
            risk=risk,
            notifier=notifier,
            lighter=mock_venues["lighter"],
            lighter_map={"BTC-PERP": "BTC"},
        )
        strategy.hl_to_lighter = {"BTC": "BTC-PERP"}
        strategy._cached_total_equity = 1000.0

        # Create HL-AS position
        pos = CarryPosition(
            symbol="BTCUSDT",
            hl_coin="BTC",
            direction="Long HL / Short AS",
            entry_px_hl=40000.0,
            entry_px_as=40010.0,
            size_usd=100.0,
            entry_diff_bps=50.0,
            entry_time=time.time() - 3600,
            entry_funding_hl=0.0001,
            entry_funding_as=0.0002,
        )
        strategy.positions["BTCUSDT"] = pos

        # Execute exit
        result = await strategy._execute_exit("BTCUSDT", "TEST_EXIT", 0.0, 0.0)

        # Verify HL was called with "BTC" (not "BTCUSDT")
        assert len(mock_venues["hl_calls"]) == 1
        assert mock_venues["hl_calls"][0]["symbol"] == "BTC", \
            f"HL should use 'BTC', got '{mock_venues['hl_calls'][0]['symbol']}'"

        # Verify AS was called with "BTCUSDT"
        assert len(mock_venues["as_calls"]) == 1
        assert mock_venues["as_calls"][0]["symbol"] == "BTCUSDT", \
            f"AS should use 'BTCUSDT', got '{mock_venues['as_calls'][0]['symbol']}'"

    @pytest.mark.asyncio
    async def test_hl_lt_exit_places_orders_with_correct_symbols(self, mock_venues):
        """Test that HL-LT exit calls place_order with correct symbols."""
        from strategies.carry_strategy import CarryStrategy, CarryPosition
        import time

        risk = {
            "carry": {
                "enabled": True,
                "paper_mode": False,
                "dry_run_live": False,
            }
        }
        cfg = {}
        notifier = MagicMock()

        strategy = CarryStrategy(
            hl=mock_venues["hl"],
            asr=mock_venues["asr"],
            cfg=cfg,
            pairs_map={"BTCUSDT": "BTC"},
            risk=risk,
            notifier=notifier,
            lighter=mock_venues["lighter"],
            lighter_map={"BTC-PERP": "BTC"},
        )
        strategy.hl_to_lighter = {"BTC": "BTC-PERP"}
        strategy._cached_total_equity = 1000.0

        # Create HL-LT position
        pos = CarryPosition(
            symbol="BTC-PERP",  # LT symbol stored
            hl_coin="BTC",
            direction="Short HL / Long LT",
            entry_px_hl=40000.0,
            entry_px_as=40007.0,  # Actually LT price
            size_usd=100.0,
            entry_diff_bps=50.0,
            entry_time=time.time() - 3600,
            entry_funding_hl=0.0001,
            entry_funding_as=0.00015,  # Actually LT rate
        )
        strategy.positions["BTC-PERP"] = pos

        # Execute exit
        result = await strategy._execute_exit("BTC-PERP", "TEST_EXIT", 0.0, 0.0)

        # Verify HL was called with "BTC"
        assert len(mock_venues["hl_calls"]) == 1
        assert mock_venues["hl_calls"][0]["symbol"] == "BTC", \
            f"HL should use 'BTC', got '{mock_venues['hl_calls'][0]['symbol']}'"

        # Verify LT was called with "BTC-PERP"
        assert len(mock_venues["lt_calls"]) == 1
        assert mock_venues["lt_calls"][0]["symbol"] == "BTC-PERP", \
            f"LT should use 'BTC-PERP', got '{mock_venues['lt_calls'][0]['symbol']}'"

    @pytest.mark.asyncio
    async def test_as_lt_exit_places_orders_with_correct_symbols(self, mock_venues):
        """Test that AS-LT exit calls place_order with correct symbols.

        THIS IS THE BUG #18 TEST - Previously, LT would be called with "BTCUSDT"
        instead of "BTC-PERP".
        """
        from strategies.carry_strategy import CarryStrategy, CarryPosition
        import time

        risk = {
            "carry": {
                "enabled": True,
                "paper_mode": False,
                "dry_run_live": False,
            }
        }
        cfg = {}
        notifier = MagicMock()

        strategy = CarryStrategy(
            hl=mock_venues["hl"],
            asr=mock_venues["asr"],
            cfg=cfg,
            pairs_map={"BTCUSDT": "BTC"},
            risk=risk,
            notifier=notifier,
            lighter=mock_venues["lighter"],
            lighter_map={"BTC-PERP": "BTC"},
        )
        strategy.hl_to_lighter = {"BTC": "BTC-PERP"}
        strategy._cached_total_equity = 1000.0

        # Create AS-LT position (the buggy case)
        pos = CarryPosition(
            symbol="BTCUSDT",  # AS symbol stored (this was the bug!)
            hl_coin="BTC",
            direction="Short AS / Long LT",  # AS first, LT second
            entry_px_hl=40010.0,  # Actually AS price (entry_px_hl = v1 price)
            entry_px_as=40007.0,  # Actually LT price (entry_px_as = v2 price)
            size_usd=100.0,
            entry_diff_bps=50.0,
            entry_time=time.time() - 3600,
            entry_funding_hl=0.0002,  # AS rate
            entry_funding_as=0.00015,  # LT rate
        )
        strategy.positions["BTCUSDT"] = pos

        # Execute exit
        result = await strategy._execute_exit("BTCUSDT", "TEST_EXIT", 0.0, 0.0)

        # Verify AS was called with "BTCUSDT"
        assert len(mock_venues["as_calls"]) == 1
        assert mock_venues["as_calls"][0]["symbol"] == "BTCUSDT", \
            f"AS should use 'BTCUSDT', got '{mock_venues['as_calls'][0]['symbol']}'"

        # Verify LT was called with "BTC-PERP" (NOT "BTCUSDT" which was the bug!)
        assert len(mock_venues["lt_calls"]) == 1
        assert mock_venues["lt_calls"][0]["symbol"] == "BTC-PERP", \
            f"LT should use 'BTC-PERP', got '{mock_venues['lt_calls'][0]['symbol']}' (BUG #18 not fixed!)"

        # Verify HL was NOT called (this is AS-LT, not HL)
        assert len(mock_venues["hl_calls"]) == 0, \
            "HL should NOT be called for AS-LT position"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
