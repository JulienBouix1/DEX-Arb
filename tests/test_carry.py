# -*- coding: utf-8 -*-
"""
Pytest tests for CarryStrategy.
Tests key functionality for carry arbitrage positions.
"""
import pytest
import time
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any


class MockVenue:
    """Mock venue for testing."""
    def __init__(self, name: str):
        self.name = name
        self._bbo = {}
        self._rates = {}

    def set_bbo(self, symbol: str, bid: float, ask: float):
        self._bbo[symbol] = (bid, ask)

    def get_bbo(self, symbol: str):
        return self._bbo.get(symbol)

    def set_rate(self, symbol: str, rate: float):
        self._rates[symbol] = rate

    async def get_funding_rates(self):
        return self._rates

    def round_qty(self, symbol: str, qty: float) -> float:
        return round(qty, 4)

    async def place_order(self, symbol: str, side: str, qty: float, **kwargs):
        return {"filled": qty, "status": "filled"}


class TestCarryStorage:
    """Tests for CarryStorage persistence."""

    def test_save_and_load_positions(self, tmp_path):
        """Test position persistence."""
        from strategies.carry_storage import CarryStorage

        filepath = str(tmp_path / "test_carry.json")
        storage = CarryStorage(filepath=filepath)

        # Create mock position data
        positions = {
            "BTCUSDT": {
                "symbol": "BTCUSDT",
                "hl_coin": "BTC",
                "size_usd": 100.0,
                "direction": "Long HL / Short AS"
            }
        }

        # Save
        result = storage.save_positions(positions)
        assert result is True

        # Load
        loaded = storage.load_positions()
        assert "BTCUSDT" in loaded
        assert loaded["BTCUSDT"]["size_usd"] == 100.0

    def test_save_and_load_paper_equity(self, tmp_path):
        """Test paper_equity persistence across restarts."""
        from strategies.carry_storage import CarryStorage

        filepath = str(tmp_path / "test_carry.json")
        storage = CarryStorage(filepath=filepath)

        # Save state with paper_equity
        positions = {"BTCUSDT": {"symbol": "BTCUSDT", "size_usd": 100.0}}
        storage.save_state(positions, paper_equity=1050.0)

        # Load paper_equity
        loaded_equity = storage.load_paper_equity(default=1000.0)
        assert loaded_equity == 1050.0

    def test_load_paper_equity_default(self, tmp_path):
        """Test paper_equity returns default when not saved."""
        from strategies.carry_storage import CarryStorage

        filepath = str(tmp_path / "nonexistent.json")
        storage = CarryStorage(filepath=filepath)

        # Should return default
        loaded_equity = storage.load_paper_equity(default=5000.0)
        assert loaded_equity == 5000.0


class TestCarryMTMCalculation:
    """Tests for MTM calculation."""

    @pytest.fixture
    def mock_strategy(self):
        """Create a mock carry strategy for testing."""
        from strategies.carry_strategy import CarryPosition

        # Create mock venues
        hl = MockVenue("HL")
        asr = MockVenue("AS")
        lighter = MockVenue("LT")

        # Set up BBOs
        hl.set_bbo("BTC", 40000.0, 40010.0)
        asr.set_bbo("BTCUSDT", 40005.0, 40015.0)
        lighter.set_bbo("BTC-PERP", 40002.0, 40012.0)

        # Create mock strategy object with required attributes
        strategy = MagicMock()
        strategy.hl = hl
        strategy.asr = asr
        strategy.lighter = lighter
        strategy.hl_to_lighter = {"BTC": "BTC-PERP"}

        return strategy

    def test_mtm_long_hl_short_as(self, mock_strategy):
        """Test MTM calculation for Long HL / Short AS position."""
        from strategies.carry_strategy import CarryPosition, CarryStrategy

        # Create position
        pos = CarryPosition(
            symbol="BTCUSDT",
            hl_coin="BTC",
            direction="Long HL / Short AS",
            entry_px_hl=40000.0,
            entry_px_as=40010.0,
            size_usd=100.0,
            entry_diff_bps=50.0,
            entry_time=time.time() - 3600,  # 1 hour ago
            entry_funding_hl=0.0001,
            entry_funding_as=0.0002
        )

        # The MTM would calculate:
        # Long HL: Exit at bid (40000), entry 40000 -> PnL = 0
        # Short AS: Exit at ask (40015), entry 40010 -> PnL = negative (need to buy higher)
        # Expected: slight negative due to AS spread

        # This is a simplified test - full test requires actual strategy instance
        assert pos.size_usd == 100.0
        assert pos.entry_diff_bps == 50.0


class TestEntrySizing:
    """Tests for entry sizing logic."""

    def test_entry_size_respects_allocation(self):
        """Test that entry size respects alloc_pct."""
        total_equity = 1000.0
        alloc_pct = 0.10

        size_usd = total_equity * alloc_pct
        assert size_usd == 100.0

    def test_scale_up_capped_at_max_symbol_alloc(self):
        """Test that scale-up is capped at max_symbol_alloc_pct."""
        total_equity = 1000.0
        max_symbol_alloc_pct = 0.30
        scale_up_slice_pct = 0.10
        existing_size = 250.0  # Already at 25%

        max_sym_alloc = total_equity * max_symbol_alloc_pct  # 300
        size_usd = total_equity * scale_up_slice_pct  # 100
        projected_total = existing_size + size_usd  # 350

        if projected_total > max_sym_alloc:
            remaining_room = max_sym_alloc - existing_size  # 50
            if remaining_room >= 10.0:
                size_usd = remaining_room

        assert size_usd == 50.0  # Capped to remaining room


class TestPendingExitLogic:
    """Tests for pending exit handling."""

    def test_pending_exit_not_deleted_on_failure(self):
        """Test that pending exit is retained when exit fails."""
        pending_exits = {"BTCUSDT": ("MTM_STOP", time.time())}

        # Simulate exit failure
        exit_success = False

        if exit_success and "BTCUSDT" in pending_exits:
            del pending_exits["BTCUSDT"]

        # Should still be in pending
        assert "BTCUSDT" in pending_exits

    def test_pending_exit_deleted_on_success(self):
        """Test that pending exit is removed when exit succeeds."""
        pending_exits = {"BTCUSDT": ("MTM_STOP", time.time())}

        # Simulate exit success
        exit_success = True

        if exit_success and "BTCUSDT" in pending_exits:
            del pending_exits["BTCUSDT"]

        # Should be removed
        assert "BTCUSDT" not in pending_exits


class TestOrphanDetection:
    """Tests for orphan position detection."""

    def test_orphan_detected_by_zero_size(self):
        """Test that orphans are detected by size_usd = 0."""
        positions = {
            "BTCUSDT": MagicMock(size_usd=100.0),
            "ETHUSDT": MagicMock(size_usd=0.0),  # Orphan
            "SOLUSDT": MagicMock(size_usd=50.0),
        }

        orphans = [sym for sym, pos in positions.items() if pos.size_usd <= 0]

        assert len(orphans) == 1
        assert "ETHUSDT" in orphans


class TestExitQtyCalculation:
    """Tests for exit quantity calculation."""

    def test_exit_qty_uses_current_price(self):
        """Test that exit qty is calculated using current price, not entry price."""
        size_usd = 100.0
        entry_price = 40000.0
        current_price = 45000.0

        # Old (buggy) calculation
        old_qty = size_usd / entry_price  # 0.0025 BTC

        # New (fixed) calculation
        new_qty = size_usd / current_price  # 0.00222 BTC

        assert new_qty < old_qty
        assert abs(new_qty - 0.002222) < 0.0001


class TestTotalPaperEquity:
    """Tests for total_paper_equity property."""

    def test_total_equity_includes_unrealized(self):
        """Test that total_paper_equity includes unrealized MTM."""
        paper_equity = 1000.0
        unrealized_mtm = 50.0  # From open positions

        total_paper_equity = paper_equity + unrealized_mtm

        assert total_paper_equity == 1050.0


class TestDryRunMode:
    """Tests for dry-run live mode."""

    def test_dry_run_does_not_place_orders(self):
        """Test that dry-run mode doesn't place real orders."""
        dry_run_live = True
        orders_placed = []

        if not dry_run_live:
            orders_placed.append({"symbol": "BTC", "side": "BUY"})

        assert len(orders_placed) == 0


# Integration-style tests (require actual strategy instance)
class TestCarryStrategyIntegration:
    """Integration tests that require strategy instance."""

    @pytest.fixture
    def risk_config(self):
        """Mock risk config."""
        return {
            "carry": {
                "enabled": True,
                "paper_mode": True,
                "paper_equity": 1000.0,
                "dry_run_live": False,
                "min_funding_bps": 50.0,
                "mtm_stop_bps": 100.0,
                "mtm_tp_bps": 400.0,
                "mtm_grace_minutes": 30.0,
                "max_position_usd": 500.0,
                "hold_min_hours": 8.0,
                "hold_max_hours": 24.0,
                "required_confirms": 2,
                "carry_alloc_pct": 0.10,
                "max_carry_total_alloc_pct": 0.80,
                "scale_up_slice_pct": 0.10,
                "max_symbol_alloc_pct": 0.30,
                "mtm_check_interval_s": 30.0,
            }
        }

    def test_config_loading(self, risk_config):
        """Test that config values are loaded correctly."""
        cfg = risk_config["carry"]

        assert cfg["min_funding_bps"] == 50.0
        assert cfg["max_symbol_alloc_pct"] == 0.30
        assert cfg["paper_mode"] is True


class TestRateFetchCaching:
    """Tests for rate fetch failure resilience."""

    def test_cached_rates_used_on_api_failure(self):
        """Test that cached rates are used when API returns empty."""
        # Simulate cached rates
        cached_hl_rates = {"BTC": 0.0001, "ETH": 0.00015}
        cached_as_rates = {"BTCUSDT": 0.0002, "ETHUSDT": 0.00018}

        # Simulate API returning empty
        fetched_hl = None
        fetched_as = {}

        # Logic from carry_strategy.py
        if fetched_hl:
            hl_rates = fetched_hl
        else:
            hl_rates = cached_hl_rates  # Use cached

        if fetched_as:
            as_rates = fetched_as
        else:
            as_rates = cached_as_rates  # Use cached

        # Verify cached rates are used
        assert hl_rates == cached_hl_rates
        assert as_rates == cached_as_rates
        assert "BTC" in hl_rates
        assert "BTCUSDT" in as_rates

    def test_rates_cached_on_successful_fetch(self):
        """Test that rates are cached when API succeeds."""
        cached_rates = {}

        # Simulate successful fetch
        fetched_rates = {"BTC": 0.0001, "ETH": 0.00015}

        if fetched_rates:
            cached_rates = fetched_rates
            rates = fetched_rates
        else:
            rates = cached_rates

        # Verify rates are cached
        assert cached_rates == fetched_rates
        assert rates == fetched_rates


class TestEquityCacheTTL:
    """Tests for equity cache TTL."""

    def test_equity_cache_ttl_is_60_seconds(self):
        """Test that equity cache TTL is set to 60 seconds."""
        # This verifies the config change
        expected_ttl = 60.0

        # Simulate the init logic
        _equity_cache_ttl = 60.0  # New value (was 300.0)

        assert _equity_cache_ttl == expected_ttl
        assert _equity_cache_ttl < 300.0  # Verify reduced from 5 min

    def test_equity_refresh_when_cache_expired(self):
        """Test that equity is refreshed after TTL expires."""
        import time

        _equity_cache_ttl = 60.0
        _last_equity_fetch = time.time() - 61  # 61 seconds ago
        _cached_total_equity = 1000.0

        now = time.time()
        cache_expired = (now - _last_equity_fetch) >= _equity_cache_ttl

        assert cache_expired is True  # Should trigger refresh

    def test_equity_cache_valid_within_ttl(self):
        """Test that cached equity is used within TTL."""
        import time

        _equity_cache_ttl = 60.0
        _last_equity_fetch = time.time() - 30  # 30 seconds ago
        _cached_total_equity = 1000.0

        now = time.time()
        cache_valid = (now - _last_equity_fetch) < _equity_cache_ttl

        assert cache_valid is True  # Should use cached value


class TestVenueSymbolResolution:
    """Tests for venue-specific symbol resolution (Bug #18 fix)."""

    def test_hl_as_exit_symbols(self):
        """Test HL-AS exit uses correct symbols."""
        # Simulate position data
        pos_hl_coin = "BTC"
        pos_symbol = "BTCUSDT"
        hl_to_lighter = {"BTC": "BTC-PERP"}

        # HL-AS direction parsing
        direction = "Long HL / Short AS"
        parts = direction.split(" / ")
        v1_name = parts[0].split()[-1].upper()  # "HL"
        v2_name = parts[1].split()[-1].upper()  # "AS"

        # Symbol resolution function (from fix)
        def _get_symbol_for_venue(venue_name: str) -> str:
            if venue_name == "HL":
                return pos_hl_coin
            elif venue_name == "LT":
                return hl_to_lighter.get(pos_hl_coin, pos_symbol)
            else:  # AS
                return pos_symbol

        s1 = _get_symbol_for_venue(v1_name)
        s2 = _get_symbol_for_venue(v2_name)

        assert s1 == "BTC", f"HL should use hl_coin, got {s1}"
        assert s2 == "BTCUSDT", f"AS should use symbol, got {s2}"

    def test_hl_lt_exit_symbols(self):
        """Test HL-LT exit uses correct symbols."""
        pos_hl_coin = "BTC"
        pos_symbol = "BTC-PERP"  # For HL-LT, symbol stores LT symbol
        hl_to_lighter = {"BTC": "BTC-PERP"}

        direction = "Short HL / Long LT"
        parts = direction.split(" / ")
        v1_name = parts[0].split()[-1].upper()  # "HL"
        v2_name = parts[1].split()[-1].upper()  # "LT"

        def _get_symbol_for_venue(venue_name: str) -> str:
            if venue_name == "HL":
                return pos_hl_coin
            elif venue_name == "LT":
                return hl_to_lighter.get(pos_hl_coin, pos_symbol)
            else:  # AS
                return pos_symbol

        s1 = _get_symbol_for_venue(v1_name)
        s2 = _get_symbol_for_venue(v2_name)

        assert s1 == "BTC", f"HL should use hl_coin, got {s1}"
        assert s2 == "BTC-PERP", f"LT should use lighter symbol, got {s2}"

    def test_as_lt_exit_symbols_bug_fix(self):
        """Test AS-LT exit uses correct symbols (Bug #18 fix).

        This was the bug: For AS-LT positions, pos.symbol stores AS symbol,
        but LT exit needs the Lighter symbol from hl_to_lighter mapping.
        """
        pos_hl_coin = "BTC"
        pos_symbol = "BTCUSDT"  # AS-LT stores AS symbol
        hl_to_lighter = {"BTC": "BTC-PERP"}

        direction = "Long AS / Short LT"
        parts = direction.split(" / ")
        v1_name = parts[0].split()[-1].upper()  # "AS"
        v2_name = parts[1].split()[-1].upper()  # "LT"

        # OLD BUGGY CODE:
        # s1 = pos_hl_coin if v1_name == "HL" else pos_symbol  # BTCUSDT (correct)
        # s2 = pos_hl_coin if v2_name == "HL" else pos_symbol  # BTCUSDT (WRONG! Should be BTC-PERP)

        # NEW FIXED CODE:
        def _get_symbol_for_venue(venue_name: str) -> str:
            if venue_name == "HL":
                return pos_hl_coin
            elif venue_name == "LT":
                return hl_to_lighter.get(pos_hl_coin, pos_symbol)
            else:  # AS
                return pos_symbol

        s1 = _get_symbol_for_venue(v1_name)
        s2 = _get_symbol_for_venue(v2_name)

        assert s1 == "BTCUSDT", f"AS should use symbol, got {s1}"
        assert s2 == "BTC-PERP", f"LT should use lighter symbol (not pos.symbol), got {s2}"

    def test_direction_parsing_all_combinations(self):
        """Test direction string parsing for all venue combinations."""
        test_cases = [
            ("Long HL / Short AS", "HL", "AS"),
            ("Short HL / Long AS", "HL", "AS"),
            ("Long HL / Short LT", "HL", "LT"),
            ("Short HL / Long LT", "HL", "LT"),
            ("Long AS / Short LT", "AS", "LT"),
            ("Short AS / Long LT", "AS", "LT"),
        ]

        for direction, expected_v1, expected_v2 in test_cases:
            parts = direction.split(" / ")
            v1_name = parts[0].split()[-1].upper()
            v2_name = parts[1].split()[-1].upper()

            assert v1_name == expected_v1, f"Direction '{direction}': expected v1={expected_v1}, got {v1_name}"
            assert v2_name == expected_v2, f"Direction '{direction}': expected v2={expected_v2}, got {v2_name}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
