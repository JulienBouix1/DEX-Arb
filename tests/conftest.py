# -*- coding: utf-8 -*-
"""
Pytest configuration and shared fixtures.
"""
import pytest
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def mock_risk_config():
    """Provide default risk configuration for tests."""
    return {
        "paper_mode": True,
        "scalp_paper_equity": 1000.0,
        "alloc_frac_per_trade": 0.05,
        "max_scalp_total_alloc_pct": 0.20,
        "notional_per_trade_min": 12.0,
        "notional_per_trade_max": 200.0,
        "spread_entry_bps": 40.0,
        "slippage_bps_buffer": 2.0,
        "flatten_timeout_s": 15.0,
        "estimated_exit_fees_bps": 7.0,
        "max_bbo_age_ms": 500.0,
        "min_slippage_bps": 5.0,
        "max_spread_pct": 1.0,
        "min_trade_gap_s": 0.5,
        "symbol_cooldown_s": 10.0,
        "required_confirms": 1,
        "max_edge_bps": 150.0,
        "max_volatility_bps": 50.0,
        "min_volume_24h_usd": 150000.0,
        "carry": {
            "enabled": True,
            "paper_mode": True,
            "paper_equity": 1000.0,
            "dry_run_live": False,
            "min_funding_bps": 50.0,
            "mtm_stop_bps": 100.0,
            "mtm_tp_bps": 400.0,
            "mtm_grace_minutes": 30.0,
            "mtm_yield_multiplier": 5.0,
            "mtm_check_interval_s": 30.0,
            "mtm_trailing_bps": 150.0,
            "mtm_trailing_min_bps": 100.0,
            "max_position_usd": 500.0,
            "hold_min_hours": 8.0,
            "hold_max_hours": 24.0,
            "required_confirms": 2,
            "carry_alloc_pct": 0.10,
            "max_carry_total_alloc_pct": 0.80,
            "scale_up_slice_pct": 0.10,
            "max_symbol_alloc_pct": 0.30,
            "optimization_buffer_bps": 20.0,
            "min_volume_24h_usd": 150000.0,
            "min_volume_premium_usd": 500000.0,
            "premium_funding_bps": 120.0,
        },
        "global": {
            "max_drawdown_pct": 5.0,
            "max_total_notional_usd": 5000.0,
        }
    }


@pytest.fixture
def mock_hl_venue():
    """Provide a mock Hyperliquid venue."""
    from unittest.mock import MagicMock, AsyncMock

    venue = MagicMock()
    venue.name = "HL"
    venue.get_bbo = MagicMock(return_value=(40000.0, 40010.0))
    venue.get_funding_rates = AsyncMock(return_value={"BTC": 0.0001})
    venue.round_qty = MagicMock(side_effect=lambda s, q: round(q, 4))
    venue.place_order = AsyncMock(return_value={"filled": 0.01, "status": "filled"})

    return venue


@pytest.fixture
def mock_as_venue():
    """Provide a mock Aster venue."""
    from unittest.mock import MagicMock, AsyncMock

    venue = MagicMock()
    venue.name = "AS"
    venue.get_bbo = MagicMock(return_value=(40005.0, 40015.0))
    venue.get_funding_rates = AsyncMock(return_value={"BTCUSDT": 0.0002})
    venue.round_qty = MagicMock(side_effect=lambda s, q: round(q, 4))
    venue.place_order = AsyncMock(return_value={"filled": 0.01, "status": "filled"})

    return venue


@pytest.fixture
def mock_lt_venue():
    """Provide a mock Lighter venue."""
    from unittest.mock import MagicMock, AsyncMock

    venue = MagicMock()
    venue.name = "LT"
    venue.get_bbo = MagicMock(return_value=(40002.0, 40012.0))
    venue.get_funding_rates = AsyncMock(return_value={"BTC-PERP": 0.00015})
    venue.round_qty = MagicMock(side_effect=lambda s, q: round(q, 4))
    venue.place_order = AsyncMock(return_value={"filled": 0.01, "status": "filled"})

    return venue
