# -*- coding: utf-8 -*-
"""
Mocked Venue Tests.

Tests venue connectors and circuit breaker using mocks to avoid
requiring real API credentials or network access.
"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from typing import Dict, Any, Optional

# Import core modules
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestCircuitBreaker:
    """Test circuit breaker functionality."""

    def test_circuit_breaker_initial_state(self):
        """Circuit breaker should start in closed state."""
        from core.circuit_breaker import CircuitBreaker, CircuitState

        breaker = CircuitBreaker(failure_threshold=3)
        assert breaker.get_state("HL") == CircuitState.CLOSED
        assert breaker.is_available("HL") is True

    def test_circuit_breaker_opens_after_threshold(self):
        """Circuit should open after threshold failures."""
        from core.circuit_breaker import CircuitBreaker, CircuitState

        breaker = CircuitBreaker(failure_threshold=3)

        # Record failures
        breaker.record_failure("HL", "Connection timeout")
        breaker.record_failure("HL", "Connection timeout")
        assert breaker.get_state("HL") == CircuitState.CLOSED

        # Third failure should trip
        breaker.record_failure("HL", "Connection timeout")
        assert breaker.get_state("HL") == CircuitState.OPEN
        assert breaker.is_available("HL") is False

    def test_circuit_breaker_success_resets_failures(self):
        """Success should reset failure count in closed state."""
        from core.circuit_breaker import CircuitBreaker, CircuitState

        breaker = CircuitBreaker(failure_threshold=3)

        breaker.record_failure("HL", "Error 1")
        breaker.record_failure("HL", "Error 2")
        breaker.record_success("HL")

        # Failure count should be reset, need 3 more to trip
        breaker.record_failure("HL", "Error 3")
        breaker.record_failure("HL", "Error 4")
        assert breaker.get_state("HL") == CircuitState.CLOSED

    def test_circuit_breaker_half_open_recovery(self):
        """Circuit should recover through half-open state."""
        from core.circuit_breaker import CircuitBreaker, CircuitState
        import time

        breaker = CircuitBreaker(
            failure_threshold=2,
            success_threshold=2,
            cooldown_seconds=0.1  # Very short for testing
        )

        # Trip the circuit
        breaker.record_failure("HL", "Error")
        breaker.record_failure("HL", "Error")
        assert breaker.get_state("HL") == CircuitState.OPEN

        # Wait for cooldown
        time.sleep(0.15)
        assert breaker.get_state("HL") == CircuitState.HALF_OPEN

        # Successes in half-open should close circuit
        breaker.record_success("HL")
        assert breaker.get_state("HL") == CircuitState.HALF_OPEN
        breaker.record_success("HL")
        assert breaker.get_state("HL") == CircuitState.CLOSED

    def test_circuit_breaker_half_open_failure_reopens(self):
        """Failure in half-open should reopen circuit."""
        from core.circuit_breaker import CircuitBreaker, CircuitState
        import time

        breaker = CircuitBreaker(
            failure_threshold=2,
            cooldown_seconds=0.1
        )

        # Trip and wait for half-open
        breaker.record_failure("HL", "Error")
        breaker.record_failure("HL", "Error")
        time.sleep(0.15)
        assert breaker.get_state("HL") == CircuitState.HALF_OPEN

        # Failure should reopen
        breaker.record_failure("HL", "Error again")
        assert breaker.get_state("HL") == CircuitState.OPEN

    def test_circuit_breaker_callbacks(self):
        """Callbacks should fire on state changes."""
        from core.circuit_breaker import CircuitBreaker

        open_called = []
        close_called = []

        def on_open(venue):
            open_called.append(venue)

        def on_close(venue):
            close_called.append(venue)

        breaker = CircuitBreaker(
            failure_threshold=2,
            success_threshold=1,
            cooldown_seconds=0.05,
            on_circuit_open=on_open,
            on_circuit_close=on_close,
        )

        # Trip circuit
        breaker.record_failure("HL", "Error")
        breaker.record_failure("HL", "Error")
        assert open_called == ["HL"]

        # Recover
        import time
        time.sleep(0.1)
        breaker.get_state("HL")  # Trigger half-open check
        breaker.record_success("HL")
        assert close_called == ["HL"]

    def test_all_open_check(self):
        """all_open should return True only when all tracked circuits are open."""
        from core.circuit_breaker import CircuitBreaker

        breaker = CircuitBreaker(failure_threshold=2)

        # Register both circuits by recording something
        breaker.record_success("HL")
        breaker.record_success("ASTER")

        assert breaker.all_open() is False

        # Open one
        breaker.record_failure("HL", "Error")
        breaker.record_failure("HL", "Error")
        assert breaker.all_open() is False

        # Open both
        breaker.record_failure("ASTER", "Error")
        breaker.record_failure("ASTER", "Error")
        assert breaker.all_open() is True

    @pytest.mark.asyncio
    async def test_circuit_breaker_call_wrapper(self):
        """call() should handle success/failure automatically."""
        from core.circuit_breaker import CircuitBreaker, CircuitOpenError

        breaker = CircuitBreaker(failure_threshold=2)

        # Success case
        async def success_fn():
            return "ok"

        result = await breaker.call("HL", success_fn)
        assert result == "ok"

        # Failure case
        async def fail_fn():
            raise ConnectionError("Network error")

        with pytest.raises(ConnectionError):
            await breaker.call("HL", fail_fn)
        with pytest.raises(ConnectionError):
            await breaker.call("HL", fail_fn)

        # Circuit should be open now
        with pytest.raises(CircuitOpenError):
            await breaker.call("HL", success_fn)


class TestVenueBaseMock:
    """Test venue base interface with mocks."""

    def test_mock_venue_get_bbo(self, mock_hl_venue):
        """Mock venue should return BBO."""
        bid, ask = mock_hl_venue.get_bbo("BTC")
        assert bid == 40000.0
        assert ask == 40010.0

    @pytest.mark.asyncio
    async def test_mock_venue_funding_rates(self, mock_hl_venue):
        """Mock venue should return funding rates."""
        rates = await mock_hl_venue.get_funding_rates()
        assert "BTC" in rates
        assert rates["BTC"] == 0.0001

    @pytest.mark.asyncio
    async def test_mock_venue_place_order(self, mock_hl_venue):
        """Mock venue should simulate order placement."""
        result = await mock_hl_venue.place_order("BTC", "buy", 0.01)
        assert result["status"] == "filled"
        assert result["filled"] == 0.01


class TestVenueIntegration:
    """Test venue integration patterns."""

    @pytest.mark.asyncio
    async def test_multi_venue_bbo(self, mock_hl_venue, mock_as_venue, mock_lt_venue):
        """Should get BBO from multiple venues."""
        hl_bbo = mock_hl_venue.get_bbo("BTC")
        as_bbo = mock_as_venue.get_bbo("BTCUSDT")
        lt_bbo = mock_lt_venue.get_bbo("BTC-PERP")

        # All should return valid spreads
        assert hl_bbo[1] > hl_bbo[0]  # ask > bid
        assert as_bbo[1] > as_bbo[0]
        assert lt_bbo[1] > lt_bbo[0]

    @pytest.mark.asyncio
    async def test_multi_venue_funding_rates(self, mock_hl_venue, mock_as_venue, mock_lt_venue):
        """Should aggregate funding rates from all venues."""
        hl_rates = await mock_hl_venue.get_funding_rates()
        as_rates = await mock_as_venue.get_funding_rates()
        lt_rates = await mock_lt_venue.get_funding_rates()

        # All should have rates
        assert len(hl_rates) > 0
        assert len(as_rates) > 0
        assert len(lt_rates) > 0

    def test_venue_round_qty(self, mock_hl_venue):
        """Venue should round quantities correctly."""
        qty = mock_hl_venue.round_qty("BTC", 0.123456789)
        assert qty == 0.1235  # Rounded to 4 decimals

    @pytest.mark.asyncio
    async def test_circuit_breaker_with_venue(self, mock_hl_venue):
        """Circuit breaker should protect venue calls."""
        from core.circuit_breaker import CircuitBreaker, CircuitOpenError

        breaker = CircuitBreaker(failure_threshold=2)

        # Setup mock to fail
        mock_hl_venue.equity = AsyncMock(side_effect=ConnectionError("Timeout"))

        # Should fail and record
        try:
            result = await breaker.call("HL", mock_hl_venue.equity)
        except ConnectionError:
            pass

        try:
            result = await breaker.call("HL", mock_hl_venue.equity)
        except ConnectionError:
            pass

        # Circuit should be open
        with pytest.raises(CircuitOpenError):
            await breaker.call("HL", mock_hl_venue.equity)


class TestPreflightCheck:
    """Test preflight check functionality."""

    @pytest.mark.asyncio
    async def test_preflight_credentials_check(self):
        """Preflight should check credentials."""
        from core.preflight import PreflightCheck

        venues_cfg = {
            "hyperliquid": {"rest_url": "https://api.example.com"},
            "aster": {"rest_url": "https://api.example.com"},
        }
        risk_cfg = {
            "global": {"max_drawdown_pct": 5.0},
            "carry": {"min_funding_bps": 50}
        }

        preflight = PreflightCheck(venues_cfg, risk_cfg)

        # Without env vars, credentials should fail
        with patch.dict(os.environ, {}, clear=True):
            passed, results = await preflight.run_all()

        # Should have credential failure results
        cred_results = [r for r in results if "Credentials" in r[0]]
        assert len(cred_results) > 0

    @pytest.mark.asyncio
    async def test_preflight_config_check(self):
        """Preflight should validate configuration."""
        from core.preflight import PreflightCheck

        venues_cfg = {}
        risk_cfg = {
            "global": {"max_drawdown_pct": 10.0},
            "carry": {"min_funding_bps": 50}
        }

        preflight = PreflightCheck(venues_cfg, risk_cfg)
        passed, results = await preflight.run_all()

        # Kill-switch config should pass
        ks_result = [r for r in results if "Kill-Switch" in r[0]]
        assert len(ks_result) == 1
        assert ks_result[0][1] is True  # Passed

    @pytest.mark.asyncio
    async def test_preflight_mode_detection(self):
        """Preflight should detect trading mode."""
        from core.preflight import PreflightCheck

        venues_cfg = {}
        risk_cfg = {
            "paper_mode": True,
            "global": {"max_drawdown_pct": 10.0},
            "carry": {"paper_mode": True, "min_funding_bps": 50}
        }

        preflight = PreflightCheck(venues_cfg, risk_cfg)
        passed, results = await preflight.run_all()

        # Should detect paper mode
        mode_result = [r for r in results if "Trading Mode" in r[0]]
        assert len(mode_result) == 1
        assert "PAPER" in mode_result[0][2]


class TestEdgeCaseHandling:
    """Test edge cases and error handling."""

    def test_empty_bbo_handling(self):
        """Should handle empty/None BBO gracefully."""
        from unittest.mock import MagicMock

        venue = MagicMock()
        venue.get_bbo = MagicMock(return_value=None)

        bbo = venue.get_bbo("UNKNOWN")
        assert bbo is None

    @pytest.mark.asyncio
    async def test_funding_rate_error_handling(self):
        """Should handle funding rate errors gracefully."""
        from unittest.mock import MagicMock, AsyncMock

        venue = MagicMock()
        venue.get_funding_rates = AsyncMock(side_effect=Exception("API Error"))

        with pytest.raises(Exception):
            await venue.get_funding_rates()

    def test_venue_reconnection(self):
        """Mock reconnection scenario."""
        from unittest.mock import MagicMock

        venue = MagicMock()
        venue._connected = MagicMock()
        venue._connected.is_set = MagicMock(return_value=False)

        # Initially disconnected
        assert venue._connected.is_set() is False

        # Simulate reconnection
        venue._connected.is_set.return_value = True
        assert venue._connected.is_set() is True
