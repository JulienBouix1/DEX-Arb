# -*- coding: utf-8 -*-
"""
Circuit Breaker for API Failures.

Monitors API call failures per venue and triggers protective halt when
consecutive failures exceed threshold. Prevents the bot from continuing
to trade when a venue is experiencing issues.

States:
- CLOSED: Normal operation, requests pass through
- OPEN: Circuit tripped, requests are blocked
- HALF_OPEN: Testing if service recovered (after cooldown)
"""
import asyncio
import inspect
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, Optional, Any
from collections import defaultdict

log = logging.getLogger("circuit_breaker")


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal - requests pass through
    OPEN = "open"          # Tripped - requests blocked
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class VenueCircuit:
    """Circuit breaker state for a single venue."""
    venue: str
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0.0
    last_success_time: float = 0.0
    opened_at: float = 0.0

    # Configurable thresholds
    failure_threshold: int = 5  # Consecutive failures to trip
    success_threshold: int = 2  # Successes in half-open to close
    cooldown_seconds: float = 60.0  # Time before half-open test


class CircuitBreaker:
    """
    Circuit breaker manager for all venues.

    Usage:
        breaker = CircuitBreaker(
            failure_threshold=5,
            cooldown_seconds=60,
            on_circuit_open=my_halt_callback
        )

        # Wrap API calls
        try:
            result = await breaker.call("HL", venue.place_order, *args)
        except CircuitOpenError:
            # Circuit is open, skip this venue
            pass

        # Or manually record outcomes
        breaker.record_success("HL")
        breaker.record_failure("HL", "Connection timeout")
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        cooldown_seconds: float = 60.0,
        on_circuit_open: Optional[Callable[[str], None]] = None,
        on_circuit_close: Optional[Callable[[str], None]] = None,
    ):
        """
        Initialize circuit breaker.

        Args:
            failure_threshold: Consecutive failures before opening circuit
            success_threshold: Successes in half-open state before closing
            cooldown_seconds: Time to wait before testing recovery
            on_circuit_open: Callback when circuit opens (receives venue name)
            on_circuit_close: Callback when circuit closes (receives venue name)
        """
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.cooldown_seconds = cooldown_seconds
        self.on_circuit_open = on_circuit_open
        self.on_circuit_close = on_circuit_close

        self._circuits: Dict[str, VenueCircuit] = {}
        self._lock = asyncio.Lock()

    def _get_circuit(self, venue: str) -> VenueCircuit:
        """Get or create circuit for a venue."""
        if venue not in self._circuits:
            self._circuits[venue] = VenueCircuit(
                venue=venue,
                failure_threshold=self.failure_threshold,
                success_threshold=self.success_threshold,
                cooldown_seconds=self.cooldown_seconds,
            )
        return self._circuits[venue]

    def get_state(self, venue: str) -> CircuitState:
        """Get current circuit state for a venue."""
        circuit = self._get_circuit(venue)
        self._check_half_open(circuit)
        return circuit.state

    def is_available(self, venue: str) -> bool:
        """Check if venue is available (circuit not open)."""
        state = self.get_state(venue)
        return state != CircuitState.OPEN

    def _check_half_open(self, circuit: VenueCircuit) -> None:
        """Check if circuit should transition to half-open."""
        if circuit.state == CircuitState.OPEN:
            elapsed = time.time() - circuit.opened_at
            if elapsed >= circuit.cooldown_seconds:
                log.info(f"[CIRCUIT] {circuit.venue}: OPEN -> HALF_OPEN (cooldown elapsed)")
                circuit.state = CircuitState.HALF_OPEN
                circuit.success_count = 0

    def record_success(self, venue: str) -> None:
        """Record a successful API call."""
        circuit = self._get_circuit(venue)
        circuit.last_success_time = time.time()

        if circuit.state == CircuitState.CLOSED:
            # Reset failure count on success
            circuit.failure_count = 0

        elif circuit.state == CircuitState.HALF_OPEN:
            circuit.success_count += 1
            log.debug(f"[CIRCUIT] {venue}: half-open success {circuit.success_count}/{circuit.success_threshold}")

            if circuit.success_count >= circuit.success_threshold:
                log.info(f"[CIRCUIT] {venue}: HALF_OPEN -> CLOSED (service recovered)")
                circuit.state = CircuitState.CLOSED
                circuit.failure_count = 0
                circuit.success_count = 0

                if self.on_circuit_close:
                    try:
                        self.on_circuit_close(venue)
                    except Exception as e:
                        log.error(f"[CIRCUIT] on_circuit_close callback error: {e}")

    def record_failure(self, venue: str, error: str = "") -> None:
        """
        Record a failed API call.

        Args:
            venue: Venue identifier (e.g., "HL", "ASTER", "LIGHTER")
            error: Error message for logging
        """
        circuit = self._get_circuit(venue)
        circuit.last_failure_time = time.time()
        circuit.failure_count += 1

        log.warning(f"[CIRCUIT] {venue}: failure #{circuit.failure_count} - {error}")

        if circuit.state == CircuitState.HALF_OPEN:
            # Any failure in half-open reopens the circuit
            log.warning(f"[CIRCUIT] {venue}: HALF_OPEN -> OPEN (failure during test)")
            circuit.state = CircuitState.OPEN
            circuit.opened_at = time.time()
            circuit.success_count = 0

        elif circuit.state == CircuitState.CLOSED:
            if circuit.failure_count >= circuit.failure_threshold:
                log.error(f"[CIRCUIT] {venue}: CLOSED -> OPEN (threshold {circuit.failure_threshold} exceeded)")
                circuit.state = CircuitState.OPEN
                circuit.opened_at = time.time()

                if self.on_circuit_open:
                    try:
                        self.on_circuit_open(venue)
                    except Exception as e:
                        log.error(f"[CIRCUIT] on_circuit_open callback error: {e}")

    async def call(
        self,
        venue: str,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        """
        Execute a function with circuit breaker protection.

        Args:
            venue: Venue identifier
            func: Async function to call
            *args, **kwargs: Arguments to pass to function

        Returns:
            Result of the function call

        Raises:
            CircuitOpenError: If circuit is open
            Any exception from the wrapped function
        """
        state = self.get_state(venue)

        if state == CircuitState.OPEN:
            raise CircuitOpenError(f"Circuit open for {venue}")

        try:
            if inspect.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            self.record_success(venue)
            return result

        except Exception as e:
            self.record_failure(venue, str(e))
            raise

    def reset(self, venue: str) -> None:
        """Manually reset a circuit to closed state."""
        circuit = self._get_circuit(venue)
        log.info(f"[CIRCUIT] {venue}: Manual reset to CLOSED")
        circuit.state = CircuitState.CLOSED
        circuit.failure_count = 0
        circuit.success_count = 0

    def reset_all(self) -> None:
        """Reset all circuits to closed state."""
        for venue in self._circuits:
            self.reset(venue)

    def get_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all circuits."""
        status = {}
        for venue, circuit in self._circuits.items():
            self._check_half_open(circuit)
            status[venue] = {
                "state": circuit.state.value,
                "failure_count": circuit.failure_count,
                "success_count": circuit.success_count,
                "last_failure": circuit.last_failure_time,
                "last_success": circuit.last_success_time,
            }
        return status

    def any_open(self) -> bool:
        """Check if any circuit is currently open."""
        for venue in self._circuits:
            if self.get_state(venue) == CircuitState.OPEN:
                return True
        return False

    def all_open(self) -> bool:
        """Check if all tracked circuits are open (critical state)."""
        if not self._circuits:
            return False
        return all(
            self.get_state(venue) == CircuitState.OPEN
            for venue in self._circuits
        )


class CircuitOpenError(Exception):
    """Raised when attempting to use a venue with an open circuit."""
    pass


# Global circuit breaker instance (optional singleton pattern)
_global_breaker: Optional[CircuitBreaker] = None


def get_circuit_breaker(
    failure_threshold: int = 5,
    cooldown_seconds: float = 60.0,
    **kwargs
) -> CircuitBreaker:
    """
    Get or create the global circuit breaker instance.

    First call creates the instance with the provided config.
    Subsequent calls return the existing instance.
    """
    global _global_breaker
    if _global_breaker is None:
        _global_breaker = CircuitBreaker(
            failure_threshold=failure_threshold,
            cooldown_seconds=cooldown_seconds,
            **kwargs
        )
    return _global_breaker
