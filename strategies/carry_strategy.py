# -*- coding: utf-8 -*-
"""
Carry Strategy: Funding Rate Arbitrage between HL, Aster, and Lighter.

Unlike Scalp (entry + instant exit), Carry:
- Opens delta-neutral positions when funding is favorable
- Holds for 8h+ to collect funding payments
- Exits when funding becomes unfavorable or position ages out

=============================================================================
FILE STRUCTURE (2600+ lines)
=============================================================================

SECTION 1: DATA CLASSES & INITIALIZATION (Lines ~25-260)
    - CarryPosition dataclass
    - CarryStrategy.__init__
    - Configuration loading

SECTION 2: STATE MANAGEMENT (Lines ~260-450)
    - total_paper_equity, _load_state, _save_state
    - Entry lock methods (is_entry_in_progress, _register_entry_start, etc.)
    - Logging methods (_log_opp_audit, _log_mtm, _log)
    - Blocklist methods (_track_reversal, _is_blocklisted)

SECTION 3: MTM & CALCULATIONS (Lines ~450-650)
    - _get_total_equity - Fetch equity from venues
    - _calc_mtm - Mark-to-market calculation
    - _slip_bps - Slippage estimation

SECTION 4: MAIN LOOP (Lines ~650-850)
    - tick() - Main strategy loop
    - _reconcile_state_with_venues - Periodic state sync

SECTION 5: POSITION MANAGEMENT (Lines ~850-1650)
    - _manage_positions - MTM checks, exit decisions
    - _yield_rebalance - Optimize yield allocation
    - _trigger_scale_up - Add to winning positions
    - _execute_partial_exit - Reduce position size
    - _execute_exit - Full position exit
    - _remove_position - Clean up closed position
    - close_all_positions - Emergency close all
    - _check_orphans - Detect unhedged positions

SECTION 6: ENTRY LOGIC (Lines ~1650-2370)
    - _check_entries - Scan for opportunities
    - _ensure_leverage_setup - Set venue leverage
    - _execute_smart_entry - Execute dual-leg entry
    - _cleanup_positions - Handle failed entries
    - _verify_positions_on_venues_sync - Verify fills

SECTION 7: UTILITIES (Lines ~2370-end)
    - _calc_pnl - P&L calculation
    - get_positions_summary - Dashboard data
    - get_stats - Strategy statistics
    - get_blocked_pairs - Blocklisted pairs

=============================================================================
"""
from __future__ import annotations

import asyncio
import csv
import logging
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)


class VenueHealthMonitor:
    """Track venue API health to prevent entries during degraded conditions."""

    def __init__(self, window_sec: float = 60.0, max_errors: int = 5):
        self._errors: Dict[str, list] = {}  # venue -> [(timestamp, error)]
        self._window = window_sec
        self._max_errors = max_errors

    def record_error(self, venue: str, error: str = ""):
        """Record an API error for a venue."""
        if venue not in self._errors:
            self._errors[venue] = []
        self._errors[venue].append((time.time(), error))
        # Cleanup old errors
        self._cleanup(venue)

    def _cleanup(self, venue: str):
        """Remove errors older than window."""
        cutoff = time.time() - self._window
        self._errors[venue] = [(t, e) for t, e in self._errors.get(venue, []) if t > cutoff]

    def is_healthy(self, venue: str) -> bool:
        """Check if venue is healthy (< max_errors in window)."""
        self._cleanup(venue)
        return len(self._errors.get(venue, [])) < self._max_errors

    def get_error_count(self, venue: str) -> int:
        """Get current error count for venue."""
        self._cleanup(venue)
        return len(self._errors.get(venue, []))


@dataclass
class CarryPosition:
    """Tracks a paper carry position."""
    symbol: str
    hl_coin: str
    direction: str  # "Long HL / Short AS" or "Short HL / Long AS"
    size_usd: float
    entry_time: float
    entry_funding_hl: float
    entry_funding_as: float
    entry_diff_bps: float = 0.0  # Funding diff at entry in bps (net yield, always positive)
    entry_signed_diff_bps: float = 0.0  # Raw signed diff (r1-r2)*10000 for reversal detection
    entry_px_hl: float = 0.0     # Entry price on HL (for MTM)
    entry_px_as: float = 0.0     # Entry price on Aster (for MTM)
    funding_collected: float = 0.0
    realized_funding: float = 0.0 # Phase 2: True Realized Funding PnL from venue fills
    accrued_funding_usd: float = 0.0 # Cumulative funding earned in paper mode
    last_accrual_time: float = 0.0   # Last time we "locked in" funding profit
    max_mtm_bps: float = -999.0      # Highest MTM (Price+Funding) reached so far (trailing stop)
    scale_up_count: int = 0          # Number of scale-ups performed (max 3)
    # Spike detection fields
    mtm_history: list = None         # Rolling MTM history for spike detection [(ts, mtm_bps)]
    spike_protection_until: float = 0.0  # Timestamp until spike protection active
    # PHASE 4: Maker order tracking for fee calculation
    entry_v1_maker: bool = False     # True if V1 (HL) entry was maker fill
    entry_v2_maker: bool = False     # True if V2 (AS/LT) entry was maker fill
    entry_fee_bps: float = 0.0       # Actual entry fees paid (calculated from fill types)
    # PHASE 2026-01-22: Yield momentum tracking for smarter rebalancing
    _prev_yield_bps: float = None    # Previous tick's yield (for momentum calculation)
    # PHASE 2026-01-22: Per-leg size tracking to detect and prevent hedging mismatches
    size_v1_usd: float = 0.0         # Actual USD value of V1 (leg1) position
    size_v2_usd: float = 0.0         # Actual USD value of V2 (leg2) position
    # FIX 2026-01-22: Comprehensive fee tracking across entry + scale-ups
    # Total fees accumulated in USD (not bps, since position size changes with scale-ups)
    total_fees_paid_usd: float = 0.0
    # Fee operation history: [{"op": "entry/scale_up/exit", "size_usd": float, "fee_bps": float,
    #                         "fee_usd": float, "v1_maker": bool, "v2_maker": bool, "ts": float}]
    fee_operations: list = None
    # P0-2 FIX 2026-01-23: Multi-entry tracking for accurate weighted average prices
    # Each entry: {"ts": float, "px_v1": float, "px_v2": float, "size_usd": float, "is_scale_up": bool}
    entries: list = None

    def get_weighted_avg_prices(self) -> Tuple[float, float]:
        """
        P0-2: Compute weighted average entry prices from all entries.
        Returns (weighted_px_v1, weighted_px_v2).
        Falls back to entry_px_hl/entry_px_as if no entries list.
        """
        if not self.entries or len(self.entries) == 0:
            return (self.entry_px_hl, self.entry_px_as)

        total_size = 0.0
        weighted_v1 = 0.0
        weighted_v2 = 0.0

        for entry in self.entries:
            size = entry.get("size_usd", 0.0)
            if size <= 0:
                continue
            px_v1 = entry.get("px_v1", 0.0)
            px_v2 = entry.get("px_v2", 0.0)
            weighted_v1 += px_v1 * size
            weighted_v2 += px_v2 * size
            total_size += size

        if total_size <= 0:
            return (self.entry_px_hl, self.entry_px_as)

        return (weighted_v1 / total_size, weighted_v2 / total_size)


class CarryStrategy:
    """
    Funding Rate Arbitrage Strategy.
    
    Entry: When funding rate differential is favorable (e.g., HL > AS by 2+ bps/8h)
    Hold: Collect funding every 8h
    Exit: When funding reverses or max hold time reached
    """

    def __init__(
        self,
        *,
        hl,
        asr,
        cfg: Dict[str, Any],
        pairs_map: Dict[str, str],  # AS symbol -> HL coin
        risk: Dict[str, Any],
        notifier,
        lighter: Any = None,
        lighter_map: Dict[str, str] = None,
        as_lt_map: Dict[str, str] = None,  # Direct AS->LT mapping for pairs not on HL
        blocked_bases: set = None,  # FIX 2026-01-24: Blocked bases (XAU, XAG, etc.)
    ) -> None:
        self.hl = hl
        self.asr = asr
        self.lighter = lighter
        # Map: HL Coin -> Lighter Symbol
        self.hl_to_lighter = {v: k for k, v in (lighter_map or {}).items()}
        # Direct AS -> LT mapping for pairs NOT on HL (stocks, commodities, etc.)
        self.as_lt_map = as_lt_map or {}
        # FIX 2026-01-24: Blocked bases (XAU, XAG, etc.) - block before entry
        self._blocked_bases = blocked_bases or set()
        self.cfg = cfg
        self.pairs_map = pairs_map
        self.risk = risk
        self.notifier = notifier
        # Paper mode from config
        self.paper_mode = bool(risk.get("carry", {}).get("paper_mode", True))
        # Dry-run live mode: uses real equity, calculates everything, but doesn't place orders
        # Useful for validating the full flow before going live
        self.dry_run_live = bool(risk.get("carry", {}).get("dry_run_live", False))

        # Persistence Storage
        from strategies.carry_storage import CarryStorage
        self.storage = CarryStorage()
        self.positions: Dict[str, CarryPosition] = {}
        
        # State
        self.enabled = True
        self.min_volume = float(risk.get("min_volume_24h_usd", 0))
        # Read configured min bps (default 3.0), don't hardcode
        self.min_funding_bps = float(risk.get("carry", {}).get("min_funding_bps", 3.0))
        # FIX 2026-01-24: Low-yield profitable exit threshold
        self._low_yield_exit_bps = float(risk.get("carry", {}).get("low_yield_exit_bps", 50.0))
        self.alloc_pct = float(risk.get("carry", {}).get("carry_alloc_pct", 0.10))
        # Max total allocation for carry strategy (80% of total equity)
        self.max_carry_alloc_pct = float(risk.get("carry", {}).get("max_carry_total_alloc_pct", 0.50))
        # Keep max_position_usd as a safety cap if needed, but logic moves to %
        self.max_position_usd = float(risk.get("carry", {}).get("max_position_usd", 1000.0))
        self.hold_min_hours = float(risk.get("carry", {}).get("hold_min_hours", 4.0))
        self.hold_max_hours = float(risk.get("carry", {}).get("max_hold_hours", 24.0))
        
        self.required_confirms = int(risk.get("carry", {}).get("required_confirms", 3))
        self._confirm_count: Dict[str, int] = {}  # symbol -> count
        self._pending_exits: Dict[str, Tuple[str, float]] = {}  # sym -> (reason, first_deferred_ts)
        self._pending_exit_force_timeout = 300.0  # Force exit after 5 min of stale data
        self.slip_buffer = float(risk.get("carry", {}).get("slippage_bps_buffer", 2.0))
        self.opt_buffer = float(risk.get("carry", {}).get("optimization_buffer_bps", 10.0))
        self.rebalance_min_profit_bps = float(risk.get("carry", {}).get("rebalance_min_profit_bps", 10.0))  # Min profit to allow rebalancing
        
        # Scaling (Renforcement)
        self.scale_up_slice_pct = float(risk.get("carry", {}).get("scale_up_slice_pct", 0.10))
        self.max_symbol_alloc_pct = float(risk.get("carry", {}).get("max_symbol_alloc_pct", 0.30))
        self.max_scale_ups_per_position = int(risk.get("carry", {}).get("max_scale_ups_per_position", 2))
        self.max_pairs_with_scaleup = int(risk.get("carry", {}).get("max_pairs_with_scaleup", 2))
        
        # MTM Thresholds (from config or tight defaults)
        self.mtm_stop_bps = -abs(float(risk.get("carry", {}).get("mtm_stop_bps", 35.0)))
        self.mtm_tp_bps = abs(float(risk.get("carry", {}).get("mtm_tp_bps", 25.0)))
        # BUGFIX: Increased default grace period from 15 to 30 minutes
        # This gives positions more time to recover from initial price volatility
        # before being stopped out. Combined with the 10-min forced exit cooldown,
        # this should prevent rapid churn cycles.
        self.mtm_grace_minutes = float(risk.get("carry", {}).get("mtm_grace_minutes", 30.0))
        self.mtm_yield_multiplier = float(risk.get("carry", {}).get("mtm_yield_multiplier", 2.0))
        self.mtm_multiplier_window_hours = float(risk.get("carry", {}).get("mtm_multiplier_window_hours", 1.0))  # Multiplier only applies for first N hours
        self.mtm_check_interval = float(risk.get("carry", {}).get("mtm_check_interval_s", 60.0))

        # Tiered Take-Profit System
        self.high_yield_threshold_bps = float(risk.get("carry", {}).get("high_yield_threshold_bps", 100.0))
        self.high_yield_tp_pct_initial = float(risk.get("carry", {}).get("high_yield_tp_pct_initial", 0.02))
        self.high_yield_tp_pct_after_decay = float(risk.get("carry", {}).get("high_yield_tp_pct_after_4h", 0.01))
        self.high_yield_tp_decay_hours = float(risk.get("carry", {}).get("high_yield_tp_decay_hours", 4.0))

        # Load venue fees from venues.yaml for accurate MTM calculation
        # PHASE 3: Support both maker and taker fees for maker order mode
        venues = cfg.get("venues", {})
        self._venue_fees = {
            "HL": {
                "taker": float(venues.get("hyperliquid", {}).get("fees", {}).get("taker_bps", 4.0)),
                "maker": float(venues.get("hyperliquid", {}).get("fees", {}).get("maker_bps", 1.0)),
            },
            "AS": {
                "taker": float(venues.get("aster", {}).get("fees", {}).get("taker_bps", 3.5)),
                "maker": float(venues.get("aster", {}).get("fees", {}).get("maker_bps", 1.0)),
            },
            "LT": {
                "taker": float(venues.get("lighter", {}).get("fees", {}).get("taker_bps", 2.0)),
                "maker": float(venues.get("lighter", {}).get("fees", {}).get("maker_bps", 0.2)),
            },
        }
        log.info(f"[CARRY] Loaded venue fees: HL={self._venue_fees['HL']}bps, AS={self._venue_fees['AS']}bps, LT={self._venue_fees['LT']}bps")

        # Pre-calculate round-trip fees per venue pair (entry + exit = 4 trades)
        # FIX 2026-01-21: Replace hardcoded EST_FEES_BPS=15.0 with dynamic calculation
        # PHASE 3: Calculate both taker-only and maker round-trip fees
        self._roundtrip_fees_taker = {}
        self._roundtrip_fees_maker = {}
        for v1 in ["HL", "AS", "LT"]:
            for v2 in ["HL", "AS", "LT"]:
                if v1 != v2:
                    # Taker round-trip = 2 * (taker_v1 + taker_v2)
                    rt_taker = 2 * (self._venue_fees[v1]["taker"] + self._venue_fees[v2]["taker"])
                    self._roundtrip_fees_taker[f"{v1}_{v2}"] = rt_taker
                    self._roundtrip_fees_taker[f"{v2}_{v1}"] = rt_taker

                    # Maker round-trip = 2 * (maker_v1 + maker_v2)
                    rt_maker = 2 * (self._venue_fees[v1]["maker"] + self._venue_fees[v2]["maker"])
                    self._roundtrip_fees_maker[f"{v1}_{v2}"] = rt_maker
                    self._roundtrip_fees_maker[f"{v2}_{v1}"] = rt_maker

        # Backward compatibility: _roundtrip_fees defaults to taker (conservative)
        self._roundtrip_fees = self._roundtrip_fees_taker

        log.info(f"[CARRY] Round-trip fees (taker): HL-AS={self._roundtrip_fees_taker.get('HL_AS', 17):.1f}bps, "
                f"HL-LT={self._roundtrip_fees_taker.get('HL_LT', 13):.1f}bps, AS-LT={self._roundtrip_fees_taker.get('AS_LT', 12):.1f}bps")
        log.info(f"[CARRY] Round-trip fees (maker): HL-AS={self._roundtrip_fees_maker.get('HL_AS', 4):.1f}bps, "
                f"HL-LT={self._roundtrip_fees_maker.get('HL_LT', 2.4):.1f}bps, AS-LT={self._roundtrip_fees_maker.get('AS_LT', 2.4):.1f}bps")

        # Early Reversal Detection
        # BUGFIX: Increased default from 5 to 10 minutes to reduce churn
        # Early reversal exits are now more conservative, allowing positions time to settle
        self.early_reversal_min_minutes = float(risk.get("carry", {}).get("early_reversal_min_minutes", 10.0))

        # Trailing Stop Parameters
        self.mtm_trailing_bps = float(risk.get("carry", {}).get("mtm_trailing_bps", 150.0))
        self.mtm_trailing_min = float(risk.get("carry", {}).get("mtm_trailing_min_bps", 100.0))

        # FIX 2026-01-22: Yield-Tiered Dynamic Stops
        # Load tiered stop config with sensible defaults
        stop_tiers = risk.get("carry", {}).get("stop_loss_tiers", {})
        self._stop_tiers = {
            "high": {
                "threshold_bps": float(stop_tiers.get("high", {}).get("threshold_bps", 100.0)),
                "stop_bps": float(stop_tiers.get("high", {}).get("stop_bps", 300.0)),
                "trailing_bps": float(stop_tiers.get("high", {}).get("trailing_bps", 150.0)),
            },
            "medium": {
                "threshold_bps": float(stop_tiers.get("medium", {}).get("threshold_bps", 60.0)),
                "stop_bps": float(stop_tiers.get("medium", {}).get("stop_bps", 200.0)),
                "trailing_bps": float(stop_tiers.get("medium", {}).get("trailing_bps", 100.0)),
            },
            "low": {
                "threshold_bps": float(stop_tiers.get("low", {}).get("threshold_bps", 0.0)),
                "stop_bps": float(stop_tiers.get("low", {}).get("stop_bps", 150.0)),
                "trailing_bps": float(stop_tiers.get("low", {}).get("trailing_bps", 75.0)),
            },
        }
        log.info(f"[CARRY] Yield-tiered stops loaded: high={self._stop_tiers['high']['stop_bps']}bps, "
                f"medium={self._stop_tiers['medium']['stop_bps']}bps, low={self._stop_tiers['low']['stop_bps']}bps")

        # Leverage Settings (CRITICAL for live trading)
        self.leverage = int(risk.get("carry", {}).get("leverage", 3))
        self._leverage_setup_done: Dict[str, bool] = {}  # Track which symbols have had leverage set

        # Entry Lock: Prevents Position Manager from closing "orphan" positions during multi-leg entry
        # Maps hl_coin -> timestamp when entry started
        self._entry_in_progress: Dict[str, float] = {}
        # P2 FIX 2026-01-22: Increased from 30s to 60s to reduce edge cases with network delays
        self._entry_lock_timeout = 60.0  # Grace period in seconds

        # FIX 2026-01-22: Order Tracking - Track all open orders to prevent orphans
        # Structure: {order_id: {"symbol": str, "venue": venue_obj, "venue_name": str, "side": str, "ts": float}}
        self._active_orders: Dict[str, Dict[str, Any]] = {}

        # FIX 2026-01-25: Recently-Closed Position Tracking - Prevents false orphan alerts
        # After closing a position, venue API may still show dust for up to 60s due to API lag
        # Structure: {hl_coin: close_timestamp}
        self._recently_closed: Dict[str, float] = {}
        self._recently_closed_grace_s = 60.0  # Grace period in seconds

        # Liquidity-Aware Entry (Dynamic Threshold)
        self.min_vol_premium = float(risk.get("carry", {}).get("min_volume_premium_usd", 500000.0))
        self.premium_funding_bps = float(risk.get("carry", {}).get("premium_funding_bps", 120.0))

        self.last_run = 0
        # Funding check interval can be configured (default 30s when scalp disabled, 60s otherwise)
        self._funding_check_interval = float(risk.get("carry", {}).get("funding_check_interval_s", 30.0))
        self._last_funding_check = 0
        self._last_mtm_check = 0
        self._last_heartbeat = 0
        self._last_reconciliation = 0  # For periodic state sync with venues
        # FIX 2026-01-22: Reduced from 300s to 60s for faster orphan detection
        # Lighter fills can appear after wait_for_fill timeout, causing orphaned positions
        # FIX 2026-01-22: Reduced from 60s to 30s to detect state mismatches faster
        self._reconciliation_interval = 30.0  # Every 30 seconds
        # FIX 2026-01-24: Track consecutive "missing" checks before removing positions
        # This prevents removing positions due to single API failures (caused 0G duplicate entry bug)
        self._reconcile_missing_count: Dict[str, int] = {}  # symbol -> consecutive missing count
        self._reconcile_min_missing_before_remove = 3  # Require 3 consecutive "missing" checks (~90s)

        # Hourly performance tracking
        self._hourly_stats = {
            "entries": 0,
            "exits": 0,
            "wins": 0,
            "losses": 0,
            "pnl_usd": 0.0,
            "funding_pnl_usd": 0.0,
            "price_pnl_usd": 0.0
        }

        # Position Health Logging (every 5 minutes)
        self._last_health_log = 0.0
        self._health_log_interval = 300.0  # 5 minutes
        self._last_mtm_by_symbol: Dict[str, float] = {}  # For MTM swing detection

        # FIX 2026-01-22: Open order cleanup (every 60 seconds)
        self._last_order_cleanup = 0.0
        self._order_cleanup_interval = 60.0  # Clean up stale orders every 60s

        # P0 FIX 2026-01-22: Stale MTM tracking for emergency exits
        # If MTM returns stale data for too long, we must force exit to prevent silent losses
        self._stale_mtm_counter: Dict[str, int] = {}  # hl_coin -> consecutive stale count
        self._stale_mtm_emergency_threshold = 6  # 6 ticks @ 5s = 30s before emergency exit
        self._stale_mtm_warning_threshold = 3   # Log warning after 3 consecutive stale readings

        # FIX 2026-01-25: Orphan delta tracking - track unhedged exposure from partial fills
        # Maps symbol -> unhedged notional USD (positive = net long, negative = net short)
        self._orphan_deltas: Dict[str, float] = {}

        # FIX 2026-01-25: Stress mode detection - reduce position sizes during market stress
        self._stress_mode_active = False
        self._last_stress_check = 0.0
        self._stress_check_interval = 60.0  # Check every 60 seconds

        # P1 FIX 2026-01-22: State file locking to prevent concurrent write corruption
        # Using threading.Lock since _save_state is synchronous
        import threading
        self._state_lock = threading.Lock()

        # Auto-blocklist: Track reversals per hl_coin to detect problematic pairs
        # Load from storage to persist across restarts
        self._reversal_tracker: Dict[str, List[float]] = self.storage.load_reversal_tracker()
        self._reversal_blocklist: Dict[str, float] = self.storage.load_blocklist()
        self._reversal_threshold = int(risk.get("carry", {}).get("reversal_blocklist_threshold", 2))  # N reversals (default 2)
        self._reversal_window_hours = float(risk.get("carry", {}).get("reversal_window_hours", 2.0))  # in X hours
        self._reversal_blocklist_hours = float(risk.get("carry", {}).get("reversal_blocklist_hours", 48.0))  # block for Y hours

        # Log restored blocklist if any
        if self._reversal_blocklist:
            log.info(f"[CARRY] Restored blocklist: {list(self._reversal_blocklist.keys())} ({len(self._reversal_blocklist)} pairs)")

        # BUGFIX: Symbol PnL Tracking - auto-blocklist symbols with cumulative losses
        # This prevents the bot from repeatedly trading symbols that consistently lose money
        self._symbol_pnl_tracker: Dict[str, float] = {}  # hl_coin -> cumulative PnL
        self._pnl_loss_blocklist_threshold = float(risk.get("carry", {}).get("pnl_loss_blocklist_threshold_usd", 50.0))  # $50 loss triggers blocklist
        self._pnl_blocklist_hours = float(risk.get("carry", {}).get("pnl_blocklist_hours", 24.0))  # Block for 24 hours

        # Entry Failure Cooldown - prevent rapid retries of failing symbols
        self._entry_failure_cooldown: Dict[str, float] = {}  # hl_coin -> cooldown_until timestamp
        self._entry_failure_cooldown_minutes = float(risk.get("carry", {}).get("entry_failure_cooldown_minutes", 5.0))

        # BUGFIX: Forced Exit Cooldown - prevent immediate re-entry after MTM stop/trailing stop
        # This fixes the 7-second re-entry pattern seen in BERAUSDT after MTM stop
        # FIX 2026-01-24: Track direction - allow inverted re-entry (opposite direction)
        self._forced_exit_cooldown: Dict[str, float] = {}  # hl_coin -> cooldown_until timestamp
        self._forced_exit_cooldown_direction: Dict[str, str] = {}  # hl_coin -> exited direction (e.g., "Short AS / Long LT")
        self._forced_exit_cooldown_minutes = float(risk.get("carry", {}).get("forced_exit_cooldown_minutes", 10.0))

        # BUGFIX: Scale-Up Cooldown - prevent rapid scale-ups that spam orders
        # Minimum 5 minutes between scale-ups for the same symbol
        self._last_scale_up_time: Dict[str, float] = {}  # hl_coin -> last scale-up timestamp
        self._scale_up_cooldown_seconds = float(risk.get("carry", {}).get("scale_up_cooldown_seconds", 300.0))  # 5 min default

        # FIX 2026-01-24: Scale-Down Cooldown - prevent scale-up/scale-down churn
        # Analysis showed cycles happening within seconds, wasting fees
        self._last_scale_down_time: Dict[str, float] = {}  # hl_coin -> last scale-down timestamp
        self._scale_down_cooldown_seconds = float(risk.get("carry", {}).get("scale_down_cooldown_seconds", 900.0))  # 15 min default

        # === STOP PROTECTION MECHANISMS ===
        # These protect profitable positions from being stopped out by temporary price spikes

        # 1. Funding Cushion - Use accumulated funding as extra stop buffer
        self._funding_cushion_enabled = bool(risk.get("carry", {}).get("funding_cushion_enabled", True))
        self._funding_cushion_multiplier = float(risk.get("carry", {}).get("funding_cushion_multiplier", 0.5))  # Use 50% of funding as cushion
        self._funding_cushion_cap_bps = float(risk.get("carry", {}).get("funding_cushion_cap_bps", 100.0))  # Max 100bps cushion

        # 2. Time Bonus - Reward longer-held positions with wider stops
        self._time_bonus_enabled = bool(risk.get("carry", {}).get("time_bonus_enabled", True))
        self._time_bonus_start_hours = float(risk.get("carry", {}).get("time_bonus_start_hours", 4.0))  # Start bonus after 4h
        self._time_bonus_per_hour_bps = float(risk.get("carry", {}).get("time_bonus_per_hour_bps", 10.0))  # 10bps per hour
        self._time_bonus_cap_bps = float(risk.get("carry", {}).get("time_bonus_cap_bps", 80.0))  # Max 80bps bonus

        # 3. Spike Detection - Protect against flash crashes
        self._spike_detection_enabled = bool(risk.get("carry", {}).get("spike_detection_enabled", True))
        self._spike_threshold_bps = float(risk.get("carry", {}).get("spike_threshold_bps", 50.0))  # Detect 50+ bps drops
        self._spike_window_s = float(risk.get("carry", {}).get("spike_window_s", 60.0))  # Within 60 seconds
        self._spike_protection_s = float(risk.get("carry", {}).get("spike_protection_s", 300.0))  # Grant 5 min protection
        self._spike_max_protection_bps = float(risk.get("carry", {}).get("spike_max_protection_bps", 100.0))  # Extra 100bps buffer

        # PHASE 4: Maker Order Mode - Post limit orders first to save fees
        # WARNING: Disabled by default - enable only after thorough testing
        self._maker_order_enabled = bool(risk.get("carry", {}).get("maker_order_enabled", False))
        self._maker_entry_timeout_s = float(risk.get("carry", {}).get("maker_entry_timeout_s", 3.0))
        self._maker_exit_timeout_s = float(risk.get("carry", {}).get("maker_exit_timeout_s", 5.0))
        self._maker_price_offset_bps = float(risk.get("carry", {}).get("maker_price_offset_bps", 3.0))  # FIX: Default 3 bps (was 1)
        # FIX 2026-01-22: Configurable repost erosion rate (how much closer to mid each attempt)
        self._maker_repost_erosion_bps = float(risk.get("carry", {}).get("maker_repost_erosion_bps", 1.0))
        # FIX 2026-01-24 Enhancement #5: Switch to IOC immediately after ANY partial fill
        # When enabled, partial fills trigger immediate IOC for remainder instead of reposting
        self._partial_fill_ioc_immediately = bool(risk.get("carry", {}).get("partial_fill_ioc_immediately", False))

        # Paper equity - load from storage if available, else from config (default 5000)
        config_equity = float(risk.get("carry", {}).get("paper_equity", 5000.0))
        self.paper_equity = self.storage.load_paper_equity(default=config_equity)
        if self.paper_equity != config_equity:
            log.info(f"[CARRY] Restored paper_equity: ${self.paper_equity:.2f} (config: ${config_equity:.2f})")

        # Dry-run simulated PnL (separate from paper_equity to avoid contamination)
        self._dry_run_realized_pnl = 0.0

        # Live mode realized PnL tracking (accumulated from exits, persisted to disk)
        self._live_realized_pnl = self.storage.load_live_realized_pnl(default=0.0)
        if self._live_realized_pnl != 0.0:
            log.info(f"[CARRY] Restored live_realized_pnl: ${self._live_realized_pnl:.2f}")

        # PHASE 2 FIX (2026-01-22): Session realized PNL tracking
        # This tracks ONLY closed trade PNL, separate from unrealized MTM
        self._session_realized_pnl = 0.0
        self._session_start_equity = self.paper_equity if self.paper_mode else 0.0
        self._session_start_equity_captured = self.paper_mode  # Paper mode has it immediately

        # Equity Cache to avoid redundant API calls
        self._cached_total_equity = 0.0
        self._last_equity_fetch = 0.0
        self._equity_cache_ttl = 60.0  # 60 seconds (reduced from 5min for more accurate allocations)

        # Rate Caching for API failure resilience
        self._cached_hl_rates: Dict[str, float] = {}
        self._cached_as_rates: Dict[str, float] = {}
        self._cached_lt_rates: Dict[str, float] = {}
        
        # FIX 2026-01-21: Restore wins/losses from storage so dashboard shows correct values after restart
        saved_stats = self.storage.load_stats()
        self.total_entries = saved_stats["total_entries"]
        self.total_exits = saved_stats["total_exits"]
        self.wins = saved_stats["wins"]
        self.losses = saved_stats["losses"]
        if self.wins > 0 or self.losses > 0:
            log.info(f"[CARRY] Restored stats: W={self.wins}/L={self.losses}, Entries={self.total_entries}, Exits={self.total_exits}")

        # Entry Attempts Tracking (for dashboard visibility)
        self._entry_attempts: List[Dict[str, Any]] = []
        self._max_entry_attempts_tracked = 50  # Keep last 50 attempts

        # Logging - Carry logs in dedicated subfolder
        self._csv_path = "logs/carry/carry_audit.csv"
        self._mtm_csv_path = "logs/carry/carry_mtm.csv"
        # OPTIMIZATION 2026-01-24: Decision log for tracking all entry/exit decisions
        self._decision_csv_path = "logs/carry/trade_decision_log.csv"
        self._decision_counter = 0

        try:
            os.makedirs("logs/carry", exist_ok=True)
            if not os.path.exists(self._csv_path):
                with open(self._csv_path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    # FIX 2026-01-22: Added v1_maker, v2_maker, net_profit columns
                    w.writerow([
                        "ts", "symbol", "action", "direction", "size_usd",
                        "v1_rate", "v2_rate", "diff_bps",
                        "hold_hours", "price_pnl", "funding_pnl", "paper", "equity",
                        "v1_maker", "v2_maker", "net_profit"
                    ])
            
            # SESSION SEPARATOR (16 columns to match header)
            with open(self._csv_path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([]) # Blank line
                w.writerow([time.time(), "---", "SESSION_START", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---"])
                
            # Opp Audit CSV
            self._opp_csv_path = "logs/carry/carry_opp_audit.csv"
            if not os.path.exists(self._opp_csv_path):
                with open(self._opp_csv_path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow([
                        "ts", "symbol", "diff_bps", "status", "reason", "details"
                    ])
            
            with open(self._opp_csv_path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([])
                w.writerow([time.time(), "---", "---", "SESSION_START", "---", "---"])
            
            # MTM Tracking CSV - Real-time position performance
            self._mtm_path = "logs/carry/carry_mtm.csv"
            self._mtm_csv_path = "logs/carry/carry_mtm.csv"
            if not os.path.exists(self._mtm_csv_path):
                with open(self._mtm_csv_path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow([
                        "ts", "symbol", "direction", "size_usd", "hold_hours",
                        "entry_px_leg1", "entry_px_leg2", "curr_px_leg1", "curr_px_leg2",
                        "price_pnl_usd", "funding_pnl_usd", "total_mtm_usd", "mtm_bps",
                        "entry_diff_bps", "paper_equity"
                    ])
            
            with open(self._mtm_csv_path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([])
                w.writerow([time.time(), "---", "SESSION_START", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---"])

            # FIX 2026-01-24: Comprehensive Order Tracking CSV
            # Records ALL order movements: entries, exits, rollbacks, cancellations, partials, failures
            # OPTIMIZATION 2026-01-24: Added execution quality columns (slippage, latency, maker flags)
            self._order_csv_path = "logs/carry/carry_orders.csv"
            if not os.path.exists(self._order_csv_path):
                with open(self._order_csv_path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow([
                        "ts", "event", "symbol", "hl_coin", "venue", "side",
                        "qty_requested", "qty_filled", "price", "avg_fill_price",
                        "order_type", "direction", "action", "status",
                        "error", "fees_est_usd", "equity", "details",
                        # Execution quality columns
                        "expected_price", "slippage_bps", "submit_ts", "fill_ts",
                        "latency_ms", "is_maker", "fee_savings_usd"
                    ])

            with open(self._order_csv_path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([])
                # 25 columns total (18 original + 7 execution quality)
                w.writerow([time.time(), "SESSION_START", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---"])

            # OPTIMIZATION 2026-01-24: Trade Decision Log for reconstructing WHY every decision happened
            if not os.path.exists(self._decision_csv_path):
                with open(self._decision_csv_path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow([
                        "ts", "decision_id", "symbol", "hl_coin", "decision_type", "trigger_reason",
                        "v1_venue", "v2_venue", "v1_bbo_bid", "v1_bbo_ask", "v2_bbo_bid", "v2_bbo_ask",
                        "spread_v1_bps", "spread_v2_bps", "depth_v1_usd", "depth_v2_usd",
                        "current_yield_bps", "entry_yield_bps", "mtm_bps",
                        "stop_threshold_bps", "funding_cushion_bps", "time_bonus_bps",
                        "position_size_usd", "equity_usd", "allocation_pct",
                        "outcome", "outcome_reason"
                    ])

            with open(self._decision_csv_path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([])
                # 27 columns
                w.writerow([time.time(), "---", "---", "---", "SESSION_START", "---"] + ["---"] * 21)
        except Exception:
            pass

        # Venue Health Monitoring (P2 Fix)
        # FIX 2026-01-22: Increased frequency - 30s window instead of 60s for faster detection
        self._health_monitor = VenueHealthMonitor(window_sec=30.0, max_errors=3)

        # Peak equity tracking for drawdown-based sizing (P2 Fix)
        self._peak_equity = 0.0

        # Circuit breaker logging throttle
        self._last_circuit_log = 0.0

        # Load persisted positions
        self._load_state()

    # =========================================================================
    # SECTION 2: STATE MANAGEMENT
    # =========================================================================

    @property
    def total_paper_equity(self) -> float:
        """Closed equity + Unrealized MTM of all open positions."""
        total = self.paper_equity
        # FIX 2026-01-22: Use list() to prevent RuntimeError if positions modified during iteration
        for sym, pos in list(self.positions.items()):
            mtm = self._calc_mtm(pos)
            total += mtm["total_mtm_usd"]
        return total

    @property
    def unrealized_mtm(self) -> float:
        """PHASE 2: Unrealized MTM of all open positions (separate from realized PNL)."""
        total = 0.0
        # FIX 2026-01-22: Use list() to prevent RuntimeError if positions modified during iteration
        for sym, pos in list(self.positions.items()):
            mtm = self._calc_mtm(pos)
            total += mtm["total_mtm_usd"]
        return total

    @property
    def session_realized_pnl(self) -> float:
        """PHASE 2: Realized PNL from closed trades this session only."""
        return self._session_realized_pnl

    def _load_state(self):
        """Load positions from disk."""
        raw_data = self.storage.load_positions()
        count = 0
        for sym, data in raw_data.items():
            try:
                # BUGFIX: For existing positions without entry_signed_diff_bps, estimate from direction
                # If direction starts with "Long", entry_signed_diff should have been negative
                # If direction starts with "Short", entry_signed_diff should have been positive
                entry_diff = float(data.get("entry_diff_bps", 0.0))
                entry_signed = float(data.get("entry_signed_diff_bps", 0.0))
                if entry_signed == 0.0 and entry_diff != 0.0:
                    direction = data.get("direction", "")
                    # "Long X / Short Y" means we wanted (fr1 - fr2) < 0, so signed diff was negative
                    # "Short X / Long Y" means we wanted (fr1 - fr2) > 0, so signed diff was positive
                    if direction.lower().startswith("long"):
                        entry_signed = -abs(entry_diff)  # Negative for Long first leg
                    else:
                        entry_signed = abs(entry_diff)   # Positive for Short first leg
                    log.info(f"[CARRY] Migrated {sym} entry_signed_diff_bps to {entry_signed:.1f}bps (from direction)")

                # Reconstruct CarryPosition (handle missing fields for forward compatibility)
                pos = CarryPosition(
                    symbol=data["symbol"],
                    hl_coin=data["hl_coin"],
                    direction=data["direction"],
                    size_usd=float(data["size_usd"]),
                    entry_time=float(data["entry_time"]),
                    entry_funding_hl=float(data["entry_funding_hl"]),
                    entry_funding_as=float(data["entry_funding_as"]),
                    entry_diff_bps=entry_diff,
                    entry_signed_diff_bps=entry_signed,
                    entry_px_hl=float(data.get("entry_px_hl", 0.0)),
                    entry_px_as=float(data.get("entry_px_as", 0.0)),
                    funding_collected=float(data.get("funding_collected", 0.0)),
                    realized_funding=float(data.get("realized_funding", 0.0)),
                    accrued_funding_usd=float(data.get("accrued_funding_usd", 0.0)),
                    last_accrual_time=float(data.get("last_accrual_time", data.get("entry_time", 0.0))),
                    max_mtm_bps=float(data.get("max_mtm_bps", -999.0)),
                    scale_up_count=int(data.get("scale_up_count", 0)),
                    # PHASE 4: Maker order tracking
                    entry_v1_maker=bool(data.get("entry_v1_maker", False)),
                    entry_v2_maker=bool(data.get("entry_v2_maker", False)),
                    entry_fee_bps=float(data.get("entry_fee_bps", 0.0)),
                    # Per-leg size tracking
                    size_v1_usd=float(data.get("size_v1_usd", 0.0)),
                    size_v2_usd=float(data.get("size_v2_usd", 0.0)),
                    # FIX 2026-01-22: Comprehensive fee tracking
                    total_fees_paid_usd=float(data.get("total_fees_paid_usd", 0.0)),
                    fee_operations=data.get("fee_operations", None),
                    # P0-2 FIX 2026-01-23: Multi-entry tracking
                    entries=data.get("entries", None)
                )
                # FIX 2026-01-22: Migrate old positions without per-leg sizes
                # If size_v1_usd or size_v2_usd are 0 but size_usd > 0, use 50/50 split
                if pos.size_usd > 0 and (pos.size_v1_usd <= 0 or pos.size_v2_usd <= 0):
                    pos.size_v1_usd = pos.size_usd / 2
                    pos.size_v2_usd = pos.size_usd / 2
                    log.info(f"[CARRY] Migrated {sym} per-leg sizes: V1=${pos.size_v1_usd:.2f} V2=${pos.size_v2_usd:.2f}")

                # FIX 2026-01-22: Migrate old positions without fee tracking
                # Estimate entry fees based on entry_fee_bps if available, otherwise use taker fees
                if pos.size_usd > 0 and (pos.total_fees_paid_usd <= 0 or pos.total_fees_paid_usd is None):
                    entry_bps = pos.entry_fee_bps if pos.entry_fee_bps > 0 else 8.5  # Assume ~8.5 bps taker
                    pos.total_fees_paid_usd = pos.size_usd * (entry_bps / 10000.0)
                    pos.fee_operations = [{
                        "op": "entry_migrated",
                        "size_usd": pos.size_usd,
                        "fee_bps": entry_bps,
                        "fee_usd": pos.total_fees_paid_usd,
                        "v1_maker": pos.entry_v1_maker,
                        "v2_maker": pos.entry_v2_maker,
                        "ts": pos.entry_time
                    }]
                    log.info(f"[CARRY] Migrated {sym} fee tracking: ${pos.total_fees_paid_usd:.2f} ({entry_bps:.1f}bps)")

                # P0-2 FIX 2026-01-23: Migrate old positions without entries list
                if pos.size_usd > 0 and (pos.entries is None or len(pos.entries) == 0):
                    pos.entries = [{
                        "ts": pos.entry_time,
                        "px_v1": pos.entry_px_hl,
                        "px_v2": pos.entry_px_as,
                        "size_usd": pos.size_usd,
                        "is_scale_up": False
                    }]
                    log.info(f"[CARRY] Migrated {sym} entries: 1 synthetic entry")

                self.positions[sym] = pos
                count += 1
            except Exception as e:
                log.error(f"[CARRY] Failed to load position {sym}: {e}")

        if count > 0:
            log.info(f"[CARRY] Restored {count} positions from storage.")

    def _save_state(self):
        """Save positions, paper_equity, live_realized_pnl, blocklist, and stats to disk.

        P1 FIX 2026-01-22: Added lock to prevent concurrent write corruption.
        """
        # P1 FIX 2026-01-22: Use lock to prevent concurrent file writes
        with self._state_lock:
            # FIX 2026-01-21: Include wins/losses so dashboard shows correct values after restart
            if self.paper_mode:
                self.storage.save_state(
                    self.positions,
                    self.paper_equity,
                    blocklist=self._reversal_blocklist,
                    reversal_tracker=self._reversal_tracker,
                    live_realized_pnl=None,
                    wins=self.wins,
                    losses=self.losses,
                    total_entries=self.total_entries,
                    total_exits=self.total_exits
                )
            else:
                self.storage.save_state(
                    self.positions,
                    None,
                    blocklist=self._reversal_blocklist,
                    reversal_tracker=self._reversal_tracker,
                    live_realized_pnl=self._live_realized_pnl,
                    wins=self.wins,
                    losses=self.losses,
                    total_entries=self.total_entries,
                    total_exits=self.total_exits
                )

    def is_entry_in_progress(self, hl_coin: str) -> bool:
        """
        Check if a multi-leg entry is currently in progress for a symbol.
        Used by PositionManager to avoid closing 'orphan' positions during entry.
        """
        if hl_coin not in self._entry_in_progress:
            return False
        entry_start = self._entry_in_progress[hl_coin]
        elapsed = time.time() - entry_start
        if elapsed > self._entry_lock_timeout:
            # Entry took too long - clear stale lock
            log.debug(f"[CARRY] Clearing stale entry lock for {hl_coin} (elapsed: {elapsed:.1f}s)")
            self._entry_in_progress.pop(hl_coin, None)
            return False
        return True

    def _register_entry_start(self, hl_coin: str) -> None:
        """Register that a multi-leg entry is starting for a symbol."""
        self._entry_in_progress[hl_coin] = time.time()
        log.debug(f"[CARRY] Entry lock acquired: {hl_coin}")

    def _clear_entry_lock(self, hl_coin: str) -> None:
        """Clear the entry lock after successful entry or rollback."""
        self._entry_in_progress.pop(hl_coin, None)
        log.debug(f"[CARRY] Entry lock released: {hl_coin}")

    # ============================
    # ORDER TRACKING (FIX 2026-01-22)
    # ============================

    def _track_order(self, order_id: str, symbol: str, venue, venue_name: str, side: str) -> None:
        """Track an open order for later cleanup if needed."""
        if not order_id:
            return
        self._active_orders[order_id] = {
            "symbol": symbol,
            "venue": venue,
            "venue_name": venue_name,
            "side": side,
            "ts": time.time()
        }
        log.debug(f"[CARRY] Order tracked: {order_id} ({venue_name} {side} {symbol})")

    def _untrack_order(self, order_id: str) -> None:
        """Remove order from tracking (after fill or cancel)."""
        if order_id and order_id in self._active_orders:
            del self._active_orders[order_id]
            log.debug(f"[CARRY] Order untracked: {order_id}")

    async def _cancel_all_tracked_orders(self) -> int:
        """Cancel all open orders. Call on shutdown or error recovery.

        Returns the number of orders successfully cancelled.
        """
        cancelled = 0
        for oid, info in list(self._active_orders.items()):
            try:
                log.info(f"[CARRY] Cancelling tracked order {oid} ({info['venue_name']} {info['side']} {info['symbol']})")
                if hasattr(info["venue"], "cancel_order"):
                    await info["venue"].cancel_order(oid, info["symbol"])
                self._untrack_order(oid)
                cancelled += 1
            except Exception as e:
                log.error(f"[CARRY] Failed to cancel tracked order {oid}: {e}")
        if cancelled > 0:
            log.info(f"[CARRY] Cancelled {cancelled} tracked orders")
        return cancelled

    async def _cancel_stale_tracked_orders(self, max_age_s: float = 120.0) -> int:
        """Cancel orders older than max_age_s. Run periodically to cleanup orphans.

        Returns the number of orders cancelled.
        """
        now = time.time()
        cancelled = 0
        for oid, info in list(self._active_orders.items()):
            age = now - info["ts"]
            if age > max_age_s:
                log.debug(f"[CARRY] Stale order detected: {oid} age={age:.0f}s > {max_age_s}s. Cancelling...")
                try:
                    if hasattr(info["venue"], "cancel_order"):
                        await info["venue"].cancel_order(oid, info["symbol"])
                    self._untrack_order(oid)
                    cancelled += 1
                except Exception as e:
                    log.error(f"[CARRY] Failed to cancel stale order {oid}: {e}")
        return cancelled

    async def validate_orphan(self, symbol: str, venue: str, size: float) -> bool:
        """
        P4 FIX: Callback from PositionManager to validate orphan before closing.

        Returns True if the position should be closed as orphan.
        Returns False if this is a valid carry position leg that should NOT be closed.

        This provides carry strategy final say on orphan closure, protecting against:
        - Stale data causing misdetection
        - Cross-venue pairs (AS-LT) where HL leg doesn't exist
        - Timing issues during entry/exit
        """
        # Extract HL coin from symbol (e.g., "AXSUSDT" -> "AXS")
        hl_coin = symbol.replace("USDT", "").replace("USD", "").replace("-PERP", "")

        # Check if this coin is part of a tracked carry position
        # FIX 2026-01-22: Use list() to prevent RuntimeError if positions modified during iteration
        for pos in list(self.positions.values()):
            if pos.hl_coin == hl_coin:
                log.info(
                    f"[CARRY] Rejecting orphan closure: {symbol} on {venue} is part of "
                    f"tracked position {pos.symbol} ({pos.direction}, ${pos.size_usd:.2f})"
                )
                return False  # Don't close - this is our position

        # Check if entry is in progress
        if self.is_entry_in_progress(hl_coin):
            log.info(f"[CARRY] Rejecting orphan closure: {symbol} - entry in progress")
            return False

        # FIX 2026-01-25: Check if position was recently closed (grace period for API lag)
        close_time = self._recently_closed.get(hl_coin, 0)
        if close_time > 0:
            age_s = time.time() - close_time
            if age_s < self._recently_closed_grace_s:
                log.debug(f"[CARRY] Rejecting orphan closure: {symbol} recently closed ({age_s:.1f}s ago)")
                return False
            else:
                # Grace period expired, clean up the entry
                del self._recently_closed[hl_coin]

        # Not tracked by carry strategy - allow position manager to close
        log.debug(f"[CARRY] Approving orphan closure: {symbol} not tracked")
        return True

    def update_mappings(self, pairs_map: Dict[str, str], lighter_map: Dict[str, str],
                        as_lt_map: Dict[str, str] = None) -> None:
        """Update symbol mappings dynamically."""
        self.pairs_map = pairs_map
        self.hl_to_lighter = {v: k for k, v in (lighter_map or {}).items()}
        if as_lt_map is not None:
            self.as_lt_map = as_lt_map

    def _log_opp_audit(self, symbol: str, diff_bps: float, status: str, 
                       reason: str = "", details: str = "") -> None:
        """Log skipped opportunities for analysis."""
        try:
            with open(self._opp_csv_path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([
                    f"{time.time():.3f}", symbol, f"{diff_bps:.2f}",
                    status, reason, details
                ])
        except Exception:
            pass

    def _log_mtm(self, pos: CarryPosition, mtm: Dict[str, float], curr_px1: float, curr_px2: float) -> None:
        """Log real-time MTM for position tracking."""
        try:
            # Use correct equity based on mode
            if self.paper_mode:
                equity = self.total_paper_equity
            else:
                equity = self._cached_total_equity if self._cached_total_equity > 0 else self.total_paper_equity

            with open(self._mtm_csv_path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                hold_hours = (time.time() - pos.entry_time) / 3600.0
                w.writerow([
                    f"{time.time():.3f}", pos.symbol, pos.direction, f"{pos.size_usd:.2f}",
                    f"{hold_hours:.2f}", f"{pos.entry_px_hl:.4f}", f"{pos.entry_px_as:.4f}",
                    f"{curr_px1:.4f}", f"{curr_px2:.4f}",
                    f"{mtm['price_pnl_usd']:.4f}", f"{mtm['funding_pnl_usd']:.4f}",
                    f"{mtm['total_mtm_usd']:.4f}", f"{mtm['mtm_bps']:.2f}",
                    f"{pos.entry_diff_bps:.2f}", f"{equity:.2f}"
                ])
        except Exception:
            pass

    def _log(self, symbol: str, action: str, direction: str, size_usd: float,
             r1: float, r2: float, v1_name: str, v2_name: str,
             hold_hours: float = 0.0, price_pnl: float = 0.0, funding_pnl: float = 0.0,
             equity: float = 0.0,
             v1_maker: bool = None, v2_maker: bool = None) -> None:
        """Log carry activity to CSV with explicit venue tracing and maker/taker tracking.

        FIX 2026-01-22: Added v1_maker, v2_maker params to track order execution type.
        """
        try:
            # Convert decimal to bps (multiply by 10000)
            r1_bps = r1 * 10000
            r2_bps = r2 * 10000
            diff_bps = (r1 - r2) * 10000

            # Safety: Ensure equity is correctly set if not provided
            if equity <= 0:
                equity = self.total_paper_equity if self.paper_mode else self._cached_total_equity

            # FIX 2026-01-22: Added net_profit column (price_pnl + funding_pnl)
            net_profit = price_pnl + funding_pnl

            with open(self._csv_path, "a", newline="", encoding="utf-8") as f:
                # FIX 2026-01-22: Check if file is empty and write headers if needed
                # This handles case where file is deleted during runtime
                if os.path.getsize(self._csv_path) == 0:
                    w = csv.writer(f)
                    w.writerow([
                        "ts", "symbol", "action", "direction", "size_usd",
                        "v1_rate", "v2_rate", "diff_bps",
                        "hold_hours", "price_pnl", "funding_pnl", "paper", "equity",
                        "v1_maker", "v2_maker", "net_profit"
                    ])
                w = csv.writer(f)
                w.writerow([
                    f"{time.time():.3f}", symbol, action, direction, f"{size_usd:.2f}",
                    f"{v1_name}:{r1_bps:.2f}", f"{v2_name}:{r2_bps:.2f}", f"{diff_bps:.2f}",
                    f"{hold_hours:.2f}", f"{price_pnl:.4f}", f"{funding_pnl:.4f}", self.paper_mode,
                    f"{equity:.2f}",
                    str(v1_maker) if v1_maker is not None else "",
                    str(v2_maker) if v2_maker is not None else "",
                    f"{net_profit:.4f}"
                ])
        except Exception as e:
            log.warning("[CARRY] Log error: %s", e)

    def _log_order(
        self,
        event: str,
        symbol: str,
        hl_coin: str,
        venue: str,
        side: str,
        qty_requested: float,
        qty_filled: float = 0.0,
        price: float = 0.0,
        avg_fill_price: float = 0.0,
        order_type: str = "IOC",
        direction: str = "",
        action: str = "",
        status: str = "PENDING",
        error: str = "",
        fees_est_usd: float = 0.0,
        details: str = "",
        # OPTIMIZATION 2026-01-24: Execution quality metrics
        expected_price: float = 0.0,
        submit_ts: float = 0.0,
        fill_ts: float = 0.0,
        is_maker: bool = False,
        taker_fee_would_be: float = 0.0
    ) -> None:
        """Log comprehensive order data to CSV for audit and analysis.

        FIX 2026-01-24: New comprehensive order tracking system.
        Records ALL order movements including entries, exits, rollbacks, failures, partials.

        OPTIMIZATION 2026-01-24: Added execution quality metrics:
            - expected_price: BBO price at order time (for slippage calculation)
            - submit_ts: Timestamp when order was submitted
            - fill_ts: Timestamp when fill was received
            - is_maker: Whether fill was maker (post-only)
            - taker_fee_would_be: What taker fee would have been (for fee savings calc)

        Args:
            event: Event type (ORDER_PLACED, ORDER_FILLED, ORDER_PARTIAL, ORDER_FAILED,
                   ORDER_CANCELLED, ROLLBACK, CLEANUP, ORPHAN_EXIT)
            symbol: Trading symbol (e.g., BTCUSDT, BTC)
            hl_coin: Normalized coin name (e.g., BTC)
            venue: Venue name (HL, AS, LT)
            side: Order side (buy, sell)
            qty_requested: Quantity requested
            qty_filled: Quantity actually filled
            price: Limit price (0 for market/IOC)
            avg_fill_price: Average fill price
            order_type: IOC, LIMIT, MARKET
            direction: Position direction (e.g., "Long HL / Short AS")
            action: Action type (ENTRY_LEG1, ENTRY_LEG2, EXIT_LEG1, EXIT_LEG2, SCALE_UP, SCALE_DOWN, etc.)
            status: Order status (SUCCESS, PARTIAL, FAILED, CANCELLED)
            error: Error message if failed
            fees_est_usd: Estimated fees in USD
            details: Additional context
            expected_price: BBO at order time for slippage calculation
            submit_ts: Order submission timestamp
            fill_ts: Fill received timestamp
            is_maker: True if maker fill
            taker_fee_would_be: Taker fee for fee savings calculation
        """
        try:
            # Get current equity for context
            if self.paper_mode:
                equity = self.total_paper_equity
            else:
                equity = self._cached_total_equity if self._cached_total_equity > 0 else self.total_paper_equity

            # OPTIMIZATION 2026-01-24: Calculate execution quality metrics
            slippage_bps = 0.0
            if expected_price > 0 and avg_fill_price > 0:
                slippage_bps = ((avg_fill_price - expected_price) / expected_price) * 10000
                # For sells, positive slippage is bad (sold lower than expected)
                # For buys, positive slippage is also bad (bought higher than expected)
                # Normalize so positive = worse execution
                if side.lower() == "sell":
                    slippage_bps = -slippage_bps  # Selling lower is negative slippage (bad)

            latency_ms = 0.0
            if submit_ts > 0 and fill_ts > 0:
                latency_ms = (fill_ts - submit_ts) * 1000

            fee_savings_usd = 0.0
            if is_maker and taker_fee_would_be > 0 and fees_est_usd >= 0:
                fee_savings_usd = taker_fee_would_be - fees_est_usd

            with open(self._order_csv_path, "a", newline="", encoding="utf-8") as f:
                # Check if file is empty and write headers if needed
                if os.path.getsize(self._order_csv_path) == 0:
                    w = csv.writer(f)
                    w.writerow([
                        "ts", "event", "symbol", "hl_coin", "venue", "side",
                        "qty_requested", "qty_filled", "price", "avg_fill_price",
                        "order_type", "direction", "action", "status",
                        "error", "fees_est_usd", "equity", "details",
                        # OPTIMIZATION 2026-01-24: Execution quality columns
                        "expected_price", "slippage_bps", "submit_ts", "fill_ts",
                        "latency_ms", "is_maker", "fee_savings_usd"
                    ])
                w = csv.writer(f)
                w.writerow([
                    f"{time.time():.3f}",
                    event,
                    symbol,
                    hl_coin,
                    venue,
                    side,
                    f"{qty_requested:.6f}",
                    f"{qty_filled:.6f}",
                    f"{price:.4f}" if price > 0 else "",
                    f"{avg_fill_price:.4f}" if avg_fill_price > 0 else "",
                    order_type,
                    direction,
                    action,
                    status,
                    error[:100] if error else "",  # Truncate long errors
                    f"{fees_est_usd:.4f}" if fees_est_usd > 0 else "",
                    f"{equity:.2f}",
                    details[:200] if details else "",  # Truncate long details
                    # OPTIMIZATION 2026-01-24: Execution quality values
                    f"{expected_price:.4f}" if expected_price > 0 else "",
                    f"{slippage_bps:.2f}" if slippage_bps != 0 else "",
                    f"{submit_ts:.3f}" if submit_ts > 0 else "",
                    f"{fill_ts:.3f}" if fill_ts > 0 else "",
                    f"{latency_ms:.1f}" if latency_ms > 0 else "",
                    "Y" if is_maker else "N",
                    f"{fee_savings_usd:.4f}" if fee_savings_usd > 0 else ""
                ])
        except Exception as e:
            log.warning("[CARRY] Order log error: %s", e)

    def _log_decision(
        self,
        symbol: str,
        hl_coin: str,
        decision_type: str,
        trigger_reason: str,
        v1_venue: str = "",
        v2_venue: str = "",
        v1_bbo: tuple = None,
        v2_bbo: tuple = None,
        depth_v1_usd: float = 0.0,
        depth_v2_usd: float = 0.0,
        current_yield_bps: float = 0.0,
        entry_yield_bps: float = 0.0,
        mtm_bps: float = 0.0,
        stop_threshold_bps: float = 0.0,
        funding_cushion_bps: float = 0.0,
        time_bonus_bps: float = 0.0,
        position_size_usd: float = 0.0,
        equity_usd: float = 0.0,
        outcome: str = "",
        outcome_reason: str = ""
    ) -> str:
        """Log a trade decision with full context for post-hoc analysis.

        OPTIMIZATION 2026-01-24: Comprehensive decision logging.
        Records WHY every entry/exit/skip happened with all relevant context.

        Args:
            symbol: Trading symbol (e.g., BTCUSDT)
            hl_coin: Normalized coin name (e.g., BTC)
            decision_type: ENTRY_ATTEMPT, ENTRY_SUCCESS, ENTRY_FAILED, EXIT_TRIGGER,
                          SCALE_UP, SCALE_DOWN, OPPORTUNITY_SKIPPED
            trigger_reason: Why this decision was triggered
            v1_bbo: (bid, ask) tuple for V1 venue
            v2_bbo: (bid, ask) tuple for V2 venue
            depth_v1_usd: Aggregated depth on V1 in USD
            depth_v2_usd: Aggregated depth on V2 in USD
            current_yield_bps: Current yield at decision time
            entry_yield_bps: Yield at position entry (if applicable)
            mtm_bps: Mark-to-market in basis points
            stop_threshold_bps: Current stop threshold
            funding_cushion_bps: Funding cushion bonus
            time_bonus_bps: Time-based stop widening bonus
            position_size_usd: Current position size
            equity_usd: Current equity
            outcome: Result of decision (SUCCESS, FAILED, SKIPPED)
            outcome_reason: Explanation of outcome

        Returns:
            str: The decision_id assigned to this log entry
        """
        try:
            self._decision_counter += 1
            decision_id = f"D{self._decision_counter:06d}"

            # Calculate spreads
            v1_bid = v1_bbo[0] if v1_bbo else 0.0
            v1_ask = v1_bbo[1] if v1_bbo else 0.0
            v2_bid = v2_bbo[0] if v2_bbo else 0.0
            v2_ask = v2_bbo[1] if v2_bbo else 0.0

            spread_v1_bps = 0.0
            spread_v2_bps = 0.0
            if v1_bid > 0 and v1_ask > 0:
                spread_v1_bps = ((v1_ask - v1_bid) / ((v1_ask + v1_bid) / 2)) * 10000
            if v2_bid > 0 and v2_ask > 0:
                spread_v2_bps = ((v2_ask - v2_bid) / ((v2_ask + v2_bid) / 2)) * 10000

            # Calculate allocation percentage
            allocation_pct = 0.0
            if equity_usd > 0:
                allocation_pct = position_size_usd / equity_usd

            with open(self._decision_csv_path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([
                    f"{time.time():.3f}",
                    decision_id,
                    symbol,
                    hl_coin,
                    decision_type,
                    trigger_reason[:100] if trigger_reason else "",
                    v1_venue,
                    v2_venue,
                    f"{v1_bid:.4f}" if v1_bid > 0 else "",
                    f"{v1_ask:.4f}" if v1_ask > 0 else "",
                    f"{v2_bid:.4f}" if v2_bid > 0 else "",
                    f"{v2_ask:.4f}" if v2_ask > 0 else "",
                    f"{spread_v1_bps:.2f}" if spread_v1_bps > 0 else "",
                    f"{spread_v2_bps:.2f}" if spread_v2_bps > 0 else "",
                    f"{depth_v1_usd:.0f}" if depth_v1_usd > 0 else "",
                    f"{depth_v2_usd:.0f}" if depth_v2_usd > 0 else "",
                    f"{current_yield_bps:.2f}" if current_yield_bps != 0 else "",
                    f"{entry_yield_bps:.2f}" if entry_yield_bps != 0 else "",
                    f"{mtm_bps:.2f}" if mtm_bps != 0 else "",
                    f"{stop_threshold_bps:.2f}" if stop_threshold_bps > 0 else "",
                    f"{funding_cushion_bps:.2f}" if funding_cushion_bps > 0 else "",
                    f"{time_bonus_bps:.2f}" if time_bonus_bps > 0 else "",
                    f"{position_size_usd:.2f}" if position_size_usd > 0 else "",
                    f"{equity_usd:.2f}" if equity_usd > 0 else "",
                    f"{allocation_pct:.4f}" if allocation_pct > 0 else "",
                    outcome,
                    outcome_reason[:200] if outcome_reason else ""
                ])
            log.debug(f"[CARRY] Decision logged: {decision_id} {decision_type} {symbol}")
            return decision_id
        except Exception as e:
            log.warning(f"[CARRY] Decision log error: {e}")
            import traceback
            log.warning(traceback.format_exc())
            return ""

    def _track_reversal(self, hl_coin: str) -> None:
        """Track a reversal event for a coin. May trigger auto-blocklist."""
        now = time.time()
        window_start = now - (self._reversal_window_hours * 3600)

        # Initialize or clean old entries
        if hl_coin not in self._reversal_tracker:
            self._reversal_tracker[hl_coin] = []

        # Remove old entries outside the window
        self._reversal_tracker[hl_coin] = [ts for ts in self._reversal_tracker[hl_coin] if ts > window_start]

        # Add new reversal
        self._reversal_tracker[hl_coin].append(now)
        count = len(self._reversal_tracker[hl_coin])
        log.info(f"[CARRY] REVERSAL TRACKED: {hl_coin} ({count}/{self._reversal_threshold} in {self._reversal_window_hours}h window)")

        # Check if threshold exceeded
        if count >= self._reversal_threshold:
            blocklist_until = now + (self._reversal_blocklist_hours * 3600)
            self._reversal_blocklist[hl_coin] = blocklist_until
            log.warning(f"[CARRY] AUTO-BLOCKLIST: {hl_coin} blocked for {self._reversal_blocklist_hours}h "
                       f"({count} reversals in {self._reversal_window_hours}h)")
            # Clear tracker after blocklisting
            self._reversal_tracker[hl_coin] = []
            # Persist blocklist immediately so it survives restarts
            self._save_state()

    def _is_blocklisted(self, hl_coin: str) -> bool:
        """Check if a coin is currently blocklisted."""
        if hl_coin not in self._reversal_blocklist:
            return False

        if time.time() > self._reversal_blocklist[hl_coin]:
            # Blocklist expired, remove it
            del self._reversal_blocklist[hl_coin]
            log.info(f"[CARRY] BLOCKLIST EXPIRED: {hl_coin} can trade again")
            return False

        return True

    def _is_entry_cooldown(self, hl_coin: str) -> bool:
        """Check if a coin is in entry failure cooldown (recent failed entry)."""
        if hl_coin not in self._entry_failure_cooldown:
            return False

        if time.time() > self._entry_failure_cooldown[hl_coin]:
            # Cooldown expired, remove it
            del self._entry_failure_cooldown[hl_coin]
            log.debug(f"[CARRY] ENTRY COOLDOWN EXPIRED: {hl_coin} can retry entry")
            return False

        remaining = (self._entry_failure_cooldown[hl_coin] - time.time()) / 60.0
        log.debug(f"[CARRY] {hl_coin} in entry cooldown ({remaining:.1f}m remaining)")
        return True

    def _set_entry_cooldown(self, hl_coin: str) -> None:
        """Set entry failure cooldown for a symbol after failed entry."""
        cooldown_until = time.time() + (self._entry_failure_cooldown_minutes * 60)
        self._entry_failure_cooldown[hl_coin] = cooldown_until
        log.warning(f"[CARRY] ENTRY COOLDOWN SET: {hl_coin} blocked for {self._entry_failure_cooldown_minutes:.0f}m after failed entry")

    def _is_direction_inverted(self, dir1: str, dir2: str) -> bool:
        """Check if two directions are inverted (opposite long/short on same venues).

        Example:
            "Short AS / Long LT" and "Long AS / Short LT" -> True (inverted)
            "Short AS / Long LT" and "Short AS / Long LT" -> False (same)
            "Short HL / Long AS" and "Short AS / Long LT" -> False (different venues)
        """
        if not dir1 or not dir2:
            return False

        # Parse direction format: "Long/Short V1 / Long/Short V2"
        # e.g., "Short AS / Long LT" -> ("Short", "AS", "Long", "LT")
        try:
            parts1 = dir1.split(" / ")
            parts2 = dir2.split(" / ")
            if len(parts1) != 2 or len(parts2) != 2:
                return False

            side1_v1, venue1_v1 = parts1[0].split()[:2]
            side1_v2, venue1_v2 = parts1[1].split()[:2]
            side2_v1, venue2_v1 = parts2[0].split()[:2]
            side2_v2, venue2_v2 = parts2[1].split()[:2]

            # Same venues but opposite sides = inverted
            same_venues = (venue1_v1 == venue2_v1 and venue1_v2 == venue2_v2)
            opposite_sides = (side1_v1 != side2_v1 and side1_v2 != side2_v2)

            return same_venues and opposite_sides
        except (ValueError, IndexError):
            return False

    def _is_forced_exit_cooldown(self, hl_coin: str, intended_direction: str = "") -> bool:
        """Check if a coin is in forced exit cooldown (recent MTM stop/trailing stop).

        FIX 2026-01-24: If intended_direction is provided and is INVERTED from the
        exited direction, allow re-entry (no cooldown). This permits funding flip trades.
        """
        if hl_coin not in self._forced_exit_cooldown:
            return False

        if time.time() > self._forced_exit_cooldown[hl_coin]:
            # Cooldown expired, remove it
            del self._forced_exit_cooldown[hl_coin]
            self._forced_exit_cooldown_direction.pop(hl_coin, None)
            log.info(f"[CARRY] FORCED EXIT COOLDOWN EXPIRED: {hl_coin} can re-enter")
            return False

        # FIX 2026-01-24: Check if intended direction is inverted (opposite)
        if intended_direction:
            exited_direction = self._forced_exit_cooldown_direction.get(hl_coin, "")
            if self._is_direction_inverted(exited_direction, intended_direction):
                log.info(f"[CARRY] {hl_coin} INVERTED direction allowed: exited={exited_direction}, entering={intended_direction}")
                return False  # Allow inverted re-entry

        remaining = (self._forced_exit_cooldown[hl_coin] - time.time()) / 60.0
        log.debug(f"[CARRY] {hl_coin} in forced exit cooldown ({remaining:.1f}m remaining)")
        return True

    def _set_forced_exit_cooldown(self, hl_coin: str, reason: str = "", direction: str = "") -> None:
        """Set forced exit cooldown for a symbol after MTM stop or trailing stop.

        BUGFIX: Prevents the 7-second re-entry pattern seen in logs where
        positions were stopped out and immediately re-entered into the same
        losing trade.

        FIX 2026-01-24: Also tracks the exited direction so inverted re-entries are allowed.
        """
        cooldown_until = time.time() + (self._forced_exit_cooldown_minutes * 60)
        self._forced_exit_cooldown[hl_coin] = cooldown_until
        if direction:
            self._forced_exit_cooldown_direction[hl_coin] = direction
        log.warning(f"[CARRY] FORCED EXIT COOLDOWN SET: {hl_coin} blocked for {self._forced_exit_cooldown_minutes:.0f}m after {reason} (direction={direction})")

    def _get_tiered_stop_params(self, yield_bps: float) -> Tuple[float, float]:
        """FIX 2026-01-22 P2.1-P2.2: Get yield-tiered stop parameters.

        Returns (stop_bps, trailing_bps) based on the yield tier.
        Higher yield = wider stops (more room for volatility).
        """
        abs_yield = abs(yield_bps)

        if abs_yield >= self._stop_tiers["high"]["threshold_bps"]:
            return (self._stop_tiers["high"]["stop_bps"],
                    self._stop_tiers["high"]["trailing_bps"])
        elif abs_yield >= self._stop_tiers["medium"]["threshold_bps"]:
            return (self._stop_tiers["medium"]["stop_bps"],
                    self._stop_tiers["medium"]["trailing_bps"])
        else:
            return (self._stop_tiers["low"]["stop_bps"],
                    self._stop_tiers["low"]["trailing_bps"])

    def _calc_funding_cushion(self, pos, mtm: Dict[str, float]) -> float:
        """Calculate funding cushion in bps.

        Funding profit provides a buffer against price drawdown.
        Example: $50 funding on $500 position = 1000bps funding.
        With 0.5x multiplier, we get 500bps cushion, capped at 100bps.
        """
        if not self._funding_cushion_enabled:
            return 0.0

        funding_pnl_usd = mtm.get("funding_pnl_usd", 0.0)
        if funding_pnl_usd <= 0:
            return 0.0  # No cushion for negative/zero funding

        # Convert funding profit to bps relative to position size
        funding_bps = (funding_pnl_usd / pos.size_usd) * 10000.0 if pos.size_usd > 0 else 0.0

        # Apply multiplier (default 0.5 = use half of funding as cushion)
        cushion_bps = funding_bps * self._funding_cushion_multiplier

        # Cap to prevent excessive cushion
        return min(cushion_bps, self._funding_cushion_cap_bps)

    def _calc_time_bonus(self, pos) -> float:
        """Calculate time-based stop widening bonus.

        Positions held longer have proven the carry thesis works.
        They deserve more room to ride out temporary volatility.

        Example: 12h position gets (12-4)*10 = 80bps bonus
        """
        if not self._time_bonus_enabled:
            return 0.0

        hold_hours = (time.time() - pos.entry_time) / 3600.0

        if hold_hours < self._time_bonus_start_hours:
            return 0.0

        bonus_hours = hold_hours - self._time_bonus_start_hours
        bonus_bps = bonus_hours * self._time_bonus_per_hour_bps

        return min(bonus_bps, self._time_bonus_cap_bps)

    def _parse_direction_venues(self, direction: str) -> Tuple[str, str]:
        """Parse direction string to extract venue names (e.g., 'Long HL / Short AS' -> ('HL', 'AS'))."""
        parts = direction.split(" / ")
        v1_name = parts[0].split()[-1].upper() if len(parts) >= 1 else "HL"
        v2_name = parts[1].split()[-1].upper() if len(parts) >= 2 else "AS"
        return v1_name, v2_name

    def _detect_spike(self, pos, current_mtm_bps: float) -> tuple:
        """Detect sudden MTM drops (flash crashes, liquidation cascades).

        Returns (is_spike, drop_bps).
        A spike is > spike_threshold_bps drop within spike_window_s.
        """
        if not self._spike_detection_enabled:
            return False, 0.0

        now = time.time()

        # Initialize mtm_history if needed
        if pos.mtm_history is None:
            pos.mtm_history = []

        # Maintain rolling history (keep last 5 minutes)
        pos.mtm_history.append((now, current_mtm_bps))
        cutoff = now - 300
        pos.mtm_history = [(t, m) for t, m in pos.mtm_history if t > cutoff]

        # Check if in active spike protection
        if now < pos.spike_protection_until:
            return True, 0.0

        # Look for spike in recent window
        window_cutoff = now - self._spike_window_s
        recent = [(t, m) for t, m in pos.mtm_history if t > window_cutoff]

        if len(recent) < 2:
            return False, 0.0

        max_recent_mtm = max(m for _, m in recent)
        drop_bps = max_recent_mtm - current_mtm_bps

        if drop_bps >= self._spike_threshold_bps:
            pos.spike_protection_until = now + self._spike_protection_s
            log.warning(
                f"[CARRY] SPIKE DETECTED {pos.symbol}: "
                f"MTM dropped {drop_bps:.1f}bps in {self._spike_window_s}s, "
                f"granting {self._spike_protection_s}s protection"
            )
            return True, drop_bps

        return False, drop_bps

    def _track_symbol_pnl(self, hl_coin: str, pnl_usd: float) -> None:
        """Track cumulative PnL per symbol and auto-blocklist losers.

        BUGFIX: Prevents repeated trading of symbols that consistently lose money.
        If cumulative loss exceeds threshold, symbol is blocklisted for configured period.
        """
        # Update cumulative PnL
        current_pnl = self._symbol_pnl_tracker.get(hl_coin, 0.0)
        new_pnl = current_pnl + pnl_usd
        self._symbol_pnl_tracker[hl_coin] = new_pnl

        log.info(f"[CARRY] SYMBOL PnL TRACKED: {hl_coin} trade PnL=${pnl_usd:.2f}, cumulative=${new_pnl:.2f}")

        # Check if loss threshold exceeded
        if new_pnl < -self._pnl_loss_blocklist_threshold:
            # Add to blocklist
            blocklist_until = time.time() + (self._pnl_blocklist_hours * 3600)
            self._reversal_blocklist[hl_coin] = blocklist_until
            log.warning(f"[CARRY] PnL BLOCKLIST: {hl_coin} blocked for {self._pnl_blocklist_hours}h "
                       f"(cumulative loss ${-new_pnl:.2f} > ${self._pnl_loss_blocklist_threshold:.2f})")
            # Reset tracker after blocklisting
            self._symbol_pnl_tracker[hl_coin] = 0.0
            # Persist blocklist
            self._save_state()

    # =========================================================================
    # SECTION 3: MTM & CALCULATIONS
    # =========================================================================

    async def _get_total_equity(self) -> float:
        """Fetch total equity across all venues with caching."""
        # Paper mode: always use simulated paper_equity for consistent sizing
        if self.paper_mode:
            return self.paper_equity

        # Live mode: fetch real equity from venues
        now = time.time()
        if now - self._last_equity_fetch < self._equity_cache_ttl and self._cached_total_equity > 0:
            return self._cached_total_equity

        try:
            eq_hl = await self.hl.equity() or 0.0
            eq_as = await self.asr.equity() or 0.0
            eq_lt = 0.0
            if self.lighter:
                eq_lt = await self.lighter.equity() or 0.0

            total = eq_hl + eq_as + eq_lt
            if total > 0:
                self._cached_total_equity = total
                self._last_equity_fetch = now

                # FIX 2026-01-24: Capture session start equity on first successful fetch
                if not self._session_start_equity_captured:
                    self._session_start_equity = total
                    self._session_start_equity_captured = True
                    log.info(f"[CARRY] SESSION START EQUITY CAPTURED: ${total:.2f}")

                return total
        except Exception as e:
            log.debug(f"[CARRY] Equity fetch error: {e}")

        return self._cached_total_equity

    def _calc_mtm(self, pos: CarryPosition, current_rates: Dict[str, float] = None) -> Dict[str, float]:
        """
        Calculate Mark-to-Market for a Carry position.
        Returns dict with price_pnl, funding_pnl, total_mtm (all in USD and bps).
        """
        # Parse direction to determine which venues are involved
        # Format: "Long V1 / Short V2" or "Short V1 / Long V2"
        # e.g., "Short AS / Long LT", "Long HL / Short AS"
        parts = pos.direction.split(" / ")
        if len(parts) != 2:
            return {"price_pnl_usd": 0.0, "funding_pnl_usd": 0.0, "total_mtm_usd": 0.0, "mtm_bps": 0.0, "stale": True}
        
        # Extract venues: "Long AS" -> "AS", "Short LT" -> "LT"
        leg1_venue = parts[0].split()[-1].upper()  # "Long AS" -> "AS"
        leg2_venue = parts[1].split()[-1].upper()  # "Short LT" -> "LT"
        
        # Get BBO from correct venue for each leg
        # Determine symbol for each venue
        def get_bbo_for_venue(venue: str) -> tuple:
            """Get BBO for position from specified venue, with correct symbol mapping."""
            try:
                if venue == "HL":
                    return self.hl.get_bbo(pos.hl_coin)
                elif venue == "AS":
                    return self.asr.get_bbo(pos.symbol)
                elif venue == "LT":
                    if self.lighter:
                        # Phase 4.2 Fix: Use stored mapping to get correct Lighter symbol
                        lt_sym = self.hl_to_lighter.get(pos.hl_coin)
                        # FIX 2026-01-22: Fallback to as_lt_map for AS-LT exclusive pairs (not on HL)
                        if not lt_sym:
                            lt_sym = self.as_lt_map.get(pos.symbol)
                            if lt_sym:
                                log.debug(f"[CARRY] AS-LT symbol resolved: {pos.symbol} -> LT:{lt_sym}")
                        if lt_sym:
                            bbo = self.lighter.get_bbo(lt_sym)
                            if not bbo:
                                log.debug(f"[CARRY] No BBO from Lighter for {lt_sym} (pos: {pos.symbol})")
                            return bbo
                return None
            except Exception as e:
                # FIX 2026-01-22: Use type(e).__name__ to avoid problematic characters in exception str
                log.debug(f"[CARRY] BBO fetch error for {venue}: {type(e).__name__}")
                return None

        bbo_leg1 = get_bbo_for_venue(leg1_venue)
        bbo_leg2 = get_bbo_for_venue(leg2_venue)

        # Require valid entry prices
        if pos.entry_px_hl <= 0 or pos.entry_px_as <= 0:
            return {"price_pnl_usd": 0.0, "funding_pnl_usd": 0.0, "total_mtm_usd": 0.0, "mtm_bps": 0.0, "stale": True}

        # P0 FIX: When BBO unavailable, mark MTM as stale and use 0.0 to avoid false profit display
        # Using entry price as fallback was dangerous - showed fake profit when actually losing
        mtm_stale = False
        if bbo_leg1:
            current_px_leg1 = (bbo_leg1[0] + bbo_leg1[1]) * 0.5
        else:
            current_px_leg1 = 0.0
            mtm_stale = True
            # FIX 2026-01-22: Changed from warning to debug to reduce log flooding
            # Aggregated staleness is already reported by circuit breaker
            log.debug(f"[CARRY] MTM stale for {pos.hl_coin}: No BBO for leg1")

        if bbo_leg2:
            current_px_leg2 = (bbo_leg2[0] + bbo_leg2[1]) * 0.5
        else:
            current_px_leg2 = 0.0
            mtm_stale = True
            # FIX 2026-01-22: Changed from warning to debug to reduce log flooding
            # Aggregated staleness is already reported by circuit breaker
            log.debug(f"[CARRY] MTM stale for {pos.hl_coin}: No BBO for leg2")

        # If MTM is stale, return zero price PnL with stale flag
        if mtm_stale:
            return {"price_pnl_usd": 0.0, "funding_pnl_usd": 0.0, "total_mtm_usd": 0.0, "mtm_bps": 0.0, "stale": True}
        
        # FIX 2026-01-22: Map entry prices based on which venue each leg uses
        # IMPORTANT: Entry stores prices as:
        #   - entry_px_hl = prices["v1"] if v1 is HL, else prices["v2"]
        #   - entry_px_as = prices["v2"] if v1 is HL, else prices["v1"]
        #
        # This means:
        # - For HL-AS: entry_px_hl=HL, entry_px_as=AS
        # - For HL-LT: entry_px_hl=HL, entry_px_as=LT
        # - For AS-LT: entry_px_hl=LT (v2), entry_px_as=AS (v1)  <-- KEY INSIGHT
        #
        # To correctly map, we need to check BOTH venues:
        def get_entry_price_for_venue(venue: str) -> float:
            """Get the correct entry price for a specific venue."""
            if venue == "HL":
                return pos.entry_px_hl
            elif venue == "AS":
                # AS price is in entry_px_as UNLESS this is an HL-AS pair where AS is v2
                # Check if HL is involved - if so, AS is in entry_px_as
                # If no HL involved (AS-LT), AS is v1, stored in entry_px_as
                return pos.entry_px_as
            elif venue == "LT":
                # LT price location depends on pair type:
                # - HL-LT: LT is v2, stored in entry_px_as
                # - AS-LT: LT is v2, stored in entry_px_hl (because v1=AS, not HL)
                has_hl = "HL" in pos.direction.upper()
                return pos.entry_px_as if has_hl else pos.entry_px_hl
            return pos.entry_px_as  # Fallback

        entry_px_leg1 = get_entry_price_for_venue(leg1_venue)
        entry_px_leg2 = get_entry_price_for_venue(leg2_venue)

        # FIX 2026-01-22: Use actual per-leg USD sizes, not size_usd / 2
        # The old calculation assumed equal leg sizes which is wrong after partial fills or price divergence
        # Now we use size_v1_usd and size_v2_usd if available, with fallback to size_usd / 2
        if hasattr(pos, 'size_v1_usd') and hasattr(pos, 'size_v2_usd') and pos.size_v1_usd > 0 and pos.size_v2_usd > 0:
            # Map V1/V2 to leg1/leg2 based on direction
            # Direction format: "Long V1 / Short V2" where V1 and V2 are venue names
            # leg1_venue = V1, leg2_venue = V2 from the direction string
            size_usd_leg1 = pos.size_v1_usd
            size_usd_leg2 = pos.size_v2_usd
        else:
            # Fallback to equal split
            size_usd_leg1 = pos.size_usd / 2
            size_usd_leg2 = pos.size_usd / 2

        # Calculate size in BASE UNITS (coins) from USD
        size_base_leg1 = size_usd_leg1 / entry_px_leg1 if entry_px_leg1 > 0 else 0
        size_base_leg2 = size_usd_leg2 / entry_px_leg2 if entry_px_leg2 > 0 else 0

        # Determine if long or short on leg1
        is_long_leg1 = pos.direction.lower().startswith("long")
        
        # LIQUIDATION PRICES FOR ROBUST MTM
        # If Long Leg 1: Exit is SELL (Bid) on Leg 1, BUY (Ask) on Leg 2
        # If Short Leg 1: Exit is BUY (Ask) on Leg 1, SELL (Bid) on Leg 2
        # Fallback to entry price if BBO unavailable (price PnL = 0 in that case)
        if is_long_leg1:
            liq_px_leg1 = bbo_leg1[0] if bbo_leg1 else entry_px_leg1  # Bid
            liq_px_leg2 = bbo_leg2[1] if bbo_leg2 else entry_px_leg2  # Ask
            # PnL = (Exit - Entry) for Long, (Entry - Exit) for Short
            pnl_leg1 = (liq_px_leg1 - entry_px_leg1) * size_base_leg1
            pnl_leg2 = (entry_px_leg2 - liq_px_leg2) * size_base_leg2
        else:
            liq_px_leg1 = bbo_leg1[1] if bbo_leg1 else entry_px_leg1  # Ask
            liq_px_leg2 = bbo_leg2[0] if bbo_leg2 else entry_px_leg2  # Bid
            pnl_leg1 = (entry_px_leg1 - liq_px_leg1) * size_base_leg1
            pnl_leg2 = (liq_px_leg2 - entry_px_leg2) * size_base_leg2

        price_pnl_usd = pnl_leg1 + pnl_leg2

        # DEBUG 2026-01-22: Log price_pnl details for troubleshooting
        log.debug(f"[MTM] {pos.hl_coin} price_pnl: "
                 f"L1({leg1_venue})={'Long' if is_long_leg1 else 'Short'} "
                 f"entry=${entry_px_leg1:.4f} exit=${liq_px_leg1:.4f} size={size_base_leg1:.6f} pnl=${pnl_leg1:.4f} | "
                 f"L2({leg2_venue})={'Short' if is_long_leg1 else 'Long'} "
                 f"entry=${entry_px_leg2:.4f} exit=${liq_px_leg2:.4f} size={size_base_leg2:.6f} pnl=${pnl_leg2:.4f} | "
                 f"TOTAL=${price_pnl_usd:.4f}")
        
        # Funding PnL (estimated from hold time and current rates for a more realistic value)
        hold_hours = (time.time() - pos.entry_time) / 3600.0
        
        # Determine current diff bps for yield estimation
        v1_name, v2_name = self._parse_direction_venues(pos.direction)
        
        def get_rate_safe(venue: str, coin: str, symbol: str) -> float:
            try:
                rates = {}
                # BUGFIX: Use cached rates from carry strategy, not venue._funding_rates
                # Venues don't store _funding_rates - carry strategy caches them
                if venue == "HL": rates = self._cached_hl_rates
                elif venue == "AS": rates = self._cached_as_rates
                elif venue == "LT": rates = self.lighter._funding_rates if self.lighter else {}

                # Correctly Map Symbol for Lighter
                lookup_sym = symbol
                if venue == "LT":
                    # FIX 2026-01-22: Also check as_lt_map for AS-LT exclusive pairs
                    lookup_sym = self.hl_to_lighter.get(coin) or self.as_lt_map.get(symbol) or symbol

                rate = rates.get(coin if venue == "HL" else lookup_sym, 0.0)
                return rate * 8 if venue in ("HL", "LT") else rate
            except Exception: return 0.0

        curr_r1 = get_rate_safe(v1_name, pos.hl_coin, pos.symbol)
        curr_r2 = get_rate_safe(v2_name, pos.hl_coin, pos.symbol)
        curr_diff = (curr_r1 - curr_r2) * 10000  # Current yield in bps

        # FIX 2026-01-22: Use CURRENT yield for funding calculation, with proper sign
        # - If current diff has same sign as entry diff, we're RECEIVING funding
        # - If current diff has opposite sign, we're PAYING funding (negative)
        now = time.time()
        last_t = pos.last_accrual_time if pos.last_accrual_time > 0 else pos.entry_time
        hold_hours_since_accrual = (now - last_t) / 3600.0

        # Calculate pending funding with proper sign based on rate direction
        # We entered to receive funding when entry_signed_diff_bps was positive
        # If current diff sign matches entry sign, we're still earning
        entry_sign = pos.entry_signed_diff_bps if hasattr(pos, 'entry_signed_diff_bps') else pos.entry_diff_bps
        if entry_sign != 0 and curr_diff * entry_sign >= 0:
            # Same direction - we're receiving funding (use current rate)
            pending_funding_usd = abs(curr_diff) / 10000.0 * pos.size_usd * (hold_hours_since_accrual / 8.0)
        elif entry_sign != 0 and curr_diff * entry_sign < 0:
            # Opposite direction - we're PAYING funding (negative, use current rate)
            pending_funding_usd = -abs(curr_diff) / 10000.0 * pos.size_usd * (hold_hours_since_accrual / 8.0)
        else:
            # Fallback: entry_sign is 0 (edge case), assume positive
            pending_funding_usd = abs(curr_diff) / 10000.0 * pos.size_usd * (hold_hours_since_accrual / 8.0)

        funding_pnl_usd = pos.accrued_funding_usd + pending_funding_usd
        
        # Total MTM (Gross)
        total_mtm_usd_gross = price_pnl_usd + funding_pnl_usd

        # PHASE 8: Net MTM with comprehensive fee calculation
        # FIX 2026-01-22: Use total_fees_paid_usd which includes entry + all scale-ups
        # Exit fees are still projected (assume taker worst case until actual exit)
        v1_name, v2_name = self._parse_direction_venues(pos.direction)

        # Get taker fees for exit (worst case assumption until actual exit)
        fee_v1 = self._venue_fees.get(v1_name, {}).get("taker", 4.5)
        fee_v2 = self._venue_fees.get(v2_name, {}).get("taker", 4.0)
        exit_fee_bps = fee_v1 + fee_v2
        exit_fee_usd = pos.size_usd * (exit_fee_bps / 10000.0)

        # FIX 2026-01-22: Use actual accumulated fees (entry + scale-ups) if available
        # Otherwise fallback to old entry_fee_bps calculation for backward compatibility
        if hasattr(pos, 'total_fees_paid_usd') and pos.total_fees_paid_usd > 0:
            entry_fees_usd = pos.total_fees_paid_usd
        else:
            # Fallback: use entry_fee_bps * position size (doesn't account for scale-ups correctly)
            entry_fee_bps = pos.entry_fee_bps if hasattr(pos, 'entry_fee_bps') and pos.entry_fee_bps > 0 else exit_fee_bps
            entry_fees_usd = pos.size_usd * (entry_fee_bps / 10000.0)

        # Total fee cost = actual entry/scale-up fees (sunk) + projected exit fees (taker)
        fee_cost_usd = entry_fees_usd + exit_fee_usd
        total_mtm_usd = total_mtm_usd_gross - fee_cost_usd

        # Convert to bps
        # Use one-leg notional for bps calculation (matches basis move)
        notional = pos.size_usd
        mtm_bps = (total_mtm_usd / notional * 10000.0) if notional > 0 else 0.0

        # FIX 2026-01-22: Sanitize all numeric values to prevent NaN/Inf from crashing logging
        def safe_float(v, default=0.0):
            if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
                return default
            return v

        return {
            "price_pnl_usd": safe_float(price_pnl_usd),
            "funding_pnl_usd": safe_float(funding_pnl_usd),
            "total_mtm_usd": safe_float(total_mtm_usd),
            "total_mtm_usd_gross": safe_float(total_mtm_usd_gross),
            "fee_cost_usd": safe_float(fee_cost_usd),
            "mtm_bps": safe_float(mtm_bps),
            "curr_diff_bps": safe_float(curr_diff),
            "stale": False,
            # FIX 2026-01-22: Include prices for dashboard debugging
            "liq_px_leg1": safe_float(liq_px_leg1),
            "liq_px_leg2": safe_float(liq_px_leg2),
            "entry_px_leg1": safe_float(entry_px_leg1),
            "entry_px_leg2": safe_float(entry_px_leg2),
            "size_base_leg1": safe_float(size_base_leg1),
            "size_base_leg2": safe_float(size_base_leg2),
            "pnl_leg1": safe_float(pnl_leg1),
            "pnl_leg2": safe_float(pnl_leg2),
        }


    def _slip_bps(self, bbo1: Tuple[float, float], bbo2: Tuple[float, float]) -> float:
        """Estimate slippage from spreads on both venues."""
        hb, ha = bbo1[0], bbo1[1]
        ab, aa = bbo2[0], bbo2[1]
        mid1 = (hb + ha) * 0.5
        mid2 = (ab + aa) * 0.5
        if mid1 <= 0 or mid2 <= 0: return 0.0
        half1 = (ha - hb) / mid1 * 5000.0
        half2 = (aa - ab) / mid2 * 5000.0
        return max(0.0, half1 + half2)

    def _get_roundtrip_fees_bps(self, v1_name: str, v2_name: str) -> float:
        """
        Get round-trip fees (entry + exit) for a venue pair.
        FIX 2026-01-21: Replace hardcoded EST_FEES_BPS=15.0 with dynamic calculation.

        Round-trip = 2 * (taker_fee_v1 + taker_fee_v2)
        - HL+AS: 2 * (4.5 + 4.0) = 17 bps
        - HL+LT: 2 * (4.5 + 2.0) = 13 bps
        - AS+LT: 2 * (4.0 + 2.0) = 12 bps
        """
        key = f"{v1_name}_{v2_name}"
        if key in self._roundtrip_fees:
            return self._roundtrip_fees[key]
        # Fallback to calculation if key not found
        fee_v1 = self._venue_fees.get(v1_name, 4.5)
        fee_v2 = self._venue_fees.get(v2_name, 4.0)
        return 2 * (fee_v1 + fee_v2)

    async def _calculate_replacement_cost_bps(
        self,
        exit_venue1: str,
        exit_venue2: str,
        entry_venue1: str,
        entry_venue2: str,
        exit_symbol: str,
        entry_hl_coin: str,
    ) -> float:
        """
        FIX 2026-01-25: Calculate true replacement cost using actual venue fees + live spread + safety margin.

        This replaces static threshold (40bps) with dynamic calculation based on:
        1. Actual taker fees from venues.yaml for both exit and entry positions
        2. Live spreads from current BBO (cross half the spread on each side)
        3. Configurable safety margin for execution slippage

        Args:
            exit_venue1: First venue of position to close (e.g., "HL")
            exit_venue2: Second venue of position to close (e.g., "AS")
            entry_venue1: First venue of new position (e.g., "AS")
            entry_venue2: Second venue of new position (e.g., "LT")
            exit_symbol: Symbol being closed (for spread lookup)
            entry_hl_coin: HL coin of new position (for spread lookup)

        Returns:
            Total replacement cost in bps
        """
        # 1. Get taker fees from venues.yaml
        exit_fee_bps = (
            self._venue_fees.get(exit_venue1, {}).get("taker", 5.0) +
            self._venue_fees.get(exit_venue2, {}).get("taker", 5.0)
        )
        entry_fee_bps = (
            self._venue_fees.get(entry_venue1, {}).get("taker", 5.0) +
            self._venue_fees.get(entry_venue2, {}).get("taker", 5.0)
        )

        # 2. Get live spreads for exit position
        exit_spread_bps = 0.0
        try:
            # Get BBO for exit legs
            if exit_venue1 == "HL":
                bbo1 = self.hl.get_bbo(exit_symbol.replace("USDT", "") if "USDT" in exit_symbol else exit_symbol)
            elif exit_venue1 == "AS":
                bbo1 = self.asr.get_bbo(exit_symbol if "USDT" in exit_symbol else f"{exit_symbol}USDT")
            elif exit_venue1 == "LT" and self.lighter:
                lt_sym = self.hl_to_lighter.get(exit_symbol.replace("USDT", "")) or exit_symbol
                bbo1 = self.lighter.get_bbo(lt_sym)
            else:
                bbo1 = None

            if exit_venue2 == "HL":
                bbo2 = self.hl.get_bbo(exit_symbol.replace("USDT", "") if "USDT" in exit_symbol else exit_symbol)
            elif exit_venue2 == "AS":
                bbo2 = self.asr.get_bbo(exit_symbol if "USDT" in exit_symbol else f"{exit_symbol}USDT")
            elif exit_venue2 == "LT" and self.lighter:
                lt_sym = self.hl_to_lighter.get(exit_symbol.replace("USDT", "")) or exit_symbol
                bbo2 = self.lighter.get_bbo(lt_sym)
            else:
                bbo2 = None

            if bbo1 and bbo1[0] > 0 and bbo1[1] > 0:
                mid1 = (bbo1[0] + bbo1[1]) / 2
                exit_spread_bps += ((bbo1[1] - bbo1[0]) / mid1) * 10000 / 2  # Half spread
            if bbo2 and bbo2[0] > 0 and bbo2[1] > 0:
                mid2 = (bbo2[0] + bbo2[1]) / 2
                exit_spread_bps += ((bbo2[1] - bbo2[0]) / mid2) * 10000 / 2  # Half spread
        except Exception as e:
            log.debug(f"[REPLACE COST] Exit spread calc error: {e}, using default")
            exit_spread_bps = 10.0  # Conservative default

        # 3. Get live spreads for entry position
        entry_spread_bps = 0.0
        try:
            # Get BBO for entry legs using entry_hl_coin
            if entry_venue1 == "HL":
                bbo1 = self.hl.get_bbo(entry_hl_coin)
            elif entry_venue1 == "AS":
                bbo1 = self.asr.get_bbo(f"{entry_hl_coin}USDT")
            elif entry_venue1 == "LT" and self.lighter:
                lt_sym = self.hl_to_lighter.get(entry_hl_coin) or entry_hl_coin
                bbo1 = self.lighter.get_bbo(lt_sym)
            else:
                bbo1 = None

            if entry_venue2 == "HL":
                bbo2 = self.hl.get_bbo(entry_hl_coin)
            elif entry_venue2 == "AS":
                bbo2 = self.asr.get_bbo(f"{entry_hl_coin}USDT")
            elif entry_venue2 == "LT" and self.lighter:
                lt_sym = self.hl_to_lighter.get(entry_hl_coin) or entry_hl_coin
                bbo2 = self.lighter.get_bbo(lt_sym)
            else:
                bbo2 = None

            if bbo1 and bbo1[0] > 0 and bbo1[1] > 0:
                mid1 = (bbo1[0] + bbo1[1]) / 2
                entry_spread_bps += ((bbo1[1] - bbo1[0]) / mid1) * 10000 / 2  # Half spread
            if bbo2 and bbo2[0] > 0 and bbo2[1] > 0:
                mid2 = (bbo2[0] + bbo2[1]) / 2
                entry_spread_bps += ((bbo2[1] - bbo2[0]) / mid2) * 10000 / 2  # Half spread
        except Exception as e:
            log.debug(f"[REPLACE COST] Entry spread calc error: {e}, using default")
            entry_spread_bps = 10.0  # Conservative default

        # 4. Safety margin from risk.yaml (default 5bps)
        safety_margin_bps = float(self.risk.get("carry", {}).get("replacement_safety_margin_bps", 5.0))

        # 5. Total cost = exit fees + entry fees + exit spread + entry spread + safety
        total_cost = (
            exit_fee_bps +
            entry_fee_bps +
            exit_spread_bps +
            entry_spread_bps +
            safety_margin_bps
        )

        log.debug(f"[REPLACE COST] {exit_venue1}-{exit_venue2} -> {entry_venue1}-{entry_venue2}: "
                  f"exit_fees={exit_fee_bps:.1f}bps, entry_fees={entry_fee_bps:.1f}bps, "
                  f"exit_spread={exit_spread_bps:.1f}bps, entry_spread={entry_spread_bps:.1f}bps, "
                  f"safety={safety_margin_bps:.1f}bps, TOTAL={total_cost:.1f}bps")

        return total_cost

    def _get_venue_aware_threshold(self, v1_name: str, v2_name: str) -> float:
        """
        PHASE 3: Get venue-aware entry threshold based on fee structure.

        Lower fees = lower threshold = more entries on profitable pairs.
        - HL-AS: 60 bps (17 bps fees, highest)
        - HL-LT: 50 bps (13 bps fees, medium)
        - AS-LT: 45 bps (12 bps fees, lowest)
        """
        # Venue pair thresholds (sorted by fees)
        venue_thresholds = {
            ("HL", "AS"): 60.0,  # 17 bps fees - standard threshold
            ("AS", "HL"): 60.0,  # Same pair reversed
            ("HL", "LT"): 50.0,  # 13 bps fees - 10 bps lower
            ("LT", "HL"): 50.0,  # Same pair reversed
            ("AS", "LT"): 45.0,  # 12 bps fees - 15 bps lower
            ("LT", "AS"): 45.0,  # Same pair reversed
        }

        return venue_thresholds.get((v1_name, v2_name), self.min_funding_bps)

    def _get_maker_timeout(self, spread_bps: float, is_scale_up: bool = False) -> float:
        """Dynamic timeout based on spread size - worth waiting longer for bigger spreads.

        FIX 2026-01-24: Scale-up uses shorter timeout (position already exists, less risky).
        """
        if is_scale_up:
            # Scale-up: shorter timeouts since we already have exposure
            if spread_bps > 100:
                return 45.0  # Big spread
            elif spread_bps > 80:
                return 30.0
            elif spread_bps > 60:
                return 20.0
            else:
                return 12.0  # Small spread - speed matters
        else:
            # Entry: can afford to wait longer for maker fills
            if spread_bps > 100:
                return 60.0  # Big spread - worth waiting for maker
            elif spread_bps > 80:
                return 45.0
            elif spread_bps > 60:
                return 30.0
            else:
                return 15.0  # Small spread - speed matters

    def _count_recent_failures(self, hl_coin: str, minutes: float = 10.0) -> int:
        """Count recent entry failures for a symbol (used for dynamic cooldown)."""
        cutoff = time.time() - (minutes * 60)
        count = 0
        for attempt in self._entry_attempts:
            if attempt.get("hl_coin") == hl_coin and attempt.get("ts", 0) > cutoff:
                if "FAILED" in str(attempt.get("v1_status", "")) or "FAILED" in str(attempt.get("v2_status", "")):
                    count += 1
        return count

    def _get_yield_tiered_allocation_cap(self, ann_rate_bps: float) -> float:
        """
        FIX 2026-01-25: Yield-tiered allocation caps for better capital efficiency.

        Higher-yield positions are allowed more concentration:
        - Tier 1: 100+ bps yield -> 50% allocation cap
        - Tier 2: 60-100 bps yield -> 40% allocation cap (current default)
        - Tier 3: <60 bps yield -> 25% allocation cap

        This allows exceptional opportunities to capture more capital while
        limiting exposure to marginal opportunities.

        Args:
            ann_rate_bps: Annualized funding rate differential in basis points

        Returns:
            Allocation cap as a fraction (0.25 to 0.50)
        """
        # Get tier thresholds from config, with sensible defaults
        tier1_threshold = float(self.risk.get("carry", {}).get("yield_tier1_threshold_bps", 100.0))
        tier1_cap = float(self.risk.get("carry", {}).get("yield_tier1_alloc_cap", 0.50))

        if ann_rate_bps >= tier1_threshold:
            return tier1_cap  # High-yield: 50%
        elif ann_rate_bps >= self.min_funding_bps:
            return self.max_symbol_alloc_pct  # Medium-yield: 40% (default)
        else:
            return 0.25  # Low-yield: 25%

    async def _detect_stress_mode(self) -> bool:
        """
        FIX 2026-01-25: Detect market stress conditions.

        Stress mode triggers when:
        - Spread > 50bps on any active position
        - Fill rate < 50% in last hour (too many failed entries)
        - High funding rate volatility (future enhancement)

        In stress mode, reduce position sizes to minimize risk.

        Returns:
            True if stress conditions detected, False otherwise
        """
        STRESS_SPREAD_BPS = 50.0
        STRESS_FILL_RATE_THRESHOLD = 0.50
        STRESS_CHECK_HOURS = 1.0

        # 1. Check spread stress on active positions
        for sym, pos in list(self.positions.items()):
            # Parse venues from direction
            parts = pos.direction.split(" / ")
            v1_name = parts[0].split()[-1].upper() if len(parts) >= 1 else "HL"
            v2_name = parts[1].split()[-1].upper() if len(parts) >= 2 else "AS"

            # Get BBOs
            v_map = {"HL": self.hl, "AS": self.asr, "LT": self.lighter}
            v1_obj = v_map.get(v1_name)
            v2_obj = v_map.get(v2_name)

            if not v1_obj or not v2_obj:
                continue

            # Get BBO for each leg
            s1 = pos.hl_coin if v1_name == "HL" else (pos.symbol if v1_name == "AS" else self.hl_to_lighter.get(pos.hl_coin))
            s2 = pos.hl_coin if v2_name == "HL" else (pos.symbol if v2_name == "AS" else self.hl_to_lighter.get(pos.hl_coin))

            try:
                bbo1 = v1_obj.get_bbo(s1) if s1 else None
                bbo2 = v2_obj.get_bbo(s2) if s2 else None

                if bbo1 and bbo1[0] > 0 and bbo1[1] > 0:
                    mid1 = (bbo1[0] + bbo1[1]) / 2
                    spread1_bps = ((bbo1[1] - bbo1[0]) / mid1) * 10000
                    if spread1_bps > STRESS_SPREAD_BPS:
                        log.info(f"[STRESS] Detected wide spread on {v1_name}/{s1}: {spread1_bps:.1f}bps > {STRESS_SPREAD_BPS}bps")
                        return True

                if bbo2 and bbo2[0] > 0 and bbo2[1] > 0:
                    mid2 = (bbo2[0] + bbo2[1]) / 2
                    spread2_bps = ((bbo2[1] - bbo2[0]) / mid2) * 10000
                    if spread2_bps > STRESS_SPREAD_BPS:
                        log.info(f"[STRESS] Detected wide spread on {v2_name}/{s2}: {spread2_bps:.1f}bps > {STRESS_SPREAD_BPS}bps")
                        return True
            except Exception as e:
                log.debug(f"[STRESS] Spread check error for {pos.hl_coin}: {e}")

        # 2. Check fill rate stress
        if hasattr(self, '_entry_attempts') and self._entry_attempts:
            now = time.time()
            cutoff = now - (STRESS_CHECK_HOURS * 3600)

            recent_attempts = [a for a in self._entry_attempts if a.get("ts", 0) > cutoff]
            if len(recent_attempts) >= 3:  # Need at least 3 attempts to judge
                successes = sum(1 for a in recent_attempts
                               if "FILLED" in str(a.get("v1_status", "")) and "FILLED" in str(a.get("v2_status", "")))
                fill_rate = successes / len(recent_attempts)

                if fill_rate < STRESS_FILL_RATE_THRESHOLD:
                    log.info(f"[STRESS] Low fill rate: {fill_rate*100:.1f}% < {STRESS_FILL_RATE_THRESHOLD*100:.1f}% "
                            f"({successes}/{len(recent_attempts)} in last {STRESS_CHECK_HOURS}h)")
                    return True

        return False

    def _get_stress_adjusted_slice_pct(self, base_pct: float) -> float:
        """
        Get position slice percentage, adjusted for stress mode.

        In stress mode, reduces slice size by 50% to minimize risk.
        Uses cached stress state to avoid repeated detection calls.
        """
        stress_mode_pct = float(self.risk.get("carry", {}).get("stress_mode_slice_pct", base_pct * 0.5))
        if hasattr(self, '_stress_mode_active') and self._stress_mode_active:
            return stress_mode_pct
        return base_pct

    async def _should_use_maker_for_venue(self, venue, symbol: str, qty: float, venue_name: str = "") -> bool:
        """
        FIX 2026-01-25: Decide if maker mode makes sense for this venue/symbol.

        Skips maker mode when:
        - Maker mode is globally disabled
        - Venue has thin depth (less than 50% of our order size)

        This prevents wasted maker attempts on illiquid venues.
        """
        if not self._maker_order_enabled:
            return False

        # Get current depth
        try:
            if hasattr(venue, 'get_bbo_with_depth'):
                depth_data = venue.get_bbo_with_depth(symbol)
                if not depth_data:
                    # No depth data available, use maker (benefit of doubt)
                    return True
                (bid_px, bid_sz), (ask_px, ask_sz) = depth_data
            else:
                # Fallback to simple BBO check
                bbo = venue.get_bbo(symbol)
                if not bbo or bbo[0] <= 0 or bbo[1] <= 0:
                    return False  # No valid BBO, skip maker
                return True  # Can't check depth, assume maker is OK

            # Check depth on our side (we're providing liquidity)
            # For BUY: we post on bid side, check ask depth (what we're taking from)
            # For SELL: we post on ask side, check bid depth
            # Actually for maker, we're posting our order, so check opposite side
            min_depth = min(bid_sz, ask_sz)

            # Require at least 50% of our order size available
            if min_depth < qty * 0.5:
                if not venue_name:
                    venue_name = getattr(venue, "__class__", type(venue)).__name__[:2].upper()
                log.debug(f"[MAKER] {venue_name}/{symbol}: thin depth ({min_depth:.4f} vs qty={qty:.4f}), using taker")
                return False

            return True
        except Exception as e:
            log.debug(f"[MAKER] _should_use_maker_for_venue error: {e}")
            return True  # Default to maker on error

    async def _execute_with_maker_fallback(
        self,
        venue,
        symbol: str,
        side: str,
        qty: float,
        bbo: Tuple[float, float],
        timeout_s: float,
        is_entry: bool = True,
        reduce_only: bool = False,
        venue_name: str = ""
    ) -> Dict[str, Any]:
        """
        PHASE 5 (Enhanced 2026-01-22): Execute order with aggressive maker-first strategy.

        Flow:
        1. If maker disabled or bad BBO → direct IOC
        2. REPOSTING LOOP (target: 80% maker fills):
           - Post GTC limit at aggressive price
           - Wait 12s per attempt (5 attempts = 60s total)
           - On partial/timeout: cancel, get fresh BBO, repost tighter
           - Track cumulative fills across attempts
        3. Final IOC fallback for any remainder
        4. Return: {filled, avg_price, is_maker, status}

        Args:
            venue: Venue object (hl, asr, lighter)
            symbol: Trading symbol
            side: "BUY" or "SELL"
            qty: Order quantity
            bbo: (bid, ask) tuple (initial - will be refreshed each attempt)
            timeout_s: Total timeout for all attempts (divided among attempts)
            is_entry: True for entry, False for exit (affects logging)
            reduce_only: Use reduce_only flag (not supported on Aster for exits)
            venue_name: Short venue identifier ("HL", "AS", "LT")

        Returns:
            Dict with keys: filled, avg_price, is_maker, status, maker_partial
        """
        # Determine venue name if not provided
        if not venue_name:
            venue_name = getattr(venue, "__class__", type(venue)).__name__[:2].upper()

        # Step 1: Check if maker mode is viable
        if not self._maker_order_enabled or bbo[0] <= 0 or bbo[1] <= 0:
            # Maker mode disabled or no valid BBO - use market IOC directly
            result = await venue.place_order(symbol, side, qty, ioc=True, reduce_only=reduce_only)
            return {
                "filled": float(result.get("filled", 0.0)),
                "avg_price": float(result.get("avgPx", result.get("avg_price", 0.0)) or 0.0),
                "is_maker": False,
                "status": result.get("status", "filled"),
                "maker_partial": 0.0
            }

        action = "ENTRY" if is_entry else "EXIT"

        # FIX 2026-01-22: Reposting loop for 80% maker target
        # Divide total timeout into multiple attempts with aggressive repricing
        MAX_ATTEMPTS = 5
        attempt_timeout = max(timeout_s / MAX_ATTEMPTS, 10.0)  # At least 10s per attempt

        total_filled = 0.0
        total_value = 0.0  # For weighted avg price calculation
        remaining_qty = qty
        order_id = None  # Track order ID for final cleanup

        for attempt in range(MAX_ATTEMPTS):
            if remaining_qty <= qty * 0.01:  # Less than 1% remaining
                break

            # Get fresh BBO for each attempt (except first which uses passed bbo)
            if attempt > 0:
                fresh_bbo = venue.get_bbo(symbol)
                if fresh_bbo and fresh_bbo[0] > 0 and fresh_bbo[1] > 0:
                    bbo = fresh_bbo

            mid_px = (bbo[0] + bbo[1]) / 2
            spread_bps = ((bbo[1] - bbo[0]) / mid_px) * 10000

            # FIX 2026-01-24: Use mid price for faster fills
            # Previous approach started inside spread and eroded toward mid over attempts
            # New approach: start at mid immediately for quicker execution
            # This still potentially earns maker rebate if price moves toward our order
            limit_px = mid_px

            log.info(f"[CARRY] MAKER {action} {venue_name} [attempt {attempt+1}/{MAX_ATTEMPTS}]: "
                    f"GTC {side} {remaining_qty:.6f} @ mid=${limit_px:.4f} "
                    f"(bid=${bbo[0]:.4f}, ask=${bbo[1]:.4f}, spread={spread_bps:.1f}bps)")

            try:
                # Post GTC limit order
                result = await venue.place_order(symbol, side, remaining_qty, price=limit_px, ioc=False, reduce_only=reduce_only)
                order_id = result.get("order_id") or result.get("oid")
                initial_filled = float(result.get("filled", 0.0))
                fill_price = float(result.get("avgPx", result.get("avg_price", 0.0)) or limit_px)

                # FIX 2026-01-25: Track GTC orders for later cleanup if needed
                if order_id and not result.get("dry_run"):
                    self._track_order(str(order_id), symbol, venue, venue_name, side)

                if initial_filled > 0:
                    total_filled += initial_filled
                    total_value += initial_filled * fill_price
                    remaining_qty -= initial_filled

                if remaining_qty <= qty * 0.01:  # Done!
                    log.info(f"[CARRY] MAKER {action} {venue_name}: Immediate fill! qty={total_filled:.6f}")
                    # FIX 2026-01-25: Untrack order on successful fill
                    if order_id:
                        self._untrack_order(str(order_id))
                    return {
                        "filled": total_filled,
                        "avg_price": total_value / total_filled if total_filled > 0 else fill_price,
                        "is_maker": True,
                        "status": "filled",
                        "maker_partial": total_filled
                    }

                # Wait for fill using venue-specific method
                if order_id and hasattr(venue, "wait_for_fill"):
                    if venue_name == "LT":
                        fill_result = await venue.wait_for_fill(
                            symbol=symbol, order_id=order_id, expected_qty=remaining_qty + initial_filled,
                            side=side, timeout_s=attempt_timeout, poll_interval=2.0
                        )
                    elif venue_name == "AS":
                        fill_result = await venue.wait_for_fill(
                            symbol=symbol, order_id=order_id, expected_qty=remaining_qty + initial_filled,
                            timeout_s=attempt_timeout, poll_interval=2.0
                        )
                    else:  # HL
                        fill_result = await venue.wait_for_fill(
                            order_id=order_id, symbol=symbol, expected_qty=remaining_qty + initial_filled,
                            timeout_s=attempt_timeout, poll_interval=2.0
                        )

                    attempt_filled = float(fill_result.get("filled", 0.0)) - initial_filled
                    if attempt_filled > 0:
                        fill_price = float(fill_result.get("avg_price", 0.0) or limit_px)
                        total_filled += attempt_filled
                        total_value += attempt_filled * fill_price
                        remaining_qty -= attempt_filled

                    if fill_result.get("status") == "filled" or remaining_qty <= qty * 0.01:
                        log.info(f"[CARRY] MAKER {action} {venue_name}: Filled! qty={total_filled:.6f}")
                        # FIX 2026-01-25: Untrack order on successful fill
                        if order_id:
                            self._untrack_order(str(order_id))
                        return {
                            "filled": total_filled,
                            "avg_price": total_value / total_filled if total_filled > 0 else fill_price,
                            "is_maker": True,
                            "status": "filled",
                            "maker_partial": total_filled
                        }

                # FIX 2026-01-22 Bug #6: Query final order status before cancel to avoid missing fills
                # Cancel unfilled order before reposting
                if hasattr(venue, "cancel_order") and order_id:
                    try:
                        # Query final status first - order may have filled between last poll and now
                        if hasattr(venue, "get_order_status"):
                            final_status = await venue.get_order_status(order_id)
                            if isinstance(final_status, dict):
                                final_filled = float(final_status.get("filled", final_status.get("sz", 0)) or 0)
                                if final_filled > total_filled:
                                    # Additional fill happened! Capture it
                                    extra_fill = final_filled - total_filled
                                    final_price = float(final_status.get("avgPx", final_status.get("avg_price", 0)) or limit_px)
                                    total_filled = final_filled
                                    total_value += extra_fill * final_price
                                    remaining_qty -= extra_fill
                                    log.info(f"[CARRY] MAKER {action} {venue_name}: Caught late fill! extra={extra_fill:.6f}, total={total_filled:.6f}")
                                    if remaining_qty <= qty * 0.01:
                                        # Fully filled now, no need to cancel
                                        # FIX 2026-01-25: Untrack order on successful fill
                                        self._untrack_order(str(order_id))
                                        return {
                                            "filled": total_filled,
                                            "avg_price": total_value / total_filled if total_filled > 0 else final_price,
                                            "is_maker": True,
                                            "status": "filled",
                                            "maker_partial": total_filled
                                        }
                        # Still has remaining qty - cancel the order
                        await venue.cancel_order(order_id, symbol)
                        # FIX 2026-01-25: Untrack order after cancel
                        self._untrack_order(str(order_id))
                    except Exception as cancel_e:
                        # FIX 2026-01-25: Upgraded from DEBUG to WARNING - cancel failures may orphan orders
                        log.warning(f"[CARRY] Cancel order {order_id} failed (may orphan): {cancel_e}")

                log.info(f"[CARRY] MAKER {action} {venue_name}: Attempt {attempt+1} partial: "
                        f"filled={total_filled:.6f}, remaining={remaining_qty:.6f}")

                # FIX 2026-01-24 Enhancement #5: Switch to IOC immediately after ANY partial fill
                # When enabled, partial fills trigger immediate IOC for remainder instead of reposting
                if self._partial_fill_ioc_immediately and total_filled > 0 and remaining_qty > qty * 0.01:
                    log.info(f"[CARRY] MAKER {action} {venue_name}: Partial fill detected ({total_filled:.6f}) - "
                            "switching to IOC immediately (partial_fill_ioc_immediately=true)")
                    break  # Exit reposting loop, go to IOC fallback

            except Exception as e:
                log.warning(f"[CARRY] MAKER {action} {venue_name} attempt {attempt+1} exception: {e}")
                # Try to cancel if order was placed
                if order_id and hasattr(venue, "cancel_order"):
                    try:
                        await venue.cancel_order(order_id, symbol)
                        # FIX 2026-01-25: Untrack order after cancel
                        self._untrack_order(str(order_id))
                    except Exception:
                        pass

        # FIX 2026-01-22: Cancel any remaining order from last attempt BEFORE IOC fallback
        # This is critical - without this, the last GTC order sits on the order book indefinitely
        if order_id and remaining_qty > qty * 0.01:
            try:
                log.info(f"[CARRY] MAKER {action} {venue_name}: Cancelling final unfilled order {order_id}")
                if hasattr(venue, "cancel_order"):
                    await venue.cancel_order(order_id, symbol)
                    # FIX 2026-01-25: Untrack order after cancel
                    self._untrack_order(str(order_id))
            except Exception as e:
                log.error(f"[CARRY] CRITICAL: Failed to cancel final order {order_id}: {e}")
                # Even if cancel fails, continue with IOC - order may have already been filled/cancelled

        # Reposting loop exhausted - use IOC for remainder
        filled = total_filled
        avg_price = total_value / total_filled if total_filled > 0 else 0.0
        log.info(f"[CARRY] MAKER {action} {venue_name}: Repost loop done, maker_filled={filled:.6f}, using IOC for remainder")

        # Final IOC fallback for any remaining quantity
        if remaining_qty > qty * 0.001:  # More than 0.1% remaining
            try:
                log.info(f"[CARRY] MAKER {action} {venue_name}: Final IOC fallback for remaining {remaining_qty:.6f}")
                market_result = await venue.place_order(symbol, side, remaining_qty, ioc=True, reduce_only=reduce_only)
                market_filled = float(market_result.get("filled", 0.0))
                market_price = float(market_result.get("avgPx", market_result.get("avg_price", 0.0)) or 0.0)

                if market_filled > 0:
                    total_filled += market_filled
                    if market_price > 0:
                        total_value += market_filled * market_price
                        avg_price = total_value / total_filled
            except Exception as e:
                log.warning(f"[CARRY] MAKER {action} {venue_name} IOC fallback failed: {e}")

        # Return final result
        maker_ratio = (total_filled - remaining_qty) / qty if qty > 0 else 0
        return {
            "filled": total_filled,
            "avg_price": avg_price if avg_price > 0 else 0.0,
            "is_maker": maker_ratio >= 0.8,  # Consider "maker" if 80%+ filled as maker
            "status": "filled" if total_filled >= qty * 0.95 else "partial",
            "maker_partial": total_filled - (market_filled if 'market_filled' in dir() else 0)
        }

    def _check_liquidation_risk(self, pos: CarryPosition) -> Tuple[bool, float]:
        """Check if position is too close to liquidation.

        P0 FIX: Liquidation proximity monitoring for leveraged positions.

        Returns (is_at_risk, distance_bps) where:
        - is_at_risk: True if within 15% of liquidation
        - distance_bps: Distance to liquidation in basis points
        """
        # For 2x leverage, liquidation at ~50% from entry (margin = 50%)
        # For 3x leverage, liquidation at ~33% from entry (margin = 33%)
        # Use 2x as default per Fix 3
        leverage = 2
        liq_buffer = 1.0 / leverage  # 0.50 for 2x, 0.33 for 3x

        # Get current HL price (HL is always the primary leg for carry)
        bbo_hl = self.hl.get_bbo(pos.hl_coin)
        if not bbo_hl:
            return False, 0.0  # Can't check without price

        current_px = (bbo_hl[0] + bbo_hl[1]) / 2
        entry_px = pos.entry_px_hl

        if entry_px <= 0:
            return False, 0.0

        # Check direction and calculate liquidation distance
        is_long = pos.direction.lower().startswith("long")
        if is_long:
            # Long position: liquidation when price drops by liq_buffer
            liq_px = entry_px * (1 - liq_buffer)
            if liq_px <= 0:
                return False, 0.0
            distance = (current_px - liq_px) / liq_px
        else:
            # Short position: liquidation when price rises by liq_buffer
            liq_px = entry_px * (1 + liq_buffer)
            distance = (liq_px - current_px) / liq_px

        distance_bps = distance * 10000
        is_at_risk = distance_bps < 1500  # Within 15% of liquidation

        return is_at_risk, distance_bps

    def _get_minutes_until_funding(self) -> float:
        """Returns minutes until next funding payment (00:00, 08:00, 16:00 UTC)."""
        from datetime import datetime, timezone
        now_utc = datetime.now(timezone.utc)
        hour = now_utc.hour
        minute = now_utc.minute

        # Next funding cycle hours: 0, 8, 16
        next_cycle_hour = ((hour // 8) + 1) * 8
        if next_cycle_hour >= 24:
            next_cycle_hour = 0
            # Add remaining hours until midnight + cycle hour
            hours_until = (24 - hour) + next_cycle_hour
        else:
            hours_until = next_cycle_hour - hour

        minutes_until = hours_until * 60 - minute
        return minutes_until

    # =========================================================================
    # SECTION 4: MAIN LOOP
    # =========================================================================

    async def tick(self) -> None:
        """
        Check for opportunities and manage positions.
        """
        if not self.enabled:
            return

        now = time.time()
        
        # Phase 3: Heartbeat Logging (Hourly)
        if (now - self._last_heartbeat) > 3600.0:
            self._last_heartbeat = now
            self._log_heartbeat()

        # Fetch rates once per cycle if needed
        hl_rates = {}
        as_rates = {}
        lt_rates = {}
        
        need_rates = False
        time_for_mtm = (now - self._last_mtm_check) >= self.mtm_check_interval
        time_for_entries = (now - self._last_funding_check) >= self._funding_check_interval

        # Only check rates if we have to manage positions (MTM check) OR check entries
        if (self.positions and time_for_mtm) or time_for_entries:
             need_rates = True
             
        if need_rates:
            try:
                # Fetch rates and cache on success
                fetched_hl = await self.hl.get_funding_rates()
                if fetched_hl:
                    self._cached_hl_rates = fetched_hl
                    hl_rates = fetched_hl
                else:
                    hl_rates = self._cached_hl_rates  # Use cached on failure
                    if hl_rates:
                        log.debug("[CARRY] HL rate fetch returned empty, using cached rates")

                fetched_as = await self.asr.get_funding_rates()
                if fetched_as:
                    self._cached_as_rates = fetched_as
                    as_rates = fetched_as
                else:
                    as_rates = self._cached_as_rates  # Use cached on failure
                    if as_rates:
                        log.debug("[CARRY] AS rate fetch returned empty, using cached rates")

                if self.lighter:
                    fetched_lt = await self.lighter.get_funding_rates()
                    if fetched_lt:
                        self._cached_lt_rates = fetched_lt
                        lt_rates = fetched_lt
                    else:
                        lt_rates = self._cached_lt_rates  # Use cached on failure
                        if lt_rates:
                            log.debug("[CARRY] LT rate fetch returned empty, using cached rates")
            except Exception as e:
                log.warning(f"[CARRY] Rate fetch error: {e}, using cached rates")
                # Use cached rates on exception
                hl_rates = self._cached_hl_rates
                as_rates = self._cached_as_rates
                lt_rates = self._cached_lt_rates
        
        # 1. Manage Exits and MTM (Periodic)
        if (now - self._last_mtm_check) >= self.mtm_check_interval:
            self._last_mtm_check = now
            await self._manage_positions(hl_rates, as_rates, lt_rates)
            # Yield rebalancing: scale down low-yield to scale up high-yield
            await self._yield_rebalance()
            # FIX 2026-01-25: Proactive replacement when capital exhausted
            await self._proactive_opportunity_replacement()

        # 2. Check Entries
        if (now - self._last_funding_check) >= self._funding_check_interval:
            self._last_funding_check = now
            if hl_rates and as_rates:
                await self._check_entries(hl_rates, as_rates, lt_rates)

        # 3. Periodic Reconciliation (every 30 seconds)
        # Ensures internal state matches actual venue positions
        if (now - self._last_reconciliation) >= self._reconciliation_interval:
            self._last_reconciliation = now
            if self.positions and not self.paper_mode:
                await self._reconcile_state_with_venues()

        # 3.5 FIX 2026-01-22: Periodic Order Cleanup (every 60 seconds)
        # Cancels any stale open orders left on venue order books
        if (now - self._last_order_cleanup) >= self._order_cleanup_interval:
            self._last_order_cleanup = now
            if not self.paper_mode:
                await self._cleanup_open_orders()

        # 3.6 FIX 2026-01-25: Periodic Stress Mode Detection (every 60 seconds)
        # In stress mode, reduce position sizes to minimize risk
        if (now - self._last_stress_check) >= self._stress_check_interval:
            self._last_stress_check = now
            prev_stress = self._stress_mode_active
            self._stress_mode_active = await self._detect_stress_mode()
            if self._stress_mode_active and not prev_stress:
                log.warning("[CARRY] STRESS MODE ACTIVATED: Reducing position sizes")
            elif prev_stress and not self._stress_mode_active:
                log.info("[CARRY] Stress mode deactivated: Normal sizing resumed")

        # 4. Position Health Logging (every 5 minutes)
        if (now - self._last_health_log) >= self._health_log_interval:
            self._last_health_log = now
            if self.positions:
                await self._log_position_health()

    async def _log_position_health(self) -> None:
        """Log comprehensive health summary of all open positions every 5 minutes."""
        if not self.positions:
            return

        now = time.time()
        total_size_usd = 0.0
        total_mtm_usd = 0.0
        total_funding_usd = 0.0
        total_price_pnl_usd = 0.0
        alerts = []

        log.info("[CARRY] ========== POSITION HEALTH REPORT ==========")
        # FIX 2026-01-22: Use list() to prevent RuntimeError if positions modified during iteration
        for sym, pos in list(self.positions.items()):
            mtm = self._calc_mtm(pos)
            hold_hours = (now - pos.entry_time) / 3600.0
            mtm_bps = mtm.get("mtm_bps", 0.0)
            price_pnl = mtm.get("price_pnl_usd", 0.0)
            funding_pnl = mtm.get("funding_pnl_usd", 0.0)
            total_pnl = mtm.get("total_mtm_usd", 0.0)

            total_size_usd += pos.size_usd
            total_mtm_usd += total_pnl
            total_funding_usd += funding_pnl
            total_price_pnl_usd += price_pnl

            # MTM Swing Detection (>50 bps change in <5 min)
            prev_mtm = self._last_mtm_by_symbol.get(sym, mtm_bps)
            mtm_swing = abs(mtm_bps - prev_mtm)
            self._last_mtm_by_symbol[sym] = mtm_bps

            swing_alert = ""
            if mtm_swing > 50.0:
                swing_alert = f" SWING:{mtm_swing:.1f}bps"
                alerts.append(f"{pos.hl_coin}: {mtm_swing:.1f}bps swing")

            # FIX 2026-01-22: Wrap health report logging in try-catch to prevent crashes
            try:
                log.info(f"  {pos.hl_coin} ({pos.direction}): Size=${pos.size_usd:.2f} Hold={hold_hours:.1f}h "
                        f"MTM={mtm_bps:.1f}bps Price=${price_pnl:.2f} Funding=${funding_pnl:.2f} "
                        f"Total=${total_pnl:.2f} Peak={pos.max_mtm_bps:.1f}bps Scale={pos.scale_up_count}{swing_alert}")
            except Exception as log_err:
                log.error(f"[CARRY] Failed to log position {pos.hl_coin}: {type(log_err).__name__}")

        log.info(f"  TOTAL: Positions={len(self.positions)} Size=${total_size_usd:.2f} "
                f"MTM=${total_mtm_usd:.2f} (Price=${total_price_pnl_usd:.2f} Funding=${total_funding_usd:.2f})")
        log.info("[CARRY] ================================================")

        # Send alert notification for large MTM swings
        if alerts and self.notifier:
            asyncio.create_task(self.notifier.notify(
                "MTM SWING ALERT",
                f"Large price movements detected:\n" + "\n".join(alerts)
            ))

    async def _reconcile_state_with_venues(self) -> None:
        """
        Periodic reconciliation: Query all venues and verify internal state matches reality.
        Removes positions from internal state that don't exist on venues.
        This catches cases where orders failed but internal state was incorrectly updated.
        """
        log.info("[CARRY] RECONCILIATION: Checking internal state vs venue positions...")

        try:
            # Gather all venue positions in parallel
            tasks = [self.hl.get_positions(), self.asr.get_positions()]
            if self.lighter:
                tasks.append(self.lighter.get_positions())

            results = await asyncio.gather(*tasks, return_exceptions=True)

            hl_positions = results[0] if not isinstance(results[0], Exception) else []
            as_positions = results[1] if not isinstance(results[1], Exception) else []
            lt_positions = results[2] if len(results) > 2 and not isinstance(results[2], Exception) else []

            # Normalize venue positions into dicts: symbol -> {size_coins, size_usd, price}
            # FIX 2026-01-22 Bug #3: Track sizes, not just existence
            hl_symbols = set()
            as_symbols = set()
            lt_symbols = set()
            hl_sizes: Dict[str, float] = {}  # coin -> size in USD
            as_sizes: Dict[str, float] = {}
            lt_sizes: Dict[str, float] = {}

            # FIX 2026-01-22: Use size_usd directly from venues (not size * price calculation)
            # Each venue now returns accurate USD values from their API:
            # - Aster: notional field
            # - Hyperliquid: positionValue field
            # - Lighter: position_value field

            for pos in (hl_positions or []):
                sym = pos.get('coin', pos.get('symbol', '')).upper()
                size_coins = abs(float(pos.get('size', pos.get('positionAmt', 0))))
                if sym and size_coins > 0:
                    hl_symbols.add(sym)
                    hl_symbols.add(sym + "USDT")
                    hl_symbols.add(sym.replace("USDT", ""))
                    # FIX 2026-01-22: Use size_usd from venue (positionValue) instead of calculating
                    size_usd = float(pos.get('size_usd', 0))
                    if size_usd <= 0:
                        # Fallback to calculation if size_usd not provided
                        entry_px = float(pos.get('entry', pos.get('entryPx', 0)) or 0)
                        size_usd = abs(size_coins * entry_px) if entry_px > 0 else 0
                    base_sym = sym.replace("USDT", "")
                    hl_sizes[base_sym] = hl_sizes.get(base_sym, 0) + size_usd
                    log.debug(f"[RECONCILE] HL {base_sym}: size_usd=${size_usd:.2f} (coins={size_coins:.6f})")

            for pos in (as_positions or []):
                sym = pos.get('symbol', pos.get('coin', '')).upper()
                size_coins = abs(float(pos.get('size', pos.get('positionAmt', 0))))
                leverage = int(pos.get('leverage', 1) or 1)
                if sym and size_coins > 0:
                    as_symbols.add(sym)
                    as_symbols.add(sym.replace("USDT", ""))
                    # FIX 2026-01-22: Use size_usd from venue (notional) instead of calculating
                    size_usd = float(pos.get('size_usd', 0))
                    if size_usd <= 0:
                        # Fallback to calculation if size_usd not provided
                        entry_px = float(pos.get('entry', pos.get('entryPrice', 0)) or 0)
                        size_usd = abs(size_coins * entry_px) if entry_px > 0 else 0
                    base_sym = sym.replace("USDT", "")
                    as_sizes[base_sym] = as_sizes.get(base_sym, 0) + size_usd
                    log.debug(f"[RECONCILE] AS {base_sym}: size_usd=${size_usd:.2f} (coins={size_coins:.6f}, lev={leverage}x)")

            for pos in (lt_positions or []):
                sym = pos.get('symbol', pos.get('coin', '')).upper()
                size_coins = abs(float(pos.get('size', pos.get('positionAmt', 0))))
                if sym and size_coins > 0:
                    lt_symbols.add(sym)
                    lt_symbols.add(sym + "USDT")
                    lt_symbols.add(sym.replace("USDT", ""))
                    # FIX 2026-01-22: Use size_usd from venue (position_value) instead of calculating
                    size_usd = float(pos.get('size_usd', 0))
                    if size_usd <= 0:
                        # Fallback to calculation if size_usd not provided
                        entry_px = float(pos.get('entry', 0) or 0)
                        size_usd = abs(size_coins * entry_px) if entry_px > 0 else 0
                    base_sym = sym.replace("USDT", "").replace("/USDC", "").replace("-PERP", "")
                    lt_sizes[base_sym] = lt_sizes.get(base_sym, 0) + size_usd
                    log.debug(f"[RECONCILE] LT {base_sym}: size_usd=${size_usd:.2f} (coins={size_coins:.6f})")

            # Check each internal position
            positions_to_remove = []
            for symbol, pos in list(self.positions.items()):
                hl_coin = pos.hl_coin.upper()
                direction = pos.direction

                # Determine which venues should have positions
                # Direction format: "Long HL / Short AS" or "Short AS / Long LT" etc.
                has_hl = "HL" in direction
                has_as = "AS" in direction
                has_lt = "LT" in direction

                # Check if positions exist
                v1_ok = True
                v2_ok = True

                if has_hl:
                    if hl_coin not in hl_symbols and hl_coin + "USDT" not in hl_symbols:
                        v1_ok = False
                        log.warning(f"[RECONCILE] {hl_coin}: MISSING on HL (expected from direction: {direction})")

                if has_as:
                    if hl_coin + "USDT" not in as_symbols and hl_coin not in as_symbols:
                        v2_ok = False
                        log.warning(f"[RECONCILE] {hl_coin}: MISSING on AS (expected from direction: {direction})")

                if has_lt:
                    if hl_coin not in lt_symbols and hl_coin + "USDT" not in lt_symbols:
                        if has_hl:
                            v2_ok = False
                        else:
                            v1_ok = False
                        log.warning(f"[RECONCILE] {hl_coin}: MISSING on LT (expected from direction: {direction})")

                if not v1_ok and not v2_ok:
                    # FIX 2026-01-24: Require multiple consecutive "missing" checks before removing
                    # This prevents removing positions due to single API failures (caused 0G duplicate entry bug)
                    self._reconcile_missing_count[symbol] = self._reconcile_missing_count.get(symbol, 0) + 1
                    missing_count = self._reconcile_missing_count[symbol]

                    if missing_count >= self._reconcile_min_missing_before_remove:
                        log.error(f"[RECONCILE] {hl_coin}: BOTH LEGS MISSING for {missing_count} consecutive checks! Removing from internal state.")
                        positions_to_remove.append(symbol)
                        if self.notifier:
                            await self.notifier.notify(
                                f"RECONCILE: {hl_coin}",
                                f"Both legs MISSING on venues for {missing_count} checks! Removing from tracking. "
                                f"Internal had: ${pos.size_usd:.2f}, scale_up={pos.scale_up_count}"
                            )
                    else:
                        log.warning(f"[RECONCILE] {hl_coin}: BOTH LEGS MISSING (check {missing_count}/{self._reconcile_min_missing_before_remove}). "
                                   f"Will remove after {self._reconcile_min_missing_before_remove - missing_count} more consecutive checks.")
                elif not v1_ok or not v2_ok:
                    log.error(f"[RECONCILE] {hl_coin}: ONE LEG MISSING! UNHEDGED RISK - marking for orphan recovery.")
                    # FIX 2026-01-22 Bug #9: Mark position for orphan recovery by setting size_usd = 0
                    # This triggers _check_orphans to close the remaining leg on next tick
                    pos.size_usd = 0
                    self._save_state()
                    log.info(f"[RECONCILE] {hl_coin}: Set size_usd=0 to trigger orphan recovery on next tick")
                    if self.notifier:
                        await self.notifier.notify(
                            f"RECONCILE ALERT: {hl_coin}",
                            f"ONE LEG MISSING - UNHEDGED! Direction: {direction}. "
                            f"HL: {has_hl and v1_ok}, AS: {has_as and v2_ok}, LT: {has_lt and (v2_ok if has_hl else v1_ok)}. "
                            f"Orphan recovery will attempt to close remaining leg."
                        )
                else:
                    # FIX 2026-01-24: Reset missing counter when position IS found
                    if symbol in self._reconcile_missing_count:
                        del self._reconcile_missing_count[symbol]

                    # FIX 2026-01-22 Bug #3: Check SIZE mismatch, not just existence
                    # Compare internal size_usd vs venue sizes
                    SIZE_MISMATCH_THRESHOLD = 0.10  # 10% tolerance
                    internal_size = pos.size_usd

                    # Get venue sizes for this coin
                    v1_venue_size = 0.0
                    v2_venue_size = 0.0
                    parts = direction.split(" / ")
                    v1_name = parts[0].split()[-1].upper() if len(parts) >= 2 else ""
                    v2_name = parts[1].split()[-1].upper() if len(parts) >= 2 else ""

                    if v1_name == "HL":
                        v1_venue_size = hl_sizes.get(hl_coin, 0.0)
                    elif v1_name == "AS":
                        v1_venue_size = as_sizes.get(hl_coin, 0.0)
                    elif v1_name == "LT":
                        v1_venue_size = lt_sizes.get(hl_coin, 0.0)

                    if v2_name == "HL":
                        v2_venue_size = hl_sizes.get(hl_coin, 0.0)
                    elif v2_name == "AS":
                        v2_venue_size = as_sizes.get(hl_coin, 0.0)
                    elif v2_name == "LT":
                        v2_venue_size = lt_sizes.get(hl_coin, 0.0)

                    total_venue_size = v1_venue_size + v2_venue_size

                    # FIX 2026-01-24: Check for VENUE leg divergence (actual positions on exchanges)
                    # This catches scenarios like DOLO having different sizes on AS vs LT
                    LEG_DIVERGENCE_THRESHOLD_PCT = 15.0  # 15% threshold for remediation
                    LEG_MIN_DIVERGENCE_USD = 10.0  # Minimum divergence in USD to care about
                    if v1_venue_size > 0 and v2_venue_size > 0:
                        leg_max = max(v1_venue_size, v2_venue_size)
                        leg_min = min(v1_venue_size, v2_venue_size)
                        venue_leg_diff_pct = (leg_max - leg_min) / leg_max * 100
                        venue_leg_diff_usd = leg_max - leg_min

                        if venue_leg_diff_pct > LEG_DIVERGENCE_THRESHOLD_PCT and venue_leg_diff_usd > LEG_MIN_DIVERGENCE_USD:
                            log.error(f"[RECONCILE] VENUE LEG DIVERGENCE {hl_coin}: "
                                     f"V1({v1_name})=${v1_venue_size:.2f} V2({v2_name})=${v2_venue_size:.2f} "
                                     f"({venue_leg_diff_pct:.1f}% / ${venue_leg_diff_usd:.2f} divergence)")

                            # Determine which leg is larger and should be reduced
                            larger_venue = v1_name if v1_venue_size > v2_venue_size else v2_name
                            smaller_venue = v2_name if larger_venue == v1_name else v1_name
                            reduce_usd = venue_leg_diff_usd

                            # AUTO-REMEDIATION: Reduce larger leg to match smaller
                            if self.notifier:
                                await self.notifier.notify(
                                    f"LEG DIVERGENCE: {hl_coin}",
                                    f"Unhedged exposure detected!\n"
                                    f"V1({v1_name}): ${v1_venue_size:.2f}\n"
                                    f"V2({v2_name}): ${v2_venue_size:.2f}\n"
                                    f"Diff: {venue_leg_diff_pct:.1f}% (${venue_leg_diff_usd:.2f})\n"
                                    f"Attempting auto-remediation: reduce {larger_venue} by ${reduce_usd:.2f}"
                                )

                            # Execute remediation (reduce larger leg)
                            try:
                                await self._remediate_leg_divergence(
                                    hl_coin, symbol, pos, larger_venue, smaller_venue,
                                    v1_venue_size, v2_venue_size, reduce_usd
                                )
                            except Exception as rem_err:
                                log.error(f"[RECONCILE] Remediation failed for {hl_coin}: {rem_err}")

                    if internal_size > 10.0 and total_venue_size > 0:  # Only check if meaningful size
                        size_diff_pct = abs(internal_size - total_venue_size) / max(internal_size, total_venue_size)

                        if size_diff_pct > SIZE_MISMATCH_THRESHOLD:
                            log.error(f"[RECONCILE] SIZE MISMATCH {hl_coin}: Internal=${internal_size:.2f} vs Venues=${total_venue_size:.2f} "
                                     f"(V1={v1_name}:${v1_venue_size:.2f}, V2={v2_name}:${v2_venue_size:.2f}) "
                                     f"Diff={size_diff_pct*100:.1f}%")
                            if self.notifier:
                                await self.notifier.notify(
                                    f"SIZE MISMATCH: {hl_coin}",
                                    f"Internal=${internal_size:.2f} Venues=${total_venue_size:.2f} ({size_diff_pct*100:.1f}% diff)\n"
                                    f"V1({v1_name})=${v1_venue_size:.2f} V2({v2_name})=${v2_venue_size:.2f}\n"
                                    f"Check for partial fills or orphaned positions!"
                                )
                        else:
                            log.debug(f"[RECONCILE] {hl_coin}: OK - both legs confirmed, size match OK (internal=${internal_size:.2f} venue=${total_venue_size:.2f})")

                    # FIX 2026-01-24: Update per-leg sizes from actual venue positions
                    # This ensures dashboard shows CURRENT position sizes, not just entry-time values
                    if v1_venue_size > 0 and v2_venue_size > 0:
                        old_v1 = pos.size_v1_usd
                        old_v2 = pos.size_v2_usd
                        pos.size_v1_usd = v1_venue_size
                        pos.size_v2_usd = v2_venue_size
                        pos.size_usd = v1_venue_size + v2_venue_size
                        # Only log if sizes changed significantly (>$1)
                        if abs(old_v1 - v1_venue_size) > 1.0 or abs(old_v2 - v2_venue_size) > 1.0:
                            log.info(f"[RECONCILE] {hl_coin}: Updated sizes from venues - "
                                    f"V1: ${old_v1:.2f} -> ${v1_venue_size:.2f}, "
                                    f"V2: ${old_v2:.2f} -> ${v2_venue_size:.2f}")
                            self._save_state()

                    # FIX 2026-01-22: Check per-leg size mismatch (internal tracking)
                    # This catches situations where bot has tracked V1 and V2 fills differently
                    if hasattr(pos, 'size_v1_usd') and hasattr(pos, 'size_v2_usd'):
                        leg_max = max(pos.size_v1_usd, pos.size_v2_usd, 1)
                        leg_diff_pct = abs(pos.size_v1_usd - pos.size_v2_usd) / leg_max * 100
                        if leg_diff_pct > 10.0:
                            log.error(f"[RECONCILE] LEG SIZE MISMATCH {hl_coin}: "
                                     f"V1=${pos.size_v1_usd:.2f} V2=${pos.size_v2_usd:.2f} ({leg_diff_pct:.1f}% diff)")
                            log.error(f"[RECONCILE] Position has UNHEDGED EXPOSURE - consider manual intervention!")
                            if self.notifier:
                                await self.notifier.notify(
                                    f"LEG MISMATCH: {hl_coin}",
                                    f"Unhedged exposure detected!\n"
                                    f"V1: ${pos.size_v1_usd:.2f}\n"
                                    f"V2: ${pos.size_v2_usd:.2f}\n"
                                    f"Diff: {leg_diff_pct:.1f}%\n"
                                    f"Consider manual rebalancing!"
                                )
                    else:
                        log.debug(f"[RECONCILE] {hl_coin}: OK - both legs confirmed on venues")

            # Remove positions that don't exist on venues
            if positions_to_remove:
                log.warning(f"[RECONCILE] Removing {len(positions_to_remove)} stale positions: {positions_to_remove}")
                for sym in positions_to_remove:
                    del self.positions[sym]
                    # FIX 2026-01-24: Clean up missing counter
                    if sym in self._reconcile_missing_count:
                        del self._reconcile_missing_count[sym]
                self._save_state()

            # CRITICAL FIX: Detect extra positions on venues that we DON'T track (orphans)
            # These are positions that exist on venues but the bot doesn't know about
            tracked_hl_coins = set()
            tracked_as_symbols = set()
            tracked_lt_symbols = set()

            # FIX 2026-01-22: Use list() to prevent RuntimeError if positions modified during iteration
            for sym, pos in list(self.positions.items()):
                hl_coin = pos.hl_coin.upper()
                direction = pos.direction

                if "HL" in direction:
                    tracked_hl_coins.add(hl_coin)
                    tracked_hl_coins.add(hl_coin + "USDT")
                if "AS" in direction:
                    tracked_as_symbols.add(hl_coin + "USDT")
                    tracked_as_symbols.add(hl_coin)
                if "LT" in direction:
                    tracked_lt_symbols.add(hl_coin)
                    tracked_lt_symbols.add(hl_coin + "USDT")

            # Find EXTRA positions on venues (orphans we don't track)
            extra_hl = hl_symbols - tracked_hl_coins
            extra_as = as_symbols - tracked_as_symbols
            extra_lt = lt_symbols - tracked_lt_symbols

            # Remove duplicates like "BTC" vs "BTCUSDT"
            def dedupe_symbols(s: set) -> set:
                result = set()
                for sym in s:
                    base = sym.replace("USDT", "").replace("-PERP", "")
                    result.add(base)
                return result

            extra_hl = dedupe_symbols(extra_hl)
            extra_as = dedupe_symbols(extra_as)
            extra_lt = dedupe_symbols(extra_lt)

            if extra_hl or extra_as or extra_lt:
                log.error("=" * 60)
                log.error("[!!!] ORPHAN POSITIONS DETECTED - NOT TRACKED BY BOT [!!!]")
                if extra_hl:
                    log.error(f"  HL has EXTRA positions: {extra_hl}")
                if extra_as:
                    log.error(f"  AS has EXTRA positions: {extra_as}")
                if extra_lt:
                    log.error(f"  LT has EXTRA positions: {extra_lt}")
                log.error("These positions are LOSING/GAINING money without bot awareness!")
                log.error("ACTION: Close them manually or add to bot tracking!")
                log.error("=" * 60)

                if self.notifier:
                    await self.notifier.notify(
                        "ORPHAN ALERT",
                        f"Untracked positions detected!\n"
                        f"HL: {extra_hl if extra_hl else 'none'}\n"
                        f"AS: {extra_as if extra_as else 'none'}\n"
                        f"LT: {extra_lt if extra_lt else 'none'}\n"
                        f"Manual intervention required!"
                    )

            # FIX 2026-01-25: Auto-close dust positions below $50 USD
            # Increased from $5 to catch maker exit residuals (typically $10-$200)
            DUST_THRESHOLD_USD = 50.0
            dust_closed = 0
            for pos_list, venue_obj, venue_name, sizes_dict in [
                (hl_positions, self.hl, "HL", hl_sizes),
                (as_positions, self.asr, "AS", as_sizes),
                (lt_positions, self.lighter if self.lighter else None, "LT", lt_sizes)
            ]:
                if not venue_obj or not pos_list:
                    continue
                for pos in pos_list:
                    sym = pos.get('symbol', pos.get('coin', '')).upper()
                    size_coins = abs(float(pos.get('size', pos.get('positionAmt', 0))))
                    size_usd = float(pos.get('size_usd', 0))
                    if size_usd <= 0:
                        entry_px = float(pos.get('entry', pos.get('entryPx', pos.get('entryPrice', 0))) or 0)
                        size_usd = abs(size_coins * entry_px) if entry_px > 0 else 0

                    if 0 < size_usd < DUST_THRESHOLD_USD and size_coins > 0:
                        # Determine side based on position direction
                        pos_side = pos.get('side', '')
                        if not pos_side:
                            # Infer from size sign
                            raw_size = float(pos.get('size', pos.get('positionAmt', 0)))
                            pos_side = "LONG" if raw_size > 0 else "SHORT"

                        # To close: sell if long, buy if short
                        close_side = "SELL" if pos_side.upper() in ("LONG", "BUY") else "BUY"

                        log.warning(f"[DUST CLEANUP] {venue_name} {sym}: ${size_usd:.2f} < ${DUST_THRESHOLD_USD:.0f} - closing with market order")
                        try:
                            # Use reduce_only for safety (except Aster which doesn't support it)
                            use_reduce_only = venue_name != "AS"
                            result = await venue_obj.place_order(sym, close_side, size_coins, ioc=True, reduce_only=use_reduce_only)
                            filled = float(result.get("filled", 0))
                            if filled > 0:
                                log.info(f"[DUST CLEANUP] {venue_name} {sym}: Closed {filled:.6f} coins (${size_usd:.2f})")
                                dust_closed += 1
                            else:
                                log.warning(f"[DUST CLEANUP] {venue_name} {sym}: Market order got 0 fill")
                        except Exception as e:
                            log.error(f"[DUST CLEANUP] {venue_name} {sym}: Failed to close - {e}")

            if dust_closed > 0:
                log.info(f"[DUST CLEANUP] Closed {dust_closed} dust position(s)")

            log.info(f"[CARRY] RECONCILIATION COMPLETE: {len(self.positions)} positions tracked, "
                    f"HL has {len(hl_symbols)} symbols, AS has {len(as_symbols)} symbols, LT has {len(lt_symbols)} symbols")

        except Exception as e:
            log.error(f"[CARRY] Reconciliation error: {e}")
            import traceback
            traceback.print_exc()

    async def _cleanup_open_orders(self) -> None:
        """
        FIX 2026-01-22: Cancel all stale open orders on all venues.

        This prevents orphaned limit orders from sitting on the order book
        when the bot has moved on (e.g., after maker order timeout/repost).

        Called:
        - Periodically every 60 seconds
        - After each trade (entry/exit)
        """
        if self.paper_mode:
            return  # No real orders in paper mode

        cancelled_total = 0
        errors = []

        # FIX 2026-01-25: Cancel tracked orders older than 120s
        # This uses the _active_orders dict populated by _track_order()
        try:
            stale_cancelled = await self._cancel_stale_tracked_orders(max_age_s=120.0)
            if stale_cancelled > 0:
                log.info(f"[CARRY] Cancelled {stale_cancelled} stale tracked orders (>120s old)")
                cancelled_total += stale_cancelled
        except Exception as e:
            log.warning(f"[CARRY] Stale tracked order cleanup error: {e}")

        try:
            # Aster cleanup
            if hasattr(self.asr, 'cancel_all_orders'):
                result = await self.asr.cancel_all_orders()
                cancelled = result.get("cancelled", 0)
                if cancelled:
                    log.warning(f"[CARRY] ORDER CLEANUP: Cancelled {cancelled} stale orders on Aster")
                    cancelled_total += cancelled if isinstance(cancelled, int) else 1
                if result.get("errors"):
                    errors.extend(result["errors"])

            # Hyperliquid cleanup (if it has the method)
            if hasattr(self.hl, 'cancel_all_orders'):
                result = await self.hl.cancel_all_orders()
                cancelled = result.get("cancelled", 0)
                if cancelled:
                    log.warning(f"[CARRY] ORDER CLEANUP: Cancelled {cancelled} stale orders on HL")
                    cancelled_total += cancelled if isinstance(cancelled, int) else 1

            # Lighter cleanup (if it has the method)
            if self.lighter and hasattr(self.lighter, 'cancel_all_orders'):
                result = await self.lighter.cancel_all_orders()
                cancelled = result.get("cancelled", 0)
                if cancelled:
                    log.warning(f"[CARRY] ORDER CLEANUP: Cancelled {cancelled} stale orders on Lighter")
                    cancelled_total += cancelled if isinstance(cancelled, int) else 1

            if cancelled_total > 0:
                log.warning(f"[CARRY] ORDER CLEANUP COMPLETE: Cancelled {cancelled_total} total stale orders")
                if self.notifier:
                    await self.notifier.notify(
                        "STALE ORDER CLEANUP",
                        f"Cancelled {cancelled_total} orphaned orders that were left on order books"
                    )

            if errors:
                log.warning(f"[CARRY] ORDER CLEANUP ERRORS: {errors}")

        except Exception as e:
            log.error(f"[CARRY] Order cleanup error: {e}")

    # =========================================================================
    # SECTION 5: POSITION MANAGEMENT
    # =========================================================================

    async def _manage_positions(self, hl_rates: Dict, as_rates: Dict, lt_rates: Dict) -> None:
        """
        Unified position management.
        """
        if not self.positions:
            return

        # Phase 4.1: Detect and recover orphan positions (size_usd = 0)
        await self._check_orphans()

        # FIX 2026-01-26: Fetch Aster funding intervals for proper rate normalization
        # Aster has variable intervals (1h, 4h, 8h) per symbol - must normalize to 8h basis
        as_intervals = await self.asr.get_funding_intervals() if hasattr(self.asr, 'get_funding_intervals') else {}

        now = time.time()
        to_close: List[Tuple[str, str, float, float]] = []

        # AUDIT FIX: Circuit breaker on stale data
        # For Carry we use a more permissive threshold (2000ms vs 500ms for Scalp)
        # because we're not doing instant arbitrage, just MTM monitoring
        MAX_CARRY_BBO_AGE_MS = 2000.0
        stale_venues: List[str] = []

        # Check HL freshness
        # FIX 2026-01-22: Use list() to prevent RuntimeError if positions modified during iteration
        if hasattr(self.hl, 'get_bbo_age_ms'):
            # Check age for any position's coin
            for sym, pos in list(self.positions.items()):
                hl_age = self.hl.get_bbo_age_ms(pos.hl_coin)
                if hl_age > MAX_CARRY_BBO_AGE_MS:
                    stale_venues.append(f"HL({pos.hl_coin}:{hl_age:.0f}ms)")
                    break

        # Check Aster freshness
        if hasattr(self.asr, 'get_bbo_age_ms'):
            for sym, pos in list(self.positions.items()):
                as_age = self.asr.get_bbo_age_ms(pos.symbol)
                if as_age > MAX_CARRY_BBO_AGE_MS:
                    stale_venues.append(f"AS({pos.symbol}:{as_age:.0f}ms)")
                    break

        # Check Lighter freshness if used
        if self.lighter and hasattr(self.lighter, 'get_bbo_age_ms'):
            for sym, pos in list(self.positions.items()):
                # FIX 2026-01-22: Also check as_lt_map for AS-LT exclusive pairs
                lt_sym = self.hl_to_lighter.get(pos.hl_coin) or self.as_lt_map.get(pos.symbol)
                if lt_sym:
                    lt_age = self.lighter.get_bbo_age_ms(lt_sym)
                    if lt_age > MAX_CARRY_BBO_AGE_MS:
                        stale_venues.append(f"LT({lt_sym}:{lt_age:.0f}ms)")
                        break

        # If any venue has stale data, skip exit execution (but still log MTM)
        circuit_breaker_active = len(stale_venues) > 0
        if circuit_breaker_active:
            # FIX 2026-01-22: Reduced to debug level - keep in background, don't spam logs
            log.debug(f"[CARRY] CIRCUIT BREAKER: Stale data on {', '.join(stale_venues)}. Skipping exit execution.") 

        # FIX 2026-01-22: Use list() to prevent RuntimeError if positions modified during iteration
        for sym, pos in list(self.positions.items()):
            hold_hours = (now - pos.entry_time) / 3600.0

            # Pass rates if available for Funding PnL refinement (future)
            mtm = self._calc_mtm(pos, current_rates=None)

            # P0 FIX 2026-01-22: Handle stale MTM data - emergency exit if persistently stale
            # This prevents positions from avoiding stop-loss when BBO data is unavailable
            if mtm.get("stale", False):
                stale_count = self._stale_mtm_counter.get(pos.hl_coin, 0) + 1
                self._stale_mtm_counter[pos.hl_coin] = stale_count

                if stale_count >= self._stale_mtm_emergency_threshold:
                    # EMERGENCY EXIT: MTM has been stale for too long
                    log.error(
                        f"[CARRY] EMERGENCY EXIT {sym}: MTM stale for {stale_count} consecutive checks "
                        f"(>{self._stale_mtm_emergency_threshold}). Forcing market exit to prevent silent losses!"
                    )
                    to_close.append((sym, f"emergency_stale_data({stale_count}_consecutive)", 0.0, 0.0))
                    self._stale_mtm_counter.pop(pos.hl_coin, None)  # Reset counter
                    continue  # Skip normal checks, force exit
                elif stale_count >= self._stale_mtm_warning_threshold:
                    log.warning(
                        f"[CARRY] STALE WARNING {sym}: MTM stale for {stale_count} consecutive checks. "
                        f"Emergency exit in {self._stale_mtm_emergency_threshold - stale_count} more ticks."
                    )
                # Skip stop-loss checks when data is stale - we don't want to use 0 MTM
                continue
            else:
                # Fresh data - reset stale counter
                self._stale_mtm_counter.pop(pos.hl_coin, None)

            # --- LOCK FUNDING ACCRUAL ---
            # Every MTM check, we "solidify" the earned funding to survive rate changes
            pos.accrued_funding_usd = mtm["funding_pnl_usd"]
            pos.last_accrual_time = now
            # No need to _save_state on every pair, we'll save once at the bottom if positions changed
            # or just rely on the periodic mtm log/dashboard save

            # --- P0 CRITICAL: LIQUIDATION PROXIMITY CHECK ---
            # Force exit if position is within 15% of liquidation price
            at_risk, dist_bps = self._check_liquidation_risk(pos)
            if at_risk:
                log.error(f"[CARRY] LIQUIDATION ALERT: {sym} only {dist_bps:.0f}bps from liquidation!")
                to_close.append((sym, f"LIQUIDATION_PROXIMITY({dist_bps:.0f}bps)", mtm["price_pnl_usd"], mtm["funding_pnl_usd"]))
                continue  # Skip other checks, force exit

            # --- 1. Max Hold Time Exit ---
            if hold_hours >= self.hold_max_hours:
                to_close.append((sym, "max_hold_time", mtm["price_pnl_usd"], mtm["funding_pnl_usd"]))
                continue

            # Update Peak MTM for Trailing Stop
            current_mtm_bps = mtm["mtm_bps"]
            if current_mtm_bps > pos.max_mtm_bps:
                pos.max_mtm_bps = current_mtm_bps

            # --- FIX 2026-01-24: Low-Yield Profitable Exit ---
            # If yield drops below threshold AND position is in profit, exit with market orders
            # This captures profits before yield erodes further
            curr_yield_bps = abs(mtm.get("curr_diff_bps", 0.0))
            total_mtm_usd = mtm.get("total_mtm_usd", 0.0)

            # Only trigger after minimum hold time (avoid flash exits on temporary yield dips)
            min_hold_for_low_yield_exit_hours = 0.5  # 30 minutes minimum
            if (curr_yield_bps < self._low_yield_exit_bps and
                total_mtm_usd > 0 and
                hold_hours >= min_hold_for_low_yield_exit_hours):
                log.info(f"[CARRY] LOW_YIELD_PROFITABLE_EXIT {sym}: "
                        f"yield={curr_yield_bps:.1f}bps < {self._low_yield_exit_bps:.0f}bps, "
                        f"MTM=${total_mtm_usd:.2f} (profitable), held {hold_hours:.1f}h")
                to_close.append((sym, f"low_yield_profitable(yield={curr_yield_bps:.1f}bps,mtm=${total_mtm_usd:.2f})",
                                mtm["price_pnl_usd"], mtm["funding_pnl_usd"]))
                continue

            # --- 2. MTM Stop-Loss/Take-Profit ---
            # Get current yield, but protect against transient 0-rate reporting (API glitches)
            # We use the MAX of live yield and entry yield as our stability anchor
            curr_y = abs(mtm.get("curr_diff_bps", 0.0))
            anchor_yield = max(curr_y, abs(pos.entry_diff_bps))

            # FIX 2026-01-22 P2.1-P2.2: Get yield-tiered stop parameters
            tiered_stop_bps, tiered_trailing_bps = self._get_tiered_stop_params(anchor_yield)

            # Dynamic Stop = Base Floor (Negative) - (Anchor Yield * Multiplier)
            # BUT: Multiplier only applies during the first N hours after entry (volatility window)
            # After that, hard stop at tiered_stop_bps applies to prevent runaway losses
            if hold_hours <= self.mtm_multiplier_window_hours:
                # Early period: allow wider stop based on yield
                base_stop_floor = -tiered_stop_bps - (anchor_yield * self.mtm_yield_multiplier)
            else:
                # After multiplier window: hard stop only (using tiered value)
                base_stop_floor = -tiered_stop_bps

            # === STOP PROTECTION MECHANISMS ===
            # Apply funding cushion (widens the floor based on accumulated funding)
            funding_cushion = self._calc_funding_cushion(pos, mtm)
            base_stop_floor -= funding_cushion

            # Apply time bonus for longer-held positions
            time_bonus = self._calc_time_bonus(pos)
            base_stop_floor -= time_bonus

            # Spike detection and protection
            is_spike, spike_drop = self._detect_spike(pos, current_mtm_bps)
            if is_spike:
                base_stop_floor -= self._spike_max_protection_bps

            # Trailing Profit Protection (FIX 2026-01-22 P2.2: Use tiered trailing)
            trailing_floor = -9999.0
            if pos.max_mtm_bps >= self.mtm_trailing_min:
                trailing_floor = pos.max_mtm_bps - tiered_trailing_bps

            # Final Floor is the tighter of base stop and trailing stop
            exit_floor = max(base_stop_floor, trailing_floor)

            # Apply Grace Period: Don't stop out in the first N minutes
            if hold_hours < (self.mtm_grace_minutes / 60.0):
                pass # Still in grace period
            elif current_mtm_bps < exit_floor:
                stop_type = "trailing_stop" if trailing_floor > base_stop_floor else "mtm_stop"
                # Enhanced logging with protection breakdown (FIX 2026-01-22: show tiered values)
                log.info(
                    f"[CARRY] EXIT {sym}: {stop_type} (MTM={current_mtm_bps:.1f} < Floor={exit_floor:.1f}) "
                    f"[TieredStop={tiered_stop_bps:.0f}, TieredTrail={tiered_trailing_bps:.0f}, "
                    f"FundCush={funding_cushion:.0f}, TimeBonus={time_bonus:.0f}, Spike={self._spike_max_protection_bps if is_spike else 0:.0f}]"
                )
                to_close.append((sym, f"{stop_type}({current_mtm_bps:.1f} < {exit_floor:.1f}bps)", mtm["price_pnl_usd"], mtm["funding_pnl_usd"]))
                continue

            # TIERED TAKE-PROFIT SYSTEM (configurable via risk.yaml)
            # High-yield pairs (≥high_yield_threshold_bps): % TP with time decay
            # Low-yield pairs: TP when MTM ≥ entry rate (net of fees)
            # BUGFIX 2026-01-21: Use CURRENT yield (not just entry yield) to determine tier
            # A position that entered low-yield but now has excellent current yield should NOT be closed
            entry_yield_bps = abs(pos.entry_diff_bps)
            current_yield_bps = abs(mtm.get("curr_diff_bps", 0.0))
            effective_yield_bps = max(entry_yield_bps, current_yield_bps)  # Use best of entry/current
            is_high_yield = effective_yield_bps >= self.high_yield_threshold_bps

            if is_high_yield:
                # High-yield: Use % of size_usd, with time decay
                if hold_hours >= self.high_yield_tp_decay_hours:
                    tp_pct = self.high_yield_tp_pct_after_decay
                else:
                    tp_pct = self.high_yield_tp_pct_initial
                tp_usd_threshold = pos.size_usd * tp_pct
                if mtm["total_mtm_usd"] >= tp_usd_threshold:
                    tp_label = f"high_yield_tp_{int(tp_pct*100)}pct"
                    to_close.append((sym, f"{tp_label}(${mtm['total_mtm_usd']:.2f}>=${tp_usd_threshold:.2f})", mtm["price_pnl_usd"], mtm["funding_pnl_usd"]))
                    continue
            else:
                # Low-yield: TP when MTM (already net of fees) exceeds entry rate
                # BUGFIX: Also verify CURRENT yield is still low before closing
                # If current yield has improved to high-yield levels, keep the position open
                if current_yield_bps >= self.high_yield_threshold_bps:
                    # Current yield has improved - skip low-yield TP, let it ride
                    log.debug(f"[CARRY] {sym} skipping low_yield_tp: entry={entry_yield_bps:.1f}bps but curr={current_yield_bps:.1f}bps (now high-yield)")
                    pass  # Don't close - yield has improved
                else:
                    # Both entry and current yield are low - apply low-yield TP
                    entry_rate = entry_yield_bps
                    if current_mtm_bps > 0 and current_mtm_bps >= entry_rate:
                        to_close.append((sym, f"low_yield_tp({current_mtm_bps:.1f}>={entry_rate:.1f}bps)", mtm["price_pnl_usd"], mtm["funding_pnl_usd"]))
                        continue

            # Fallback: Fixed bps TP for windfall scenarios (applies to both tiers)
            if current_mtm_bps > self.mtm_tp_bps:
                to_close.append((sym, f"mtm_tp_bps({current_mtm_bps:.1f}bps)", mtm["price_pnl_usd"], mtm["funding_pnl_usd"]))
                continue

            # --- 3. Funding Reversal Exit ---
            # AUDIT FIX: Added EARLY reversal detection (before hold_min_hours)
            # If funding completely flips sign, exit immediately regardless of hold time
            if hl_rates and as_rates:
                parts = pos.direction.split(" / ")
                leg1_venue = parts[0].split()[-1].upper()
                leg2_venue = parts[1].split()[-1].upper()

                def get_rate_norm(venue: str, coin: str, symbol: str) -> float:
                    # All rates normalized to 8h basis
                    # HL: Returns HOURLY rate → multiply by 8
                    if venue == "HL": return (hl_rates.get(coin, 0.0) or 0.0) * 8
                    # AS: Returns PER-INTERVAL rate (1h/4h/8h) → normalize to 8h
                    # FIX 2026-01-26: Use actual interval from API, not assume 8h
                    if venue == "AS":
                        interval = as_intervals.get(symbol, 8)
                        return (as_rates.get(symbol, 0.0) or 0.0) * (8.0 / interval)
                    # LT: Returns 8H rate directly → NO multiplier
                    if venue == "LT":
                        # FIX 2026-01-22: Also check as_lt_map for AS-LT exclusive pairs
                        lt_sym = self.hl_to_lighter.get(coin) or self.as_lt_map.get(symbol)
                        return lt_rates.get(lt_sym, 0.0) or 0.0
                    return 0.0

                fr1 = get_rate_norm(leg1_venue, pos.hl_coin, pos.symbol)
                fr2 = get_rate_norm(leg2_venue, pos.hl_coin, pos.symbol)

                is_long_leg1 = pos.direction.lower().startswith("long")
                curr_diff_bps = (fr1 - fr2) * 10000.0

                # EARLY REVERSAL: If funding has completely flipped (sign change), exit
                # This protects against paying funding instead of collecting it
                # BUGFIX: Use entry_signed_diff_bps (raw signed diff), NOT entry_diff_bps (always positive net_yield)
                # Previously: entry_diff_bps was always positive, causing false sign flips
                early_reversal_min_hold_hours = self.early_reversal_min_minutes / 60.0
                entry_sign = 1 if pos.entry_signed_diff_bps > 0 else -1
                current_sign = 1 if curr_diff_bps > 0 else -1
                if entry_sign != current_sign and abs(curr_diff_bps) > 5.0 and hold_hours >= early_reversal_min_hold_hours:
                    to_close.append((sym, f"early_reversal(sign_flip:{curr_diff_bps:.1f}bps)", mtm["price_pnl_usd"], mtm["funding_pnl_usd"]))
                    log.warning(f"[CARRY] EARLY REVERSAL {sym}: EntrySignedDiff={pos.entry_signed_diff_bps:.1f}bps Now={curr_diff_bps:.1f}bps (sign flip after {hold_hours*60:.1f}min)")
                    continue

                # STANDARD REVERSAL: After hold_min_hours, exit if funding is no longer favorable
                if hold_hours >= self.hold_min_hours:
                    exit_buffer = 1.0
                    should_exit_rev = False
                    # If Long Leg1 (Long low rate / Short high rate) -> We want (fr1 - fr2) < 0
                    if is_long_leg1 and curr_diff_bps > -exit_buffer:
                        should_exit_rev = True
                    # If Short Leg1 (Short high rate / Long low rate) -> We want (fr1 - fr2) > 0
                    elif not is_long_leg1 and curr_diff_bps < exit_buffer:
                        should_exit_rev = True

                    if should_exit_rev:
                        to_close.append((sym, f"reversal({curr_diff_bps:.1f}bps)", mtm["price_pnl_usd"], mtm["funding_pnl_usd"]))
                        # FIX: Log standard reversal exits (was missing, only early_reversal was logged)
                        log.warning(f"[CARRY] REVERSAL EXIT {sym}: Entry={pos.entry_diff_bps:.1f}bps Now={curr_diff_bps:.1f}bps (held {hold_hours:.1f}h)")
                        continue

            # Periodic logging
            bbo_hl = self.hl.get_bbo(pos.hl_coin)
            parts = pos.direction.split(" / ")
            leg2_v = parts[1].split()[-1].upper()
            bbo_v2 = None
            if leg2_v == "AS": bbo_v2 = self.asr.get_bbo(pos.symbol)
            elif leg2_v == "LT":
                # FIX 2026-01-22: Also check as_lt_map for AS-LT exclusive pairs
                lt_sym = self.hl_to_lighter.get(pos.hl_coin) or self.as_lt_map.get(pos.symbol)
                if lt_sym: bbo_v2 = self.lighter.get_bbo(lt_sym) if self.lighter else None
            
            px1 = (bbo_hl[0] + bbo_hl[1]) * 0.5 if bbo_hl else 0.0
            px2 = (bbo_v2[0] + bbo_v2[1]) * 0.5 if bbo_v2 else 0.0
            self._log_mtm(pos, mtm, px1, px2)

        # PERSIST ACCRUALS (Survival of restarts)
        if self.positions:
            self._save_state()

        # Execute Closures safely with pending exits tracking
        now = time.time()

        # First, check for force-exit on long-pending exits (even if circuit breaker active)
        forced_exits = []
        for sym, (reason, first_ts) in list(self._pending_exits.items()):
            if now - first_ts > self._pending_exit_force_timeout:
                log.warning(f"[CARRY] FORCE EXIT {sym}: Pending for {(now-first_ts)/60:.1f}min. Executing despite stale data.")
                forced_exits.append((sym, f"forced_{reason}", 0.0, 0.0))  # PnL will be recalculated
                del self._pending_exits[sym]

        for sym, reason, pnl_price, pnl_funding in forced_exits:
            # BUG FIX: Track reversal for forced exits too (was missing!)
            if "reversal" in reason.lower():
                pos = self.positions.get(sym)
                if pos:
                    log.info(f"[CARRY] Tracking reversal for {pos.hl_coin} (forced exit: {reason})")
                    self._track_reversal(pos.hl_coin)
            await self._execute_exit(sym, reason, pnl_price, pnl_funding)

        # Handle new exits
        if circuit_breaker_active and to_close:
            # Track deferred exits with timestamp
            for sym, reason, pnl_price, pnl_funding in to_close:
                if sym not in self._pending_exits:
                    self._pending_exits[sym] = (reason, now)
                    log.debug(f"[CARRY] Exit DEFERRED for {sym}: {reason} (stale data)")
            log.warning(f"[CARRY] {len(to_close)} exit(s) DEFERRED. Pending: {list(self._pending_exits.keys())}")
        else:
            # Data is fresh - execute all exits (including any pending ones)
            # First execute newly detected exits
            for sym, reason, pnl_price, pnl_funding in to_close:
                # Track reversal for auto-blocklist before executing exit
                log.debug(f"[CARRY] Processing exit {sym}: reason={reason}")
                if "reversal" in reason.lower():
                    pos = self.positions.get(sym)
                    if pos:
                        log.info(f"[CARRY] Tracking reversal for {pos.hl_coin} (reason: {reason})")
                        self._track_reversal(pos.hl_coin)
                    else:
                        log.warning(f"[CARRY] Cannot track reversal for {sym}: position not found")

                success = await self._execute_exit(sym, reason, pnl_price, pnl_funding)
                # FIX BUG #17: Only clear from pending if exit was successful
                if success and sym in self._pending_exits:
                    del self._pending_exits[sym]

            # Then retry any remaining pending exits (they may have recovered)
            for sym, (reason, first_ts) in list(self._pending_exits.items()):
                if sym in self.positions:
                    log.info(f"[CARRY] Retrying pending exit {sym}: {reason}")
                    success = await self._execute_exit(sym, reason, 0.0, 0.0)
                    # Only delete if successful
                    if success:
                        del self._pending_exits[sym]
                else:
                    # Position no longer exists (closed elsewhere), cleanup pending
                    del self._pending_exits[sym]

    async def _yield_rebalance(self) -> None:
        """
        Rebalance positions by yield: scale down lower-yield positions (in profit)
        to fund scale-up of higher-yield positions.

        Logic:
        1. Find positions that are undersized (scale_up_count < max) with high yield
        2. Find positions that are oversized (scale_up_count > 0) with lower yield AND in profit
        3. Scale down the lower-yield position repeatedly until high-yield is at max
        """
        if len(self.positions) < 2:
            return

        rebalance_count = 0
        max_rebalances_per_tick = 4  # Safety limit

        while rebalance_count < max_rebalances_per_tick:
            # Refresh position data each iteration (positions change after partial exits)
            pos_data = []
            # FIX 2026-01-22: Use list() to prevent RuntimeError if positions modified during iteration
            for sym, pos in list(self.positions.items()):
                mtm = self._calc_mtm(pos)
                # CRITICAL FIX: Only consider profitable if TOTAL PnL (price + funding) > 0
                # Previously was allowing scale-down when funding > 0 but price was negative
                total_pnl = mtm["total_mtm_usd"]
                # If MTM calc returns 0 (BBO unavailable), use accrued funding as estimate
                if total_pnl == 0 and pos.accrued_funding_usd > 0:
                    total_pnl = pos.accrued_funding_usd
                is_profitable = total_pnl > 0
                # BUGFIX: Use CURRENT yield (curr_diff_bps) not ENTRY yield for rebalancing decisions
                # This fixes the issue where AXS was scaled DOWN despite having excellent current yield
                current_yield_bps = abs(mtm.get("curr_diff_bps", 0.0))
                entry_yield_bps = abs(pos.entry_diff_bps)

                pos_data.append({
                    "sym": sym,
                    "pos": pos,
                    "yield_bps": current_yield_bps,  # FIXED: Use CURRENT yield
                    "entry_yield_bps": entry_yield_bps,  # Keep for reference
                    "size_usd": pos.size_usd,
                    "scale_count": pos.scale_up_count,
                    "total_pnl": total_pnl,
                    "in_profit": is_profitable
                })
                log.info(f"[CARRY] REBAL CHECK {pos.hl_coin}: curr_yield={current_yield_bps:.1f}bps (entry={entry_yield_bps:.1f}), scale={pos.scale_up_count}, "
                         f"mtm=${total_pnl:.2f}, accrued=${pos.accrued_funding_usd:.2f}, in_profit={is_profitable}")

            # Sort by yield (highest first)
            pos_data.sort(key=lambda x: x["yield_bps"], reverse=True)

            # Find candidates for rebalancing
            # High-yield undersized: can still scale up
            # Low-yield oversized: has scaled up and is in profit
            found_opportunity = False
            for high_yield in pos_data:
                if high_yield["scale_count"] >= self.max_scale_ups_per_position:
                    continue  # Already at max scale

                for low_yield in reversed(pos_data):  # Start from lowest yield
                    if low_yield["sym"] == high_yield["sym"]:
                        continue
                    if low_yield["scale_count"] == 0:
                        continue  # Not oversized (never scaled up)
                    if not low_yield["in_profit"]:
                        continue  # Only scale down if in profit

                    # BUGFIX 2026-01-21: Don't scale down positions that were JUST scaled up
                    # This prevents the race condition where we scale up then immediately scale down
                    last_scale_time = self._last_scale_up_time.get(low_yield["pos"].hl_coin, 0)
                    time_since_scale = time.time() - last_scale_time
                    if time_since_scale < self._scale_up_cooldown_seconds:
                        remaining_min = (self._scale_up_cooldown_seconds - time_since_scale) / 60.0
                        log.info(f"[CARRY] REBALANCE SKIP {low_yield['pos'].hl_coin}: "
                                f"Scaled up {time_since_scale/60:.1f}m ago, need {remaining_min:.1f}m more cooldown")
                        continue

                    # FIX 2026-01-24 Issue #11: Fee-aware profitability check
                    # Parse venue names from position direction (e.g., "LONG HL / SHORT AS")
                    direction_parts = low_yield["pos"].direction.split(" / ")
                    v1_name = direction_parts[0].split()[-1].upper() if len(direction_parts) >= 1 else "HL"
                    v2_name = direction_parts[1].split()[-1].upper() if len(direction_parts) >= 2 else "AS"

                    # Calculate expected exit fees (taker on both legs + slippage)
                    slice_size_for_fees = low_yield["size_usd"] * self.scale_up_slice_pct
                    v1_fee_bps = self._venue_fees.get(v1_name, {}).get("taker", 5.0)
                    v2_fee_bps = self._venue_fees.get(v2_name, {}).get("taker", 5.0)
                    slippage_bps = 3.0  # Conservative slippage estimate for market orders

                    exit_cost_bps = v1_fee_bps + v2_fee_bps + slippage_bps
                    exit_cost_usd = slice_size_for_fees * (exit_cost_bps / 10000.0)

                    # Require profit to exceed BOTH minimum threshold AND 2x exit costs
                    min_profit_usd = max(
                        low_yield["size_usd"] * (self.rebalance_min_profit_bps / 10000.0),
                        exit_cost_usd * 2.0  # 2x safety margin on fees
                    )

                    if low_yield["total_pnl"] < min_profit_usd:
                        log.debug(f"[CARRY] SCALE-DOWN BLOCKED {low_yield['pos'].hl_coin}: "
                                 f"profit ${low_yield['total_pnl']:.2f} < min ${min_profit_usd:.2f} "
                                 f"(fees=${exit_cost_usd:.2f})")
                        continue  # Profit too small after fees

                    # FIX 2026-01-24: Scale-Down Cooldown - prevent churn
                    last_scale_down = self._last_scale_down_time.get(low_yield["pos"].hl_coin, 0)
                    time_since_scale_down = time.time() - last_scale_down
                    if time_since_scale_down < self._scale_down_cooldown_seconds:
                        remaining_min = (self._scale_down_cooldown_seconds - time_since_scale_down) / 60.0
                        log.debug(f"[CARRY] SCALE-DOWN COOLDOWN {low_yield['pos'].hl_coin}: {remaining_min:.1f}min remaining")
                        continue  # In cooldown, skip this candidate

                    # Check if high_yield is significantly better than low_yield
                    # Rebalance if: high_yield > low_yield + buffer (meaningful improvement)
                    if high_yield["yield_bps"] <= low_yield["yield_bps"] + self.opt_buffer:
                        continue  # High-yield not significantly better than low-yield

                    # FIX 2026-01-22: Yield Momentum Check
                    # Block scale-down if the low-yield position's yield is IMPROVING
                    # This prevents scaling down positions like AXS that have rising rates
                    low_yield_pos = low_yield["pos"]
                    if low_yield_pos._prev_yield_bps is not None:
                        yield_delta = low_yield["yield_bps"] - low_yield_pos._prev_yield_bps
                        if yield_delta > 5.0:  # Yield improved by 5+ bps since last check
                            log.info(f"[CARRY] SCALE-DOWN BLOCKED {low_yield_pos.hl_coin}: "
                                    f"yield improving +{yield_delta:.1f}bps ({low_yield_pos._prev_yield_bps:.1f} → {low_yield['yield_bps']:.1f})")
                            continue  # Skip this low-yield position, yield is trending up
                        elif yield_delta < -5.0:
                            # Yield is dropping - this position is a better candidate for scale-down
                            log.debug(f"[CARRY] {low_yield_pos.hl_coin} yield dropping {yield_delta:.1f}bps - OK to scale down")

                    # Update stored yield for next check (do this for ALL positions, not just scale-down candidates)
                    low_yield_pos._prev_yield_bps = low_yield["yield_bps"]

                    # FIX 2026-01-24 Enhancement #9: Build comprehensive reason string for audit trail
                    yield_delta_str = "N/A"
                    if low_yield_pos._prev_yield_bps is not None:
                        yield_delta_str = f"{yield_delta:+.1f}" if 'yield_delta' in locals() else "N/A"

                    reason_parts = [
                        f"FROM:{low_yield['pos'].hl_coin}@{low_yield['yield_bps']:.0f}bps",
                        f"TO:{high_yield['pos'].hl_coin}@{high_yield['yield_bps']:.0f}bps",
                        f"DELTA:{high_yield['yield_bps'] - low_yield['yield_bps']:.0f}bps",
                        f"PnL:${low_yield['total_pnl']:.2f}",
                        f"MOMENTUM:{yield_delta_str}bps",
                    ]
                    scale_down_reason = f"YIELD_REBALANCE[{','.join(reason_parts)}]"

                    # Found a rebalance opportunity!
                    log.info(f"[CARRY] SCALE-DOWN DECISION: {scale_down_reason}")
                    log.info(f"[CARRY] YIELD REBALANCE [{rebalance_count+1}]: SCALE UP {high_yield['pos'].hl_coin} (curr={high_yield['yield_bps']:.1f}bps) "
                            f"<-- SCALE DOWN {low_yield['pos'].hl_coin} (curr={low_yield['yield_bps']:.1f}bps, scale={low_yield['scale_count']}, PnL: ${low_yield['total_pnl']:.2f})")

                    # Execute partial exit on low-yield position (scale down by one slice)
                    slice_size = low_yield["size_usd"] / (low_yield["scale_count"] + 1)  # Original slice size
                    success = await self._execute_partial_exit(low_yield["sym"], slice_size, scale_down_reason)

                    if success:
                        rebalance_count += 1
                        found_opportunity = True
                        # FIX 2026-01-24: Track scale-down time for cooldown
                        self._last_scale_down_time[low_yield["pos"].hl_coin] = time.time()

                        # Trigger immediate scale-up on the high-yield position
                        high_pos = high_yield["pos"]
                        if high_pos.scale_up_count < self.max_scale_ups_per_position:
                            log.info(f"[CARRY] YIELD REBALANCE: Triggering scale-up on {high_pos.hl_coin}")
                            await self._trigger_scale_up(high_pos)

                        break  # Re-evaluate positions after change
                    else:
                        return  # Stop if partial exit failed

                if found_opportunity:
                    break  # Re-evaluate from top

            if not found_opportunity:
                break  # No more rebalancing opportunities

        if rebalance_count > 0:
            log.info(f"[CARRY] Yield rebalancing complete: {rebalance_count} scale-down(s) executed")

        # FIX 2026-01-22: Update yield history for ALL positions (for momentum tracking)
        # Do this at the end regardless of whether rebalancing occurred
        # Use list() to prevent RuntimeError if positions modified during iteration
        for sym, pos in list(self.positions.items()):
            mtm = self._calc_mtm(pos)
            current_yield = abs(mtm.get("curr_diff_bps", 0.0))
            pos._prev_yield_bps = current_yield

    async def _proactive_opportunity_replacement(self) -> None:
        """
        FIX 2026-01-25: Proactive opportunity replacement when capital exhausted.

        When ALL capital is deployed, find positions to replace with better external opportunities.
        This enables yield maximization even when no free capital is available.

        Logic:
        1. Check if at max allocation (capital exhausted)
        2. Get current positions ranked by yield (worst first)
        3. Scan for external opportunities (not currently held)
        4. Calculate true replacement cost (fees + spread + safety)
        5. Check profit gate on position to close
        6. Execute replacement if yield gain > replacement cost
        """
        if len(self.positions) == 0:
            return

        # 1. Check if at max allocation
        current_exposure = sum(p.size_usd for p in list(self.positions.values()))
        total_equity = await self._get_total_equity()
        max_notional = total_equity * self.max_carry_alloc_pct

        # Only trigger proactive replacement when capital is 90%+ deployed
        if current_exposure < max_notional * 0.90:
            log.debug(f"[REPLACE] Capital not exhausted ({current_exposure/max_notional*100:.1f}%), skipping proactive replacement")
            return

        # 2. Get current positions ranked by yield (worst first)
        pos_data = []
        for sym, pos in list(self.positions.items()):
            mtm = self._calc_mtm(pos)
            total_pnl = mtm["total_mtm_usd"]
            # If MTM calc returns 0, use accrued funding as estimate
            if total_pnl == 0 and pos.accrued_funding_usd > 0:
                total_pnl = pos.accrued_funding_usd
            is_profitable = total_pnl > 0
            current_yield_bps = abs(mtm.get("curr_diff_bps", 0.0))

            # Parse venue info from direction
            parts = pos.direction.split(" / ")
            v1_name = parts[0].split()[-1].upper() if len(parts) >= 1 else "HL"
            v2_name = parts[1].split()[-1].upper() if len(parts) >= 2 else "AS"

            pos_data.append({
                "sym": sym,
                "pos": pos,
                "yield_bps": current_yield_bps,
                "total_pnl": total_pnl,
                "in_profit": is_profitable,
                "v1_name": v1_name,
                "v2_name": v2_name,
            })

        # Sort by yield (worst first)
        pos_data.sort(key=lambda x: x["yield_bps"])

        if not pos_data:
            return

        # 3. Scan for external opportunities
        # Use cached rates from main loop
        hl_rates = self._cached_hl_rates
        as_rates = self._cached_as_rates
        lt_rates = self._cached_lt_rates

        if not hl_rates or not as_rates:
            log.debug("[REPLACE] No cached rates available for opportunity scan")
            return

        # Get Aster funding intervals for rate normalization
        as_intervals = await self.asr.get_funding_intervals() if hasattr(self.asr, 'get_funding_intervals') else {}

        # Get set of currently held hl_coins
        held_coins = {p.hl_coin for p in self.positions.values()}

        external_opportunities = []
        for as_sym, hl_coin in self.pairs_map.items():
            # Skip if already holding this symbol
            if hl_coin in held_coins:
                continue

            # Skip blocklisted
            if hasattr(self, '_blocked_bases') and hl_coin.upper() in self._blocked_bases:
                continue
            if self._is_blocklisted(hl_coin):
                continue

            # Gather rates (all normalized to 8h basis)
            # HL: Returns HOURLY rate → multiply by 8
            r_hl = (hl_rates.get(hl_coin) or 0.0) * 8.0
            # AS: Returns PER-INTERVAL rate → normalize to 8h
            as_interval = as_intervals.get(as_sym, 8)
            r_as = (as_rates.get(as_sym) or 0.0) * (8.0 / as_interval)
            # LT: Returns 8H rate directly → NO multiplier
            lt_sym = self.hl_to_lighter.get(hl_coin)
            r_lt = (lt_rates.get(lt_sym) or 0.0) if lt_sym and lt_rates else None

            # Check all venue pairs
            venue_pairs = [("HL", r_hl, hl_coin), ("AS", r_as, as_sym)]
            if self.lighter and r_lt is not None:
                venue_pairs.append(("LT", r_lt, lt_sym))

            for i in range(len(venue_pairs)):
                for j in range(i + 1, len(venue_pairs)):
                    v1_n, r1, s1 = venue_pairs[i]
                    v2_n, r2, s2 = venue_pairs[j]
                    if r1 is None or r2 is None:
                        continue

                    raw_diff_bps = (r1 - r2) * 10000.0
                    abs_diff_bps = abs(raw_diff_bps)

                    # Apply minimum threshold for external opportunities
                    if abs_diff_bps < self.min_funding_bps:
                        continue

                    # Net yield after fees
                    est_fees = self._get_roundtrip_fees_bps(v1_n, v2_n)
                    net_yield = abs_diff_bps - est_fees

                    if net_yield < self.min_funding_bps:
                        continue

                    external_opportunities.append({
                        "hl_coin": hl_coin,
                        "as_sym": as_sym,
                        "s1": s1,
                        "s2": s2,
                        "v1_n": v1_n,
                        "v2_n": v2_n,
                        "ann_rate": net_yield,
                        "direction": "Long" if raw_diff_bps > 0 else "Short",
                    })

        # Sort by yield (best first)
        external_opportunities.sort(key=lambda x: x["ann_rate"], reverse=True)

        if not external_opportunities:
            log.debug("[REPLACE] No external opportunities found")
            return

        # 4. Find replacement candidate
        worst_pos = pos_data[0]  # Lowest current yield
        best_opp = external_opportunities[0]  # Highest external yield

        yield_gain = best_opp["ann_rate"] - worst_pos["yield_bps"]

        # 5. Calculate dynamic replacement cost
        replacement_cost = await self._calculate_replacement_cost_bps(
            exit_venue1=worst_pos["v1_name"],
            exit_venue2=worst_pos["v2_name"],
            entry_venue1=best_opp["v1_n"],
            entry_venue2=best_opp["v2_n"],
            exit_symbol=worst_pos["sym"],
            entry_hl_coin=best_opp["hl_coin"],
        )

        if yield_gain < replacement_cost:
            log.debug(f"[REPLACE] Gain {yield_gain:.1f}bps < cost {replacement_cost:.1f}bps, skipping")
            return

        # 6. Check profit gate
        if not worst_pos["in_profit"]:
            log.info(f"[REPLACE] {worst_pos['pos'].hl_coin} not in profit (${worst_pos['total_pnl']:.2f}), skipping replacement")
            return

        # 7. Check fee-aware minimum profit (must cover replacement cost)
        min_profit_usd = worst_pos["pos"].size_usd * (replacement_cost / 10000.0) * 2.0  # 2x safety
        if worst_pos["total_pnl"] < min_profit_usd:
            log.info(f"[REPLACE] {worst_pos['pos'].hl_coin} profit ${worst_pos['total_pnl']:.2f} < min ${min_profit_usd:.2f}, skipping")
            return

        # 8. Execute replacement
        log.info(f"[REPLACE] PROACTIVE ROTATION: {worst_pos['pos'].hl_coin} ({worst_pos['yield_bps']:.1f}bps) -> "
                 f"{best_opp['hl_coin']} ({best_opp['ann_rate']:.1f}bps), gain={yield_gain:.1f}bps, cost={replacement_cost:.1f}bps")

        # Exit worst position
        reason = f"PROACTIVE_REPLACE[gain={yield_gain:.1f}bps,cost={replacement_cost:.1f}bps,to={best_opp['hl_coin']}]"
        success = await self._execute_exit(worst_pos["sym"], reason, worst_pos["total_pnl"], worst_pos["total_pnl"])

        if success:
            log.info(f"[REPLACE] Exit successful, new entry will be picked up in next funding check cycle")
            # Note: Don't execute entry here - let the regular entry scan pick it up
            # This avoids race conditions and ensures proper allocation checks
        else:
            log.warning(f"[REPLACE] Exit failed for {worst_pos['pos'].hl_coin}")

    async def _trigger_scale_up(self, pos: CarryPosition) -> bool:
        """
        Trigger a scale-up on a position after yield rebalancing freed capital.
        Simplified version that directly increases the position size.
        """
        if pos.scale_up_count >= self.max_scale_ups_per_position:
            return False

        total_equity = await self._get_total_equity()
        # FIX 2026-01-25: Use stress-adjusted slice percentage
        effective_slice_pct = self._get_stress_adjusted_slice_pct(self.scale_up_slice_pct)
        slice_size = total_equity * self.max_carry_alloc_pct * effective_slice_pct

        # FIX 2026-01-25: Use yield-tiered allocation cap
        # Higher-yield positions can have higher concentration
        mtm = self._calc_mtm(pos)
        current_yield_bps = abs(mtm.get("curr_diff_bps", pos.entry_diff_bps))
        allocation_cap = self._get_yield_tiered_allocation_cap(current_yield_bps)
        max_sym_alloc = total_equity * allocation_cap
        if pos.size_usd + slice_size > max_sym_alloc:
            remaining = max_sym_alloc - pos.size_usd
            if remaining < 10.0:
                log.info(f"[CARRY] SCALE_UP SKIPPED {pos.hl_coin}: At max allocation")
                return False
            slice_size = remaining

        if self.paper_mode or self.dry_run_live:
            # Paper/Dry-run: Just increase position size
            # FIX 2026-01-22: Track scale-up fees (assume taker for paper mode)
            parts = pos.direction.split(" / ")
            v1_name = parts[0].split()[-1].upper() if len(parts) >= 2 else "HL"
            v2_name = parts[1].split()[-1].upper() if len(parts) >= 2 else "AS"
            scale_fee_bps = self._venue_fees[v1_name]["taker"] + self._venue_fees[v2_name]["taker"]
            scale_fee_usd = slice_size * (scale_fee_bps / 10000.0)

            pos.size_usd += slice_size
            pos.scale_up_count += 1
            self._last_scale_up_time[pos.hl_coin] = time.time()  # BUGFIX: Track scale-up time for cooldown

            # FIX 2026-01-22: Record fee operation
            if pos.fee_operations is None:
                pos.fee_operations = []
            pos.fee_operations.append({
                "op": "scale_up",
                "size_usd": slice_size,
                "fee_bps": scale_fee_bps,
                "fee_usd": scale_fee_usd,
                "v1_maker": False,  # Paper mode = assume taker
                "v2_maker": False,
                "ts": time.time()
            })
            # P2 FIX 2026-01-22: Cap fee_operations list to prevent unbounded growth
            if len(pos.fee_operations) > 50:
                pos.fee_operations = pos.fee_operations[-50:]
            pos.total_fees_paid_usd = (pos.total_fees_paid_usd or 0.0) + scale_fee_usd

            # P0-2 FIX 2026-01-23: Record scale-up entry (paper mode)
            if pos.entries is None:
                pos.entries = []
            pos.entries.append({
                "ts": time.time(),
                "px_v1": pos.entry_px_hl,
                "px_v2": pos.entry_px_as,
                "size_usd": slice_size,
                "is_scale_up": True
            })
            if len(pos.entries) > 50:
                pos.entries = pos.entries[-50:]

            self._save_state()

            log.info(f"[CARRY] SCALE_UP SUCCESS {pos.hl_coin}: New Size ${pos.size_usd:.2f} "
                    f"(scale-up #{pos.scale_up_count}/{self.max_scale_ups_per_position}) "
                    f"Fees: +{scale_fee_bps:.1f}bps/${scale_fee_usd:.2f} (taker, Total: ${pos.total_fees_paid_usd:.2f})")

            # Log to carry_audit.csv with actual venue names and funding rates
            # FIX 2026-01-21: Use real data instead of placeholders
            r1 = self._cached_hl_rates.get(pos.hl_coin, 0.0) if v1_name == "HL" else (
                 self._cached_as_rates.get(pos.symbol, 0.0) if v1_name == "AS" else
                 self._cached_lt_rates.get(pos.hl_coin, 0.0))
            r2 = self._cached_hl_rates.get(pos.hl_coin, 0.0) if v2_name == "HL" else (
                 self._cached_as_rates.get(pos.symbol, 0.0) if v2_name == "AS" else
                 self._cached_lt_rates.get(pos.hl_coin, 0.0))
            self._log(
                pos.symbol, "SCALE_UP", pos.direction, slice_size,
                r1, r2, v1_name, v2_name,
                0.0, 0.0, 0.0, self.paper_equity
            )
            return True

        # LIVE MODE: Execute actual scale-up orders (FIX 2026-01-22 Bug #1)
        log.info(f"[CARRY] SCALE_UP LIVE {pos.hl_coin}: Executing scale-up orders, size=${slice_size:.2f}")

        # Parse direction to get venues
        v1_name, v2_name = self._parse_direction_venues(pos.direction)

        v_map = {"HL": self.hl, "AS": self.asr, "LT": self.lighter}
        v1_obj, v2_obj = v_map.get(v1_name), v_map.get(v2_name)

        if not v1_obj or not v2_obj:
            log.error(f"[CARRY] Scale-up failed: Unknown venue(s) {v1_name}/{v2_name}")
            return False

        # Get symbols for each venue
        def _get_symbol_for_venue(venue_name: str) -> str:
            if venue_name == "HL":
                return pos.hl_coin
            elif venue_name == "LT":
                # FIX 2026-01-22: Also check as_lt_map for AS-LT exclusive pairs
                return self.hl_to_lighter.get(pos.hl_coin) or self.as_lt_map.get(pos.symbol) or pos.hl_coin
            else:
                return pos.symbol

        s1 = _get_symbol_for_venue(v1_name)
        s2 = _get_symbol_for_venue(v2_name)

        # Determine sides based on direction
        is_long1 = pos.direction.lower().startswith("long")
        side1 = "BUY" if is_long1 else "SELL"
        side2 = "SELL" if is_long1 else "BUY"

        # Get fresh BBOs for pricing
        if v1_name == "LT" and hasattr(v1_obj, 'get_fresh_bbo'):
            bbo1 = await v1_obj.get_fresh_bbo(s1)
        else:
            bbo1 = v1_obj.get_bbo(s1)

        if v2_name == "LT" and hasattr(v2_obj, 'get_fresh_bbo'):
            bbo2 = await v2_obj.get_fresh_bbo(s2)
        else:
            bbo2 = v2_obj.get_bbo(s2)

        if not bbo1 or not bbo2:
            log.warning(f"[CARRY] Scale-up aborted {pos.hl_coin}: No BBO available")
            return False

        # Calculate quantities
        size_per_leg = slice_size / 2
        p1 = bbo1[1] if side1 == "BUY" else bbo1[0]
        p2 = bbo2[1] if side2 == "BUY" else bbo2[0]
        qty1 = v1_obj.round_qty(s1, size_per_leg / p1)
        qty2 = v2_obj.round_qty(s2, size_per_leg / p2)

        # FIX 2026-01-24: Pre-check depth on BOTH venues before scale-up attempt
        # This prevents fee waste from scale-ups that will fail due to low depth
        min_depth_for_scale = size_per_leg * 2  # Need 2x order size in depth

        # FIX 2026-01-24: Use sentinel (-1.0) to distinguish "venue doesn't support depth" from "zero liquidity"
        v1_supports_depth = hasattr(v1_obj, 'get_aggregated_depth_usd')
        v2_supports_depth = hasattr(v2_obj, 'get_aggregated_depth_usd')

        d1_check = v1_obj.get_aggregated_depth_usd(s1, levels=5) if v1_supports_depth else -1.0
        d2_check = v2_obj.get_aggregated_depth_usd(s2, levels=5) if v2_supports_depth else -1.0

        # V1 depth check: block if venue supports depth AND has insufficient liquidity (including 0)
        if v1_supports_depth and d1_check >= 0 and d1_check < min_depth_for_scale:
            reason = "NO_LIQUIDITY" if d1_check == 0 else "LOW_DEPTH"
            log.info(f"[CARRY] SCALE_UP SKIPPED {pos.hl_coin}: V1 {reason} ${d1_check:.0f} < ${min_depth_for_scale:.0f}")
            return False

        # V2 depth check: block if venue supports depth AND has insufficient liquidity (including 0)
        if v2_supports_depth and d2_check >= 0 and d2_check < min_depth_for_scale:
            reason = "NO_LIQUIDITY" if d2_check == 0 else "LOW_DEPTH"
            log.info(f"[CARRY] SCALE_UP SKIPPED {pos.hl_coin}: V2 {reason} ${d2_check:.0f} < ${min_depth_for_scale:.0f}")
            return False

        # Setup leverage if needed
        await self._ensure_leverage_setup(v1_name, s1, v1_obj)
        await self._ensure_leverage_setup(v2_name, s2, v2_obj)

        # FIX 2026-01-24: Calculate dynamic timeout for scale-up (shorter than entry)
        mid_px = (bbo1[0] + bbo1[1]) / 2 if bbo1 else p1
        spread_bps = ((bbo1[1] - bbo1[0]) / mid_px) * 10000 if mid_px > 0 else 50.0
        scale_timeout = self._get_maker_timeout(spread_bps, is_scale_up=True)

        # FIX 2026-01-25: Adaptive V1 timeout based on V2 depth for scale-up
        v2_depth_check = v2_obj.get_aggregated_depth_usd(s2, levels=5) if hasattr(v2_obj, 'get_aggregated_depth_usd') else -1.0
        order_size_usd = scale_usd
        if v2_depth_check >= 0 and v2_depth_check < order_size_usd * 2:
            original_timeout = scale_timeout
            scale_timeout = min(scale_timeout, 15.0)
            if scale_timeout < original_timeout:
                log.info(f"[MAKER] Scale-up V2 depth thin (${v2_depth_check:.0f}), reducing V1 timeout: {original_timeout:.0f}s -> {scale_timeout:.0f}s")

        # FIX 2026-01-25: Pre-compute venue-aware maker mode decisions for scale-up
        use_maker_v1 = await self._should_use_maker_for_venue(v1_obj, s1, qty1, v1_name) if bbo1 and bbo1[0] > 0 and bbo1[1] > 0 else False
        use_maker_v2 = await self._should_use_maker_for_venue(v2_obj, s2, qty2, v2_name) if bbo2 and bbo2[0] > 0 and bbo2[1] > 0 else False

        # Execute Leg 1
        is_maker_v1 = False
        f1, actual_p1 = 0.0, 0.0
        try:
            if use_maker_v1:
                # FIX 2026-01-24: Use dynamic timeout for scale-up
                timeout_s = scale_timeout
                res1 = await self._execute_with_maker_fallback(
                    venue=v1_obj, symbol=s1, side=side1, qty=qty1,
                    bbo=bbo1, timeout_s=timeout_s, is_entry=True,
                    reduce_only=False, venue_name=v1_name
                )
            else:
                res1 = await v1_obj.place_order(s1, side1, qty1, ioc=True)
            f1 = float(res1.get("filled", 0.0))
            is_maker_v1 = res1.get("is_maker", False)
            actual_p1 = float(res1.get("avg_price", 0.0)) or p1
        except Exception as e:
            log.error(f"[CARRY] Scale-up Leg 1 exception: {e}")
            return False

        if f1 <= 0:
            log.warning(f"[CARRY] Scale-up Leg 1 failed for {pos.hl_coin}")
            return False

        # Execute Leg 2 - match V1's USD value
        # FIX 2026-01-24: V1 filled - V2 tries maker first with 30s wait, then IOC fallback
        is_maker_v2 = False
        v1_fill_usd = f1 * actual_p1
        qty2 = v2_obj.round_qty(s2, v1_fill_usd / p2)
        f2, actual_p2 = 0.0, 0.0
        # FIX 2026-01-25: Dynamic V2 timeout for scale-up based on spread
        v2_spread_bps = ((bbo2[1] - bbo2[0]) / ((bbo2[0] + bbo2[1]) / 2)) * 10000 if bbo2 and bbo2[0] > 0 and bbo2[1] > 0 else 50.0
        v2_timeout = self._get_maker_timeout(v2_spread_bps, is_scale_up=True)
        log.info(f"[CARRY] Scale-up V1 filled ${v1_fill_usd:.2f} - V2 trying maker ({v2_timeout:.0f}s, spread={v2_spread_bps:.1f}bps), then IOC fallback")
        try:
            if use_maker_v2:
                # FIX 2026-01-25: Dynamic timeout for V2 maker after V1 fills
                timeout_s = v2_timeout
                res2 = await self._execute_with_maker_fallback(
                    venue=v2_obj, symbol=s2, side=side2, qty=qty2,
                    bbo=bbo2, timeout_s=timeout_s, is_entry=True,
                    reduce_only=False, venue_name=v2_name
                )
            else:
                res2 = await v2_obj.place_order(s2, side2, qty2, ioc=True)
            f2 = float(res2.get("filled", 0.0))
            is_maker_v2 = res2.get("is_maker", False)
            actual_p2 = float(res2.get("avg_price", 0.0)) or p2
        except Exception as e:
            log.error(f"[CARRY] Scale-up Leg 2 exception: {e}")
            # Rollback Leg 1
            inv_side = "SELL" if side1 == "BUY" else "BUY"
            use_reduce_only = v1_name != "AS"
            try:
                await v1_obj.place_order(s1, inv_side, f1, ioc=True, reduce_only=use_reduce_only)
                log.info(f"[CARRY] Scale-up rollback V1 completed")
            except Exception as rb_e:
                log.error(f"[CARRY] Scale-up rollback failed: {rb_e}")
            return False

        if f2 <= 0:
            # FIX 2026-01-22: Forced market order for V2 when V1 already filled (scale-up)
            # V1 is committed - we MUST hedge with V2
            log.error(f"[CARRY] Scale-up V2 failed for {pos.hl_coin}. V1 FILLED - FORCING V2 MARKET!")

            V2_FORCED_MAX = 5
            v2_forced_success = False
            total_v2_filled = 0.0
            total_v2_value = 0.0
            remaining_qty2 = qty2

            for forced_attempt in range(V2_FORCED_MAX):
                if remaining_qty2 <= qty2 * 0.01:
                    v2_forced_success = True
                    break

                # Refresh BBO
                if v2_name == "LT" and hasattr(v2_obj, 'get_fresh_bbo'):
                    fresh_bbo2 = await v2_obj.get_fresh_bbo(s2)
                else:
                    fresh_bbo2 = v2_obj.get_bbo(s2)

                if fresh_bbo2 and fresh_bbo2[0] > 0:
                    remaining_usd = v1_fill_usd - total_v2_value
                    fresh_p2 = fresh_bbo2[1] if side2 == "BUY" else fresh_bbo2[0]
                    retry_qty2 = v2_obj.round_qty(s2, remaining_usd / fresh_p2)
                else:
                    retry_qty2 = v2_obj.round_qty(s2, remaining_qty2)
                    fresh_p2 = p2

                if retry_qty2 <= 0:
                    break

                log.warning(f"[CARRY] Scale-up V2 FORCED #{forced_attempt+1}/{V2_FORCED_MAX}: IOC {side2} {retry_qty2:.6f}")

                try:
                    res2 = await v2_obj.place_order(s2, side2, retry_qty2, ioc=True)
                    attempt_filled = float(res2.get("filled", 0.0))
                    if attempt_filled > 0:
                        fill_price = float(res2.get("avg_price", 0.0)) or fresh_p2
                        total_v2_filled += attempt_filled
                        total_v2_value += attempt_filled * fill_price
                        remaining_qty2 -= attempt_filled
                        log.info(f"[CARRY] Scale-up V2 forced fill #{forced_attempt+1}: +{attempt_filled:.6f}")

                        if remaining_qty2 <= qty2 * 0.01:
                            v2_forced_success = True
                            break
                except Exception as e:
                    log.error(f"[CARRY] Scale-up V2 forced #{forced_attempt+1} exception: {e}")

                await asyncio.sleep(0.3)

            # Update f2/p2 from total fills
            if total_v2_filled > 0:
                f2 = total_v2_filled
                actual_p2 = total_v2_value / total_v2_filled

            # FIX 2026-01-24: Partial fill acceptance for scale-up (same as entry)
            fill_pct = total_v2_filled / qty2 if qty2 > 0 else 0.0

            if not v2_forced_success and fill_pct >= 0.70:
                # 70%+ filled - accept scale-up with USD mismatch
                log.warning(f"[CARRY] Scale-up V2 {fill_pct*100:.0f}% filled, ACCEPTING partial")
                v2_forced_success = True
                f2 = total_v2_filled
                actual_p2 = total_v2_value / total_v2_filled if total_v2_filled > 0 else p2

            # If still failed (< 70% filled), rollback V1
            if not v2_forced_success and f2 <= 0:
                log.error(f"[CARRY] Scale-up V2 only {fill_pct*100:.0f}% filled after {V2_FORCED_MAX} attempts. Rolling back V1.")
                inv_side = "SELL" if side1 == "BUY" else "BUY"
                use_reduce_only = v1_name != "AS"
                try:
                    await v1_obj.place_order(s1, inv_side, f1, ioc=True, reduce_only=use_reduce_only)
                    log.info(f"[CARRY] Scale-up rollback V1 completed")
                except Exception as rb_e:
                    log.error(f"[CARRY] Scale-up rollback failed: {rb_e}")
                return False

        # Both legs filled - update position
        actual_v1_usd = f1 * actual_p1
        actual_v2_usd = f2 * actual_p2
        actual_total_usd = actual_v1_usd + actual_v2_usd

        # FIX 2026-01-22: Check leg size mismatch BEFORE updating position
        # Reject scale-ups with >10% mismatch to prevent unhedged exposure
        MAX_LEG_MISMATCH_PCT = 10.0
        size_diff_pct = abs(actual_v1_usd - actual_v2_usd) / max(actual_v1_usd, actual_v2_usd, 1) * 100
        if size_diff_pct > MAX_LEG_MISMATCH_PCT:
            log.error(f"[CARRY] SCALE_UP REJECTED: Leg size mismatch too large! "
                     f"V1=${actual_v1_usd:.2f} V2=${actual_v2_usd:.2f} ({size_diff_pct:.1f}% diff > {MAX_LEG_MISMATCH_PCT}%)")
            log.error(f"[CARRY] Rolling back BOTH legs to prevent unhedged exposure...")

            # Rollback V1
            inv_side1 = "SELL" if side1 == "BUY" else "BUY"
            use_reduce_only_v1 = v1_name != "AS"
            try:
                rb1 = await v1_obj.place_order(s1, inv_side1, f1, ioc=True, reduce_only=use_reduce_only_v1)
                rb1_filled = float(rb1.get("filled", 0))
                log.info(f"[CARRY] Scale-up mismatch rollback V1: closed {rb1_filled:.6f} of {f1:.6f}")
            except Exception as e:
                log.error(f"[CARRY] Scale-up mismatch rollback V1 FAILED: {e}")

            # Rollback V2
            inv_side2 = "SELL" if side2 == "BUY" else "BUY"
            use_reduce_only_v2 = v2_name != "AS"
            try:
                rb2 = await v2_obj.place_order(s2, inv_side2, f2, ioc=True, reduce_only=use_reduce_only_v2)
                rb2_filled = float(rb2.get("filled", 0))
                log.info(f"[CARRY] Scale-up mismatch rollback V2: closed {rb2_filled:.6f} of {f2:.6f}")
            except Exception as e:
                log.error(f"[CARRY] Scale-up mismatch rollback V2 FAILED: {e}")

            if self.notifier:
                await self.notifier.notify(
                    f"SCALE_UP REJECTED: {pos.hl_coin}",
                    f"Leg size mismatch {size_diff_pct:.1f}% > {MAX_LEG_MISMATCH_PCT}%\n"
                    f"V1: ${actual_v1_usd:.2f}\n"
                    f"V2: ${actual_v2_usd:.2f}\n"
                    f"Both legs rolled back to prevent unhedged exposure."
                )
            return False

        # FIX 2026-01-22: Calculate and track scale-up fees based on actual maker/taker fills
        fee_v1 = self._venue_fees[v1_name]["maker"] if is_maker_v1 else self._venue_fees[v1_name]["taker"]
        fee_v2 = self._venue_fees[v2_name]["maker"] if is_maker_v2 else self._venue_fees[v2_name]["taker"]
        scale_fee_bps = fee_v1 + fee_v2
        scale_fee_usd = actual_total_usd * (scale_fee_bps / 10000.0)

        old_size = pos.size_usd
        px1 = actual_p1 if v1_name == "HL" else actual_p2
        px2 = actual_p2 if v1_name == "HL" else actual_p1

        # Weighted averages
        pos.entry_px_hl = (old_size * pos.entry_px_hl + actual_total_usd * px1) / (old_size + actual_total_usd)
        pos.entry_px_as = (old_size * pos.entry_px_as + actual_total_usd * px2) / (old_size + actual_total_usd)
        pos.size_usd = old_size + actual_total_usd
        pos.size_v1_usd += actual_v1_usd
        pos.size_v2_usd += actual_v2_usd
        pos.scale_up_count += 1
        self._last_scale_up_time[pos.hl_coin] = time.time()

        # FIX 2026-01-22: Record fee operation for scale-up
        if pos.fee_operations is None:
            pos.fee_operations = []
        pos.fee_operations.append({
            "op": "scale_up",
            "size_usd": actual_total_usd,
            "fee_bps": scale_fee_bps,
            "fee_usd": scale_fee_usd,
            "v1_maker": is_maker_v1,
            "v2_maker": is_maker_v2,
            "ts": time.time()
        })
        # P2 FIX 2026-01-22: Cap fee_operations list to prevent unbounded growth
        if len(pos.fee_operations) > 50:
            pos.fee_operations = pos.fee_operations[-50:]
        pos.total_fees_paid_usd = (pos.total_fees_paid_usd or 0.0) + scale_fee_usd

        # P0-2 FIX 2026-01-23: Record scale-up entry (live mode)
        if pos.entries is None:
            pos.entries = []
        pos.entries.append({
            "ts": time.time(),
            "px_v1": px1,
            "px_v2": px2,
            "size_usd": actual_total_usd,
            "is_scale_up": True
        })
        if len(pos.entries) > 50:
            pos.entries = pos.entries[-50:]

        self._save_state()

        log.info(f"[CARRY] SCALE_UP SUCCESS {pos.hl_coin}: New Size ${pos.size_usd:.2f} "
                f"(V1=${pos.size_v1_usd:.2f}/V2=${pos.size_v2_usd:.2f}) "
                f"(scale-up #{pos.scale_up_count}/{self.max_scale_ups_per_position}) "
                f"Fees: V1={'maker' if is_maker_v1 else 'taker'} V2={'maker' if is_maker_v2 else 'taker'} "
                f"+{scale_fee_bps:.1f}bps/${scale_fee_usd:.2f} (Total: ${pos.total_fees_paid_usd:.2f})")

        # Log to audit
        r1 = self._cached_hl_rates.get(pos.hl_coin, 0.0) if v1_name == "HL" else (
             self._cached_as_rates.get(pos.symbol, 0.0) if v1_name == "AS" else
             self._cached_lt_rates.get(pos.hl_coin, 0.0))
        r2 = self._cached_hl_rates.get(pos.hl_coin, 0.0) if v2_name == "HL" else (
             self._cached_as_rates.get(pos.symbol, 0.0) if v2_name == "AS" else
             self._cached_lt_rates.get(pos.hl_coin, 0.0))
        total_equity = await self._get_total_equity()
        self._log(
            pos.symbol, "SCALE_UP", pos.direction, actual_total_usd,
            r1, r2, v1_name, v2_name,
            0.0, 0.0, 0.0, total_equity,
            v1_maker=is_maker_v1, v2_maker=is_maker_v2
        )
        return True

    async def _remediate_leg_divergence(
        self, hl_coin: str, symbol: str, pos, larger_venue: str,
        smaller_venue: str, v1_size: float, v2_size: float, reduce_usd: float
    ) -> None:
        """
        FIX 2026-01-24: Reduce the larger leg to match the smaller leg.
        This fixes divergence between venue positions (e.g., DOLO having more on LT than AS).

        Args:
            hl_coin: The base coin (e.g., "DOLO")
            symbol: The position symbol (e.g., "DOLOUSDT")
            pos: The CarryPosition object
            larger_venue: Venue with more ("AS" or "LT")
            smaller_venue: Venue with less
            v1_size: V1 venue size in USD
            v2_size: V2 venue size in USD
            reduce_usd: Amount to reduce in USD
        """
        log.warning(f"[REMEDIATE] Starting leg divergence fix for {hl_coin}: "
                   f"Reduce {larger_venue} by ${reduce_usd:.2f} to match {smaller_venue}")

        # Map venue names to objects
        v_map = {"HL": self.hl, "AS": self.asr, "LT": self.lighter}
        venue_obj = v_map.get(larger_venue)

        if not venue_obj:
            log.error(f"[REMEDIATE] Unknown venue: {larger_venue}")
            return

        # Determine symbol and side for the reduce order
        # Direction format: "Long AS / Short LT" -> If reducing AS (long), we SELL
        parts = pos.direction.split(" / ")
        is_larger_v1 = larger_venue in parts[0]

        if is_larger_v1:
            is_long = parts[0].startswith("Long")
        else:
            is_long = parts[1].startswith("Long")

        # To reduce a long position, we SELL. To reduce a short, we BUY.
        reduce_side = "SELL" if is_long else "BUY"

        # Get the correct symbol for the venue
        if larger_venue == "HL":
            reduce_symbol = hl_coin
        elif larger_venue == "AS":
            reduce_symbol = symbol  # e.g., "DOLOUSDT"
        elif larger_venue == "LT":
            # Try hl_to_lighter mapping first, then as_lt_map
            reduce_symbol = self.hl_to_lighter.get(hl_coin) or self.as_lt_map.get(symbol) or hl_coin

        # Get current price for qty calculation
        bbo = venue_obj.get_bbo(reduce_symbol)
        if not bbo or bbo[0] <= 0 or bbo[1] <= 0:
            log.error(f"[REMEDIATE] No BBO for {reduce_symbol} on {larger_venue}")
            return

        reduce_price = bbo[0] if reduce_side == "SELL" else bbo[1]
        reduce_qty = venue_obj.round_qty(reduce_symbol, reduce_usd / reduce_price)

        if reduce_qty <= 0:
            log.warning(f"[REMEDIATE] Reduce qty rounds to 0 for ${reduce_usd:.2f} at ${reduce_price:.6f}")
            return

        log.info(f"[REMEDIATE] {larger_venue} {reduce_symbol}: {reduce_side} {reduce_qty:.6f} @ ~${reduce_price:.6f}")

        # Execute the reduce order
        try:
            # Use IOC to reduce quickly
            use_reduce_only = larger_venue != "AS"  # Aster doesn't support reduce_only
            res = await venue_obj.place_order(
                reduce_symbol, reduce_side, reduce_qty,
                ioc=True, reduce_only=use_reduce_only
            )
            filled = float(res.get("filled", 0.0))
            avg_price = float(res.get("avg_price", 0.0)) or reduce_price

            if filled > 0:
                filled_usd = filled * avg_price
                log.info(f"[REMEDIATE] SUCCESS: Reduced {larger_venue} {reduce_symbol} by {filled:.6f} (~${filled_usd:.2f})")

                # Update internal position tracking
                if is_larger_v1:
                    pos.size_v1_usd = max(0, pos.size_v1_usd - filled_usd)
                else:
                    pos.size_v2_usd = max(0, pos.size_v2_usd - filled_usd)
                pos.size_usd = pos.size_v1_usd + pos.size_v2_usd
                self._save_state()

                if self.notifier:
                    await self.notifier.notify(
                        f"REMEDIATION SUCCESS: {hl_coin}",
                        f"Reduced {larger_venue} by ${filled_usd:.2f}\n"
                        f"New sizes: V1=${pos.size_v1_usd:.2f} V2=${pos.size_v2_usd:.2f}"
                    )
            else:
                log.error(f"[REMEDIATE] FAILED: No fill for reduce order")
                if self.notifier:
                    await self.notifier.notify(
                        f"REMEDIATION FAILED: {hl_coin}",
                        f"Could not reduce {larger_venue} position\n"
                        f"Manual intervention may be required"
                    )
        except Exception as e:
            log.error(f"[REMEDIATE] Exception: {e}")
            if self.notifier:
                await self.notifier.notify(
                    f"REMEDIATION ERROR: {hl_coin}",
                    f"Exception: {e}\n"
                    f"Manual intervention required"
                )

    async def _execute_partial_exit(self, sym: str, size_to_reduce: float, reason: str) -> bool:
        """
        Partially exit a position by reducing its size.
        Used for yield rebalancing.
        """
        pos = self.positions.get(sym)
        if not pos:
            return False

        if size_to_reduce >= pos.size_usd:
            # Full exit
            return await self._execute_exit(sym, reason, 0.0, 0.0)

        log.info(f"[CARRY] PARTIAL EXIT {sym}: Reducing by ${size_to_reduce:.2f} ({reason})")

        # Calculate proportional PnL for the portion being closed
        mtm = self._calc_mtm(pos)
        reduction_ratio = size_to_reduce / pos.size_usd
        partial_pnl = mtm["total_mtm_usd"] * reduction_ratio

        if self.paper_mode or self.dry_run_live:
            # Paper/Dry-run: Just update the position size and equity
            self.paper_equity += partial_pnl
            pos.size_usd -= size_to_reduce
            pos.scale_up_count = max(0, pos.scale_up_count - 1)
            self._save_state()

            log.info(f"[CARRY] PARTIAL EXIT OK {sym}: New size ${pos.size_usd:.2f}, PnL: ${partial_pnl:.2f}, scale_count: {pos.scale_up_count}")

            # FIX 2026-01-22 Bug #8: Check if remaining size is below venue minimum
            MIN_POSITION_USD = 10.0  # Below this, close fully to avoid orphan micro-positions
            if pos.size_usd < MIN_POSITION_USD and pos.size_usd > 0:
                log.warning(f"[CARRY] PARTIAL EXIT {sym}: Remaining size ${pos.size_usd:.2f} < min ${MIN_POSITION_USD:.0f}, triggering full exit")
                # Don't call _execute_exit (infinite loop risk), just mark for removal
                self._cleanup_positions([sym])
                return True

            # Track W/L for this partial exit
            self.total_exits += 1
            if partial_pnl > 0:
                self.wins += 1
            elif partial_pnl < 0:
                self.losses += 1

            # PHASE 2 FIX (2026-01-22): Track session realized PNL for partial exits
            self._session_realized_pnl += partial_pnl

            # Log to carry_audit.csv with actual venue names and funding rates
            # FIX 2026-01-21: Use real data instead of placeholders
            parts = pos.direction.split(" / ")
            v1_name = parts[0].split()[-1].upper() if len(parts) >= 2 else "V1"
            v2_name = parts[1].split()[-1].upper() if len(parts) >= 2 else "V2"
            r1 = self._cached_hl_rates.get(pos.hl_coin, 0.0) if v1_name == "HL" else (
                 self._cached_as_rates.get(pos.symbol, 0.0) if v1_name == "AS" else
                 self._cached_lt_rates.get(pos.hl_coin, 0.0))
            r2 = self._cached_hl_rates.get(pos.hl_coin, 0.0) if v2_name == "HL" else (
                 self._cached_as_rates.get(pos.symbol, 0.0) if v2_name == "AS" else
                 self._cached_lt_rates.get(pos.hl_coin, 0.0))
            self._log(
                sym, "SCALE_DOWN", pos.direction, size_to_reduce,
                r1, r2, v1_name, v2_name,
                0.0, partial_pnl, 0.0, self.paper_equity
            )

            return True

        # LIVE MODE: Execute actual partial close orders
        # Parse direction to get venues
        v1_name, v2_name = self._parse_direction_venues(pos.direction)

        v_map = {"HL": self.hl, "AS": self.asr, "LT": self.lighter}
        v1_obj, v2_obj = v_map.get(v1_name), v_map.get(v2_name)

        if not v1_obj or not v2_obj:
            log.error(f"[CARRY] Partial exit failed: Unknown venue(s) {v1_name}/{v2_name}")
            return False

        is_long1 = pos.direction.lower().startswith("long")
        side1 = "SELL" if is_long1 else "BUY"  # Close opposite of entry
        side2 = "BUY" if is_long1 else "SELL"

        # Get symbols
        def _get_symbol_for_venue(venue_name: str) -> str:
            if venue_name == "HL":
                return pos.hl_coin
            elif venue_name == "LT":
                # FIX 2026-01-22: Also check as_lt_map for AS-LT exclusive pairs
                return self.hl_to_lighter.get(pos.hl_coin) or self.as_lt_map.get(pos.symbol) or pos.hl_coin
            else:
                return pos.symbol

        s1 = _get_symbol_for_venue(v1_name)
        s2 = _get_symbol_for_venue(v2_name)

        # FIX 2026-01-22 Bug 2C: Calculate qty per-venue using each venue's price
        bbo1 = v1_obj.get_bbo(s1)
        bbo2 = v2_obj.get_bbo(s2)
        # FIX 2026-01-22: Correct entry price mapping for fallback
        # Entry stores: entry_px_hl = v1 price if v1=HL else v2 price
        #               entry_px_as = v2 price if v1=HL else v1 price
        def _get_entry_px(venue: str) -> float:
            if venue == "HL":
                return pos.entry_px_hl
            elif venue == "AS":
                return pos.entry_px_as
            elif venue == "LT":
                has_hl = "HL" in pos.direction.upper()
                return pos.entry_px_as if has_hl else pos.entry_px_hl
            return pos.entry_px_as
        current_px1 = ((bbo1[0] + bbo1[1]) / 2) if bbo1 else _get_entry_px(v1_name)
        current_px2 = ((bbo2[0] + bbo2[1]) / 2) if bbo2 else _get_entry_px(v2_name)
        size_per_leg = size_to_reduce / 2
        qty1 = size_per_leg / current_px1  # V1 qty at V1's price
        qty2 = size_per_leg / current_px2  # V2 qty at V2's price (FIX: was using qty1)

        # Execute partial closes
        # CRITICAL: Aster doesn't support reduce_only=True on exit orders
        is_maker_v1, is_maker_v2 = False, False  # Track maker status for logging
        try:
            use_reduce_only_v1 = v1_name != "AS"
            use_reduce_only_v2 = v2_name != "AS"
            res1 = await v1_obj.place_order(s1, side1, v1_obj.round_qty(s1, qty1), ioc=True, reduce_only=use_reduce_only_v1)
            f1 = float(res1.get("filled", 0.0))
            is_maker_v1 = res1.get("is_maker", False)

            res2 = await v2_obj.place_order(s2, side2, v2_obj.round_qty(s2, qty2), ioc=True, reduce_only=use_reduce_only_v2)
            f2 = float(res2.get("filled", 0.0))
            is_maker_v2 = res2.get("is_maker", False)

            if f1 > 0 and f2 > 0:
                # FIX 2026-01-22: Calculate actual closed USD values
                closed_v1_usd = f1 * current_px1
                closed_v2_usd = f2 * current_px2
                actual_closed_usd = closed_v1_usd + closed_v2_usd

                # Calculate partial PnL proportionally
                mtm = self._calc_mtm(pos)
                reduction_ratio = actual_closed_usd / (pos.size_usd + actual_closed_usd)  # Before reduction
                partial_pnl = mtm["total_mtm_usd"] * reduction_ratio

                pos.size_usd -= actual_closed_usd  # FIX: Use actual closed, not intended
                # FIX 2026-01-22: Update per-leg sizes
                pos.size_v1_usd = max(0.0, pos.size_v1_usd - closed_v1_usd)
                pos.size_v2_usd = max(0.0, pos.size_v2_usd - closed_v2_usd)
                pos.scale_up_count = max(0, pos.scale_up_count - 1)
                self._save_state()
                log.info(f"[CARRY] PARTIAL EXIT OK {sym}: New size ${pos.size_usd:.2f} (V1=${pos.size_v1_usd:.2f}/V2=${pos.size_v2_usd:.2f}), PnL: ${partial_pnl:.2f}")

                # FIX 2026-01-22 Bug #8: Check if remaining size is below venue minimum
                MIN_POSITION_USD = 10.0  # Below this, orphan positions can form
                if pos.size_usd < MIN_POSITION_USD and pos.size_usd > 0:
                    log.warning(f"[CARRY] PARTIAL EXIT {sym}: Remaining ${pos.size_usd:.2f} < min ${MIN_POSITION_USD:.0f}, "
                               "triggering full exit via _execute_exit")
                    # Attempt to fully close remaining position
                    # Use asyncio.create_task to avoid blocking
                    asyncio.create_task(self._execute_exit(sym, "MIN_SIZE_CLEANUP", 0.0, 0.0))

                # BUGFIX 2026-01-21: Track W/L for partial exits in LIVE mode (was missing)
                self.total_exits += 1
                if partial_pnl > 0:
                    self.wins += 1
                elif partial_pnl < 0:
                    self.losses += 1
                self._live_realized_pnl += partial_pnl
                # PHASE 2 FIX (2026-01-22): Track session realized PNL
                self._session_realized_pnl += partial_pnl

                # BUGFIX 2026-01-21: Log SCALE_DOWN to carry_audit.csv in LIVE mode (was missing)
                r1 = self._cached_hl_rates.get(pos.hl_coin, 0.0) if v1_name == "HL" else (
                     self._cached_as_rates.get(pos.symbol, 0.0) if v1_name == "AS" else
                     self._cached_lt_rates.get(pos.hl_coin, 0.0))
                r2 = self._cached_hl_rates.get(pos.hl_coin, 0.0) if v2_name == "HL" else (
                     self._cached_as_rates.get(pos.symbol, 0.0) if v2_name == "AS" else
                     self._cached_lt_rates.get(pos.hl_coin, 0.0))
                total_equity = await self._get_total_equity()
                # FIX 2026-01-22: Add maker/taker visibility to SCALE_DOWN
                self._log(
                    sym, "SCALE_DOWN", pos.direction, actual_closed_usd,
                    r1, r2, v1_name, v2_name,
                    0.0, partial_pnl, 0.0, total_equity,
                    v1_maker=is_maker_v1, v2_maker=is_maker_v2
                )

                return True
            else:
                log.warning(f"[CARRY] Partial exit incomplete: f1={f1}, f2={f2}")
                return False

        except Exception as e:
            log.error(f"[CARRY] Partial exit exception: {e}")
            return False

    async def _execute_exit(self, sym: str, reason: str, pnl_price: float, pnl_funding: float) -> bool:
        """
        Handle actual exit execution with Safety checks and retry logic.
        NEVER delete position unless confirmed closed or paper mode.
        Returns True if exit was successful, False otherwise.
        """
        pos = self.positions.get(sym)
        if not pos: return True  # No position = already closed, considered success

        # BUGFIX: Set forced exit cooldown to prevent immediate re-entry
        # This catches patterns like BERAUSDT being stopped and re-entered 7 seconds later
        # FIX 2026-01-24: Also apply to reversal exits, but track direction to allow inverted re-entry
        reason_lower = reason.lower()
        if "mtm_stop" in reason_lower or "trailing_stop" in reason_lower or "reversal" in reason_lower:
            self._set_forced_exit_cooldown(pos.hl_coin, reason, direction=pos.direction)

        # FIX: Guard against double exits - track exits in progress
        if not hasattr(self, '_exit_in_progress'):
            self._exit_in_progress = set()

        if sym in self._exit_in_progress:
            log.warning(f"[CARRY] BLOCKED DOUBLE EXIT: {sym} already has exit in progress!")
            return False

        self._exit_in_progress.add(sym)
        try:
            return await self._execute_exit_inner(sym, reason, pnl_price, pnl_funding, pos)
        finally:
            self._exit_in_progress.discard(sym)

    async def _execute_exit_inner(self, sym: str, reason: str, pnl_price: float, pnl_funding: float, pos) -> bool:
        """Inner exit logic, called by _execute_exit with double-exit protection."""
        # BUG FIX: Recalculate PnL if passed as 0,0 (forced exits, retries)
        # This ensures losses ARE counted in paper_equity and W/L stats
        if pnl_price == 0.0 and pnl_funding == 0.0:
            mtm = self._calc_mtm(pos)
            pnl_price = mtm.get("price_pnl_usd", 0.0)
            pnl_funding = mtm.get("funding_pnl_usd", 0.0)
            log.info(f"[CARRY] Recalculated PnL for {sym}: price=${pnl_price:.2f} funding=${pnl_funding:.2f}")

        log.info(f"[CARRY] EXIT REQUEST {sym}: {reason}. PnL Est: ${pnl_price+pnl_funding:.2f}")

        if self.paper_mode:
            self.paper_equity += (pnl_price + pnl_funding)
            # FIX 2026-01-22: Capture info for post-exit sync before removing position
            _exit_hl_coin = pos.hl_coin
            _exit_direction = pos.direction
            self._remove_position(sym, reason, pnl_price, pnl_funding, self.paper_equity)
            return True

        # --- LIVE/DRY-RUN EXIT EXECUTION ---
        v1_name, v2_name = self._parse_direction_venues(pos.direction)

        v_map = {"HL": self.hl, "AS": self.asr, "LT": self.lighter}
        v1_obj, v2_obj = v_map.get(v1_name), v_map.get(v2_name)

        if not v1_obj or not v2_obj:
            log.error(f"[CARRY] Exit failed: Unknown venue(s) {v1_name}/{v2_name}")
            return False

        is_long1 = pos.direction.lower().startswith("long")
        side1 = "SELL" if is_long1 else "BUY"
        side2 = "BUY" if is_long1 else "SELL"

        # FIX BUG #18: Correctly resolve symbol for each venue type
        # - HL uses hl_coin (e.g., "BTC")
        # - AS uses pos.symbol (e.g., "BTCUSDT")
        # - LT uses hl_to_lighter mapping (e.g., "BTC-PERP") or as_lt_map for exclusive pairs
        def _get_symbol_for_venue(venue_name: str) -> str:
            if venue_name == "HL":
                return pos.hl_coin
            elif venue_name == "LT":
                # FIX 2026-01-22: Also check as_lt_map for AS-LT exclusive pairs
                return self.hl_to_lighter.get(pos.hl_coin) or self.as_lt_map.get(pos.symbol) or pos.hl_coin
            else:  # AS
                return pos.symbol

        s1 = _get_symbol_for_venue(v1_name)
        s2 = _get_symbol_for_venue(v2_name)

        # FIX BUG #14: Use CURRENT price for qty calculation, not entry price
        # This ensures we close the correct notional amount even if price moved
        bbo1 = v1_obj.get_bbo(s1)
        bbo2 = v2_obj.get_bbo(s2)
        # FIX 2026-01-22: Correct entry price mapping for fallback
        def _get_entry_px_exit(venue: str) -> float:
            if venue == "HL":
                return pos.entry_px_hl
            elif venue == "AS":
                return pos.entry_px_as
            elif venue == "LT":
                has_hl = "HL" in pos.direction.upper()
                return pos.entry_px_as if has_hl else pos.entry_px_hl
            return pos.entry_px_as
        current_px1 = ((bbo1[0] + bbo1[1]) / 2) if bbo1 else _get_entry_px_exit(v1_name)
        current_px2 = ((bbo2[0] + bbo2[1]) / 2) if bbo2 else _get_entry_px_exit(v2_name)
        # size_usd is total position size, each leg gets half
        size_per_leg = pos.size_usd / 2
        qty_v1 = size_per_leg / current_px1

        # DRY-RUN LIVE MODE: Log what would happen but don't execute
        if self.dry_run_live:
            log.info(f"[CARRY DRY-RUN] WOULD PLACE EXIT ({reason}):")
            log.info(f"  Leg 1: {v1_name} {s1} {side1} qty={v1_obj.round_qty(s1, qty_v1):.6f} @ ${current_px1:.4f}")
            log.info(f"  Leg 2: {v2_name} {s2} {side2} qty={v2_obj.round_qty(s2, qty_v1):.6f} @ ${current_px2:.4f}")
            log.info(f"  Est PnL: Price=${pnl_price:.2f} Funding=${pnl_funding:.2f} Total=${pnl_price+pnl_funding:.2f}")
            # In dry-run, track simulated PnL separately (don't contaminate paper_equity)
            self._dry_run_realized_pnl += (pnl_price + pnl_funding)
            # Use real equity for tracking in dry-run mode
            self._remove_position(sym, reason, pnl_price, pnl_funding, self._cached_total_equity)
            return True

        # FIX 2026-01-25: Pre-compute venue-aware maker mode decisions for exits
        exit_qty_v1 = v1_obj.round_qty(s1, qty_v1)
        exit_qty_v2 = v2_obj.round_qty(s2, size_per_leg / current_px2)
        use_maker_v1 = await self._should_use_maker_for_venue(v1_obj, s1, exit_qty_v1, v1_name) if bbo1 and bbo1[0] > 0 and bbo1[1] > 0 else False
        use_maker_v2 = await self._should_use_maker_for_venue(v2_obj, s2, exit_qty_v2, v2_name) if bbo2 and bbo2[0] > 0 and bbo2[1] > 0 else False

        # Retry logic for Leg 1 with maker fallback (3 attempts with backoff)
        f1 = 0.0
        exit_is_maker_v1 = False
        res1 = {}
        for attempt in range(3):
            try:
                log.info(f"[CARRY] Closing Leg 1 {v1_name} {side1} (attempt {attempt+1}/3)...")
                # CRITICAL: Aster doesn't support reduce_only=True on exit orders (per CLAUDE.md)
                use_reduce_only = v1_name != "AS"

                # PHASE 7: Use maker fallback for exits if enabled
                if use_maker_v1:
                    timeout_s = float(self.risk.get("carry", {}).get("maker_exit_timeout_s", 60.0))
                    res1 = await self._execute_with_maker_fallback(
                        venue=v1_obj, symbol=s1, side=side1, qty=v1_obj.round_qty(s1, qty_v1),
                        bbo=bbo1, timeout_s=timeout_s, is_entry=False,
                        reduce_only=use_reduce_only, venue_name=v1_name
                    )
                    f1 = float(res1.get("filled", 0.0))
                    exit_is_maker_v1 = res1.get("is_maker", False)
                else:
                    res1 = await v1_obj.place_order(s1, side1, v1_obj.round_qty(s1, qty_v1),
                                                    ioc=True, reduce_only=use_reduce_only)
                    f1 = float(res1.get("filled", 0.0))

                # FIX 2026-01-25: Check fill percentage, not just > 0
                # Partial fills (< 95%) should continue retry loop to close remaining qty
                qty_v1_requested = v1_obj.round_qty(s1, qty_v1)
                fill_pct_v1 = f1 / qty_v1_requested if qty_v1_requested > 0 else 0

                if f1 > 0 and fill_pct_v1 >= 0.95:
                    log.info(f"[CARRY] Leg 1 filled: {f1} ({fill_pct_v1*100:.0f}%) (maker={exit_is_maker_v1})")
                    break
                elif f1 > 0:
                    log.warning(f"[CARRY] Exit Leg 1 PARTIAL: {f1}/{qty_v1_requested} ({fill_pct_v1*100:.1f}%) - continuing retry loop")
                    # Update qty_v1 to remaining amount for next attempt
                    qty_v1 = qty_v1_requested - f1
                    # Continue retry loop for remaining qty

                log.warning(f"[CARRY] Exit Leg 1 {v1_name} attempt {attempt+1} result: fill={f1}, status={res1.get('status')}")

            except Exception as e:
                log.warning(f"[CARRY] Exit Leg 1 exception (attempt {attempt+1}): {e}")
                self._health_monitor.record_error(v1_name, str(e))  # P2 Fix: Track venue errors

            await asyncio.sleep(1.0 * (attempt + 1))  # Backoff: 1s, 2s, 3s

        if f1 <= 0:
            log.error(f"[CARRY] Exit Leg 1 {v1_name} failed after 3 attempts. Position remains open.")
            # FIX 2026-01-24: Log failed exit leg 1 to orders CSV
            self._log_order(
                event="ORDER_FAILED",
                symbol=s1,
                hl_coin=pos.hl_coin,
                venue=v1_name,
                side=side1,
                qty_requested=v1_obj.round_qty(s1, qty_v1),
                qty_filled=0.0,
                order_type="MAKER/IOC" if self._maker_order_enabled else "IOC",
                direction=pos.direction,
                action="EXIT_LEG1",
                status="FAILED",
                error="No fill after 3 attempts",
                details=f"reason={reason}, position_remains_open=true"
            )
            return False  # Do NOT delete position, return failure

        # Retry logic for Leg 2 with maker fallback (3 attempts with backoff)
        # FIX 2026-01-25: V1 closed - V2 tries maker first with dynamic timeout, then IOC fallback
        # FIX 2026-01-22 Bug 2A: Match V1's USD value on V2, not coin qty
        exit_v1_usd = f1 * current_px1  # Actual USD closed on V1
        qty_v2_target = v2_obj.round_qty(s2, exit_v1_usd / current_px2)  # Match USD, not coins
        f2 = 0.0
        exit_is_maker_v2 = False
        res2 = {}
        # FIX 2026-01-25: Dynamic V2 timeout for exits based on spread
        v2_spread_bps = ((bbo2[1] - bbo2[0]) / ((bbo2[0] + bbo2[1]) / 2)) * 10000 if bbo2 and bbo2[0] > 0 and bbo2[1] > 0 else 50.0
        v2_exit_timeout = self._get_maker_timeout(v2_spread_bps, is_scale_up=False)  # Use entry timeouts for exits
        log.info(f"[CARRY] V1 closed ${exit_v1_usd:.2f} - V2 trying maker ({v2_exit_timeout:.0f}s, spread={v2_spread_bps:.1f}bps), then IOC fallback")
        for attempt in range(3):
            try:
                log.info(f"[CARRY] Closing Leg 2 {v2_name} {side2} (qty={qty_v2_target}, attempt {attempt+1}/3)...")
                # CRITICAL: Aster doesn't support reduce_only=True on exit orders (per CLAUDE.md)
                use_reduce_only_v2 = v2_name != "AS"

                # FIX 2026-01-25: Dynamic timeout for V2 maker after V1 closes
                if use_maker_v2:
                    timeout_s = v2_exit_timeout
                    res2 = await self._execute_with_maker_fallback(
                        venue=v2_obj, symbol=s2, side=side2, qty=qty_v2_target,
                        bbo=bbo2, timeout_s=timeout_s, is_entry=False,
                        reduce_only=use_reduce_only_v2, venue_name=v2_name
                    )
                    f2 = float(res2.get("filled", 0.0))
                    exit_is_maker_v2 = res2.get("is_maker", False)
                else:
                    res2 = await v2_obj.place_order(s2, side2, qty_v2_target,
                                                    ioc=True, reduce_only=use_reduce_only_v2)
                    f2 = float(res2.get("filled", 0.0))

                # FIX 2026-01-25: Check fill percentage, not just > 0
                # Partial fills (< 95%) should continue retry loop to close remaining qty
                fill_pct_v2 = f2 / qty_v2_target if qty_v2_target > 0 else 0

                if f2 > 0 and fill_pct_v2 >= 0.95:
                    log.info(f"[CARRY] Leg 2 filled: {f2} ({fill_pct_v2*100:.0f}%) (maker={exit_is_maker_v2})")
                    break
                elif f2 > 0:
                    log.warning(f"[CARRY] Exit Leg 2 PARTIAL: {f2}/{qty_v2_target} ({fill_pct_v2*100:.1f}%) - continuing retry loop")
                    # Update qty_v2_target to remaining amount for next attempt
                    qty_v2_target = qty_v2_target - f2
                    # Continue retry loop for remaining qty

                log.warning(f"[CARRY] Exit Leg 2 {v2_name} attempt {attempt+1} result: fill={f2}, status={res2.get('status')}")

            except Exception as e:
                log.warning(f"[CARRY] Exit Leg 2 exception (attempt {attempt+1}): {e}")
                self._health_monitor.record_error(v2_name, str(e))  # P2 Fix: Track venue errors

            await asyncio.sleep(1.0 * (attempt + 1))

        if f2 <= 0:
            # CRITICAL: Leg 1 closed but Leg 2 failed - orphan created!
            # FIX 2026-01-24 Issue #1: Force exit the orphan leg instead of leaving it unhedged
            log.error(f"[CARRY] CRITICAL: Leg 2 {v2_name} failed but Leg 1 already closed. FORCING V2 EXIT...")

            # Calculate remaining quantity to close (use qty_v2_target from exit loop)
            v2_remaining = qty_v2_target - f2 if f2 > 0 else qty_v2_target

            # Force exit orphan leg with aggressive IOC retries
            force_success = await self._force_exit_orphan_leg(
                venue=v2_obj, symbol=s2, side=side2, qty=v2_remaining,
                venue_name=v2_name, hl_coin=pos.hl_coin
            )

            if force_success:
                log.info(f"[CARRY] ORPHAN RESOLVED {pos.hl_coin}: Forced V2 exit successful")
                pos.size_usd = 0
                # FIX 2026-01-25: Track recently-closed to prevent false orphan alerts
                self._recently_closed[pos.hl_coin] = time.time()
                del self.positions[sym]
            else:
                log.error(f"[CARRY] ORPHAN UNRESOLVED {pos.hl_coin}: V2 force exit failed - MANUAL INTERVENTION NEEDED")
                # Track orphan state for dashboard/monitoring
                pos.orphan_state = "V2_FAILED_FORCE_EXIT"
                pos.size_usd = 0  # Mark as effectively closed on our side

            self._save_state()
            return force_success

        # CONFIRMED CLOSE - Both legs exited successfully
        # FIX 2026-01-24: Log exit orders to orders CSV
        exit_px_v1_log = float(res1.get("avgPx", res1.get("avg_price", 0))) or current_px1
        exit_px_v2_log = float(res2.get("avgPx", res2.get("avg_price", 0))) or current_px2
        self._log_order(
            event="ORDER_FILLED",
            symbol=s1,
            hl_coin=pos.hl_coin,
            venue=v1_name,
            side=side1,
            qty_requested=v1_obj.round_qty(s1, qty_v1),
            qty_filled=f1,
            avg_fill_price=exit_px_v1_log,
            order_type="MAKER" if exit_is_maker_v1 else "IOC",
            direction=pos.direction,
            action="EXIT_LEG1",
            status="SUCCESS",
            fees_est_usd=(f1 * exit_px_v1_log) * (self._venue_fees.get(v1_name, {}).get("maker" if exit_is_maker_v1 else "taker", 5.0) / 10000),
            details=f"reason={reason}"
        )
        self._log_order(
            event="ORDER_FILLED",
            symbol=s2,
            hl_coin=pos.hl_coin,
            venue=v2_name,
            side=side2,
            qty_requested=qty_v2_target,
            qty_filled=f2,
            avg_fill_price=exit_px_v2_log,
            order_type="MAKER" if exit_is_maker_v2 else "IOC",
            direction=pos.direction,
            action="EXIT_LEG2",
            status="SUCCESS",
            fees_est_usd=(f2 * exit_px_v2_log) * (self._venue_fees.get(v2_name, {}).get("maker" if exit_is_maker_v2 else "taker", 5.0) / 10000),
            details=f"reason={reason}"
        )

        # FIX: Use ACTUAL fill prices instead of estimates for PnL calculation
        # This ensures wins/losses are based on real execution, not MTM estimates

        # Get actual fill prices from order results
        exit_px_v1 = float(res1.get("avgPx", 0)) or current_px1  # Fallback to BBO mid if not available
        exit_px_v2 = float(res2.get("avgPx", 0)) or current_px2

        # Calculate REAL price PnL based on actual entry vs exit prices
        # For Long V1 / Short V2: profit when V1 goes up AND V2 goes down
        # For Short V1 / Long V2: profit when V1 goes down AND V2 goes up

        # FIX 2026-01-22: Map venue to correct entry price
        # Entry prices are stored as:
        #   - entry_px_hl = prices["v1"] if v1 is HL at entry, else prices["v2"]
        #   - entry_px_as = prices["v2"] if v1 is HL at entry, else prices["v1"]
        # For AS-LT pairs: entry_px_hl=LT price, entry_px_as=AS price
        def get_entry_price_for_venue(venue: str) -> float:
            if venue == "HL":
                return pos.entry_px_hl
            elif venue == "AS":
                return pos.entry_px_as
            elif venue == "LT":
                # LT price location depends on pair type
                has_hl = "HL" in pos.direction.upper()
                return pos.entry_px_as if has_hl else pos.entry_px_hl
            return pos.entry_px_as
        entry_px_v1 = get_entry_price_for_venue(v1_name)
        entry_px_v2 = get_entry_price_for_venue(v2_name)

        # Calculate price PnL for each leg
        # V1 leg: closed with side1 (SELL if was long, BUY if was short)
        if side1 == "SELL":  # Was long V1 -> profit = exit - entry
            pnl_v1 = (exit_px_v1 - entry_px_v1) * f1
        else:  # Was short V1 -> profit = entry - exit
            pnl_v1 = (entry_px_v1 - exit_px_v1) * f1

        # V2 leg: closed with side2 (opposite of side1)
        if side2 == "SELL":  # Was long V2 -> profit = exit - entry
            pnl_v2 = (exit_px_v2 - entry_px_v2) * f2
        else:  # Was short V2 -> profit = entry - exit
            pnl_v2 = (entry_px_v2 - exit_px_v2) * f2

        actual_price_pnl = pnl_v1 + pnl_v2

        # Use ACTUAL realized funding instead of estimated
        actual_funding_pnl = pos.realized_funding if pos.realized_funding != 0.0 else pos.accrued_funding_usd

        log.info(f"[CARRY] ACTUAL PnL for {sym}: price=${actual_price_pnl:.2f} (V1: ${pnl_v1:.2f} @ {exit_px_v1:.4f}, V2: ${pnl_v2:.2f} @ {exit_px_v2:.4f}), funding=${actual_funding_pnl:.2f}")
        log.info(f"[CARRY] Exit fills: V1 {v1_name} qty={f1:.6f} avgPx={exit_px_v1:.4f}, V2 {v2_name} qty={f2:.6f} avgPx={exit_px_v2:.4f}")

        # FIX 2026-01-22: Post-exit position verification to prevent orphans
        # Check actual venue positions and cleanup any remaining qty
        await self._verify_and_cleanup_exit(
            v1_obj=v1_obj, v2_obj=v2_obj, s1=s1, s2=s2,
            side1=side1, side2=side2, v1_name=v1_name, v2_name=v2_name,
            expected_qty_v1=qty_v1, expected_qty_v2=qty_v2_target,
            actual_fill_v1=f1, actual_fill_v2=f2
        )

        # OPTIMIZATION 2026-01-24: Log exit decision with full context
        try:
            self._log_decision(
                symbol=sym,
                hl_coin=pos.hl_coin,
                decision_type="EXIT_SUCCESS",
                trigger_reason=reason,
                v1_venue=v1_name,
                v2_venue=v2_name,
                entry_yield_bps=pos.entry_diff_bps,
                position_size_usd=pos.size_usd,
                equity_usd=self._cached_total_equity,
                outcome="SUCCESS",
                outcome_reason=f"price=${actual_price_pnl:.2f} funding=${actual_funding_pnl:.2f} hold={hold_hours:.1f}h"
            )
        except Exception as e:
            log.warning(f"[CARRY] Exit decision log failed: {e}")

        # Use actual PnL for wins/losses tracking instead of MTM estimates
        # FIX 2026-01-22: Pass exit maker info to logging
        self._remove_position(sym, reason, actual_price_pnl, actual_funding_pnl, self._cached_total_equity,
                              exit_v1_maker=exit_is_maker_v1, exit_v2_maker=exit_is_maker_v2)
        return True  # Exit successful

    async def _force_exit_orphan_leg(
        self, venue, symbol: str, side: str, qty: float,
        venue_name: str, hl_coin: str, max_retries: int = 3
    ) -> bool:
        """FIX 2026-01-24 Issue #1: Force exit an orphan leg using aggressive IOC orders.

        Called when one leg exits successfully but the other fails.
        Uses IOC market orders with retries to ensure the orphan is closed.

        Args:
            venue: Venue object (HL, AS, or LT)
            symbol: Symbol to close
            side: "buy" or "sell" to close the position
            qty: Quantity to close
            venue_name: Venue name for logging
            hl_coin: HL coin for logging
            max_retries: Maximum IOC attempts (default 3)

        Returns:
            True if position fully closed, False if orphan remains
        """
        original_qty = qty  # FIX 2026-01-24: Track original qty for logging
        total_filled = 0.0
        for attempt in range(max_retries):
            try:
                log.warning(f"[CARRY] ORPHAN FORCE EXIT {hl_coin} {venue_name}: "
                           f"Attempt {attempt+1}/{max_retries} - {side} {qty:.6f} {symbol}")

                result = await venue.place_order(
                    symbol=symbol, side=side, qty=qty,
                    ioc=True, reduce_only=True
                )

                filled = float(result.get("filled", 0))
                if filled > 0:
                    log.info(f"[CARRY] ORPHAN FORCE EXIT {hl_coin}: Filled {filled:.6f} on attempt {attempt+1}")
                    total_filled += filled
                    qty -= filled
                    if qty < 0.001:  # Fully closed
                        # Log successful orphan exit
                        self._log_order(
                            event="ORPHAN_EXIT",
                            symbol=symbol,
                            hl_coin=hl_coin,
                            venue=venue_name,
                            side=side,
                            qty_requested=original_qty,
                            qty_filled=total_filled,
                            order_type="IOC",
                            direction="",
                            action="ORPHAN_FORCE_EXIT",
                            status="SUCCESS",
                            details=f"attempts={attempt+1}/{max_retries}"
                        )
                        return True

                await asyncio.sleep(2.0)  # Brief pause between retries

            except Exception as e:
                log.error(f"[CARRY] ORPHAN FORCE EXIT {hl_coin}: Attempt {attempt+1} failed: {e}")
                await asyncio.sleep(2.0)

        # FIX 2026-01-24: Log orphan force exit result to orders CSV (failure/partial case)
        success = qty < 0.001
        self._log_order(
            event="ORPHAN_EXIT",
            symbol=symbol,
            hl_coin=hl_coin,
            venue=venue_name,
            side=side,
            qty_requested=original_qty,
            qty_filled=total_filled,
            order_type="IOC",
            direction="",
            action="ORPHAN_FORCE_EXIT",
            status="SUCCESS" if success else "PARTIAL",
            error="" if success else f"Remaining qty: {qty:.6f}",
            details=f"attempts={max_retries}/{max_retries}"
        )
        return success

    async def _verify_and_cleanup_exit(
        self, v1_obj, v2_obj, s1: str, s2: str,
        side1: str, side2: str, v1_name: str, v2_name: str,
        expected_qty_v1: float, expected_qty_v2: float,
        actual_fill_v1: float, actual_fill_v2: float
    ) -> None:
        """FIX 2026-01-22: Post-exit verification to cleanup any orphaned positions.

        Checks actual venue positions and attempts to close any remaining qty
        that wasn't captured by the normal exit flow (partial fills, etc).
        """
        try:
            # FIX 2026-01-22 Bug #12: Wait for venue API propagation before querying
            # This prevents false "ORPHAN DETECTED" errors from stale data
            await asyncio.sleep(2.0)
            # Check for unfilled remainder on leg 1
            remainder_v1 = expected_qty_v1 - actual_fill_v1
            if remainder_v1 > expected_qty_v1 * 0.01:  # More than 1% unfilled
                log.warning(f"[CARRY] ORPHAN CLEANUP {v1_name}: {remainder_v1:.6f} unfilled on leg 1. Attempting IOC cleanup...")
                try:
                    # CRITICAL: Don't use reduce_only on Aster
                    use_reduce_only = v1_name != "AS"
                    cleanup_result = await v1_obj.place_order(s1, side1, remainder_v1, ioc=True, reduce_only=use_reduce_only)
                    cleanup_filled = float(cleanup_result.get("filled", 0.0))
                    if cleanup_filled > 0:
                        log.info(f"[CARRY] ORPHAN CLEANUP {v1_name}: Recovered {cleanup_filled:.6f} (IOC)")
                    else:
                        log.error(f"[CARRY] ORPHAN CLEANUP {v1_name}: Failed to recover {remainder_v1:.6f}. MANUAL INTERVENTION NEEDED!")
                except Exception as e:
                    log.error(f"[CARRY] ORPHAN CLEANUP {v1_name} exception: {e}")

            # Check for unfilled remainder on leg 2
            remainder_v2 = expected_qty_v2 - actual_fill_v2
            if remainder_v2 > expected_qty_v2 * 0.01:  # More than 1% unfilled
                log.warning(f"[CARRY] ORPHAN CLEANUP {v2_name}: {remainder_v2:.6f} unfilled on leg 2. Attempting IOC cleanup...")
                try:
                    use_reduce_only = v2_name != "AS"
                    cleanup_result = await v2_obj.place_order(s2, side2, remainder_v2, ioc=True, reduce_only=use_reduce_only)
                    cleanup_filled = float(cleanup_result.get("filled", 0.0))
                    if cleanup_filled > 0:
                        log.info(f"[CARRY] ORPHAN CLEANUP {v2_name}: Recovered {cleanup_filled:.6f} (IOC)")
                    else:
                        log.error(f"[CARRY] ORPHAN CLEANUP {v2_name}: Failed to recover {remainder_v2:.6f}. MANUAL INTERVENTION NEEDED!")
                except Exception as e:
                    log.error(f"[CARRY] ORPHAN CLEANUP {v2_name} exception: {e}")

            # FIX 2026-01-25: Query actual venue positions and AUTO-CLOSE any residuals
            # Force-close dust positions instead of just logging warnings
            # Increased from $5 to $50 to catch maker exit residuals (typically $10-$200)
            DUST_THRESHOLD_USD = 50.0
            for venue_obj, symbol, venue_name, exit_side in [(v1_obj, s1, v1_name, side1), (v2_obj, s2, v2_name, side2)]:
                try:
                    positions = await venue_obj.get_positions()
                    for p in positions:
                        p_sym = p.get("symbol", p.get("coin", "")).upper()
                        if symbol.upper() in p_sym or p_sym in symbol.upper():
                            p_qty = abs(float(p.get("size", p.get("qty", p.get("positionAmt", 0.0)))))
                            p_size_usd = float(p.get("size_usd", 0))
                            if p_size_usd <= 0:
                                entry_px = float(p.get("entry", p.get("entryPx", p.get("entryPrice", 0))) or 0)
                                p_size_usd = p_qty * entry_px if entry_px > 0 else 0

                            if p_qty > 0.0001:
                                if p_size_usd < DUST_THRESHOLD_USD:
                                    # AUTO-CLOSE dust with market order
                                    log.warning(f"[CARRY] DUST RESIDUAL on {venue_name}: {symbol} has {p_qty:.6f} (${p_size_usd:.2f}) - AUTO-CLOSING")
                                    try:
                                        use_reduce_only = venue_name != "AS"
                                        cleanup_result = await venue_obj.place_order(symbol, exit_side, p_qty, ioc=True, reduce_only=use_reduce_only)
                                        cleanup_filled = float(cleanup_result.get("filled", 0.0))
                                        if cleanup_filled > 0:
                                            log.info(f"[CARRY] DUST CLOSED on {venue_name}: {symbol} filled {cleanup_filled:.6f}")
                                        else:
                                            log.warning(f"[CARRY] DUST CLEANUP on {venue_name}: 0 fill for {p_qty:.6f}")
                                    except Exception as ce:
                                        log.error(f"[CARRY] DUST CLEANUP {venue_name} exception: {ce}")
                                else:
                                    log.error(f"[CARRY] ORPHAN DETECTED on {venue_name}: {symbol} has {p_qty:.6f} (${p_size_usd:.2f}) - TOO LARGE FOR AUTO-CLOSE!")
                except Exception as e:
                    log.debug(f"[CARRY] Position verification for {venue_name} failed: {e}")

        except Exception as e:
            log.warning(f"[CARRY] Post-exit verification failed: {e}")

    def _remove_position(self, sym: str, reason: str, pnl_price: float, pnl_funding: float, equity: float,
                         exit_v1_maker: bool = None, exit_v2_maker: bool = None):
        """Cleanly remove position and save state.

        FIX 2026-01-22: Added exit_v1_maker, exit_v2_maker params for order type tracking.
        """
        if sym in self.positions:
            pos = self.positions[sym]

            parts = pos.direction.split(" / ")
            v1_n = parts[0].split()[-1].upper()
            v2_n = parts[1].split()[-1].upper()

            # FIX 2026-01-21: Log actual funding rates and keep more exit details
            r1 = self._cached_hl_rates.get(pos.hl_coin, 0.0) if v1_n == "HL" else (
                 self._cached_as_rates.get(pos.symbol, 0.0) if v1_n == "AS" else
                 self._cached_lt_rates.get(pos.hl_coin, 0.0))
            r2 = self._cached_hl_rates.get(pos.hl_coin, 0.0) if v2_n == "HL" else (
                 self._cached_as_rates.get(pos.symbol, 0.0) if v2_n == "AS" else
                 self._cached_lt_rates.get(pos.hl_coin, 0.0))

            # Keep detailed reason (replace parentheses with brackets for CSV safety)
            # e.g., "mtm_stop(MTM=16.3 < Floor=186.8)" -> "EXIT_MTM_STOP[MTM=16.3<Floor=186.8]"
            action_base = reason.split('(')[0].upper().replace(' ', '_')
            if '(' in reason:
                details_raw = reason.split('(', 1)[1].rstrip(')')
                # Compact details for CSV: remove spaces, shorten
                details = details_raw.replace(' ', '').replace('bps', '').replace('$', '')[:50]
                action = f"EXIT_{action_base}[{details}]"
            else:
                action = f"EXIT_{action_base}"

            # FIX 2026-01-22: Log exit maker/taker status
            self._log(sym, action, pos.direction, pos.size_usd,
                      r1, r2, v1_n, v2_n,
                      hold_hours=(time.time()-pos.entry_time)/3600,
                      price_pnl=pnl_price,
                      funding_pnl=pnl_funding,
                      equity=equity,
                      v1_maker=exit_v1_maker,
                      v2_maker=exit_v2_maker)

            # Track W/L stats
            total_pnl = pnl_price + pnl_funding
            self.total_exits += 1
            if total_pnl > 0:
                self.wins += 1
            elif total_pnl < 0:
                self.losses += 1

            # Track hourly stats
            self._hourly_stats["exits"] += 1
            self._hourly_stats["pnl_usd"] += total_pnl
            self._hourly_stats["funding_pnl_usd"] += pnl_funding
            self._hourly_stats["price_pnl_usd"] += pnl_price
            if total_pnl > 0:
                self._hourly_stats["wins"] += 1
            elif total_pnl < 0:
                self._hourly_stats["losses"] += 1

            # BUGFIX: Track realized PnL in live mode for dashboard display
            if not self.paper_mode and not self.dry_run_live:
                self._live_realized_pnl += total_pnl
                log.info(f"[CARRY] Live realized PnL updated: +${total_pnl:.2f} (cumulative: ${self._live_realized_pnl:.2f})")

            # PHASE 2 FIX (2026-01-22): Track session realized PNL for all modes
            self._session_realized_pnl += total_pnl
            log.info(f"[CARRY] Session realized PnL: +${total_pnl:.2f} (session total: ${self._session_realized_pnl:.2f})")

            # BUGFIX: Track symbol performance and auto-blocklist losers
            self._track_symbol_pnl(pos.hl_coin, total_pnl)

            # FIX 2026-01-25: Track recently-closed to prevent false orphan alerts
            self._recently_closed[pos.hl_coin] = time.time()
            del self.positions[sym]
            self._save_state()

    async def close_all_positions(self, reason: str = "DASHBOARD_CLOSE_ALL") -> dict:
        """
        Close all open carry positions. Called from dashboard buttons.
        Returns dict with list of closed positions and any errors.
        """
        log.warning(f"[CARRY] CLOSE ALL POSITIONS triggered: {reason}")
        result = {"closed": [], "errors": [], "total_pnl": 0.0}

        if not self.positions:
            log.info("[CARRY] No positions to close")
            return result

        # Copy keys to avoid dict modification during iteration
        syms_to_close = list(self.positions.keys())

        for sym in syms_to_close:
            pos = self.positions.get(sym)
            if not pos:
                continue

            try:
                mtm = self._calc_mtm(pos)
                pnl_price = mtm.get("price_pnl_usd", 0.0)
                pnl_funding = mtm.get("funding_pnl_usd", 0.0)
                total_pnl = mtm.get("total_mtm_usd", 0.0)

                success = await self._execute_exit(sym, reason, pnl_price, pnl_funding)

                if success:
                    result["closed"].append({
                        "symbol": sym,
                        "coin": pos.hl_coin,
                        "direction": pos.direction,
                        "size_usd": pos.size_usd,
                        "pnl_usd": total_pnl
                    })
                    result["total_pnl"] += total_pnl
                    log.info(f"[CARRY] Closed {sym}: PnL=${total_pnl:.2f}")
                else:
                    result["errors"].append({"symbol": sym, "error": "Exit failed"})
                    log.error(f"[CARRY] Failed to close {sym}")

            except Exception as e:
                log.error(f"[CARRY] Exception closing {sym}: {e}")
                result["errors"].append({"symbol": sym, "error": str(e)})

        log.warning(f"[CARRY] CLOSE ALL complete: {len(result['closed'])} closed, {len(result['errors'])} errors, Total PnL=${result['total_pnl']:.2f}")
        return result

    async def close_single_position(self, symbol: str) -> dict:
        """
        Close a single position by symbol. Called from dashboard manual close button.
        Returns dict with success status and message.
        """
        log.warning(f"[CARRY] MANUAL CLOSE requested for {symbol}")
        result = {"success": False, "message": "", "pnl_usd": 0.0}

        pos = self.positions.get(symbol)
        if not pos:
            result["message"] = f"Position {symbol} not found"
            log.warning(f"[CARRY] Manual close failed: {symbol} not found in positions")
            return result

        try:
            mtm = self._calc_mtm(pos)
            pnl_price = mtm.get("price_pnl_usd", 0.0)
            pnl_funding = mtm.get("funding_pnl_usd", 0.0)
            total_pnl = mtm.get("total_mtm_usd", 0.0)

            success = await self._execute_exit(symbol, "MANUAL_CLOSE_DASHBOARD", pnl_price, pnl_funding)

            if success:
                result["success"] = True
                result["pnl_usd"] = total_pnl
                result["message"] = f"Closed {symbol}: PnL=${total_pnl:.2f}"
                log.info(f"[CARRY] Manual close success: {symbol}, PnL=${total_pnl:.2f}")
            else:
                result["message"] = f"Failed to close {symbol}"
                log.error(f"[CARRY] Manual close failed: {symbol}")

        except Exception as e:
            log.error(f"[CARRY] Exception during manual close {symbol}: {e}")
            result["message"] = f"Error: {e}"

        return result

    async def _check_orphans(self) -> None:
        """
        Phase 4.1: Detect orphan positions (size_usd=0) and attempt recovery.
        Orphans occur when Leg 1 exit succeeds but Leg 2 fails.
        """
        orphans = [sym for sym, pos in list(self.positions.items()) if pos.size_usd <= 0]

        if not orphans:
            return

        for sym in orphans:
            pos = self.positions.get(sym)
            if not pos:
                continue

            log.warning(f"[CARRY] ORPHAN DETECTED: {sym} - Attempting recovery (close remaining Leg 2)")

            # Parse venue info from direction
            parts = pos.direction.split(" / ")
            v2_name = parts[1].split()[-1].upper() if len(parts) > 1 else "?"

            # Determine which venue has the remaining position
            v2_obj = None
            s2 = None

            if "HL" in v2_name:
                v2_obj = self.hl
                s2 = pos.hl_coin
            elif "AS" in v2_name:
                v2_obj = self.asr
                s2 = pos.symbol
            elif "LT" in v2_name and self.lighter:
                v2_obj = self.lighter
                # FIX 2026-01-22: Also check as_lt_map for AS-LT exclusive pairs
                s2 = self.hl_to_lighter.get(pos.hl_coin) or self.as_lt_map.get(pos.symbol)

            if not v2_obj or not s2:
                log.error(f"[CARRY] Cannot recover orphan {sym}: Unknown venue {v2_name}")
                # Remove stale orphan after 1 hour
                if (time.time() - pos.entry_time) > 3600:
                    log.warning(f"[CARRY] Removing stale orphan {sym} (age > 1h)")
                    del self.positions[sym]
                    self._save_state()
                continue

            # Determine close direction (opposite of entry)
            # Entry direction in v2 is stored in direction string
            entry_side_v2 = "SELL" if "long" in parts[1].lower() else "BUY"
            close_side = "BUY" if entry_side_v2 == "SELL" else "SELL"

            # Try to close the orphan leg
            try:
                # Get current position size from venue (if possible)
                bbo = await v2_obj.get_bbo(s2)
                if not bbo or not bbo[0]:
                    log.warning(f"[CARRY] Orphan {sym}: Cannot get BBO for {s2}")
                    continue

                # Use original entry price to estimate qty
                est_qty = pos.entry_qty_v2 if hasattr(pos, 'entry_qty_v2') else 0.001  # Fallback
                qty = v2_obj.round_qty(s2, est_qty)
                mid_px = (bbo[0] + bbo[1]) * 0.5

                # BUGFIX: Aster $5 minimum notional - skip tiny orphans
                notional = qty * mid_px
                if v2_name == "AS" and notional < 5.0:
                    log.warning(f"[CARRY] Orphan {sym} below Aster $5 min (${notional:.2f}), removing from tracking without close")
                    hold_hours = (time.time() - pos.entry_time) / 3600.0
                    # Log the orphan removal
                    self._log(
                        sym, "EXIT_ORPHAN_SKIP", pos.direction, notional,
                        pos.entry_funding_hl, pos.entry_funding_as, "?", v2_name,
                        hold_hours, 0.0, 0.0, self.paper_equity if self.paper_mode else 0.0
                    )
                    del self.positions[sym]
                    self._save_state()
                    continue

                orphan_is_maker = None  # Track maker status for orphan close
                if self.dry_run_live:
                    log.info(f"[CARRY DRY-RUN] WOULD CLOSE ORPHAN: {v2_name} {s2} {close_side} qty={qty:.6f}")
                else:
                    log.info(f"[CARRY] Closing orphan leg: {v2_name} {s2} {close_side} qty={qty:.6f}")
                    # CRITICAL: Aster doesn't support reduce_only=True
                    use_reduce_only = v2_name != "AS"
                    res = await v2_obj.place_order(s2, close_side, qty, ioc=True, reduce_only=use_reduce_only)
                    filled = float(res.get("filled", 0.0))
                    orphan_is_maker = res.get("is_maker", False)  # FIX 2026-01-22: Track maker status

                    if filled > 0:
                        log.info(f"[CARRY] Orphan {sym} recovered! Filled: {filled} (maker={orphan_is_maker})")
                    else:
                        log.warning(f"[CARRY] Orphan {sym} recovery failed: {res.get('status')}")
                        continue  # Don't delete, retry next tick

                # BUGFIX: Log orphan exit to carry_audit.csv
                hold_hours = (time.time() - pos.entry_time) / 3600.0
                # FIX 2026-01-22: Add maker/taker visibility (only V2 for orphan recovery)
                self._log(
                    sym, "EXIT_ORPHAN", pos.direction, notional,
                    pos.entry_funding_hl, pos.entry_funding_as, "?", v2_name,
                    hold_hours, 0.0, 0.0, self.paper_equity if self.paper_mode else 0.0,
                    v1_maker=None, v2_maker=orphan_is_maker
                )

                # Remove orphan from tracking
                del self.positions[sym]
                self._save_state()
                log.info(f"[CARRY] Orphan {sym} removed from tracking")

            except Exception as e:
                log.error(f"[CARRY] Orphan recovery error for {sym}: {e}")

    def _log_heartbeat(self):
        """Phase 3: Log operational heartbeat stats with hourly performance summary."""
        try:
            total_exp = sum(p.size_usd for p in list(self.positions.values()))

            # Current position summary
            log.info(f"[HEARTBEAT] ========== HOURLY PERFORMANCE SUMMARY ==========")
            log.info(f"[HEARTBEAT] Positions: {len(self.positions)} | Exposure: ${total_exp:.0f} | Equity: ${self._cached_total_equity:.0f}")

            # Hourly stats
            hs = self._hourly_stats
            winrate = f"{hs['wins']}/{hs['exits']}" if hs['exits'] > 0 else "N/A"
            winrate_pct = f"({100*hs['wins']/hs['exits']:.0f}%)" if hs['exits'] > 0 else ""
            pnl_color = "+" if hs["pnl_usd"] >= 0 else ""

            log.info(f"[HEARTBEAT] Hourly Entries: {hs['entries']} | Exits: {hs['exits']} | W/L: {winrate} {winrate_pct}")
            log.info(f"[HEARTBEAT] Hourly PnL: {pnl_color}${hs['pnl_usd']:.2f} (Funding: {pnl_color}${hs['funding_pnl_usd']:.2f}, Price: ${hs['price_pnl_usd']:.2f})")

            # Session totals
            session_pnl = (self.paper_equity - 1000.0) if self.paper_mode else self._live_realized_pnl
            mode = "PAPER" if self.paper_mode else ("DRY-RUN" if self.dry_run_live else "LIVE")
            log.info(f"[HEARTBEAT] Session [{mode}]: Entries={self.total_entries} | Exits={self.total_exits} | W={self.wins}/L={self.losses} | Realized PnL=${session_pnl:.2f}")

            # FIX 2026-01-25: Log orphan exposure summary if any exists
            total_orphan = sum(self._orphan_deltas.values()) if self._orphan_deltas else 0.0
            if abs(total_orphan) > 1.0:
                orphan_detail = ", ".join(f"{k}:${v:.0f}" for k, v in self._orphan_deltas.items() if abs(v) > 1.0)
                log.info(f"[HEARTBEAT] ORPHAN EXPOSURE: Total=${total_orphan:.2f} | Details: {orphan_detail}")

            # FIX 2026-01-24: Session equity MTM tracking (start vs current)
            if self._session_start_equity_captured and self._session_start_equity > 0:
                equity_mtm = self._cached_total_equity - self._session_start_equity
                equity_pct = (equity_mtm / self._session_start_equity) * 100 if self._session_start_equity > 0 else 0.0
                sign = "+" if equity_mtm >= 0 else ""
                log.info(f"[HEARTBEAT] Equity: Start=${self._session_start_equity:.2f} | Current=${self._cached_total_equity:.2f} | MTM={sign}${equity_mtm:.2f} ({sign}{equity_pct:.2f}%)")
            log.info(f"[HEARTBEAT] ==============================================")

            # Send Discord notification (if notifier available)
            if self.notifier:
                summary = (
                    f"**Hourly Summary [{mode}]**\n"
                    f"Positions: {len(self.positions)} | Exposure: ${total_exp:.0f}\n"
                    f"Entries: {hs['entries']} | Exits: {hs['exits']} | W/L: {winrate} {winrate_pct}\n"
                    f"Hourly PnL: {pnl_color}${hs['pnl_usd']:.2f}\n"
                    f"Session PnL: ${session_pnl:.2f}"
                )
                asyncio.create_task(self.notifier.notify("Carry Hourly", summary))

            # Reset hourly stats for next hour
            self._hourly_stats = {
                "entries": 0,
                "exits": 0,
                "wins": 0,
                "losses": 0,
                "pnl_usd": 0.0,
                "funding_pnl_usd": 0.0,
                "price_pnl_usd": 0.0
            }

        except Exception as e:
            log.warning(f"[HEARTBEAT] Error logging heartbeat: {e}")

    # =========================================================================
    # SECTION 6: ENTRY LOGIC
    # =========================================================================

    def _get_drawdown_multiplier(self) -> float:
        """Reduce position size based on current drawdown (P2 Fix)."""
        if self._cached_total_equity <= 0:
            return 1.0

        # Update peak
        if self._cached_total_equity > self._peak_equity:
            self._peak_equity = self._cached_total_equity

        # Calculate drawdown
        if self._peak_equity <= 0:
            return 1.0
        drawdown_pct = (self._peak_equity - self._cached_total_equity) / self._peak_equity

        # Size reduction based on drawdown
        if drawdown_pct >= 0.10:  # -10% drawdown
            return 0.25  # 75% size reduction
        elif drawdown_pct >= 0.05:  # -5% drawdown
            return 0.50  # 50% size reduction
        elif drawdown_pct >= 0.02:  # -2% drawdown
            return 0.75  # 25% size reduction
        else:
            return 1.0  # Full size

    async def _check_entries(self, hl_rates: Dict[str, float], as_rates: Dict[str, float], lt_rates: Dict[str, float] = None) -> None:
        """Look for new carry opportunities with fee accounting."""
        lt_rates = lt_rates or {}
        now = time.time()

        # FIX 2026-01-25: Fetch Aster funding intervals for rate normalization
        # Aster has variable intervals per symbol (1h, 4h, 8h), must normalize to 8h basis
        as_intervals = await self.asr.get_funding_intervals() if hasattr(self.asr, 'get_funding_intervals') else {}
        # Log non-standard intervals for debugging (once per session)
        if as_intervals and not hasattr(self, '_logged_intervals'):
            non_std_count = sum(1 for i in as_intervals.values() if i != 8)
            if non_std_count > 0:
                log.info("[CARRY] Aster has %d symbols with non-8h funding intervals", non_std_count)
            self._logged_intervals = True

        # VENUE HEALTH CHECK - Block entries if venues degraded (P2 Fix)
        # FIX 2026-01-22: Reduced to debug level - keep in background
        if not self._health_monitor.is_healthy("HL"):
            hl_errors = self._health_monitor.get_error_count("HL")
            log.debug(f"[CARRY] Skipping entries: HL degraded ({hl_errors} errors/30s)")
            return
        if not self._health_monitor.is_healthy("AS"):
            as_errors = self._health_monitor.get_error_count("AS")
            log.debug(f"[CARRY] Skipping entries: Aster degraded ({as_errors} errors/30s)")
            return

        # NOTE: Per-pair staleness checks happen later in the loop (get_fresh_bbo with REST fallback)
        # Global circuit breaker removed - was too aggressive and blocked all opportunities

        # FUNDING CYCLE CHECK - Warn but don't block entries close to funding
        # (Reduced from 60 to 15 minutes - only warn, don't block)
        minutes_to_funding = self._get_minutes_until_funding()
        if minutes_to_funding < 15:
            if not hasattr(self, '_last_funding_skip_log') or (now - self._last_funding_skip_log) > 60:
                log.info(f"[CARRY] Note: {minutes_to_funding:.0f}min until funding cycle (entries still allowed)")
                self._last_funding_skip_log = now
            # Don't return - let entries proceed, they may still capture funding

        # 1. Allocation & Exposure Check
        current_exposure = sum(p.size_usd for p in list(self.positions.values()))
        total_equity = await self._get_total_equity()
        max_notional = total_equity * self.max_carry_alloc_pct
        at_max_alloc = current_exposure >= max_notional
        
        opportunities: List[Tuple[str, str, str, str, float, float, float, Tuple[str, str]]] = []

        # FIX 2026-01-21: Removed hardcoded EST_FEES_BPS = 15.0
        # Now using self._get_roundtrip_fees_bps(v1, v2) for accurate per-venue-pair fees

        for as_sym, hl_coin in self.pairs_map.items():
            # FIX 2026-01-24: Check config blocked_bases FIRST (XAU, XAG, etc.)
            # This was checked too late (at execution) causing fee waste on blocked symbols
            if hasattr(self, '_blocked_bases') and hl_coin.upper() in self._blocked_bases:
                continue  # Silently skip - these are permanently blocked

            # Check auto-blocklist (too many reversals)
            if self._is_blocklisted(hl_coin):
                log.info(f"[CARRY] Skipping {hl_coin} (AS: {as_sym}): BLOCKLISTED")
                continue

            # Check entry failure cooldown (recent failed entry)
            if self._is_entry_cooldown(hl_coin):
                continue

            # NOTE: forced_exit_cooldown check moved to opportunity processing loop
            # where direction is known, enabling inverted re-entry (see line ~4645)

            # Check allocation for this specific symbol
            # FIX 2026-01-25: Use yield-tiered allocation cap based on position's current yield
            pos = self.positions.get(as_sym)
            if pos:
                mtm = self._calc_mtm(pos)
                pos_yield = abs(mtm.get("curr_diff_bps", pos.entry_diff_bps))
                pos_cap = self._get_yield_tiered_allocation_cap(pos_yield)
                if pos.size_usd >= (total_equity * pos_cap):
                    continue

            # If several AS symbols map to same HL coin, check that too
            if not pos and any(p.hl_coin == hl_coin for p in list(self.positions.values())):
                continue
            
            # --- Gather Rates ---
            # All rates are normalized to 8h basis for comparison
            # HL: Returns HOURLY rate → multiply by 8 for 8h equivalent
            r_hl = (hl_rates.get(hl_coin) or 0.0) * 8.0
            # AS: Returns PER-INTERVAL rate (4h or 8h) → normalize to 8h
            as_interval = as_intervals.get(as_sym, 8)
            r_as = (as_rates.get(as_sym) or 0.0) * (8.0 / as_interval)
            # LT: Returns 8H rate directly → NO multiplier needed
            lt_sym = self.hl_to_lighter.get(hl_coin)
            r_lt = (lt_rates.get(lt_sym) or 0.0) if lt_sym and lt_rates else None

            log.debug(f"[CARRY] {as_sym} rates: HL={r_hl:.6f} AS={r_as:.6f} (interval={as_interval}h) LT={r_lt}")

            # --- Define Pairs to Check ---
            venue_pairs = [("HL", r_hl, hl_coin), ("AS", r_as, as_sym)]
            if self.lighter and r_lt is not None:
                venue_pairs.append(("LT", r_lt, lt_sym))
                
            # Cross-check all pairs
            for i in range(len(venue_pairs)):
                for j in range(i + 1, len(venue_pairs)):
                    v1_n, r1, s1 = venue_pairs[i]
                    v2_n, r2, s2 = venue_pairs[j]
                    if r1 is None or r2 is None: continue

                    raw_diff_bps = (r1 - r2) * 10000.0
                    abs_diff_bps = abs(raw_diff_bps)

                    # --- SANITY CHECK: Alert on anomalously high funding rate differential ---
                    # Rates >400 bps are suspicious and warrant manual verification
                    FUNDING_SANITY_THRESHOLD_BPS = 400.0
                    if abs_diff_bps > FUNDING_SANITY_THRESHOLD_BPS:
                        log.warning(f"[CARRY] FUNDING SANITY ALERT {hl_coin}: {abs_diff_bps:.1f}bps differential is unusually high! "
                                   f"V1({v1_n})={r1*10000:.1f}bps V2({v2_n})={r2*10000:.1f}bps - Verify data accuracy before trading")
                        if self.notifier and not hasattr(self, f'_funding_sanity_alerted_{hl_coin}'):
                            setattr(self, f'_funding_sanity_alerted_{hl_coin}', True)
                            asyncio.create_task(self.notifier.notify(
                                f"FUNDING SANITY ALERT: {hl_coin}",
                                f"Diff={abs_diff_bps:.1f}bps is >400bps. V1({v1_n})={r1*10000:.1f}bps V2({v2_n})={r2*10000:.1f}bps. Verify data!"
                            ))

                    # --- Net Yield Check (Fees + Slippage) ---
                    # Get BBOs for slippage checking (with REST fallback for stale WS data)
                    v_map = {"HL": self.hl, "AS": self.asr, "LT": self.lighter}
                    v1_obj = v_map[v1_n]
                    v2_obj = v_map[v2_n]

                    # FIX: Use get_fresh_bbo with REST fallback instead of get_bbo alone
                    # This fetches from REST API if WebSocket data is stale
                    MAX_ENTRY_BBO_AGE_MS = 5000.0  # 5 seconds max - stricter to reduce stale entries

                    # Try cached BBO first, then REST fallback if stale
                    if hasattr(v1_obj, 'get_fresh_bbo'):
                        b1 = await v1_obj.get_fresh_bbo(s1, MAX_ENTRY_BBO_AGE_MS)
                    else:
                        b1 = v1_obj.get_bbo(s1)

                    if hasattr(v2_obj, 'get_fresh_bbo'):
                        b2 = await v2_obj.get_fresh_bbo(s2, MAX_ENTRY_BBO_AGE_MS)
                    else:
                        b2 = v2_obj.get_bbo(s2)

                    if not b1 or not b2 or (b1[0]+b1[1])<=0 or (b2[0]+b2[1])<=0:
                        # Log but don't spam - BBO unavailable after REST fallback
                        log.debug(f"[CARRY] No BBO for {hl_coin} after REST fallback: b1={b1} b2={b2}")
                        continue

                    # P2-5 FIX 2026-01-23: Validate orderbook depth
                    # 2026-01-24: Use aggregated depth across 5 levels for better liquidity estimate
                    MIN_DEPTH_USD = 100.0

                    # Use aggregated depth (sum of 5 levels) for better Aster/Lighter liquidity estimates
                    v1_depth_usd = v1_obj.get_aggregated_depth_usd(s1, levels=5) if hasattr(v1_obj, 'get_aggregated_depth_usd') else 0.0
                    v2_depth_usd = v2_obj.get_aggregated_depth_usd(s2, levels=5) if hasattr(v2_obj, 'get_aggregated_depth_usd') else 0.0

                    if v1_depth_usd < MIN_DEPTH_USD or v2_depth_usd < MIN_DEPTH_USD:
                        log.debug(f"[CARRY] Skip {hl_coin}: Low depth V1=${v1_depth_usd:.0f} V2=${v2_depth_usd:.0f}")
                        self._log_opp_audit(hl_coin, abs_diff_bps, "LOW_DEPTH",
                                          f"V1=${v1_depth_usd:.0f} V2=${v2_depth_usd:.0f}", f"{v1_n}/{v2_n}")
                        continue

                    # After get_fresh_bbo, check if we still have stale data (REST also failed)
                    is_stale = False
                    if hasattr(v1_obj, 'get_bbo_age_ms'):
                        age1 = v1_obj.get_bbo_age_ms(s1)
                        if 0 < age1 < 100000 and age1 > MAX_ENTRY_BBO_AGE_MS:
                            is_stale = True
                            log.debug(f"[CARRY] V1 {v1_n} {s1} BBO still stale after REST: {age1:.0f}ms")
                    if hasattr(v2_obj, 'get_bbo_age_ms') and not is_stale:
                        age2 = v2_obj.get_bbo_age_ms(s2)
                        if 0 < age2 < 100000 and age2 > MAX_ENTRY_BBO_AGE_MS:
                            is_stale = True
                            log.debug(f"[CARRY] V2 {v2_n} {s2} BBO still stale after REST: {age2:.0f}ms")
                    if is_stale:
                        self._log_opp_audit(hl_coin, abs_diff_bps, "STALE_DATA",
                                          "BBO data stale after REST fallback", f"{v1_n}/{v2_n}")
                        continue

                    # SPREAD WIDTH CHECK - Reject if spread too wide (illiquidity)
                    MAX_SPREAD_PCT = 0.01  # 1% max spread
                    mid1 = (b1[0] + b1[1]) / 2 if b1 else 0
                    mid2 = (b2[0] + b2[1]) / 2 if b2 else 0
                    if mid1 > 0 and mid2 > 0:
                        spread1_pct = (b1[1] - b1[0]) / mid1
                        spread2_pct = (b2[1] - b2[0]) / mid2
                        if spread1_pct > MAX_SPREAD_PCT or spread2_pct > MAX_SPREAD_PCT:
                            log.debug(f"[CARRY] Skip {hl_coin}: spread too wide V1={spread1_pct*100:.2f}% V2={spread2_pct*100:.2f}%")
                            self._log_opp_audit(hl_coin, abs_diff_bps, "WIDE_SPREAD",
                                              f"Spread too wide: {max(spread1_pct,spread2_pct)*100:.1f}%", f"{v1_n}/{v2_n}")
                            continue

                    slip = self._slip_bps(b1, b2)
                    roundtrip_fees = self._get_roundtrip_fees_bps(v1_n, v2_n)
                    net_yield = abs_diff_bps - (slip + self.slip_buffer + roundtrip_fees)
                
                    # --- Dynamic Entry Threshold (Liquidity + Venue-Aware) ---
                    # PHASE 3 FIX (2026-01-22): Venue-aware thresholds based on fee structure
                    # Lower fees = lower threshold = more entries on profitable pairs
                    base_threshold = self._get_venue_aware_threshold(v1_n, v2_n)

                    # Retrieve 24h volume for the pair (use HL volume as proxy)
                    hl_vol = 0.0
                    try:
                        # Access the hl venue's cached volume from metaAndAssetCtxs
                        hl_vol = getattr(self.hl, "_cached_vols", {}).get(hl_coin, 0.0)
                    except Exception: pass

                    # Determine effective threshold: premium (120bps) if < 500k vol, else venue-aware
                    effective_threshold = base_threshold
                    is_premium_tier = hl_vol < self.min_vol_premium
                    if is_premium_tier:
                        effective_threshold = self.premium_funding_bps

                    # SCALE-UP THRESHOLD REDUCTION: If we already have a position with good entry yield,
                    # allow scale-ups at 50% of normal threshold (we're already in the trade)
                    pair_key = f"{hl_coin}_{v1_n}_{v2_n}"  # Define early for all code paths

                    # FIX (2026-01-22): Venue pair matching for scale-ups
                    # Only treat as scale-up if the venue pair matches the existing position's venue pair
                    is_same_venue_pair = False
                    if pos is not None:
                        # Extract venue pair from position direction, e.g. "Short AS / Long LT" -> ("AS", "LT")
                        try:
                            parts = pos.direction.split(" / ")
                            pos_v1 = parts[0].split()[-1].upper()  # "AS" from "Short AS"
                            pos_v2 = parts[1].split()[-1].upper()  # "LT" from "Long LT"
                            # Check if current pair matches (order doesn't matter)
                            current_pair = (v1_n.upper(), v2_n.upper())
                            is_same_venue_pair = (pos_v1, pos_v2) == current_pair or (pos_v2, pos_v1) == current_pair
                        except Exception:
                            is_same_venue_pair = False

                    is_scale_up = pos is not None and pos.scale_up_count < self.max_scale_ups_per_position and is_same_venue_pair
                    if is_scale_up:
                        # For scale-ups: require meaningful yield to avoid diluting position
                        # FIX: Changed from 25bps (50% of min) to fixed 50bps to prevent yield dilution
                        scale_up_threshold = 50.0  # Fixed 50bps threshold for scale-ups

                        # PHASE 3 FIX (2026-01-22): Adaptive scale-up with confidence tiers
                        # Higher current yield = more confidence = looser requirements
                        current_yield_bps = abs_diff_bps  # Raw funding differential (unsigned)
                        entry_yield_bps = abs(pos.entry_diff_bps)

                        # Confidence-based minimum yield multiplier
                        if current_yield_bps >= 100.0:
                            # High conviction: allow 15% tolerance
                            min_yield_multiplier = 0.85
                        elif current_yield_bps >= 75.0:
                            # Medium conviction: allow 10% tolerance
                            min_yield_multiplier = 0.90
                        else:
                            # Standard: require full entry yield
                            min_yield_multiplier = 1.0

                        spread_improving = current_yield_bps >= entry_yield_bps * min_yield_multiplier

                        # FIX 2026-01-24 Issue #2: High-yield override - allow scale-up if yield still > 100 bps
                        # Even if yield is worsening from entry, 100+ bps is still excellent
                        HIGH_YIELD_OVERRIDE_BPS = 100.0
                        is_high_yield = current_yield_bps >= HIGH_YIELD_OVERRIDE_BPS

                        if not spread_improving and not is_high_yield:
                            # Only block if BOTH worsening AND not high-yield
                            min_required = entry_yield_bps * min_yield_multiplier
                            self._log_opp_audit(pair_key, abs_diff_bps, "SKIPPED",
                                              reason="Scale-up spread WORSENING and LOW",
                                              details=f"curr={current_yield_bps:.1f}bps < min={min_required:.1f}bps AND < {HIGH_YIELD_OVERRIDE_BPS}bps")
                            log.info(f"[CARRY] SCALE-UP BLOCKED {hl_coin}: spread worsening AND low ({current_yield_bps:.1f}bps < {HIGH_YIELD_OVERRIDE_BPS}bps override)")
                            continue

                        if not spread_improving and is_high_yield:
                            log.info(f"[CARRY] SCALE-UP ALLOWED {hl_coin}: yield worsening BUT still high "
                                    f"({current_yield_bps:.1f}bps > {HIGH_YIELD_OVERRIDE_BPS}bps override)")

                        # FIX 2026-01-22: Require profit before FIRST scale-up only
                        # Subsequent scale-ups can proceed even if underwater
                        if pos.scale_up_count == 0:
                            mtm_check = self._calc_mtm(pos)
                            total_pnl = mtm_check.get("total_mtm_usd", 0)
                            if total_pnl <= 0:
                                self._log_opp_audit(pair_key, abs_diff_bps, "SKIPPED",
                                                  reason="Scale-up requires profit first",
                                                  details=f"PnL=${total_pnl:.2f} (need >0 for 1st scale-up)")
                                log.info(f"[CARRY] SCALE-UP BLOCKED {hl_coin}: Wait for profit before 1st scale-up (PnL: ${total_pnl:.2f})")
                                continue

                        if net_yield >= scale_up_threshold:
                            log.info(f"[CARRY] SCALE-UP QUALIFIED {hl_coin}: net={net_yield:.1f}bps curr={current_yield_bps:.1f}bps entry={entry_yield_bps:.1f}bps (tier={min_yield_multiplier})")
                            # Skip the normal threshold check - proceed to opportunity
                            pass  # Fall through to add opportunity
                        else:
                            # Even scale-up doesn't meet minimum threshold
                            self._log_opp_audit(pair_key, abs_diff_bps, "SKIPPED",
                                              reason=f"Scale-up below min ({scale_up_threshold:.0f}bps)",
                                              details=f"net={net_yield:.1f} vol=${hl_vol/1000:.0f}K slip={slip:.1f}")
                            continue
                    else:
                        # NEW ENTRY: Apply full threshold
                        # Log if this is a different venue pair for existing position
                        if pos is not None and not is_same_venue_pair:
                            log.debug(f"[CARRY] {hl_coin}: position exists on different venue pair ({pos.direction}), treating {v1_n}-{v2_n} as NEW ENTRY")

                        if net_yield < effective_threshold:
                            # Log if it has SOME yield (>10bps) so user sees why it was skipped
                            if abs_diff_bps >= 10.0:
                                reason = f"Low Net Edge (<{effective_threshold})"
                                if is_premium_tier and net_yield >= self.min_funding_bps:
                                     reason = f"Low Liquidity Premium (<{self.premium_funding_bps})"

                                self._log_opp_audit(pair_key, abs_diff_bps, "SKIPPED",
                                                  reason=reason,
                                                  details=f"net={net_yield:.1f} vol=${hl_vol/1000:.0f}K slip={slip:.1f}")
                            # Reset confirmation if opportunity dropped below threshold
                            if pair_key in self._confirm_count:
                                self._confirm_count[pair_key] = 0
                            continue

                    direction = f"Short {v1_n} / Long {v2_n}" if raw_diff_bps > 0 else f"Long {v1_n} / Short {v2_n}"

                    # 4. Scaling Safety (MTM Check)
                    if pos:
                        mtm = self._calc_mtm(pos)
                        if mtm["mtm_bps"] < -30.0:
                            self._log_opp_audit(pair_key, abs_diff_bps, "SKIPPED",
                                              reason="Scaling Restricted",
                                              details=f"mtm={mtm['mtm_bps']:.1f}bps too low")
                            continue

                    # AUDIT FIX: Implement confirmation system (was declared but not used)
                    # Require N consecutive ticks with valid opportunity before entering
                    self._confirm_count[pair_key] = self._confirm_count.get(pair_key, 0) + 1
                    confirms = self._confirm_count[pair_key]

                    if confirms < self.required_confirms:
                        self._log_opp_audit(pair_key, abs_diff_bps, "CONFIRMING",
                                          reason=direction,
                                          details=f"net={net_yield:.1f} confirms={confirms}/{self.required_confirms}")
                        continue  # Need more confirmations

                    self._log_opp_audit(pair_key, abs_diff_bps, "DETECTED", reason=direction, details=f"net={net_yield:.1f} confirms={confirms}")
                    # raw_diff_bps is SIGNED for reversal detection; net_yield is always positive
                    opportunities.append((hl_coin, s1, s2, direction, net_yield, r1, r2, (v1_n, v2_n), raw_diff_bps))

        # --- PHASE: AS-LT Direct Pairs (not on HL) ---
        # These are stocks, commodities, etc. that exist on both Aster and Lighter but NOT on Hyperliquid
        if self.as_lt_map and self.lighter and lt_rates:
            for as_sym, lt_sym in self.as_lt_map.items():
                # Use AS symbol as the canonical "coin" identifier (since no HL coin exists)
                base_coin = as_sym.replace("USDT", "").upper()

                # FIX 2026-01-24: Check config blocked_bases FIRST (XAU, XAG, etc.)
                if hasattr(self, '_blocked_bases') and base_coin in self._blocked_bases:
                    continue  # Silently skip - permanently blocked

                # Check auto-blocklist
                if self._is_blocklisted(base_coin):
                    continue

                # Check entry failure cooldown (forced_exit_cooldown checked later with direction)
                if self._is_entry_cooldown(base_coin):
                    continue

                # Check existing position (keyed by as_sym)
                # FIX 2026-01-25: Use yield-tiered allocation cap based on position's current yield
                pos = self.positions.get(as_sym)
                if pos:
                    mtm = self._calc_mtm(pos)
                    pos_yield = abs(mtm.get("curr_diff_bps", pos.entry_diff_bps))
                    pos_cap = self._get_yield_tiered_allocation_cap(pos_yield)
                    if pos.size_usd >= (total_equity * pos_cap):
                        continue

                # Skip if we have any position on this base (different symbol)
                if not pos and any(p.hl_coin == base_coin for p in list(self.positions.values())):
                    continue

                # Gather rates (AS and LT only - no HL)
                # FIX 2026-01-25: Normalize Aster rate to 8h basis (variable intervals per symbol)
                as_interval = as_intervals.get(as_sym, 8)
                r_as = (as_rates.get(as_sym) or 0.0) * (8.0 / as_interval)
                # FIX 2026-01-26: Lighter returns 8h rates directly - NO multiplier
                r_lt = lt_rates.get(lt_sym) or 0.0
                if as_rates.get(as_sym) is None or lt_rates.get(lt_sym) is None:
                    continue

                log.debug(f"[CARRY] AS-LT {as_sym}: AS={r_as:.6f} (interval={as_interval}h) LT={r_lt:.6f}")

                raw_diff_bps = (r_as - r_lt) * 10000.0
                abs_diff_bps = abs(raw_diff_bps)

                # Sanity check
                if abs_diff_bps > 400.0:
                    log.warning(f"[CARRY] AS-LT SANITY ALERT {base_coin}: {abs_diff_bps:.1f}bps is unusually high!")
                    continue

                # Get BBOs
                b_as = await self.asr.get_fresh_bbo(as_sym, 5000.0) if hasattr(self.asr, 'get_fresh_bbo') else self.asr.get_bbo(as_sym)
                b_lt = await self.lighter.get_fresh_bbo(lt_sym, 5000.0) if hasattr(self.lighter, 'get_fresh_bbo') else self.lighter.get_bbo(lt_sym)

                if not b_as or not b_lt or (b_as[0]+b_as[1])<=0 or (b_lt[0]+b_lt[1])<=0:
                    continue

                # Spread width check
                mid_as = (b_as[0] + b_as[1]) / 2
                mid_lt = (b_lt[0] + b_lt[1]) / 2
                spread_as_pct = (b_as[1] - b_as[0]) / mid_as if mid_as > 0 else 1.0
                spread_lt_pct = (b_lt[1] - b_lt[0]) / mid_lt if mid_lt > 0 else 1.0
                if spread_as_pct > 0.01 or spread_lt_pct > 0.01:
                    continue

                # OPTIMIZATION 2026-01-24: Pre-flight Lighter depth check
                # 97.9% of AS-LT failures involve Lighter with $0 liquidity
                # Skip early to prevent fee waste from failed entries
                pair_key_early = f"{base_coin}_AS_LT"
                try:
                    # FIX 2026-01-26: Use get_aggregated_depth_usd which returns total USD depth
                    lt_depth_usd = self.lighter.get_aggregated_depth_usd(lt_sym, levels=3)
                    if lt_depth_usd < 100:
                        self._log_opp_audit(pair_key_early, abs_diff_bps, "SKIPPED",
                                          reason="LT_NO_DEPTH",
                                          details=f"depth=${lt_depth_usd:.0f} (need $100+)")
                        continue
                except Exception as e:
                    self._log_opp_audit(pair_key_early, abs_diff_bps, "SKIPPED",
                                      reason="LT_DEPTH_ERR",
                                      details=str(e)[:50])
                    continue

                # Net yield calculation
                slip = self._slip_bps(b_as, b_lt)
                roundtrip_fees = self._get_roundtrip_fees_bps("AS", "LT")
                net_yield = abs_diff_bps - (slip + self.slip_buffer + roundtrip_fees)

                # Threshold check (use AS-LT fees)
                effective_threshold = self._get_venue_aware_threshold("AS", "LT")

                # Scale-up detection
                pair_key = f"{base_coin}_AS_LT"
                is_scale_up = pos is not None and pos.scale_up_count < self.max_scale_ups_per_position
                if is_scale_up:
                    required_threshold = max(50.0, effective_threshold * 0.5)
                else:
                    required_threshold = effective_threshold

                if net_yield < required_threshold:
                    if pair_key in self._confirm_count:
                        self._confirm_count[pair_key] = 0
                    continue

                # Confirmation
                self._confirm_count[pair_key] = self._confirm_count.get(pair_key, 0) + 1
                confirms = self._confirm_count[pair_key]
                if confirms < self.required_confirms:
                    continue

                # Direction
                direction = f"Short AS / Long LT" if raw_diff_bps > 0 else f"Long AS / Short LT"

                self._log_opp_audit(pair_key, abs_diff_bps, "DETECTED", reason=direction, details=f"AS-LT net={net_yield:.1f}")
                # Use base_coin as hl_coin placeholder, as_sym as s1, lt_sym as s2
                opportunities.append((base_coin, as_sym, lt_sym, direction, net_yield, r_as, r_lt, ("AS", "LT"), raw_diff_bps))

        # 2. Yield Optimization or Multi-Entry
        if not opportunities: return

        # FIX 2026-01-22: Sort opportunities with profit-based prioritization for scale-ups
        # Scale-ups should be prioritized by current PnL (higher profit = higher priority)
        # New entries continue to use net_yield as primary sort key
        def _opp_sort_key(opp):
            hl_coin = opp[0]
            net_yield = opp[4]
            v1_n, v2_n = opp[7]
            fees = self._get_roundtrip_fees_bps(v1_n, v2_n)

            # Check if this is a scale-up (existing position)
            # Use target_sym_check logic same as in processing loop
            target_sym = opp[2] if v1_n == "HL" else opp[1]  # s2 if v1_n==HL else s1
            pos = self.positions.get(target_sym)

            if pos is not None:
                # SCALE-UP: Sort by current profit (higher profit gets priority)
                # This ensures more profitable positions get scaled before less profitable ones
                mtm = self._calc_mtm(pos)
                current_profit = mtm.get("total_mtm_usd", 0)
                # Primary: profit (higher = first), Secondary: yield, Tertiary: lower fees
                return (1, current_profit, net_yield, -fees)  # 1 = scale-ups first
            else:
                # NEW ENTRY: Sort by net_yield (primary) and prefer lower-fee venue pairs (secondary)
                return (0, 0, net_yield, -fees)  # 0 = new entries after scale-ups

        opportunities.sort(key=_opp_sort_key, reverse=True)

        # Multi-Entry: Enter on ALL valid opportunities (different coins), not just the best one
        # Track which HL coins we've already entered on this tick to avoid duplicates
        entered_coins: set = set()

        for opp in opportunities:
            hl_coin, s1, s2, direction, net_yield, r1, r2, (v1_n, v2_n), raw_diff_bps = opp

            # Skip if we already entered on this coin this tick
            if hl_coin in entered_coins:
                continue

            # FIX 2026-01-24: Direction-aware forced exit cooldown
            # Blocks same-direction re-entry, allows inverted re-entry (funding flip)
            if self._is_forced_exit_cooldown(hl_coin, direction):
                log.debug(f"[CARRY] Skip {hl_coin}: in forced exit cooldown for same direction")
                continue

            # FIX: Block NEW entry if we already have a position on the same underlying (hl_coin)
            # This prevents ZK + ZKUSDT double exposure (both would be Long/Short on same underlying)
            # BUT: Allow SCALE-UP if the existing position is on the SAME symbol (same venue pair)
            target_sym_check = s2 if v1_n == "HL" else s1  # Same logic as in _execute_smart_entry
            existing_pos_for_sym = self.positions.get(target_sym_check)

            if not existing_pos_for_sym:
                # NEW ENTRY: No limit on concurrent positions, but check duplicate underlying
                already_has_underlying = any(p.hl_coin == hl_coin for p in list(self.positions.values()))
                if already_has_underlying:
                    log.debug(f"[CARRY] Skip NEW entry {hl_coin}: already have position on same underlying via different symbol")
                    continue
            else:
                # SCALE-UP: Check max scale-ups per position limit (max 2 scale-ups = 3 total entries)
                # FIX: Changed to debug level (these logs spam heartbeat when positions at max)
                log.debug(f"[CARRY] SCALE-UP CHECK {hl_coin}: scale_up_count={existing_pos_for_sym.scale_up_count}, max={self.max_scale_ups_per_position}, size=${existing_pos_for_sym.size_usd:.2f}")
                if existing_pos_for_sym.scale_up_count >= self.max_scale_ups_per_position:
                    log.debug(f"[CARRY] BLOCKED SCALE-UP {hl_coin}: max scale-ups reached ({existing_pos_for_sym.scale_up_count}/{self.max_scale_ups_per_position})")
                    continue

                # ABSOLUTE SIZE BLOCK: Regardless of scale_up_count, block if size >= 3x initial slice
                # This catches any bugs where scale_up_count doesn't increment properly
                initial_slice = total_equity * self.max_carry_alloc_pct * self.alloc_pct
                max_position_size = initial_slice * 3.0  # Entry + 2 scale-ups = 3x
                if existing_pos_for_sym.size_usd >= max_position_size * 0.95:  # 95% tolerance
                    log.debug(f"[CARRY] SIZE BLOCKED SCALE-UP {hl_coin}: size ${existing_pos_for_sym.size_usd:.2f} >= max ${max_position_size:.2f}")
                    continue

                # SCALE-UP: Check max pairs that can scale-up (only 2 pairs can have scale-ups)
                # FIX 2026-01-24 Issue #10: Use projected count to allow 2 pairs to scale when limit is 2
                # Old logic: blocked 2nd pair because it checked if count >= max BEFORE considering this pair
                pairs_with_scaleups = sum(1 for p in list(self.positions.values()) if p.scale_up_count > 0)

                # If this pair hasn't scaled yet, it would ADD to the count
                would_be_new_scaled_pair = existing_pos_for_sym.scale_up_count == 0
                projected_scaled_pairs = pairs_with_scaleups + (1 if would_be_new_scaled_pair else 0)

                if projected_scaled_pairs > self.max_pairs_with_scaleup:
                    log.debug(f"[CARRY] Skip SCALE-UP {hl_coin}: would exceed max pairs "
                             f"({projected_scaled_pairs} > {self.max_pairs_with_scaleup})")
                    continue

            # Re-check allocation limits (may have changed after previous entry)
            current_exposure = sum(p.size_usd for p in list(self.positions.values()))
            max_notional = total_equity * self.max_carry_alloc_pct
            at_max_alloc = current_exposure >= max_notional

            if at_max_alloc:
                # Check if any current position is much worse
                worst_pos_sym = None
                worst_yield = float('inf')
                # FIX 2026-01-22: Use list() to prevent RuntimeError if positions modified during iteration
                # FIX 2026-01-25: Use CURRENT yield from _calc_mtm instead of stale entry_diff_bps
                # This ensures we compare actual current performance, not frozen entry-time values
                for sym, pos in list(self.positions.items()):
                    mtm = self._calc_mtm(pos)
                    curr_y = abs(mtm.get("curr_diff_bps", pos.entry_diff_bps))  # Fallback to entry if MTM stale
                    if curr_y < worst_yield:
                        worst_yield = curr_y
                        worst_pos_sym = sym

                if worst_pos_sym and net_yield > worst_yield + self.opt_buffer:
                    log.info(f"[CARRY] OPTIMIZING: Replacing {worst_pos_sym} with {hl_coin} (Gain: {net_yield-worst_yield:.1f}bps)")
                    await self._execute_exit(worst_pos_sym, "OPTIMIZATION", 0.0, 0.0) # placeholders for pnl estimation
                else:
                    # No more room and no optimization possible, stop trying
                    break

            # 3. Execute Entry
            # Phase 2: Use Smart Entry
            # Pass all opportunities so scale-up can compare with better alternatives
            # raw_diff_bps is the SIGNED diff for reversal detection
            await self._execute_smart_entry(hl_coin, s1, s2, direction, net_yield, r1, r2, (v1_n, v2_n), opportunities, raw_diff_bps)
            entered_coins.add(hl_coin)

            # Small delay between entries to avoid rate limiting
            await asyncio.sleep(0.3)

    async def _ensure_leverage_setup(self, venue_name: str, symbol: str, venue_obj) -> bool:
        """
        Ensure leverage is properly set for a symbol before placing orders.
        CRITICAL: Must be called before any live order to avoid margin issues.
        Returns True if leverage is ready, False if setup failed.
        """
        if self.paper_mode or self.dry_run_live:
            return True  # No need to setup leverage in paper/dry-run mode

        cache_key = f"{venue_name}:{symbol}"
        if cache_key in self._leverage_setup_done:
            return self._leverage_setup_done[cache_key]

        try:
            # Skip Lighter leverage setup (drains quota, per CLAUDE.md)
            if venue_name == "LT":
                log.debug(f"[CARRY] Skipping leverage setup for Lighter {symbol} (quota issue)")
                self._leverage_setup_done[cache_key] = True
                return True

            if hasattr(venue_obj, 'set_leverage'):
                result = await venue_obj.set_leverage(symbol, self.leverage)
                if result:
                    log.info(f"[CARRY] Leverage {self.leverage}x set for {venue_name} {symbol}")
                    self._leverage_setup_done[cache_key] = True
                    return True
                else:
                    log.warning(f"[CARRY] Failed to set leverage for {venue_name} {symbol}")
                    # Don't cache failure - retry next time
                    return False
            elif hasattr(venue_obj, 'set_isolated_margin'):
                result = await venue_obj.set_isolated_margin(symbol, self.leverage)
                if result:
                    log.info(f"[CARRY] Isolated margin {self.leverage}x set for {venue_name} {symbol}")
                    self._leverage_setup_done[cache_key] = True
                    return True
                else:
                    log.warning(f"[CARRY] Failed to set isolated margin for {venue_name} {symbol}")
                    return False
            else:
                log.debug(f"[CARRY] Venue {venue_name} doesn't support leverage setup")
                self._leverage_setup_done[cache_key] = True
                return True

        except Exception as e:
            log.warning(f"[CARRY] Leverage setup error for {venue_name} {symbol}: {e}")
            return False

    async def _execute_smart_entry(self, hl_coin: str, s1: str, s2: str, direction: str,
                                   diff_bps: float, r1: float, r2: float, venues: Tuple[str, str],
                                   all_opportunities: List = None, raw_diff_bps: float = 0.0) -> None:
        """
        handle atomic entry with rollback.
        Phase 2: Use Maker orders where possible for the first leg (smart routing).

        Args:
            raw_diff_bps: Signed (r1-r2)*10000 for reversal detection. diff_bps is net_yield (always positive).
        """
        v1_n, v2_n = venues
        v_map = {"HL": self.hl, "AS": self.asr, "LT": self.lighter}
        v1_obj, v2_obj = v_map.get(v1_n), v_map.get(v2_n)

        if not v1_obj or not v2_obj: return

        total_equity = await self._get_total_equity()

        # Check if this is a SCALE UP or a NEW ENTRY
        # The symbol key is based on the AS or Lighter symbol
        target_sym = s2 if v1_n == "HL" else s1
        existing_pos = self.positions.get(target_sym)

        if existing_pos:
            # CRITICAL: Double-check scale-up limit (should be caught by _check_entries but be safe)
            log.info(f"[CARRY] SCALE-UP VERIFY {hl_coin}: count={existing_pos.scale_up_count}, max={self.max_scale_ups_per_position}, sym={target_sym}")
            if existing_pos.scale_up_count >= self.max_scale_ups_per_position:
                log.warning(f"[CARRY] HARD BLOCK SCALE-UP {hl_coin}: already at max ({existing_pos.scale_up_count}/{self.max_scale_ups_per_position})")
                return

            # BUGFIX: Scale-up cooldown - prevent order spam
            last_scale = self._last_scale_up_time.get(hl_coin, 0)
            time_since_scale = time.time() - last_scale
            if time_since_scale < self._scale_up_cooldown_seconds:
                remaining = (self._scale_up_cooldown_seconds - time_since_scale) / 60.0
                log.info(f"[CARRY] SCALE-UP COOLDOWN {hl_coin}: {remaining:.1f}m remaining (last scale-up {time_since_scale/60:.1f}m ago)")
                return

            # BUGFIX: Minimum hold time before scale-up - prevent scaling into position before confirming it's stable
            # FIX 2026-01-21: Increased from 5 min to 30 min to let positions stabilize before adding size
            MIN_HOLD_BEFORE_SCALE_UP_SECONDS = 3600.0  # 1 hour (FIX 2026-01-24: Increased from 30min)
            time_since_entry = time.time() - existing_pos.entry_time
            if time_since_entry < MIN_HOLD_BEFORE_SCALE_UP_SECONDS:
                remaining_hold = (MIN_HOLD_BEFORE_SCALE_UP_SECONDS - time_since_entry) / 60.0
                log.info(f"[CARRY] SCALE-UP HOLD {hl_coin}: Position too young ({time_since_entry/60:.1f}m old), need {remaining_hold:.1f}m more hold time")
                return

            # SCALE-UP PROTECTION: Verify both legs exist on venues before adding more size
            # This prevents accumulating unhedged risk if one leg was orphan-closed
            if not self.paper_mode:
                try:
                    v1_positions = await v1_obj.get_positions() or []
                    v2_positions = await v2_obj.get_positions() or []
                    v1_found = any(
                        pos.get('symbol', '').upper() in (hl_coin.upper(), s1.upper())
                        for pos in v1_positions
                        if abs(float(pos.get('size', 0))) > 0
                    )
                    v2_found = any(
                        pos.get('symbol', '').upper() in (hl_coin.upper(), s2.upper())
                        for pos in v2_positions
                        if abs(float(pos.get('size', 0))) > 0
                    )
                    if not v1_found or not v2_found:
                        log.error(f"[CARRY] SCALE-UP BLOCKED {hl_coin}: Missing leg! V1({v1_n})={v1_found}, V2({v2_n})={v2_found}. Position may be unhedged!")
                        if self.notifier:
                            await self.notifier.notify(
                                f"SCALE-UP BLOCKED: {hl_coin}",
                                f"V1({v1_n})={v1_found}, V2({v2_n})={v2_found}. Position may be UNHEDGED!"
                            )
                        return
                    log.debug(f"[CARRY] SCALE-UP VERIFIED {hl_coin}: Both legs confirmed on venues")
                except Exception as e:
                    log.warning(f"[CARRY] SCALE-UP position check failed for {hl_coin}: {e}. Proceeding cautiously...")

            # ABSOLUTE BLOCK: Check size-based limit (3 entries = 3x slice_size)
            # max_size = equity * max_carry_alloc_pct * alloc_pct * 3 (initial + 2 scale-ups)
            max_expected_size = total_equity * self.max_carry_alloc_pct * self.alloc_pct * 3.0
            if existing_pos.size_usd >= max_expected_size:
                log.warning(f"[CARRY] SIZE BLOCK SCALE-UP {hl_coin}: position size ${existing_pos.size_usd:.2f} already >= max ${max_expected_size:.2f}")
                return

            # SMART SCALE-UP: Check if there's a better opportunity without a position
            # If yes, skip scale-up to allocate capital to the better opportunity instead
            if all_opportunities:
                for opp in all_opportunities:
                    # FIX: Updated to handle 9-element tuple (added raw_diff_bps)
                    opp_hl_coin, _, _, _, opp_yield, _, _, _, _ = opp
                    # Skip self
                    if opp_hl_coin == hl_coin:
                        continue
                    # Check if this opportunity has no position and better yield
                    has_pos = any(p.hl_coin == opp_hl_coin for p in list(self.positions.values()))
                    if not has_pos and opp_yield > diff_bps + self.opt_buffer:
                        log.info(f"[CARRY] SCALE_UP SKIPPED {hl_coin} ({diff_bps:.1f}bps): "
                                f"Better opportunity {opp_hl_coin} ({opp_yield:.1f}bps) has no position")
                        return

            # FIX 2026-01-25: Use stress-adjusted slice percentage
            effective_slice_pct = self._get_stress_adjusted_slice_pct(self.scale_up_slice_pct)
            size_usd = total_equity * self.max_carry_alloc_pct * effective_slice_pct
            action_name = "SCALE_UP"

            # Phase 4.3 Fix: Check that scale-up doesn't exceed max_symbol_alloc_pct
            # FIX 2026-01-25: Use yield-tiered allocation cap based on position's current yield
            mtm = self._calc_mtm(existing_pos)
            pos_yield_bps = abs(mtm.get("curr_diff_bps", existing_pos.entry_diff_bps))
            allocation_cap = self._get_yield_tiered_allocation_cap(pos_yield_bps)
            max_sym_alloc = total_equity * allocation_cap
            projected_total = existing_pos.size_usd + size_usd
            if projected_total > max_sym_alloc:
                # Cap the size to stay within limit
                remaining_room = max_sym_alloc - existing_pos.size_usd
                if remaining_room < 10.0:  # Less than $10 room, skip scale-up
                    log.info(f"[CARRY] SCALE_UP SKIPPED {hl_coin}: Already at max allocation (${existing_pos.size_usd:.2f} / ${max_sym_alloc:.2f})")
                    return
                size_usd = remaining_room
                log.info(f"[CARRY] SCALE_UP CAPPED {hl_coin}: Reducing size to ${size_usd:.2f} (max: ${max_sym_alloc:.2f})")
        else:
            size_usd = total_equity * self.max_carry_alloc_pct * self.alloc_pct
            action_name = "ENTRY"

        # Apply drawdown multiplier to position size (P2 Fix)
        dd_mult = self._get_drawdown_multiplier()
        if dd_mult < 1.0:
            original_size = size_usd
            size_usd = size_usd * dd_mult
            log.info(f"[CARRY] Position size reduced to {dd_mult*100:.0f}% due to drawdown: ${original_size:.2f} -> ${size_usd:.2f}")

        # PHASE 3 FIX (2026-01-22): Dynamic position sizing based on funding yield
        # Higher yield = more confidence = larger position (up to 1.5x)
        # Lower yield = smaller position to reduce risk on marginal opportunities
        abs_diff = abs(diff_bps)
        if action_name == "ENTRY":
            # Only apply to new entries (scale-ups use fixed slice_size)
            yield_multiplier = min(abs_diff / 100.0, 1.5)  # 50bps = 0.5x, 100bps = 1.0x, 150bps = 1.5x
            yield_multiplier = max(yield_multiplier, 0.5)  # Floor at 50% to ensure minimum position
            if yield_multiplier != 1.0:
                original_size = size_usd
                size_usd = size_usd * yield_multiplier
                log.info(f"[CARRY] YIELD SIZING {hl_coin}: yield={abs_diff:.1f}bps -> multiplier={yield_multiplier:.2f}x "
                        f"Size: ${original_size:.2f} -> ${size_usd:.2f}")

        # BUGFIX: Volatility-adjusted position sizing
        # High funding rate spreads (>400bps) typically indicate volatile market conditions
        # The BERAUSDT loss was -$5.65 price PnL on 393bps spread - volatility exceeded funding
        # For high spreads, reduce position size to limit price volatility exposure
        HIGH_VOLATILITY_THRESHOLD_BPS = 400.0
        HIGH_VOLATILITY_SIZE_REDUCTION = 0.7  # 70% of normal size (30% reduction)
        if abs_diff > HIGH_VOLATILITY_THRESHOLD_BPS:
            original_size = size_usd
            size_usd = size_usd * HIGH_VOLATILITY_SIZE_REDUCTION
            log.warning(f"[CARRY] HIGH VOLATILITY SIZING {hl_coin}: spread={abs_diff:.1f}bps > {HIGH_VOLATILITY_THRESHOLD_BPS}bps. "
                       f"Reducing size: ${original_size:.2f} -> ${size_usd:.2f} ({HIGH_VOLATILITY_SIZE_REDUCTION*100:.0f}%)")

        log.info(f"[CARRY] {action_name} {hl_coin}: {direction}. Size: ${size_usd:.2f}")
        
        prices = {"v1": 0.0, "v2": 0.0}

        # PHASE 6: Initialize maker tracking (will be set in live mode)
        is_maker_v1 = False
        is_maker_v2 = False

        # Determine Sides
        is_long1 = direction.lower().startswith("long")
        side1 = "BUY" if is_long1 else "SELL"
        side2 = "SELL" if is_long1 else "BUY"

        if self.paper_mode:
            b1 = v1_obj.get_bbo(s1)
            b2 = v2_obj.get_bbo(s2)
            # Use realistic prices (bid for sell, ask for buy) instead of optimistic mid
            prices["v1"] = (b1[1] if side1 == "BUY" else b1[0]) if b1 else 100
            prices["v2"] = (b2[1] if side2 == "BUY" else b2[0]) if b2 else 100

            # --- 0 bps Favorable Entry Check (paper mode) ---
            if b1 and b2:
                entry_slip = self._slip_bps(b1, b2)
                entry_raw_diff = abs((r1 - r2) * 10000.0)
                roundtrip_fees = self._get_roundtrip_fees_bps(v1_n, v2_n)
                entry_net_yield = entry_raw_diff - (entry_slip + self.slip_buffer + roundtrip_fees)
                if entry_net_yield < diff_bps:
                    log.info(f"[CARRY] {action_name} SKIPPED {hl_coin}: basis narrowed "
                            f"entry={entry_net_yield:.1f}bps < detection={diff_bps:.1f}bps")
                    self._log_opp_audit(target_sym, diff_bps, "SKIPPED", "Adverse Entry Basis",
                                       f"entry={entry_net_yield:.1f} det={diff_bps:.1f}")
                    return

        elif self.dry_run_live:
            # DRY-RUN LIVE: Calculate everything but don't place orders
            # Use get_fresh_bbo for Lighter (has REST fallback if WS stale)
            if v1_n == "LT" and hasattr(v1_obj, 'get_fresh_bbo'):
                bbo1 = await v1_obj.get_fresh_bbo(s1)
            else:
                bbo1 = v1_obj.get_bbo(s1)

            if v2_n == "LT" and hasattr(v2_obj, 'get_fresh_bbo'):
                bbo2 = await v2_obj.get_fresh_bbo(s2)
            else:
                bbo2 = v2_obj.get_bbo(s2)

            # Use fallback price if BBO unavailable (e.g., Lighter rate-limited)
            # For scale-up of existing position, use entry price as fallback
            if not bbo1 or not bbo2:
                if existing_pos:
                    # Scale-up: use existing entry prices as fallback
                    p1 = existing_pos.entry_px_hl if v1_n == "HL" else existing_pos.entry_px_as
                    p2 = existing_pos.entry_px_as if v1_n == "HL" else existing_pos.entry_px_hl
                    log.warning(f"[CARRY DRY-RUN] No BBO for {s1} or {s2}, using entry prices as fallback")
                else:
                    log.warning(f"[CARRY DRY-RUN] No BBO for {s1} or {s2}, skipping new entry")
                    return
            else:
                # Realistic prices - size_per_leg = size_usd / 2 (half per venue)
                p1 = bbo1[1] if side1 == "BUY" else bbo1[0]
                p2 = bbo2[1] if side2 == "BUY" else bbo2[0]

                # --- 0 bps Favorable Entry Check ---
                # Recalculate entry-time basis using fresh BBOs
                entry_slip = self._slip_bps(bbo1, bbo2)
                entry_raw_diff = abs((r1 - r2) * 10000.0)
                roundtrip_fees = self._get_roundtrip_fees_bps(v1_n, v2_n)
                entry_net_yield = entry_raw_diff - (entry_slip + self.slip_buffer + roundtrip_fees)

                # Skip if entry basis < detection basis (spread narrowed)
                if entry_net_yield < diff_bps:
                    log.info(f"[CARRY] {action_name} SKIPPED {hl_coin}: basis narrowed "
                            f"entry={entry_net_yield:.1f}bps < detection={diff_bps:.1f}bps")
                    self._log_opp_audit(target_sym, diff_bps, "SKIPPED", "Adverse Entry Basis",
                                       f"entry={entry_net_yield:.1f} det={diff_bps:.1f}")
                    return

            size_per_leg = size_usd / 2
            qty1 = v1_obj.round_qty(s1, size_per_leg / p1)
            qty2 = v2_obj.round_qty(s2, size_per_leg / p2)

            log.info(f"[CARRY DRY-RUN] WOULD PLACE ENTRY:")
            log.info(f"  Leg 1: {v1_n} {s1} {side1} qty={qty1:.6f} @ ${p1:.4f}")
            log.info(f"  Leg 2: {v2_n} {s2} {side2} qty={qty2:.6f} @ ${p2:.4f}")
            log.info(f"  Total notional: ${size_usd:.2f} | Direction: {direction}")

            # FIX 2026-01-25: Log dry-run entries to decision log
            self._log_decision(
                symbol=target_sym,
                hl_coin=hl_coin,
                decision_type="ENTRY_ATTEMPT" if action_name == "ENTRY" else "SCALE_UP_ATTEMPT",
                trigger_reason=f"yield={diff_bps:.1f}bps",
                v1_venue=v1_n,
                v2_venue=v2_n,
                v1_bbo=(bbo1[0], bbo1[1]) if bbo1 else None,
                v2_bbo=(bbo2[0], bbo2[1]) if bbo2 else None,
                current_yield_bps=diff_bps,
                position_size_usd=size_usd,
                equity_usd=total_equity,
                outcome="DRY_RUN",
                outcome_reason=f"V1={v1_n}:{s1} V2={v2_n}:{s2} {direction}"
            )

            prices["v1"] = p1
            prices["v2"] = p2
        else:
            # LIVE SMART ENTRY / SCALE UP WITH RETRY

            # FIX 2026-01-24 Bug #4: Check blocked_bases BEFORE any entry attempt
            # Prevents entries on blocked markets (XAU, XAG, etc.)
            if hl_coin.upper() in self._blocked_bases:
                log.info(f"[CARRY] PRE-CHECK SKIP {hl_coin}: Base is blocked")
                self._log_opp_audit(hl_coin, diff_bps, "BLOCKED_BASE",
                                    f"Base {hl_coin} in blocklist", f"{v1_n}/{v2_n}")
                return  # Exit function early - don't attempt entry on blocked base

            # FIX 2026-01-24 Bug #2: Pre-check Lighter volume quota BEFORE any entry attempt
            # Prevents fee waste from entries that will fail due to blocked venue
            if v2_n == "LT" and self.lighter and self.lighter.is_volume_quota_blocked():
                remaining_min = self.lighter.get_volume_quota_remaining_minutes()
                log.info(f"[CARRY] PRE-CHECK SKIP {hl_coin}: Lighter blocked ({remaining_min:.0f} min remaining)")
                self._log_opp_audit(hl_coin, diff_bps, "LIGHTER_BLOCKED",
                                    f"Volume quota: {remaining_min:.0f}min", f"{v1_n}/{v2_n}")
                return  # Exit function early - don't attempt entry on blocked venue
            if v1_n == "LT" and self.lighter and self.lighter.is_volume_quota_blocked():
                remaining_min = self.lighter.get_volume_quota_remaining_minutes()
                log.info(f"[CARRY] PRE-CHECK SKIP {hl_coin}: Lighter blocked ({remaining_min:.0f} min remaining)")
                self._log_opp_audit(hl_coin, diff_bps, "LIGHTER_BLOCKED",
                                    f"Volume quota: {remaining_min:.0f}min", f"{v1_n}/{v2_n}")
                return  # Exit function early - don't attempt entry on blocked venue

            # CRITICAL: Setup leverage before placing orders (only once per symbol)
            await self._ensure_leverage_setup(v1_n, s1, v1_obj)
            await self._ensure_leverage_setup(v2_n, s2, v2_obj)

            # Use get_fresh_bbo for Lighter (has REST fallback if WS stale)
            if v1_n == "LT" and hasattr(v1_obj, 'get_fresh_bbo'):
                bbo1 = await v1_obj.get_fresh_bbo(s1)
            else:
                bbo1 = v1_obj.get_bbo(s1)

            if v2_n == "LT" and hasattr(v2_obj, 'get_fresh_bbo'):
                bbo2 = await v2_obj.get_fresh_bbo(s2)
            else:
                bbo2 = v2_obj.get_bbo(s2)

            def get_fallback_price(bbo, side):
                if not bbo: return 100.0
                return bbo[1] if side == "BUY" else bbo[0]

            ft1_px = (bbo1[0] + bbo1[1])/2 if bbo1 else 100.0
            size_per_leg = size_usd / 2  # Half per venue
            qty1 = v1_obj.round_qty(s1, size_per_leg/ft1_px)

            # FIX 2026-01-24: Pre-check depth on BOTH venues before attempting entry
            # This prevents fee waste from entries that will fail due to low depth
            order_size_usd = size_per_leg
            min_depth_for_order = order_size_usd * 2  # Need 2x order size in depth

            # FIX 2026-01-24: Use sentinel to distinguish "venue doesn't support depth" from "zero liquidity"
            # -1.0 = venue doesn't support depth (allow entry)
            # 0.0 = venue supports depth but has no liquidity (BLOCK entry - this was the bug!)
            # >0.0 = venue has liquidity (check if sufficient)
            v1_supports_depth = hasattr(v1_obj, 'get_aggregated_depth_usd')
            v2_supports_depth = hasattr(v2_obj, 'get_aggregated_depth_usd')

            d1_check = v1_obj.get_aggregated_depth_usd(s1, levels=5) if v1_supports_depth else -1.0
            d2_check = v2_obj.get_aggregated_depth_usd(s2, levels=5) if v2_supports_depth else -1.0

            # V1 depth check: block if venue supports depth AND has insufficient liquidity (including 0)
            if v1_supports_depth and d1_check >= 0 and d1_check < min_depth_for_order:
                reason = "NO_LIQUIDITY" if d1_check == 0 else "LOW_DEPTH"
                log.info(f"[CARRY] PRE-CHECK SKIP {hl_coin}: V1 {reason} ${d1_check:.0f} < ${min_depth_for_order:.0f} (order=${order_size_usd:.0f})")
                self._log_opp_audit(hl_coin, diff_bps, f"{reason}_V1_PRECHECK",
                                    f"Need ${min_depth_for_order:.0f}, have ${d1_check:.0f}", f"{v1_n}/{v2_n}")
                return  # Exit function early - not in a loop

            # V2 depth check: block if venue supports depth AND has insufficient liquidity (including 0)
            if v2_supports_depth and d2_check >= 0 and d2_check < min_depth_for_order:
                reason = "NO_LIQUIDITY" if d2_check == 0 else "LOW_DEPTH"
                log.info(f"[CARRY] PRE-CHECK SKIP {hl_coin}: V2 {reason} ${d2_check:.0f} < ${min_depth_for_order:.0f} (order=${order_size_usd:.0f})")
                self._log_opp_audit(hl_coin, diff_bps, f"{reason}_V2_PRECHECK",
                                    f"Need ${min_depth_for_order:.0f}, have ${d2_check:.0f}", f"{v1_n}/{v2_n}")
                return  # Exit function early - not in a loop

            # ENTRY LOCK: Prevent Position Manager from closing "orphan" during multi-leg entry
            self._register_entry_start(hl_coin)

            # FIX 2026-01-25: Pre-compute venue-aware maker mode decisions
            # This prevents wasted maker attempts on venues with thin depth
            use_maker_v1 = await self._should_use_maker_for_venue(v1_obj, s1, qty1, v1_n) if bbo1 and bbo1[0] > 0 and bbo1[1] > 0 else False
            use_maker_v2 = await self._should_use_maker_for_venue(v2_obj, s2, qty2, v2_n) if bbo2 and bbo2[0] > 0 and bbo2[1] > 0 else False

            if not use_maker_v1 and not use_maker_v2:
                log.info(f"[CARRY] Both venues have thin depth for {hl_coin}, using taker mode for both legs")

            # FIX 2026-01-25: Log entry attempt BEFORE placing orders
            self._log_decision(
                symbol=target_sym,
                hl_coin=hl_coin,
                decision_type="ENTRY_ATTEMPT" if action_name == "ENTRY" else "SCALE_UP_ATTEMPT",
                trigger_reason=f"yield={diff_bps:.1f}bps",
                v1_venue=v1_n,
                v2_venue=v2_n,
                v1_bbo=(bbo1[0], bbo1[1]) if bbo1 else None,
                v2_bbo=(bbo2[0], bbo2[1]) if bbo2 else None,
                depth_v1_usd=d1_check if d1_check >= 0 else 0.0,
                depth_v2_usd=d2_check if d2_check >= 0 else 0.0,
                current_yield_bps=diff_bps,
                position_size_usd=size_usd,
                equity_usd=total_equity,
                outcome="ATTEMPTING",
                outcome_reason=f"maker_v1={use_maker_v1} maker_v2={use_maker_v2}"
            )

            # Leg 1 Entry with maker fallback (if enabled)
            # FIX 2026-01-24: Calculate dynamic timeout based on spread size
            spread1_bps = ((bbo1[1] - bbo1[0]) / ft1_px) * 10000 if bbo1 and ft1_px > 0 else 50.0
            dynamic_timeout = self._get_maker_timeout(diff_bps, is_scale_up=False)

            # FIX 2026-01-25: Adaptive V1 timeout based on V2 depth
            # If V2 has thin depth, reduce V1 timeout to minimize orphan window
            if d2_check >= 0 and d2_check < order_size_usd * 2:  # Less than 2x our order size
                original_timeout = dynamic_timeout
                dynamic_timeout = min(dynamic_timeout, 15.0)  # Cap at 15s
                if dynamic_timeout < original_timeout:
                    log.info(f"[MAKER] V2 depth thin (${d2_check:.0f} < ${order_size_usd * 2:.0f}), reducing V1 timeout: {original_timeout:.0f}s -> {dynamic_timeout:.0f}s")

            f1 = 0.0
            p1 = 0.0
            for attempt in range(2):
                try:
                    if use_maker_v1:
                        # FIX 2026-01-24: Use dynamic timeout based on spread
                        timeout_s = dynamic_timeout
                        res1 = await self._execute_with_maker_fallback(
                            venue=v1_obj, symbol=s1, side=side1, qty=qty1,
                            bbo=bbo1, timeout_s=timeout_s, is_entry=True,
                            reduce_only=False, venue_name=v1_n
                        )
                        f1 = float(res1.get("filled", 0.0))
                        is_maker_v1 = res1.get("is_maker", False)
                        if f1 > 0:
                            p1 = float(res1.get("avg_price", 0.0))
                            if p1 <= 0:
                                p1 = get_fallback_price(bbo1, side1)
                                log.warning(f"[CARRY] {s1} fill price missing. Fallback to {side1}: {p1}")
                            break
                    else:
                        # Direct IOC order (maker disabled or no valid BBO)
                        res1 = await v1_obj.place_order(s1, side1, qty1, ioc=True)
                        f1 = float(res1.get("filled", 0.0))
                        if f1 > 0:
                            p1 = float(res1.get("avg_price", 0.0))
                            if p1 <= 0:
                                p1 = get_fallback_price(bbo1, side1)
                                log.warning(f"[CARRY] {s1} fill price missing. Fallback to {side1}: {p1}")
                            break
                except Exception as e:
                    log.warning(f"[CARRY] Entry Leg 1 exception (attempt {attempt+1}): {e}")
                    self._health_monitor.record_error(v1_n, str(e))  # P2 Fix: Track venue errors
                await asyncio.sleep(0.5)

            if f1 <= 0:
                log.warning(f"[CARRY] Entry/Scale Leg 1 failed for {hl_coin} after 2 attempts")
                # Track failed entry attempt
                self._entry_attempts.insert(0, {
                    "ts": time.time(),
                    "symbol": target_sym,
                    "hl_coin": hl_coin,
                    "v1": v1_n,
                    "v2": v2_n,
                    "v1_status": "FAILED",
                    "v2_status": "NOT_ATTEMPTED",
                    "orphan_created": False,
                    "orphan_closed": False,
                    "reason": "Leg1_Order_Failed"
                })
                self._entry_attempts = self._entry_attempts[:self._max_entry_attempts_tracked]
                self._set_entry_cooldown(hl_coin)  # Prevent rapid retries
                # FIX 2026-01-22: Log failed entry attempt to CSV for audit trail
                equity = await self._get_total_equity()
                self._log(
                    target_sym, "ENTRY_FAILED_LEG1", direction, size_usd,
                    r1, r2, v1_n, v2_n,
                    0.0, 0.0, 0.0, equity
                )
                # FIX 2026-01-24: Log order to comprehensive orders CSV
                self._log_order(
                    event="ORDER_FAILED",
                    symbol=s1,
                    hl_coin=hl_coin,
                    venue=v1_n,
                    side=side1,
                    qty_requested=qty1,
                    qty_filled=0.0,
                    order_type="MAKER/IOC" if self._maker_order_enabled else "IOC",
                    direction=direction,
                    action="ENTRY_LEG1",
                    status="FAILED",
                    error="No fill after 2 attempts",
                    details=f"diff_bps={diff_bps:.1f}"
                )
                # FIX 2026-01-25: Log failed entry to decision log
                self._log_decision(
                    symbol=target_sym,
                    hl_coin=hl_coin,
                    decision_type="ENTRY_FAILED" if action_name == "ENTRY" else "SCALE_UP_FAILED",
                    trigger_reason=f"yield={diff_bps:.1f}bps",
                    v1_venue=v1_n,
                    v2_venue=v2_n,
                    v1_bbo=(bbo1[0], bbo1[1]) if bbo1 else None,
                    v2_bbo=(bbo2[0], bbo2[1]) if bbo2 else None,
                    current_yield_bps=diff_bps,
                    position_size_usd=size_usd,
                    equity_usd=equity,
                    outcome="FAILED",
                    outcome_reason="Leg1_Order_Failed after 2 attempts"
                )
                self._clear_entry_lock(hl_coin)
                # FIX 2026-01-24: Cleanup any orphaned orders on V1 after failure
                # (maker loop should have cleaned up, but this is defensive)
                if hasattr(v1_obj, 'cancel_all_orders'):
                    try:
                        await v1_obj.cancel_all_orders(symbol=s1)
                        log.debug(f"[CARRY] Cleaned up V1 ({v1_n}) orders for {s1} after Leg1 failure")
                    except Exception as e:
                        log.debug(f"[CARRY] V1 cleanup skipped (expected if no orders): {e}")
                # FIX 2026-01-22: Run post-trade sync even on failure to detect false failures
                asyncio.create_task(self._post_trade_position_sync(hl_coin, v1_n, v2_n, direction, target_sym))
                return

            prices["v1"] = p1

            # FIX 2026-01-24: Log successful V1 entry to orders CSV
            # OPTIMIZATION 2026-01-24: Include execution quality metrics
            is_maker_v1 = res1.get("is_maker", False) if 'res1' in dir() else False
            expected_px_v1 = bbo1[1] if side1 == "BUY" else bbo1[0]  # Expected fill at BBO
            taker_rate_v1 = self._venue_fees.get(v1_n, {}).get("taker", 5.0) / 10000
            maker_rate_v1 = self._venue_fees.get(v1_n, {}).get("maker", 2.0) / 10000
            actual_fee_v1 = (f1 * p1) * (maker_rate_v1 if is_maker_v1 else taker_rate_v1)
            taker_fee_v1 = (f1 * p1) * taker_rate_v1
            self._log_order(
                event="ORDER_FILLED",
                symbol=s1,
                hl_coin=hl_coin,
                venue=v1_n,
                side=side1,
                qty_requested=qty1,
                qty_filled=f1,
                avg_fill_price=p1,
                order_type="MAKER" if is_maker_v1 else "IOC",
                direction=direction,
                action="ENTRY_LEG1",
                status="SUCCESS",
                fees_est_usd=actual_fee_v1,
                details=f"diff_bps={diff_bps:.1f}, fill_usd=${f1*p1:.2f}",
                expected_price=expected_px_v1,
                is_maker=is_maker_v1,
                taker_fee_would_be=taker_fee_v1
            )

            # Leg 2 Entry with maker fallback (if enabled)
            f2 = 0.0
            p2 = 0.0
            # FIX 2026-01-22 Bug 2A: Convert V1's filled USD value to V2 qty at V2's price
            # Previously used f1 (V1 coins) directly, causing USD mismatch when prices differ
            v1_fill_usd = f1 * p1  # Actual USD value filled on V1
            p2_estimate = (bbo2[1] if side2 == "BUY" else bbo2[0]) if bbo2 and bbo2[0] > 0 else p1
            qty2 = v2_obj.round_qty(s2, v1_fill_usd / p2_estimate)  # Match USD, not coins

            # FIX 2026-01-24: Re-check V2 depth before Leg 2 (fee waste prevention)
            d2_fresh = v2_obj.get_aggregated_depth_usd(s2, levels=5) if hasattr(v2_obj, 'get_aggregated_depth_usd') else 0.0
            if d2_fresh > 0 and d2_fresh < v1_fill_usd:
                log.warning(f"[CARRY] V2 depth dropped to ${d2_fresh:.0f} < V1 fill ${v1_fill_usd:.0f} - early rollback")
                # Rollback V1 immediately before wasting more time
                inv_side = "SELL" if side1 == "BUY" else "BUY"
                use_reduce_only_rb = v1_n != "AS"
                try:
                    rb_res = await v1_obj.place_order(s1, inv_side, f1, ioc=True, reduce_only=use_reduce_only_rb)
                    rb_filled = float(rb_res.get("filled", 0.0))
                    log.info(f"[CARRY] EARLY V1 ROLLBACK (low V2 depth): filled {rb_filled:.6f}/{f1:.6f}")
                except Exception as rb_e:
                    log.error(f"[CARRY] Early V1 rollback exception: {rb_e}")
                self._entry_attempts.insert(0, {
                    "ts": time.time(), "symbol": target_sym, "hl_coin": hl_coin,
                    "v1": v1_n, "v2": v2_n, "v1_status": "FILLED", "v2_status": "LOW_DEPTH_ABORT",
                    "orphan_created": False, "orphan_closed": True, "reason": "V2_Depth_Dropped"
                })
                self._entry_attempts = self._entry_attempts[:self._max_entry_attempts_tracked]

                # FIX 2026-01-24: Track early rollback fee waste
                v1_fee_waste_bps = self._venue_fees[v1_n]["taker"] * 2  # Entry + rollback
                v1_fill_usd_early = f1 * p1
                v1_fee_waste_usd_early = v1_fill_usd_early * (v1_fee_waste_bps / 10000.0)
                self._live_realized_pnl -= v1_fee_waste_usd_early
                log.warning(f"[CARRY] EARLY ROLLBACK FEE WASTE: ${v1_fee_waste_usd_early:.4f} (V1 entry+rollback) subtracted from realized PnL (cumulative: ${self._live_realized_pnl:.2f})")

                # Short cooldown - this is a market condition, not a failure
                self._set_entry_cooldown(hl_coin, minutes=1.0)
                self._clear_entry_lock(hl_coin)
                return

            # FIX 2026-01-25: V1 filled - V2 tries maker first with dynamic timeout, then IOC fallback
            # FIX 2026-01-25: Dynamic V2 timeout based on spread (same logic as V1)
            v2_spread_bps = ((bbo2[1] - bbo2[0]) / ((bbo2[0] + bbo2[1]) / 2)) * 10000 if bbo2 and bbo2[0] > 0 and bbo2[1] > 0 else 50.0
            if v2_spread_bps < 20:
                v2_timeout = 15.0
            elif v2_spread_bps < 50:
                v2_timeout = 30.0
            else:
                v2_timeout = 60.0
            log.info(f"[CARRY] V1 filled ${v1_fill_usd:.2f} - V2 trying maker ({v2_timeout:.0f}s, spread={v2_spread_bps:.1f}bps), then IOC fallback")

            for attempt in range(2):
                try:
                    if use_maker_v2:
                        # FIX 2026-01-25: Dynamic timeout for V2 maker after V1 fills
                        timeout_s = v2_timeout
                        res2 = await self._execute_with_maker_fallback(
                            venue=v2_obj, symbol=s2, side=side2, qty=qty2,
                            bbo=bbo2, timeout_s=timeout_s, is_entry=True,
                            reduce_only=False, venue_name=v2_n
                        )
                        f2 = float(res2.get("filled", 0.0))
                        is_maker_v2 = res2.get("is_maker", False)
                        if f2 > 0:
                            p2 = float(res2.get("avg_price", 0.0))
                            if p2 <= 0:
                                p2 = get_fallback_price(bbo2, side2)
                                log.warning(f"[CARRY] {s2} fill price missing. Fallback to {side2}: {p2}")
                            break
                    else:
                        # Direct IOC order (maker disabled or no valid BBO)
                        res2 = await v2_obj.place_order(s2, side2, qty2, ioc=True)
                        f2 = float(res2.get("filled", 0.0))
                        if f2 > 0:
                            p2 = float(res2.get("avg_price", 0.0))
                            if p2 <= 0:
                                p2 = get_fallback_price(bbo2, side2)
                                log.warning(f"[CARRY] {s2} fill price missing. Fallback to {side2}: {p2}")
                            break
                except Exception as e:
                    log.warning(f"[CARRY] Entry Leg 2 exception (attempt {attempt+1}): {e}")
                    self._health_monitor.record_error(v2_n, str(e))  # P2 Fix: Track venue errors
                await asyncio.sleep(0.5)

            if f2 <= 0:
                # FIX 2026-01-22: FORCED MARKET ORDER for V2 when V1 already filled
                # V1 is already on the books - we MUST hedge with V2, no edge checking
                # This is critical: an unhedged V1 position = naked directional exposure

                V2_FORCED_MAX_ATTEMPTS = 5  # More attempts, no edge decay check
                log.error(f"[CARRY] V2 {s2} failed initial attempts. V1 ALREADY FILLED - FORCING V2 MARKET FILL!")
                log.error(f"[CARRY] This is a HEDGE EMERGENCY - V1 filled ${v1_fill_usd:.2f}, V2 must match!")

                v2_retry_success = False
                total_v2_filled = 0.0
                total_v2_value = 0.0
                remaining_v2_qty = qty2

                for forced_attempt in range(V2_FORCED_MAX_ATTEMPTS):
                    if remaining_v2_qty <= qty2 * 0.01:  # Less than 1% remaining
                        log.info(f"[CARRY] V2 FORCED FILL complete: filled {total_v2_filled:.6f}")
                        v2_retry_success = True
                        break

                    # Fetch fresh BBO for V2
                    if v2_n == "LT" and hasattr(v2_obj, 'get_fresh_bbo'):
                        fresh_bbo2 = await v2_obj.get_fresh_bbo(s2)
                    else:
                        fresh_bbo2 = v2_obj.get_bbo(s2)

                    if fresh_bbo2 and fresh_bbo2[0] > 0 and fresh_bbo2[1] > 0:
                        # Recalculate V2 qty based on remaining V1 USD to hedge
                        remaining_usd = v1_fill_usd - total_v2_value
                        fresh_p2 = fresh_bbo2[1] if side2 == "BUY" else fresh_bbo2[0]
                        retry_qty2 = v2_obj.round_qty(s2, remaining_usd / fresh_p2)
                    else:
                        # No valid BBO - use last known price
                        retry_qty2 = v2_obj.round_qty(s2, remaining_v2_qty)
                        fresh_p2 = p2_estimate
                        log.warning(f"[CARRY] V2 forced attempt #{forced_attempt+1}: No valid BBO, using estimate ${fresh_p2:.4f}")

                    if retry_qty2 <= 0:
                        log.warning(f"[CARRY] V2 forced attempt #{forced_attempt+1}: qty rounds to 0, breaking")
                        break

                    log.warning(f"[CARRY] V2 FORCED MARKET #{forced_attempt+1}/{V2_FORCED_MAX_ATTEMPTS}: "
                               f"IOC {side2} {retry_qty2:.6f} @ market (remaining ${remaining_v2_qty * fresh_p2:.2f})")

                    try:
                        # Pure IOC market order - no maker, no waiting
                        res2 = await v2_obj.place_order(s2, side2, retry_qty2, ioc=True)
                        attempt_filled = float(res2.get("filled", 0.0))
                        if attempt_filled > 0:
                            fill_price = float(res2.get("avg_price", 0.0)) or fresh_p2
                            total_v2_filled += attempt_filled
                            total_v2_value += attempt_filled * fill_price
                            remaining_v2_qty -= attempt_filled
                            log.info(f"[CARRY] V2 FORCED FILL #{forced_attempt+1}: "
                                    f"+{attempt_filled:.6f} @ ${fill_price:.4f} "
                                    f"(total={total_v2_filled:.6f}, remaining={remaining_v2_qty:.6f})")

                            if remaining_v2_qty <= qty2 * 0.01:  # Done!
                                f2 = total_v2_filled
                                p2 = total_v2_value / total_v2_filled if total_v2_filled > 0 else fill_price
                                v2_retry_success = True
                                log.info(f"[CARRY] V2 FORCED FILL SUCCESS: {f2:.6f} @ ${p2:.4f}")
                                break
                    except Exception as e:
                        log.error(f"[CARRY] V2 forced attempt #{forced_attempt+1} exception: {e}")
                        self._health_monitor.record_error(v2_n, str(e))

                    # Minimal delay between attempts (just enough for order book to update)
                    await asyncio.sleep(0.3)

                # Update f2 and p2 from total fills
                if total_v2_filled > 0:
                    f2 = total_v2_filled
                    p2 = total_v2_value / total_v2_filled

                # FIX 2026-01-24: Partial fill acceptance to reduce fee waste
                # If V2 >= 70% filled, accept the position instead of rolling back
                fill_pct = total_v2_filled / qty2 if qty2 > 0 else 0.0

                if not v2_retry_success and fill_pct >= 0.70:
                    # 70%+ filled - accept position with USD mismatch
                    log.warning(f"[CARRY] V2 {fill_pct*100:.0f}% filled ({total_v2_filled:.6f}/{qty2:.6f}), ACCEPTING partial position")
                    v2_retry_success = True  # Treat as success to proceed with position creation
                    f2 = total_v2_filled
                    p2 = total_v2_value / total_v2_filled if total_v2_filled > 0 else p2_estimate

                    # FIX 2026-01-25: Record orphan delta for tracking
                    # V1 filled more than V2, so we have unhedged exposure
                    v1_fill_usd = f1 * p1
                    v2_fill_usd = total_v2_filled * p2
                    orphan_delta = v1_fill_usd - v2_fill_usd
                    if abs(orphan_delta) > 1.0:  # Only track if > $1
                        self._orphan_deltas[hl_coin] = self._orphan_deltas.get(hl_coin, 0.0) + orphan_delta
                        log.warning(f"[ORPHAN] {hl_coin} delta recorded: ${orphan_delta:.2f} (total: ${self._orphan_deltas.get(hl_coin, 0):.2f})")

                # If all forced market attempts failed AND < 70% filled, execute rollback
                if not v2_retry_success:
                    # Only rollback if < 50% filled (too much imbalance)
                    if fill_pct >= 0.50:
                        # 50-70% filled - one more forced attempt
                        log.warning(f"[CARRY] V2 {fill_pct*100:.0f}% filled, making final completion attempt")
                        try:
                            final_qty = v2_obj.round_qty(s2, qty2 - total_v2_filled)
                            if final_qty > 0:
                                res_final = await v2_obj.place_order(s2, side2, final_qty, ioc=True)
                                final_filled = float(res_final.get("filled", 0.0))
                                if final_filled > 0:
                                    final_price = float(res_final.get("avg_price", 0.0)) or p2_estimate
                                    total_v2_filled += final_filled
                                    total_v2_value += final_filled * final_price
                                    f2 = total_v2_filled
                                    p2 = total_v2_value / total_v2_filled
                                    fill_pct = total_v2_filled / qty2
                                    if fill_pct >= 0.70:
                                        log.info(f"[CARRY] V2 final attempt pushed to {fill_pct*100:.0f}%, accepting")
                                        v2_retry_success = True
                        except Exception as e:
                            log.error(f"[CARRY] V2 final completion attempt failed: {e}")

                if not v2_retry_success:
                    log.error(f"[CARRY] CRITICAL: V2 {s2} only {fill_pct*100:.0f}% filled after {V2_FORCED_MAX_ATTEMPTS} attempts. ROLLING BACK Leg 1.")
                    inv_side = "SELL" if side1 == "BUY" else "BUY"
                    # CRITICAL: Aster doesn't support reduce_only=True
                    use_reduce_only_rb = v1_n != "AS"
                    rollback_success = False
                    # FIX 2026-01-22 Bug 2D: Track cumulative rollback fills
                    total_rollback_filled = 0.0
                    remaining_to_rollback = f1
                    for rb_attempt in range(3):
                        try:
                            qty_to_rollback = v1_obj.round_qty(s1, remaining_to_rollback)
                            if qty_to_rollback <= 0:
                                log.info(f"[CARRY] Rollback complete (remaining={remaining_to_rollback:.8f} rounds to 0)")
                                rollback_success = True
                                break
                            rb_res = await v1_obj.place_order(s1, inv_side, qty_to_rollback, ioc=True, reduce_only=use_reduce_only_rb)
                            rb_filled = float(rb_res.get("filled", 0.0))
                            if rb_filled > 0:
                                total_rollback_filled += rb_filled
                                remaining_to_rollback -= rb_filled
                                log.info(f"[CARRY] Rollback attempt {rb_attempt+1}: filled {rb_filled:.6f}, total={total_rollback_filled:.6f}/{f1:.6f}")
                                # Check if fully rolled back (with small tolerance for rounding)
                                if remaining_to_rollback <= f1 * 0.01 or total_rollback_filled >= f1 * 0.99:
                                    log.info(f"[CARRY] Rollback successful after V2 retry exhaustion (filled {total_rollback_filled:.6f}/{f1:.6f})")
                                    rollback_success = True
                                    break
                        except Exception as e:
                            log.error(f"[CARRY] Rollback attempt {rb_attempt+1} failed: {e}")
                        await asyncio.sleep(1.0)
                    # Log partial rollback warning
                    if not rollback_success and total_rollback_filled > 0:
                        log.error(f"[CARRY] PARTIAL V1 ROLLBACK: Only {total_rollback_filled:.6f}/{f1:.6f} rolled back. Orphan qty: {remaining_to_rollback:.6f}")
                        # FIX 2026-01-25: Record orphan delta for partial rollback
                        orphan_usd = remaining_to_rollback * p1
                        if orphan_usd > 1.0:  # Only track if > $1
                            # Determine direction: V1 was filled, so orphan is in that direction
                            direction_sign = 1.0 if side1 == "BUY" else -1.0
                            self._orphan_deltas[hl_coin] = self._orphan_deltas.get(hl_coin, 0.0) + (orphan_usd * direction_sign)
                            log.warning(f"[ORPHAN] {hl_coin} rollback delta recorded: ${orphan_usd:.2f} (total: ${self._orphan_deltas.get(hl_coin, 0):.2f})")

                    # FIX 2026-01-24: Log V1 rollback to orders CSV
                    self._log_order(
                        event="ROLLBACK",
                        symbol=s1,
                        hl_coin=hl_coin,
                        venue=v1_n,
                        side=inv_side,
                        qty_requested=f1,
                        qty_filled=total_rollback_filled,
                        order_type="IOC",
                        direction=direction,
                        action="ROLLBACK_V1_AFTER_V2_FAIL",
                        status="SUCCESS" if rollback_success else "PARTIAL",
                        fees_est_usd=(total_rollback_filled * p1) * (self._venue_fees.get(v1_n, {}).get("taker", 5.0) / 10000),
                        details=f"V2_failed, rolled_back={total_rollback_filled:.6f}/{f1:.6f}"
                    )

                    # FIX 2026-01-22: Also rollback V2 partial fills if any exist
                    # If V2 partially filled (but didn't reach 99%), we need to close those too
                    v2_rollback_success = False
                    if total_v2_filled > 0:
                        log.warning(f"[CARRY] V2 had partial fill {total_v2_filled:.6f} - rolling back V2 orphan too!")
                        inv_side2 = "SELL" if side2 == "BUY" else "BUY"
                        use_reduce_only_v2 = v2_n != "AS"  # Aster doesn't support reduce_only
                        total_v2_rollback = 0.0
                        remaining_v2_rollback = total_v2_filled
                        for v2_rb_attempt in range(3):
                            try:
                                qty_to_rb_v2 = v2_obj.round_qty(s2, remaining_v2_rollback)
                                if qty_to_rb_v2 <= 0:
                                    log.info(f"[CARRY] V2 orphan rollback complete (remaining rounds to 0)")
                                    v2_rollback_success = True
                                    break
                                rb_v2_res = await v2_obj.place_order(s2, inv_side2, qty_to_rb_v2, ioc=True, reduce_only=use_reduce_only_v2)
                                rb_v2_filled = float(rb_v2_res.get("filled", 0.0))
                                if rb_v2_filled > 0:
                                    total_v2_rollback += rb_v2_filled
                                    remaining_v2_rollback -= rb_v2_filled
                                    log.info(f"[CARRY] V2 orphan rollback attempt {v2_rb_attempt+1}: {rb_v2_filled:.6f}, total={total_v2_rollback:.6f}/{total_v2_filled:.6f}")
                                    if remaining_v2_rollback <= total_v2_filled * 0.01 or total_v2_rollback >= total_v2_filled * 0.99:
                                        log.info(f"[CARRY] V2 orphan rollback SUCCESS")
                                        v2_rollback_success = True
                                        break
                            except Exception as e:
                                log.error(f"[CARRY] V2 orphan rollback attempt {v2_rb_attempt+1} failed: {e}")
                            await asyncio.sleep(1.0)
                        if not v2_rollback_success and total_v2_rollback > 0:
                            log.error(f"[CARRY] PARTIAL V2 ROLLBACK: Only {total_v2_rollback:.6f}/{total_v2_filled:.6f} rolled back")
                    else:
                        v2_rollback_success = True  # No V2 to rollback

                    # Track partial fill with rollback (both legs considered)
                    both_rolled_back = rollback_success and v2_rollback_success
                    self._entry_attempts.insert(0, {
                        "ts": time.time(),
                        "symbol": target_sym,
                        "hl_coin": hl_coin,
                        "v1": v1_n,
                        "v2": v2_n,
                        "v1_status": "FILLED",
                        "v2_status": f"PARTIAL({total_v2_filled:.4f})" if total_v2_filled > 0 else "FAILED",
                        "orphan_created": not both_rolled_back,
                        "orphan_closed": both_rolled_back,
                        "reason": "Leg2_Failed_Rollback" + ("_BOTH_OK" if both_rolled_back else "_PARTIAL")
                    })
                    self._entry_attempts = self._entry_attempts[:self._max_entry_attempts_tracked]
                    self._set_entry_cooldown(hl_coin)  # Prevent rapid retries
                    # FIX 2026-01-22: Log failed entry (Leg 2) to CSV for audit trail
                    equity = await self._get_total_equity()
                    action = "ENTRY_FAILED_BOTH_ROLLBACK_OK" if both_rolled_back else "ENTRY_FAILED_ORPHAN_REMAINING"
                    # FIX 2026-01-24: Track failed entry fee waste in realized PnL
                    # V1: entry fee (taker) + rollback fee (taker) = 2x taker
                    # V2: if partial fill, entry fee (taker) + rollback fee (taker)
                    v1_fee_waste_bps = self._venue_fees[v1_n]["taker"] * 2  # Entry + rollback
                    v1_fill_usd = f1 * p1
                    v1_fee_waste_usd = v1_fill_usd * (v1_fee_waste_bps / 10000.0)

                    v2_fee_waste_usd = 0.0
                    if total_v2_filled > 0:
                        v2_fee_waste_bps = self._venue_fees[v2_n]["taker"] * 2  # Entry + rollback
                        v2_fill_usd = total_v2_filled * (total_v2_value / total_v2_filled if total_v2_filled > 0 else p2_estimate)
                        v2_fee_waste_usd = v2_fill_usd * (v2_fee_waste_bps / 10000.0)

                    total_fee_waste_usd = v1_fee_waste_usd + v2_fee_waste_usd
                    self._live_realized_pnl -= total_fee_waste_usd
                    log.warning(f"[CARRY] FAILED ENTRY FEE WASTE: ${total_fee_waste_usd:.4f} (V1=${v1_fee_waste_usd:.4f} V2=${v2_fee_waste_usd:.4f}) subtracted from realized PnL (cumulative: ${self._live_realized_pnl:.2f})")

                    self._log(
                        target_sym, action, direction, size_usd,
                        r1, r2, v1_n, v2_n,
                        0.0, 0.0, -total_fee_waste_usd, equity  # net_profit is negative fee waste
                    )
                    # FIX 2026-01-25: Log failed entry (Leg2) to decision log
                    self._log_decision(
                        symbol=target_sym,
                        hl_coin=hl_coin,
                        decision_type="ENTRY_FAILED" if action_name == "ENTRY" else "SCALE_UP_FAILED",
                        trigger_reason=f"yield={diff_bps:.1f}bps",
                        v1_venue=v1_n,
                        v2_venue=v2_n,
                        v1_bbo=(bbo1[0], bbo1[1]) if bbo1 else None,
                        v2_bbo=(bbo2[0], bbo2[1]) if bbo2 else None,
                        current_yield_bps=diff_bps,
                        position_size_usd=size_usd,
                        equity_usd=equity,
                        outcome="FAILED",
                        outcome_reason=f"Leg2_Failed_Rollback, fee_waste=${total_fee_waste_usd:.2f}"
                    )
                    self._clear_entry_lock(hl_coin)
                    # FIX 2026-01-24: Cleanup any orphaned orders on BOTH venues after Leg2 failure
                    for venue, sym, name in [(v1_obj, s1, v1_n), (v2_obj, s2, v2_n)]:
                        if hasattr(venue, 'cancel_all_orders'):
                            try:
                                await venue.cancel_all_orders(symbol=sym)
                                log.debug(f"[CARRY] Cleaned up {name} orders for {sym} after Leg2 failure")
                            except Exception as e:
                                log.debug(f"[CARRY] {name} cleanup skipped (expected if no orders): {e}")
                    # FIX 2026-01-22: Run post-trade sync even on failure to detect false failures
                    asyncio.create_task(self._post_trade_position_sync(hl_coin, v1_n, v2_n, direction, target_sym))
                    return  # Abort Entry

            prices["v2"] = p2

            # FIX 2026-01-24: Log successful V2 entry to orders CSV
            # OPTIMIZATION 2026-01-24: Include execution quality metrics
            is_maker_v2 = res2.get("is_maker", False) if 'res2' in dir() and res2 else False
            expected_px_v2 = bbo2[1] if side2 == "BUY" else bbo2[0]  # Expected fill at BBO
            taker_rate_v2 = self._venue_fees.get(v2_n, {}).get("taker", 5.0) / 10000
            maker_rate_v2 = self._venue_fees.get(v2_n, {}).get("maker", 2.0) / 10000
            actual_fee_v2 = (f2 * p2) * (maker_rate_v2 if is_maker_v2 else taker_rate_v2)
            taker_fee_v2 = (f2 * p2) * taker_rate_v2
            self._log_order(
                event="ORDER_FILLED",
                symbol=s2,
                hl_coin=hl_coin,
                venue=v2_n,
                side=side2,
                qty_requested=qty2,
                qty_filled=f2,
                avg_fill_price=p2,
                order_type="MAKER" if is_maker_v2 else "IOC",
                direction=direction,
                action="ENTRY_LEG2",
                status="SUCCESS",
                fees_est_usd=actual_fee_v2,
                details=f"diff_bps={diff_bps:.1f}, fill_usd=${f2*p2:.2f}, v1_fill=${f1*p1:.2f}",
                expected_price=expected_px_v2,
                is_maker=is_maker_v2,
                taker_fee_would_be=taker_fee_v2
            )

            # CRITICAL FIX: Verify positions exist on venues BEFORE updating internal state
            # This prevents hallucinated positions (like AVNT scale_up_count=2 when no scale happened)
            v1_confirmed, v2_confirmed = await self._verify_positions_on_venues_sync(
                hl_coin, v1_n, v2_n, target_sym, s1, s2
            )

            if not v1_confirmed or not v2_confirmed:
                log.error(f"[CARRY] ENTRY ABORTED: Position verification failed for {hl_coin}")
                log.error(f"  V1 ({v1_n}): {'CONFIRMED' if v1_confirmed else 'MISSING'}")
                log.error(f"  V2 ({v2_n}): {'CONFIRMED' if v2_confirmed else 'MISSING'}")

                # If one leg exists but not the other, we have an orphan - AUTO-ROLLBACK
                orphan_closed = False
                if v1_confirmed and not v2_confirmed:
                    log.error(f"[CARRY] UNHEDGED: Position on {v1_n} but NOT on {v2_n}! AUTO-ROLLBACK initiated.")
                    # FIX 2026-01-22: Auto-rollback V1 when V2 verification fails
                    inv_side1 = "SELL" if side1 == "BUY" else "BUY"
                    use_reduce_only_v1 = v1_n != "AS"  # Aster doesn't support reduce_only
                    rollback_success = False
                    total_rollback_filled = 0.0
                    remaining_to_rollback = f1
                    for rb_attempt in range(3):
                        try:
                            qty_to_rollback = v1_obj.round_qty(s1, remaining_to_rollback)
                            if qty_to_rollback <= 0:
                                log.info(f"[CARRY] V1 orphan rollback complete (remaining rounds to 0)")
                                rollback_success = True
                                break
                            rb_res = await v1_obj.place_order(s1, inv_side1, qty_to_rollback, ioc=True, reduce_only=use_reduce_only_v1)
                            rb_filled = float(rb_res.get("filled", 0.0))
                            if rb_filled > 0:
                                total_rollback_filled += rb_filled
                                remaining_to_rollback -= rb_filled
                                log.info(f"[CARRY] V1 orphan rollback attempt {rb_attempt+1}: filled {rb_filled:.6f}, total={total_rollback_filled:.6f}/{f1:.6f}")
                                if remaining_to_rollback <= f1 * 0.01 or total_rollback_filled >= f1 * 0.99:
                                    log.info(f"[CARRY] V1 orphan rollback SUCCESS after verification failure")
                                    rollback_success = True
                                    break
                        except Exception as e:
                            log.error(f"[CARRY] V1 orphan rollback attempt {rb_attempt+1} failed: {e}")
                        await asyncio.sleep(1.0)
                    orphan_closed = rollback_success
                    if self.notifier:
                        status_msg = "CLOSED" if rollback_success else "FAILED TO CLOSE"
                        await self.notifier.notify(
                            f"ENTRY FAILED: {hl_coin}",
                            f"V2 ({v2_n}) missing after verification.\nV1 ({v1_n}) orphan rollback: {status_msg}\n"
                            f"Rolled back: {total_rollback_filled:.6f}/{f1:.6f}"
                        )
                elif v2_confirmed and not v1_confirmed:
                    log.error(f"[CARRY] UNHEDGED: Position on {v2_n} but NOT on {v1_n}! AUTO-ROLLBACK initiated.")
                    # FIX 2026-01-22: Auto-rollback V2 when V1 verification fails
                    inv_side2 = "SELL" if side2 == "BUY" else "BUY"
                    use_reduce_only_v2 = v2_n != "AS"  # Aster doesn't support reduce_only
                    rollback_success = False
                    total_rollback_filled = 0.0
                    remaining_to_rollback = f2
                    for rb_attempt in range(3):
                        try:
                            qty_to_rollback = v2_obj.round_qty(s2, remaining_to_rollback)
                            if qty_to_rollback <= 0:
                                log.info(f"[CARRY] V2 orphan rollback complete (remaining rounds to 0)")
                                rollback_success = True
                                break
                            rb_res = await v2_obj.place_order(s2, inv_side2, qty_to_rollback, ioc=True, reduce_only=use_reduce_only_v2)
                            rb_filled = float(rb_res.get("filled", 0.0))
                            if rb_filled > 0:
                                total_rollback_filled += rb_filled
                                remaining_to_rollback -= rb_filled
                                log.info(f"[CARRY] V2 orphan rollback attempt {rb_attempt+1}: filled {rb_filled:.6f}, total={total_rollback_filled:.6f}/{f2:.6f}")
                                if remaining_to_rollback <= f2 * 0.01 or total_rollback_filled >= f2 * 0.99:
                                    log.info(f"[CARRY] V2 orphan rollback SUCCESS after verification failure")
                                    rollback_success = True
                                    break
                        except Exception as e:
                            log.error(f"[CARRY] V2 orphan rollback attempt {rb_attempt+1} failed: {e}")
                        await asyncio.sleep(1.0)
                    orphan_closed = rollback_success
                    if self.notifier:
                        status_msg = "CLOSED" if rollback_success else "FAILED TO CLOSE"
                        await self.notifier.notify(
                            f"ENTRY FAILED: {hl_coin}",
                            f"V1 ({v1_n}) missing after verification.\nV2 ({v2_n}) orphan rollback: {status_msg}\n"
                            f"Rolled back: {total_rollback_filled:.6f}/{f2:.6f}"
                        )
                else:
                    log.error(f"[CARRY] Both legs missing on venues - order may have been rejected")

                # Track verification failure / orphan creation
                self._entry_attempts.insert(0, {
                    "ts": time.time(),
                    "symbol": target_sym,
                    "hl_coin": hl_coin,
                    "v1": v1_n,
                    "v2": v2_n,
                    "v1_status": "CONFIRMED" if v1_confirmed else "MISSING",
                    "v2_status": "CONFIRMED" if v2_confirmed else "MISSING",
                    "orphan_created": (v1_confirmed != v2_confirmed),
                    "orphan_closed": orphan_closed,  # FIX 2026-01-22: Use actual rollback status
                    "reason": "Position_Verification_Failed"
                })
                self._entry_attempts = self._entry_attempts[:self._max_entry_attempts_tracked]
                self._set_entry_cooldown(hl_coin)  # Prevent rapid retries

                # FIX 2026-01-24: Track orphan rollback fee waste
                orphan_fee_waste_usd = 0.0
                if v1_confirmed and not v2_confirmed and orphan_closed:
                    # V1 existed, rolled back: entry + rollback fees
                    v1_fee_waste_bps = self._venue_fees[v1_n]["taker"] * 2
                    v1_fill_usd_orphan = f1 * prices.get("v1", p1)
                    orphan_fee_waste_usd = v1_fill_usd_orphan * (v1_fee_waste_bps / 10000.0)
                elif v2_confirmed and not v1_confirmed and orphan_closed:
                    # V2 existed, rolled back: entry + rollback fees
                    v2_fee_waste_bps = self._venue_fees[v2_n]["taker"] * 2
                    v2_fill_usd_orphan = f2 * prices.get("v2", p2)
                    orphan_fee_waste_usd = v2_fill_usd_orphan * (v2_fee_waste_bps / 10000.0)

                if orphan_fee_waste_usd > 0:
                    self._live_realized_pnl -= orphan_fee_waste_usd
                    log.warning(f"[CARRY] ORPHAN ROLLBACK FEE WASTE: ${orphan_fee_waste_usd:.4f} subtracted from realized PnL (cumulative: ${self._live_realized_pnl:.2f})")

                # FIX 2026-01-22: Log position verification failure to CSV for audit trail
                equity = await self._get_total_equity()
                if v1_confirmed != v2_confirmed:
                    # FIX 2026-01-22: Distinguish between auto-closed and still-open orphans
                    action = "ENTRY_FAILED_ORPHAN_CLOSED" if orphan_closed else "ENTRY_FAILED_ORPHAN_OPEN"
                else:
                    action = "ENTRY_FAILED_BOTH_MISSING"  # Neither leg exists
                self._log(
                    target_sym, action, direction, size_usd,
                    r1, r2, v1_n, v2_n,
                    0.0, 0.0, -orphan_fee_waste_usd, equity  # net_profit is negative fee waste
                )

                # DO NOT update internal state - positions don't exist
                self._clear_entry_lock(hl_coin)
                # FIX 2026-01-22: Run post-trade sync to catch false verification failures
                # Sometimes verification returns "missing" but positions exist (timing/symbol matching)
                asyncio.create_task(self._post_trade_position_sync(hl_coin, v1_n, v2_n, direction, target_sym))
                return

            # ENTRY LOCK RELEASE: Both legs completed and verified successfully
            self._clear_entry_lock(hl_coin)

        # Update or Create Position (ONLY if live verification passed or in paper/dry-run mode)
        # FIX 2026-01-22 Bug 2B: Calculate actual filled sizes, not intended
        actual_v1_usd = f1 * prices["v1"]  # Actual USD filled on V1
        actual_v2_usd = f2 * prices["v2"]  # Actual USD filled on V2
        actual_total_usd = actual_v1_usd + actual_v2_usd

        # FIX 2026-01-22: Check leg size mismatch BEFORE creating position
        # Reject entries with >10% mismatch to prevent unhedged exposure
        MAX_LEG_MISMATCH_PCT = 10.0
        size_diff_pct = abs(actual_v1_usd - actual_v2_usd) / max(actual_v1_usd, actual_v2_usd, 1) * 100
        if size_diff_pct > MAX_LEG_MISMATCH_PCT:
            log.error(f"[CARRY] ENTRY REJECTED: Leg size mismatch too large! "
                     f"V1=${actual_v1_usd:.2f} V2=${actual_v2_usd:.2f} ({size_diff_pct:.1f}% diff > {MAX_LEG_MISMATCH_PCT}%)")
            log.error(f"[CARRY] Rolling back BOTH legs to prevent unhedged exposure...")

            # Rollback V1
            inv_side1 = "SELL" if side1 == "BUY" else "BUY"
            use_reduce_only_v1 = v1_n != "AS"
            try:
                rb1 = await v1_obj.place_order(s1, inv_side1, f1, ioc=True, reduce_only=use_reduce_only_v1)
                rb1_filled = float(rb1.get("filled", 0))
                log.info(f"[CARRY] Rollback V1: closed {rb1_filled:.6f} of {f1:.6f}")
            except Exception as e:
                log.error(f"[CARRY] Rollback V1 FAILED: {e}")

            # Rollback V2
            inv_side2 = "SELL" if side2 == "BUY" else "BUY"
            use_reduce_only_v2 = v2_n != "AS"
            try:
                rb2 = await v2_obj.place_order(s2, inv_side2, f2, ioc=True, reduce_only=use_reduce_only_v2)
                rb2_filled = float(rb2.get("filled", 0))
                log.info(f"[CARRY] Rollback V2: closed {rb2_filled:.6f} of {f2:.6f}")
            except Exception as e:
                log.error(f"[CARRY] Rollback V2 FAILED: {e}")

            if self.notifier:
                await self.notifier.notify(
                    f"ENTRY REJECTED: {hl_coin}",
                    f"Leg size mismatch {size_diff_pct:.1f}% > {MAX_LEG_MISMATCH_PCT}%\n"
                    f"V1: ${actual_v1_usd:.2f}\n"
                    f"V2: ${actual_v2_usd:.2f}\n"
                    f"Both legs rolled back to prevent unhedged exposure."
                )
            return

        if size_diff_pct > 2.0:
            log.warning(f"[CARRY] SIZE MISMATCH on entry: V1=${actual_v1_usd:.2f} V2=${actual_v2_usd:.2f} ({size_diff_pct:.1f}% diff)")

        if existing_pos:
            old_size = existing_pos.size_usd
            new_size = actual_total_usd  # FIX: Use actual filled, not intended
            total_size = old_size + new_size

            # FIX 2026-01-22: Calculate and track scale-up fees based on actual maker/taker fills
            if not self.paper_mode and not self.dry_run_live:
                fee_v1 = self._venue_fees[v1_n]["maker"] if is_maker_v1 else self._venue_fees[v1_n]["taker"]
                fee_v2 = self._venue_fees[v2_n]["maker"] if is_maker_v2 else self._venue_fees[v2_n]["taker"]
            else:
                fee_v1 = self._venue_fees[v1_n]["taker"]
                fee_v2 = self._venue_fees[v2_n]["taker"]
            scale_fee_bps = fee_v1 + fee_v2
            scale_fee_usd = new_size * (scale_fee_bps / 10000.0)

            px1 = prices["v1"] if v1_n == "HL" else prices["v2"]
            px2 = prices["v2"] if v1_n == "HL" else prices["v1"]

            # Weighted Averages
            existing_pos.entry_px_hl = (old_size * existing_pos.entry_px_hl + new_size * px1) / total_size
            existing_pos.entry_px_as = (old_size * existing_pos.entry_px_as + new_size * px2) / total_size
            existing_pos.entry_diff_bps = (old_size * existing_pos.entry_diff_bps + new_size * diff_bps) / total_size
            # BUGFIX: Also track weighted average of SIGNED diff for reversal detection
            existing_pos.entry_signed_diff_bps = (old_size * existing_pos.entry_signed_diff_bps + new_size * raw_diff_bps) / total_size
            existing_pos.size_usd = total_size
            # FIX 2026-01-22: Update per-leg sizes
            existing_pos.size_v1_usd += actual_v1_usd
            existing_pos.size_v2_usd += actual_v2_usd
            existing_pos.scale_up_count += 1  # Increment scale-up counter
            self._last_scale_up_time[hl_coin] = time.time()  # BUGFIX: Track scale-up time for cooldown

            # FIX 2026-01-22: Record fee operation for scale-up via entry flow
            if existing_pos.fee_operations is None:
                existing_pos.fee_operations = []
            existing_pos.fee_operations.append({
                "op": "scale_up",
                "size_usd": new_size,
                "fee_bps": scale_fee_bps,
                "fee_usd": scale_fee_usd,
                "v1_maker": is_maker_v1 if not self.paper_mode and not self.dry_run_live else False,
                "v2_maker": is_maker_v2 if not self.paper_mode and not self.dry_run_live else False,
                "ts": time.time()
            })
            # P2 FIX 2026-01-22: Cap fee_operations list to prevent unbounded growth
            if len(existing_pos.fee_operations) > 50:
                existing_pos.fee_operations = existing_pos.fee_operations[-50:]
            existing_pos.total_fees_paid_usd = (existing_pos.total_fees_paid_usd or 0.0) + scale_fee_usd

            # P0-2 FIX 2026-01-23: Record scale-up entry
            if existing_pos.entries is None:
                existing_pos.entries = []
            existing_pos.entries.append({
                "ts": time.time(),
                "px_v1": prices["v1"],
                "px_v2": prices["v2"],
                "size_usd": new_size,
                "is_scale_up": True
            })
            if len(existing_pos.entries) > 50:
                existing_pos.entries = existing_pos.entries[-50:]

            pairs_scaled = sum(1 for p in list(self.positions.values()) if p.scale_up_count > 0)
            log.info(f"[CARRY] SCALE_UP SUCCESS {hl_coin}: New Size ${total_size:.2f} (V1=${existing_pos.size_v1_usd:.2f}/V2=${existing_pos.size_v2_usd:.2f}) Avg Yield: {existing_pos.entry_diff_bps:.1f}bps (scale-up #{existing_pos.scale_up_count}/{self.max_scale_ups_per_position}, pairs scaled: {pairs_scaled}/{self.max_pairs_with_scaleup}) "
                    f"Fees: V1={'maker' if is_maker_v1 else 'taker'} V2={'maker' if is_maker_v2 else 'taker'} +{scale_fee_bps:.1f}bps/${scale_fee_usd:.2f} (Total: ${existing_pos.total_fees_paid_usd:.2f})")
        else:
            # PHASE 6: Calculate actual entry fees based on maker/taker fills
            # For paper mode or dry-run, assume taker fees (conservative)
            if not self.paper_mode and not self.dry_run_live:
                # Live mode: calculate actual fees based on fill types
                fee_v1 = self._venue_fees[v1_n]["maker"] if is_maker_v1 else self._venue_fees[v1_n]["taker"]
                fee_v2 = self._venue_fees[v2_n]["maker"] if is_maker_v2 else self._venue_fees[v2_n]["taker"]
                entry_fee_bps = fee_v1 + fee_v2
                log.info(f"[CARRY] Entry fees: {v1_n}={'maker' if is_maker_v1 else 'taker'}({fee_v1:.1f}bps) + "
                        f"{v2_n}={'maker' if is_maker_v2 else 'taker'}({fee_v2:.1f}bps) = {entry_fee_bps:.1f}bps")
            else:
                # Paper/dry-run: assume taker fees
                entry_fee_bps = self._venue_fees[v1_n]["taker"] + self._venue_fees[v2_n]["taker"]

            # FIX 2026-01-22: Calculate entry fee in USD for proper tracking
            entry_fee_usd = actual_total_usd * (entry_fee_bps / 10000.0)
            entry_fee_op = {
                "op": "entry",
                "size_usd": actual_total_usd,
                "fee_bps": entry_fee_bps,
                "fee_usd": entry_fee_usd,
                "v1_maker": is_maker_v1 if not self.paper_mode and not self.dry_run_live else False,
                "v2_maker": is_maker_v2 if not self.paper_mode and not self.dry_run_live else False,
                "ts": time.time()
            }

            new_pos = CarryPosition(
                symbol=target_sym,
                hl_coin=hl_coin,
                direction=direction,
                size_usd=actual_total_usd,  # FIX 2026-01-22 Bug 2B: Use actual fills, not intended
                entry_time=time.time(),
                entry_funding_hl=r1 if v1_n == "HL" else r2,
                entry_funding_as=r2 if v1_n == "HL" else r1,
                entry_px_hl=prices["v1"] if v1_n == "HL" else prices["v2"],
                entry_px_as=prices["v2"] if v1_n == "HL" else prices["v1"],
                entry_diff_bps=diff_bps,
                entry_signed_diff_bps=raw_diff_bps,  # BUGFIX: Store SIGNED diff for reversal detection
                realized_funding=0.0,
                accrued_funding_usd=0.0,
                last_accrual_time=time.time(),
                # PHASE 6: Maker tracking fields
                entry_v1_maker=is_maker_v1 if not self.paper_mode and not self.dry_run_live else False,
                entry_v2_maker=is_maker_v2 if not self.paper_mode and not self.dry_run_live else False,
                entry_fee_bps=entry_fee_bps,
                # FIX 2026-01-22: Per-leg size tracking
                size_v1_usd=actual_v1_usd,
                size_v2_usd=actual_v2_usd,
                # FIX 2026-01-22: Comprehensive fee tracking
                total_fees_paid_usd=entry_fee_usd,
                fee_operations=[entry_fee_op],
                # P0-2 FIX 2026-01-23: Multi-entry tracking
                entries=[{
                    "ts": time.time(),
                    "px_v1": prices["v1"],
                    "px_v2": prices["v2"],
                    "size_usd": actual_total_usd,
                    "is_scale_up": False
                }]
            )
            self.positions[new_pos.symbol] = new_pos
            self.total_entries += 1
            self._hourly_stats["entries"] += 1  # Track hourly entries

        self._save_state() # PERSIST IMMEDIATELY
        # FIX 2026-01-22: Log maker/taker status for entry (use actual filled size)
        self._log(target_sym, action_name, direction, actual_total_usd, r1, r2, v1_n, v2_n,
                  equity=(self.total_paper_equity if self.paper_mode else total_equity),
                  v1_maker=is_maker_v1, v2_maker=is_maker_v2)

        # OPTIMIZATION 2026-01-24: Log entry decision with full context
        try:
            self._log_decision(
                symbol=target_sym,
                hl_coin=hl_coin,
                decision_type="ENTRY_SUCCESS" if not is_scale_up else "SCALE_UP_SUCCESS",
                trigger_reason=f"yield={diff_bps:.1f}bps",
                v1_venue=v1_n,
                v2_venue=v2_n,
                current_yield_bps=diff_bps,
                position_size_usd=actual_total_usd,
                equity_usd=self._cached_total_equity if self._cached_total_equity > 0 else self.total_paper_equity,
                outcome="SUCCESS",
                outcome_reason=f"V1=${actual_v1_usd:.0f} V2=${actual_v2_usd:.0f}"
            )
        except Exception as e:
            log.warning(f"[CARRY] Entry decision log failed: {e}")

        # NOTE: Position verification now happens BEFORE state update (see above)
        # Old async fire-and-forget verification removed - was unreliable

        # FIX 2026-01-22: Post-trade sync to ensure internal state matches venues
        asyncio.create_task(self._post_trade_position_sync(hl_coin, v1_n, v2_n, direction, target_sym))

    async def _post_trade_position_sync(self, hl_coin: str, v1_name: str, v2_name: str,
                                        direction: str, symbol: str) -> None:
        """
        FIX 2026-01-22: Fast position sync AFTER each trade to ensure internal state matches reality.

        This function addresses the critical issue: "WE CANT ALLOW THAT THE BOT DOES NOT REFLECT REALITY"

        Called after:
        - Successful entries
        - Failed entries (to catch false failures where positions exist on venues)
        - Exits

        If positions exist on venues but not internally:
        - RECOVERS them by adding to internal state
        - Logs a CORRECTION to carry_audit.csv

        If positions exist internally but not on venues:
        - Flags them for reconciliation (handled by _reconcile_state_with_venues)
        """
        try:
            # Wait for position propagation (venues can be slow to reflect)
            await asyncio.sleep(2.0)

            # Get venue objects
            v1_obj = self.hl if v1_name == "HL" else (self.asr if v1_name == "AS" else self.lighter)
            v2_obj = self.hl if v2_name == "HL" else (self.asr if v2_name == "AS" else self.lighter)

            if not v1_obj or not v2_obj:
                return

            # Fetch positions from both venues
            try:
                v1_positions, v2_positions = await asyncio.gather(
                    v1_obj.get_positions(),
                    v2_obj.get_positions(),
                    return_exceptions=True
                )
            except Exception as e:
                log.debug(f"[SYNC] Position fetch failed: {e}")
                return

            if isinstance(v1_positions, Exception):
                v1_positions = []
            if isinstance(v2_positions, Exception):
                v2_positions = []

            v1_positions = v1_positions or []
            v2_positions = v2_positions or []

            # Normalize coin name for matching
            base_coin = hl_coin.upper().replace("USDT", "").replace("USD", "").replace("-PERP", "")

            # Check if this coin exists on venues
            v1_size = 0.0
            v1_price = 0.0
            v2_size = 0.0
            v2_price = 0.0

            for pos in v1_positions:
                pos_sym = pos.get('coin', pos.get('symbol', '')).upper().replace("USDT", "").replace("-PERP", "")
                if pos_sym == base_coin:
                    v1_size = abs(float(pos.get('size', pos.get('positionAmt', 0))))
                    v1_price = float(pos.get('markPx', pos.get('entryPx', pos.get('markPrice', 0)))) or 0.0
                    break

            for pos in v2_positions:
                pos_sym = pos.get('coin', pos.get('symbol', '')).upper().replace("USDT", "").replace("-PERP", "")
                if pos_sym == base_coin:
                    v2_size = abs(float(pos.get('size', pos.get('positionAmt', 0))))
                    v2_price = float(pos.get('markPx', pos.get('entryPx', pos.get('markPrice', 0)))) or 0.0
                    break

            has_v1_on_venue = v1_size > 0
            has_v2_on_venue = v2_size > 0
            has_internal = symbol in self.positions or any(p.hl_coin == hl_coin for p in list(self.positions.values()))

            log.debug(f"[SYNC] {hl_coin}: V1({v1_name})={v1_size:.6f} V2({v2_name})={v2_size:.6f} internal={has_internal}")

            # CRITICAL FIX: If both legs exist on venues but NOT internally -> RECOVER
            if has_v1_on_venue and has_v2_on_venue and not has_internal:
                # Positions exist on venues but bot doesn't know about them!
                # This is the "false failure" case - RECOVER by adding to internal state

                v1_usd = v1_size * v1_price if v1_price > 0 else v1_size * 100.0
                v2_usd = v2_size * v2_price if v2_price > 0 else v2_size * 100.0
                total_usd = v1_usd + v2_usd

                log.warning(f"[SYNC] RECOVERY: {hl_coin} has positions on BOTH venues but NOT tracked internally!")
                log.warning(f"[SYNC] RECOVERY: V1({v1_name})=${v1_usd:.2f} V2({v2_name})=${v2_usd:.2f} Total=${total_usd:.2f}")

                # Get current funding rates for this pair
                r1 = self._cached_hl_rates.get(hl_coin, 0.0) if v1_name == "HL" else (
                     self._cached_as_rates.get(symbol, 0.0) if v1_name == "AS" else
                     self._cached_lt_rates.get(hl_coin, 0.0))
                r2 = self._cached_hl_rates.get(hl_coin, 0.0) if v2_name == "HL" else (
                     self._cached_as_rates.get(symbol, 0.0) if v2_name == "AS" else
                     self._cached_lt_rates.get(hl_coin, 0.0))

                entry_diff = abs((r1 - r2) * 10000.0)

                # Create position from recovered data
                recovered_pos = CarryPosition(
                    symbol=symbol,
                    hl_coin=hl_coin,
                    direction=direction,
                    size_usd=total_usd,
                    entry_time=time.time(),
                    entry_funding_hl=r1 if v1_name == "HL" else r2,
                    entry_funding_as=r2 if v1_name == "HL" else r1,
                    entry_px_hl=v1_price if v1_name == "HL" else v2_price,
                    entry_px_as=v2_price if v1_name == "HL" else v1_price,
                    entry_diff_bps=entry_diff,
                    entry_signed_diff_bps=(r1 - r2) * 10000.0,
                    realized_funding=0.0,
                    accrued_funding_usd=0.0,
                    last_accrual_time=time.time(),
                    size_v1_usd=v1_usd,
                    size_v2_usd=v2_usd
                )

                self.positions[symbol] = recovered_pos
                self._save_state()

                # Log recovery to audit CSV
                equity = await self._get_total_equity()
                self._log(
                    symbol, "RECOVERED_FROM_VENUES", direction, total_usd,
                    r1, r2, v1_name, v2_name,
                    0.0, 0.0, 0.0, equity
                )

                log.warning(f"[SYNC] RECOVERED {hl_coin}: Added to internal state with ${total_usd:.2f}")

                if self.notifier:
                    await self.notifier.notify(
                        f"POSITION RECOVERED: {hl_coin}",
                        f"Positions found on venues but not tracked internally!\n"
                        f"V1({v1_name}): ${v1_usd:.2f}\n"
                        f"V2({v2_name}): ${v2_usd:.2f}\n"
                        f"Total: ${total_usd:.2f}\n"
                        f"Now tracking in internal state."
                    )

            # If one leg exists but not the other - flag for orphan recovery
            elif (has_v1_on_venue != has_v2_on_venue) and not has_internal:
                missing_leg = v2_name if has_v1_on_venue else v1_name
                existing_leg = v1_name if has_v1_on_venue else v2_name
                log.warning(f"[SYNC] ORPHAN DETECTED: {hl_coin} has position on {existing_leg} but NOT on {missing_leg}")
                # Let the orphan detection system handle this

        except Exception as e:
            log.debug(f"[SYNC] Post-trade sync error for {hl_coin}: {e}")

    def _cleanup_positions(self, symbols: List[str]):
        """Remove positions from the active tracking."""
        for sym in symbols:
            if sym in self.positions:
                del self.positions[sym]
        self._save_state()

    async def _verify_positions_on_venues_sync(
        self, hl_coin: str, v1_name: str, v2_name: str, symbol: str, s1: str, s2: str
    ) -> Tuple[bool, bool]:
        """
        SYNCHRONOUS position verification: Query venues and confirm positions exist.
        Called BEFORE updating internal state to prevent hallucinated positions.

        Returns: (v1_confirmed, v2_confirmed) - tuple of bools indicating if each leg exists

        FIX 2026-01-09: Added retry logic with longer initial wait for Lighter position propagation.
        Lighter positions can take 2-3 seconds to appear in the account API after order fill.
        """
        VERIFY_MAX_ATTEMPTS = 3
        VERIFY_INITIAL_WAIT = 2.0  # Increased from 1.0s
        VERIFY_RETRY_WAIT = 1.5

        await asyncio.sleep(VERIFY_INITIAL_WAIT)  # Allow order propagation time

        try:
            # Get venue objects
            v1_obj = self.hl if v1_name == "HL" else (self.asr if v1_name == "AS" else self.lighter)
            v2_obj = self.hl if v2_name == "HL" else (self.asr if v2_name == "AS" else self.lighter)

            if not v1_obj or not v2_obj:
                log.error(f"[CARRY] Position verification BLOCKED - venue unavailable")
                # FIX 2026-01-22 Bug #5: Return False to BLOCK entry if venues unavailable
                return False, False

            # Symbol normalization for matching - expanded to cover more formats
            base_coin = hl_coin.upper().replace("USDT", "").replace("USDC", "").replace("-PERP", "")
            search_symbols = {
                hl_coin.upper(),
                symbol.upper(),
                s1.upper(),
                base_coin,
                base_coin + "USDT",
                base_coin + "USDC",
                base_coin + "/USDC",
                base_coin + "-USDC",  # FIX 2026-01-24 Bug #1: Lighter uses dash format
                base_coin + "-PERP",
            }
            search_symbols_v2 = {
                hl_coin.upper(),
                symbol.upper(),
                s2.upper(),
                base_coin,
                base_coin + "USDT",
                base_coin + "USDC",
                base_coin + "/USDC",
                base_coin + "-USDC",  # FIX 2026-01-24 Bug #1: Lighter uses dash format (DOLO-USDC)
                base_coin + "-PERP",
            }

            v1_found = False
            v2_found = False
            v1_size = 0.0
            v2_size = 0.0

            for attempt in range(VERIFY_MAX_ATTEMPTS):
                # Fetch positions from both venues in parallel
                try:
                    v1_positions, v2_positions = await asyncio.gather(
                        v1_obj.get_positions(),
                        v2_obj.get_positions(),
                        return_exceptions=True
                    )
                except Exception as e:
                    log.error(f"[CARRY] Error fetching positions for verification: {e}")
                    # FIX 2026-01-22 Bug #5: Return False to BLOCK entry on API errors
                    # Better to miss an entry than create hallucinated positions
                    return False, False

                # Handle exceptions from gather
                if isinstance(v1_positions, Exception):
                    log.warning(f"[CARRY] V1 position fetch failed: {v1_positions}")
                    v1_positions = []
                if isinstance(v2_positions, Exception):
                    log.warning(f"[CARRY] V2 position fetch failed: {v2_positions}")
                    v2_positions = []

                v1_positions = v1_positions or []
                v2_positions = v2_positions or []

                # Log received positions on first attempt for debugging
                if attempt == 0:
                    v1_syms = [p.get('symbol', p.get('coin', '?')) for p in v1_positions]
                    v2_syms = [p.get('symbol', p.get('coin', '?')) for p in v2_positions]
                    log.info(f"[CARRY] VERIFY {hl_coin}: V1({v1_name}) has {len(v1_positions)} positions: {v1_syms}")
                    log.info(f"[CARRY] VERIFY {hl_coin}: V2({v2_name}) has {len(v2_positions)} positions: {v2_syms}")
                    log.info(f"[CARRY] VERIFY {hl_coin}: Searching for: {search_symbols_v2}")

                # Find V1 position
                if not v1_found:
                    for pos in v1_positions:
                        pos_symbol = pos.get('symbol', '').upper()
                        pos_coin = pos.get('coin', '').upper()
                        if pos_symbol in search_symbols or pos_coin in search_symbols or pos_coin == base_coin:
                            v1_found = True
                            v1_size = abs(float(pos.get('size', 0)))
                            log.info(f"[CARRY] V1 {v1_name}: Found position for {pos_symbol}/{pos_coin} size={v1_size:.6f}")
                            break

                # Find V2 position
                if not v2_found:
                    for pos in v2_positions:
                        pos_symbol = pos.get('symbol', '').upper()
                        pos_coin = pos.get('coin', '').upper()
                        if pos_symbol in search_symbols_v2 or pos_coin in search_symbols_v2 or pos_coin == base_coin:
                            v2_found = True
                            v2_size = abs(float(pos.get('size', 0)))
                            log.info(f"[CARRY] V2 {v2_name}: Found position for {pos_symbol}/{pos_coin} size={v2_size:.6f}")
                            break

                if v1_found and v2_found:
                    log.info(f"[CARRY] VERIFY OK: {hl_coin} confirmed on both venues "
                            f"({v1_name}={v1_size:.6f}, {v2_name}={v2_size:.6f}) after {attempt+1} attempt(s)")
                    return True, True

                # If not both found and we have more attempts, wait and retry
                if attempt < VERIFY_MAX_ATTEMPTS - 1:
                    log.info(f"[CARRY] VERIFY attempt {attempt+1}/{VERIFY_MAX_ATTEMPTS}: "
                            f"V1={v1_found} V2={v2_found}, retrying in {VERIFY_RETRY_WAIT}s...")
                    await asyncio.sleep(VERIFY_RETRY_WAIT)

            # Final result after all attempts
            log.warning(f"[CARRY] VERIFY INCOMPLETE after {VERIFY_MAX_ATTEMPTS} attempts: "
                       f"{hl_coin} V1({v1_name})={v1_found} V2({v2_name})={v2_found}")
            return v1_found, v2_found

        except Exception as e:
            log.error(f"[CARRY] Position verification exception: {e}")
            import traceback
            traceback.print_exc()
            # FIX 2026-01-22 Bug #5: Return False to BLOCK entry on errors
            return False, False

    async def _verify_position_against_venues(self, hl_coin: str, v1_name: str, v2_name: str, symbol: str):
        """
        Async verification: Query venues and confirm position exists after entry.
        Log warning if positions don't match expected state.
        """
        await asyncio.sleep(2.0)  # Allow order propagation time

        try:
            # Get venue objects
            v1_obj = self.hl if v1_name == "HL" else (self.asr if v1_name == "AS" else self.lighter)
            v2_obj = self.hl if v2_name == "HL" else (self.asr if v2_name == "AS" else self.lighter)

            if not v1_obj or not v2_obj:
                log.warning(f"[CARRY] Position verification skipped - venue unavailable for {symbol}")
                return

            # Fetch positions from both venues
            v1_positions = await v1_obj.get_positions() or []
            v2_positions = await v2_obj.get_positions() or []

            # Find our positions
            v1_found = False
            v2_found = False
            v1_size = 0.0
            v2_size = 0.0

            for pos in v1_positions:
                pos_symbol = pos.get('symbol', '').upper()
                if pos_symbol == hl_coin.upper() or pos_symbol == symbol.upper():
                    v1_found = True
                    v1_size = abs(float(pos.get('size', 0)))
                    break

            for pos in v2_positions:
                pos_symbol = pos.get('symbol', '').upper()
                if pos_symbol == hl_coin.upper() or pos_symbol == symbol.upper():
                    v2_found = True
                    v2_size = abs(float(pos.get('size', 0)))
                    break

            # Check if internal state matches venue state
            internal_pos = self.positions.get(symbol)
            if not internal_pos:
                log.warning(f"[CARRY] VERIFY: No internal position for {symbol} - may have been exited")
                return

            if v1_found and v2_found:
                log.info(f"[CARRY] VERIFY OK: {symbol} confirmed on both venues (V1={v1_size:.4f}, V2={v2_size:.4f})")
            elif v1_found and not v2_found:
                log.error(f"[CARRY] POSITION MISMATCH: {symbol} exists on {v1_name} but NOT on {v2_name}! UNHEDGED RISK!")
                if self.notifier:
                    await self.notifier.notify(
                        f"MISMATCH: {symbol}",
                        f"Found on {v1_name} (size={v1_size:.4f}), MISSING on {v2_name}!"
                    )
            elif not v1_found and v2_found:
                log.error(f"[CARRY] POSITION MISMATCH: {symbol} exists on {v2_name} but NOT on {v1_name}! UNHEDGED RISK!")
                if self.notifier:
                    await self.notifier.notify(
                        f"MISMATCH: {symbol}",
                        f"Found on {v2_name} (size={v2_size:.4f}), MISSING on {v1_name}!"
                    )
            else:
                log.error(f"[CARRY] POSITION MISSING: {symbol} not found on EITHER venue! Entry may have failed!")
                if self.notifier:
                    await self.notifier.notify(
                        f"MISSING: {symbol}",
                        f"Not found on {v1_name} or {v2_name}! Entry may have failed."
                    )

        except Exception as e:
            log.error(f"[CARRY] Position verification failed for {symbol}: {e}")

    # =========================================================================
    # SECTION 7: UTILITIES
    # =========================================================================

    def _calc_pnl(self, pos: CarryPosition, current_diff: float, hold_hours: float) -> float:
        """Estimate PnL = Yield Accrued + Realized Capital PnL (if any)."""
        # Yield = Absolute Entry Diff * Time
        yield_bps = abs(pos.entry_diff_bps) * (hold_hours / 8.0)
        pnl_usd = pos.size_usd * (yield_bps / 10000.0)
        return pnl_usd

    def get_positions_summary(self) -> List[Dict[str, Any]]:
        """Return summary of open positions for UI/Reporting."""
        summary = []
        now = time.time()
        # FIX 2026-01-22: Use list() to prevent RuntimeError if positions modified during iteration
        for sym, pos in list(self.positions.items()):
            mtm = self._calc_mtm(pos)
            # FIX 2026-01-21: Include CURRENT yield for dashboard display (not just entry yield)
            # FIX 2026-01-25: REMOVE abs() - keep sign for accurate display (positive=receiving, negative=paying)
            current_yield_bps = mtm.get("curr_diff_bps", 0.0)

            # FIX 2026-01-22: Include entry prices and current BBO for debugging
            # Parse direction to get venue names
            parts = pos.direction.split(" / ")
            leg1_venue = parts[0].split()[-1].upper() if len(parts) >= 1 else "?"
            leg2_venue = parts[1].split()[-1].upper() if len(parts) >= 2 else "?"

            # FIX 2026-01-22: Calculate fee breakdown for dashboard
            total_fees_paid = getattr(pos, 'total_fees_paid_usd', 0.0) or 0.0
            fee_operations = getattr(pos, 'fee_operations', None) or []

            # Count maker/taker operations
            maker_ops = sum(1 for op in fee_operations if op.get('v1_maker') or op.get('v2_maker'))
            taker_ops = len(fee_operations) * 2 - maker_ops  # Each op has 2 legs

            # Calculate projected exit fees (taker worst case)
            fee_v1 = self._venue_fees.get(leg1_venue, {}).get("taker", 4.5)
            fee_v2 = self._venue_fees.get(leg2_venue, {}).get("taker", 4.0)
            exit_fee_bps = fee_v1 + fee_v2
            exit_fee_projected = pos.size_usd * (exit_fee_bps / 10000.0)

            summary.append({
                "symbol": sym,
                "hl_coin": pos.hl_coin,
                "direction": pos.direction,
                "hold_hours": (now - pos.entry_time) / 3600.0,
                "size_usd": pos.size_usd,
                "size_v1_usd": getattr(pos, 'size_v1_usd', pos.size_usd / 2),
                "size_v2_usd": getattr(pos, 'size_v2_usd', pos.size_usd / 2),
                "mtm_bps": mtm["mtm_bps"],
                "pnl_usd": mtm["total_mtm_usd"],
                "funding_pnl_usd": mtm["funding_pnl_usd"],
                "price_pnl_usd": mtm["price_pnl_usd"],
                "total_mtm_usd": mtm["total_mtm_usd"],
                "fee_cost_usd": mtm.get("fee_cost_usd", 0.0),
                "entry_funding_diff_bps": pos.entry_diff_bps,
                "current_funding_diff_bps": current_yield_bps,
                "yield_is_receiving": current_yield_bps >= 0,  # FIX 2026-01-25: True if receiving funding
                "scale_up_count": pos.scale_up_count,
                # FIX 2026-01-22: Include prices for debugging
                "entry_px_hl": pos.entry_px_hl,
                "entry_px_as": pos.entry_px_as,
                "current_px_leg1": mtm.get("liq_px_leg1", 0.0),
                "current_px_leg2": mtm.get("liq_px_leg2", 0.0),
                "leg1_venue": leg1_venue,
                "leg2_venue": leg2_venue,
                # FIX 2026-01-22: Comprehensive fee tracking for dashboard
                "total_fees_paid_usd": total_fees_paid,
                "exit_fee_projected_usd": exit_fee_projected,
                "fee_operations": fee_operations,
                "entry_v1_maker": getattr(pos, 'entry_v1_maker', False),
                "entry_v2_maker": getattr(pos, 'entry_v2_maker', False),
                "maker_legs": maker_ops,
                "taker_legs": taker_ops,
            })
        return summary

    def get_stats(self) -> Dict[str, Any]:
        """Return overall strategy stats."""
        total_eq = self.total_paper_equity
        # Realized PnL = current paper_equity - starting equity
        # BUG FIX: Also track realized PnL in dry_run_live mode (was only tracking in paper_mode)
        # FIX: Read start_equity from correct location (risk.carry.paper_equity, not cfg.paper_equity)
        if self.paper_mode or self.dry_run_live:
            start_equity = float(self.risk.get("carry", {}).get("paper_equity", 5000.0))
            realized_pnl = self.paper_equity - start_equity
        else:
            start_equity = self._cached_total_equity
            realized_pnl = self._live_realized_pnl  # BUGFIX: Use accumulated live realized PnL
        # FIX 2026-01-24: Calculate session equity MTM
        session_equity_mtm = 0.0
        session_start_eq = self._session_start_equity
        if self._session_start_equity_captured and self._session_start_equity > 0:
            session_equity_mtm = self._cached_total_equity - self._session_start_equity

        return {
            "open_positions": len(self.positions),
            "paper_equity": self.paper_equity,
            "entries": self.total_entries,
            "exits": self.total_exits,
            "wins": self.wins,
            "losses": self.losses,
            "equity": total_eq,
            "enabled": self.enabled,
            "realized_pnl": realized_pnl,
            "start_equity": start_equity,
            # PHASE 2 FIX (2026-01-22): Add session tracking for dashboard
            "session_realized_pnl": self._session_realized_pnl,
            "unrealized_mtm": self.unrealized_mtm,
            # FIX 2026-01-24: Session equity tracking
            "session_start_equity": session_start_eq,
            "session_equity_mtm": session_equity_mtm
        }

    def generate_session_summary(self) -> Dict[str, Any]:
        """Generate comprehensive session summary for analysis.

        OPTIMIZATION 2026-01-24: Comprehensive session summary JSON.
        Contains PnL breakdown, execution quality metrics, position stats.

        Returns:
            dict: Full session summary suitable for JSON serialization
        """
        import datetime
        now = time.time()

        # Calculate session duration
        session_start_ts = getattr(self, '_session_start_ts', now)
        duration_hours = (now - session_start_ts) / 3600.0

        # Get current and start equity
        current_equity = self._cached_total_equity if self._cached_total_equity > 0 else self.total_paper_equity
        start_equity = self._session_start_equity if self._session_start_equity > 0 else current_equity

        # Calculate realized funding PnL from positions
        total_funding_pnl = sum(pos.accrued_funding_usd for pos in self.positions.values())
        total_realized_funding = sum(pos.realized_funding for pos in self.positions.values())

        # Entry success rate from attempts tracking
        entry_attempts = self._entry_attempts
        successful_entries = sum(1 for a in entry_attempts if a.get("v2_status") == "FILLED")
        failed_entries = sum(1 for a in entry_attempts if a.get("v1_status") == "FAILED" or a.get("v2_status") == "FAILED")
        total_attempts = successful_entries + failed_entries
        entry_success_rate = successful_entries / total_attempts if total_attempts > 0 else 1.0

        # Calculate total fees paid
        total_fees_paid = sum(pos.total_fees_paid_usd for pos in self.positions.values())

        # Fee savings from maker orders
        maker_v1_count = sum(1 for pos in self.positions.values() if pos.entry_v1_maker)
        maker_v2_count = sum(1 for pos in self.positions.values() if pos.entry_v2_maker)

        # Position stats
        position_sizes = [pos.size_usd for pos in self.positions.values()]
        avg_position_size = sum(position_sizes) / len(position_sizes) if position_sizes else 0
        max_concurrent = len(self.positions)  # Current count

        # Hold times
        hold_times = [(now - pos.entry_time) / 3600.0 for pos in self.positions.values()]
        avg_hold_hours = sum(hold_times) / len(hold_times) if hold_times else 0

        # Capital utilization
        total_position_value = sum(pos.size_usd for pos in self.positions.values())
        capital_utilization = total_position_value / current_equity if current_equity > 0 else 0

        # Top performers (by unrealized PnL)
        top_performers = []
        for sym, pos in self.positions.items():
            mtm = self._calc_mtm(pos)
            top_performers.append({
                "symbol": sym,
                "hl_coin": pos.hl_coin,
                "size_usd": pos.size_usd,
                "entry_yield_bps": pos.entry_diff_bps,
                "mtm_usd": mtm["total_mtm_usd"],
                "mtm_bps": mtm["mtm_bps"],
                "hold_hours": (now - pos.entry_time) / 3600.0,
                "funding_pnl": pos.accrued_funding_usd
            })
        top_performers.sort(key=lambda x: x["mtm_usd"], reverse=True)

        # Generate session ID
        session_id = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        summary = {
            "session_id": session_id,
            "generated_at": now,
            "duration_hours": round(duration_hours, 2),
            "start_equity": round(start_equity, 2),
            "end_equity": round(current_equity, 2),

            "pnl_breakdown": {
                "funding_pnl": round(total_funding_pnl + total_realized_funding, 2),
                "price_pnl": round(self._session_realized_pnl - (total_funding_pnl + total_realized_funding), 2),
                "fees_paid": round(-total_fees_paid, 2),
                "net_pnl": round(current_equity - start_equity, 2),
                "session_realized_pnl": round(self._session_realized_pnl, 2)
            },

            "execution_quality": {
                "entry_attempts": total_attempts,
                "entry_successes": successful_entries,
                "entry_failures": failed_entries,
                "entry_success_rate": round(entry_success_rate, 3),
                "maker_entries_v1": maker_v1_count,
                "maker_entries_v2": maker_v2_count,
                "total_fees_paid_usd": round(total_fees_paid, 2)
            },

            "position_stats": {
                "open_positions": len(self.positions),
                "total_entries": self.total_entries,
                "total_exits": self.total_exits,
                "wins": self.wins,
                "losses": self.losses,
                "win_rate": round(self.wins / (self.wins + self.losses), 3) if (self.wins + self.losses) > 0 else 0,
                "avg_position_size_usd": round(avg_position_size, 2),
                "max_concurrent_positions": max_concurrent,
                "avg_hold_hours": round(avg_hold_hours, 2),
                "capital_utilization_pct": round(capital_utilization, 4)
            },

            "top_performers": top_performers[:5],
            "worst_performers": list(reversed(top_performers))[:5] if top_performers else [],

            "blocklist_status": {
                "reversal_blocked": list(self._reversal_blocklist.keys()),
                "pnl_blocked": list(getattr(self, '_pnl_blocklist', {}).keys()),
                "config_blocked": list(getattr(self, '_blocked_bases', set()))
            }
        }

        return summary

    def save_session_summary(self, filepath: str = None) -> str:
        """Save session summary to JSON file.

        Args:
            filepath: Optional custom path. Defaults to logs/carry/session_summary.json

        Returns:
            str: Path to saved file
        """
        import json
        if filepath is None:
            filepath = "logs/carry/session_summary.json"

        summary = self.generate_session_summary()

        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2)
            log.info(f"[CARRY] Session summary saved to {filepath}")
            return filepath
        except Exception as e:
            log.error(f"[CARRY] Failed to save session summary: {e}")
            return ""

    def get_entry_attempts(self) -> List[Dict[str, Any]]:
        """Return entry attempts for dashboard display (failures, partial fills, orphans)."""
        result = []
        for attempt in self._entry_attempts:
            age_hours = (time.time() - attempt["ts"]) / 3600.0
            result.append({
                "timestamp": attempt["ts"],
                "symbol": attempt.get("symbol", "?"),
                "hl_coin": attempt.get("hl_coin", "?"),
                "v1": attempt.get("v1", "?"),
                "v2": attempt.get("v2", "?"),
                "v1_status": attempt.get("v1_status", "?"),
                "v2_status": attempt.get("v2_status", "?"),
                "orphan_created": attempt.get("orphan_created", False),
                "orphan_closed": attempt.get("orphan_closed", False),
                "reason": attempt.get("reason", "Unknown"),
                "age_hours": round(age_hours, 2)
            })
        return result

    def get_blocked_pairs(self) -> List[Dict[str, Any]]:
        """Return list of currently blocklisted pairs for dashboard display."""
        now = time.time()
        blocked = []
        for hl_coin, until_ts in self._reversal_blocklist.items():
            if until_ts > now:
                remaining_hours = (until_ts - now) / 3600.0
                blocked.append({
                    "symbol": hl_coin,
                    "reason": "Reversals",
                    "until_ts": until_ts,
                    "remaining_hours": remaining_hours
                })
        return blocked
