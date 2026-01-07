# -*- coding: utf-8 -*-
"""
Carry Strategy: Funding Rate Arbitrage between HL and Aster.

Unlike Scalp (entry + instant exit), Carry:
- Opens delta-neutral positions when funding is favorable
- Holds for 8h+ to collect funding payments
- Exits when funding becomes unfavorable or position ages out

Paper mode only for initial testing.
"""
from __future__ import annotations

import asyncio
import csv
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)


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
    entry_diff_bps: float = 0.0  # Funding diff at entry in bps
    entry_px_hl: float = 0.0     # Entry price on HL (for MTM)
    entry_px_as: float = 0.0     # Entry price on Aster (for MTM)
    funding_collected: float = 0.0
    realized_funding: float = 0.0 # Phase 2: True Realized Funding PnL from venue fills
    accrued_funding_usd: float = 0.0 # Cumulative funding earned in paper mode
    last_accrual_time: float = 0.0   # Last time we "locked in" funding profit
    max_mtm_bps: float = -999.0      # Highest MTM (Price+Funding) reached so far (trailing stop)
    scale_up_count: int = 0          # Number of scale-ups performed (max 3)


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
    ) -> None:
        self.hl = hl
        self.asr = asr
        self.lighter = lighter
        # Map: HL Coin -> Lighter Symbol
        self.hl_to_lighter = {v: k for k, v in (lighter_map or {}).items()}
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
        
        # Scaling (Renforcement)
        self.scale_up_slice_pct = float(risk.get("carry", {}).get("scale_up_slice_pct", 0.10))
        self.max_symbol_alloc_pct = float(risk.get("carry", {}).get("max_symbol_alloc_pct", 0.30))
        self.max_scale_ups_per_position = int(risk.get("carry", {}).get("max_scale_ups_per_position", 2))
        self.max_pairs_with_scaleup = int(risk.get("carry", {}).get("max_pairs_with_scaleup", 2))
        
        # MTM Thresholds (from config or tight defaults)
        self.mtm_stop_bps = -abs(float(risk.get("carry", {}).get("mtm_stop_bps", 35.0)))
        self.mtm_tp_bps = abs(float(risk.get("carry", {}).get("mtm_tp_bps", 25.0)))
        self.mtm_tp_pct = float(risk.get("carry", {}).get("mtm_tp_pct", 0.02))  # TP at 2% of size_usd
        self.mtm_grace_minutes = float(risk.get("carry", {}).get("mtm_grace_minutes", 15.0))
        self.mtm_yield_multiplier = float(risk.get("carry", {}).get("mtm_yield_multiplier", 2.0))
        self.mtm_check_interval = float(risk.get("carry", {}).get("mtm_check_interval_s", 60.0))
        
        # Trailing Stop Parameters
        self.mtm_trailing_bps = float(risk.get("carry", {}).get("mtm_trailing_bps", 150.0))
        self.mtm_trailing_min = float(risk.get("carry", {}).get("mtm_trailing_min_bps", 100.0))

        # Leverage Settings (CRITICAL for live trading)
        self.leverage = int(risk.get("carry", {}).get("leverage", 3))
        self._leverage_setup_done: Dict[str, bool] = {}  # Track which symbols have had leverage set

        # Liquidity-Aware Entry (Dynamic Threshold)
        self.min_vol_premium = float(risk.get("carry", {}).get("min_volume_premium_usd", 500000.0))
        self.premium_funding_bps = float(risk.get("carry", {}).get("premium_funding_bps", 120.0))

        self.last_run = 0
        # Funding check interval can be configured (default 30s when scalp disabled, 60s otherwise)
        self._funding_check_interval = float(risk.get("carry", {}).get("funding_check_interval_s", 30.0))
        self._last_funding_check = 0
        self._last_mtm_check = 0
        self._last_heartbeat = 0

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
        
        # Paper equity - load from storage if available, else from config (default 5000)
        config_equity = float(risk.get("carry", {}).get("paper_equity", 5000.0))
        self.paper_equity = self.storage.load_paper_equity(default=config_equity)
        if self.paper_equity != config_equity:
            log.info(f"[CARRY] Restored paper_equity: ${self.paper_equity:.2f} (config: ${config_equity:.2f})")

        # Dry-run simulated PnL (separate from paper_equity to avoid contamination)
        self._dry_run_realized_pnl = 0.0

        # Equity Cache to avoid redundant API calls
        self._cached_total_equity = 0.0
        self._last_equity_fetch = 0.0
        self._equity_cache_ttl = 60.0  # 60 seconds (reduced from 5min for more accurate allocations)

        # Rate Caching for API failure resilience
        self._cached_hl_rates: Dict[str, float] = {}
        self._cached_as_rates: Dict[str, float] = {}
        self._cached_lt_rates: Dict[str, float] = {}
        
        self.total_entries = 0
        self.total_exits = 0
        self.wins = 0
        self.losses = 0

        # Logging - Carry logs in dedicated subfolder
        self._csv_path = "logs/carry/carry_audit.csv"
        self._mtm_csv_path = "logs/carry/carry_mtm.csv"

        try:
            os.makedirs("logs/carry", exist_ok=True)
            if not os.path.exists(self._csv_path):
                with open(self._csv_path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow([
                        "ts", "symbol", "action", "direction", "size_usd",
                        "v1_rate", "v2_rate", "diff_bps",
                        "hold_hours", "price_pnl", "funding_pnl", "paper", "equity"
                    ])
            
            # SESSION SEPARATOR
            with open(self._csv_path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([]) # Blank line
                w.writerow([time.time(), "---", "SESSION_START", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---"])
                
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
        except Exception:
            pass

        # Load persisted positions
        self._load_state()

    @property
    def total_paper_equity(self) -> float:
        """Closed equity + Unrealized MTM of all open positions."""
        total = self.paper_equity
        for sym, pos in self.positions.items():
            mtm = self._calc_mtm(pos)
            total += mtm["total_mtm_usd"]
        return total

    def _load_state(self):
        """Load positions from disk."""
        raw_data = self.storage.load_positions()
        count = 0
        for sym, data in raw_data.items():
            try:
                # Reconstruct CarryPosition (handle missing fields for forward compatibility)
                pos = CarryPosition(
                    symbol=data["symbol"],
                    hl_coin=data["hl_coin"],
                    direction=data["direction"],
                    size_usd=float(data["size_usd"]),
                    entry_time=float(data["entry_time"]),
                    entry_funding_hl=float(data["entry_funding_hl"]),
                    entry_funding_as=float(data["entry_funding_as"]),
                    entry_diff_bps=float(data.get("entry_diff_bps", 0.0)),
                    entry_px_hl=float(data.get("entry_px_hl", 0.0)),
                    entry_px_as=float(data.get("entry_px_as", 0.0)),
                    funding_collected=float(data.get("funding_collected", 0.0)),
                    realized_funding=float(data.get("realized_funding", 0.0)),
                    accrued_funding_usd=float(data.get("accrued_funding_usd", 0.0)),
                    last_accrual_time=float(data.get("last_accrual_time", data.get("entry_time", 0.0))),
                    max_mtm_bps=float(data.get("max_mtm_bps", -999.0)),
                    scale_up_count=int(data.get("scale_up_count", 0))
                )
                self.positions[sym] = pos
                count += 1
            except Exception as e:
                log.error(f"[CARRY] Failed to load position {sym}: {e}")
        
        if count > 0:
            log.info(f"[CARRY] Restored {count} positions from storage.")

    def _save_state(self):
        """Save positions, paper_equity, and blocklist to disk."""
        if self.paper_mode:
            self.storage.save_state(
                self.positions,
                self.paper_equity,
                blocklist=self._reversal_blocklist,
                reversal_tracker=self._reversal_tracker
            )
        else:
            self.storage.save_state(
                self.positions,
                None,
                blocklist=self._reversal_blocklist,
                reversal_tracker=self._reversal_tracker
            )

    def update_mappings(self, pairs_map: Dict[str, str], lighter_map: Dict[str, str]) -> None:
        """Update symbol mappings dynamically."""
        self.pairs_map = pairs_map
        self.hl_to_lighter = {v: k for k, v in (lighter_map or {}).items()}

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
             equity: float = 0.0) -> None:
        """Log carry activity to CSV with explicit venue tracing."""
        try:
            # Convert decimal to bps (multiply by 10000)
            r1_bps = r1 * 10000
            r2_bps = r2 * 10000
            diff_bps = (r1 - r2) * 10000

            # Safety: Ensure equity is correctly set if not provided
            if equity <= 0:
                equity = self.total_paper_equity if self.paper_mode else self._cached_total_equity

            with open(self._csv_path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([
                    f"{time.time():.3f}", symbol, action, direction, f"{size_usd:.2f}",
                    f"{v1_name}:{r1_bps:.2f}", f"{v2_name}:{r2_bps:.2f}", f"{diff_bps:.2f}",
                    f"{hold_hours:.2f}", f"{price_pnl:.4f}", f"{funding_pnl:.4f}", self.paper_mode,
                    f"{equity:.2f}"
                ])
        except Exception as e:
            log.warning("[CARRY] Log error: %s", e)

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
            return {"price_pnl_usd": 0.0, "funding_pnl_usd": 0.0, "total_mtm_usd": 0.0, "mtm_bps": 0.0}
        
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
                        if lt_sym:
                            return self.lighter.get_bbo(lt_sym)
                return None
            except Exception as e:
                log.debug(f"[CARRY] BBO fetch error for {venue}: {e}")
                return None

        bbo_leg1 = get_bbo_for_venue(leg1_venue)
        bbo_leg2 = get_bbo_for_venue(leg2_venue)

        # Require valid entry prices
        if pos.entry_px_hl <= 0 or pos.entry_px_as <= 0:
            return {"price_pnl_usd": 0.0, "funding_pnl_usd": 0.0, "total_mtm_usd": 0.0, "mtm_bps": 0.0}

        # BUG FIX: Use entry prices as fallback when BBO unavailable (e.g., after restart)
        # This shows funding PnL even when price data is stale
        current_px_leg1 = (bbo_leg1[0] + bbo_leg1[1]) * 0.5 if bbo_leg1 else pos.entry_px_hl
        current_px_leg2 = (bbo_leg2[0] + bbo_leg2[1]) * 0.5 if bbo_leg2 else pos.entry_px_as
        
        # entry_px_hl = leg1 entry price, entry_px_as = leg2 entry price
        entry_px_leg1 = pos.entry_px_hl
        entry_px_leg2 = pos.entry_px_as
        
        # Calculate size in base units from USD (size_usd is total, each leg gets half)
        size_per_leg = pos.size_usd / 2
        size_base_leg1 = size_per_leg / entry_px_leg1
        size_base_leg2 = size_per_leg / entry_px_leg2
        
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
        
        # Funding PnL (estimated from hold time and current rates for a more realistic value)
        hold_hours = (time.time() - pos.entry_time) / 3600.0
        
        # Determine current diff bps for yield estimation
        parts = pos.direction.split(" / ")
        v1_name = parts[0].split()[-1].upper()
        v2_name = parts[1].split()[-1].upper()
        
        def get_rate_safe(venue: str, coin: str, symbol: str) -> float:
            try:
                rates = {}
                if venue == "HL": rates = self.hl._funding_rates
                elif venue == "AS": rates = self.asr._funding_rates if hasattr(self.asr, "_funding_rates") else {}
                elif venue == "LT": rates = self.lighter._funding_rates if self.lighter else {}
                
                # Correctly Map Symbol for Lighter
                lookup_sym = symbol
                if venue == "LT":
                    lookup_sym = self.hl_to_lighter.get(coin) or symbol

                rate = rates.get(coin if venue == "HL" else lookup_sym, 0.0)
                return rate * 8 if venue in ("HL", "LT") else rate
            except Exception: return 0.0

        curr_r1 = get_rate_safe(v1_name, pos.hl_coin, pos.symbol)
        curr_r2 = get_rate_safe(v2_name, pos.hl_coin, pos.symbol)
        curr_diff = (curr_r1 - curr_r2) * 10000
        
        # Funding PnL (Accrued + Current Pending)
        # Since we don't have historical snapshots, we accrue continuously in Paper Mode
        now = time.time()
        last_t = pos.last_accrual_time if pos.last_accrual_time > 0 else pos.entry_time
        hold_hours_since_accrual = (now - last_t) / 3600.0
        
        # Pending = current rate applied to the time since last lock
        pending_funding_usd = (abs(curr_diff) / 10000.0) * pos.size_usd * (hold_hours_since_accrual / 8.0)
        
        funding_pnl_usd = pos.accrued_funding_usd + pending_funding_usd
        
        # Total MTM
        total_mtm_usd = price_pnl_usd + funding_pnl_usd
        
        # Convert to bps
        # Use one-leg notional for bps calculation (matches basis move)
        notional = pos.size_usd
        mtm_bps = (total_mtm_usd / notional * 10000.0) if notional > 0 else 0.0
        
        return {
            "price_pnl_usd": price_pnl_usd,
            "funding_pnl_usd": funding_pnl_usd,
            "total_mtm_usd": total_mtm_usd,
            "mtm_bps": mtm_bps,
            "curr_diff_bps": curr_diff
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

        # 2. Check Entries
        if (now - self._last_funding_check) >= self._funding_check_interval:
            self._last_funding_check = now
            if hl_rates and as_rates:
                await self._check_entries(hl_rates, as_rates, lt_rates)

    async def _manage_positions(self, hl_rates: Dict, as_rates: Dict, lt_rates: Dict) -> None:
        """
        Unified position management.
        """
        if not self.positions:
            return

        # Phase 4.1: Detect and recover orphan positions (size_usd = 0)
        await self._check_orphans()

        now = time.time()
        to_close: List[Tuple[str, str, float, float]] = []

        # AUDIT FIX: Circuit breaker on stale data
        # For Carry we use a more permissive threshold (2000ms vs 500ms for Scalp)
        # because we're not doing instant arbitrage, just MTM monitoring
        MAX_CARRY_BBO_AGE_MS = 2000.0
        stale_venues: List[str] = []

        # Check HL freshness
        if hasattr(self.hl, 'get_bbo_age_ms'):
            # Check age for any position's coin
            for sym, pos in self.positions.items():
                hl_age = self.hl.get_bbo_age_ms(pos.hl_coin)
                if hl_age > MAX_CARRY_BBO_AGE_MS:
                    stale_venues.append(f"HL({pos.hl_coin}:{hl_age:.0f}ms)")
                    break

        # Check Aster freshness
        if hasattr(self.asr, 'get_bbo_age_ms'):
            for sym, pos in self.positions.items():
                as_age = self.asr.get_bbo_age_ms(pos.symbol)
                if as_age > MAX_CARRY_BBO_AGE_MS:
                    stale_venues.append(f"AS({pos.symbol}:{as_age:.0f}ms)")
                    break

        # Check Lighter freshness if used
        if self.lighter and hasattr(self.lighter, 'get_bbo_age_ms'):
            for sym, pos in self.positions.items():
                lt_sym = self.hl_to_lighter.get(pos.hl_coin)
                if lt_sym:
                    lt_age = self.lighter.get_bbo_age_ms(lt_sym)
                    if lt_age > MAX_CARRY_BBO_AGE_MS:
                        stale_venues.append(f"LT({lt_sym}:{lt_age:.0f}ms)")
                        break

        # If any venue has stale data, skip exit execution (but still log MTM)
        circuit_breaker_active = len(stale_venues) > 0
        if circuit_breaker_active:
            log.warning(f"[CARRY] CIRCUIT BREAKER: Stale data detected on {', '.join(stale_venues)}. Skipping exit execution this cycle.") 

        for sym, pos in self.positions.items():
            hold_hours = (now - pos.entry_time) / 3600.0
            
            # Pass rates if available for Funding PnL refinement (future)
            mtm = self._calc_mtm(pos, current_rates=None)
            
            # --- LOCK FUNDING ACCRUAL ---
            # Every MTM check, we "solidify" the earned funding to survive rate changes
            pos.accrued_funding_usd = mtm["funding_pnl_usd"]
            pos.last_accrual_time = now
            # No need to _save_state on every pair, we'll save once at the bottom if positions changed 
            # or just rely on the periodic mtm log/dashboard save

            # --- 1. Max Hold Time Exit ---
            if hold_hours >= self.hold_max_hours:
                to_close.append((sym, "max_hold_time", mtm["price_pnl_usd"], mtm["funding_pnl_usd"]))
                continue

            # Update Peak MTM for Trailing Stop
            current_mtm_bps = mtm["mtm_bps"]
            if current_mtm_bps > pos.max_mtm_bps:
                pos.max_mtm_bps = current_mtm_bps

            # --- 2. MTM Stop-Loss/Take-Profit ---
            # Get current yield, but protect against transient 0-rate reporting (API glitches)
            # We use the MAX of live yield and entry yield as our stability anchor
            curr_y = abs(mtm.get("curr_diff_bps", 0.0))
            anchor_yield = max(curr_y, abs(pos.entry_diff_bps))
            
            # Dynamic Stop = Base Floor (Negative) - (Anchor Yield * Multiplier)
            base_stop_floor = -self.mtm_stop_bps - (anchor_yield * self.mtm_yield_multiplier)
            
            # Trailing Profit Protection
            trailing_floor = -9999.0
            if pos.max_mtm_bps >= self.mtm_trailing_min:
                trailing_floor = pos.max_mtm_bps - self.mtm_trailing_bps
            
            # Final Floor is the tighter of base stop and trailing stop
            exit_floor = max(base_stop_floor, trailing_floor)

            # Apply Grace Period: Don't stop out in the first N minutes
            if hold_hours < (self.mtm_grace_minutes / 60.0):
                pass # Still in grace period
            elif current_mtm_bps < exit_floor:
                stop_type = "trailing_stop" if trailing_floor > base_stop_floor else "mtm_stop"
                to_close.append((sym, f"{stop_type}({current_mtm_bps:.1f} < {exit_floor:.1f}bps)", mtm["price_pnl_usd"], mtm["funding_pnl_usd"]))
                continue

            # Take Profit: % of size_usd (NEW - primary TP trigger)
            tp_usd_threshold = pos.size_usd * self.mtm_tp_pct
            if mtm["total_mtm_usd"] >= tp_usd_threshold:
                to_close.append((sym, f"mtm_tp_pct(${mtm['total_mtm_usd']:.2f}>=${tp_usd_threshold:.2f})", mtm["price_pnl_usd"], mtm["funding_pnl_usd"]))
                continue
            # Take Profit: Fixed bps (fallback for windfall scenarios)
            elif current_mtm_bps > self.mtm_tp_bps:
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
                    if venue == "HL": return (hl_rates.get(coin, 0.0) or 0.0) * 8
                    if venue == "AS": return as_rates.get(symbol, 0.0) or 0.0
                    if venue == "LT":
                        lt_sym = self.hl_to_lighter.get(coin)
                        return (lt_rates.get(lt_sym, 0.0) or 0.0) * 8
                    return 0.0

                fr1 = get_rate_norm(leg1_venue, pos.hl_coin, pos.symbol)
                fr2 = get_rate_norm(leg2_venue, pos.hl_coin, pos.symbol)

                is_long_leg1 = pos.direction.lower().startswith("long")
                curr_diff_bps = (fr1 - fr2) * 10000.0

                # EARLY REVERSAL: If funding has completely flipped (sign change), exit
                # This protects against paying funding instead of collecting it
                # BUG FIX: Added minimum hold time (10 min) to avoid exiting on noise
                early_reversal_min_hold_hours = 10.0 / 60.0  # 10 minutes
                entry_sign = 1 if pos.entry_diff_bps > 0 else -1
                current_sign = 1 if curr_diff_bps > 0 else -1
                if entry_sign != current_sign and abs(curr_diff_bps) > 5.0 and hold_hours >= early_reversal_min_hold_hours:
                    to_close.append((sym, f"early_reversal(sign_flip:{curr_diff_bps:.1f}bps)", mtm["price_pnl_usd"], mtm["funding_pnl_usd"]))
                    log.warning(f"[CARRY] EARLY REVERSAL {sym}: Entry={pos.entry_diff_bps:.1f}bps Now={curr_diff_bps:.1f}bps (sign flip after {hold_hours*60:.1f}min)")
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
                        continue

            # Periodic logging
            bbo_hl = self.hl.get_bbo(pos.hl_coin)
            parts = pos.direction.split(" / ")
            leg2_v = parts[1].split()[-1].upper()
            bbo_v2 = None
            if leg2_v == "AS": bbo_v2 = self.asr.get_bbo(pos.symbol)
            elif leg2_v == "LT": 
                lt_sym = self.hl_to_lighter.get(pos.hl_coin)
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
                    log.warning(f"[CARRY] Exit DEFERRED for {sym}: {reason} (stale data)")
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
            for sym, pos in self.positions.items():
                mtm = self._calc_mtm(pos)
                # Use accrued_funding as fallback for profitability if MTM calc returns 0
                # (happens when BBO unavailable)
                total_pnl = mtm["total_mtm_usd"]
                is_profitable = total_pnl > 0 or pos.accrued_funding_usd > 0.01  # Any meaningful accrued funding
                pos_data.append({
                    "sym": sym,
                    "pos": pos,
                    "yield_bps": abs(pos.entry_diff_bps),
                    "size_usd": pos.size_usd,
                    "scale_count": pos.scale_up_count,
                    "total_pnl": total_pnl,
                    "in_profit": is_profitable
                })
                log.debug(f"[CARRY] REBAL CHECK {pos.hl_coin}: yield={abs(pos.entry_diff_bps):.1f}bps, scale={pos.scale_up_count}, "
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
                    # Check if high_yield is significantly better than low_yield
                    # Rebalance if: high_yield > low_yield + buffer (meaningful improvement)
                    if high_yield["yield_bps"] <= low_yield["yield_bps"] + self.opt_buffer:
                        continue  # High-yield not significantly better than low-yield

                    # Found a rebalance opportunity!
                    log.info(f"[CARRY] YIELD REBALANCE [{rebalance_count+1}]: {high_yield['pos'].hl_coin} ({high_yield['yield_bps']:.1f}bps) "
                            f"<-- {low_yield['pos'].hl_coin} ({low_yield['yield_bps']:.1f}bps, scale={low_yield['scale_count']}, PnL: ${low_yield['total_pnl']:.2f})")

                    # Execute partial exit on low-yield position (scale down by one slice)
                    slice_size = low_yield["size_usd"] / (low_yield["scale_count"] + 1)  # Original slice size
                    success = await self._execute_partial_exit(low_yield["sym"], slice_size, "YIELD_REBALANCE")

                    if success:
                        rebalance_count += 1
                        found_opportunity = True

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

    async def _trigger_scale_up(self, pos: CarryPosition) -> bool:
        """
        Trigger a scale-up on a position after yield rebalancing freed capital.
        Simplified version that directly increases the position size.
        """
        if pos.scale_up_count >= self.max_scale_ups_per_position:
            return False

        total_equity = await self._get_total_equity()
        slice_size = total_equity * self.max_carry_alloc_pct * self.scale_up_slice_pct

        # Check max allocation
        max_sym_alloc = total_equity * self.max_symbol_alloc_pct
        if pos.size_usd + slice_size > max_sym_alloc:
            remaining = max_sym_alloc - pos.size_usd
            if remaining < 10.0:
                log.info(f"[CARRY] SCALE_UP SKIPPED {pos.hl_coin}: At max allocation")
                return False
            slice_size = remaining

        if self.paper_mode or self.dry_run_live:
            # Paper/Dry-run: Just increase position size
            pos.size_usd += slice_size
            pos.scale_up_count += 1
            self._save_state()

            log.info(f"[CARRY] SCALE_UP SUCCESS {pos.hl_coin}: New Size ${pos.size_usd:.2f} "
                    f"(scale-up #{pos.scale_up_count}/{self.max_scale_ups_per_position})")

            # Log to carry_audit.csv
            self._log(
                pos.symbol, "SCALE_UP_REBAL", pos.direction, slice_size,
                0.0, 0.0, "REBAL", "REBAL",
                0.0, 0.0, 0.0, self.paper_equity
            )
            return True

        # LIVE MODE would need actual order execution
        # For now, just log and skip
        log.warning(f"[CARRY] SCALE_UP in LIVE mode not implemented for yield rebalancing")
        return False

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

            # Track W/L for this partial exit
            self.total_exits += 1
            if partial_pnl > 0:
                self.wins += 1
            elif partial_pnl < 0:
                self.losses += 1

            # Log to carry_audit.csv
            self._log(
                sym, "SCALE_DOWN", pos.direction, size_to_reduce,
                0.0, 0.0, "REBAL", "REBAL",
                0.0, partial_pnl, 0.0, self.paper_equity
            )

            return True

        # LIVE MODE: Execute actual partial close orders
        # Parse direction to get venues
        parts = pos.direction.split(" / ")
        v1_name = parts[0].split()[-1].upper()
        v2_name = parts[1].split()[-1].upper()

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
                return self.hl_to_lighter.get(pos.hl_coin, pos.symbol)
            else:
                return pos.symbol

        s1 = _get_symbol_for_venue(v1_name)
        s2 = _get_symbol_for_venue(v2_name)

        # Calculate qty for partial close
        bbo1 = v1_obj.get_bbo(s1)
        current_px1 = ((bbo1[0] + bbo1[1]) / 2) if bbo1 else pos.entry_px_hl
        size_per_leg = size_to_reduce / 2
        qty = size_per_leg / current_px1

        # Execute partial closes
        # CRITICAL: Aster doesn't support reduce_only=True on exit orders
        try:
            use_reduce_only_v1 = v1_name != "AS"
            use_reduce_only_v2 = v2_name != "AS"
            res1 = await v1_obj.place_order(s1, side1, v1_obj.round_qty(s1, qty), ioc=True, reduce_only=use_reduce_only_v1)
            f1 = float(res1.get("filled", 0.0))

            res2 = await v2_obj.place_order(s2, side2, v2_obj.round_qty(s2, qty), ioc=True, reduce_only=use_reduce_only_v2)
            f2 = float(res2.get("filled", 0.0))

            if f1 > 0 and f2 > 0:
                pos.size_usd -= size_to_reduce
                pos.scale_up_count = max(0, pos.scale_up_count - 1)
                self._save_state()
                log.info(f"[CARRY] PARTIAL EXIT OK {sym}: New size ${pos.size_usd:.2f}")
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
            self._remove_position(sym, reason, pnl_price, pnl_funding, self.paper_equity)
            return True

        # --- LIVE/DRY-RUN EXIT EXECUTION ---
        parts = pos.direction.split(" / ")
        v1_name = parts[0].split()[-1].upper()
        v2_name = parts[1].split()[-1].upper()

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
        # - LT uses hl_to_lighter mapping (e.g., "BTC-PERP")
        def _get_symbol_for_venue(venue_name: str) -> str:
            if venue_name == "HL":
                return pos.hl_coin
            elif venue_name == "LT":
                return self.hl_to_lighter.get(pos.hl_coin, pos.symbol)
            else:  # AS
                return pos.symbol

        s1 = _get_symbol_for_venue(v1_name)
        s2 = _get_symbol_for_venue(v2_name)

        # FIX BUG #14: Use CURRENT price for qty calculation, not entry price
        # This ensures we close the correct notional amount even if price moved
        bbo1 = v1_obj.get_bbo(s1)
        bbo2 = v2_obj.get_bbo(s2)
        current_px1 = ((bbo1[0] + bbo1[1]) / 2) if bbo1 else pos.entry_px_hl
        current_px2 = ((bbo2[0] + bbo2[1]) / 2) if bbo2 else pos.entry_px_as
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

        # Retry logic for Leg 1 (3 attempts with backoff)
        f1 = 0.0
        for attempt in range(3):
            try:
                log.info(f"[CARRY] Closing Leg 1 {v1_name} {side1} (attempt {attempt+1}/3)...")
                # CRITICAL: Aster doesn't support reduce_only=True on exit orders (per CLAUDE.md)
                use_reduce_only = v1_name != "AS"
                res1 = await v1_obj.place_order(s1, side1, v1_obj.round_qty(s1, qty_v1),
                                                ioc=True, reduce_only=use_reduce_only)

                f1 = float(res1.get("filled", 0.0))
                if f1 > 0:
                    log.info(f"[CARRY] Leg 1 filled: {f1}")
                    break

                log.warning(f"[CARRY] Exit Leg 1 {v1_name} attempt {attempt+1} failed (0 fill). Status: {res1.get('status')}")

            except Exception as e:
                log.warning(f"[CARRY] Exit Leg 1 exception (attempt {attempt+1}): {e}")

            await asyncio.sleep(1.0 * (attempt + 1))  # Backoff: 1s, 2s, 3s

        if f1 <= 0:
            log.error(f"[CARRY] Exit Leg 1 {v1_name} failed after 3 attempts. Position remains open.")
            return False  # Do NOT delete position, return failure

        # Retry logic for Leg 2 (3 attempts with backoff)
        qty_v2_target = v2_obj.round_qty(s2, f1)
        f2 = 0.0
        for attempt in range(3):
            try:
                log.info(f"[CARRY] Closing Leg 2 {v2_name} {side2} (qty={qty_v2_target}, attempt {attempt+1}/3)...")
                # CRITICAL: Aster doesn't support reduce_only=True on exit orders (per CLAUDE.md)
                use_reduce_only_v2 = v2_name != "AS"
                res2 = await v2_obj.place_order(s2, side2, qty_v2_target,
                                                ioc=True, reduce_only=use_reduce_only_v2)

                f2 = float(res2.get("filled", 0.0))
                if f2 > 0:
                    log.info(f"[CARRY] Leg 2 filled: {f2}")
                    break

                log.warning(f"[CARRY] Exit Leg 2 {v2_name} attempt {attempt+1} failed (0 fill). Status: {res2.get('status')}")

            except Exception as e:
                log.warning(f"[CARRY] Exit Leg 2 exception (attempt {attempt+1}): {e}")

            await asyncio.sleep(1.0 * (attempt + 1))

        if f2 <= 0:
            # CRITICAL: Leg 1 closed but Leg 2 failed - orphan created!
            log.error(f"[CARRY] CRITICAL: Leg 2 {v2_name} failed but Leg 1 already closed. ORPHAN POSITION!")
            # Update position to reflect partial close (only hedge leg remains)
            # The position manager will detect this as orphan on next tick
            pos.size_usd = 0  # Mark as "closed on leg 1"
            self._save_state()
            return False  # Orphan created, exit failed

        # CONFIRMED CLOSE - Both legs exited successfully
        self._remove_position(sym, reason, pnl_price, pnl_funding, self._cached_total_equity)
        return True  # Exit successful

    def _remove_position(self, sym: str, reason: str, pnl_price: float, pnl_funding: float, equity: float):
        """Cleanly remove position and save state."""
        if sym in self.positions:
            pos = self.positions[sym]

            parts = pos.direction.split(" / ")
            v1_n = parts[0].split()[-1].upper()
            v2_n = parts[1].split()[-1].upper()

            self._log(sym, f"EXIT_{reason.split('(')[0].upper()}", pos.direction, pos.size_usd,
                      0.0, 0.0, v1_n, v2_n,
                      hold_hours=(time.time()-pos.entry_time)/3600,
                      price_pnl=pnl_price,
                      funding_pnl=pnl_funding,
                      equity=equity)

            # Track W/L stats
            total_pnl = pnl_price + pnl_funding
            self.total_exits += 1
            if total_pnl > 0:
                self.wins += 1
            elif total_pnl < 0:
                self.losses += 1

            del self.positions[sym]
            self._save_state()

    async def _check_orphans(self) -> None:
        """
        Phase 4.1: Detect orphan positions (size_usd=0) and attempt recovery.
        Orphans occur when Leg 1 exit succeeds but Leg 2 fails.
        """
        orphans = [sym for sym, pos in self.positions.items() if pos.size_usd <= 0]

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
                s2 = self.hl_to_lighter.get(pos.hl_coin)

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

                if self.dry_run_live:
                    log.info(f"[CARRY DRY-RUN] WOULD CLOSE ORPHAN: {v2_name} {s2} {close_side} qty={qty:.6f}")
                else:
                    log.info(f"[CARRY] Closing orphan leg: {v2_name} {s2} {close_side} qty={qty:.6f}")
                    # CRITICAL: Aster doesn't support reduce_only=True
                    use_reduce_only = v2_name != "AS"
                    res = await v2_obj.place_order(s2, close_side, qty, ioc=True, reduce_only=use_reduce_only)
                    filled = float(res.get("filled", 0.0))

                    if filled > 0:
                        log.info(f"[CARRY] Orphan {sym} recovered! Filled: {filled}")
                    else:
                        log.warning(f"[CARRY] Orphan {sym} recovery failed: {res.get('status')}")
                        continue  # Don't delete, retry next tick

                # Remove orphan from tracking
                del self.positions[sym]
                self._save_state()
                log.info(f"[CARRY] Orphan {sym} removed from tracking")

            except Exception as e:
                log.error(f"[CARRY] Orphan recovery error for {sym}: {e}")

    def _log_heartbeat(self):
        """Phase 3: Log operational heartbeat stats."""
        try:
            total_exp = sum(p.size_usd for p in self.positions.values())
            log.info(f"[HEARTBEAT] Positions: {len(self.positions)} | Exp: ${total_exp:.0f} | Equity: ${self._cached_total_equity:.0f}")
        except Exception: 
            pass

    async def _check_entries(self, hl_rates: Dict[str, float], as_rates: Dict[str, float], lt_rates: Dict[str, float] = None) -> None:
        """Look for new carry opportunities with fee accounting."""
        lt_rates = lt_rates or {}
        now = time.time()
        
        # 1. Allocation & Exposure Check
        current_exposure = sum(p.size_usd for p in self.positions.values())
        total_equity = await self._get_total_equity()
        max_notional = total_equity * self.max_carry_alloc_pct
        at_max_alloc = current_exposure >= max_notional
        
        opportunities: List[Tuple[str, str, str, str, float, float, float, Tuple[str, str]]] = []
        
        # Fees Configuration (BPS per leg roundtrip)
        # Entry/Exit for 2 legs = ~15 bps total
        EST_FEES_BPS = 15.0

        for as_sym, hl_coin in self.pairs_map.items():
            # Check auto-blocklist (too many reversals)
            if self._is_blocklisted(hl_coin):
                log.debug(f"[CARRY] Skipping {hl_coin} (AS: {as_sym}): BLOCKLISTED")
                continue

            # Check allocation for this specific symbol
            pos = self.positions.get(as_sym)
            if pos and pos.size_usd >= (total_equity * self.max_symbol_alloc_pct):
                continue

            # If several AS symbols map to same HL coin, check that too
            if not pos and any(p.hl_coin == hl_coin for p in self.positions.values()):
                continue
            
            # --- Gather Rates ---
            r_hl = (hl_rates.get(hl_coin) or 0.0) * 8.0
            r_as = as_rates.get(as_sym)
            lt_sym = self.hl_to_lighter.get(hl_coin)
            r_lt = (lt_rates.get(lt_sym) or 0.0) * 8.0 if lt_sym and lt_rates else None

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
                    
                    # --- Net Yield Check (Fees + Slippage) ---
                    # Get BBOs for slippage checking
                    v_map = {"HL": self.hl, "AS": self.asr, "LT": self.lighter}
                    b1 = v_map[v1_n].get_bbo(s1)
                    b2 = v_map[v2_n].get_bbo(s2)
                    
                    if not b1 or not b2 or (b1[0]+b1[1])<=0 or (b2[0]+b2[1])<=0: continue
                    
                    slip = self._slip_bps(b1, b2)
                    net_yield = abs_diff_bps - (slip + self.slip_buffer + EST_FEES_BPS)
                
                    # --- Dynamic Entry Threshold (Liquidity-Aware) ---
                    # Retrieve 24h volume for the pair (use HL volume as proxy)
                    hl_vol = 0.0
                    try:
                        # Access the hl venue's cached volume from metaAndAssetCtxs
                        hl_vol = getattr(self.hl, "_cached_vols", {}).get(hl_coin, 0.0)
                    except Exception: pass

                    # Determine effective threshold: 120bps if < 500k vol, else 50bps
                    # CRITICAL FIX: For SCALE-UPS (pos exists), use lower threshold since we're already committed
                    effective_threshold = self.min_funding_bps
                    is_premium_tier = hl_vol < self.min_vol_premium
                    if is_premium_tier:
                        effective_threshold = self.premium_funding_bps

                    # SCALE-UP THRESHOLD REDUCTION: If we already have a position with good entry yield,
                    # allow scale-ups at 50% of normal threshold (we're already in the trade)
                    pair_key = f"{hl_coin}_{v1_n}_{v2_n}"  # Define early for all code paths
                    is_scale_up = pos is not None and pos.scale_up_count < self.max_scale_ups_per_position
                    if is_scale_up:
                        # For scale-ups: require only that current net_yield > 0 (still profitable after fees)
                        # AND the position's entry yield was good enough when we entered
                        scale_up_threshold = max(0.0, self.min_funding_bps * 0.5)  # 25 bps minimum for scale-ups
                        if net_yield >= scale_up_threshold:
                            log.info(f"[CARRY] SCALE-UP QUALIFIED {hl_coin}: net_yield={net_yield:.1f}bps >= scale_up_threshold={scale_up_threshold:.1f}bps")
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
                    opportunities.append((hl_coin, s1, s2, direction, net_yield, r1, r2, (v1_n, v2_n)))

        # 2. Yield Optimization or Multi-Entry
        if not opportunities: return
        opportunities.sort(key=lambda x: x[4], reverse=True)

        # Multi-Entry: Enter on ALL valid opportunities (different coins), not just the best one
        # Track which HL coins we've already entered on this tick to avoid duplicates
        entered_coins: set = set()

        for opp in opportunities:
            hl_coin, s1, s2, direction, net_yield, r1, r2, (v1_n, v2_n) = opp

            # Skip if we already entered on this coin this tick
            if hl_coin in entered_coins:
                continue

            # FIX: Block NEW entry if we already have a position on the same underlying (hl_coin)
            # This prevents ZK + ZKUSDT double exposure (both would be Long/Short on same underlying)
            # BUT: Allow SCALE-UP if the existing position is on the SAME symbol (same venue pair)
            target_sym_check = s2 if v1_n == "HL" else s1  # Same logic as in _execute_smart_entry
            existing_pos_for_sym = self.positions.get(target_sym_check)

            if not existing_pos_for_sym:
                # NEW ENTRY: No limit on concurrent positions, but check duplicate underlying
                already_has_underlying = any(p.hl_coin == hl_coin for p in self.positions.values())
                if already_has_underlying:
                    log.debug(f"[CARRY] Skip NEW entry {hl_coin}: already have position on same underlying via different symbol")
                    continue
            else:
                # SCALE-UP: Check max scale-ups per position limit (max 2 scale-ups = 3 total entries)
                # DEBUG: Log scale_up_count to diagnose bypass issue
                log.info(f"[CARRY] SCALE-UP CHECK {hl_coin}: scale_up_count={existing_pos_for_sym.scale_up_count}, max={self.max_scale_ups_per_position}, size=${existing_pos_for_sym.size_usd:.2f}")
                if existing_pos_for_sym.scale_up_count >= self.max_scale_ups_per_position:
                    log.warning(f"[CARRY] BLOCKED SCALE-UP {hl_coin}: max scale-ups reached ({existing_pos_for_sym.scale_up_count}/{self.max_scale_ups_per_position})")
                    continue

                # ABSOLUTE SIZE BLOCK: Regardless of scale_up_count, block if size >= 3x initial slice
                # This catches any bugs where scale_up_count doesn't increment properly
                initial_slice = total_equity * self.max_carry_alloc_pct * self.alloc_pct
                max_position_size = initial_slice * 3.0  # Entry + 2 scale-ups = 3x
                if existing_pos_for_sym.size_usd >= max_position_size * 0.95:  # 95% tolerance
                    log.warning(f"[CARRY] SIZE BLOCKED SCALE-UP {hl_coin}: size ${existing_pos_for_sym.size_usd:.2f} >= max ${max_position_size:.2f}")
                    continue

                # SCALE-UP: Check max pairs that can scale-up (only 2 pairs can have scale-ups)
                pairs_with_scaleups = sum(1 for p in self.positions.values() if p.scale_up_count > 0)
                if existing_pos_for_sym.scale_up_count == 0 and pairs_with_scaleups >= self.max_pairs_with_scaleup:
                    log.debug(f"[CARRY] Skip SCALE-UP {hl_coin}: max pairs with scale-ups reached ({pairs_with_scaleups}/{self.max_pairs_with_scaleup})")
                    continue

            # Re-check allocation limits (may have changed after previous entry)
            current_exposure = sum(p.size_usd for p in self.positions.values())
            max_notional = total_equity * self.max_carry_alloc_pct
            at_max_alloc = current_exposure >= max_notional

            if at_max_alloc:
                # Check if any current position is much worse
                worst_pos_sym = None
                worst_yield = float('inf')
                for sym, pos in self.positions.items():
                    curr_y = abs(pos.entry_diff_bps) # Simplified for optimization check
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
            await self._execute_smart_entry(hl_coin, s1, s2, direction, net_yield, r1, r2, (v1_n, v2_n), opportunities)
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
                                   all_opportunities: List = None) -> None:
        """
        handle atomic entry with rollback.
        Phase 2: Use Maker orders where possible for the first leg (smart routing).
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
                    opp_hl_coin, _, _, _, opp_yield, _, _, _ = opp
                    # Skip self
                    if opp_hl_coin == hl_coin:
                        continue
                    # Check if this opportunity has no position and better yield
                    has_pos = any(p.hl_coin == opp_hl_coin for p in self.positions.values())
                    if not has_pos and opp_yield > diff_bps + self.opt_buffer:
                        log.info(f"[CARRY] SCALE_UP SKIPPED {hl_coin} ({diff_bps:.1f}bps): "
                                f"Better opportunity {opp_hl_coin} ({opp_yield:.1f}bps) has no position")
                        return

            size_usd = total_equity * self.max_carry_alloc_pct * self.scale_up_slice_pct
            action_name = "SCALE_UP"

            # Phase 4.3 Fix: Check that scale-up doesn't exceed max_symbol_alloc_pct
            max_sym_alloc = total_equity * self.max_symbol_alloc_pct
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

        log.info(f"[CARRY] {action_name} {hl_coin}: {direction}. Size: ${size_usd:.2f}")
        
        prices = {"v1": 0.0, "v2": 0.0}
        
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
            size_per_leg = size_usd / 2
            qty1 = v1_obj.round_qty(s1, size_per_leg / p1)
            qty2 = v2_obj.round_qty(s2, size_per_leg / p2)

            log.info(f"[CARRY DRY-RUN] WOULD PLACE ENTRY:")
            log.info(f"  Leg 1: {v1_n} {s1} {side1} qty={qty1:.6f} @ ${p1:.4f}")
            log.info(f"  Leg 2: {v2_n} {s2} {side2} qty={qty2:.6f} @ ${p2:.4f}")
            log.info(f"  Total notional: ${size_usd:.2f} | Direction: {direction}")

            prices["v1"] = p1
            prices["v2"] = p2
        else:
            # LIVE SMART ENTRY / SCALE UP WITH RETRY

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

            # Leg 1 Entry with retry (2 attempts)
            f1 = 0.0
            p1 = 0.0
            for attempt in range(2):
                try:
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
                await asyncio.sleep(0.5)

            if f1 <= 0:
                log.warning(f"[CARRY] Entry/Scale Leg 1 failed for {hl_coin} after 2 attempts")
                return

            prices["v1"] = p1

            # Leg 2 Entry with retry (2 attempts)
            f2 = 0.0
            p2 = 0.0
            qty2 = v2_obj.round_qty(s2, f1)

            for attempt in range(2):
                try:
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
                await asyncio.sleep(0.5)

            if f2 <= 0:
                # ATOMIC ROLLBACK
                log.error(f"[CARRY] CRITICAL: Leg 2 {s2} failed after 2 attempts. ROLLING BACK Leg 1.")
                inv_side = "SELL" if side1 == "BUY" else "BUY"
                # CRITICAL: Aster doesn't support reduce_only=True
                use_reduce_only_rb = v1_n != "AS"
                for rb_attempt in range(3):
                    try:
                        rb_res = await v1_obj.place_order(s1, inv_side, v1_obj.round_qty(s1, f1), ioc=True, reduce_only=use_reduce_only_rb)
                        if float(rb_res.get("filled", 0.0)) > 0:
                            log.info(f"[CARRY] Rollback successful")
                            break
                    except Exception as e:
                        log.error(f"[CARRY] Rollback attempt {rb_attempt+1} failed: {e}")
                    await asyncio.sleep(1.0)
                return  # Abort Entry

            prices["v2"] = p2

        # Update or Create Position
        if existing_pos:
            old_size = existing_pos.size_usd
            new_size = size_usd
            total_size = old_size + new_size

            px1 = prices["v1"] if v1_n == "HL" else prices["v2"]
            px2 = prices["v2"] if v1_n == "HL" else prices["v1"]

            # Weighted Averages
            existing_pos.entry_px_hl = (old_size * existing_pos.entry_px_hl + new_size * px1) / total_size
            existing_pos.entry_px_as = (old_size * existing_pos.entry_px_as + new_size * px2) / total_size
            existing_pos.entry_diff_bps = (old_size * existing_pos.entry_diff_bps + new_size * diff_bps) / total_size
            existing_pos.size_usd = total_size
            existing_pos.scale_up_count += 1  # Increment scale-up counter

            pairs_scaled = sum(1 for p in self.positions.values() if p.scale_up_count > 0)
            log.info(f"[CARRY] SCALE_UP SUCCESS {hl_coin}: New Size ${total_size:.2f} Avg Yield: {existing_pos.entry_diff_bps:.1f}bps (scale-up #{existing_pos.scale_up_count}/{self.max_scale_ups_per_position}, pairs scaled: {pairs_scaled}/{self.max_pairs_with_scaleup})")
        else:
            new_pos = CarryPosition(
                symbol=target_sym,
                hl_coin=hl_coin,
                direction=direction,
                size_usd=size_usd,
                entry_time=time.time(),
                entry_funding_hl=r1 if v1_n == "HL" else r2,
                entry_funding_as=r2 if v1_n == "HL" else r1,
                entry_px_hl=prices["v1"] if v1_n == "HL" else prices["v2"],
                entry_px_as=prices["v2"] if v1_n == "HL" else prices["v1"],
                entry_diff_bps=diff_bps,
                realized_funding=0.0,
                accrued_funding_usd=0.0,
                last_accrual_time=time.time()
            )
            self.positions[new_pos.symbol] = new_pos
            self.total_entries += 1
        
        self._save_state() # PERSIST IMMEDIATELY
        self._log(target_sym, action_name, direction, size_usd, r1, r2, v1_n, v2_n,
                  equity=(self.total_paper_equity if self.paper_mode else total_equity))

    def _cleanup_positions(self, symbols: List[str]):
        """Remove positions from the active tracking."""
        for sym in symbols:
            if sym in self.positions:
                del self.positions[sym]
        self._save_state()

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
        for sym, pos in self.positions.items():
            mtm = self._calc_mtm(pos)
            summary.append({
                "symbol": sym,
                "hl_coin": pos.hl_coin,
                "direction": pos.direction,
                "hold_hours": (now - pos.entry_time) / 3600.0,
                "size_usd": pos.size_usd,
                "mtm_bps": mtm["mtm_bps"],
                "pnl_usd": mtm["total_mtm_usd"],
                "funding_pnl_usd": mtm["funding_pnl_usd"],
                "price_pnl_usd": mtm["price_pnl_usd"],
                "total_mtm_usd": mtm["total_mtm_usd"],
                "entry_funding_diff_bps": pos.entry_diff_bps,
                "scale_up_count": pos.scale_up_count
            })
        return summary

    def get_stats(self) -> Dict[str, Any]:
        """Return overall strategy stats."""
        total_eq = self.total_paper_equity
        # Realized PnL = current paper_equity - starting equity
        # BUG FIX: Also track realized PnL in dry_run_live mode (was only tracking in paper_mode)
        if self.paper_mode or self.dry_run_live:
            start_equity = self.cfg.get("paper_equity", 1000.0)
            realized_pnl = self.paper_equity - start_equity
        else:
            start_equity = self._cached_total_equity
            realized_pnl = 0.0  # Live mode: real PnL tracked by venues
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
            "start_equity": start_equity
        }

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
