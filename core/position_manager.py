# -*- coding: utf-8 -*-
"""
Hedge Fund Grade Position Manager.
Centralizes monitoring of all open positions (HL + Aster + Lighter).
"""
import asyncio
import logging
import os
import time
from typing import Dict, List, Optional, Any, Tuple

from venues.hyperliquid import Hyperliquid
from venues.aster import Aster

class PositionManager:
    def __init__(self, hl: Hyperliquid, asr: Aster, risk_cfg: Dict[str, Any], symbol_map: Dict[str, str], live_mode: bool = False, lighter=None, lighter_map=None, strategy=None):
        self.hl = hl
        self.asr = asr
        self.lighter = lighter
        self.risk_cfg = risk_cfg
        self.symbol_map = symbol_map  # Aster Symbol -> HL Coin
        self.lighter_map = lighter_map or {} # Lighter Symbol -> HL Coin
        self.log = logging.getLogger("pos_mgr")
        self.live_mode = live_mode
        self.strategy = strategy  # Reference to PerpPerpArb for paper equity updates
        self.carry_strategy = None  # Will be set after CarryStrategy init - prevents orphan closure during entry

        # Check if scalp strategy is enabled (Position Manager is primarily for scalp management)
        self.scalp_enabled = bool(risk_cfg.get("scalp_enabled", False))
        if not self.scalp_enabled:
            self.log.info("[POS MGR] Scalp strategy DISABLED - Position Manager will only handle orphan detection")

        # Risk params - Load from scalp_management section if available
        mgr_cfg = self.risk_cfg.get("scalp_management", {})
        self.take_profit_pct = float(mgr_cfg.get("take_profit_pct", 0.15))
        self.stop_loss_bps = float(mgr_cfg.get("stop_loss_bps", 50.0))
        self.min_profit_bps = float(mgr_cfg.get("min_profit_bps", 15.0))
        self.max_hold_hours = float(mgr_cfg.get("max_hold_hours", 0.167))

        # State
        self.open_scalps: Dict[str, Dict[str, Any]] = {} # symbol -> {entry_time, entry_basis, ...}
        self.position_start_times: Dict[str, float] = {} # symbol -> tracking_start_time
        self.bad_symbols: Dict[str, float] = {} # symbol -> cooldown_expiry_ts (for stop-loss protection)

        # Log Rate Limiting: Prevent heartbeat pollution
        self._last_monitor_log: Dict[str, float] = {}  # symbol -> last_log_ts
        self._monitor_log_interval = 60.0  # Log each position at most every 60 seconds

        # Duplicate Close Prevention: Track in-flight close operations
        self._closing_in_progress: set = set()  # symbols currently being closed

        # Orphan close cooldown: prevent spam when close fails (e.g., below min notional)
        self._orphan_close_failures: Dict[str, Tuple[float, int]] = {}  # symbol -> (last_attempt_ts, failure_count)
        self._orphan_cooldown_seconds = 300.0  # 5 minute cooldown after failed close
        self._orphan_max_failures_before_alert = 3  # Alert after 3 consecutive failures

        # P1 FIX: Orphan grace period - wait before closing to allow stale data to refresh
        self._orphan_first_seen: Dict[str, float] = {}  # orphan_key -> first_seen_ts
        self._orphan_grace_period_seconds = 30.0  # Wait 30s before closing orphan

        # Notifier for alerts (will be set externally)
        self.notifier = None

        # Reset real_pnl.csv on startup (fresh session)
        self._reset_real_pnl_csv()

        # Clear startup mode confirmation
        mode_str = "LIVE" if self.live_mode else "PAPER"
        self.log.info(f"[POS MGR] Initialized in {mode_str} mode")

    def _reset_real_pnl_csv(self):
        """Reset real_pnl.csv with fresh header on startup (truncate history)."""
        fpath = "logs/scalp/real_pnl.csv"
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        
        header = (
            "exit_ts,symbol,duration_s,exit_reason,"
            "entry_basis_bps,exit_basis_bps,est_gross_pnl_bps,"
            "pnl_reel_usd,pnl_reel_bps,"
            "hl_entry_px,hl_exit_px,as_entry_px,as_exit_px,"
            "hl_size,as_size,data_source"
        )
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(header + "\n")
        self.log.info(f"[POS MGR] Reset {fpath} for new session")

    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Check if there's an open position for a symbol.
        Returns the position dict if exists, None otherwise.
        """
        return self.open_scalps.get(symbol)

    async def tick(self):
        """
        Main loop logic:
        1. Fetch positions from both venues.
        2. Match them (find paired positions).
        3. Check exit conditions (Convergence, Stop Loss, Time Limit).
        4. Execute exits if needed.

        NOTE: When scalp_enabled=False, skip scalp management but still detect orphans.
        """
        try:
            # Skip scalp-specific management when scalp is disabled
            # (Orphan detection still runs to catch unhedged positions)
            if not self.scalp_enabled and not self.open_scalps:
                # No scalp positions to manage and scalp disabled - quick return
                # Still check for orphans periodically (every 10th call)
                if not hasattr(self, '_orphan_check_counter'):
                    self._orphan_check_counter = 0
                self._orphan_check_counter += 1
                if self._orphan_check_counter < 10:
                    return
                self._orphan_check_counter = 0

            # 1. Fetch Real Positions
            # HL: list of dicts {coin, sizing, entryPx, ...}
            # Aster: list of dicts {symbol, positionAmt, entryPrice, ...}
            # We need to standardize.

            # NOTE: SDKs must support get_positions.
            # Assuming hl.get_positions() and asr.get_positions() exist and work.

            hl_pos_raw = await self._fetch_hl_positions()
            as_pos_raw = await self._fetch_as_positions()
            lt_pos_raw = await self._fetch_lt_positions() if self.lighter else []

            # 2. Match Pairs & Detect Orphans
            matches, orphans = self._match_positions(hl_pos_raw, as_pos_raw, lt_pos_raw)

            # 3. Manage Paired Positions (Convergence) - Skip if scalp disabled
            if self.scalp_enabled:
                for m in matches:
                    await self._manage_pair(m)

            # 4. Manage Orphan Positions with Grace Period
            now = time.time()
            current_orphan_keys = set()

            for o in orphans:
                venue = o['venue']
                sym = o.get('symbol') or o.get('coin')
                size = o['size']
                orphan_key = f"{venue}:{sym}"
                current_orphan_keys.add(orphan_key)

                # P4 FIX: Ask carry strategy to validate before closing
                if self.carry_strategy and hasattr(self.carry_strategy, 'validate_orphan'):
                    if not await self.carry_strategy.validate_orphan(sym, venue, size):
                        self.log.info(f"[ORPHAN] Carry strategy rejected orphan closure for {orphan_key}")
                        continue

                # P1 FIX: Track first-seen time and enforce grace period
                if orphan_key not in self._orphan_first_seen:
                    self._orphan_first_seen[orphan_key] = now
                    # P2 FIX: Enhanced logging with full context
                    self.log.warning(
                        f"[ORPHAN CANDIDATE] Detected on {venue}: {sym} size={size:.4f} | "
                        f"Grace period: {self._orphan_grace_period_seconds}s before close"
                    )
                    continue  # Don't close yet - wait for grace period

                first_seen = self._orphan_first_seen[orphan_key]
                elapsed = now - first_seen

                if elapsed < self._orphan_grace_period_seconds:
                    remaining = self._orphan_grace_period_seconds - elapsed
                    self.log.debug(f"[ORPHAN] {orphan_key} in grace period ({remaining:.1f}s remaining)")
                    continue  # Still in grace period

                # Grace period elapsed - proceed to close
                self.log.warning(
                    f"[ORPHAN] Closing {venue} orphan: {sym} size={size:.4f} | "
                    f"First seen {elapsed:.1f}s ago"
                )
                await self._close_orphan(o)

            # Clean up tracking for orphans no longer detected
            stale_keys = set(self._orphan_first_seen.keys()) - current_orphan_keys
            for key in stale_keys:
                del self._orphan_first_seen[key]
                self.log.debug(f"[ORPHAN] Cleared tracking for {key} - no longer detected")

        except Exception as e:
            self.log.error(f"Tick error: {e}")

    async def _fetch_hl_positions(self) -> List[Dict]:
        try:
            return await self.hl.get_positions() or []
        except Exception:
            return []

    async def _fetch_as_positions(self) -> List[Dict]:
        try:
            return await self.asr.get_positions() or []
        except Exception:
            return []

    async def _fetch_lt_positions(self) -> List[Dict]:
        try:
            return await self.lighter.get_positions() or []
        except Exception:
            return []
            
    async def _close_orphan(self, o: Dict):
        """
        Close a single orphan position.

        NOTE: We use reduce_only=True for all venues to prevent accidentally
        creating new positions. If the position was already closed or changed direction,
        the reduce_only error is expected and handled gracefully.

        FIX: Aster DOES support reduceOnly - the previous comment was incorrect.
        Using reduce_only=True allows closing positions below min notional ($5.0).
        """
        venue = o['venue']
        size = o['size']
        sym = o.get('symbol') or o.get('coin')
        orphan_key = f"{venue}:{sym}"

        # Check cooldown - skip if we recently failed to close this orphan
        now = time.time()
        if orphan_key in self._orphan_close_failures:
            last_attempt, failure_count = self._orphan_close_failures[orphan_key]
            if now - last_attempt < self._orphan_cooldown_seconds:
                # Still in cooldown - log at debug level to reduce spam
                remaining = (self._orphan_cooldown_seconds - (now - last_attempt)) / 60.0
                self.log.debug(f"[ORPHAN] {orphan_key} in cooldown ({remaining:.1f}m remaining, {failure_count} failures)")
                return

        try:
            success = False
            if venue == 'HYPERLIQUID':
                coin = o['symbol']
                side = "SELL" if size > 0 else "BUY"
                result = await self.hl.place_order(coin, side, abs(size), ioc=False, reduce_only=True, price=None)
                # FIX: Check for reduce_only error and handle gracefully
                if result.get("status") == "error" and "Reduce only" in str(result.get("reason", "")):
                    self.log.info(f"[ORPHAN FIX] HL {coin} position already closed or changed direction")
                    success = True  # Position already gone, consider success
                else:
                    self.log.info(f"[ORPHAN FIX] Closed HL orphan {coin} size={size}")
                    success = True

            elif venue == 'ASTER':
                sym = o['symbol']
                side = "SELL" if size > 0 else "BUY"
                # FIX: Aster DOES support reduce_only=True - this allows closing dust positions below min notional
                result = await self.asr.place_order(sym, side, abs(size), ioc=False, reduce_only=True, price=None)
                # Check for errors
                if result.get("code") and result.get("code") != 200:
                    error_msg = result.get("msg", str(result))
                    self.log.warning(f"[ORPHAN] AS {sym} close failed: {error_msg}")
                    await self._handle_orphan_failure(orphan_key, sym, venue, size, error_msg)
                else:
                    self.log.info(f"[ORPHAN FIX] Closed AS orphan {sym} size={size}")
                    success = True

            elif venue == 'LIGHTER':
                sym = o['symbol']
                side = "SELL" if size > 0 else "BUY"
                result = await self.lighter.place_order(sym, side, abs(size), ioc=False, reduce_only=True, price=None)
                # FIX: Check for reduce_only error and handle gracefully
                if result.get("status") == "error" and "reduce" in str(result.get("reason", "")).lower():
                    self.log.info(f"[ORPHAN FIX] LT {sym} position already closed or changed direction")
                    success = True
                else:
                    self.log.info(f"[ORPHAN FIX] Closed LT orphan {sym} size={size}")
                    success = True

            # Clear failure tracking on success
            if success and orphan_key in self._orphan_close_failures:
                del self._orphan_close_failures[orphan_key]

        except Exception as e:
            error_msg = str(e)
            self.log.error(f"Failed to close orphan {o}: {e}")
            await self._handle_orphan_failure(orphan_key, sym, venue, size, error_msg)

    async def _handle_orphan_failure(self, orphan_key: str, sym: str, venue: str, size: float, error_msg: str):
        """Track orphan close failures and alert after repeated failures."""
        now = time.time()

        # Update failure tracking
        if orphan_key in self._orphan_close_failures:
            _, failure_count = self._orphan_close_failures[orphan_key]
            failure_count += 1
        else:
            failure_count = 1
        self._orphan_close_failures[orphan_key] = (now, failure_count)

        # Alert after max failures
        if failure_count >= self._orphan_max_failures_before_alert:
            alert_msg = f"ORPHAN CLOSE FAILED: {venue} {sym} (size={size:.6f}) - {failure_count} attempts failed. Error: {error_msg}"
            self.log.error(f"[ORPHAN ALERT] {alert_msg}")

            # Send notification if notifier available
            if self.notifier:
                try:
                    await self.notifier.notify("Orphan Close Failed", alert_msg)
                except Exception as e:
                    self.log.warning(f"[ORPHAN] Failed to send alert: {e}")

            # Reset counter after alert (will alert again after another N failures)
            self._orphan_close_failures[orphan_key] = (now, 0)

    def _match_positions(self, hl_pos: List[Dict], as_pos: List[Dict], lt_pos: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Pair up Delta Neutral positions (Standardization to HL Coin as base).
        """
        matches = []
        orphans = []
        
        # 1. Create maps by common HL Coin
        hl_map = {p['symbol'].upper(): p for p in hl_pos if abs(float(p.get('size', 0))) > 0}
        
        # Aster Mapping: hl_coin -> pos
        as_hl_map = {}
        for p in as_pos:
            sz = float(p.get('size', 0))
            if abs(sz) <= 0: continue
            sym = p['symbol'].upper()
            hl_coin = self.symbol_map.get(sym)
            if hl_coin:
                as_hl_map[hl_coin] = p
            else:
                # Orphan (No mapping)
                orphans.append({'venue': 'ASTER', 'symbol': sym, 'size': sz, 'pos': p})

        # Lighter Mapping: hl_coin -> pos
        lt_hl_map = {}
        for p in lt_pos:
            sz = float(p.get('size', 0))
            if abs(sz) <= 0: continue
            sym = p['symbol'].upper()
            hl_coin = self.lighter_map.get(sym)
            if hl_coin:
                lt_hl_map[hl_coin] = p
            else:
                # Orphan (No mapping)
                orphans.append({'venue': 'LIGHTER', 'symbol': sym, 'size': sz, 'pos': p})

        # 2. Iterate HL coins to find matches
        all_coins = set(hl_map.keys()) | set(as_hl_map.keys()) | set(lt_hl_map.keys())
        
        for coin in all_coins:
            hp = hl_map.get(coin)
            ap = as_hl_map.get(coin)
            lp = lt_hl_map.get(coin)
            
            # A pair is valid if at least two legs exist for the same coin
            # But the logic currently assumes HL vs (AS or LT)
            
            # Case A: HL vs Aster
            if hp and ap:
                matches.append({
                    'symbol': ap['symbol'],
                    'hl_coin': coin,
                    'hl_size': float(hp['size']),
                    'as_size': float(ap['size']),
                    'hl_entry': float(hp.get('entry', 0)),
                    'as_entry': float(ap.get('entry', 0)),
                    'venue_pair': 'HL-AS'
                })
            # Case B: HL vs Lighter
            elif hp and lp:
                matches.append({
                    'symbol': lp['symbol'],
                    'hl_coin': coin,
                    'hl_size': float(hp['size']),
                    'lt_size': float(lp['size']),
                    'hl_entry': float(hp.get('entry', 0)),
                    'lt_entry': float(lp.get('entry', 0)),
                    'venue_pair': 'HL-LT'
                })
            # Case C: Aster vs Lighter
            elif ap and lp:
                 matches.append({
                    'symbol': f"{ap['symbol']}-{lp['symbol']}",
                    'hl_coin': coin,
                    'as_size': float(ap['size']),
                    'lt_size': float(lp['size']),
                    'as_entry': float(ap.get('entry', 0)),
                    'lt_entry': float(lp.get('entry', 0)),
                    'venue_pair': 'AS-LT'
                })
            # Case D: Orphans
            else:
                # CRITICAL: Check if carry strategy is currently placing a multi-leg entry
                # If so, skip orphan detection to prevent closing legs mid-entry
                if self.carry_strategy and hasattr(self.carry_strategy, 'is_entry_in_progress'):
                    if self.carry_strategy.is_entry_in_progress(coin):
                        self.log.debug(f"[POS MGR] Skipping orphan check for {coin} - carry entry in progress")
                        continue  # Skip this coin entirely

                # CRITICAL FIX: Check if this coin is part of a tracked carry position
                # Carry positions may have legs on different venue pairs (HL-AS, AS-LT, HL-LT)
                # so position_manager may see only one leg but it's NOT an orphan
                if self.carry_strategy and hasattr(self.carry_strategy, 'positions'):
                    is_tracked = any(
                        pos.hl_coin == coin
                        for pos in self.carry_strategy.positions.values()
                    )
                    if is_tracked:
                        self.log.debug(f"[POS MGR] Skipping orphan for {coin} - tracked by carry strategy")
                        continue  # Skip - this is a valid carry position leg

                if hp: orphans.append({'venue': 'HYPERLIQUID', 'symbol': coin, 'size': float(hp['size']), 'pos': hp})
                if ap: orphans.append({'venue': 'ASTER', 'symbol': ap['symbol'], 'size': float(ap['size']), 'pos': ap})
                if lp: orphans.append({'venue': 'LIGHTER', 'symbol': lp['symbol'], 'size': float(lp['size']), 'pos': lp})

        return matches, orphans

    async def _manage_pair(self, p: Dict):
        """
        Decide whether to close the pair.
        """
        sym = p['symbol']
        hl_coin = p['hl_coin']
        
        # MERGE: Get entry data from open_scalps (if available)
        # BUGFIX: Use hl_coin as key (consistent with strategy) instead of exchange-specific symbol
        scalp_data = self.open_scalps.get(hl_coin, {})
        if scalp_data:
            # Use our stored entry prices for accurate PnL calculation
            p['hl_entry'] = scalp_data.get('hl_entry', p.get('hl_entry', 0.0))
            p['as_entry'] = scalp_data.get('as_entry', p.get('as_entry', 0.0))
            p['entry_basis'] = scalp_data.get('entry_basis', 0.0)
            p['hl_oid'] = scalp_data.get('hl_oid')
            p['as_oid'] = scalp_data.get('as_oid')
        
        v_pair = p.get('venue_pair', 'HL-AS')
        
        # Calc Current Basis (Spread)
        # We need current prices.
        bbo_hl = self.hl.get_bbo(hl_coin)
        bbo_as = self.asr.get_bbo(sym)
        bbo_lt = self.lighter.get_bbo(sym) if self.lighter else None
        
        px1, px2 = 0.0, 0.0
        if v_pair == 'HL-AS':
            if not bbo_hl or not bbo_as: return
            px1 = (bbo_hl[0] + bbo_hl[1]) / 2
            px2 = (bbo_as[0] + bbo_as[1]) / 2
        elif v_pair == 'HL-LT':
            if not bbo_hl or not bbo_lt: return
            px1 = (bbo_hl[0] + bbo_hl[1]) / 2
            px2 = (bbo_lt[0] + bbo_lt[1]) / 2
        elif v_pair == 'AS-LT':
            if not bbo_as or not bbo_lt: return
            px1 = (bbo_as[0] + bbo_as[1]) / 2
            px2 = (bbo_lt[0] + bbo_lt[1]) / 2
        else:
            return

        # Current Spread (Basis) in BPS
        basis_bps = (px1 - px2) / px2 * 10000.0

        # Calc Imbalance and Direction
        # Standardization: s1 is the 'base' leg (usually HL if present, else AS)
        s1 = p.get('hl_size', p.get('as_size', 0.0))
        s2 = p.get('as_size', p.get('lt_size', 0.0)) if 'hl_size' in p else p.get('lt_size', 0.0)
        
        direction = "UNKNOWN"
        if s1 > 0 and s2 < 0:
            direction = f"LONG_{v_pair.split('-')[0]}_SHORT_{v_pair.split('-')[1]}"
        elif s1 < 0 and s2 > 0:
            direction = f"SHORT_{v_pair.split('-')[0]}_LONG_{v_pair.split('-')[1]}"
        # EXIT CONDITIONS (in priority order):
        # 1. Convergence: |basis| <= target_exit_bps (5 bps) -> PROFIT
        # 2. Stop-Loss: unrealized PnL < -stop_loss_bps (250 bps) -> CUT LOSS
        # 3. Time Limit: hold_time > max_hold_hours (24h) -> FORCE EXIT
        should_close = False
        reason = ""
        
        # Calculate hold time - use stored entry_time from open_scalps, or persist first detection
        scalp_entry_time = self.open_scalps.get(hl_coin, {}).get('entry_time')
        if scalp_entry_time:
            start_time = scalp_entry_time
            # Also store in position_start_times for consistency
            if sym not in self.position_start_times:
                self.position_start_times[sym] = start_time
        elif sym in self.position_start_times:
            start_time = self.position_start_times[sym]
        else:
            # First time seeing this position (e.g. after restart) - store now
            start_time = time.time()
            self.position_start_times[sym] = start_time
            self.log.warning(f"[POS MGR] First detection of {sym} - starting timeout counter NOW")
        
        hold_hours = (time.time() - start_time) / 3600.0
        
        # Calculate unrealized PnL in bps
        # Entry basis is stored in open_scalps or we estimate from entry prices
        # Standardize entry prices based on the pair
        ep1, ep2 = 0.0, 0.0
        if v_pair == 'HL-AS': ep1, ep2 = p.get('hl_entry', 0.0), p.get('as_entry', 0.0)
        elif v_pair == 'HL-LT': ep1, ep2 = p.get('hl_entry', 0.0), p.get('lt_entry', 0.0)
        elif v_pair == 'AS-LT': ep1, ep2 = p.get('as_entry', 0.0), p.get('lt_entry', 0.0)
        
        entry_basis = p.get('entry_basis', 0.0)
        if entry_basis == 0.0 and ep2 > 0:
            entry_basis = (ep1 - ep2) / ep2 * 10000.0
        
        # Unrealized PnL
        # Antigravity Fix: Use entry_mid_basis if available to avoid being stopped out by our own entry slippage
        pnl_baseline = scalp_data.get('entry_mid_basis', entry_basis)
        
        # 2. LIQUIDATED PNL CHECK (Hedge Fund Grade)
        # We calculate PnL based on the actual price we would get if we exited NOW
        # To exit Long V1: Sell at V1 Bid
        # To exit Short V2: Buy at V2 Ask
        if s1 > 0:  # Long V1 / Short V2
            liqd_px1 = bbo_hl[0] if v_pair.startswith('HL') else (bbo_as[0] if v_pair.startswith('AS') else 0)
            liqd_px2 = bbo_as[1] if v_pair.endswith('AS') else (bbo_lt[1] if v_pair.endswith('LT') else 0)
        else:  # Short V1 / Long V2
            liqd_px1 = bbo_hl[1] if v_pair.startswith('HL') else (bbo_as[1] if v_pair.startswith('AS') else 0)
            liqd_px2 = bbo_as[0] if v_pair.endswith('AS') else (bbo_lt[0] if v_pair.endswith('LT') else 0)

        if liqd_px1 <= 0 or liqd_px2 <= 0:
            return

        liqd_basis_bps = (liqd_px1 - liqd_px2) / liqd_px2 * 10000.0
        
        # Liquidated PnL (What we actually get after crossing spreads)
        if s1 > 0:
            liqd_unrealized_pnl_bps = liqd_basis_bps - pnl_baseline
        else:
            liqd_unrealized_pnl_bps = pnl_baseline - liqd_basis_bps
            
        # Add exit fees (approx 7bps total for two legs Taker)
        net_liqd_pnl_bps = liqd_unrealized_pnl_bps - 7.0

        # 1. TAKE PROFIT CHECK (Highest Priority - Capture % of entry spread)
        # Calculate profit captured as % of entry spread
        abs_entry_basis = abs(entry_basis)
        profit_captured_bps = net_liqd_pnl_bps
        profit_captured_pct = (profit_captured_bps / abs_entry_basis) if abs_entry_basis > 0 else 0.0
        
        if profit_captured_pct >= self.take_profit_pct and profit_captured_bps >= self.min_profit_bps:
            should_close = True
            reason = f"TAKE_PROFIT (Captured {profit_captured_pct*100:.1f}% >= {self.take_profit_pct*100:.0f}% | +{profit_captured_bps:.1f}bps net >= {self.min_profit_bps}bps)"
        
        # 1b. TRAILING STOP (Protect Profits)
        # Track max PnL seen and close if dropped >25% from peak
        max_pnl = self.open_scalps.get(hl_coin, {}).get('max_pnl_bps', 0.0)
        if net_liqd_pnl_bps > max_pnl:
            if hl_coin in self.open_scalps:
                self.open_scalps[hl_coin]['max_pnl_bps'] = net_liqd_pnl_bps
            max_pnl = net_liqd_pnl_bps
        
        if not should_close and max_pnl >= 10.0:  # Only if meaningful net profit
            drawdown_from_peak = max_pnl - net_liqd_pnl_bps
            if drawdown_from_peak >= max_pnl * 0.25:  # Dropped 25%+ from peak
                should_close = True
                reason = f"TRAILING_STOP (Peak={max_pnl:.1f}bps Now={net_liqd_pnl_bps:.1f}bps Giveback={drawdown_from_peak:.1f}bps)"
                self.log.warning(f"[TRAILING] {sym} profit giveback! Peak={max_pnl:.1f} Now={net_liqd_pnl_bps:.1f}")
        
        # 2. STOP-LOSS CHECK (Cut Loss)
        if not should_close and net_liqd_pnl_bps < -self.stop_loss_bps:
            should_close = True
            reason = f"STOP_LOSS (NetLiqPnL {net_liqd_pnl_bps:.1f}bps < -{self.stop_loss_bps})"
            self.log.warning(f"[STOP-LOSS] {sym} hit stop-loss! PnL={net_liqd_pnl_bps:.1f}bps")
        
        # 3. TIMEOUT CHECK (Force Exit)
        if not should_close and hold_hours >= self.max_hold_hours:
            should_close = True
            reason = f"TIMEOUT (Hold {hold_hours:.1f}h >= {self.max_hold_hours}h)"
            self.log.warning(f"[TIMEOUT] {sym} hit time limit! Hold={hold_hours:.1f}h")
                
        if should_close:
            self.log.info(f"[POS MGR] Closing {sym} | {reason} | HL Size: {p['hl_size']} | PnL: {net_liqd_pnl_bps:.1f}bps")
            await self._execute_close(p, reason, basis_bps)
        else:
            # Update tracking if valid match
            if sym not in self.position_start_times:
                self.position_start_times[sym] = time.time()
                
            # Log status periodically with convergence data (RATE LIMITED)
            now = time.time()
            last_log = self._last_monitor_log.get(sym, 0.0)
            if now - last_log >= self._monitor_log_interval:
                self._last_monitor_log[sym] = now
                target_captured = abs_entry_basis * self.take_profit_pct
                self.log.info(
                    f"[POS MGR] MONITORING {sym} | Entry={abs_entry_basis:.1f}bps Target={target_captured:.1f}bps (15%) | "
                    f"Dir={direction} | PnL={net_liqd_pnl_bps:.1f}bps ({profit_captured_pct*100:.0f}%) | Hold={hold_hours:.2f}h | HL_Size={p['hl_size']}"
                )

    async def _execute_close(self, p: Dict, reason: str, exit_basis: float):
        """
        Execute dual close (Market or Aggressive Limit) and LOG result.
        """
        hl_coin = p['hl_coin']
        as_sym = p['symbol']

        # DUPLICATE CLOSE GUARD: Prevent re-triggering close for the same position
        if as_sym in self._closing_in_progress:
            self.log.debug(f"[POS MGR] {as_sym} close already in progress, skipping duplicate")
            return
        self._closing_in_progress.add(as_sym)

        v_pair = p.get('venue_pair', 'HL-AS')
        hl_size = p.get('hl_size', 0.0)
        as_size = p.get('as_size', 0.0)
        lt_size = p.get('lt_size', 0.0)

        self.log.info(f"[POS MGR] EXECUTING CLOSE for {as_sym}. Reason: {reason} ...")

        # Penalize symbol if it hit stop loss (long cooldown to avoid revenge trading)
        if "STOP_LOSS" in reason:
            cooldown_duration = 1800 # 30 minutes
            self.bad_symbols[hl_coin] = time.time() + cooldown_duration
            self.log.warning(f"[POS MGR] Adding {hl_coin} to bad_symbols for {cooldown_duration}s due to Stop Loss")
        
        # Calculate Stats
        start_time = self.position_start_times.get(as_sym, time.time())
        duration = time.time() - start_time
        
        hl_entry = p.get('hl_entry', 0.0)
        as_entry = p.get('as_entry', 0.0)
        entry_basis = 0.0
        if as_entry > 0:
            entry_basis = (hl_entry - as_entry) / as_entry * 10000.0
            
        # PnL Estimation (Gross BPS captured)
        # BUGFIX: The logic was reversed for Scalp logic.
        # Long V1 / Short V2 (hl_size > 0) -> Profit if Basis (px1-px2) INCREASES (e.g. from -50 to 0)
        # Unrealized PnL = Basis - Entry Basis
        # Gross PnL from Exit = Basis - Entry Basis
        est_pnl_bps = 0.0
        if hl_size > 0:  # Long HL / Short AS
             est_pnl_bps = exit_basis - entry_basis
        else:  # Short HL / Long AS
             est_pnl_bps = entry_basis - exit_basis

        # Remove tracker
        if as_sym in self.position_start_times:
            del self.position_start_times[as_sym]

        # Close HL (Aggressive Limit / Reduce Only)
        side_hl = "SELL" if hl_size > 0 else "BUY"
        hl_oid = None
        if self.live_mode:
            try:
                # Add 2bps buffer to ensure fill while avoiding market order slippage
                bbo = self.hl.get_bbo(hl_coin)
                p_hl = None
                if bbo:
                    p_hl = bbo[0] * 0.9998 if side_hl == "SELL" else bbo[1] * 1.0002
                    p_hl = self.hl.round_price(hl_coin, p_hl)
                    
                res_hl = await self.hl.place_order(hl_coin, side_hl, abs(hl_size), ioc=True, reduce_only=True, price=p_hl)
                hl_oid = res_hl.get("oid")
                self.log.info(f"[POS MGR] HL Close Order Sent: {res_hl}")
            except Exception as e:
                self.log.error(f"[POS MGR] HL Close Failed: {e}")
        else:
            self.log.info(f"[POS MGR] [PAPER] HL Close Simulated: {hl_coin} {side_hl} {abs(hl_size)}")
            hl_oid = f"paper_close_hl_{int(time.time())}"

        # Close Aster (Aggressive Limit / Reduce Only)
        side_as = "SELL" if as_size > 0 else "BUY"
        as_oid = None
        if v_pair in ('HL-AS', 'AS-LT'):
            if self.live_mode:
                try:
                    bbo = self.asr.get_bbo(as_sym)
                    p_as = None
                    if bbo:
                        p_as = bbo[0] * 0.9998 if side_as == "SELL" else bbo[1] * 1.0002
                        p_as = self.asr.round_price(as_sym, p_as)
                    
                    res_as = await self.asr.place_order(as_sym, side_as, abs(as_size), ioc=False, reduce_only=True, price=p_as)
                    as_oid = res_as.get("orderId")
                    self.log.info(f"[POS MGR] AS Close Order Sent: {res_as}")
                except Exception as e:
                    self.log.error(f"[POS MGR] AS Close Failed: {e}")
            else:
                self.log.info(f"[POS MGR] [PAPER] AS Close Simulated: {as_sym} {side_as} {abs(as_size)}")
                as_oid = f"paper_close_as_{int(time.time())}"

        # Close Lighter (Aggressive Limit / Reduce Only)
        # lt_size already retrieved at line 428
        side_lt = "SELL" if lt_size > 0 else "BUY"
        lt_oid = None
        if v_pair in ('HL-LT', 'AS-LT') and self.lighter:
            if self.live_mode:
                try:
                    bbo = self.lighter.get_bbo(as_sym)
                    p_lt = None
                    if bbo:
                        p_lt = bbo[0] * 0.9998 if side_lt == "SELL" else bbo[1] * 1.0002
                        p_lt = self.lighter.round_price(as_sym, p_lt)
                        
                    res_lt = await self.lighter.place_order(as_sym, side_lt, abs(lt_size), ioc=True, reduce_only=True, price=p_lt)
                    lt_oid = res_lt.get("order_id")
                    self.log.info(f"[POS MGR] LT Close Order Sent: {res_lt}")
                except Exception as e:
                    self.log.error(f"[POS MGR] LT Close Failed: {e}")
            else:
                self.log.info(f"[POS MGR] [PAPER] LT Close Simulated: {as_sym} {side_lt} {abs(lt_size)}")
                lt_oid = f"paper_close_lt_{int(time.time())}"
        
        self.log.info(f"[POS MGR] Close Sequence Initiated for {as_sym}. Est PnL: {est_pnl_bps:.1f}bps. Duration: {duration:.1f}s")
        
        # Trigger Async Reconciliation (Real PnL) - ALWAYS, even without oids
        asyncio.create_task(self._reconcile_trade(
            symbol=as_sym, 
            hl_coin=hl_coin, 
            hl_oid=hl_oid, 
            as_oid=as_oid, 
            lt_oid=lt_oid,
            entry_hl=hl_entry, 
            entry_as=as_entry, 
            entry_lt=p.get('lt_entry', 0.0),
            size_hl=hl_size, 
            size_as=as_size,
            size_lt=lt_size,
            duration_s=duration,
            entry_basis_bps=entry_basis,
            exit_basis_bps=exit_basis,
            est_pnl_bps=est_pnl_bps,
            exit_reason=reason
        ))
        
        # CLEANUP: Remove from open_scalps to prevent stacking
        # BUGFIX: Consistent use of hl_coin as key
        if hl_coin in self.open_scalps:
            del self.open_scalps[hl_coin]
            self.log.info(f"[POS MGR] Removed {hl_coin} from open_scalps")
        
        # Cleanup other tracking
        if as_sym in self.position_start_times:
            del self.position_start_times[as_sym]
        if hl_coin in self.position_start_times:
            del self.position_start_times[hl_coin]

        # Remove from closing_in_progress (allow future closes for this symbol)
        self._closing_in_progress.discard(as_sym)

    async def _reconcile_trade(
        self, 
        symbol: str, 
        hl_coin: str, 
        hl_oid: Any, 
        as_oid: Any, 
        lt_oid: Any,
        entry_hl: float, 
        entry_as: float, 
        entry_lt: float,
        size_hl: float, 
        size_as: float,
        size_lt: float,
        duration_s: float,
        entry_basis_bps: float,
        exit_basis_bps: float,
        est_pnl_bps: float,
        exit_reason: str
    ):
        """
        Wait for fills and calculate REAL PnL.
        ALWAYS writes to unified real_pnl.csv, even if fill prices are unavailable.
        """
        await asyncio.sleep(5.0)  # Wait 5s for fills/propagation
        
        try:
            exit_hl = 0.0
            exit_as = 0.0
            data_source = "ESTIMATED"
            
            # Fetch HL Fill
            if hl_oid:
                try:
                    px = await self.hl.get_order_fill_price(hl_oid)
                    if px: exit_hl = px
                except Exception as e:
                    self.log.warning(f"[RECONCILIATION] Failed to get HL fill price: {e}")
            
            # Fetch Aster Fill
            if as_oid:
                try:
                    px = await self.asr.get_order_fill_price(symbol, as_oid)
                    if px: exit_as = px
                except Exception as e:
                    self.log.warning(f"[RECONCILIATION] Failed to get Aster fill price: {e}")

            # Fetch Lighter Fill
            exit_lt = 0.0
            if lt_oid and self.lighter:
                try:
                    px = await self.lighter.get_order_fill_price(symbol, lt_oid)
                    if px: exit_lt = px
                except Exception as e:
                    self.log.warning(f"[RECONCILIATION] Failed to get Lighter fill price: {e}")
            
            # Calculate Real PnL if we have both fills
            pnl_reel_usd = 0.0
            pnl_reel_bps = 0.0
            
            if exit_hl > 0 and exit_as > 0:
                data_source = "REAL"
                # PnL HL ($) = (Exit - Entry) * Size
                pnl_hl_usd = (exit_hl - entry_hl) * size_hl
                pnl_as_usd = (exit_as - entry_as) * size_as
                
                # ESTIMATE FEES (Approx 7bps per side = 14bps total roundtrip)
                notional = abs(entry_hl * size_hl) + abs(entry_as * size_as)
                est_fees_usd = notional * (14.0 / 10000.0)
                
                pnl_reel_usd = (pnl_hl_usd + pnl_as_usd) - est_fees_usd
                
                # BPS on Notional
                pnl_reel_bps = (pnl_reel_usd / notional * 10000.0) if notional > 0 else 0.0
                
                self.log.info(f"[RECONCILIATION] {symbol} REAL PNL: ${pnl_reel_usd:.2f} ({pnl_reel_bps:.1f} bps) [After ~14bps Fees]")
            else:
                # Use estimated values when fills not available
                self.log.warning(f"[RECONCILIATION] {symbol} Using ESTIMATED PnL (HL={exit_hl}, AS={exit_as})")
                
                # Subtract fees from estimated BPS
                pnl_reel_bps = est_pnl_bps - 14.0
                
                # Estimate USD from BPS
                notional = abs(entry_hl * size_hl) + abs(entry_as * size_as)
                pnl_reel_usd = (pnl_reel_bps / 10000.0) * notional if notional > 0 else 0.0
            
            # UNIFIED CSV - ALWAYS WRITE
            fpath = "logs/scalp/real_pnl.csv"
            header = (
                "exit_ts,symbol,duration_s,exit_reason,"
                "entry_basis_bps,exit_basis_bps,est_gross_pnl_bps,"
                "pnl_reel_usd,pnl_reel_bps,"
                "hl_entry_px,hl_exit_px,as_entry_px,as_exit_px,"
                "hl_size,as_size,data_source"
            )
            
            if not os.path.exists(fpath):
                with open(fpath, "w", encoding="utf-8") as f: 
                    f.write(header + "\n")
            
            row = (
                f"{time.time()},{symbol},{duration_s:.1f},{exit_reason},"
                f"{entry_basis_bps:.2f},{exit_basis_bps:.2f},{est_pnl_bps:.2f},"
                f"{pnl_reel_usd:.4f},{pnl_reel_bps:.2f},"
                f"{entry_hl:.6f},{exit_hl:.6f},{entry_as:.6f},{exit_as:.6f},"
                f"{size_hl:.6f},{size_as:.6f},{data_source}"
            )
            
            with open(fpath, "a", encoding="utf-8") as f:
                f.write(row + "\n")

            self.log.info(f"[RECONCILIATION] Logged to real_pnl.csv: {symbol} | ${pnl_reel_usd:.2f} | {data_source}")

            # Update paper equity in strategy (for paper mode compounding)
            if self.strategy and not self.live_mode:
                old_equity = self.strategy.paper_equity
                self.strategy.paper_equity += pnl_reel_usd
                self.log.info(f"[RECONCILIATION] Paper equity updated: ${old_equity:.2f} -> ${self.strategy.paper_equity:.2f} (P&L: ${pnl_reel_usd:.2f})")

        except Exception as e:
            self.log.error(f"[RECONCILIATION] Error: {e}")

