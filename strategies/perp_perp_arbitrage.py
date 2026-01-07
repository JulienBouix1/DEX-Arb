# -*- coding: utf-8 -*-
"""Perp-Perp arbitrage: scalp HL <-> Aster. Exécution symétrique, fermeture immédiate, no-carry."""
from __future__ import annotations

import asyncio
import time
import csv
import os
import logging
from dataclasses import dataclass
from typing import Dict, Tuple, Optional, Any, List, Deque
from collections import deque

log = logging.getLogger(__name__)

from risk.dyn_allocator import compute_notional
from core.arb_patch.executor import dual_ioc


def _bps(x: float) -> float:
    return float(x) * 10000.0


class KillSwitch(Exception):
    pass


@dataclass
class ArbExecResult:
    symbol: str
    direction: str
    qty: float
    net_mid_bps: float
    slip_bps: float
    net_adj_bps: float
    filled_hl: float
    filled_as: float
    hl_ok: bool
    as_ok: bool
    hl_close_ok: bool
    as_close_ok: bool
    hl_reason: str
    as_reason: str
    pnl_usd: float
    dry_run: bool
    v1_label: str = "HL"
    v2_label: str = "AS"


class PerpPerpArb:
    """
    Stratégie de scalp pur:

    - Découverte / mapping des paires = runner.
    - Ici:
        * calcul de l'edge net_mid_bps (fees incluses) HL vs Aster.
        * estimation de la "slippage" en bps via les spreads locaux.
        * sélection de la meilleure paire par net_adj_bps = net_mid_bps - slip_bps.
        * sizing en USD sur equity HL (risk.alloc_frac/min/max_notional), conversion en qty base.
        * exécution dual IOC via dual_ioc.
        * flatten immédiat du résiduel (no carry) via reduce_only sur le côté en surplus.
        * logging CSV (arb_exec.csv) + notifier.

    Rien ici ne tient de position: chaque tick() est autonome et cherche un scalp instantané.
    """

    def __init__(
        self,
        *,
        hl,
        asr,
        cfg: Dict[str, Any],
        active_pairs: List[str],
        risk: Dict[str, Any],
        notifier,
        pnl_csv_path: str,
        live_mode: bool,
        dashboard: Any = None,
        pos_mgr: Any = None,
        lighter: Any = None,
        lighter_map: Dict[str, str] = None,
    ) -> None:
        self.hl = hl
        self.asr = asr
        self.lighter = lighter
        # Map: HL Coin -> Lighter Symbol (Reverse of lighter_map passed in)
        self.hl_to_lighter = {v: k for k, v in (lighter_map or {}).items()}
        self.cfg = cfg or {}
        self.symbol_map: Dict[str, Dict[str, str]] = self.cfg.get("symbol_map") or {}
        self.active_pairs = list(active_pairs or [])
        self.risk = risk or {}
        self.notifier = notifier
        self.live_mode = bool(live_mode)
        self.dashboard = dashboard
        self.pos_mgr = pos_mgr

        # SCALP MASTER SWITCH - allows completely disabling scalp
        self.enabled = bool(risk.get("scalp_enabled", True))
        if not self.enabled:
            log.warning("[SCALP] Strategy DISABLED via scalp_enabled=false in risk.yaml")

        # Paper mode equity tracking (for compounding simulation)
        self.paper_equity = float(self.risk.get("scalp_paper_equity", 1000.0))
        self.paper_equity_start = self.paper_equity  # For P&L calculation

        # Note: STABLEUSDT and other blocked symbols are now managed in config/blocklist.yaml

        # Risk params
        self.alloc_frac = float(self.risk.get("alloc_frac_per_trade", 0.10))
        self.min_usd = float(self.risk.get("notional_per_trade_min", 12.0))
        self.max_usd = float(self.risk.get("notional_per_trade_max", 200.0))
        self.entry_bps = float(self.risk.get("spread_entry_bps", 40.0))
        self.max_edge_bps = float(self.risk.get("max_edge_bps", 80.0))  # Reject suspiciously high edges
        self.max_spread_pct = float(self.risk.get("max_spread_pct", 1.0))
        self.max_scalp_alloc = float(self.risk.get("max_scalp_total_alloc_pct", 0.20))
        self.flatten_timeout_s = float(self.risk.get("flatten_timeout_s", 10.0))
        self.slip_buffer = float(self.risk.get("slippage_bps_buffer", 10.0))
        self.exit_fees_bps = float(self.risk.get("estimated_exit_fees_bps", 7.0))
        self.min_slippage_bps = float(self.risk.get("min_slippage_bps", 5.0))

        # backoff global
        self._last_trade_ts = 0.0
        self._min_gap_s = float(self.risk.get("min_trade_gap_s", 0.75))
        
        # 3-tick confirmation: require opportunity to persist for N consecutive ticks
        self._required_confirms = int(self.risk.get("required_confirms", 3))
        self._confirm_count: Dict[str, int] = {}  # symbol -> consecutive tick count above threshold
        
        # Per-symbol cooldown after ANY trade attempt (success or fail)
        self._symbol_cooldowns: Dict[str, float] = {}
        self._cooldown_s = float(self.risk.get("symbol_cooldown_s", 180.0))
        
        # Volatility tracking
        self.max_vol_bps = float(self.risk.get("max_volatility_bps", 35.0))
        self._price_history: Dict[str, List[Tuple[float, float]]] = {}  # symbol -> [(ts, price), ...]
        
        # BBO freshness requirement (max age in ms before skipping trade)
        self._max_bbo_age_ms = float(self.risk.get("max_bbo_age_ms", 500.0))
        
        # LATENCY OPT: Cache fee values at init to avoid method calls in hot path
        self._cached_hl_fee = self.hl.fees_taker_bps() / 10000.0
        self._cached_as_fee = self.asr.fees_taker_bps() / 10000.0
        self._cached_lt_fee = self.lighter.fees_taker_bps() / 10000.0 if self.lighter else 0.0
        
        # Momentum filter state (short-term price velocity)
        self._momentum_window_s = 1.0  # 1 second window
        self._price_velocity: Dict[str, Deque[Tuple[float, float]]] = {} # sym -> deque of (ts, mid)

        # csv - Scalp logs in dedicated subfolder
        self._csv = pnl_csv_path
        self._opp_csv = "logs/scalp/opp_audit.csv"
        self._trades_csv = "logs/scalp/trades_audit.csv"
        try:
            os.makedirs("logs/scalp", exist_ok=True)
            
            # Reset arb_exec.csv on each run
            with open(self._csv, "w", newline="", encoding="utf-8") as f:
                 w = csv.writer(f)
                 w.writerow(["ts", "symbol", "direction", "net_mid_bps", "slip_bps", "net_adj_bps", "qty",
                             "filled_hl", "filled_as", "hl_ok", "as_ok", "hl_close_ok", "as_close_ok", 
                             "pnl_usd", "dry_run"])

            # Reset opp_audit.csv on each run
            with open(self._opp_csv, "w", newline="", encoding="utf-8") as f:
                 w = csv.writer(f)
                 w.writerow(["ts", "symbol", "edge_bps", "status", "reason", "pnl_usd", "details"])

            # Reset trades_audit.csv on each run (DETAILED trade log)
            with open(self._trades_csv, "w", newline="", encoding="utf-8") as f:
                 w = csv.writer(f)
                 w.writerow(["ts", "symbol", "direction", "status", 
                             "edge_bps", "qty", 
                             "hl_bid", "hl_ask", "as_bid", "as_ask",
                             "px_hl_sent", "px_as_sent",
                             "hl_fill", "as_fill", 
                             "hl_status", "as_status",
                             "hl_err", "as_err",
                             "flatten_hl", "flatten_as",
                             "dry_run"])
        except Exception:
            pass

    def update_mappings(self, pairs_map: Dict[str, str], lighter_map: Dict[str, str]) -> None:
        """Update symbol mappings dynamically from discovery loop."""
        # pairs_map is simple AS->HL. Ensure symbol_map (used for lookup) contains these.
        for as_sym, hl_coin in pairs_map.items():
            if as_sym not in self.symbol_map:
                self.symbol_map[as_sym] = {"hyperliquid": hl_coin, "aster": as_sym}
        
        # Rebuild HL->Lighter reverse map
        self.hl_to_lighter = {v: k for k, v in (lighter_map or {}).items()}


    # ---------- utilities ----------

    def _map(self, alias_symbol: str) -> Tuple[str, str]:
        mp = self.symbol_map.get(alias_symbol, {})
        return (
            str(mp.get("hyperliquid") or alias_symbol).upper(),
            str(mp.get("aster") or alias_symbol).upper(),
        )

    def _bbo(
        self, alias_symbol: str
    ) -> Optional[Tuple[Tuple[float, float], Tuple[float, float]]]:
        hl_coin, as_sym = self._map(alias_symbol)
        bbo_hl = self.hl.get_bbo(hl_coin)
        bbo_as = self.asr.get_bbo(as_sym)
        if not (bbo_hl and bbo_as):
            return None
        return (bbo_hl, bbo_as)

    def _mid(
        self, bbo: Tuple[Tuple[float, float], Tuple[float, float]]
    ) -> Tuple[float, float]:
        # BBO can be 2-tuple or 4-tuple from each venue
        hb, ha = bbo[0][0], bbo[0][1]
        ab, aa = bbo[1][0], bbo[1][1]
        return ((hb + ha) * 0.5, (ab + aa) * 0.5)

    def _fees(self) -> Tuple[float, float]:
        # LATENCY OPT: Use cached values
        return (self._cached_hl_fee, self._cached_as_fee)

    def _check_volatility(self, symbol: str, current_price: float, now: float) -> bool:
        """
        Check instant volatility (max deviation in last 5s).
        Returns True if volatility is acceptable (SAFE).
        Returns False if volatility is too high (UNSAFE).
        """
        hist = self._price_history.setdefault(symbol, [])
        hist.append((now, current_price))
        
        # Prune older than 5s
        cutoff = now - 5.0
        while hist and hist[0][0] < cutoff:
            hist.pop(0)
            
        if len(hist) < 2:
            return True
            
        prices = [p for _, p in hist]
        low, high = min(prices), max(prices)
        if low <= 0: return False
        
        vol_bps = (high - low) / low * 10000.0
        return vol_bps <= self.max_vol_bps

    def _slip_bps(
        self, bbo: Tuple[Tuple[float, float], Tuple[float, float]]
    ) -> float:
        # BBO can be 2-tuple or 4-tuple from each venue
        hb, ha = bbo[0][0], bbo[0][1]
        ab, aa = bbo[1][0], bbo[1][1]
        mid_hl = (hb + ha) * 0.5
        mid_as = (ab + aa) * 0.5
        if mid_hl <= 0 or mid_as <= 0:
            return 0.0
        half_hl = (ha - hb) / mid_hl * 5000.0
        half_as = (aa - ab) / mid_as * 5000.0
        return max(0.0, half_hl + half_as)

    def _edge_mid_bps(
        self, 
        bbo: Tuple[Tuple[float, float], Tuple[float, float]], 
        fees: Tuple[float, float]
    ) -> Tuple[float, float]:
        # BBO can be 2-tuple or 4-tuple from each venue
        hb, ha = bbo[0][0], bbo[0][1]
        ab, aa = bbo[1][0], bbo[1][1]
        hl_fee, as_fee = fees
        mid = ((hb + ha) + (ab + aa)) * 0.25
        if mid <= 0:
            return (0.0, 0.0)
        # BUY A / SELL B
        e1 = (ab * (1 - as_fee) - ha * (1 + hl_fee)) / mid
        # SELL A / BUY B
        e2 = (hb * (1 - hl_fee) - aa * (1 + as_fee)) / mid
        return (_bps(e1), _bps(e2))

    def update_pairs(self, new_map: Dict[str, str]) -> None:
        """
        Injected at runtime by discovery Loop.
        Adds new pairs to the symbol map and active list.
        """
        import logging
        log = logging.getLogger("strategy")
        
        added = []
        for as_sym, hl_coin in new_map.items():
            if as_sym not in self.symbol_map:
                self.symbol_map[as_sym] = {"hyperliquid": hl_coin, "aster": as_sym}
                self.active_pairs.append(as_sym)
                self.active_pairs.append(as_sym)
                added.append(f"{as_sym}:{hl_coin}")
                
        # Also update Lighter map if passed (hacky dynamic injection or just rely on manual restart for Lighter discovery)
        # Ideally we'd need a separate update_lighter_map method or pass it here. 
        # For now assume static lighter map or it's updated via direct attribute access if needed.
        
        if added:
            log.info("[STRAT] Dynamic Discovery added %d pairs: %s", len(added), ", ".join(added[:10]))

    def update_lighter_map(self, lighter_map: Dict[str, str]) -> None:
        """Update HL->Lighter mapping dynamically."""
        self.hl_to_lighter = {v: k for k, v in lighter_map.items()}

    async def _size_qty(self, px_avg: float) -> float:
        """
        Sizing en USD basé sur:
        - PAPER MODE: paper_equity (pour simulation de compounding)
        - LIVE MODE: MIN(equity HL, equity Aster, equity Lighter) pour éviter les rejets margin.
        """
        if not self.live_mode:
            # PAPER MODE: Use simulated paper equity
            safe_eq = self.paper_equity
            if safe_eq <= 0:
                return 0.0
        else:
            # LIVE MODE: Use real venue equity
            eq_hl = await self.hl.equity()
            eq_as = await self.asr.equity()
            eq_lt = None
            if self.lighter:
                try:
                    eq_lt = await self.lighter.equity()
                except Exception:
                    pass

            # Safe fallback if one is None
            val_hl = eq_hl if eq_hl is not None else 0.0
            val_as = eq_as if eq_as is not None else 0.0
            val_lt = eq_lt if eq_lt is not None else float('inf')  # Don't limit if not available

            # Use min of all venues with positive equity to prevent over-allocation
            valid_equities = [v for v in [val_hl, val_as, val_lt] if v > 0 and v < float('inf')]
            if not valid_equities:
                return 0.0
            safe_eq = min(valid_equities)

        # Total Scalp Exposure check (Enforce 20% limit)
        current_scalp_exposure = 0.0
        if self.pos_mgr:
            # Sum up sizes in USD from pos_mgr.open_scalps
            for s_data in self.pos_mgr.open_scalps.values():
                sz = abs(s_data.get('hl_size', 0.0))
                px = s_data.get('hl_entry', 0.0)
                current_scalp_exposure += sz * px
        
        max_scalp_notional = safe_eq * self.max_scalp_alloc
        if current_scalp_exposure >= max_scalp_notional:
            log.debug("[SCALP] At max allocation: $%.2f >= $%.2f (%.0f%% of $%.2f)",
                      current_scalp_exposure, max_scalp_notional, self.max_scalp_alloc * 100, safe_eq)
            return 0.0

        plan = await compute_notional(safe_eq, self.alloc_frac, self.min_usd, self.max_usd)
        
        # Ensure new trade doesn't push us over the limit
        if current_scalp_exposure + plan.notional_usd > max_scalp_notional:
            allowed = max_scalp_notional - current_scalp_exposure
            if allowed < self.min_usd:
                return 0.0
            plan.notional_usd = allowed
            log.info("[SCALP] Sizing capped by max scalp allocation: $%.2f", allowed)

        if plan.notional_usd <= 0 or px_avg <= 0:
            return 0.0
        return plan.notional_usd / max(px_avg, 1e-9)

    async def _flatten_residual(
        self,
        hl_coin: str,
        as_sym: str,
        hl_side: str,
        as_side: str,
        f_hl: float,
        f_as: float,
    ) -> Tuple[bool, bool]:
        """
        Fermeture immédiate des résidus avec retry:
        - Si HL a plus rempli que AS -> reduce_only côté HL sur l'excès.
        - Si AS a plus rempli que HL -> reduce_only côté Aster sur l'excès.
        - 3 tentatives avec backoff.
        """
        import logging
        log = logging.getLogger("trade")

        ok_hl = True
        ok_as = True

        # HL needs flattening?
        if f_hl > f_as:
            diff = abs(f_hl - f_as)
            side = "SELL" if hl_side == "BUY" else "BUY"
            ok_hl = False
            for i in range(3):
                try:
                    r = await self.hl.place_order(
                        hl_coin,
                        side,
                        diff,
                        price=None,
                        ioc=True,
                        reduce_only=True,
                    )
                    status = (r.get("status") or "").lower()
                    if status in ("filled", "accepted"):
                        ok_hl = True
                        break
                    else:
                        log.warning(
                            "Flatten HL attempt %d failed: status=%s reason=%s",
                            i + 1,
                            status,
                            r.get("reason", ""),
                        )
                except Exception as e:
                    log.warning("Flatten HL attempt %d exc: %s", i + 1, e)
                
                await asyncio.sleep(0.5 * (i + 1))  # 0.5s, 1.0s, ...
            
            if not ok_hl:
                log.critical("CRITICAL: Failed to flatten HL residual after 3 attempts! diff=%f", diff)
                await self.notifier.notify("CRITICAL", f"Failed to flatten HL residual: {hl_coin} diff={diff}")

        # Aster needs flattening?
        elif f_as > f_hl:
            diff = abs(f_as - f_hl)
            side = "SELL" if as_side == "BUY" else "BUY"
            ok_as = False
            for i in range(3):
                try:
                    r = await self.asr.place_order(
                        as_sym,
                        side,
                        diff,
                        price=None,
                        ioc=True,
                        reduce_only=True,
                    )
                    status = (r.get("status") or "").lower()
                    if status in ("filled", "accepted"):
                        ok_as = True
                        break
                    else:
                        log.warning(
                            "Flatten Aster attempt %d failed: status=%s reason=%s",
                            i + 1,
                            status,
                            r.get("reason", ""),
                        )
                except Exception as e:
                    log.warning("Flatten Aster attempt %d exc: %s", i + 1, e)

                await asyncio.sleep(0.5 * (i + 1))

            if not ok_as:
                log.critical("CRITICAL: Failed to flatten Aster residual after 3 attempts! diff=%f", diff)
                await self.notifier.notify("CRITICAL", f"Failed to flatten Aster residual: {as_sym} diff={diff}")

        return ok_hl, ok_as

    async def _notify_exec(self, res: ArbExecResult) -> None:
        """
        Notif détaillée uniquement en mode LIVE (sinon, ça spamme pour rien).
        """
        # Update paper equity for paper mode PnL tracking
        if not self.live_mode and res.hl_ok and res.as_ok:
            # Calculate PnL from net_adj_bps and notional
            notional = res.qty * ((res.filled_hl + res.filled_as) / 2) if res.filled_hl > 0 else res.qty * 100
            pnl_usd = (res.net_adj_bps / 10000.0) * notional
            self.paper_equity += pnl_usd

        # Always update dashboard if present
        if self.dashboard:
            self.dashboard.add_trade({
                "timestamp": time.time(),
                "symbol": res.symbol,
                "direction": res.direction,
                "net_adj_bps": res.net_adj_bps,
                "qty": res.qty,
                "filled_hl": res.filled_hl,
                "filled_as": res.filled_as,
                "hl_ok": res.hl_ok,
                "as_ok": res.as_ok,
                "dry_run": res.dry_run,
                "v1_label": res.v1_label,
                "v2_label": res.v2_label
            })

        if not self.live_mode:
            return
        msg = (
            f"[SCALP LIVE] {res.symbol} | "
            f"{res.direction} | "
            f"net_mid={res.net_mid_bps:.2f} bps | "
            f"depth_slip≈{res.slip_bps:.2f} bps => net_adj≈{res.net_adj_bps:.2f} bps | "
            f"alloc={res.qty:.4f} | "
            f"{res.v1_label}_ok={res.hl_ok} {res.v2_label}_ok={res.as_ok} "
            f"{res.v1_label}_close_ok={res.hl_close_ok} {res.v2_label}_close_ok={res.as_close_ok} "
            f"v1_reason={res.hl_reason or ''} v2_reason={res.as_reason or ''}"
            f"{' dry_run=True' if res.dry_run else ''}"
        )
        try:
            self.notifier.notify_sync("Arb Exec", msg)
        except Exception:
            pass

    def _write_csv(self, res: ArbExecResult) -> None:
        try:
            new = not os.path.exists(self._csv)
            with open(self._csv, "a", newline="", encoding="utf-8") as fh:
                w = csv.writer(fh)
                if new:
                    w.writerow(
                        [
                            "ts",
                            "symbol",
                            "direction",
                            "net_mid_bps",
                            "slip_bps",
                            "net_adj_bps",
                            "qty",
                            "filled_hl",
                            "filled_as",
                            "HL_ok",
                            "AS_ok",
                            "HL_close_ok",
                            "AS_close_ok",
                            "pnl_usd",
                            "dry_run",
                            "v1_label",
                            "v2_label",
                        ]
                    )
                w.writerow(
                    [
                        time.time(),
                        res.symbol,
                        res.direction,
                        res.net_mid_bps,
                        res.slip_bps,
                        res.net_adj_bps,
                        res.qty,
                        res.filled_hl,
                        res.filled_as,
                        res.hl_ok,
                        res.as_ok,
                        res.hl_close_ok,
                        res.as_close_ok,
                        f"{res.pnl_usd:.4f}",
                        res.dry_run,
                        res.v1_label,
                        res.v2_label,
                    ]
                )
        except Exception:
            pass

    def _log_opp(self, symbol: str, edge_bps: float, status: str, reason: str = "", pnl_usd: float = 0.0, details: str = "") -> None:
        """Log structured opportunity audit."""
        try:
            with open(self._opp_csv, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([
                    time.time(), symbol, f"{edge_bps:.2f}", status, reason, f"{pnl_usd:.4f}", details
                ])
        except Exception:
            pass

    def _log_trade(self, symbol: str, direction: str, status: str, edge_bps: float,
                   qty: float, bbo_hl: Tuple[float, float], bbo_as: Tuple[float, float],
                   px_hl: float, px_as: float,
                   hl_fill: float, as_fill: float,
                   hl_status: str, as_status: str,
                   hl_err: str, as_err: str,
                   flatten_hl: bool, flatten_as: bool,
                   dry: bool,
                   v1_label: str = "HL", v2_label: str = "AS") -> None:
        """Log detailed trade info for executed trades only."""
        try:
            with open(self._trades_csv, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([
                    time.time(), symbol, direction, status,
                    f"{edge_bps:.2f}", f"{qty:.6f}",
                    f"{bbo_hl[0]:.6f}", f"{bbo_hl[1]:.6f}",
                    f"{bbo_as[0]:.6f}", f"{bbo_as[1]:.6f}",
                    f"{px_hl:.6f}", f"{px_as:.6f}" if isinstance(px_as, (int, float)) else px_as,
                    f"{hl_fill:.6f}", f"{as_fill:.6f}",
                    hl_status, as_status,
                    hl_err, as_err,
                    flatten_hl, flatten_as,
                    dry,
                    v1_label, v2_label
                ])
        except Exception:
            pass

    # ---------- public ----------

    async def tick(self) -> None:
        """
        Une itération de scalp:
        - Choix de la meilleure paire par net_adj_bps.
        - Application d'un gap minimal entre deux scalps.
        """
        # MASTER SWITCH - skip all scalp logic if disabled
        if not self.enabled:
            return

        now = time.time()
        if (now - self._last_trade_ts) < self._min_gap_s:
            return

        # Load blocklist once per tick
        blocked_symbols: set = set()
        try:
            import yaml
            blocklist_path = "config/blocklist.yaml"
            if os.path.exists(blocklist_path):
                with open(blocklist_path, "r", encoding="utf-8") as f:
                    blocklist_cfg = yaml.safe_load(f) or {}
                blocked_symbols = set(blocklist_cfg.get("blocked_symbols", []))
        except Exception:
            pass

        candidates = []

        # BUILD UNIVERSAL COIN SET
        # Instead of iterating only Aster symbols, we iterate all known HL coins
        # and check each venue independently.
        all_hl_coins = set()
        
        # From symbol_map (AS -> HL mappings)
        for as_sym, mapping in self.symbol_map.items():
            hl_coin = mapping.get("hyperliquid")
            if hl_coin:
                all_hl_coins.add(hl_coin)
        
        # From Lighter map (LT -> HL, so values are HL coins)
        all_hl_coins.update(self.hl_to_lighter.keys())

        for hl_coin in all_hl_coins:
            # BLOCKLIST CHECK (check HL coin)
            if hl_coin.upper() in blocked_symbols:
                continue
            
            # Get BBOs from each venue
            b_hl = self.hl.get_bbo(hl_coin)
            
            # Find Aster symbol for this HL coin (reverse lookup)
            as_sym = None
            for s, m in self.symbol_map.items():
                if m.get("hyperliquid") == hl_coin:
                    as_sym = s
                    break
            b_as = self.asr.get_bbo(as_sym) if as_sym else None
            
            # Find Lighter symbol
            lt_sym = self.hl_to_lighter.get(hl_coin)
            b_lt = self.lighter.get_bbo(lt_sym) if (self.lighter and lt_sym) else None

            # Skip if no data from any venue
            if not b_hl and not b_as and not b_lt:
                continue

            # -- 1. HL vs AS --
            if b_hl and b_as:
                b_hl_as = (b_hl, b_as)
                e1, e2 = self._edge_mid_bps(b_hl_as, self._fees())
                slip = self._slip_bps(b_hl_as)
                net_mid = max(e1, e2)
                net_adj = net_mid - self.slip_buffer
                
                # Only detect if there's a POSITIVE edge (profitable direction exists)
                if net_adj > 10.0:
                     self._log_opp(hl_coin, net_adj, "DETECTED", details=f"HL-AS mid={net_mid:.2f} slip={slip:.2f}")
                     candidates.append((hl_coin, net_mid, slip, net_adj, b_hl_as, ("HL", "AS"), (hl_coin, as_sym)))
            
            # -- 2. HL vs LT --
            if b_hl and b_lt:
                b_hl_lt = (b_hl, b_lt)
                # LATENCY OPT: Use cached fees
                e1_lt, e2_lt = self._edge_mid_bps(b_hl_lt, (self._cached_hl_fee, self._cached_lt_fee))
                slip_lt = self._slip_bps(b_hl_lt)
                net_mid_lt = max(e1_lt, e2_lt)
                net_adj_lt = net_mid_lt - self.slip_buffer
                
                # Only detect if there's a POSITIVE edge (profitable direction exists)
                if net_adj_lt > 10.0:
                    self._log_opp(hl_coin, net_adj_lt, "DETECTED", details=f"HL-LT mid={net_mid_lt:.2f} slip={slip_lt:.2f}")
                    candidates.append((hl_coin, net_mid_lt, slip_lt, net_adj_lt, b_hl_lt, ("HL", "LT"), (hl_coin, lt_sym)))

            # -- 3. AS vs LT --
            if b_as and b_lt:
                b_as_lt = (b_as, b_lt)
                # LATENCY OPT: Use cached fees
                e1_al, e2_al = self._edge_mid_bps(b_as_lt, (self._cached_as_fee, self._cached_lt_fee))
                slip_al = self._slip_bps(b_as_lt)
                net_mid_al = max(e1_al, e2_al)
                net_adj_al = net_mid_al - self.slip_buffer
                
                # Only detect if there's a POSITIVE edge (profitable direction exists)
                if net_adj_al > 10.0:
                    self._log_opp(hl_coin, net_adj_al, "DETECTED", details=f"AS-LT mid={net_mid_al:.2f} slip={slip_al:.2f}")
                    candidates.append((hl_coin, net_mid_al, slip_al, net_adj_al, b_as_lt, ("AS", "LT"), (as_sym, lt_sym)))


        # If no candidates > 10bps, return
        if not candidates:
            return

        # Sort candidates by Edge (net_adj) Descending
        # We want to process the BEST opportunities first
        candidates.sort(key=lambda x: x[3], reverse=True)

        for candidate in candidates:
            sym, net_mid, slip_bps, net_adj, bbo, venue_pair, symbols = candidate
            
            # Resolve Venue Objects and Symbols
            v1_name, v2_name = venue_pair
            s1, s2 = symbols
            
            # Helper to get adapter and fees
            def get_adapter(name):
                if name == "HL": return self.hl
                if name == "AS": return self.asr
                if name == "LT": return self.lighter
                return None
            
            v1 = get_adapter(v1_name)
            v2 = get_adapter(v2_name)
            
            hl_coin = s1 if v1_name == "HL" else (s2 if v2_name == "HL" else self.symbol_map.get(sym, {}).get("hyperliquid"))
            as_sym = s1 if v1_name == "AS" else (s2 if v2_name == "AS" else self.symbol_map.get(sym, {}).get("aster"))
            
            # Volatility Check (Hedges Fund Grade Protection)
            # BBO can be 2-tuple (bid, ask) or 4-tuple (bid, ask, bid_sz, ask_sz)
            b1, b2 = bbo
            hb, ha = b1[0], b1[1]
            ab, aa = b2[0], b2[1]
            mid_price = (hb + ha + ab + aa) * 0.25
            if not self._check_volatility(sym, mid_price, now):
                self._log_opp(sym, net_adj, "SKIPPED", reason="High Volatility", 
                              details=f"max_vol={self.max_vol_bps}bps")
                continue # Try next candidate
            
            # Unique confirmation key per venue pair (to avoid counter conflicts)
            confirm_key = f"{sym}:{v1_name}-{v2_name}"
            
            if net_adj < self.entry_bps:
                if net_adj > 5.0:
                     self._log_opp(sym, net_adj, "SKIPPED", reason="Low Edge", details=f"req={self.entry_bps}")
                # Reset confirmation counter for this symbol (edge dropped)
                self._confirm_count[confirm_key] = 0
                continue # Try next candidate
            
            # SAFETY: Reject suspiciously high edges (likely fake/manipulation/illiquidity)
            if net_adj > self.max_edge_bps:
                self._log_opp(sym, net_adj, "SKIPPED", reason="Edge Too High (suspicious)", 
                              details=f"max={self.max_edge_bps}")
                self._confirm_count[confirm_key] = 0
                continue # Try next candidate
            
            # 3-tick confirmation: increment counter, only trade when confirmed
            current_count = self._confirm_count.get(confirm_key, 0) + 1
            self._confirm_count[confirm_key] = current_count
            
            if current_count < self._required_confirms:
                self._log_opp(sym, net_adj, "CONFIRMING", reason=f"Tick {current_count}/{self._required_confirms}", 
                              details=f"mid={net_mid:.2f} slip={slip_bps:.2f} ({v1_name}-{v2_name})")
                continue # Try next candidate
                # NOTE: If we continue here, we technically skip higher-ranked opps that are still confirming
                # to potentially execute lower-ranked opps that ARE confirmed.
                # This is acceptable: liquidity is fleeting. Better to take the 45bps ready NOW
                # than wait for the 50bps that might disappear.
            
            # Reset counter after confirmed - will need to re-confirm for next trade
            self._confirm_count[confirm_key] = 0

            # BBO can be 2-tuple or 4-tuple
            hb, ha = bbo[0][0], bbo[0][1]
            ab, aa = bbo[1][0], bbo[1][1]
            # Re-fetch fees for this specific pair
            f1 = v1.fees_taker_bps() / 10000.0
            f2 = v2.fees_taker_bps() / 10000.0
            e1, e2 = self._edge_mid_bps(bbo, (f1, f2))

            if e1 >= e2:
                direction = f"Long {v1_name} / Short {v2_name}"
                v1_side, v2_side = "BUY", "SELL"
                px_avg = (ha + ab) * 0.5
            else:
                direction = f"Short {v1_name} / Long {v2_name}"
                v1_side, v2_side = "SELL", "BUY"
                px_avg = (hb + aa) * 0.5

            # --- QUANTITY ALIGNMENT ---
            # Independent rounding for both venues, then take MINIMUM
            # This prevents partial fills due to stepSize differences.
            raw_qty = await self._size_qty(px_avg)
            if raw_qty <= 0:
                self._log_opp(sym, net_adj, "SKIPPED", reason="Min Size/qty", details=f"px={px_avg:.2f}")
                continue
                
            qty_v1 = v1.round_qty(s1, raw_qty)
            qty_v2 = v2.round_qty(s2, raw_qty)
            qty = min(qty_v1, qty_v2)

            # PRE-EXECUTION VALIDATION: Check WS connectivity OR fresh BBO data
            # Allow trading if BBO is fresh (<2s) even if WS momentarily disconnected
            v1_connected = v1.is_connected()
            v2_connected = v2.is_connected()
            
            if not v1_connected or not v2_connected:
                # WS is down - try REST fallback FIRST to get fresh BBO
                fresh_hl_bbo = None
                if not v1_connected:
                    fresh_v1_bbo = await v1.fetch_bbo_rest(s1)
                    
                # Re-check BBO age after potential REST refresh
                # Standardize get_bbo_age_ms across all adapters
                bbo_age_v1 = v1.get_bbo_age_ms(s1) if hasattr(v1, 'get_bbo_age_ms') else 0
                bbo_age_v2 = v2.get_bbo_age_ms(s2) if hasattr(v2, 'get_bbo_age_ms') else 0
                max_acceptable_age = 2000  # 2s max - consistent with _max_bbo_age_ms
                
                if bbo_age_v1 > max_acceptable_age or bbo_age_v2 > max_acceptable_age:
                    self._log_opp(sym, net_adj, "SKIPPED", reason="WS disconnected + stale BBO", 
                                  details=f"{v1_name}={v1_connected} {v2_name}={v2_connected} age1={bbo_age_v1:.0f}ms")
                    continue # Try next candidate
                # Else: BBO is fresh enough (via WS cache or REST fallback), proceed with trade
                import logging
                logging.getLogger("trade").debug(f"[TRADE] Proceeding despite WS disconnect - BBO fresh ({bbo_age_v1:.0f}ms)")
                
                # Update local bbo with fresh REST data if we fetched it
                if fresh_v1_bbo:
                    (hb, ha) = fresh_v1_bbo
                    bbo = ((hb, ha), (ab, aa))
            
            
            # PER-SYMBOL COOLDOWN: Prevent spam trading on same symbol
            last_attempt = self._symbol_cooldowns.get(sym, 0.0)
            if (now - last_attempt) < self._cooldown_s:
                # Don't log spam
                continue # Try next candidate

            # STACKING CHECK: Do not enter if PositionManager already has a position for this symbol
            if self.pos_mgr:
                if self.pos_mgr.get_position(sym):
                    # Silently skip, we are already exposed
                    continue # Try next candidate
                
                # BAD SYMBOLS CHECK: Skip if symbol recently hit stop-loss
                expiry = self.pos_mgr.bad_symbols.get(hl_coin)
                if expiry and now < expiry:
                    self._log_opp(sym, net_adj, "SKIPPED", reason="Post-StopLoss Cooldown", 
                                  details=f"wait {int(expiry - now)}s")
                    continue
            
            # BBO FRESHNESS CHECK: Verify BOTH venues have fresh data
            # AUDIT FIX: Previously only checked V1, now check both V1 and V2
            bbo_age_v1 = v1.get_bbo_age_ms(s1) if hasattr(v1, 'get_bbo_age_ms') else 0
            bbo_age_v2 = v2.get_bbo_age_ms(s2) if hasattr(v2, 'get_bbo_age_ms') else 0
            bbo_age_ms = max(bbo_age_v1, bbo_age_v2)  # Use worst case

            # --- MOMENTUM FILTER (Antigravity Optimization) ---
            # Avoid entries if one venue is moving much faster than the other (toxic drift)
            m_hist = self._price_velocity.setdefault(sym, deque(maxlen=20))
            m_hist.append((now, mid_price))
            if len(m_hist) >= 5:
                dt = m_hist[-1][0] - m_hist[0][0]
                if dt > 0.1: # Need at least 100ms of data
                    vel_bps = (m_hist[-1][1] - m_hist[0][1]) / m_hist[0][1] * 10000.0 / dt
                    if abs(vel_bps) > 100.0: # Trending > 100 bps/sec is toxic
                        self._log_opp(sym, net_adj, "SKIPPED", reason="Toxic Momentum", 
                                      details=f"vel={vel_bps:.1f} bps/s")
                        continue
            
            # --- ADAPTIVE EDGE (Antigravity Optimization) ---
            # Demand 50% more edge if volatility is high (>25bps 5s vol)
            dynamic_min_edge = self.entry_bps
            hist_vol = self._price_history.get(sym, [])
            if len(hist_vol) > 5:
                prices = [p for _, p in hist_vol if p > 0]
                if len(prices) > 2:
                    low, high = min(prices), max(prices)
                    vol_bps = (high - low) / low * 10000.0 if low > 0 else 0
                    if vol_bps > 25.0:
                        dynamic_min_edge *= 1.5
                        if net_adj < dynamic_min_edge:
                            self._log_opp(sym, net_adj, "SKIPPED", reason="High Vol Safety", 
                                          details=f"edge={net_adj:.1f} < req={dynamic_min_edge:.1f}")
                            continue

            if bbo_age_ms > self._max_bbo_age_ms:
                # AUDIT FIX: Removed REST fallback. Speed is safety.
                # If WS is stale, we skip immediately rather than blocking on slow REST calls.
                stale_venue = v1_name if bbo_age_v1 >= bbo_age_v2 else v2_name
                self._log_opp(sym, net_adj, "SKIPPED", reason=f"Stale {stale_venue} BBO",
                              details=f"V1={bbo_age_v1:.0f}ms V2={bbo_age_v2:.0f}ms > {self._max_bbo_age_ms:.0f}ms")
                continue # Try next candidate
            
            # Re-check BBO freshness and edge before firing orders
            # This prevents asymmetric fills when one venue's data is stale
            # Manually reconstruct BBO from adapters
            b_v1 = v1.get_bbo(s1)
            b_v2 = v2.get_bbo(s2)
            fresh_bbo = (b_v1, b_v2) if b_v1 and b_v2 else None
            if not fresh_bbo:
                self._log_opp(sym, net_adj, "SKIPPED", reason="Stale BBO", details="Pre-exec check failed")
                continue # Try next candidate
            
            # Re-calculate edge with fresh data
            fresh_e1, fresh_e2 = self._edge_mid_bps(fresh_bbo, (f1, f2))
            fresh_slip = self._slip_bps(fresh_bbo)
            fresh_net_adj = max(fresh_e1, fresh_e2) - self.slip_buffer
            
            # Edge must still be valid (at least 80% of original)
            if fresh_net_adj < self.entry_bps * 0.8:
                self._log_opp(sym, net_adj, "SKIPPED", reason="Edge degraded", 
                              details=f"fresh_net_adj={fresh_net_adj:.1f} < {self.entry_bps * 0.8:.1f}")
                continue # Try next candidate
            
            # Check spread width on both venues (max 0.35% spread per venue)
            # BBO can be 2-tuple or 4-tuple
            fresh_hb, fresh_ha = fresh_bbo[0][0], fresh_bbo[0][1]
            fresh_ab, fresh_aa = fresh_bbo[1][0], fresh_bbo[1][1]
            hl_spread_pct = (fresh_ha - fresh_hb) / fresh_ha * 100 if fresh_ha > 0 else 999
            as_spread_pct = (fresh_aa - fresh_ab) / fresh_aa * 100 if fresh_aa > 0 else 999
            
            # Mid Price for PnL estimation
            mid_px = (fresh_ha + fresh_hb + fresh_aa + fresh_ab) / 4.0 if (fresh_ha > 0 and fresh_aa > 0) else 0.0
            
            if hl_spread_pct > self.max_spread_pct or as_spread_pct > self.max_spread_pct:
                self._log_opp(sym, net_adj, "SKIPPED", reason="Wide spread", 
                              details=f"HL={hl_spread_pct:.2f}% AS={as_spread_pct:.2f}% (max={self.max_spread_pct}%)")
                continue # Try next candidate
            
            # CRITICAL: Roundtrip cost check for Scalp profitability
            # On Scalp exit, we pay spread AGAIN on both venues
            # Using 1.5x multiplier (not 2x) because aggressive IOC orders don't always cross full spread
            entry_spread_bps = (hl_spread_pct + as_spread_pct) * 100  # Convert % to bps
            roundtrip_spread_bps = entry_spread_bps * 1.5  # Entry + partial Exit (aggressive orders)
            exit_fees_bps = self.exit_fees_bps  # Approximate exit fees
            total_roundtrip_cost = roundtrip_spread_bps + exit_fees_bps
            
            if fresh_net_adj < total_roundtrip_cost * 1.1:  # Require 10% safety margin
                self._log_opp(sym, net_adj, "SKIPPED", reason="Low vs roundtrip cost", 
                              details=f"edge={fresh_net_adj:.1f} < cost={total_roundtrip_cost:.1f}*1.1")
                continue # Try next candidate

            # LIQUIDITY/DEPTH CHECK: Ensure enough liquidity to fill our order
            # Get depth from both venues
            depth_v1 = v1.get_bbo_with_depth(s1) if hasattr(v1, 'get_bbo_with_depth') else None
            depth_v2 = v2.get_bbo_with_depth(s2) if hasattr(v2, 'get_bbo_with_depth') else None
            
            min_liquidity_usd = self.min_usd * 2  # Need at least 2x our order size
            
            if depth_v1 and depth_v2:
                # Extract relevant side sizes
                # For BUY, we need ask liquidity; for SELL, we need bid liquidity
                (bid_v1, ask_v1) = depth_v1
                (bid_v2, ask_v2) = depth_v2
                
                # V1 liquidity check
                v1_liq_usd = ask_v1[1] * ask_v1[0] if v1_side == "BUY" else bid_v1[1] * bid_v1[0]
                v2_liq_usd = bid_v2[1] * bid_v2[0] if v2_side == "SELL" else ask_v2[1] * ask_v2[0]
                
                # Only check if we actually have depth data (size > 0)
                if v1_liq_usd > 0 and v1_liq_usd < qty * mid_px * 6.6: # Strictly need 6.6x depth (15% impact)
                    self._log_opp(sym, net_adj, "SKIPPED", reason=f"Low {v1_name} depth",
                                  details=f"${v1_liq_usd:.0f} < ${qty * mid_px * 6.6:.0f} (15% limit)")
                    continue
                if v2_liq_usd > 0 and v2_liq_usd < qty * mid_px * 6.6:
                    self._log_opp(sym, net_adj, "SKIPPED", reason=f"Low {v2_name} depth",
                                  details=f"${v2_liq_usd:.0f} < ${qty * mid_px * 6.6:.0f} (15% limit)")
                    continue
            # DYNAMIC SLIPPAGE (Aggr): Ensure we cross the spread
            # We use max(5bps, 1.2x current spread) to ensure fill on wide books
            # REDUCED from 15.0 to 5.0 to avoid massive unrealized loss at start
            current_spread_bps = (hl_spread_pct + as_spread_pct) * 100
            # Cap dynamic slippage at 40% of the total edge to preserve profitability
            slippage_bps = max(self.min_slippage_bps, min(current_spread_bps * 1.2, net_adj * 0.4))
            aggressiveness = 1.0 + (slippage_bps / 10000.0)
            
            if v1_side == "BUY":
                px_v1 = fresh_ha * aggressiveness  # Pay above ask
            else:
                px_v1 = fresh_hb / aggressiveness  # Sell below bid
            
            if v2_side == "BUY":
                px_v2 = fresh_aa * aggressiveness  # Pay above ask
            else:
                px_v2 = fresh_ab / aggressiveness  # Sell below bid
            
            # SEQUENTIAL EXECUTION to prevent asymmetric fills:
            # 1. Execute HL first (IOC with aggressive price)
            # 2. Only execute Aster if HL fills > 0
            # This prevents the scenario where HL fills but Aster returns 0
            
            # Record trade attempt timestamp for per-symbol cooldown
            self._symbol_cooldowns[sym] = time.time()
            
            # Round quantity and prices for each venue BEFORE logging or executing
            # ALIGNED QTY: Use the same minimum quantity for both venues
            q1 = v1.round_qty(s1, qty) if hasattr(v1, 'round_qty') else qty
            q2 = v2.round_qty(s2, qty) if hasattr(v2, 'round_qty') else qty
            p1 = v1.round_price(s1, px_v1) if hasattr(v1, 'round_price') else round(px_v1, 6)
            p2 = v2.round_price(s2, px_v2) if hasattr(v2, 'round_price') else round(px_v2, 6)

            # Log with rounded values for accuracy
            self._log_opp(sym, net_adj, "EXECUTING", details=f"qty={q1:.4f} px_{v1_name}={p1:.4f} px_{v2_name}={p2:.4f} aggr={slippage_bps:.1f}bps via {v1_name}-{v2_name} [PARALLEL]")
            
            # Note: Isolated margin mode (5x leverage) is now set once at startup in run script.
            
            # PARALLEL ORDER EXECUTION: Fire both orders simultaneously
            # This cuts latency by ~50% (100ms saved per trade)
            
            async def place_v1():
                return await v1.place_order(
                    s1, v1_side, q1,
                    price=p1,
                    ioc=True, reduce_only=False
                )
            
            async def place_v2():
                # V2 can be Aster or Lighter
                if v2_name == "AS":
                    # FIX: Use LIMIT with GTC (ioc=False) to avoid "OrderExpiry is invalid"
                    # We use an aggressive price to ensure immediate fill
                    return await v2.place_order(
                        s2, v2_side, q2,
                        price=p2,
                        ioc=False, reduce_only=False
                    )
                else:
                    # AGGRESSIVE LIMIT for Lighter (requires price)
                    # Use the p2 we already calculated with buffer
                    return await v2.place_order(
                        s2, v2_side, q2,
                        price=p2,
                        ioc=True, reduce_only=False
                    )
            
            # Execute both (Simulate in PAPER, fire in LIVE)
            if self.live_mode:
                results = await asyncio.gather(place_v1(), place_v2(), return_exceptions=True)
                
                # Parse results (handle exceptions)
                if isinstance(results[0], Exception):
                    res_v1 = {"status": "error", "reason": str(results[0]), "filled": 0.0}
                else:
                    res_v1 = results[0]
                    
                if isinstance(results[1], Exception):
                    res_v2 = {"status": "error", "reason": str(results[1]), "filled": 0.0}
                else:
                    res_v2 = results[1]
            else:
                # PAPER MODE: Simulate instant fills
                res_v1 = {"status": "dry_run", "filled": q1, "dry_run": True, "avg_price": p1, "order_id": f"paper_v1_{int(time.time())}"}
                res_v2 = {"status": "dry_run", "filled": q2, "dry_run": True, "avg_price": p2, "order_id": f"paper_v2_{int(time.time())}"}
            
            st_hl = str(res_v1.get("status", ""))
            f_hl = float(res_v1.get("filled", 0.0) or 0.0)
            hl_reason = str(res_v1.get("reason", "") or "")
            is_dry_1 = bool(res_v1.get("dry_run", False))
            
            st_as = str(res_v2.get("status", ""))
            f_as = float(res_v2.get("filled", 0.0) or 0.0)
            as_reason = str(res_v2.get("reason", "") or "")
            is_dry_2 = bool(res_v2.get("dry_run", False))
            dry = is_dry_1 or is_dry_2

            # Check if order was accepted by the venue (even if not filled yet)
            ok_hl_status = st_hl.lower() in ("filled", "accepted", "ok", "dry_run")
            ok_as_status = st_as.lower() in ("filled", "accepted", "new", "dry_run")
            
            # CRITICAL FIX for GTC/IOC orders:
            # If V2 (Aster or Lighter) returned 'accepted'/'new' but f_as is 0, wait 500ms and poll once
            # to avoid emergency flattening while the order is actually filling.
            if not dry and f_as <= 0 and ok_as_status:
                await asyncio.sleep(0.5)
                # Poll fill status
                try:
                    v2_pos = await v2.get_positions()
                    found_fill = False
                    for p in v2_pos:
                        if p['symbol'].upper() == s2.upper():
                            fill_sz = abs(float(p['size']))
                            if fill_sz > 0:
                                f_as = fill_sz
                                st_as = "filled"
                                log.info(f"[STRAT] Post-placement poll found {v2_name} fill: {f_as}")
                                found_fill = True
                                break

                    if not found_fill and v2_name == "AS":
                        # AUDIT FIX: Cancel Orphaned Order (only for GTC orders like Aster)
                        # If still not filled after poll, we MUST cancel the GTC order
                        # to prevent it from filling later (unhedged).
                        log.warning(f"[STRAT] {v2_name} order {res_v2.get('order_id')} not filled after poll. CANCELLING.")
                        try:
                            await v2.cancel_order(symbol=s2, order_id=res_v2.get('order_id'))
                            as_reason += " [CANCELLED_AFTER_POLL]"
                            st_as = "cancelled"
                            ok_as_status = False # Mark as failed so we trigger flatten of V1
                        except Exception as c_err:
                            log.error(f"[STRAT] FAILED TO CANCEL {v2_name} ORDER {res_v2.get('order_id')}: {c_err}")
                            as_reason += f" [CANCEL_FAILED: {c_err}]"
                            # This is a critical state - we might have an orphan.
                            # But we proceed to flatten V1 anyway.
                    elif not found_fill and v2_name == "LT":
                        # Lighter uses IOC, so no cancel needed - just mark as failed
                        log.warning(f"[STRAT] {v2_name} IOC order not filled after poll.")
                        as_reason += " [IOC_UNFILLED]"
                        st_as = "unfilled"
                        ok_as_status = False

                except Exception as e:
                    log.debug(f"[STRAT] Post-placement {v2_name} poll/cancel failed: {e}")
            
            ok_hl = ok_hl_status and (f_hl > 0.0 or dry)
            ok_as = ok_as_status and (f_as > 0.0 or dry)
            
            # PARALLEL EXECUTION GUARDRAILS: Handle all mismatch scenarios
            # Case 1: V1 filled, V2 didn't -> Flatten V1
            if f_hl > 0 and f_as <= 0 and not dry:
                self._log_opp(sym, net_adj, "FAILED", 
                              reason=f"{v1_name}={st_hl} {v2_name}={st_as}",
                              details=f"fill1={f_hl} fill2={f_as} - CRITICAL: flattening {v1_name}")
                
                # Emergency flatten V1 position
                flatten_side = "SELL" if v1_side == "BUY" else "BUY"
                if self.live_mode:
                    res_flat = await v1.place_order(s1, flatten_side, f_hl, price=None, ioc=True, reduce_only=True)
                    # Update reason with flatten result to inform user
                    flat_status = res_flat.get("status", "unknown")
                    flat_filled = float(res_flat.get("filled", 0.0) or 0.0)
                    if flat_filled >= f_hl:
                        hl_reason += f" [FLATTENED OK {flat_filled}]"
                    else:
                        hl_reason += f" [FLATTEN FAIL {flat_status} filled={flat_filled}]"
                        log.error(f"[STRAT] EMERGENCY FLATTEN FAILED for {sym}: {res_flat}")
                else:
                    hl_reason += f" [PAPER_FLATTENED {f_hl}]"
                
                # REPORT this failure so it appears in dashboard/CSV
                # We record filled_as=0, and status=FAILED
                res_fail = ArbExecResult(
                    symbol=sym,
                    direction=direction,
                    qty=qty,
                    net_mid_bps=net_mid,
                    slip_bps=slip_bps,
                    net_adj_bps=net_adj,
                    filled_hl=f_hl,
                    filled_as=0.0,
                    hl_ok=True,
                    as_ok=False,
                    hl_close_ok=False, # It was emergency flattened, not normal close
                    as_close_ok=False,
                    hl_reason=hl_reason,
                    as_reason=as_reason,
                    pnl_usd= -1.0 * qty * mid_px * 0.0020, # Estimation of loss (20bps)
                    dry_run=dry,
                    v1_label=v1_name,
                    v2_label=v2_name
                )
                self._last_trade_ts = time.time()
                await self._notify_exec(res_fail)
                self._write_csv(res_fail)
                # Log to trades_audit for debugging
                self._log_trade(
                    symbol=sym, direction=direction, status="FAILED", edge_bps=net_adj,
                    qty=qty, bbo_hl=(hb, ha), bbo_as=(ab, aa),
                    px_hl=p1, px_as=p2 if isinstance(p2, (int, float)) else "N/A",
                    hl_fill=f_hl, as_fill=0.0,
                    hl_status=st_hl, as_status=st_as,
                    hl_err=hl_reason, as_err=as_reason,
                    flatten_hl=True, flatten_as=False, dry=dry,
                    v1_label=v1_name, v2_label=v2_name
                )
                return
            
            # Case 2: V2 filled, V1 didn't -> Flatten V2
            if f_as > 0 and f_hl <= 0 and not dry:
                self._log_opp(sym, net_adj, "FAILED", 
                              reason=f"{v1_name}={st_hl} {v2_name}={st_as}",
                              details=f"fill1={f_hl} fill2={f_as} - CRITICAL: flattening {v2_name}")
                
                # Emergency flatten V2 position
                flatten_side_v2 = "SELL" if v2_side == "BUY" else "BUY"
                if self.live_mode:
                    res_flat_v2 = await v2.place_order(s2, flatten_side_v2, f_as, price=None, ioc=True, reduce_only=True)
                    
                    flat_status_v2 = res_flat_v2.get("status", "unknown")
                    flat_filled_v2 = float(res_flat_v2.get("filled", 0.0) or 0.0)
                    if flat_filled_v2 >= f_as:
                        as_reason += f" [FLATTENED OK {flat_filled_v2}]"
                    else:
                        as_reason += f" [FLATTEN FAIL {flat_status_v2} filled={flat_filled_v2}]"
                        log.error(f"[STRAT] EMERGENCY FLATTEN V2 FAILED for {sym}: {res_flat_v2}")
                else:
                    as_reason += f" [PAPER_FLATTENED {f_as}]"
                
                # Log and return
                res_fail_v2 = ArbExecResult(
                    symbol=sym,
                    direction=direction,
                    qty=qty,
                    net_mid_bps=net_mid,
                    slip_bps=slip_bps,
                    net_adj_bps=net_adj,
                    filled_hl=0.0,
                    filled_as=f_as,
                    hl_ok=False,
                    as_ok=True,
                    hl_close_ok=False,
                    as_close_ok=False,
                    hl_reason=hl_reason,
                    as_reason=as_reason,
                    pnl_usd= -1.0 * qty * mid_px * 0.0020, # Estimation of loss (20bps)
                    dry_run=dry,
                    v1_label=v1_name,
                    v2_label=v2_name
                )
                self._last_trade_ts = time.time()
                await self._notify_exec(res_fail_v2)
                self._write_csv(res_fail_v2)

                self._log_trade(
                    symbol=sym, direction=direction, status="FAILED", edge_bps=net_adj,
                    qty=q1, bbo_hl=(hb, ha), bbo_as=(ab, aa),
                    px_hl=p1, px_as=p2 if isinstance(p2, (int, float)) else "N/A",
                    hl_fill=0.0, as_fill=f_as,
                    hl_status=st_hl, as_status=st_as,
                    hl_err=hl_reason, as_err=as_reason,
                    flatten_hl=False, flatten_as=True, dry=dry,
                    v1_label=v1_name, v2_label=v2_name
                )
                return

            cl_hl, cl_as = await self._flatten_residual(
                hl_coin, as_sym, v1_side, v2_side, f_hl, f_as
            )

            if f_as > 0:
                as_reason += f" [FILLED {f_as}]"
            else:
                as_reason += f" [FAIL {st_as}]"
                 
            # Log Success
            res_success = ArbExecResult(
                symbol=sym,
                direction=direction,
                qty=qty,
                net_mid_bps=net_mid,
                slip_bps=slip_bps,
                net_adj_bps=net_adj,
                filled_hl=f_hl,
                filled_as=f_as,
                hl_ok=ok_hl,
                as_ok=ok_as,
                hl_close_ok=False, # We HOLD for convergence
                as_close_ok=False, # We HOLD for convergence
                hl_reason=hl_reason,
                as_reason=as_reason,
                pnl_usd= qty * mid_px * (net_adj / 10000.0),
                dry_run=dry,
                v1_label=v1_name,
                v2_label=v2_name
            )
            self._symbol_cooldowns[sym] = time.time()
            await self._notify_exec(res_success)
            self._write_csv(res_success)
            
            # Log detailed trade for audit
            self._log_trade(
                symbol=sym, direction=direction, status="FILLED_OPEN", edge_bps=net_adj,
                qty=q1, bbo_hl=(hb, ha), bbo_as=(ab, aa),
                px_hl=p1, px_as=p2 if isinstance(p2, (int, float)) else "N/A",
                hl_fill=f_hl, as_fill=f_as,
                hl_status=st_hl, as_status=st_as,
                hl_err=hl_reason, as_err=as_reason,
                flatten_hl=cl_hl, flatten_as=cl_as, dry=dry,
                v1_label=v1_name, v2_label=v2_name
            )
            
            # REGISTER POSITION in PositionManager for exit tracking
            if self.pos_mgr and f_hl > 0 and f_as > 0:
                # Unified order IDs mapping
                oids = {v1_name: res_v1.get('order_id'), v2_name: res_v2.get('order_id')}

                # Calculate ACTUAL fill basis (spread between the prices we sent)
                # This is the baseline for PnL tracking in PositionManager
                actual_fill_basis = (p1 - p2) / p2 * 10000.0 if v1_side == 'BUY' else (p2 - p1) / p1 * 10000.0
                
                self.pos_mgr.open_scalps[sym] = {
                    'symbol': sym,
                    'hl_coin': hl_coin,
                    'direction': direction,
                    'hl_size': f_hl if v1_side == 'BUY' else -f_hl,
                    'as_size': f_as if v2_side == 'BUY' else -f_as,
                    'hl_entry': p1,
                    'as_entry': p2,
                    'entry_time': time.time(),
                    'entry_basis': actual_fill_basis,
                    'entry_mid_basis': fresh_net_adj + self.slip_buffer, # Back-calculate original mid basis
                    'hl_oid': oids.get('HL'),
                    'as_oid': oids.get('AS'),
                    'lt_oid': oids.get('LT'),
                }
                self.pos_mgr.position_start_times[sym] = time.time()
                log.info(f"[STRAT] Registered position {sym} in PositionManager for exit tracking")
            
            return  # Successfully executed, exit tick()
