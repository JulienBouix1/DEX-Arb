# -*- coding: utf-8 -*-
"""
Perp-perp arbitrage engine: Hyperliquid perps (USDC collateral) <-> Aster perps.

Strict invariants:
- After each scalp both venues must be flat on the traded symbol, or the symbol is quarantined.
- No implicit assumptions; measure, log, validate before acting.
- Gating on HL min notional and quantization to eliminate rounding and invalid size.
- Defensive flatten on any partial or failure.

All non-ASCII glyphs have been removed to avoid tokenizer issues in static analyzers.
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any, Deque
import time
from collections import deque
import math

from core.pnl_logger import CsvPNLLogger


def _mid(bid, ask):
    try:
        b = float(bid)
        a = float(ask)
        if b > 0.0 and a > 0.0:
            return (a + b) / 2.0
    except Exception:
        pass
    return None


def _bps_from_prices(px_a, px_b):
    try:
        a = float(px_a)
        b = float(px_b)
        if a > 0.0 and b > 0.0:
            return (a - b) / ((a + b) / 2.0) * 1e4
    except Exception:
        pass
    return None


class KillSwitch(Exception):
    pass


class PerpPerpArb:
    """
    HL <-> Aster

    Live flow:
      1) Aster MARKET first
      2) HL MARKET on the actual filled base size from Aster, strict HL quantization
      3) Immediate flatten on both legs (HL then AS) using MARKET
      4) IOC and flatten retries as fallbacks
      5) L2 gating and depth checks
    """

    def __init__(
        self,
        hl: Any,
        asr: Any,
        cfg: Dict[str, Any],
        active_pairs: List[str],
        risk: Dict[str, Any],
        notifier: Any,
        pnl_csv_path: str = "logs/arb_exec.csv",
        live_mode: bool = False,
    ):
        self.hl = hl
        self.asr = asr
        self.cfg = cfg or {}
        self.active_pairs = list(active_pairs or [])
        self.risk = risk or {}
        self.notifier = notifier
        self.live_mode = bool(live_mode)

        # Equity
        self.equity_mode: str = str(self.risk.get("equity_mode", "real_only")).lower()
        if self.equity_mode not in ("paper_only", "real_only", "hybrid"):
            self.equity_mode = "real_only"
        self.initial_capital_usd: float = float(self.risk.get("paper_capital_usd_arb", 1000.0))
        self.cash_usd: float = self.initial_capital_usd
        self.equity_usd: float = self.initial_capital_usd
        self.equity_real_usd: Optional[float] = None

        # Fees and slippage
        fees_hl = float(self.cfg.get("hyperliquid", {}).get("fees", {}).get("taker_bps", 4.5))
        fees_as = float(self.cfg.get("aster", {}).get("fees", {}).get("taker_bps", 3.5))
        self.fees_bps: float = fees_hl + fees_as
        self.slip_bps: float = float(self.risk.get("slippage_bps_buffer", 6.0))

        self._pnl_logger = CsvPNLLogger(pnl_csv_path)
        self._symbol_map: Dict[str, Dict[str, str]] = dict(self.cfg.get("symbol_map", {}))

        # Anti retap and quotas
        self._cooldowns: Dict[str, int] = {}
        self._hits: Dict[str, int] = {}
        self._armed: Dict[str, bool] = {}
        self._last_entry_net: Dict[str, float] = {}
        self._last_exec_ts: Dict[str, int] = {}
        self._last_exec_mids: Dict[str, Tuple[float, float]] = {}
        self._per_min_notional: Dict[str, Deque[Tuple[int, float]]] = {}
        self._per_min_global: Deque[Tuple[int, float]] = deque()

        # Insufficient balance backoff
        self._insufficient_counts: Dict[str, int] = {}
        self._insufficient_cooldown_until: Dict[str, float] = {}
        self._insuff_retry_max: int = int(self.risk.get("insufficient_retry_max", 3))
        self._insuff_backoff_s: float = float(self.risk.get("insufficient_backoff_s", 20))
        self._alloc_downscale_on_fail: float = float(self.risk.get("alloc_downscale_on_fail", 0.5))
        self._per_symbol_alloc_scale: Dict[str, float] = {}

        # Min notional HL handling
        self._allow_alloc_up_to_min_hl: bool = bool(self.risk.get("allow_alloc_up_to_min_hl", True))
        self._headroom_bps_for_min_hl: int = int(self.risk.get("headroom_bps_for_min_hl", 25))
        self._allow_aster_topup_to_match_hl: bool = bool(self.risk.get("allow_aster_topup_to_match_hl", True))

        # Backoff counters per symbol
        self._backoff_counts: Dict[str, Dict[str, int]] = {}
        self._backoff_report_every_min: int = int(self.risk.get("backoff_report_every_min", 5))
        self._last_backoff_report_ts: float = 0.0

        self._rearm_hyst_bps: float = float(self.risk.get("rearm_hysteresis_bps", 6.0))
        self._post_trade_cooldown_ms: int = int(self.risk.get("post_trade_cooldown_ms", 1000))
        self._min_mid_change_bps: float = float(self.risk.get("min_mid_change_bps", 3.0))
        self._max_usd_per_symbol_per_min: float = float(self.risk.get("max_usd_per_symbol_per_min", 2000.0))
        self._retap_delay_ms: int = int(self.risk.get("retap_delay_ms", 60_000))

        self._min_hits: int = int(self.risk.get("min_consecutive_hits", 3))
        self._signal_cooldown_ms: int = int(self.risk.get("signal_cooldown_ms", 400))

        self._alloc_frac: float = float(self.risk.get("alloc_frac_per_trade", 0.20))
        self._notional_min: float = float(self.risk.get("notional_per_trade_min", 12.0))
        self._notional_max: float = float(self.risk.get("notional_per_trade_max", 10_000_000.0))

        self.entry_bps: float = float(self.risk.get("spread_entry_bps", 25.0))
        self._fees_refresh_ms: int = int(self.risk.get("fees_refresh_ms", 10 * 60 * 1000))

        # Quarantine per symbol
        self._quarantine: set[str] = set()
        self._quarantine_reason: dict[str, str] = {}
        self._backoff_next_log_ts: dict[str, float] = {}
        self._tick_idx: int = 0
        self._scalps_done: int = 0
        self._eq_hist: list[float] = []
        self._last_equity_log_ts: float = 0.0
        self._last_fees_refresh_ts: int = 0

        # LIVE checks
        self._posttrade_every_n: int = int(self.risk.get("posttrade_equity_check_every", 5))
        self._monotonic_tol_usd: float = float(self.risk.get("monotonic_tolerance_usd", 0.10))
        self._exec_count_since_check: int = 0
        self._last_checked_equity: Optional[float] = None

        # Heartbeat time-based fallback
        self._hb_every_s: float = float(self.risk.get("equity_hb_interval_s", 60.0))
        self._last_hb_ts: float = 0.0

        self._kill_on_insufficient: bool = bool(self.risk.get("kill_on_insufficient_balance", False))
        self._reserve_floor: float = float(self.risk.get("reserve_floor_usd_per_venue", 10.0))
        self._real_only_update_on_check: bool = bool(self.risk.get("real_only_update_on_check", True))

        # L2 and instant scalp
        self.require_l2_hl: bool = bool(self.risk.get("require_l2_hl", True))
        self.require_l2_aster: bool = bool(self.risk.get("require_l2_aster", True))
        self.instant_flatten_after_entry: bool = bool(self.risk.get("instant_flatten_after_entry", True))
        self.min_l2_cover_frac: float = float(self.risk.get("min_l2_cover_frac", 0.8))

        # Flatten
        self.flatten_on_partial: bool = bool(self.risk.get("flatten_on_partial", True))
        self.flatten_retry_ms: int = int(self.risk.get("flatten_retry_ms", 800))
        self.flatten_timeout_s: int = int(self.risk.get("flatten_timeout_s", 10))

        # Kill flatten
        self.flatten_on_kill: bool = bool(self.risk.get("flatten_on_kill", True))
        self.kill_flatten_retry_ms: int = int(self.risk.get("kill_flatten_retry_ms", 600))
        self.kill_flatten_timeout_s: int = int(self.risk.get("kill_flatten_timeout_s", 10))

        # Sanity
        sf = dict(self.risk.get("sanity_filter", {}))
        self._l2_wait_s: float = float(sf.get("max_wait_s", 1.0))
        self._l2_wait_step_s: float = 0.10

    # ----------------------------- Seed / setters -----------------------------
    def seed_from_equity(self, equity_real: float) -> None:
        eq = max(0.0, float(equity_real or 0.0))
        self.equity_real_usd = eq
        self.cash_usd = eq
        self.equity_usd = eq

    def set_equity_real(self, equity_real: float) -> None:
        eq = max(0.0, float(equity_real or 0.0))
        self.equity_real_usd = eq
        if self.equity_mode == "real_only" and self.live_mode:
            self.equity_usd = eq

    def _cooldown_active(self, sym: str) -> bool:
        until = self._insufficient_cooldown_until.get(sym, 0.0)
        return time.time() < float(until)

    # ----------------------------- Books helpers -----------------------------
    def _hl_book_tuple(self, sym_usdt: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        coin = (self._symbol_map.get(sym_usdt, {}).get("hyperliquid") or sym_usdt).upper().replace("USDT", "")
        bk = self.hl.get_book(coin)
        if not bk or len(bk) < 2:
            return None, None, None
        return bk

    def _as_book_tuple(self, sym_usdt: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        sym = (self._symbol_map.get(sym_usdt, {}).get("aster") or sym_usdt).upper()
        bk = self.asr.get_book(sym)
        if not bk or len(bk) < 2:
            return None, None, None
        return bk

    async def _maybe_refresh_fees(self, t_ms: int) -> None:
        if t_ms - self._last_fees_refresh_ts < self._fees_refresh_ms:
            return
        self._last_fees_refresh_ts = t_ms

        async def _pull(v: Any):
            try:
                fn = getattr(v, "get_fees", None)
                if fn is None:
                    return None
                if asyncio.iscoroutinefunction(fn):
                    return await fn()
                return fn()
            except Exception:
                return None

        hl_fees = await _pull(self.hl)
        as_fees = await _pull(self.asr)

        taker_hl = float(hl_fees.get("taker_bps")) if hl_fees and "taker_bps" in hl_fees else math.nan
        taker_as = float(as_fees.get("taker_bps")) if as_fees and "taker_bps" in as_fees else math.nan

        if not math.isnan(taker_hl) or not math.isnan(taker_as):
            hl_default = float(self.cfg.get("hyperliquid", {}).get("fees", {}).get("taker_bps", 4.5))
            as_default = float(self.cfg.get("aster", {}).get("fees", {}).get("taker_bps", 3.5))
            hl_bps = taker_hl if not math.isnan(taker_hl) else hl_default
            as_bps = taker_as if not math.isnan(taker_as) else as_default
            self.fees_bps = float(hl_bps) + float(as_bps)

    async def _extract_aster_filled(self, res: dict) -> tuple[float, float]:
        try:
            raw = (res or {}).get("raw") or {}
            if not isinstance(raw, dict):
                return float(res.get("qty") or 0.0), 0.0
            exe = raw.get("executedQty") or raw.get("executed_qty") or raw.get("cumBase") or raw.get("cumBaseQty")
            avg = raw.get("avgPrice") or raw.get("avg_price") or raw.get("price") or 0.0
            if exe is None and isinstance(raw.get("fills"), list):
                try:
                    exe = sum(float(f.get("qty") or f.get("executedQty") or 0.0) for f in raw["fills"])
                    if not avg and exe:
                        num = sum(float((f.get("price") or 0.0)) * float((f.get("qty") or 0.0)) for f in raw["fills"])
                        avg = (num / float(exe)) if exe else 0.0
                except Exception:
                    pass
            if exe is None:
                cq = raw.get("cummulativeQuoteQty") or raw.get("cumQuote") or raw.get("cumQuoteQty")
                p = float(avg or 0.0)
                if cq and p:
                    try:
                        exe = float(cq) / float(p) if p > 0 else 0.0
                    except Exception:
                        exe = None
            q = float(exe or 0.0)
            a = float(avg or 0.0)
            if q <= 0.0:
                q = float(res.get("qty") or 0.0)
            return q, a
        except Exception:
            try:
                return float(res.get("qty") or 0.0), 0.0
            except Exception:
                return 0.0, 0.0

    async def _quantize_hl_floor(self, coin: str, qty: float) -> float:
        try:
            get_dec = getattr(self.hl, "_get_sz_decimals", None)
            if get_dec:
                if asyncio.iscoroutinefunction(get_dec):
                    szd = int(await get_dec(coin))
                else:
                    szd = int(get_dec(coin))
            else:
                szd = 4
        except Exception:
            szd = 4
        try:
            q = getattr(self.hl, "_dec_floor")(float(qty), int(szd))  # type: ignore
        except Exception:
            step = 10.0 ** (-max(0, int(szd)))
            q = float(int(float(qty) / step)) * step
        return max(0.0, float(q))

    async def _notify_exec(self, text: str) -> None:
        try:
            await self.notifier.notify("Arb Exec", text)
        except Exception:
            try:
                self.notifier.notify_sync("Arb Exec", text)
            except Exception:
                pass

    # ----------------------------- L2 helpers -----------------------------
    def _get_l2_levels(self, venue: Any, sym: str, side: str) -> Optional[List[Tuple[float, float]]]:
        try:
            if venue is self.hl:
                bk = self.hl.get_l2_book(sym)
                if not bk or side not in bk:
                    return None
                return list(bk[side])[:20]
            l2 = getattr(self.asr, "get_l2_book", None) if venue is self.asr else None
            if l2:
                bk = self.asr.get_l2_book(sym)
                if not bk or side not in bk:
                    return None
                return list(bk[side])[:20]
            return None
        except Exception:
            return None

    def _sum_depth_usd(self, levels: List[Tuple[float, float]] | None) -> float:
        if not levels:
            return 0.0
        s = 0.0
        for px, qty in levels:
            if px > 0 and qty > 0:
                s += px * qty
        return s

    def _estimate_vwap(self, levels: List[Tuple[float, float]] | None, alloc_usd: float) -> Optional[float]:
        if not levels or alloc_usd <= 0:
            return None
        remaining = float(alloc_usd)
        cost = 0.0
        qty_acq = 0.0
        for px, qty in levels:
            if px <= 0 or qty <= 0:
                continue
            usd_liq = qty * px
            take_usd = min(usd_liq, remaining)
            take_qty = take_usd / px
            cost += take_qty * px
            qty_acq += take_qty
            remaining -= take_usd
            if remaining <= 1e-9:
                break
        if qty_acq <= 0:
            return None
        return cost / qty_acq

    def _net_edge_bps(self, sym_usdt: str, alloc_usd: float) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        hl_bid, hl_ask, _ = self._hl_book_tuple(sym_usdt)
        as_bid, as_ask, _ = self._as_book_tuple(sym_usdt)
        mid_hl = _mid(hl_bid, hl_ask)
        mid_as = _mid(as_bid, as_ask)
        if mid_hl is None or mid_as is None or mid_hl <= 0 or mid_as <= 0:
            return None, None, None
        raw_bps = _bps_from_prices(mid_hl, mid_as)
        net_mid_bps = raw_bps - self.fees_bps
        return net_mid_bps, mid_hl, mid_as

    def _depth_slippage_bps(self, sym_usdt: str, side: int, alloc_usd: float) -> Optional[float]:
        hl_bid, hl_ask, _ = self._hl_book_tuple(sym_usdt)
        as_bid, as_ask, _ = self._as_book_tuple(sym_usdt)
        mid_hl = _mid(hl_bid, hl_ask)
        mid_as = _mid(as_bid, as_ask)
        if mid_hl is None or mid_as is None or mid_hl <= 0 or mid_as <= 0:
            return None

        m = self._symbol_map.get(sym_usdt, {})
        hl_sym = (m.get("hyperliquid") or sym_usdt).upper().replace("USDT", "")
        as_sym = (m.get("aster") or sym_usdt).upper()

        total_bps = 0.0
        if side > 0:
            asks_hl = self._get_l2_levels(self.hl, hl_sym, side="asks")
            vwap_buy_hl = self._estimate_vwap(asks_hl, alloc_usd) if asks_hl else None
            if vwap_buy_hl and mid_hl > 0:
                total_bps += max(0.0, _bps_from_prices(vwap_buy_hl, mid_hl))
            bids_as = self._get_l2_levels(self.asr, as_sym, side="bids")
            vwap_sell_as = self._estimate_vwap(bids_as, alloc_usd) if bids_as else None
            if vwap_sell_as and mid_as > 0:
                total_bps += max(0.0, _bps_from_prices(mid_as, vwap_sell_as))
        else:
            asks_as = self._get_l2_levels(self.asr, as_sym, side="asks")
            vwap_buy_as = self._estimate_vwap(asks_as, alloc_usd) if asks_as else None
            if vwap_buy_as and mid_as > 0:
                total_bps += max(0.0, _bps_from_prices(vwap_buy_as, mid_as))
            bids_hl = self._get_l2_levels(self.hl, hl_sym, side="bids")
            vwap_sell_hl = self._estimate_vwap(bids_hl, alloc_usd) if bids_hl else None
            if vwap_sell_hl and mid_hl > 0:
                total_bps += max(0.0, _bps_from_prices(mid_hl, vwap_sell_hl))
        return total_bps

    def _enough_depth(self, sym_usdt: str, side: int, alloc_usd: float) -> bool:
        m = self._symbol_map.get(sym_usdt, {})
        hl_sym = (m.get("hyperliquid") or sym_usdt).upper().replace("USDT", "")
        as_sym = (m.get("aster") or sym_usdt).upper()
        need = float(alloc_usd) * float(min(max(self.min_l2_cover_frac, 0.0), 1.0))

        if side > 0:
            hl_asks = self._get_l2_levels(self.hl, hl_sym, "asks")
            as_bids = self._get_l2_levels(self.asr, as_sym, "bids")
            ok_hl = (self.hl.get_book(hl_sym) is not None) if not hl_asks else (self._sum_depth_usd(hl_asks) >= need)
            ok_as = (self.asr.get_book(as_sym) is not None) if not as_bids else (self._sum_depth_usd(as_bids) >= need)
            return ok_hl and ok_as
        else:
            as_asks = self._get_l2_levels(self.asr, as_sym, "asks")
            hl_bids = self._get_l2_levels(self.hl, hl_sym, "bids")
            ok_as = (self.asr.get_book(as_sym) is not None) if not as_asks else (self._sum_depth_usd(as_asks) >= need)
            ok_hl = (self.hl.get_book(hl_sym) is not None) if not hl_bids else (self._sum_depth_usd(hl_bids) >= need)
            return ok_as and ok_hl

    # ----------------------------- Helpers strat -----------------------------
    def _bump_hit(self, sym: str, ok_entry: bool) -> bool:
        h = int(self._hits.get(sym, 0))
        h = h + 1 if ok_entry else 0
        self._hits[sym] = h
        return h >= self._min_hits

    def _armed_check_and_update(self, sym: str, abs_edge: float, entry_bps: float, _net_adj_bps: float) -> bool:
        armed = bool(self._armed.get(sym, False))
        if not armed:
            self._armed[sym] = True
            return False
        if abs_edge < max(0.0, entry_bps - self._rearm_hyst_bps):
            self._armed[sym] = False
            return False
        return True

    def _allowed_usd_per_symbol_minute(self) -> float:
        frac = float(self.risk.get("cap_sym_frac_per_min", 0.0))
        floor = float(self.risk.get("cap_sym_floor_usd", 0.0))
        ceil = float(self.risk.get("cap_sym_ceil_usd", 0.0))
        if frac > 0:
            limit = frac * self.equity_usd
            if floor > 0:
                limit = max(limit, floor)
            if ceil > 0:
                limit = min(limit, ceil)
            return float(limit)
        return float(self._max_usd_per_symbol_per_min)

    def _global_cap_ok(self, t_ms: int, alloc_usd: float) -> bool:
        frac = float(self.risk.get("cap_global_frac_per_min", 0.0))
        if frac <= 0:
            return True
        while self._per_min_global and (t_ms - self._per_min_global[0][0] > 60_000):
            self._per_min_global.popleft()
        s = sum(v for _, v in self._per_min_global)
        return (s + float(alloc_usd)) <= (frac * self.equity_usd)

    def _post_trade_windows_ok(self, sym: str, t_ms: int, alloc_usd: float) -> bool:
        last = int(self._last_exec_ts.get(sym, 0))
        if t_ms - last < self._post_trade_cooldown_ms:
            return False
        if t_ms - last < self._retap_delay_ms:
            return False
        q = self._per_min_notional.get(sym)
        if q is None:
            return self._global_cap_ok(t_ms, alloc_usd)
        while q and (t_ms - q[0][0] > 60_000):
            q.popleft()
        s = sum(v for _, v in q)
        limit_sym = self._allowed_usd_per_symbol_minute()
        if (s + float(alloc_usd)) > limit_sym:
            return False
        return self._global_cap_ok(t_ms, alloc_usd)

    def _mids_changed_enough(self, sym: str, mid_hl: float, mid_as: float) -> bool:
        last = self._last_exec_mids.get(sym)
        if not last:
            return True
        p_hl, p_as = last
        if p_hl <= 0 or p_as <= 0:
            return True
        bps_hl = abs(_bps_from_prices(mid_hl, p_hl))
        bps_as = abs(_bps_from_prices(mid_as, p_as))
        return (bps_hl >= self._min_mid_change_bps) or (bps_as >= self._min_mid_change_bps)

    # ----------------------------- LIVE helpers -----------------------------
    def _extract_fail_reason(self, res: Any) -> str:
        if isinstance(res, dict):
            for k in ("why", "error", "message", "msg"):
                v = res.get(k)
                if isinstance(v, str) and v:
                    return v
        return "ok" if self._resp_ok(res) else "unknown"

    def _resp_ok(self, res: Any) -> bool:
        if not isinstance(res, dict):
            return False
        if res.get("ok") is True:
            return True
        s = str(res.get("status", "")).lower()
        return s in ("ok", "success", "filled", "succeeded")

    async def _ensure_l2_ready(self, hl_sym: str, as_sym: str) -> Tuple[bool, bool]:
        need_hl = self.require_l2_hl
        need_as = self.require_l2_aster

        has_hl_l2 = bool(getattr(self.hl, "get_l2_book", None) and self.hl.get_l2_book(hl_sym))
        has_as_l2 = bool(getattr(self.asr, "get_l2_book", None) and self.asr.get_l2_book(as_sym))
        has_hl_bbo = bool(self.hl.get_book(hl_sym) is not None)
        has_as_bbo = bool(self.asr.get_book(as_sym) is not None)

        if not has_hl_l2 and hasattr(self.hl, "subscribe_orderbook"):
            try:
                await self.hl.subscribe_orderbook(hl_sym)
            except Exception:
                pass
        if not has_as_l2 and hasattr(self.asr, "subscribe_orderbook"):
            try:
                await self.asr.subscribe_orderbook(as_sym)
            except Exception:
                pass

        if self._l2_wait_s > 0 and ((need_hl and not has_hl_l2) or (need_as and not has_as_l2)):
            deadline = time.time() + float(self._l2_wait_s)
            while time.time() < deadline:
                has_hl_l2 = bool(self.hl.get_l2_book(hl_sym)) or has_hl_l2
                has_as_l2 = bool(self.asr.get_l2_book(as_sym)) or has_as_l2
                has_hl_bbo = bool(self.hl.get_book(hl_sym)) or has_hl_bbo
                has_as_bbo = bool(self.asr.get_book(as_sym)) or has_as_bbo
                if ((need_hl and (has_hl_l2 or has_hl_bbo)) or not need_hl) and \
                   ((need_as and (has_as_l2 or has_as_bbo)) or not need_as):
                    break
                await asyncio.sleep(self._l2_wait_step_s)

        ok_hl = (not need_hl) or has_hl_l2 or has_hl_bbo
        ok_as = (not need_as) or has_as_l2 or has_as_bbo
        return ok_hl, ok_as

    # ----------------------------- Quantization HL -----------------------------
    async def _hl_qty_for_usd(self, hl_sym: str, notional_usd: float, mid_hl: float) -> float:
        notional_usd = max(0.0, float(notional_usd))
        if notional_usd <= 0 or mid_hl <= 0:
            return 0.0
        try:
            get_dec = getattr(self.hl, "_get_sz_decimals", None)
            if get_dec:
                if asyncio.iscoroutinefunction(get_dec):
                    szd = int(await get_dec(hl_sym))
                else:
                    szd = int(get_dec(hl_sym))
            else:
                szd = 4
        except Exception:
            szd = 4
        qty = float(notional_usd) / float(mid_hl)
        try:
            q = getattr(self.hl, "_dec_floor")(float(qty), int(szd))  # type: ignore
        except Exception:
            step = 10.0 ** (-max(0, int(szd)))
            q = float(int(float(qty) / step)) * step

        try:
            min_usd = float(self.cfg.get("hyperliquid", {}).get("min_qty_usd", 12.0))
        except Exception:
            min_usd = 12.0

        if min_usd > 0 and (q * float(mid_hl) < min_usd):
            need = float(min_usd) / float(mid_hl)
            try:
                q = getattr(self.hl, "_dec_ceil")(float(need), int(szd))  # type: ignore
            except Exception:
                q = math.ceil(need / (10.0 ** (-max(0, int(szd))))) * (10.0 ** (-max(0, int(szd))))

        if q <= 0:
            try:
                q = getattr(self.hl, "_dec_floor")(10 ** (-max(0, int(szd))), int(szd))  # type: ignore
            except Exception:
                q = 10.0 ** (-max(0, int(szd)))
        return float(q)

    # ----------------------------- Tick loop -----------------------------
    def _block_symbol(self, sym: str, reason: str = "unspecified") -> None:
        self._quarantine.add(sym)
        self._quarantine_reason[sym] = reason

    async def _is_flat_both(self, sym: str) -> bool:
        m = self._symbol_map.get(sym, {})
        hl_sym = (m.get("hyperliquid") or sym).upper().replace("USDT", "")
        as_sym = (m.get("aster") or sym).upper()
        ok = True
        try:
            hl_addr = (self.cfg.get("hyperliquid", {}) or {}).get("user_address") \
                      or getattr(self.hl, "_user_addr", None) or ""
            if hl_addr:
                poss = await self.hl.get_open_positions(hl_addr)
                for p in poss or []:
                    if (p.get("coin") or "").upper() == hl_sym and abs(float(p.get("qty", 0.0))) > 0:
                        ok = False
                        break
        except Exception:
            ok = False
        try:
            pos_as = await self.asr.get_open_positions()
            for p in pos_as or []:
                if (p.get("symbol") or "").upper() == as_sym and abs(float(p.get("qty", 0.0))) > 0:
                    ok = False
                    break
        except Exception:
            ok = False
        return ok

    async def _unblock_if_flat(self, sym: str) -> None:
        if sym in self._quarantine:
            if await self._is_flat_both(sym):
                try:
                    await self._notify_exec(f"[UNBLOCK] {sym} back to tradable")
                except Exception:
                    pass
                self._quarantine.discard(sym)
                self._quarantine_reason.pop(sym, None)

    async def _get_hl_pos_qty(self, hl_sym: str) -> float:
        """Return HL position size on provided coin."""
        try:
            hl_addr = (self.cfg.get("hyperliquid", {}) or {}).get("user_address") \
                      or getattr(self.hl, "_user_addr", "") or ""
            if not hl_addr:
                return 0.0
            poss = await self.hl.get_open_positions(hl_addr)
            for p in poss or []:
                if str(p.get("coin", "")).upper() == str(hl_sym).upper():
                    try:
                        return float(p.get("qty") or 0.0)
                    except Exception:
                        return 0.0
        except Exception:
            return 0.0
        return 0.0

    async def _hard_flatten_hl(self, hl_sym: str) -> None:
        """Aggressively close any HL leftover position on hl_sym."""
        for attempt in range(5):
            qty_left = float(abs(await self._get_hl_pos_qty(hl_sym)))
            if qty_left <= 0.0:
                return

            q = await self._quantize_hl_floor(hl_sym, qty_left)
            if q <= 0.0:
                break

            signed_left = await self._get_hl_pos_qty(hl_sym)
            side = "SELL" if signed_left > 0 else "BUY"
            try:
                _ = await self.hl.place_order(
                    hl_sym,
                    side,
                    qty=float(q),
                    order_type="MARKET",
                    price=None,
                    slippage_bps=float(self.slip_bps) * (attempt + 1),
                    reduce_only=True,
                )
            except Exception:
                pass
            await asyncio.sleep(0.25)

        try:
            qty_left = float(abs(await self._get_hl_pos_qty(hl_sym)))
            if qty_left > 0.0:
                hl_addr = (self.cfg.get("hyperliquid", {}) or {}).get("user_address") \
                          or getattr(self.hl, "_user_addr", "") or ""
                if hl_addr:
                    _ = await self.hl.flatten_best_effort(
                        hl_addr,
                        coins=[hl_sym],
                        slippage_bps=float(self.slip_bps) * 2.0,
                    )
        except Exception:
            pass

    async def equity_check_now(self) -> None:
        try:
            hl_addr = (self.cfg.get("hyperliquid", {}) or {}).get("user_address") or getattr(self.hl, "_user_addr", "") or ""
            hl_eq = None
            if hl_addr:
                try:
                    hl_eq = await self.hl.get_equity_usd(hl_addr)
                except Exception:
                    hl_eq = None
            try:
                as_eq = await self.asr.get_futures_balance_usd()
            except Exception:
                as_eq = None

            if hl_eq is None and as_eq is None:
                return
            if hl_eq is not None and as_eq is not None:
                tot = float(hl_eq) + float(as_eq)
                self._eq_hist.append(tot)
                if len(self._eq_hist) > 10:
                    self._eq_hist = self._eq_hist[-10:]
                try:
                    await self._notify_exec(f"[EQUITY] HL={hl_eq:.2f} AS={as_eq:.2f} TOT={tot:.2f}")
                except Exception:
                    pass
                tol = float(self.risk.get("monotonic_tolerance_usd", 0.5))
                if len(self._eq_hist) >= 2 and (self._eq_hist[-1] + tol) < self._eq_hist[-2]:
                    await self._notify_exec("[FLATTEN] reason=kill_switch")
                    hl_addr2 = hl_addr
                    deadline = time.time() + float(self.risk.get("kill_flatten_timeout_s", 10.0))
                    while time.time() < deadline:
                        try:
                            if hl_addr2:
                                r = await self.hl.flatten_best_effort(hl_addr2, coins=None, slippage_bps=self.slip_bps)
                                if r and r.get("ok", False):
                                    break
                        except Exception:
                            pass
                        await asyncio.sleep(float(self.risk.get("kill_flatten_retry_ms", 600)) / 1000.0)
                    try:
                        await self.asr.flatten_best_effort(symbols=None)
                    except Exception:
                        pass
                    try:
                        all_flat = await self._is_flat_both(self.active_pairs[0] if self.active_pairs else "")
                    except Exception:
                        all_flat = False
                    try:
                        await self._notify_exec(f"[KILL DONE] all flat={all_flat}")
                    except Exception:
                        pass
                    raise KillSwitch("equity_monotonic_drop")
            else:
                try:
                    if hl_eq is not None:
                        await self._notify_exec(f"[EQUITY] HL={hl_eq:.2f} AS=? TOT=?")
                    elif as_eq is not None:
                        await self._notify_exec(f"[EQUITY] HL=? AS={as_eq:.2f} TOT=?")
                except Exception:
                    pass

        except KillSwitch:
            raise
        except Exception:
            pass

    async def tick(self) -> None:
        t_ms = int(time.time() * 1000)
        try:
            await self._maybe_refresh_fees(t_ms)
        except Exception:
            pass

        try:
            now = time.time()
            if self._hb_every_s > 0 and (now - self._last_hb_ts) >= self._hb_every_s:
                await self.equity_check_now()
                self._last_hb_ts = now
        except Exception:
            pass

        alloc_usd = max(self._notional_min, min(self._notional_max, self._alloc_frac * self.equity_usd))

        for sym in list(self.active_pairs):
            try:
                await self._unblock_if_flat(sym)
                if sym in self._quarantine:
                    try:
                        now2 = time.time()
                        nxt = float(self._backoff_next_log_ts.get(sym, 0.0))
                        if now2 >= nxt:
                            try:
                                await self._notify_exec(f"[BACKOFF] {sym} blocked due to: {self._quarantine_reason.get(sym,'unknown')}")
                            except Exception:
                                pass
                            self._backoff_next_log_ts[sym] = now2 + 15.0
                    except Exception:
                        pass
                    continue

                if self._cooldown_active(sym):
                    continue
                if not self._post_trade_windows_ok(sym, t_ms, alloc_usd):
                    continue

                hl_bid, hl_ask, _ = self._hl_book_tuple(sym)
                as_bid, as_ask, _ = self._as_book_tuple(sym)
                mid_hl = _mid(hl_bid, hl_ask)
                mid_as = _mid(as_bid, as_ask)
                if mid_hl is None or mid_as is None:
                    continue
                if not self._mids_changed_enough(sym, mid_hl, mid_as):
                    continue

                net_mid_bps, mid_hl2, mid_as2 = self._net_edge_bps(sym, alloc_usd)
                if net_mid_bps is None or mid_hl2 is None or mid_as2 is None:
                    continue

                # Direction of legs only; do not flip the sign of the edge used for entry gating
                side_sign = 1 if (mid_as2 > mid_hl2) else -1  # +1 => Long HL / Short AS
                depth_slip_bps = self._depth_slippage_bps(sym, side_sign, alloc_usd) or 0.0
                gross_bps = abs(float(net_mid_bps))
                eff_adj_bps = gross_bps - float(depth_slip_bps)

                ok_entry = eff_adj_bps >= float(self.entry_bps)
                if not self._bump_hit(sym, ok_entry):
                    continue
                if not ok_entry:
                    try:
                        await self._notify_exec("[BACKOFF] neg_expected_edge")
                    except Exception:
                        pass
                    continue
                if not self._armed_check_and_update(sym, abs(net_mid_bps), float(self.entry_bps), eff_adj_bps):
                    continue

                res_hl_o, res_as_o, res_hl_c, res_as_c, ok_hl_all, ok_as_all = await self._live_dual_order(
                    sym, side_sign, alloc_usd, float(mid_hl2), float(mid_as2)
                )

                self._last_exec_ts[sym] = t_ms
                self._last_exec_mids[sym] = (float(mid_hl2), float(mid_as2))
                dq = self._per_min_notional.get(sym)
                if dq is None:
                    dq = deque()
                    self._per_min_notional[sym] = dq
                dq.append((t_ms, float(alloc_usd)))

                try:
                    self._exec_count_since_check += 1
                    if self._exec_count_since_check >= max(1, int(self._posttrade_every_n)):
                        await self.equity_check_now()
                        self._exec_count_since_check = 0
                except Exception:
                    pass

                try:
                    hl_reason = self._extract_fail_reason(res_hl_o)
                    txt = (
                        f"[SCALP LIVE] {sym} | "
                        f"{'Long HL / Short AS' if side_sign>0 else 'Long AS / Short HL'} | "
                        f"net_mid={net_mid_bps:.2f} bps | depth_slip≈{float(depth_slip_bps):.2f} bps => "
                        f"net_adj≈{eff_adj_bps:.2f} bps | alloc={alloc_usd:.2f}"
                    )
                    txt += (
                        f" | HL_ok={self._resp_ok(res_hl_o)} AS_ok={self._resp_ok(res_as_o)} "
                        f"HL_close_ok={self._resp_ok(res_hl_c)} AS_close_ok={self._resp_ok(res_as_c)} "
                        f"hl_reason={hl_reason}"
                    )
                    await self._notify_exec(txt)
                except Exception:
                    pass

            except Exception as e:
                try:
                    await self._notify_exec(f"[tick:{sym}] {e!r}")
                except Exception:
                    pass

        try:
            now_ts = time.time()
            if self._backoff_counts and (now_ts - float(self._last_backoff_report_ts)) >= max(60, 60 * int(self._backoff_report_every_min)):
                parts = []
                for _sym, reasons in sorted(self._backoff_counts.items()):
                    for _r, cnt in sorted(reasons.items()):
                        parts.append(f"{_sym}:{_r}={cnt}")
                if parts:
                    try:
                        await self._notify_exec("[BACKOFF] counters " + " | ".join(parts))
                    except Exception:
                        pass
                self._last_backoff_report_ts = now_ts
        except Exception:
            pass

    # ----------------------------- Live execution primitive -----------------------------
    async def _live_dual_order(
        self, sym: str, side_sign: int, alloc_usd: float, mid_hl: float, mid_as: float
    ) -> Tuple[dict, dict, dict, dict, bool, bool]:

        mapp = self._symbol_map.get(sym, {})
        hl_sym = (mapp.get("hyperliquid") or sym).upper().replace("USDT", "")
        as_sym = (mapp.get("aster") or sym).upper()

        # 0) L2 readiness (BBO/L2 presence)
        hl_ready, as_ready = await self._ensure_l2_ready(hl_sym, as_sym)
        if not hl_ready or not as_ready:
            return (
                {"ok": False, "venue": "HYPERLIQ", "why": "l2_unavailable"},
                {"ok": False, "venue": "ASTER", "why": "l2_unavailable"},
                {},
                {},
                False,
                False,
            )

        # A) SDK mismatch gating
        try:
            api_ready = None
            if hasattr(self.hl, "api_ready"):
                api_ready = self.hl.api_ready()
            else:
                hl_sdk = getattr(self.hl, "_sdk", None)
                api_ready = {"ok": bool(getattr(hl_sdk, "_api_ok", True))} if hl_sdk is not None else {"ok": False}
        except Exception:
            api_ready = {"ok": False}
        if not bool(api_ready.get("ok", False)):
            try:
                await self._notify_exec("[BACKOFF] sdk_mismatch")
            except Exception:
                pass
            try:
                self._backoff_counts.setdefault(sym, {}).setdefault("sdk_mismatch", 0)
                self._backoff_counts[sym]["sdk_mismatch"] += 1
            except Exception:
                pass
            return (
                {"ok": False, "venue": "HYPERLIQ", "why": "sdk_mismatch"},
                {"ok": False, "venue": "ASTER", "why": "peer_skipped"},
                {},
                {},
                False,
                False,
            )

        # B) Min notional and sizing pre-gate on HL
        try:
            min_usd_hl = float(((self.cfg.get("hyperliquid") or {}).get("min_qty_usd") or 0.0))
        except Exception:
            min_usd_hl = 0.0
        px_hl = float(mid_hl or 0.0)
        px_as = float(mid_as or 0.0)

        q_hl_est = 0.0
        try:
            est_base = float(alloc_usd) / float(px_as) if px_as > 0 else 0.0
            q_hl_est = await self._quantize_hl_floor(hl_sym, est_base)
        except Exception:
            q_hl_est = 0.0

        # Compute q_min_hl and step
        try:
            get_dec = getattr(self.hl, "_get_sz_decimals", None)
            if get_dec:
                if asyncio.iscoroutinefunction(get_dec):
                    szd = int(await get_dec(hl_sym))
                else:
                    szd = int(get_dec(hl_sym))
            else:
                szd = 4
        except Exception:
            szd = 4
        step = 10.0 ** (-max(0, int(szd)))

        q_min_hl = 0.0
        if px_hl > 0 and min_usd_hl > 0:
            raw_min = float(min_usd_hl) / float(px_hl)
            try:
                q_min_hl = getattr(self.hl, "_dec_ceil")(raw_min, szd)  # type: ignore
            except Exception:
                q_min_hl = math.ceil(raw_min / step) * step

        used_opt_a = False
        if (q_hl_est <= 0.0) or (px_hl > 0 and (q_hl_est * px_hl) < float(min_usd_hl)):
            if self._allow_alloc_up_to_min_hl and q_min_hl > 0.0:
                headroom = 1.0 + (float(self._headroom_bps_for_min_hl) / 10000.0)
                target_usd_as = q_min_hl * px_as * headroom if px_as > 0 else 0.0
                target_usd_as = min(float(target_usd_as), float(self._notional_max))
                allowed_sym = float(self._allowed_usd_per_symbol_minute())
                t_ms_now = int(time.time() * 1000)
                if (allowed_sym <= 0.0 or target_usd_as <= allowed_sym) and self._global_cap_ok(t_ms_now, target_usd_as):
                    try:
                        await self._notify_exec(f"[ALLOC ADJ] {sym} target_usd_as={target_usd_as:.2f} q_min_hl={q_min_hl:.8f} headroom_bps={int(self._headroom_bps_for_min_hl)}")
                    except Exception:
                        pass
                    alloc_usd = float(target_usd_as)
                    used_opt_a = True

            q_target = float(q_min_hl if used_opt_a else q_hl_est)
            need_usd = q_target * px_hl

            # C) Funding HL
            try:
                hl_addr = (self.cfg.get("hyperliquid") or {}).get("user_address") or getattr(self.hl, "_user_addr", "") or ""
            except Exception:
                hl_addr = ""
            hl_eq = None
            if hl_addr:
                try:
                    hl_eq = await self.hl.get_equity_usd(hl_addr)
                except Exception:
                    hl_eq = None
            if hl_eq is not None and need_usd > float(hl_eq):
                try:
                    await self._notify_exec(f"[BACKOFF] {sym} reason=insufficient_margin need≈{need_usd:.2f} equity_hl≈{float(hl_eq):.2f}")
                except Exception:
                    pass
                try:
                    self._backoff_counts.setdefault(sym, {}).setdefault("insufficient_margin", 0)
                    self._backoff_counts[sym]["insufficient_margin"] += 1
                except Exception:
                    pass
                return (
                    {"ok": False, "venue": "HYPERLIQ", "why": "insufficient_margin"},
                    {"ok": False, "venue": "ASTER", "why": "insufficient_margin"},
                    {},
                    {},
                    False,
                    False,
                )

            if not used_opt_a and not bool(self._allow_aster_topup_to_match_hl):
                try:
                    await self._notify_exec(f"[BACKOFF] {sym} reason=hl_min_gate q_est={q_hl_est:.8f} q_min={q_min_hl:.8f} step={step:.8f}")
                except Exception:
                    pass
                try:
                    self._backoff_counts.setdefault(sym, {}).setdefault("hl_min_gate", 0)
                    self._backoff_counts[sym]["hl_min_gate"] += 1
                except Exception:
                    pass
                return (
                    {"ok": False, "venue": "HYPERLIQ", "why": "hl_min_gate"},
                    {"ok": False, "venue": "ASTER", "why": "hl_min_gate"},
                    {},
                    {},
                    False,
                    False,
                )

        # D) Depth L2 gating final
        if not self._enough_depth(sym, 1 if side_sign > 0 else -1, alloc_usd):
            try:
                await self._notify_exec(f"[BACKOFF] depth_insufficient")
            except Exception:
                pass
            try:
                self._backoff_counts.setdefault(sym, {}).setdefault("depth_insufficient", 0)
                self._backoff_counts[sym]["depth_insufficient"] += 1
            except Exception:
                pass
            return (
                {"ok": False, "venue": "HYPERLIQ", "why": "depth_insufficient"},
                {"ok": False, "venue": "ASTER", "why": "depth_insufficient"},
                {},
                {},
                False,
                False,
            )

        # 1) Aster open
        res_as_open = await self.asr.place_order(
            as_sym,
            side_as := ("SELL" if side_sign > 0 else "BUY"),
            qty=None,
            order_type="MARKET",
            price=None,
            reduce_only=False,
            quote_usd=float(alloc_usd),
        )
        if not self._resp_ok(res_as_open):
            return ({"ok": False, "venue": "HYPERLIQ", "why": "peer_skipped"}, res_as_open, {}, {}, False, False)

        filled_as, _ = await self._extract_aster_filled(res_as_open)

        # Option B: top up AS if allowed
        try:
            try:
                get_dec = getattr(self.hl, "_get_sz_decimals", None)
                if get_dec:
                    if asyncio.iscoroutinefunction(get_dec):
                        szd2 = int(await get_dec(hl_sym))
                    else:
                        szd2 = int(get_dec(hl_sym))
                else:
                    szd2 = 4
            except Exception:
                szd2 = 4
            step2 = 10.0 ** (-max(0, int(szd2)))
            q_min_hl_2 = 0.0
            if px_hl > 0 and min_usd_hl > 0:
                raw_min2 = float(min_usd_hl) / float(px_hl)
                try:
                    q_min_hl_2 = getattr(self.hl, "_dec_ceil")(raw_min2, szd2)  # type: ignore
                except Exception:
                    q_min_hl_2 = math.ceil(raw_min2 / step2) * step2

            if bool(self._allow_aster_topup_to_match_hl) and q_min_hl_2 > 0.0 and (filled_as + 1e-12) < q_min_hl_2:
                delta = q_min_hl_2 - float(filled_as)
                try:
                    await self._notify_exec(f"[TOPUP AS] {sym} delta_base={delta:.8f}")
                except Exception:
                    pass
                try:
                    _ = await self.asr.place_order(as_sym, side_as, qty=float(delta), order_type="MARKET", reduce_only=False)
                    filled_as = float(q_min_hl_2)
                except Exception:
                    pass
        except Exception:
            pass

        # 2) HL open quantized + log
        q_hl = await self._quantize_hl_floor(hl_sym, filled_as)
        try:
            await self._notify_exec(f"[HL QTY] sym={hl_sym} szDecimals={szd2} step={step2:.8f} q_raw={float(filled_as):.8f} q_send={float(q_hl):.8f}")
        except Exception:
            pass

        min_usd_hl = float(((self.cfg.get("hyperliquid") or {}).get("min_qty_usd") or 0.0))
        px_hl = float(mid_hl or 0.0)
        if q_hl <= 0.0 or (min_usd_hl > 0 and px_hl > 0 and q_hl * px_hl < min_usd_hl):
            try:
                await self._notify_exec(f"[PARTIAL-REVERT] {sym} qty={abs(filled_as):.8f} side={'BUY' if side_as == 'SELL' else 'SELL'} reason=hl_below_min_notional")
            except Exception:
                pass
            try:
                await self.asr.place_order(
                    as_sym,
                    ("BUY" if side_as == "SELL" else "SELL"),
                    qty=abs(filled_as),
                    order_type="MARKET",
                    reduce_only=True,
                )
            except Exception:
                pass
            return ({"ok": False, "venue": "HYPERLIQ", "why": "hl_below_min_or_zero"}, res_as_open, {}, {}, False, True)

        res_hl_open = await self.hl.place_order(
            hl_sym,
            ("BUY" if side_sign > 0 else "SELL"),
            qty=float(q_hl),
            order_type="MARKET",
            price=None,
            slippage_bps=self.slip_bps,
            reduce_only=False,
        )
        if not self._resp_ok(res_hl_open):
            try:
                reason = self._extract_fail_reason(res_hl_open)
                await self._notify_exec(
                    f"[PARTIAL-REVERT] {sym} qty={abs(filled_as):.8f} side={'BUY' if side_as == 'SELL' else 'SELL'} reason=hl_open_failed:{reason}"
                )
            except Exception:
                pass
            try:
                await self.asr.place_order(
                    as_sym,
                    ("BUY" if side_as == "SELL" else "SELL"),
                    qty=abs(filled_as),
                    order_type="MARKET",
                    reduce_only=True,
                )
            except Exception:
                pass

        # 3) Close instant
        res_hl_close = await self.hl.place_order(
            hl_sym,
            ("SELL" if side_sign > 0 else "BUY"),
            qty=float(q_hl),
            order_type="MARKET",
            price=None,
            slippage_bps=self.slip_bps,
            reduce_only=True,
        )
        hl_close_ok = self._resp_ok(res_hl_close)
        res_as_close = await self.asr.place_order(
            as_sym,
            ("BUY" if side_as == "SELL" else "SELL"),
            qty=abs(filled_as),
            order_type="MARKET",
            reduce_only=True,
        )
        as_close_ok = self._resp_ok(res_as_close)

        # Enforce flat HL even if API says ok
        try:
            left = float(abs(await self._get_hl_pos_qty(hl_sym)))
            if left > 0.0:
                try:
                    await self._notify_exec(f"[POST-CLOSE HL LEFTOVER] {sym} hl_sym={hl_sym} qty≈{left:.8f}")
                except Exception:
                    pass
                await self._hard_flatten_hl(hl_sym)
        except Exception:
            pass

        # Fallback flatten
        if not hl_close_ok:
            try:
                hl_addr = (self.cfg.get("hyperliquid", {}) or {}).get("user_address") or getattr(self.hl, "_user_addr", "")
                if hl_addr:
                    deadline = time.time() + float(self.risk.get("flatten_timeout_s", 10.0))
                    retry_ms = float(self.risk.get("flatten_retry_ms", 800))
                    while time.time() < deadline:
                        r = await self.hl.flatten_best_effort(hl_addr, coins=[hl_sym], slippage_bps=self.slip_bps)
                        if r and r.get("ok", False):
                            break
                        await asyncio.sleep(retry_ms / 1000.0)
            except Exception:
                pass

        if not as_close_ok:
            try:
                await self._notify_exec(
                    f"[PARTIAL-REVERT] {sym} qty={abs(filled_as):.8f} side={'BUY' if side_as == 'SELL' else 'SELL'} reason=as_close_failed_flatten"
                )
            except Exception:
                pass
            try:
                deadline = time.time() + float(self.risk.get("flatten_timeout_s", 10.0))
                retry_ms = float(self.risk.get("flatten_retry_ms", 800))
                while time.time() < deadline:
                    rr = await self.asr.flatten_best_effort(symbols=[as_sym])
                    if rr and rr.get("ok", False):
                        break
                    await asyncio.sleep(retry_ms / 1000.0)
            except Exception:
                pass

        # Post-trade invariant
        try:
            if not await self._is_flat_both(sym):
                self._block_symbol(sym, reason="post_trade_not_flat")
                await self._notify_exec(f"[FLATTEN] post-trade invariant failed on {sym} -> attempting flatten both")
                try:
                    hl_addr = (self.cfg.get("hyperliquid", {}) or {}).get("user_address") or getattr(self.hl, "_user_addr", "")
                    if hl_addr:
                        await self.hl.flatten_best_effort(hl_addr, coins=[hl_sym], slippage_bps=self.slip_bps)
                except Exception:
                    pass
                try:
                    await self.asr.flatten_best_effort(symbols=[as_sym])
                except Exception:
                    pass
        except Exception:
            pass

        return (
            res_hl_open,
            res_as_open,
            res_hl_close,
            res_as_close,
            self._resp_ok(res_hl_open) and hl_close_ok,
            self._resp_ok(res_as_open) and as_close_ok,
        )
