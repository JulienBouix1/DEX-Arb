# -*- coding: utf-8 -*-
"""
Runner HL <-> Aster with integrated strategy, clean heartbeat, periodic equity recap, kill-switch.

- Discovery:
    * Hyperliquid: /info {type: meta} -> universe of coins.
    * Aster: /fapi/v1/exchangeInfo -> symbols TRADING.
    * Mapping by base (OPENUSDT -> OX, FET->ASI, etc.), with alias 1000x -> K.
    * Sanity filter: keep only pairs with consistent HL/AS mids
      (relative diff <= sanity_filter.rel_tol, default 0.45) with a max wait time
      configurable (risk.yaml).

- Heartbeat:
    * HL eq, AS eq, TOT eq.
    * Comptage: both / hl_only / as_only, puis affichage des paires oprationnelles tries
      par |edge| en bps (edge = HL_mid - AS_mid, fees ignores pour le HB).

- Notifier:
    * Every 5 heartbeat iterations: send message "[EQUITY] HL=.. AS=.. TOT=..".
    * If TOT decreases 3 times in a row on these recaps, kill-switch.

- Kill-switch:
    * 3 consecutive recaps with decreasing TOT -> attempt flat_all via core.arb_patch,
      then clean stop (stop loops, close venues).
    * If flat_all is not available or has different signature, don't fail: stop
      the runner anyway.

- Stratgie:
    * PerpPerpArb (strategies.perp_perp_arbitrage) fait du scalp pur: dual IOC, flatten
      immdiat des rsiduels sur les deux venues (no-carry).
    * Le runner ne place aucun ordre directement; il orchestre uniquement discovery,
      heartbeat, killswitch, lancement de tick().

Launch:
  python -m run.perp_perp --auto --hb-rows 40 --hb-interval 6.0

=============================================================================
FILE STRUCTURE (1800+ lines)
=============================================================================

SECTION 1: IMPORTS & LOGGING (Lines ~37-92)
SECTION 2: DISCOVERY - Fetch universe from venues (Lines ~95-280)
SECTION 3: MAPPING - Map symbols across venues (Lines ~286-372)
SECTION 4: HELPERS - Price formatting, edge calculation (Lines ~372-460)
SECTION 5: HEARTBEAT - Equity monitoring & display (Lines ~460-580)
SECTION 6: SANITY FILTER - Validate pair consistency (Lines ~580-710)
SECTION 7: MAIN - Bot initialization and event loops (Lines ~710+)
    - Config loading
    - Venue initialization
    - Strategy setup
    - Heartbeat loop
    - Trade loop
    - Discovery loop
    - Carry loop
    - Graceful shutdown

=============================================================================
"""
from __future__ import annotations

import asyncio
import argparse
import logging
import os
import time
import signal
import sys
from typing import Any, Dict, List, Optional, Tuple

# Load environment variables from .env file (must be done FIRST, before other imports)
try:
    from dotenv import load_dotenv
    load_dotenv()  # Loads .env from current directory or parent directories
except ImportError:
    pass  # dotenv not installed - will use system environment variables only

import aiohttp
import yaml

# Side-effect shim si prsent (HL orders patch)
try:
    import run.hl_inject  # noqa: F401
except Exception:
    pass

from venues.hyperliquid import Hyperliquid, SUB_RATE_LIMIT_HZ
from venues.aster import Aster
from venues.lighter import Lighter
from core.notifier import Notifier
from core.license import check_live_mode_access
from strategies.perp_perp_arbitrage import PerpPerpArb, KillSwitch
from strategies.carry_strategy import CarryStrategy
from core.position_manager import PositionManager
from core.dns_utils import get_connector
from core.preflight import PreflightCheck, print_preflight_report
from core.circuit_breaker import CircuitBreaker, CircuitOpenError


# ---------- Logging ----------


def _setup_logging() -> None:
    root = logging.getLogger()
    if not root.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)5s | %(name)s | %(message)s",
                "%H:%M:%S",
            )
        )
        root.addHandler(h)
    root.setLevel(logging.INFO)


# ---------- Discovery / Mapping ----------


async def _hl_universe(session: aiohttp.ClientSession, rest_url: str) -> List[str]:
    """
    Rcupre la liste des coins HL via /info {type: "meta"}.
    Retourne une liste de strings en UPPER (names).
    Includes internal retry logic (5 attempts).
    """
    url = rest_url.rstrip("/") + "/info"
    for attempt in range(5):
        try:
            async with session.post(url, json={"type": "meta"}) as r:
                if r.status != 200:
                    logging.getLogger("discovery").warning(
                        "HL meta returned HTTP %s (attempt %d/5)", r.status, attempt + 1
                    )
                    await asyncio.sleep(2 ** attempt)
                    continue
                j = await r.json()
                uni = j.get("universe") or []
                out: List[str] = []
                for it in uni:
                    try:
                        if isinstance(it, dict):
                            name = str(it.get("name") or "").upper()
                        else:
                            name = str(it or "").upper()
                        if name:
                            out.append(name)
                    except Exception:
                        continue
                return out
        except Exception as e:
            wait = 2 ** attempt
            logging.getLogger("discovery").warning("HL universe error (attempt %d/5): %r. Retrying in %ds...", attempt + 1, e, wait)
            await asyncio.sleep(wait)
    return []


async def _aster_symbols(session: aiohttp.ClientSession, rest_url: str) -> List[str]:
    """
    Rcupre tous les symbols TRADING ct Aster (Binance-like exchangeInfo).
    Includes internal retry logic (5 attempts).
    """
    url = rest_url.rstrip("/") + "/fapi/v1/exchangeInfo"
    for attempt in range(5):
        try:
            async with session.get(url) as r:
                if r.status != 200:
                    logging.getLogger("discovery").warning(
                        "Aster exchangeInfo returned HTTP %s (attempt %d/5)", r.status, attempt + 1
                    )
                    await asyncio.sleep(2 ** attempt)
                    continue
                j = await r.json()
                out: List[str] = []
                for it in (j.get("symbols") or []):
                    try:
                        sym = str(it.get("symbol") or "").upper()
                        status = str(it.get("status") or "TRADING").upper()
                        if sym and status == "TRADING":
                            out.append(sym)
                    except Exception:
                        continue
                return out
        except Exception as e:
            wait = 2 ** attempt
            logging.getLogger("discovery").warning("Aster symbols error (attempt %d/5): %r. Retrying in %ds...", attempt + 1, e, wait)
            await asyncio.sleep(wait)
    return []


async def _lighter_symbols(session: aiohttp.ClientSession, rest_url: str) -> List[str]:
    """
    Fetch Lighter symbols via /api/v1/orderBooks or /api/v1/exchangeInfo.
    """
    # Defensive sleep to avoid hitting quota if HL and Aster just finished
    await asyncio.sleep(1.0)
    url = rest_url.rstrip("/") + "/api/v1/orderBooks"
    for attempt in range(5):
        try:
            async with session.get(url) as r:
                if r.status != 200:
                    logging.getLogger("discovery").warning(
                        "Lighter symbols returned HTTP %s (attempt %d/5)", r.status, attempt + 1
                    )
                    await asyncio.sleep(2 ** attempt)
                    continue
                j = await r.json()
                out: List[str] = []
                # If list of objects with "symbol"
                if isinstance(j, list):
                    for row in j:
                        if isinstance(row, dict):
                             sym = row.get("symbol")
                             if sym: out.append(str(sym).upper())
                        elif isinstance(row, str):
                             out.append(row.upper())
                # If dict with "symbols"
                elif isinstance(j, dict):
                    syms = j.get("symbols") or j.get("order_books") or j.get("orderBooks") or []
                    for row in syms:
                        if isinstance(row, dict):
                             sym = row.get("symbol")
                             if sym: out.append(str(sym).upper())
                 
                logging.getLogger("discovery").info(f"[DISCOVERY] Lighter raw symbols fetched: {len(out)} (sample: {out[:5]})")
                return out
        except Exception as e:
            wait = 2 ** attempt
            logging.getLogger("discovery").warning("Lighter symbols error (attempt %d/5): %r. Retrying in %ds...", attempt + 1, e, wait)
            await asyncio.sleep(wait)
    return []


async def _aster_24h_volumes(session: aiohttp.ClientSession, rest_url: str) -> Dict[str, float]:
    """
    Fetch 24h quote volumes (USD) for all Aster symbols.
    Used to filter out low-liquidity pairs.
    """
    url = rest_url.rstrip("/") + "/fapi/v1/ticker/24hr"
    try:
        async with session.get(url) as r:
            if r.status != 200:
                logging.getLogger("discovery").warning("Aster 24h volumes returned HTTP %s", r.status)
                return {}
            j = await r.json()
            vols: Dict[str, float] = {}
            if isinstance(j, list):
                for row in j:
                    try:
                        sym = str(row.get("symbol") or "").upper()
                        if sym.endswith("USDT"):
                            vols[sym] = float(row.get("quoteVolume", "0"))
                    except Exception:
                        continue
            return vols
    except Exception as e:
        logging.getLogger("discovery").warning("Aster 24h volumes error: %r", e)
        return {}


async def _hl_24h_volumes(session: aiohttp.ClientSession, rest_url: str) -> Dict[str, float]:
    """
    Fetch 24h notional volumes (USD) for all HL coins.
    """
    url = rest_url.rstrip("/") + "/info"
    try:
        async with session.post(url, json={"type": "metaAndAssetCtxs"}) as r:
            if r.status != 200:
                logging.getLogger("discovery").warning("HL 24h volumes returned HTTP %s", r.status)
                return {}
            data = await r.json()
            if not isinstance(data, list) or len(data) != 2:
                return {}
            
            universe = data[0].get("universe", [])
            assetCtxs = data[1]
            vols: Dict[str, float] = {}
            for i, item in enumerate(universe):
                if i >= len(assetCtxs): break
                coin = str(item.get("name", "")).upper()
                ctx = assetCtxs[i]
                # dayNtlVlm is the 24h volume in USD
                v = ctx.get("dayNtlVlm")
                if coin and v is not None:
                    try: vols[coin] = float(v)
                    except Exception: continue
            return vols
    except Exception as e:
        logging.getLogger("discovery").warning("HL 24h volumes error: %r", e)
        return {}


_BASE_ALIAS_FOR_HL: Dict[str, str] = {
    # Known naming differences between Aster (Binance-style) and Hyperliquid
    "RENDER": "RNDR",       # Render Token
    "FET": "ASI",           # Fetch.ai -> ASI
    "OPEN": "OX",           # OpenDAO
    "NEIRO": "KNEIRO",      # Micro NEIRO
    "BEAMX": "BEAM",        # Beam X -> BEAM
    "AGIX": "ASI",          # SingularityNET -> ASI
    "OCEAN": "ASI",         # Ocean Protocol -> ASI (merged)
    "LUNA2": "LUNA",        # Terra 2.0
    "LUNC": "LUNC",         # Terra Classic (explicit)
    "SHIB1000": "KSHIB",    # 1000SHIB alternate
    "FLOKI1000": "KFLOKI",  # 1000FLOKI alternate
    "BONK1000": "KBONK",    # 1000BONK alternate
    "PEPE1000": "KPEPE",    # 1000PEPE alternate
    "BTT": "KBTT",          # BitTorrent (sometimes listed as 1000BTT)
}


def _normalize_base_for_hl(base: str) -> str:
    """
    Normalize Aster base to HL base:
    - 1000XXXX -> KXXXX
    - apply aliases (FET->ASI, etc.)
    """
    b = (base or "").upper()
    if not b:
        return b
    if b.startswith("1000"):
        b = "K" + b[4:]
    return _BASE_ALIAS_FOR_HL.get(b, b)


def _base_from_symbol(sym: str) -> str:
    """
    Extrait la base  partir d'un symbol style XXXUSDT / XXXUSD.
    """
    s = (sym or "").upper()
    for suf in ("USDT", "USD"):
        if s.endswith(suf):
            return s[: -len(suf)]
    return s


def _map_aster_to_hl(as_symbols: List[str], hl_coins: List[str]) -> Dict[str, str]:
    """
    Construit un mapping Aster symbol -> HL coin par base normalise.
    Ne filtre pas par volume: objectif = couvrir tout l'intersection.
    Applies blocklist from config/blocklist.yaml.
    """
    # Load blocked symbols from config
    blocked_symbols: set = set()
    try:
        blocklist_path = os.path.join("config", "blocklist.yaml")
        if os.path.exists(blocklist_path):
            with open(blocklist_path, "r", encoding="utf-8") as f:
                blocklist_cfg = yaml.safe_load(f) or {}
            blocked_symbols = set(blocklist_cfg.get("blocked_symbols", []))
    except Exception:
        pass
    
    hl_set = set(c.upper() for c in hl_coins)
    mapping: Dict[str, str] = {}
    for s in as_symbols:
        sym = s.upper()
        # Skip blocked symbols
        if sym in blocked_symbols:
            continue
        base = _normalize_base_for_hl(_base_from_symbol(s))
        if base in hl_set:
            mapping[sym] = base
    for s in as_symbols:
        sym = s.upper()
        # Skip blocked symbols
        if sym in blocked_symbols:
            continue
        base = _normalize_base_for_hl(_base_from_symbol(s))
        if base in hl_set:
            mapping[sym] = base
    return mapping


def _map_lighter_to_hl(lt_symbols: List[str], hl_coins: List[str]) -> Dict[str, str]:
    """
    Map Lighter symbol (e.g. ETH-USDC) to HL coin (ETH).
    """
    hl_set = set(c.upper() for c in hl_coins)
    mapping: Dict[str, str] = {}
    for s in lt_symbols:
        # Lighter format often "BASE-QUOTE" (ETH-USDC)
        # We try to extract BASE
        base = s.upper().split("-")[0].split("_")[0]
        base = _normalize_base_for_hl(base)
        if base in hl_set:
            mapping[s] = base
            
    logging.getLogger("discovery").info(f"[DISCOVERY] Lighter mapping: {len(mapping)} pairs mapped from {len(lt_symbols)} symbols (HL set: {len(hl_set)})")
    if not mapping and lt_symbols:
         logging.getLogger("discovery").warning(f"[DISCOVERY] Sample LT: {lt_symbols[:5]} | Sample HL: {list(hl_set)[:5]}")
    return mapping


# ---------- Equity recap / kill-switch ----------


class _Recap:
    __slots__ = ("last_tot", "neg_streak")

    def __init__(self) -> None:
        self.last_tot: Optional[float] = None
        self.neg_streak: int = 0


async def _try_flat_all(
    hl: Hyperliquid, asr: Aster, pairs_map: Dict[str, str]
) -> None:
    """
    Tente d'appeler core.arb_patch.flat_all avec les signatures les plus probables.
    Si rien n'est dispo ou que a choue, on ne fait rien (mais on stoppe quand mme le runner).
    """
    flat_fn = None
    try:
        # Cas o core/arb_patch/__init__.py expose flat_all
        from core import arb_patch as ap  # type: ignore

        if hasattr(ap, "flat_all"):
            flat_fn = ap.flat_all  # type: ignore[attr-defined]
    except Exception:
        pass

    if flat_fn is None:
        try:
            # Cas package: core/arb_patch/flat.py
            from core.arb_patch.flat import flat_all as flat_mod  # type: ignore

            flat_fn = flat_mod
        except Exception:
            flat_fn = None

    if flat_fn is None:
        logging.getLogger("kill").warning("flat_all not available; skipping flatten")
        return

    async def _call_with(*args: Any) -> bool:
        try:
            if asyncio.iscoroutinefunction(flat_fn):
                await flat_fn(*args)
            else:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, lambda: flat_fn(*args))
            return True
        except TypeError:
            # signature qui ne matche pas -> on tente la suivante
            return False
        except Exception as e:
            logging.getLogger("kill").warning("flat_all error: %r", e)
            return True  # on ne retente pas sur autre erreur

    # 1) flat_all(hl, asr, cfg-like)
    cfg_like = {"pairs_map": pairs_map}
    if await _call_with(hl, asr, cfg_like):
        return
    # 2) flat_all(hl, asr)
    if await _call_with(hl, asr):
        return
    # 3) flat_all()
    await _call_with()


# ---------- Heartbeat ----------


def _fmt_price(x: float) -> str:
    if x >= 1000:
        return f"{x:.1f}"
    if x >= 100:
        return f"{x:.2f}"
    if x >= 10:
        return f"{x:.4f}"
    if x >= 1:
        return f"{x:.4f}"
    return f"{x:.6f}"


def _edge_bps(ph: float, pa: float) -> float:
    mid = (ph + pa) / 2.0
    if mid <= 0:
        return 0.0
    # Returns Net Edge approx (Gross - 8bps fees) to match Strategy/CSV view
    raw = (ph - pa) / mid * 10000.0
    return raw - 8.0 if raw > 0 else raw + 8.0 # Subtract fees from direction of trade


async def _heartbeat_once(
    iter_no: int, hl: Hyperliquid, asr: Aster, lighter: Optional[Lighter], pairs_map: Dict[str, str], rows: int,
    lighter_map: Dict[str, str] = None, lighter_enabled: bool = False,
    circuit_breaker: Optional["CircuitBreaker"] = None
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], str]:
    hl_eq = None
    try:
        if circuit_breaker and not circuit_breaker.is_available("HL"):
            logging.getLogger("hb").warning("[HB] HL circuit OPEN - skipping equity call")
        else:
            hl_eq = await hl.equity()
            if circuit_breaker:
                circuit_breaker.record_success("HL")
    except Exception as e:
        logging.getLogger("hb").warning(f"HL Equity Error: {e}")
        if circuit_breaker:
            circuit_breaker.record_failure("HL", str(e))

    as_eq = None
    try:
        if circuit_breaker and not circuit_breaker.is_available("ASTER"):
            logging.getLogger("hb").warning("[HB] Aster circuit OPEN - skipping equity call")
        else:
            as_eq = await asr.equity()
            if circuit_breaker:
                circuit_breaker.record_success("ASTER")
    except Exception as e:
        logging.getLogger("hb").warning(f"Aster Equity Error: {e}")
        if circuit_breaker:
            circuit_breaker.record_failure("ASTER", str(e))

    lt_eq = None
    if lighter:
        try:
            if circuit_breaker and not circuit_breaker.is_available("LIGHTER"):
                logging.getLogger("hb").warning("[HB] Lighter circuit OPEN - skipping equity call")
            else:
                lt_eq = await lighter.equity()
                if circuit_breaker:
                    circuit_breaker.record_success("LIGHTER")
        except Exception as e:
            logging.getLogger("hb").warning(f"Lighter Equity Error: {e}")
            if circuit_breaker:
                circuit_breaker.record_failure("LIGHTER", str(e))
        
    tot = None
    # Loose sum: sum whatever is available
    # Strict total: only calculated if all required venues report valid equity
    tot: Optional[float] = None
    
    # Check strict readiness
    ready = True
    if hl_eq is None: ready = False
    if as_eq is None and (pairs_map or asr): ready = False # If AS is used/enabled
    if lighter_enabled and lt_eq is None: ready = False
    
    if ready:
         tot = (hl_eq or 0.0) + (as_eq or 0.0) + (lt_eq or 0.0)

    both = hl_only = as_only = 0
    opportunities: List[str] = []

    # Seuil pour afficher une paire dans le log (viter le spam)
    DISPLAY_EDGE_BPS = 5.0

    # HL <-> Aster
    for as_sym, hl_coin in pairs_map.items():
        b_hl = hl.get_bbo(hl_coin)
        b_as = asr.get_bbo(as_sym)
        if b_hl and b_as:
            both += 1
            ph = (b_hl[0] + b_hl[1]) * 0.5
            pa = (b_as[0] + b_as[1]) * 0.5
            if ph > 0 and pa > 0:
                edge = _edge_bps(ph, pa)
                if abs(edge) >= DISPLAY_EDGE_BPS:
                    opportunities.append(f"{as_sym}:{edge:.1f}bps")
        else:
            if b_hl and not b_as:
                hl_only += 1
            if b_as and not b_hl:
                as_only += 1
                
    # HL <-> Lighter (if enabled)
    lt_map_rev = {v: k for k, v in (lighter_map or {}).items()} # coin -> lt_sym
    if lighter and lighter_map:
        # AS <-> LT Mapping (via HL)
        hl_to_as = {v: k for k, v in pairs_map.items()}
        
        for lt_sym, hl_coin in lighter_map.items():
             b_lt = lighter.get_bbo(lt_sym)
             if not b_lt: continue
             
             pl = (b_lt[0] + b_lt[1]) * 0.5
             if pl <= 0: continue

             # 1. HL vs LT
             b_hl = hl.get_bbo(hl_coin)
             if b_hl:
                 ph = (b_hl[0] + b_hl[1]) * 0.5
                 if ph > 0:
                     edge = _edge_bps(ph, pl)
                     if abs(edge) >= DISPLAY_EDGE_BPS:
                         opportunities.append(f"LT-{lt_sym}:{edge:.1f}bps")
            
             # 2. AS vs LT
             as_sym = hl_to_as.get(hl_coin)
             if as_sym:
                 b_as = asr.get_bbo(as_sym)
                 if b_as:
                     pa = (b_as[0] + b_as[1]) * 0.5
                     if pa > 0:
                         edge = _edge_bps(pa, pl)
                         if abs(edge) >= DISPLAY_EDGE_BPS:
                             opportunities.append(f"AS-LT:{edge:.1f}bps")

    # Format compact
    # [HB] HL=123.4 AS=456.7 LT=10.0 TOT=590.1 | Pairs=12 | Opps=[FET:12bps, ...]
    hl_str = f"{hl_eq:.1f}" if hl_eq is not None else "?"
    as_str = f"{as_eq:.1f}" if as_eq is not None else "?"
    lt_str = f"{lt_eq:.1f}" if lt_eq is not None else "?"
    tot_str = f"{tot:.1f}" if tot is not None else "?"
    
    # If Lighter is enabled, show it in the log
    eq_part = f"HL={hl_str} AS={as_str}"
    if lighter:
        eq_part += f" LT={lt_str}"
    eq_part += f" TOT={tot_str}"
    
    opps_str = ", ".join(opportunities[:5])  # Max 5 top opps
    total_pairs = len(pairs_map)
    if lighter_map:
        total_pairs += len(lighter_map)
    
    msg = (
        f"[HB {iter_no}] {eq_part} | "
        f"Pairs={total_pairs} ({both}ok/{hl_only}hl/{as_only}as)"
    )
    if opps_str:
        msg += f" | Opps: {opps_str}"
        
    return hl_eq, as_eq, lt_eq, tot, msg


async def _sanity_filter_pairs(
    hl: Hyperliquid,
    asr: Aster,
    pairs_map: Dict[str, str],
    max_wait_s: float,
    rel_tol: float,
) -> Dict[str, str]:
    """
    Sanity filter on HL<->Aster mapping:
    - wait up to max_wait_s for books to arrive
    - keep pairs with consistent HL mid / AS mid (relative diff <= rel_tol)
    - if nothing passes, keep complete mapping (don't erase tutto).
    """
    if max_wait_s <= 0 or rel_tol <= 0:
        return pairs_map

    log_sanity = logging.getLogger("sanity")
    loop = asyncio.get_event_loop()
    start = loop.time()
    kept: Dict[str, str] = {}
    already_checked: set = set()
    
    # Load blocked symbols from config/blocklist.yaml
    blocked_symbols: set = set()
    try:
        blocklist_path = os.path.join("config", "blocklist.yaml")
        if os.path.exists(blocklist_path):
            with open(blocklist_path, "r", encoding="utf-8") as f:
                blocklist_cfg = yaml.safe_load(f) or {}
            blocked_symbols = set(blocklist_cfg.get("blocked_symbols", []))
            log_sanity.info(f"[SANITY] Loaded {len(blocked_symbols)} blocked symbols from config")
    except Exception as e:
        log_sanity.warning(f"[SANITY] Could not load blocklist.yaml: {e}")

    # Log initial status
    log_sanity.info(f"[SANITY] Starting filter for {len(pairs_map)} pairs, max_wait={max_wait_s}s, rel_tol={rel_tol}")

    check_interval = 2.0  # Check every 2 seconds
    last_log_time = start
    
    while (loop.time() - start) < max_wait_s:
        pending = 0
        newly_added = 0
        hl_missing = 0
        as_missing = 0
        price_mismatch = 0
        
        for as_sym, hl_coin in pairs_map.items():
            # BLACKLIST check: Load blocked symbols from config/blocklist.yaml
            # This is loaded once at startup and cached
            if as_sym in blocked_symbols:
                if as_sym not in already_checked:
                    log_sanity.warning(f"[SANITY] Blacklisted (config): {as_sym}")
                    already_checked.add(as_sym)
                continue

            # Skip already validated pairs
            if as_sym in kept:
                continue
                
            b_hl = hl.get_bbo(hl_coin)
            b_as = asr.get_bbo(as_sym)
            
            if not b_hl:
                log_sanity.debug(f"[SANITY] {as_sym}: HL missing BBO")
                hl_missing += 1
                pending += 1
                continue
            if not b_as:
                log_sanity.debug(f"[SANITY] {as_sym}: AS missing BBO")
                as_missing += 1
                pending += 1
                continue
                
            ph = (b_hl[0] + b_hl[1]) * 0.5
            pa = (b_as[0] + b_as[1]) * 0.5
            
            if ph <= 0 or pa <= 0:
                log_sanity.debug(f"[SANITY] {as_sym}: Zero price PH={ph} PA={pa}")
                pending += 1
                continue
                
            diff = abs(ph - pa) / max(ph, pa)
            if diff <= rel_tol:
                kept[as_sym] = hl_coin
                newly_added += 1
            else:
                log_sanity.warning(f"[SANITY] {as_sym}: Price mismatch diff={diff*100:.2f}% > tol={rel_tol*100:.2f}% (HL={ph} AS={pa})")
                price_mismatch += 1
        
        elapsed = loop.time() - start
        
        # Log progress every 10 seconds
        if (loop.time() - last_log_time) >= 10.0:
            log_sanity.info(
                f"[SANITY] @{elapsed:.0f}s: {len(kept)}/{len(pairs_map)} passed | "
                f"pending={pending} hl_missing={hl_missing} as_missing={as_missing} mismatch={price_mismatch}"
            )
            last_log_time = loop.time()
        
        # Exit early if we got most pairs
        if len(kept) >= len(pairs_map) * 0.8:
            log_sanity.info(f"[SANITY] Early exit: {len(kept)}/{len(pairs_map)} pairs validated (80%+ threshold)")
            break
            
        await asyncio.sleep(check_interval)

    # Final logging
    elapsed = loop.time() - start
    log_sanity.info(f"[SANITY] Final: {len(kept)}/{len(pairs_map)} pairs passed after {elapsed:.1f}s")

    if not kept:
        log_sanity.warning(
            "[SANITY] No pairs validated after %.1fs. Returning full mapping (optimistic mode).",
            max_wait_s,
        )
        return pairs_map

    return kept


# ---------- Main ----------


async def main() -> None:
    _setup_logging()
    ap = argparse.ArgumentParser()
    ap.add_argument("--auto", action="store_true")  # conserv pour compat future, mais toujours auto-discovery
    ap.add_argument("--hb-rows", type=int, default=40)
    ap.add_argument("--hb-interval", type=float, default=6.0)
    ap.add_argument("--config", default="config/venues.yaml")
    args = ap.parse_args()

    # Config venues
    with open(args.config, "r", encoding="utf-8") as f:
        venues_cfg = yaml.safe_load(f) or {}

    # Config risk (spare)
    risk_cfg: Dict[str, Any] = {}
    risk_path = os.path.join("config", "risk.yaml")
    if os.path.exists(risk_path):
        try:
            with open(risk_path, "r", encoding="utf-8") as f:
                risk_cfg = yaml.safe_load(f) or {}
        except Exception as e:
            logging.getLogger("config").warning("failed to load risk.yaml: %r", e)

    # Mode Detection (Reference to risk.yaml ONLY - venues.yaml mode is ignored)
    scalp_enabled = bool(risk_cfg.get("scalp_enabled", True))
    scalp_paper = bool(risk_cfg.get("paper_mode", True))
    carry_paper = bool(risk_cfg.get("carry", {}).get("paper_mode", True))

    # Log if scalp is disabled
    if not scalp_enabled:
        print("\n*** [INFO] SCALP STRATEGY DISABLED via scalp_enabled=false ***")

    # REMOTE ACCESS: Auto-start Cloudflare tunnel if enabled
    tunnel_enabled = bool(risk_cfg.get("remote_access", {}).get("enabled", False))
    tunnel_process = None
    tunnel_url = None  # Will store the public URL once tunnel is ready
    if tunnel_enabled:
        import shutil
        import subprocess
        import threading
        import re

        # Check PATH first, then common install locations
        cloudflared_path = shutil.which("cloudflared")
        if not cloudflared_path:
            # Check common locations
            common_paths = [
                os.path.expanduser("~\\cloudflared.exe"),  # User home (Windows)
                os.path.expanduser("~/cloudflared"),  # User home (Unix)
                "C:\\Program Files\\cloudflared\\cloudflared.exe",
                "/usr/local/bin/cloudflared",
            ]
            for p in common_paths:
                if os.path.exists(p):
                    cloudflared_path = p
                    break
        if cloudflared_path:
            dashboard_port = risk_cfg.get("remote_access", {}).get("dashboard_port", 8081)
            print(f"\n*** [TUNNEL] Starting Cloudflare tunnel for dashboard on port {dashboard_port} ***")
            print(f"*** [TUNNEL] Using cloudflared at: {cloudflared_path} ***")
            try:
                # Start cloudflared in background - route stderr to stdout for unified capture
                tunnel_process = subprocess.Popen(
                    [cloudflared_path, "tunnel", "--url", f"http://localhost:{dashboard_port}"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True
                )

                # Event to signal when URL is found
                tunnel_ready = threading.Event()

                def read_tunnel_output():
                    nonlocal tunnel_url
                    for line in tunnel_process.stdout:
                        line = line.strip()
                        # Look for the tunnel URL in the output
                        if "trycloudflare.com" in line or ".cloudflare" in line.lower():
                            print(f"[TUNNEL] {line}")
                            # Extract URL pattern
                            urls = re.findall(r'https://[a-zA-Z0-9\-]+\.trycloudflare\.com', line)
                            if urls:
                                tunnel_url = urls[0]
                                print(f"\n" + "=" * 60)
                                print(f"*** [TUNNEL] REMOTE DASHBOARD URL: {tunnel_url} ***")
                                print(f"=" * 60 + "\n")
                                tunnel_ready.set()
                        elif "error" in line.lower() or "failed" in line.lower():
                            print(f"[TUNNEL] ERROR: {line}")
                        elif "INF" in line and ("Starting" in line or "Registered" in line or "Connection" in line):
                            print(f"[TUNNEL] {line}")

                # Start reader thread
                reader_thread = threading.Thread(target=read_tunnel_output, daemon=True)
                reader_thread.start()

                # Wait up to 15 seconds for tunnel to be ready
                print("[TUNNEL] Waiting for tunnel to establish (max 15s)...")
                if tunnel_ready.wait(timeout=15.0):
                    print("[TUNNEL] Tunnel established successfully!")
                else:
                    print("[TUNNEL] Warning: Tunnel URL not detected in 15s, continuing anyway...")
                    print("[TUNNEL] Check terminal output for the URL once available")
            except Exception as e:
                print(f"[TUNNEL] Failed to start: {e}")
                import traceback
                traceback.print_exc()
        else:
            print("\n*** [TUNNEL] cloudflared not installed. Install with: winget install cloudflare.cloudflared ***")

    # LICENSE CHECK: Block live mode if no valid license
    if not scalp_paper or not carry_paper:
        license_ok, license_msg = check_live_mode_access()
        print(f"\n[LICENSE] {license_msg}")

        if not license_ok:
            print("[LICENSE] LIVE MODE BLOCKED - Forcing PAPER mode for all strategies.")
            print("[LICENSE] To enable live mode, generate a license key:")
            print("          python -m core.license your@email.com 365")
            scalp_paper = True
            carry_paper = True

    # Position Manager needs to be in LIVE mode if EITHER strategy is live
    # This ensures real positions from Carry (even if Scalp is paper) get proper handling
    live_mode = not scalp_paper or not carry_paper

    if scalp_paper:
        print("\n*** [DRY-RUN] Scalp Mode (risk.yaml) = Paper. No real scalp orders. ***")
    else:
        print("\n*** [LIVE] Scalp Mode (risk.yaml) = LIVE. REAL ORDERS ACTIVE. ***")

    # Log Position Manager mode explicitly
    print(f"\n*** [POS MGR] Mode = {'LIVE' if live_mode else 'PAPER'} (any strategy live = live) ***")

    if carry_paper:
        print("*** [DRY-RUN] Carry Mode (risk.yaml) = Paper. ***")
    else:
        print("*** [LIVE] Carry Mode (risk.yaml) = LIVE. ***")

    # Prominent LIVE TRADING banner when any strategy is live
    if live_mode:
        print("\n" + "=" * 60)
        print("  ⚠️  LIVE TRADING ACTIVE - REAL MONEY AT RISK  ⚠️")
        print("=" * 60)
        print(f"  Scalp: {'LIVE' if not scalp_paper else 'Paper'}")
        print(f"  Carry: {'LIVE' if not carry_paper else 'Paper'}")
        print("=" * 60 + "\n")

    # Session start timestamp for PnL tracking
    bot_start_ts = time.time()
    
    notifier = Notifier(venues_cfg)

    # Venues
    hl_cfg = venues_cfg.get("hyperliquid") or {}
    as_cfg = venues_cfg.get("aster") or {}
    
    hl_rest = str(hl_cfg.get("rest_url", ""))
    as_rest = str(as_cfg.get("rest_url", ""))

    hl = Hyperliquid(hl_cfg)
    asr = Aster(as_cfg)
    
    lt_cfg = venues_cfg.get("lighter")
    if not lt_cfg:
        lt_cfg = risk_cfg.get("lighter") or {}

    lighter = Lighter(lt_cfg)
    # Check enabled in venues.yaml first, then risk.yaml as fallback
    lighter_enabled = bool(lt_cfg.get("enabled", False)) or bool(risk_cfg.get("lighter", {}).get("enabled", False))

    # Start WS (start() si dispo, sinon connect())
    # STRICT MODE CHECK for Live Trading
    if live_mode:
        missing = []
        if not hl.check_credentials():
            missing.append("Hyperliquid")
        if not asr.check_credentials():
            missing.append("Aster")
        if lighter_enabled and not lighter.check_credentials():
            missing.append("Lighter")
            
        if missing:
            logging.getLogger("main").critical(
                f"[FATAL] LIVE MODE requires valid credentials for: {', '.join(missing)}. "
                "Please check venues.yaml and environment variables. Aborting."
            )
            sys.exit(1)

    for v in (hl, asr, lighter):
        if v is lighter and not lighter_enabled: 
            continue
        started = False
        for meth in ("start", "connect"):
            if hasattr(v, meth):
                fn = getattr(v, meth)
                try:
                    res = fn()
                    if asyncio.iscoroutine(res):
                        await res
                    started = True
                    break
                except TypeError:
                    try:
                        res = fn()
                        if asyncio.iscoroutine(res):
                            await res
                        started = True
                        break
                    except Exception:
                        pass
        if not started:
            raise RuntimeError(f"Venue missing start/connect: {v}")

    # =========================================================================
    # PRE-FLIGHT CHECKLIST
    # =========================================================================
    print("\nRunning pre-flight checks...")
    preflight = PreflightCheck(venues_cfg, risk_cfg)
    pf_passed, pf_results = await preflight.run_all(
        hl=hl,
        asr=asr,
        lighter=lighter if lighter_enabled else None
    )
    print_preflight_report(pf_results, pf_passed)

    if not pf_passed:
        # Allow continuing in paper mode even if some checks fail
        if not live_mode:
            logging.getLogger("preflight").warning(
                "[PREFLIGHT] Some checks failed but continuing in PAPER mode..."
            )
        else:
            logging.getLogger("preflight").critical(
                "[PREFLIGHT] Pre-flight checks failed in LIVE mode! Aborting."
            )
            sys.exit(1)

    # =========================================================================
    # CIRCUIT BREAKER (API Failure Protection)
    # =========================================================================
    def _on_circuit_open(venue: str) -> None:
        """Callback when a venue circuit breaker trips."""
        logging.getLogger("circuit").critical(
            f"[CIRCUIT BREAKER] {venue} circuit OPEN - too many consecutive failures!"
        )
        # Notify via Discord/webhook
        asyncio.create_task(notifier.notify(
            "CIRCUIT_BREAKER",
            f"⚠️ {venue} circuit breaker OPEN - API failures detected"
        ))

    def _on_circuit_close(venue: str) -> None:
        """Callback when a venue circuit breaker recovers."""
        logging.getLogger("circuit").info(
            f"[CIRCUIT BREAKER] {venue} circuit CLOSED - service recovered"
        )

    circuit_breaker = CircuitBreaker(
        failure_threshold=5,      # 5 consecutive failures to trip
        success_threshold=2,       # 2 successes to recover
        cooldown_seconds=60.0,     # 60s before testing recovery
        on_circuit_open=_on_circuit_open,
        on_circuit_close=_on_circuit_close,
    )
    logging.getLogger("boot").info("[BOOT] Circuit breaker initialized (threshold=5, cooldown=60s)")

    # Discovery (Internal retries are now inside _hl_universe / _aster_symbols)
    hl_uni = []
    as_syms = []
    try:
        async with aiohttp.ClientSession(connector=get_connector(), timeout=aiohttp.ClientTimeout(total=40)) as sess:
            # Helper functions handle 5 retries * exponential backoff (~31s max)
            hl_uni_task = asyncio.create_task(_hl_universe(sess, str(hl_cfg.get("rest_url", ""))))
            as_syms_task = asyncio.create_task(_aster_symbols(sess, str(as_cfg.get("rest_url", ""))))
            
            lt_syms_task = None
            if lighter_enabled:
                 lt_syms_task = asyncio.create_task(_lighter_symbols(sess, str(lt_cfg.get("rest_url", ""))))
            
            results = await asyncio.gather(hl_uni_task, as_syms_task, lt_syms_task) if lt_syms_task else await asyncio.gather(hl_uni_task, as_syms_task)
            
            hl_uni = results[0]
            as_syms = results[1]
            if lt_syms_task:
                lt_syms = results[2]
            else:
                lt_syms = []
    except Exception as e:
         logging.getLogger("discovery").error("[FATAL] Discovery crashed: %r", e)

    # Logging & Check
    logging.getLogger("discovery").info(
        "[DISCOVERY] HL coins=%d | Aster symbols=%d | Lighter symbols=%d", 
        len(hl_uni), len(as_syms), len(lt_syms)
    )
    if not hl_uni and not as_syms:
         logging.getLogger("discovery").error("[FATAL] Could not discover pairs after internal retries.")
         # Continue? logic raises below anyway if pairs_map empty
    pairs_map = _map_aster_to_hl(as_syms, hl_uni)
    if not pairs_map:
        raise RuntimeError("No overlapping HL<->Aster pairs found")
    logging.getLogger("discovery").info(
        "[DISCOVERY] initial HL<->Aster intersection = %d pairs", len(pairs_map)
    )
    
    lighter_map = {}
    if lighter_enabled:
        lighter_map = _map_lighter_to_hl(lt_syms, hl_uni)
        logging.getLogger("discovery").info(
            "[DISCOVERY] HL<->Lighter intersection = %d pairs", len(lighter_map)
        )
        # AS-LT Intersect Log (Startup)
        hl_coins_from_as = set(pairs_map.values())
        hl_coins_from_lt = set(lighter_map.values())
        as_lt_intersect_count = len(hl_coins_from_as.intersection(hl_coins_from_lt))
        logging.getLogger("discovery").info(f"[DISCOVERY] Aster<->Lighter intersection = {as_lt_intersect_count} pairs (via HL)")

    # Volume filter: exclude low-liquidity pairs
    min_vol = float(risk_cfg.get("min_volume_24h_usd", 0.0))
    if min_vol > 0:
        try:
            async with aiohttp.ClientSession(connector=get_connector(), timeout=aiohttp.ClientTimeout(total=20)) as sess:
                # Fetch all volumes in parallel
                logging.getLogger("discovery").info("[DISCOVERY] Fetching 24h volumes from HL and Aster...")
                v_aster_task = asyncio.create_task(_aster_24h_volumes(sess, str(as_cfg.get("rest_url", ""))))
                v_hl_task = asyncio.create_task(_hl_24h_volumes(sess, str(hl_cfg.get("rest_url", ""))))
                
                v_aster, v_hl = await asyncio.gather(v_aster_task, v_hl_task)
            
            # 1. Filter HL-Aster (Keep if EITHER side has volume >= min_vol)
            before_as = len(pairs_map)
            pairs_map = {s: c for s, c in pairs_map.items() if v_aster.get(s, 0) >= min_vol or v_hl.get(c, 0) >= min_vol}
            after_as = len(pairs_map)
            
            # 2. Filter HL-Lighter (Based on HL volume since LT volume is not easily available)
            before_lt = len(lighter_map)
            lighter_map = {s: c for s, c in lighter_map.items() if v_hl.get(c, 0) >= min_vol}
            after_lt = len(lighter_map)
            
            logging.getLogger("discovery").info(
                "[DISCOVERY] Multi-Venue Volume Filter (min=$%.1fM): "
                "HL-AS: %d -> %d (-%d) | "
                "HL-LT: %d -> %d (-%d)",
                min_vol/1e6, 
                before_as, after_as, before_as - after_as,
                before_lt, after_lt, before_lt - after_lt
            )
        except Exception as e:
            logging.getLogger("discovery").warning("[DISCOVERY] Volume filter skipped: %r", e)

    # Subscriptions (Aster throttle ici; HL throttle interne via SUB_RATE_LIMIT_HZ)
    async def _subscribe_all() -> None:
        delay = max(1.0 / max(SUB_RATE_LIMIT_HZ, 1.0), 0.05)
        for as_sym, hl_coin in pairs_map.items():
            try:
                await hl.subscribe_orderbook(hl_coin)
            except Exception:
                pass
            try:
                await asr.subscribe_orderbook(as_sym)
            except Exception:
                pass
            await asyncio.sleep(delay)
        
        # Subscribe to Lighter if enabled
        if lighter_enabled and lighter:
            logging.getLogger("discovery").info(f"[BOOT] Subscribing to {len(lighter_map)} Lighter pairs...")
            for lt_sym in lighter_map.keys():
                try:
                    await lighter.subscribe_orderbook(lt_sym)
                except Exception:
                    pass

    await _subscribe_all()

    # --- PROACTIVE VENUE SETUP (LEVERAGE & MARGIN) ---
    # Read leverage from config (carry.leverage) with fallback to 3
    default_leverage = int(risk_cfg.get("carry", {}).get("leverage", 3))
    logging.getLogger("boot").info(f"[BOOT] Setting up isolated leverage ({default_leverage}x) for all active pairs (Sequential + Adaptive Fallback)...")

    setup_ok = 0
    total_setup_calls = 0

    for as_sym, hl_coin in pairs_map.items():
        # Parallelize setup across different venues for the same symbol
        total_setup_calls += 1
        tasks = [hl.set_isolated_margin(hl_coin, default_leverage)]

        total_setup_calls += 1
        tasks.append(asr.set_isolated_margin(as_sym, default_leverage))

        lt_sym = None
        if lighter_enabled and lighter:
            for ls, hc in lighter_map.items():
                if hc == hl_coin:
                    lt_sym = ls
                    break
            if lt_sym:
                total_setup_calls += 1
                tasks.append(lighter.set_leverage(lt_sym, default_leverage))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Parse results and log errors
        idx = 0
        # 1. HL
        if isinstance(results[idx], Exception):
            logging.getLogger("boot").error(f"[BOOT] HL setup CRASHED for {hl_coin}: {results[idx]}")
        elif results[idx]:
            setup_ok += 1
        idx += 1
        
        # 2. Aster
        if isinstance(results[idx], Exception):
            logging.getLogger("boot").error(f"[BOOT] Aster setup CRASHED for {as_sym}: {results[idx]}")
        elif results[idx]:
            setup_ok += 1
        idx += 1
        
        # 3. Lighter (if added)
        if lt_sym:
            if isinstance(results[idx], Exception):
                logging.getLogger("boot").error(f"[BOOT] Lighter setup CRASHED for {lt_sym}: {results[idx]}")
            elif results[idx]:
                setup_ok += 1
            idx += 1
        
        # Small delay between symbols to prevent account-level nonce/rate issues
        await asyncio.sleep(0.5)

    logging.getLogger("boot").info(f"[BOOT] Venue setup complete: {setup_ok}/{total_setup_calls} successful calls")

    # Sanity filter sur le mapping
    print("DEBUG: Starting Sanity Filter...")
    try:
        sf_cfg = (risk_cfg.get("sanity_filter") or {}) if isinstance(risk_cfg, dict) else {}
        sf_wait = float(sf_cfg.get("max_wait_s", 30.0))
        sf_rel = float(sf_cfg.get("rel_tol", 0.45))
        pairs_map = await _sanity_filter_pairs(hl, asr, pairs_map, max_wait_s=sf_wait, rel_tol=sf_rel)
        print(f"DEBUG: Sanity Filter Done. Pairs: {len(pairs_map)}")
    except Exception as e:
        print(f"DEBUG: Sanity Filter CRASHED: {e}")
        traceback.print_exc()
        raise
    logging.getLogger("discovery").info(
        "[BOOT] after sanity filter: %d active pairs", len(pairs_map)
    )

    # Strategy
    from core.dashboard import Dashboard
    import webbrowser
    print("DEBUG: Starting Dashboard on port 8081...")
    dash = Dashboard(port=8081)
    try:
        await dash.start()
        print("DEBUG: Dashboard started.")
        # Set strategy modes and allocation on dashboard
        dash.set_strategy_info(
            scalp_mode="PAPER" if scalp_paper else "LIVE",
            carry_mode="PAPER" if carry_paper else "LIVE",
            scalp_alloc=float(risk_cfg.get("max_scalp_total_alloc_pct", 0.50)),
            carry_alloc=float(risk_cfg.get("carry", {}).get("max_carry_total_alloc_pct", 0.80))
        )
        # Initialize scalp paper equity from config
        scalp_paper_equity_init = float(risk_cfg.get("scalp_paper_equity", 1000.0))
        dash.update_scalp_paper_equity(scalp_paper_equity_init)
        try:
            # Open tunnel URL if available, otherwise localhost
            if tunnel_url:
                print(f"[DASHBOARD] Opening remote URL in browser: {tunnel_url}")
                webbrowser.open(tunnel_url)
            else:
                print("[DASHBOARD] Opening local URL in browser: http://localhost:8081")
                webbrowser.open("http://localhost:8081")
        except Exception:
            pass
    except Exception as e:
        print(f"DEBUG: Dashboard Start FAILED (Port busy?): {e}")
        # Don't crash, just continue without dashboard
        pass

    # Position Manager (Hedge Fund Grade)
    # Manages all open positions (Convergence Exits, Stop Loss)
    # Merged logic for both Scalp (Basis Exit) and Carry (Funding Exit) - for now focused on Basis
    print("DEBUG: Init PositionManager...")
    try:
        pos_mgr = PositionManager(hl, asr, risk_cfg, pairs_map, live_mode=live_mode, lighter=lighter if lighter_enabled else None, lighter_map=lighter_map)
        print("DEBUG: PositionManager Init OK")
    except Exception as e:
        print(f"DEBUG: PositionManager Init FAILED: {e}")
        import traceback
        traceback.print_exc()
        raise

    strat = PerpPerpArb(
        hl=hl,
        asr=asr,
        lighter=lighter if lighter_enabled else None,
        lighter_map=lighter_map,
        cfg={"symbol_map": {k: {"hyperliquid": v, "aster": k} for k, v in pairs_map.items()}},
        active_pairs=list(pairs_map.keys()),
        risk=risk_cfg,
        notifier=notifier,
        pnl_csv_path="logs/scalp/arb_exec.csv",
        live_mode=live_mode,
        dashboard=dash,  # Pass dashboard to strategy for trade updates
        pos_mgr=pos_mgr, # Inject PositionManager
    )

    # Link strategy to PositionManager for paper equity updates
    pos_mgr.strategy = strat
    # Carry Strategy (paper mode) - has its own pairs_map with lower volume threshold
    carry_cfg = risk_cfg.get("carry", {})
    carry_min_vol = float(carry_cfg.get("min_volume_24h_usd", 100000.0))
    
    # Create carry_pairs_map with lower volume filter
    carry_pairs_map = pairs_map.copy()
    carry_lighter_map = lighter_map.copy()
    
    if carry_min_vol < min_vol:
        try:
            async with aiohttp.ClientSession(connector=get_connector(), timeout=aiohttp.ClientTimeout(total=20)) as csess:
                logging.getLogger("carry").info("[DISCOVERY] Re-fetching volumes for lower Carry threshold...")
                v_as_full = await _aster_24h_volumes(csess, str(as_cfg.get("rest_url", "")))
                v_hl_full = await _hl_24h_volumes(csess, str(hl_cfg.get("rest_url", "")))
                
            # 1. Expand Aster-HL mapping at lower threshold (using OR logic like Scalp)
            full_map = _map_aster_to_hl(as_syms, hl_uni)
            carry_pairs_map = {s: c for s, c in full_map.items() if v_as_full.get(s, 0) >= carry_min_vol or v_hl_full.get(c, 0) >= carry_min_vol}
            
            # 2. Expand Lighter-HL mapping at lower threshold
            full_lt_map = _map_lighter_to_hl(lt_syms, hl_uni)
            carry_lighter_map = {s: c for s, c in full_lt_map.items() if v_hl_full.get(c, 0) >= carry_min_vol}
            
            logging.getLogger("carry").info(
                "[CARRY] Discovery Expanded: HL-AS=%d HL-LT=%d | Volume threshold: $%.0fK",
                len(carry_pairs_map), len(carry_lighter_map), carry_min_vol/1000
            )
        except Exception as e:
            logging.getLogger("carry").warning("[CARRY] Discovery expansion failed: %r", e)
    
    carry_strat = CarryStrategy(
        hl=hl,
        asr=asr,
        lighter=lighter if lighter_enabled else None,
        lighter_map=carry_lighter_map,  # Use carry-specific lighter map
        cfg={"venues": venues_cfg},  # Pass venues config for fee calculation
        pairs_map=carry_pairs_map,  # Use carry-specific pairs
        risk=risk_cfg,
        notifier=notifier,
    )
    
    # Subscribe to Carry-only pairs (those not in Scalp)
    carry_only = set(carry_pairs_map.keys()) - set(pairs_map.keys())
    if carry_only:
        logging.getLogger("carry").info(f"[CARRY] Subscribing to {len(carry_only)} Carry-only pairs...")
        for as_sym in carry_only:
            hl_coin = carry_pairs_map[as_sym]
            try:
                await hl.subscribe_orderbook(hl_coin)
                await asr.subscribe_orderbook(as_sym)
                # Also subscribe to Lighter if it exists for this coin
                if lighter_enabled and lighter:
                    # Logic to find lt_sym
                    lt_sym = None
                    for ls, hc in lighter_map.items():
                        if hc == hl_coin:
                            lt_sym = ls
                            break
                    if lt_sym:
                        await lighter.subscribe_orderbook(lt_sym)
            except Exception:
                pass
            await asyncio.sleep(0.1)
    
    logging.getLogger("carry").info("[CARRY] Initialized: %d pairs (%d Carry-only, %d shared)",
                                     len(carry_pairs_map), len(carry_only), len(carry_pairs_map) - len(carry_only))

    # Link CarryStrategy to PositionManager for entry lock checks
    pos_mgr.carry_strategy = carry_strat
    logging.getLogger("carry").info("[CARRY] Linked to PositionManager for entry lock protection")

    # Wire up dashboard callbacks for emergency controls
    async def _dashboard_close_all() -> dict:
        """Close all carry positions - called from dashboard button."""
        return await carry_strat.close_all_positions("DASHBOARD_CLOSE_ALL")

    async def _dashboard_emergency_stop() -> None:
        """Emergency stop - close positions and signal shutdown."""
        logging.getLogger("dashboard").warning("[EMERGENCY] Stop triggered from dashboard!")
        await carry_strat.close_all_positions("EMERGENCY_STOP")
        stop.set()  # Signal main loop to exit

    dash.set_close_positions_callback(_dashboard_close_all)
    dash.set_emergency_stop_callback(_dashboard_emergency_stop)
    logging.getLogger("dashboard").info("[DASHBOARD] Emergency callbacks configured")

    # Loops
    stop = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, stop.set)
        except Exception:
            pass

    recap = _Recap()
    iter_no = 0

    async def heartbeat_loop() -> None:
        nonlocal iter_no
        start_equity_captured = False
        while not stop.is_set():
            iter_no += 1
            # Pass Lighter instance and map if enabled
            lt_inst = lighter if lighter_enabled else None
            lt_map = lighter_map if lighter_enabled else {}
            
            hl_eq, as_eq, lt_eq, tot, text = await _heartbeat_once(
                iter_no, hl, asr, lt_inst, pairs_map, args.hb_rows, lighter_map=lt_map, lighter_enabled=lighter_enabled,
                circuit_breaker=circuit_breaker
            )

            # Equity display logic:
            # - Main equity (top bar): Always show REAL venue equity (for carry live)
            # - Scalp paper equity: Separate tracking for scalp paper mode
            display_tot = tot
            display_hl = hl_eq or 0.0
            display_as = as_eq or 0.0
            display_lt = lt_eq or 0.0

            # Update scalp paper equity separately (for Scalp tab)
            if scalp_paper:
                dash.update_scalp_paper_equity(strat.paper_equity)

            # Capture start equity on first valid reading (REAL equity)
            if display_tot is not None and not start_equity_captured:
                dash.state["start_equity"] = display_tot
                start_equity_captured = True
                logging.getLogger("hb").info(f"[HB S] Start equity captured: ${display_tot:.2f} (REAL)")

            # Update Dashboard Equity (always real venue equity)
            if display_tot is not None:
                dash.update_equity(display_hl, display_as, display_lt, display_tot)
            
            # Update Dashboard Pairs (combine Scalp + Carry pairs for complete view)
            # Load blocklist for filtering
            blocked_symbols_dash: set = set()
            try:
                blocklist_path = os.path.join("config", "blocklist.yaml")
                if os.path.exists(blocklist_path):
                    with open(blocklist_path, "r", encoding="utf-8") as f:
                        blocklist_cfg = yaml.safe_load(f) or {}
                    blocked_symbols_dash = set(blocklist_cfg.get("blocked_symbols", []))
            except Exception:
                pass
            
            pairs_data = []
            
            # CONSOLIDATED VIEW: Keyed by Aster symbol (primary), LT data consolidated
            # 1. Iterate over Aster pairs (AS -> HL mapping)
            as_to_hl = {**pairs_map, **carry_pairs_map}
            
            # Reverse lookup: HL coin -> LT symbol
            hl_to_lt = {v: k for k, v in lighter_map.items()} if lighter_map else {}
            
            # Strategy lookup sets
            scalp_set = set(pairs_map.keys())
            carry_set = set(carry_pairs_map.keys())
            
            # Track which HL coins we've already processed (to avoid LT duplicates)
            processed_hl_coins = set()
            
            for as_sym, hl_coin in as_to_hl.items():
                # Skip blocked symbols
                if as_sym.upper() in blocked_symbols_dash:
                    continue
                
                processed_hl_coins.add(hl_coin)
                
                # Get prices from all venues
                b_hl = hl.get_bbo(hl_coin)
                ph = (b_hl[0] + b_hl[1]) * 0.5 if b_hl else 0.0
                
                b_as = asr.get_bbo(as_sym)
                pa = (b_as[0] + b_as[1]) * 0.5 if b_as else 0.0
                
                # Get LT price (if same HL coin is on Lighter)
                lt_sym = hl_to_lt.get(hl_coin)
                b_lt = lighter.get_bbo(lt_sym) if (lighter and lt_sym) else None
                pl = (b_lt[0] + b_lt[1]) * 0.5 if b_lt else 0.0
                
                # Skip if no HL or AS price
                if ph <= 0 or pa <= 0:
                    continue
                
                # Calculate all 3 edges
                edge_hl_as = _edge_bps(ph, pa)
                edge_hl_lt = _edge_bps(ph, pl) if pl > 0 else 0.0
                edge_as_lt = _edge_bps(pa, pl) if pl > 0 else 0.0
                
                # Strategy flags
                in_scalp = as_sym in scalp_set
                in_carry = as_sym in carry_set
                
                # Best edge for sorting
                best_edge = max(abs(edge_hl_as), abs(edge_hl_lt), abs(edge_as_lt))
                
                pairs_data.append({
                    "symbol": as_sym,
                    "hl_coin": hl_coin,
                    "hl_price": ph,
                    "as_price": pa,
                    "lt_price": pl,
                    "edge_hl_as": edge_hl_as,
                    "edge_hl_lt": edge_hl_lt,
                    "edge_as_lt": edge_as_lt,
                    "edge_bps": best_edge,
                    "in_scalp": in_scalp,
                    "in_carry": in_carry
                })
            
            # 2. Add LT-only pairs (HL coins on Lighter but NOT on Aster)
            if lighter and lighter_map:
                for lt_sym, hl_coin in lighter_map.items():
                    if hl_coin in processed_hl_coins:
                        continue  # Already in an AS row
                    
                    b_hl = hl.get_bbo(hl_coin)
                    b_lt = lighter.get_bbo(lt_sym)
                    
                    ph = (b_hl[0] + b_hl[1]) * 0.5 if b_hl else 0.0
                    pl = (b_lt[0] + b_lt[1]) * 0.5 if b_lt else 0.0
                    
                    if ph <= 0 or pl <= 0:
                        continue
                    
                    edge_hl_lt = _edge_bps(ph, pl)
                    
                    pairs_data.append({
                        "symbol": f"LT-{lt_sym}",
                        "hl_coin": hl_coin,
                        "hl_price": ph,
                        "as_price": 0.0,
                        "lt_price": pl,
                        "edge_hl_as": 0.0,
                        "edge_hl_lt": edge_hl_lt,
                        "edge_as_lt": 0.0,
                        "edge_bps": abs(edge_hl_lt),
                        "in_scalp": False,
                        "in_carry": False
                    })
            
            # Sort by best edge (descending)
            pairs_data.sort(key=lambda x: abs(x["edge_bps"]), reverse=True)
            dash.update_pairs(pairs_data[:200])
            
            # Update pair lists for color coding (Scalp vs Carry)
            dash.update_pair_lists(
                list(pairs_map.keys()),
                list(carry_pairs_map.keys())
            )
            
            # Update Session PnL from real sources
            # Scalp: In paper mode, use paper_equity delta; in live mode, read from CSV
            scalp_pnl = 0.0
            if scalp_paper:
                # PAPER MODE: Calculate P&L from paper equity change
                scalp_pnl = strat.paper_equity - strat.paper_equity_start
            else:
                # LIVE MODE: Sum from real_pnl.csv (only entries from this session)
                try:
                    pnl_path = "logs/scalp/real_pnl.csv"
                    if os.path.exists(pnl_path):
                        with open(pnl_path, "r", encoding="utf-8") as f:
                            lines = f.readlines()[1:]  # Skip header
                            for line in lines:
                                parts = line.strip().split(",")
                                # CSV format: exit_ts(col0),symbol,...,pnl_reel_usd(col7),...
                                # Only count entries from THIS session (after bot_start_ts)
                                if len(parts) >= 8:
                                    try:
                                        entry_ts = float(parts[0])
                                        if entry_ts >= bot_start_ts:  # Only this session
                                            scalp_pnl += float(parts[7])
                                    except ValueError: pass
                except Exception:
                    pass
            # Carry: total paper equity (including unrealized) - starting equity
            start_carry = float(risk_cfg.get("carry", {}).get("paper_equity", 1000.0))
            current_carry = carry_strat.total_paper_equity if hasattr(carry_strat, 'total_paper_equity') else carry_strat.paper_equity
            carry_pnl = current_carry - start_carry
            dash.update_session_pnl(scalp_usd=scalp_pnl, carry_usd=carry_pnl)

            # Compact log - Only show scalp HB if scalp is enabled
            if scalp_enabled and iter_no % 1 == 0:  # Log every HB (10s) but compact
                logging.getLogger("hb").info(f"[HB S] {text}")

            # Equity recap + kill-switch every 5 iterations
            if iter_no % 5 == 0:
                if display_tot is not None and recap.last_tot is not None:
                    d = display_tot - recap.last_tot
                    if d < 0:
                        recap.neg_streak += 1
                    else:
                        recap.neg_streak = 0
                if display_tot is not None:
                    recap.last_tot = display_tot

                await notifier.notify(
                    "EQUITY",
                    f"[EQUITY] HL={display_hl:.1f} "
                    f"AS={display_as:.1f} "
                    f"LT={display_lt:.1f} "
                    f"TOT={display_tot:.1f} {'[PAPER]' if scalp_paper else '[LIVE]'}",
                )

            # --- Global Kill Switch (Max Drawdown) ---
            if start_equity_captured and display_tot is not None:
                start_eq = dash.state.get("start_equity")
                if start_eq and start_eq > 0:
                    dd_pct = (start_eq - display_tot) / start_eq * 100.0
                    max_dd = risk_cfg.get("global", {}).get("max_drawdown_pct", 5.0)

                    if dd_pct >= max_dd:
                        logging.getLogger("kill").critical(
                            f"GLOBAL KILL SWITCH: Drawdown {dd_pct:.2f}% >= Limit {max_dd:.2f}%! HALTING."
                        )
                        await notifier.notify(
                            "KILL_SWITCH",
                            f"Drawdown {dd_pct:.2f}% > {max_dd}%! STOPPING BOT."
                        )
                        await _try_flat_all(hl, asr, pairs_map)
                        stop.set()
                        break

            # NOTE: Legacy "5 consecutive negative recaps" check REMOVED
            # Kill-switch now relies ONLY on drawdown-based check above (max_drawdown_pct)
            # This prevents false triggers from normal equity fluctuations

            # --- Circuit Breaker Critical Check ---
            # If ALL venue circuits are open, halt the bot
            if circuit_breaker.all_open():
                logging.getLogger("circuit").critical(
                    "[CIRCUIT BREAKER] ALL CIRCUITS OPEN - All venues unreachable! HALTING."
                )
                await notifier.notify(
                    "CIRCUIT_BREAKER",
                    "🚨 CRITICAL: All venue circuits open - bot halting for safety"
                )
                stop.set()
                break

            await asyncio.sleep(args.hb_interval)

    async def trade_loop() -> None:
        last_pm_tick = 0.0
        while not stop.is_set():
            try:
                # 1. Scalp Search
                await strat.tick()
                
                # 2. Position Management (every ~1s)
                now = asyncio.get_event_loop().time()
                if now - last_pm_tick > 1.0:
                    await pos_mgr.tick()
                    last_pm_tick = now
                    
            except KillSwitch:
                logging.getLogger("trade").warning("KillSwitch raised by strategy")
                stop.set()
                break
            except Exception as e:
                logging.getLogger("trade").warning("tick error: %r", e)
            await asyncio.sleep(0.05)  # 50ms tick - 3x faster than before

    # Discovery Loop
    async def discovery_loop() -> None:
        while not stop.is_set():
            await asyncio.sleep(60)  # Wait 1min first
            if stop.is_set(): break
            
            try:
                logging.getLogger("discovery").info("[DISCOVERY] Starting periodic scan...")
                hl_uni = []
                as_syms = []
                lt_syms = []
                
                try:
                    async with aiohttp.ClientSession(connector=get_connector(), timeout=aiohttp.ClientTimeout(total=40)) as session:
                        hl_uni_task = asyncio.create_task(_hl_universe(session, hl_rest))
                        as_syms_task = asyncio.create_task(_aster_symbols(session, as_rest))
                        
                        # Fetch Lighter if enabled
                        if lighter_enabled:
                            lt_task = asyncio.create_task(_lighter_symbols(session, str(lt_cfg.get("rest_url", ""))))
                            hl_uni, as_syms, lt_syms = await asyncio.gather(hl_uni_task, as_syms_task, lt_task)
                        else:
                            hl_uni, as_syms = await asyncio.gather(hl_uni_task, as_syms_task)
                            
                except Exception as e:
                    logging.getLogger("discovery").warning("Periodic scan exception: %r", e)

                if not hl_uni or not as_syms:
                    # Lighter failure is non-critical if others work, but if everything fails...
                    if not lighter_enabled: # Critical if core fails
                        logging.getLogger("discovery").error("[DISCOVERY] Failed scan (internal retries exhausted).")
                        continue
                
                # --- Lighter Update ---
                if lighter_enabled and lt_syms:
                    new_lt_map = _map_lighter_to_hl(lt_syms, hl_uni)
                    # Update global map
                    lighter_map.update(new_lt_map)
                    if lighter:
                        for lt_s in new_lt_map.keys():
                            await lighter.subscribe_orderbook(lt_s)
                    logging.getLogger("discovery").info(f"[DISCOVERY] Lighter Map Size: {len(lighter_map)}")
                    
                # --- Aster/HL Mapping ---
                new_map: Dict[str, str] = {}
                for s in as_syms:
                    for base_alias, hl_equiv in _BASE_ALIAS_FOR_HL.items():
                        if s.startswith(base_alias) and s.endswith("USDT"):
                            # Logic alias
                            hl_candidate = hl_equiv
                            if hl_candidate in hl_uni:
                                new_map[s] = hl_candidate
                    # Logic standard - MUST extract base first like _map_aster_to_hl does
                    base = _normalize_base_for_hl(_base_from_symbol(s))
                    if base in hl_uni:
                        new_map[s] = base
                
                # Sanity check new pairs AND dropped pairs (reintegration)
                current_keys = set(pairs_map.keys())
                # All valid intersection pairs
                all_inter_keys = set(new_map.keys())
                
                # Missing = Intersection - Active (includes truly new AND previously dropped)
                missing_keys = all_inter_keys - current_keys
                
                candidates = {k: new_map[k] for k in missing_keys}
                
                # Volume filter: also apply to discovery candidates
                if min_vol > 0 and candidates:
                    try:
                        async with aiohttp.ClientSession(connector=get_connector(), timeout=aiohttp.ClientTimeout(total=20)) as vsess:
                            volumes = await _aster_24h_volumes(vsess, as_rest)
                        before_vol = len(candidates)
                        candidates = {s: c for s, c in candidates.items() if volumes.get(s, 0) >= min_vol}
                        if before_vol != len(candidates):
                            logging.getLogger("discovery").info(f"[DISCOVERY] Volume filter: {before_vol} -> {len(candidates)} candidates")
                    except Exception:
                        pass
                
                if candidates:
                    logging.getLogger("discovery").info(f"[DISCOVERY] Found {len(candidates)} candidates (new + dropped). Subscribing...")
                    # Subscribe first to get data (with throttling)
                    sub_count = 0
                    for as_sym, hl_coin in candidates.items():
                        await hl.subscribe_orderbook(hl_coin)
                        await asr.subscribe_orderbook(as_sym)
                        sub_count += 1
                        await asyncio.sleep(0.45)  # Throttle - give WS more time to process and receive BBO
                    logging.getLogger("discovery").info(f"[DISCOVERY] Subscribed to {sub_count} candidate pairs. Waiting {sf_wait}s for BBO data...")
                    
                    # Sanity filter (use config timeout)
                    validated = await _sanity_filter_pairs(hl, asr, candidates, max_wait_s=sf_wait, rel_tol=sf_rel)
                    
                    logging.getLogger("discovery").info(f"[DISCOVERY] Sanity filter result: {len(validated)}/{len(candidates)} passed")
                    
                    if validated:
                        # Update global maps (BOTH scalp and carry)
                        pairs_map.update(validated)
                        carry_pairs_map.update(validated)  # Keep carry in sync
                        # Push to strategy
                        strat.update_pairs(validated)
                        logging.getLogger("discovery").info(f"[DISCOVERY] +{len(validated)} pairs! Total now: {len(pairs_map)} (strat: {len(strat.active_pairs)})")
                    else:
                        logging.getLogger("discovery").warning(f"[DISCOVERY] 0 candidates passed sanity filter (BBO data missing?)")
                
                # These lines were moved out of the 'if validated' block and adjusted for correct placement
                # Push to strategy (this was already done for lighter_map above, but the diff implies it should be here)
                # The original diff had `strat.update_lighter_map(lighter_map)` here, but it's already done in the `if lighter_enabled` block.
                # Assuming the intent was to add the logging for intersections after all updates.
                # Update strategies with latest maps
                strat.update_mappings(pairs_map, lighter_map)
                carry_strat.update_mappings(carry_pairs_map, carry_lighter_map)  # Use carry-specific maps

                logging.getLogger("discovery").info(f"[DISCOVERY] HL<->Lighter intersection = {len(lighter_map)} pairs")
                
                # Check AS <-> LT intersection (via HL)
                # pairs_map is AS->HL. lighter_map is LT->HL.
                hl_coins_from_as = set(pairs_map.values())
                hl_coins_from_lt = set(lighter_map.values())
                as_lt_intersect_count = len(hl_coins_from_as.intersection(hl_coins_from_lt))
                logging.getLogger("discovery").info(f"[DISCOVERY] Aster<->Lighter intersection = {as_lt_intersect_count} pairs (via HL)")
                
                if not candidates: # This is the original `else` block for `if candidates:`
                    # Log current vs intersection for debugging
                    logging.getLogger("discovery").info(f"[DISCOVERY] No new candidates. Current={len(current_keys)} Intersection={len(all_inter_keys)} Missing={len(missing_keys)}")
                    
            except Exception as e:
                logging.getLogger("discovery").warning("Periodic discovery error: %r", e)

    async def carry_loop() -> None:
        """Carry strategy loop - checks funding rates periodically."""
        carry_iter = 0
        while not stop.is_set():
            try:
                await carry_strat.tick()
                carry_iter += 1
                
                # Update dashboard with carry positions (every tick)
                positions = carry_strat.get_positions_summary()
                dash.update_carry_positions(positions)

                # Update blocked pairs for dashboard
                blocked_pairs = carry_strat.get_blocked_pairs()
                dash.update_blocked_pairs(blocked_pairs)

                # Update carry stats (equity) - use REAL equity if LIVE mode
                stats = carry_strat.get_stats()
                if carry_paper:
                    carry_equity = stats.get("equity", 1000.0)  # Paper equity
                else:
                    # LIVE: Get real equity from venues
                    try:
                        real_eq = await carry_strat._get_total_equity()
                        carry_equity = real_eq if real_eq > 0 else stats.get("equity", 1000.0)
                    except:
                        carry_equity = stats.get("equity", 1000.0)
                dash.update_carry_stats(
                    entries=stats.get("entries", 0),
                    equity=carry_equity,
                    realized_pnl=stats.get("realized_pnl", 0.0),
                    exits=stats.get("exits", 0),
                    wins=stats.get("wins", 0),
                    losses=stats.get("losses", 0)
                )

                # Fetch and update funding rates for dashboard (every tick for real-time view)
                try:
                    hl_rates = await hl.get_funding_rates() or {}
                    as_rates = await asr.get_funding_rates() or {}
                    lt_rates = {}
                    if lighter_enabled and lighter:
                        lt_rates = await lighter.get_funding_rates() or {}

                    # Build funding rates list for dashboard
                    # Key by HL coin since that's our common reference
                    funding_data = []

                    # Reverse lookup maps
                    hl_to_as = {v: k for k, v in carry_pairs_map.items()}  # HL coin -> AS symbol
                    hl_to_lt = {v: k for k, v in carry_lighter_map.items()} if carry_lighter_map else {}  # HL coin -> LT symbol

                    # All unique HL coins from carry pairs
                    all_hl_coins = set(carry_pairs_map.values())
                    if carry_lighter_map:
                        all_hl_coins.update(carry_lighter_map.values())

                    for hl_coin in all_hl_coins:
                        as_sym = hl_to_as.get(hl_coin)
                        lt_sym = hl_to_lt.get(hl_coin)

                        # Get rates (in raw decimal form, e.g., 0.0001 = 1 bps)
                        hl_rate_raw = hl_rates.get(hl_coin, 0.0)
                        as_rate_raw = as_rates.get(as_sym, 0.0) if as_sym else 0.0
                        lt_rate_raw = lt_rates.get(lt_sym, 0.0) if lt_sym else 0.0

                        # Convert to bps (multiply by 10000)
                        hl_bps = hl_rate_raw * 10000
                        as_bps = as_rate_raw * 10000
                        lt_bps = lt_rate_raw * 10000

                        # Calculate best spread and direction
                        # Spread = receiver - payer (positive = opportunity to go long on payer, short on receiver)
                        spreads = []
                        if as_sym and as_rate_raw != 0:
                            spreads.append(("HL-AS", hl_bps - as_bps))
                        if lt_sym and lt_rate_raw != 0:
                            spreads.append(("HL-LT", hl_bps - lt_bps))
                        if as_sym and lt_sym and as_rate_raw != 0 and lt_rate_raw != 0:
                            spreads.append(("AS-LT", as_bps - lt_bps))

                        best_spread = 0.0
                        direction = "-"
                        if spreads:
                            # Find max absolute spread
                            best = max(spreads, key=lambda x: abs(x[1]))
                            best_spread = best[1]
                            if abs(best_spread) >= 10:  # Only show direction if significant
                                pair = best[0]
                                if best_spread > 0:
                                    # First venue pays more -> Long first, Short second
                                    direction = f"Long {pair.split('-')[1]} / Short {pair.split('-')[0]}"
                                else:
                                    direction = f"Long {pair.split('-')[0]} / Short {pair.split('-')[1]}"

                        funding_data.append({
                            "symbol": as_sym or f"LT-{lt_sym}" if lt_sym else hl_coin,
                            "hl_coin": hl_coin,
                            "hl_rate": hl_bps,
                            "as_rate": as_bps,
                            "lt_rate": lt_bps if lt_sym else None,
                            "spread": best_spread,
                            "direction": direction
                        })

                    # Sort by absolute spread (descending) - best opportunities first
                    funding_data.sort(key=lambda x: abs(x.get("spread", 0)), reverse=True)
                    dash.update_funding_rates(funding_data[:100])  # Top 100
                except Exception as e:
                    logging.getLogger("carry").debug(f"Funding rates update failed: {e}")

                # Carry HB every 5 iterations (5 min)
                if carry_iter % 5 == 0:
                    if positions:
                        pos_str = ", ".join([f"{p['symbol']}:{p['hold_hours']:.1f}h" for p in positions])
                        logging.getLogger("carry").info(f"[HB C {carry_iter}] Positions ({len(positions)}): {pos_str}")
                    else:
                        logging.getLogger("carry").info(f"[HB C {carry_iter}] No positions | Watching {len(carry_pairs_map)} pairs")
                        
            except Exception as e:
                logging.getLogger("carry").warning("carry tick error: %r", e)
            # Carry tick interval (configurable, default 2s when scalp disabled)
            carry_tick_s = float(risk_cfg.get("carry", {}).get("tick_interval_s", 5.0))
            await asyncio.sleep(carry_tick_s)

    hb = asyncio.create_task(heartbeat_loop(), name="hb_loop")
    tr = asyncio.create_task(trade_loop(), name="trade_loop")
    disco = asyncio.create_task(discovery_loop(), name="disco_loop")
    carry = asyncio.create_task(carry_loop(), name="carry_loop")

    await stop.wait()
    
    # ========== GRACEFUL SHUTDOWN ==========
    logging.getLogger("shutdown").warning("[SHUTDOWN] Signal received. Cleaning up...")
    
    # CRITICAL: Do NOT flatten positions automatically.
    # For Carry Strategy, we MUST persist positions across restarts.
    # Flattening here would destroy the strategy's value proposition.
    # If the user wants to flat, they should use a dedicated script or the dashboard.
    
    # 1. Save Carry State (Double Check)
    try:
        # Assuming carry_strat is available in scope (it is)
        if hasattr(carry_strat, "_save_state"):
             carry_strat._save_state()
             logging.getLogger("shutdown").info("[SHUTDOWN] Carry Strategy state saved.")
    except Exception as e:
        logging.getLogger("shutdown").error(f"[SHUTDOWN] Carry save error: {e}")

    # 2. Cancel open orders (Best Effort) to prevent fills while offline
    # (Implementation dependent on venue methods, skipping for now to reduce risk of API errors blocking exit)
    
    """
    # DISABLED FLATTENING LOGIC FOR CARRY SAFETY
    try:
        from run.flatten_cli import get_hl_positions, get_aster_positions, flatten_hl_position, flatten_aster_position
        # ... (flattening code removed/commented) ...
    except Exception as e:
        logging.getLogger("shutdown").error(f"[SHUTDOWN] Error during flatten: {e}")
    """
    # ========== END GRACEFUL SHUTDOWN ==========
    
    await dash.stop()
    for t in (tr, hb, disco, carry):
        try:
            t.cancel()
        except Exception:
            pass
    try:
        await hl.close()
    except Exception:
        pass
    try:
        await asr.close()
    except Exception:
        pass
    
    if lighter:
        try:
            await lighter.close()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
