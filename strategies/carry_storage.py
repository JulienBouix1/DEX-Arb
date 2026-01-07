# -*- coding: utf-8 -*-
"""
Carry Storage: Persistent state management for Carry Strategy.
Handles saving/loading positions to disk to survive restarts.
"""
import json
import logging
import os
import shutil
import time
from typing import Dict, Any, List

log = logging.getLogger(__name__)

class CarryStorage:
    def __init__(self, filepath: str = "data/carry_positions.json"):
        self.filepath = filepath
        self._ensure_dir()

    def _ensure_dir(self):
        """Ensure the data directory exists."""
        d = os.path.dirname(self.filepath)
        if d:
            os.makedirs(d, exist_ok=True)

    def save_positions(self, positions_dict: Dict[str, Any]) -> bool:
        """
        Save positions to JSON file atomically.
        positions_dict: Dict[symbol, CarryPosition]
        """
        try:
            # Convert objects to dicts
            data = {}
            for sym, pos in positions_dict.items():
                if hasattr(pos, "__dict__"):
                    data[sym] = pos.__dict__
                else:
                    data[sym] = pos

            # Atomic write: write to temp file then rename
            temp_path = self.filepath + ".tmp"
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            
            shutil.move(temp_path, self.filepath)
            return True
        except Exception as e:
            log.error(f"[CARRY_STORAGE] Save failed: {e}")
            return False

    def load_positions(self) -> Dict[str, Dict[str, Any]]:
        """
        Load positions from JSON file.
        Returns dict of raw position data dictionaries (not objects).
        """
        if not os.path.exists(self.filepath):
            return {}

        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            # Handle new format with metadata
            if "_meta" in data:
                return {k: v for k, v in data.items() if k != "_meta"}
            return data
        except Exception as e:
            log.error(f"[CARRY_STORAGE] Load failed: {e}")
            return {}

    def save_state(self, positions_dict: Dict[str, Any], paper_equity: float = None,
                   blocklist: Dict[str, float] = None, reversal_tracker: Dict[str, List[float]] = None) -> bool:
        """
        Save full state including positions, paper_equity, and blocklist.

        Args:
            positions_dict: Dict[symbol, CarryPosition]
            paper_equity: Current paper equity
            blocklist: Dict[hl_coin, until_ts] - blocked pairs
            reversal_tracker: Dict[hl_coin, List[ts]] - reversal timestamps
        """
        try:
            # Convert position objects to dicts
            data = {
                "_meta": {
                    "paper_equity": paper_equity,
                    "saved_at": time.time(),
                    "blocklist": blocklist or {},
                    "reversal_tracker": reversal_tracker or {}
                }
            }
            for sym, pos in positions_dict.items():
                if hasattr(pos, "__dict__"):
                    data[sym] = pos.__dict__
                else:
                    data[sym] = pos

            # Atomic write
            temp_path = self.filepath + ".tmp"
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

            shutil.move(temp_path, self.filepath)
            return True
        except Exception as e:
            log.error(f"[CARRY_STORAGE] Save state failed: {e}")
            return False

    def load_paper_equity(self, default: float = 1000.0) -> float:
        """
        Load paper_equity from saved state.
        Returns default if not found or on error.
        """
        if not os.path.exists(self.filepath):
            return default

        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if "_meta" in data and data["_meta"].get("paper_equity") is not None:
                return float(data["_meta"]["paper_equity"])
            return default
        except Exception as e:
            log.error(f"[CARRY_STORAGE] Load paper_equity failed: {e}")
            return default

    def load_blocklist(self) -> Dict[str, float]:
        """
        Load blocklist from saved state.
        Returns dict of {hl_coin: until_ts} for currently blocked pairs.
        Expired entries are automatically filtered out.
        """
        if not os.path.exists(self.filepath):
            return {}

        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if "_meta" in data and "blocklist" in data["_meta"]:
                raw_blocklist = data["_meta"]["blocklist"]
                now = time.time()
                # Filter out expired entries
                return {k: v for k, v in raw_blocklist.items() if v > now}
            return {}
        except Exception as e:
            log.error(f"[CARRY_STORAGE] Load blocklist failed: {e}")
            return {}

    def load_reversal_tracker(self) -> Dict[str, List[float]]:
        """
        Load reversal tracker from saved state.
        Returns dict of {hl_coin: [ts1, ts2, ...]} for reversal timestamps.
        """
        if not os.path.exists(self.filepath):
            return {}

        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if "_meta" in data and "reversal_tracker" in data["_meta"]:
                return data["_meta"]["reversal_tracker"]
            return {}
        except Exception as e:
            log.error(f"[CARRY_STORAGE] Load reversal_tracker failed: {e}")
            return {}


async def reconcile_wallet_positions(hl, asr, lighter=None, storage_path: str = "data/carry_positions.json"):
    """
    Reconcile carry_positions.json with actual wallet positions from venues.

    This function is useful when:
    - Running bot on a new device
    - After manual position changes
    - To verify position tracking accuracy

    Args:
        hl: Hyperliquid venue adapter
        asr: Aster venue adapter
        lighter: Lighter venue adapter (optional)
        storage_path: Path to carry_positions.json

    Returns:
        dict with reconciliation results:
            - stored_positions: List of positions in JSON
            - wallet_positions: List of positions from venues
            - matched: List of matched positions
            - orphans: List of positions in wallet but not in JSON
            - stale: List of positions in JSON but not in wallet
    """
    import json as json_mod

    result = {
        "stored_positions": [],
        "wallet_positions": [],
        "matched": [],
        "orphans": [],
        "stale": [],
        "summary": ""
    }

    # Load stored positions
    stored = {}
    if os.path.exists(storage_path):
        try:
            with open(storage_path, "r", encoding="utf-8") as f:
                data = json_mod.load(f)
            stored = {k: v for k, v in data.items() if k != "_meta"}
            result["stored_positions"] = list(stored.keys())
        except Exception as e:
            log.warning(f"[RECONCILE] Failed to load storage: {e}")

    # Fetch wallet positions from each venue
    wallet_hl = {}
    wallet_as = {}
    wallet_lt = {}

    try:
        hl_positions = await hl.get_positions() if hasattr(hl, 'get_positions') else []
        for pos in hl_positions:
            sym = pos.get("symbol", "").upper()
            size = float(pos.get("size", 0))
            if abs(size) > 0:
                wallet_hl[sym] = {"size": size, "side": "LONG" if size > 0 else "SHORT", "venue": "HL"}
        log.info(f"[RECONCILE] HL wallet: {len(wallet_hl)} positions")
    except Exception as e:
        log.warning(f"[RECONCILE] HL fetch error: {e}")

    try:
        as_positions = await asr.get_positions() if hasattr(asr, 'get_positions') else []
        for pos in as_positions:
            sym = pos.get("symbol", "").upper()
            size = float(pos.get("positionAmt", pos.get("size", 0)))
            if abs(size) > 0:
                wallet_as[sym] = {"size": size, "side": "LONG" if size > 0 else "SHORT", "venue": "AS"}
        log.info(f"[RECONCILE] AS wallet: {len(wallet_as)} positions")
    except Exception as e:
        log.warning(f"[RECONCILE] AS fetch error: {e}")

    if lighter:
        try:
            lt_positions = await lighter.get_positions() if hasattr(lighter, 'get_positions') else []
            for pos in lt_positions:
                sym = pos.get("symbol", "").upper()
                size = float(pos.get("size", 0))
                if abs(size) > 0:
                    wallet_lt[sym] = {"size": size, "side": "LONG" if size > 0 else "SHORT", "venue": "LT"}
            log.info(f"[RECONCILE] LT wallet: {len(wallet_lt)} positions")
        except Exception as e:
            log.warning(f"[RECONCILE] LT fetch error: {e}")

    # Combine wallet positions
    all_wallet = {
        **{f"HL:{k}": v for k, v in wallet_hl.items()},
        **{f"AS:{k}": v for k, v in wallet_as.items()},
        **{f"LT:{k}": v for k, v in wallet_lt.items()}
    }
    result["wallet_positions"] = list(all_wallet.keys())

    # Match stored positions to wallet
    for sym, pos_data in stored.items():
        hl_coin = pos_data.get("hl_coin", "")
        direction = pos_data.get("direction", "")

        # Check if corresponding wallet positions exist
        has_hl = f"HL:{hl_coin}" in all_wallet
        has_as = f"AS:{sym}" in all_wallet
        has_lt = any(f"LT:{k}" in all_wallet for k in [hl_coin, sym.replace("USDT", "")])

        if has_hl or has_as or has_lt:
            result["matched"].append(sym)
        else:
            result["stale"].append(sym)
            log.warning(f"[RECONCILE] STALE: {sym} in JSON but no wallet positions found")

    # Find orphan positions (in wallet but not tracked)
    for wallet_key, wallet_data in all_wallet.items():
        venue, sym = wallet_key.split(":", 1)
        # Check if any stored position tracks this
        found = False
        for stored_sym, stored_data in stored.items():
            if venue == "HL" and stored_data.get("hl_coin") == sym:
                found = True
                break
            elif venue == "AS" and stored_sym == sym:
                found = True
                break
            elif venue == "LT" and stored_data.get("hl_coin") == sym:
                found = True
                break
        if not found:
            result["orphans"].append(wallet_key)
            log.warning(f"[RECONCILE] ORPHAN: {wallet_key} in wallet but not tracked in JSON")

    # Summary
    result["summary"] = (
        f"Stored: {len(stored)}, Wallet: {len(all_wallet)}, "
        f"Matched: {len(result['matched'])}, Stale: {len(result['stale'])}, Orphans: {len(result['orphans'])}"
    )
    log.info(f"[RECONCILE] {result['summary']}")

    return result
