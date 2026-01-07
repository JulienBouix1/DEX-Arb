# -*- coding: utf-8 -*-
"""
Wallet Position Sync Tool

Synchronize carry_positions.json with actual wallet positions from venues.
Use this when:
1. Running bot on a new device
2. After manual position changes
3. To verify/fix position tracking

USAGE:
    python tools/sync_positions.py           # Check only (dry-run)
    python tools/sync_positions.py --sync    # Actually sync (add orphans to tracking)
    python tools/sync_positions.py --clean   # Remove stale entries from JSON
"""
import sys
import os
import asyncio
import json
import logging
import argparse
import time

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("SYNC")


async def main(args):
    import yaml
    from venues.hyperliquid import Hyperliquid
    from venues.aster import Aster
    from strategies.carry_storage import CarryStorage, reconcile_wallet_positions

    # Load configs
    venues_cfg = {}
    risk_cfg = {}

    try:
        with open("config/venues.yaml") as f:
            venues_cfg = yaml.safe_load(f) or {}
    except Exception as e:
        log.warning(f"venues.yaml: {e}")

    try:
        with open("config/risk.yaml") as f:
            risk_cfg = yaml.safe_load(f) or {}
    except Exception as e:
        log.warning(f"risk.yaml: {e}")

    # Initialize venues
    hl_cfg = venues_cfg.get("hyperliquid") or {}
    as_cfg = venues_cfg.get("aster") or {}
    lt_cfg = venues_cfg.get("lighter") or risk_cfg.get("lighter") or {}

    hl = Hyperliquid(hl_cfg)
    asr = Aster(as_cfg)
    lighter = None

    lighter_enabled = bool(lt_cfg.get("enabled", True))
    if lighter_enabled:
        try:
            from venues.lighter import Lighter
            lighter = Lighter(lt_cfg)
        except ImportError:
            log.warning("Lighter SDK not available")

    print("\n" + "=" * 60)
    print("WALLET POSITION SYNC TOOL")
    print("=" * 60)

    # Start venues
    print("\n[1/4] Connecting to venues...")

    try:
        await hl.start()
        log.info("  HL: Connected")
    except Exception as e:
        log.error(f"  HL: {e}")

    try:
        await asr.start()
        log.info("  AS: Connected")
    except Exception as e:
        log.error(f"  AS: {e}")

    if lighter:
        try:
            await lighter.start()
            log.info("  LT: Connected")
        except Exception as e:
            log.warning(f"  LT: {e}")
            lighter = None

    # Wait for WS data
    print("\n[2/4] Waiting for venue data...")
    await asyncio.sleep(2)

    # Run reconciliation
    print("\n[3/4] Reconciling positions...")
    result = await reconcile_wallet_positions(hl, asr, lighter)

    # Display results
    print("\n" + "=" * 60)
    print("RECONCILIATION RESULTS")
    print("=" * 60)

    print(f"\nStored in JSON: {len(result['stored_positions'])}")
    for sym in result['stored_positions']:
        status = "MATCHED" if sym in result['matched'] else "STALE"
        color = "\033[92m" if status == "MATCHED" else "\033[91m"
        print(f"  {color}[{status}]\033[0m {sym}")

    print(f"\nWallet positions: {len(result['wallet_positions'])}")
    for pos in result['wallet_positions']:
        is_orphan = pos in result['orphans']
        color = "\033[93m" if is_orphan else "\033[92m"
        status = "ORPHAN" if is_orphan else "TRACKED"
        print(f"  {color}[{status}]\033[0m {pos}")

    print(f"\nSummary: {result['summary']}")

    # Take action based on flags
    storage = CarryStorage()
    storage_path = "data/carry_positions.json"

    if args.clean and result['stale']:
        print("\n[4/4] Cleaning stale entries...")

        try:
            with open(storage_path, "r") as f:
                data = json.load(f)

            removed = 0
            for sym in result['stale']:
                if sym in data:
                    del data[sym]
                    removed += 1
                    log.info(f"  Removed: {sym}")

            with open(storage_path, "w") as f:
                json.dump(data, f, indent=2)

            print(f"  Removed {removed} stale entries")

        except Exception as e:
            log.error(f"  Clean failed: {e}")

    elif args.sync and result['orphans']:
        print("\n[4/4] Adding orphan positions to tracking...")
        print("  WARNING: Orphan positions require manual entry data!")
        print("  Consider closing these positions manually instead.")

        for orphan in result['orphans']:
            print(f"  {orphan}: Cannot auto-add (missing entry price/funding data)")

        print("\n  To properly track these, either:")
        print("  1. Close them manually on the venue")
        print("  2. Edit data/carry_positions.json manually with entry data")

    else:
        print("\n[4/4] No action taken (use --sync or --clean)")

    # Cleanup
    try:
        await hl.close()
        await asr.close()
        if lighter:
            await lighter.close()
    except:
        pass

    print("\n" + "=" * 60)

    # Return status
    if result['stale'] or result['orphans']:
        print("STATUS: MISMATCH DETECTED")
        print("  - Stale entries in JSON that don't exist in wallet")
        print("  - Or orphan positions in wallet not tracked in JSON")
        return 1
    else:
        print("STATUS: SYNCED (All positions matched)")
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Wallet Position Sync Tool")
    parser.add_argument("--sync", action="store_true", help="Try to add orphan positions to tracking")
    parser.add_argument("--clean", action="store_true", help="Remove stale entries from JSON")
    args = parser.parse_args()

    exit_code = asyncio.run(main(args))
    sys.exit(exit_code)
