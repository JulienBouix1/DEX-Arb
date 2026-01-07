# -*- coding: utf-8 -*-
"""
E2E Live Smoke Test: Lighter ONLY (Single Order)

This test:
1. Connects to Lighter with your API keys
2. Places ONE minimum-size order (~$12)
3. Immediately closes it
4. Verifies correct execution

RUN: python tests/test_e2e_lighter_only.py
     python tests/test_e2e_lighter_only.py --auto-confirm  # Skip confirmation

WARNING: This uses REAL MONEY. Expected cost: ~$0.02-0.05 in fees.
"""
import sys
import os
import asyncio
import logging
import argparse

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("E2E_LIGHTER")


async def test_lighter_roundtrip(auto_confirm: bool = False):
    """Test single roundtrip on Lighter only."""
    import yaml

    # Load configs
    with open("config/venues.yaml") as f:
        venues_cfg = yaml.safe_load(f)
    with open("config/risk.yaml") as f:
        risk_cfg = yaml.safe_load(f)

    log.info("=" * 60)
    log.info("E2E LIGHTER-ONLY TEST - SINGLE ORDER")
    log.info("=" * 60)
    log.info("WARNING: This test places REAL orders with REAL money!")
    log.info("Expected cost: ~$0.02-0.05 in trading fees")
    log.info("=" * 60)

    # Confirmation
    if auto_confirm:
        log.info("Auto-confirm enabled - proceeding without prompt")
    else:
        confirm = input("\nType 'YES' to continue: ")
        if confirm != "YES":
            log.info("Aborted by user")
            return

    log.info("\n[1/5] Checking Lighter SDK availability...")

    try:
        from venues.lighter import Lighter
        log.info("  LT SDK: Available")
    except ImportError as e:
        log.error(f"  LT venue: Import failed - {e}")
        log.error("Cannot run test without Lighter SDK")
        return

    # Test symbol - use BTC as it's the most liquid
    lt_symbol = "BTC"
    test_size_usd = 12.0  # Minimum safe size

    result = {
        "entry": None,
        "exit": None,
        "symbol_used": None,
        "error": None,
        "entry_fill": 0.0,
        "exit_fill": 0.0,
        "entry_price": 0.0,
        "exit_price": 0.0,
    }

    log.info("\n[2/5] Initializing Lighter venue...")

    lt_cfg = venues_cfg.get("lighter") or risk_cfg.get("lighter") or {}
    lighter = Lighter(lt_cfg)

    try:
        await lighter.start()
        log.info("  LT initialized and started")
        log.info(f"  Account Index: {lighter.account_index}")
        log.info(f"  REST URL: {lighter.rest_url}")
    except Exception as e:
        log.error(f"  LT start failed: {e}")
        result["error"] = f"Start failed: {e}"
        return

    log.info("\n[3/5] Fetching BBO for BTC...")

    try:
        # Give WS time to connect (it starts in background)
        await asyncio.sleep(2)

        # Subscribe and wait for WS data
        await lighter.subscribe_orderbook(lt_symbol)
        await asyncio.sleep(3)  # Wait for WS data

        # Try WS cache first
        bbo = lighter.get_bbo(lt_symbol)
        log.info(f"  WS BBO cache: {bbo}")

        if not bbo or bbo[0] <= 0:
            bbo = await lighter.fetch_bbo_rest(lt_symbol)
            log.info(f"  REST BBO: {bbo}")

        if not bbo or bbo[0] <= 0 or bbo[1] <= 0:
            log.error(f"  Failed to get BBO for {lt_symbol}")
            result["error"] = "No BBO"
            await lighter.close()
            return

        price = (bbo[0] + bbo[1]) / 2  # Mid price
        qty = lighter.round_qty(lt_symbol, test_size_usd / price)
        log.info(f"  Symbol: {lt_symbol}")
        log.info(f"  Price: ${price:.2f}")
        log.info(f"  Qty: {qty}")
        log.info(f"  Notional: ${qty * price:.2f}")

    except Exception as e:
        log.error(f"  BBO fetch failed: {e}")
        result["error"] = f"BBO failed: {e}"
        await lighter.close()
        return

    log.info("\n[4/5] Placing orders...")

    try:
        # Entry (BUY)
        log.info("  Placing BUY order (IOC)...")
        res = await lighter.place_order(lt_symbol, "BUY", qty, ioc=True)
        filled = float(res.get("filled", 0))
        avg_price = float(res.get("avg_price", 0))
        status = res.get("status")

        result["entry"] = filled > 0
        result["symbol_used"] = lt_symbol
        result["entry_fill"] = filled
        result["entry_price"] = avg_price if avg_price > 0 else price

        log.info(f"  Entry Result:")
        log.info(f"    Status: {status}")
        log.info(f"    Filled: {filled}")
        log.info(f"    Avg Price: ${avg_price:.2f}" if avg_price > 0 else "    Avg Price: N/A")
        log.info(f"    Order ID: {res.get('order_id')}")

        if res.get("reason"):
            log.warning(f"    Reason: {res.get('reason')}")

        if filled > 0:
            await asyncio.sleep(1)  # Brief pause

            # Exit (SELL)
            log.info("  Placing SELL order (IOC, reduce_only)...")
            res2 = await lighter.place_order(lt_symbol, "SELL", filled, ioc=True, reduce_only=True)
            filled2 = float(res2.get("filled", 0))
            avg_price2 = float(res2.get("avg_price", 0))
            status2 = res2.get("status")

            result["exit"] = filled2 > 0
            result["exit_fill"] = filled2
            result["exit_price"] = avg_price2 if avg_price2 > 0 else price

            log.info(f"  Exit Result:")
            log.info(f"    Status: {status2}")
            log.info(f"    Filled: {filled2}")
            log.info(f"    Avg Price: ${avg_price2:.2f}" if avg_price2 > 0 else "    Avg Price: N/A")

            if res2.get("reason"):
                log.warning(f"    Reason: {res2.get('reason')}")
        else:
            result["error"] = res.get("reason", "Entry not filled")
            log.warning(f"  Entry failed, skipping exit")

    except Exception as e:
        log.error(f"  Order placement failed: {e}")
        result["error"] = str(e)
        import traceback
        traceback.print_exc()

    # Summary
    log.info("\n" + "=" * 60)
    log.info("[5/5] E2E LIGHTER TEST RESULTS")
    log.info("=" * 60)

    entry_ok = "OK" if result["entry"] else "FAIL"
    exit_ok = "OK" if result["exit"] else "FAIL"
    symbol = result["symbol_used"] or "N/A"
    error = f" ({result['error']})" if result.get("error") else ""

    log.info(f"  Symbol: {symbol}")
    log.info(f"  Entry: [{entry_ok}] - Filled {result['entry_fill']:.6f} @ ${result['entry_price']:.2f}")
    log.info(f"  Exit:  [{exit_ok}] - Filled {result['exit_fill']:.6f} @ ${result['exit_price']:.2f}")

    if result["entry"] and result["exit"]:
        # Calculate PnL
        pnl = (result['exit_price'] - result['entry_price']) * result['entry_fill']
        log.info(f"  Est. PnL: ${pnl:.4f} (before fees)")

    if error:
        log.warning(f"  Error: {error}")

    log.info("=" * 60)

    if result["entry"] and result["exit"]:
        log.info("[SUCCESS] LIGHTER TEST PASSED!")
    else:
        log.warning("[WARNING] TEST FAILED - Check logs above")

    log.info("=" * 60)

    # Cleanup
    try:
        await lighter.close()
    except:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E2E Lighter-Only Test (Single Order)")
    parser.add_argument(
        "--auto-confirm",
        action="store_true",
        help="Skip interactive confirmation (CAUTION: uses real money)"
    )
    args = parser.parse_args()

    print("""
================================================================
          E2E LIGHTER-ONLY TEST - SINGLE ORDER
================================================================
  This test will:
  1. Connect to Lighter with your API keys
  2. Place ONE $12 BUY order on BTC
  3. Immediately close with SELL order
  4. Verify execution

  Expected cost: ~$0.02-0.05 in trading fees
================================================================
    """)
    asyncio.run(test_lighter_roundtrip(auto_confirm=args.auto_confirm))
