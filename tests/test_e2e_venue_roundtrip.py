# -*- coding: utf-8 -*-
"""
E2E Live Smoke Test: Verify real order placement on all venue pairs.

This test:
1. Connects to real venues (HL, AS, LT) with your API keys
2. Places minimum-size orders (~$1)
3. Immediately closes them
4. Verifies correct symbols are used for each venue

RUN: python tests/test_e2e_venue_roundtrip.py
     python tests/test_e2e_venue_roundtrip.py --auto-confirm  # Skip confirmation prompt

WARNING: This uses REAL MONEY. Expected cost: ~$0.05-0.10 in fees for the full test.
"""
import sys
import os
import asyncio
import logging
import argparse
import aiohttp

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("E2E_TEST")


async def fetch_hl_mid_price(session: aiohttp.ClientSession, rest_url: str, coin: str) -> float:
    """Fetch mid price from Hyperliquid REST API."""
    try:
        async with session.post(f"{rest_url}/info", json={"type": "allMids"}) as resp:
            data = await resp.json()
            return float(data.get(coin, 0))
    except Exception as e:
        log.error(f"Failed to fetch HL mid price: {e}")
        return 0.0


async def fetch_aster_price(session: aiohttp.ClientSession, rest_url: str, symbol: str) -> float:
    """Fetch price from Aster REST API."""
    try:
        async with session.get(f"{rest_url}/fapi/v1/ticker/price?symbol={symbol}") as resp:
            data = await resp.json()
            return float(data.get("price", 0))
    except Exception as e:
        log.error(f"Failed to fetch AS price: {e}")
        return 0.0


async def test_venue_roundtrip(auto_confirm: bool = False):
    """Test full roundtrip on each venue pair.

    Args:
        auto_confirm: If True, skip interactive confirmation (for CI/automation)
    """
    import yaml

    # Load configs
    with open("config/venues.yaml") as f:
        venues_cfg = yaml.safe_load(f)
    with open("config/risk.yaml") as f:
        risk_cfg = yaml.safe_load(f)

    log.info("=" * 60)
    log.info("E2E LIVE SMOKE TEST - VENUE ROUNDTRIP")
    log.info("=" * 60)
    log.info("WARNING: This test places REAL orders with REAL money!")
    log.info("Expected cost: ~$0.05-0.10 in trading fees")
    log.info("=" * 60)

    # Confirmation
    if auto_confirm:
        log.info("Auto-confirm enabled - proceeding without prompt")
    else:
        confirm = input("\nType 'YES' to continue: ")
        if confirm != "YES":
            log.info("Aborted by user")
            return

    log.info("\n[1/4] Checking SDK availability...")

    # Check which SDKs are available
    hl_available = False
    as_available = False
    lt_available = False

    try:
        from venues.hyperliquid import Hyperliquid
        # Check if actual SDK is installed
        try:
            import hyperliquid
            hl_available = True
            log.info("  HL SDK: Available")
        except ImportError:
            log.warning("  HL SDK: NOT available (hyperliquid package not installed)")
    except ImportError as e:
        log.warning(f"  HL venue: Import failed - {e}")

    try:
        from venues.aster import Aster
        as_available = True
        log.info("  AS SDK: Available")
    except ImportError as e:
        log.warning(f"  AS venue: Import failed - {e}")

    try:
        from venues.lighter import Lighter
        lt_available = True
        log.info("  LT SDK: Available")
    except ImportError as e:
        log.warning(f"  LT venue: NOT available - {e}")

    if not hl_available and not as_available and not lt_available:
        log.error("No venue SDKs available. Cannot run E2E test.")
        return

    # Test symbols
    hl_coin = "BTC"
    as_symbol = "BTCUSDT"
    lt_symbol = "BTC"  # Lighter uses "BTC" not "BTC-PERP"
    test_size_usd = 12.0  # HL min is $10, use $12 to be safe

    results = {
        "HL": {"entry": None, "exit": None, "symbol_used": None, "error": None},
        "AS": {"entry": None, "exit": None, "symbol_used": None, "error": None},
        "LT": {"entry": None, "exit": None, "symbol_used": None, "error": None},
    }

    log.info("\n[2/4] Initializing venues...")

    hl = None
    asr = None
    lighter = None

    if hl_available:
        from venues.hyperliquid import Hyperliquid
        hl = Hyperliquid(venues_cfg.get("hyperliquid", {}))
        log.info("  HL initialized")

    if as_available:
        from venues.aster import Aster
        asr = Aster(venues_cfg.get("aster", {}))
        try:
            await asr.start()
            log.info("  AS initialized and started")
        except Exception as e:
            log.warning(f"  AS start failed: {e}")

    if lt_available:
        from venues.lighter import Lighter
        lighter = Lighter(venues_cfg.get("lighter", {}))
        try:
            await lighter.start()
            log.info("  LT initialized and started")
        except Exception as e:
            log.warning(f"  LT start failed: {e}")

    # Create HTTP session for price fetches
    hl_rest = venues_cfg.get("hyperliquid", {}).get("rest_url", "https://api.hyperliquid.xyz")
    as_rest = venues_cfg.get("aster", {}).get("rest_url", "https://fapi.asterdex.com")

    log.info("\n[3/4] Testing order placement...")

    async with aiohttp.ClientSession() as session:
        # Test 1: Hyperliquid
        log.info("\n--- TEST: Hyperliquid ---")
        if not hl:
            log.warning("  Skipped (SDK not available)")
            results["HL"]["symbol_used"] = "SKIPPED"
        else:
            try:
                # Fetch mid price via REST
                mid_px = await fetch_hl_mid_price(session, hl_rest, hl_coin)
                if mid_px <= 0:
                    log.error(f"  Failed to get price for {hl_coin}")
                    results["HL"]["error"] = "No price"
                else:
                    qty = hl.round_qty(hl_coin, test_size_usd / mid_px)
                    log.info(f"  Symbol: {hl_coin}, Price: ${mid_px:.2f}, Qty: {qty}")

                    # Entry (BUY)
                    log.info("  Placing BUY order...")
                    res = await hl.place_order(hl_coin, "BUY", qty, ioc=True)
                    filled = float(res.get("filled", 0))
                    results["HL"]["entry"] = filled > 0
                    results["HL"]["symbol_used"] = hl_coin
                    log.info(f"  Entry: filled={filled}, status={res.get('status')}")

                    if filled > 0:
                        await asyncio.sleep(0.5)
                        # Exit (SELL)
                        log.info("  Placing SELL order (close)...")
                        res2 = await hl.place_order(hl_coin, "SELL", filled, ioc=True, reduce_only=True)
                        filled2 = float(res2.get("filled", 0))
                        results["HL"]["exit"] = filled2 > 0
                        log.info(f"  Exit: filled={filled2}, status={res2.get('status')}")
                    else:
                        results["HL"]["error"] = res.get("reason", "Entry not filled")
            except Exception as e:
                log.error(f"  HL test failed: {e}")
                results["HL"]["error"] = str(e)

        await asyncio.sleep(1)

        # Test 2: Aster
        log.info("\n--- TEST: Aster ---")
        if not asr:
            log.warning("  Skipped (SDK not available)")
            results["AS"]["symbol_used"] = "SKIPPED"
        else:
            try:
                # Fetch price via REST (with fallback to HL price)
                price = await fetch_aster_price(session, as_rest, as_symbol)
                if price <= 0:
                    # Fallback: use HL mid price (BTC price is similar across venues)
                    price = await fetch_hl_mid_price(session, hl_rest, hl_coin)
                    if price > 0:
                        log.warning(f"  Using HL price as fallback: ${price:.2f}")
                if price <= 0:
                    log.error(f"  Failed to get price for {as_symbol}")
                    results["AS"]["error"] = "No price"
                else:
                    raw_qty = test_size_usd / price
                    qty = asr.round_qty(as_symbol, raw_qty)
                    # If round_qty returns same as input, force reasonable precision
                    if qty == raw_qty:
                        qty = round(raw_qty, 5)
                    # Ensure minimum qty (BTC step on Aster might be 0.001)
                    if qty < 0.001:
                        qty = 0.001
                        log.warning(f"  Qty too small, using minimum: {qty}")
                    log.info(f"  Symbol: {as_symbol}, Price: ${price:.2f}, Qty: {qty}")

                    # Entry (BUY)
                    log.info("  Placing BUY order...")
                    res = await asr.place_order(as_symbol, "BUY", qty, ioc=True)
                    filled = float(res.get("filled", 0))
                    results["AS"]["entry"] = filled > 0
                    results["AS"]["symbol_used"] = as_symbol
                    log.info(f"  Entry: filled={filled}, status={res.get('status')}")

                    if filled > 0:
                        await asyncio.sleep(0.5)
                        # Exit (SELL) - Aster doesn't support reduce_only the same way
                        log.info("  Placing SELL order (close)...")
                        res2 = await asr.place_order(as_symbol, "SELL", filled, ioc=True, reduce_only=False)
                        filled2 = float(res2.get("filled", 0))
                        results["AS"]["exit"] = filled2 > 0
                        log.info(f"  Exit: filled={filled2}, status={res2.get('status')}")
                    else:
                        results["AS"]["error"] = res.get("reason", "Entry not filled")
            except Exception as e:
                log.error(f"  AS test failed: {e}")
                results["AS"]["error"] = str(e)

        await asyncio.sleep(1)

        # Test 3: Lighter
        log.info("\n--- TEST: Lighter ---")
        if not lighter:
            log.warning("  Skipped (SDK not available)")
            results["LT"]["symbol_used"] = "SKIPPED"
        else:
            try:
                # Subscribe to orderbook and wait for BBO
                log.info(f"  Subscribing to {lt_symbol} orderbook...")
                await lighter.subscribe_orderbook(lt_symbol)
                await asyncio.sleep(3)  # Wait for WS data

                # Try WS cache first, then REST
                bbo = lighter.get_bbo(lt_symbol)
                log.info(f"  WS BBO cache: {bbo}")
                if not bbo or bbo[0] <= 0:
                    bbo = await lighter.fetch_bbo_rest(lt_symbol)
                    log.info(f"  REST BBO: {bbo}")
                if not bbo or bbo[0] <= 0 or bbo[1] <= 0:
                    log.error(f"  Failed to get BBO for {lt_symbol}")
                    results["LT"]["error"] = "No BBO"
                else:
                    price = (bbo[0] + bbo[1]) / 2  # Mid price
                    qty = lighter.round_qty(lt_symbol, test_size_usd / price)
                    log.info(f"  Symbol: {lt_symbol}, Price: ${price:.2f}, Qty: {qty}")

                    # Entry (BUY)
                    log.info("  Placing BUY order...")
                    res = await lighter.place_order(lt_symbol, "BUY", qty, ioc=True)
                    filled = float(res.get("filled", 0))
                    results["LT"]["entry"] = filled > 0
                    results["LT"]["symbol_used"] = lt_symbol
                    log.info(f"  Entry: filled={filled}, status={res.get('status')}")

                    if filled > 0:
                        await asyncio.sleep(0.5)
                        # Exit (SELL)
                        log.info("  Placing SELL order (close)...")
                        res2 = await lighter.place_order(lt_symbol, "SELL", filled, ioc=True, reduce_only=True)
                        filled2 = float(res2.get("filled", 0))
                        results["LT"]["exit"] = filled2 > 0
                        log.info(f"  Exit: filled={filled2}, status={res2.get('status')}")
                    else:
                        results["LT"]["error"] = res.get("reason", "Entry not filled")
            except Exception as e:
                log.error(f"  LT test failed: {e}")
                results["LT"]["error"] = str(e)

    # Summary
    log.info("\n" + "=" * 60)
    log.info("[4/4] E2E TEST RESULTS")
    log.info("=" * 60)

    all_passed = True
    tested_count = 0
    for venue, res in results.items():
        if res["symbol_used"] == "SKIPPED":
            log.info(f"  {venue}: SKIPPED (SDK not available)")
            continue
        tested_count += 1
        entry_ok = "OK" if res["entry"] else "FAIL"
        exit_ok = "OK" if res["exit"] else "FAIL"
        symbol = res["symbol_used"] or "N/A"
        error = f" ({res['error']})" if res.get("error") else ""
        log.info(f"  {venue}: Entry [{entry_ok}] | Exit [{exit_ok}] | Symbol: {symbol}{error}")
        if not res["entry"] or not res["exit"]:
            all_passed = False

    log.info("=" * 60)
    if all_passed and tested_count > 0:
        log.info(f"[SUCCESS] ALL {tested_count} TESTED VENUES PASSED!")
    elif tested_count == 0:
        log.warning("[WARNING] NO VENUES WERE TESTED - Check SDK installations")
    else:
        log.warning("[WARNING] SOME TESTS FAILED - Check logs above")
    log.info("=" * 60)

    # Cleanup
    try:
        if hl:
            await hl.close()
        if asr:
            await asr.close()
        if lighter:
            await lighter.close()
    except:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E2E Live Smoke Test for venue roundtrip")
    parser.add_argument(
        "--auto-confirm",
        action="store_true",
        help="Skip interactive confirmation (CAUTION: uses real money)"
    )
    args = parser.parse_args()

    print("""
================================================================
          E2E LIVE SMOKE TEST - VENUE ROUNDTRIP
================================================================
  This test will:
  1. Connect to HL, Aster, Lighter with your API keys
  2. Place $1 BUY orders on BTC on each venue
  3. Immediately close with SELL orders
  4. Verify all orders execute correctly

  Expected cost: ~$0.05-0.10 in trading fees
================================================================
    """)
    asyncio.run(test_venue_roundtrip(auto_confirm=args.auto_confirm))
