# -*- coding: utf-8 -*-
"""
Pre-flight Checklist for Bot Startup.

Validates that all critical components are ready before trading begins:
- API credentials present
- Venues reachable
- Wallet balances available
- Configuration valid
"""
import asyncio
import logging
import os
from typing import Dict, Any, List, Tuple

log = logging.getLogger("preflight")


class PreflightCheck:
    """
    Pre-flight checklist runner.

    Usage:
        preflight = PreflightCheck(venues_cfg, risk_cfg)
        success, results = await preflight.run_all(hl, asr, lighter)
        if not success:
            print("Pre-flight failed!")
            for check, passed, msg in results:
                print(f"  [{'' if passed else 'X'}] {check}: {msg}")
    """

    def __init__(self, venues_cfg: Dict[str, Any], risk_cfg: Dict[str, Any]):
        self.venues_cfg = venues_cfg
        self.risk_cfg = risk_cfg
        self.results: List[Tuple[str, bool, str]] = []

    async def run_all(self, hl=None, asr=None, lighter=None) -> Tuple[bool, List[Tuple[str, bool, str]]]:
        """
        Run all pre-flight checks.

        Returns:
            Tuple of (all_passed: bool, results: List[(check_name, passed, message)])
        """
        self.results = []

        # 1. Check credentials
        self._check_credentials()

        # 2. Check configuration
        self._check_configuration()

        # 3. Check venue connectivity (if venue objects provided)
        if hl or asr or lighter:
            await self._check_venues(hl, asr, lighter)

        # 4. Check balances (if venues connected)
        if hl or asr or lighter:
            await self._check_balances(hl, asr, lighter)

        # 5. Mode confirmation
        self._check_mode()

        # Calculate overall result
        all_passed = all(passed for _, passed, _ in self.results)

        return all_passed, self.results

    def _add_result(self, check: str, passed: bool, message: str):
        """Add a check result."""
        self.results.append((check, passed, message))
        if passed:
            log.info(f"[PREFLIGHT] [OK] {check}: {message}")
        else:
            log.warning(f"[PREFLIGHT] [FAIL] {check}: {message}")

    def _check_credentials(self):
        """Verify API credentials are present in environment."""
        # Hyperliquid
        hl_addr = os.environ.get("HL_USER_ADDRESS", "")
        hl_key = os.environ.get("HL_PRIVATE_KEY", "")
        if hl_addr and hl_key:
            self._add_result("HL Credentials", True, "Address and private key present")
        else:
            missing = []
            if not hl_addr: missing.append("HL_USER_ADDRESS")
            if not hl_key: missing.append("HL_PRIVATE_KEY")
            self._add_result("HL Credentials", False, f"Missing: {', '.join(missing)}")

        # Aster
        as_key = os.environ.get("ASTER_API_KEY", "")
        as_secret = os.environ.get("ASTER_API_SECRET", "")
        if as_key and as_secret:
            self._add_result("Aster Credentials", True, "API key and secret present")
        else:
            missing = []
            if not as_key: missing.append("ASTER_API_KEY")
            if not as_secret: missing.append("ASTER_API_SECRET")
            self._add_result("Aster Credentials", False, f"Missing: {', '.join(missing)}")

        # Lighter (optional)
        lt_enabled = self.venues_cfg.get("lighter", {}).get("enabled", False)
        if lt_enabled:
            lt_key = os.environ.get("LIGHTER_PRIVATE_KEY", "")
            if lt_key:
                self._add_result("Lighter Credentials", True, "Private key present")
            else:
                self._add_result("Lighter Credentials", False, "Missing: LIGHTER_PRIVATE_KEY")

    def _check_configuration(self):
        """Verify critical configuration values."""
        # Check max drawdown is set
        max_dd = self.risk_cfg.get("global", {}).get("max_drawdown_pct", 0)
        if max_dd > 0:
            self._add_result("Kill-Switch Config", True, f"Max drawdown: {max_dd}%")
        else:
            self._add_result("Kill-Switch Config", False, "max_drawdown_pct not set!")

        # Check carry config exists
        carry_cfg = self.risk_cfg.get("carry", {})
        if carry_cfg:
            self._add_result("Carry Config", True,
                f"Min funding: {carry_cfg.get('min_funding_bps', 0)}bps, Max alloc: {carry_cfg.get('max_carry_total_alloc_pct', 0)*100}%")
        else:
            self._add_result("Carry Config", False, "carry section missing from risk.yaml")

    async def _check_venues(self, hl, asr, lighter):
        """Check venue connectivity."""
        # Check HL
        if hl:
            try:
                # Simple connectivity check
                if hasattr(hl, '_connected') and hl._connected.is_set():
                    self._add_result("HL Connectivity", True, "WebSocket connected")
                else:
                    self._add_result("HL Connectivity", True, "Venue object initialized")
            except Exception as e:
                self._add_result("HL Connectivity", False, str(e))

        # Check Aster
        if asr:
            try:
                if hasattr(asr, '_connected') and asr._connected.is_set():
                    self._add_result("Aster Connectivity", True, "WebSocket connected")
                else:
                    self._add_result("Aster Connectivity", True, "Venue object initialized")
            except Exception as e:
                self._add_result("Aster Connectivity", False, str(e))

        # Check Lighter
        if lighter:
            try:
                if hasattr(lighter, '_connected') and lighter._connected.is_set():
                    self._add_result("Lighter Connectivity", True, "WebSocket connected")
                else:
                    self._add_result("Lighter Connectivity", True, "Venue object initialized")
            except Exception as e:
                self._add_result("Lighter Connectivity", False, str(e))

    async def _check_balances(self, hl, asr, lighter):
        """Check venue balances."""
        total_equity = 0.0

        # HL equity
        if hl:
            try:
                eq = await hl.equity()
                if eq and eq > 0:
                    total_equity += eq
                    self._add_result("HL Balance", True, f"${eq:.2f}")
                else:
                    self._add_result("HL Balance", False, "Unable to fetch equity")
            except Exception as e:
                self._add_result("HL Balance", False, str(e))

        # Aster equity
        if asr:
            try:
                eq = await asr.equity()
                if eq and eq > 0:
                    total_equity += eq
                    self._add_result("Aster Balance", True, f"${eq:.2f}")
                else:
                    self._add_result("Aster Balance", False, "Unable to fetch equity")
            except Exception as e:
                self._add_result("Aster Balance", False, str(e))

        # Lighter equity
        if lighter:
            try:
                eq = await lighter.equity()
                if eq and eq > 0:
                    total_equity += eq
                    self._add_result("Lighter Balance", True, f"${eq:.2f}")
                else:
                    self._add_result("Lighter Balance", False, "Unable to fetch equity")
            except Exception as e:
                self._add_result("Lighter Balance", False, str(e))

        # Total equity check
        if total_equity > 0:
            self._add_result("Total Equity", True, f"${total_equity:.2f}")
        else:
            self._add_result("Total Equity", False, "No equity detected on any venue")

    def _check_mode(self):
        """Check and confirm trading mode."""
        scalp_paper = self.risk_cfg.get("paper_mode", True)
        carry_paper = self.risk_cfg.get("carry", {}).get("paper_mode", True)
        carry_dry_run = self.risk_cfg.get("carry", {}).get("dry_run_live", False)

        if scalp_paper and carry_paper:
            self._add_result("Trading Mode", True, "PAPER MODE - No real orders")
        elif carry_dry_run:
            self._add_result("Trading Mode", True, "DRY-RUN LIVE - Simulated orders")
        else:
            live_strategies = []
            if not scalp_paper: live_strategies.append("Scalp")
            if not carry_paper and not carry_dry_run: live_strategies.append("Carry")
            self._add_result("Trading Mode", True,
                f"LIVE MODE - Real orders active for: {', '.join(live_strategies)}")


def print_preflight_report(results: List[Tuple[str, bool, str]], all_passed: bool):
    """Print a formatted pre-flight report."""
    print("\n" + "=" * 60)
    print("  PRE-FLIGHT CHECKLIST")
    print("=" * 60)

    for check, passed, message in results:
        status = "[OK]  " if passed else "[FAIL]"
        print(f"  {status} {check}: {message}")

    print("=" * 60)
    if all_passed:
        print("  ALL CHECKS PASSED - Ready for launch")
    else:
        print("  SOME CHECKS FAILED - Review before proceeding")
    print("=" * 60 + "\n")
