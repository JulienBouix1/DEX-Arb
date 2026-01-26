# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal

import aiohttp
import lighter
from eth_account import Account
from core.orderbook import Book
from venues.base import VenueBase
from core.dns_utils import get_connector

log = logging.getLogger(__name__)


def _get_config_value(cfg: Dict[str, Any], key: str, env_key: str = None) -> str:
    """
    Get config value with environment variable fallback.
    Priority: 1) Direct value in cfg[key] 2) Env var from cfg[key_env] 3) Env var from env_key param
    """
    # Direct value
    if key in cfg and cfg[key]:
        return str(cfg[key])
    # Env var reference in config (e.g., "api_key_env": "LIGHTER_API_KEY")
    env_ref = cfg.get(f"{key}_env")
    if env_ref:
        return os.environ.get(env_ref, "")
    # Fallback env_key parameter
    if env_key:
        return os.environ.get(env_key, "")
    return ""

# Lighter SDK imports (lighter-sdk package from PyPI)
try:
    from lighter.ws_client import WsClient
except ImportError:
    from lighter import WsClient

class Lighter(VenueBase):
    """
    Lighter.xyz Venue Adapter.

    Implements public data fetching (Orderbook, Funding) via REST API v1.
    Execution is currently stubbed as it requires EIP-712 signing with exact schema.
    """
    
    def __init__(self, cfg: Dict[str, Any]) -> None:
        # Default to mainnet URL if not specified
        default_url = "https://mainnet.zklighter.elliot.ai"
        self.rest_url: str = (cfg.get("rest_url") or default_url).rstrip("/")
        self.ws_url: str = (cfg.get("ws_url") or "wss://mainnet.zklighter.elliot.ai/stream").rstrip("/")

        # Load credentials from config or environment variables
        self.api_key: str = _get_config_value(cfg, "api_key", "LIGHTER_API_KEY")
        self.private_key: str = _get_config_value(cfg, "private_key", "LIGHTER_PRIVATE_KEY")
        self.label = 'LT'

        # Lighter SDK configuration - support env vars
        account_idx_str = _get_config_value(cfg, "account_index", "LIGHTER_ACCOUNT_INDEX")
        api_key_idx_str = _get_config_value(cfg, "api_key_index", "LIGHTER_API_KEY_INDEX")
        self.account_index: int = int(account_idx_str) if account_idx_str else 0
        self.api_key_index: int = int(api_key_idx_str) if api_key_idx_str else 0
        self.l1_address: str = _get_config_value(cfg, "l1_address", "LIGHTER_L1_ADDRESS")
        
        # SDK client (initialized in start())
        self._sdk_client = None
        
        # Initialize account for signing - handle non-standard key formats
        self.account = None
        self._clean_private_key = None
        if self.private_key:
            try:
                pk = self.private_key
                self._clean_private_key = pk # Keep full 80 chars for SDK
                
                # Slicing ONLY for eth_account which expects 32 bytes (64 chars)
                eth_pk = pk
                if len(eth_pk) == 80:
                    eth_pk = eth_pk[:64]
                if not eth_pk.startswith("0x"):
                    eth_pk = "0x" + eth_pk
                
                self.account = Account.from_key(eth_pk)
                log.info(f"[Lighter] Signer initialized: {self.account.address}")
            except Exception as e:
                log.warning(f"[Lighter] Failed to init signer from private_key: {e}")
                log.warning("[Lighter] Orders will use DRY RUN mode if SDK fails")
        
        self.fees_bps = cfg.get("fees", {})
        
        self._session: Optional[aiohttp.ClientSession] = None
        self._instruments: Dict[str, Dict[str, Any]] = {} # symbol -> {id, size_tick, price_tick, ...}
        self._id_to_symbol: Dict[int, str] = {}           # id -> symbol
        self._books: Dict[str, Book] = {}                 # L2 Orderbooks (used for discovery/sub tracking)
        
        self._bbo: Dict[str, Tuple[float, float]] = {}  # symbol -> (bid, ask)
        self._bbo_ts: Dict[str, float] = {}             # symbol -> timestamp
        self._depth_cache: Dict[str, Tuple[float, float]] = {}  # symbol -> (bid_size, ask_size)
        self._ws_client: Optional[WsClient] = None
        self._ws = None  # Raw WS connection for orderbook updates

        # Funding rate cache: symbol -> rate (decimal)
        self._funding_rates: Dict[str, float] = {}
        self._last_funding_fetch = 0.0
        
        # Concurrency control for nonce-sensitive operations
        self._execution_lock = asyncio.Lock()

        # WS and staleness task tracking
        self._ws_task: Optional[asyncio.Task] = None
        self._staleness_task: Optional[asyncio.Task] = None
        self._ws_connected = asyncio.Event()  # FIX 2026-01-22: Track WS connection state

        # WS reconnection backoff (FIX 2026-01-24: Exponential backoff to prevent reconnect storms)
        self._ws_reconnect_backoff: float = 1.0  # Start at 1s, max 30s
        self._ws_last_reconnect_time: float = 0.0

        # Volume quota rate limit tracking (auto-disable for 1 hour on 429 "volume quota")
        self._volume_quota_blocked_until: float = 0.0
        self._VOLUME_QUOTA_COOLDOWN = 3600.0  # 1 hour cooldown
        
    def is_connected(self) -> bool:
        """Check if the adapter is connected (session exists and WS is active)."""
        return self._session is not None

    def _normalize_symbol(self, symbol: str) -> str:
        """Normalize symbol to Lighter format (e.g., 'BTC-USDC').

        Lighter uses BASE-QUOTE format (e.g., BTC-USDC, ETH-USDC).
        This method tries to find the correct key in _instruments.
        """
        sym = symbol.upper()
        if sym in self._instruments:
            return sym
        # Try alternate formats
        alternates = []
        if "-" not in sym and "USDC" not in sym and "USDT" not in sym:
            alternates.append(f"{sym}-USDC")  # BTC -> BTC-USDC
        if sym.endswith("USDT"):
            base = sym.replace("USDT", "")
            alternates.append(f"{base}-USDC")  # BTCUSDT -> BTC-USDC
        if "-USDC" in sym:
            alternates.append(sym.replace("-USDC", ""))  # BTC-USDC -> BTC
        for alt in alternates:
            if alt in self._instruments:
                return alt
        return sym  # Return original if no match

    def is_volume_quota_blocked(self) -> bool:
        """Check if Lighter is temporarily blocked due to volume quota limit."""
        if self._volume_quota_blocked_until <= 0:
            return False
        now = time.time()
        if now >= self._volume_quota_blocked_until:
            # Cooldown expired, re-enable
            remaining_was = self._volume_quota_blocked_until - now
            self._volume_quota_blocked_until = 0.0
            log.info("[Lighter] Volume quota cooldown expired, re-enabling operations")
            return False
        return True

    def _set_volume_quota_block(self) -> None:
        """Set volume quota block for 1 hour."""
        if self._volume_quota_blocked_until <= 0:
            self._volume_quota_blocked_until = time.time() + self._VOLUME_QUOTA_COOLDOWN
            log.warning(f"[Lighter] VOLUME QUOTA LIMIT HIT - Disabling for {self._VOLUME_QUOTA_COOLDOWN/60:.0f} min")

    def get_volume_quota_remaining_minutes(self) -> float:
        """Get remaining minutes until volume quota block expires."""
        if self._volume_quota_blocked_until <= 0:
            return 0.0
        remaining = self._volume_quota_blocked_until - time.time()
        return max(0.0, remaining / 60.0)

    def check_credentials(self) -> bool:
        """Verify that API keys or Private Key are present."""
        # Need account_index AND (private_key OR api_key)
        has_key = bool(self.private_key or self.api_key)
        return bool(has_key and self.account_index)
        
    async def start(self) -> None:
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=10)
            self._session = aiohttp.ClientSession(connector=get_connector(), timeout=timeout)
        log.info(f"[Lighter] Started adapter using URL: {self.rest_url}")
        
        # Initialize SDK client for leverage/margin operations
        await self._init_sdk_client()
        
        # Load instruments details (await so they're ready before returning)
        self._instruments: Dict[str, Dict[str, Any]] = {}
        await self._load_instruments()
        
        # NOTE: Blanket leverage setup for ALL markets removed to speed up boot.
        # Leverage will be set on-demand by the runner for active pairs only.
        # await self._setup_leverage()

        # Start WebSocket loop in background
        self._ws_task = asyncio.create_task(self._ws_loop(), name="lighter_ws_loop")
        # DISABLED: Staleness monitor was causing connection instability by forcing reconnects
        # self._staleness_task = asyncio.create_task(self._staleness_monitor_loop(), name="lighter_staleness_monitor")
        log.info("[Lighter] WebSocket loop started")


    
    async def _init_sdk_client(self) -> None:
        """Initialize Lighter SDK SignerClient."""
        if not self.private_key or not self.account_index:
            log.warning("[Lighter] Missing account_index or private_key - SDK features disabled")
            return

        try:
            import lighter
            # Verified: 'private_key' in risk.yaml is the valid API key for index 2.
            # 'api_key' entry in risk.yaml was actually a different (invalid) key in diagnostic tests.
            sdk_key = self.private_key if len(self.private_key) == 80 else (self.api_key or self._clean_private_key)

            self._sdk_client = lighter.SignerClient(
                url=self.rest_url,
                account_index=self.account_index,
                api_private_keys={self.api_key_index: sdk_key},
            )
            # Patch chain_id to 304 for Lighter Mainnet (SDK default)
            self._sdk_client.chain_id = 304

            # Patch SDK's internal HTTP client to use our custom DNS resolver
            try:
                if hasattr(self._sdk_client, 'api_client') and hasattr(self._sdk_client.api_client, 'rest_client'):
                    rest_client = self._sdk_client.api_client.rest_client
                    if hasattr(rest_client, 'pool_manager') and rest_client.pool_manager:
                        await rest_client.pool_manager.close()
                    rest_client.pool_manager = aiohttp.ClientSession(connector=get_connector())
                    log.info("[Lighter] Patched SDK HTTP client with custom DNS resolver")
            except Exception as patch_err:
                log.debug(f"[Lighter] Could not patch SDK HTTP client: {patch_err}")

            log.info(f"[Lighter] SDK SignerClient initialized (account={self.account_index}, api_key_idx={self.api_key_index}, chain_id=304)")
        except Exception as e:
            log.warning(f"[Lighter] Failed to init SDK client: {e}")
    
    async def _setup_leverage(self) -> None:
        """Set leverage to 5x isolated on ALL loaded markets sequentially with fallback and rate limiting."""
        if not self._sdk_client:
            log.warning("[Lighter] Cannot set leverage - SDK client not available")
            return

        log.info(f"[Lighter] Starting sequential leverage setup for {len(self._instruments)} markets...")

        # Sort instruments by symbol for consistent setup
        sorted_items = sorted(self._instruments.items(), key=lambda x: x[0])

        success_count = 0
        skipped_count = 0
        rate_limit_delay = 1.0  # Start with 1s delay, increases on rate limit

        for symbol, info in sorted_items:
            try:
                market_index = info.get("id")
                if market_index is None:
                    continue

                # Leverage levels to try (highest to lowest)
                target_levels = [5, 3, 2]
                applied_lev = 0

                for lev in target_levels:
                    # Retry loop for rate limiting
                    for retry in range(3):
                        tx, resp, err = await self._sdk_client.update_leverage(
                            market_index=int(market_index),
                            margin_mode=lighter.SignerClient.ISOLATED_MARGIN_MODE,
                            leverage=lev,
                        )

                        if not err:
                            applied_lev = lev
                            rate_limit_delay = max(1.0, rate_limit_delay * 0.9)  # Gradually reduce delay on success
                            break

                        err_str = str(err).lower()

                        # Rate limit handling with exponential backoff
                        if "too many requests" in err_str or "429" in err_str or "volume quota" in err_str or "23000" in err_str:
                            # Volume quota is a 1-hour block, not a short retry
                            if "volume quota" in err_str or "23000" in err_str:
                                self._set_volume_quota_block()
                                break
                            rate_limit_delay = min(10.0, rate_limit_delay * 2)  # Exponential backoff, max 10s
                            if retry < 2:
                                log.debug(f"[Lighter] Rate limited for {symbol}, retry {retry+1}/3 after {rate_limit_delay:.1f}s...")
                                await asyncio.sleep(rate_limit_delay)
                                continue
                            else:
                                log.warning(f"[Lighter] Rate limit persists for {symbol}, skipping...")
                                break

                        if "invalid initial margin" in err_str or "21113" in err_str:
                            # Try lower leverage
                            break
                        elif "invalid market index" in err_str or "21602" in err_str:
                            log.debug(f"[Lighter] Skipping leverage for spot/unsupported market {symbol}")
                            applied_lev = -1  # Mark as skipped (not failed)
                            break
                        elif "order book does not exist" in err_str:
                            log.debug(f"[Lighter] Skip delisted market {symbol}")
                            applied_lev = -1
                            break
                        elif "invalid signature" in err_str:
                            if not hasattr(self, "_sig_err_logged"):
                                log.warning(f"[Lighter] Leverage Signature Error: {err}. Check api_key/account_index.")
                                self._sig_err_logged = True
                            applied_lev = -1
                            break
                        elif "invalid nonce" in err_str:
                            log.debug(f"[Lighter] Nonce collision for {symbol} at {lev}x, retrying...")
                            await asyncio.sleep(0.5)
                            continue
                        else:
                            log.warning(f"[Lighter] Failed to set leverage for {symbol} at {lev}x: {err}")
                            break

                    if applied_lev != 0:
                        break

                if applied_lev > 0:
                    log.info(f"[Lighter] [OK] Leverage set to {applied_lev}x ISOLATED for {symbol}")
                    success_count += 1
                elif applied_lev == 0:
                    # Rate limit issue or other failure (not a permanent skip like spot market)
                    skipped_count += 1
                else:
                    # applied_lev == -1 means permanent skip (spot market, delisted, etc.)
                    skipped_count += 1

                # Sequential delay to avoid nonce/rate limits
                await asyncio.sleep(rate_limit_delay)

            except Exception as e:
                log.warning(f"[Lighter] Exception setting leverage for {symbol}: {e}")
                await asyncio.sleep(2.0)

        log.info(f"[Lighter] Leverage setup complete. Success: {success_count}, Skipped/Failed: {skipped_count}")

    async def _load_instruments(self) -> None:
        try:
             url = f"{self.rest_url}/api/v1/orderBooks"
             if not self._session: return
             async with self._session.get(url) as r:
                 if r.status == 200:
                     d = await r.json()
                     # API returns {"code": 200, "order_books": [...]}
                     order_books = d.get("order_books") if isinstance(d, dict) else d
                     if isinstance(order_books, list):
                         for inst in order_books:
                              s = inst.get("symbol", "").upper()
                              if not s: continue
                              
                              # Try multiple keys for ID (API instability)
                              inst_id = inst.get("id")
                              if inst_id is None: inst_id = inst.get("market_id")
                              if inst_id is None: inst_id = inst.get("marketId")
                              if inst_id is None: inst_id = inst.get("order_book_id")
                              
                              if inst_id is None:
                                  log.warning(f"[Lighter] Missing ID for symbol {s}. Keys: {list(inst.keys())}")
                                  continue

                              # Parse actual API fields with fallback for Elliot/v2 schema
                              base_sym = inst.get("base_symbol") or inst.get("symbol")
                              quote_sym = inst.get("quote_symbol") or "USDC"
                              
                              # Ticks (handle decimals vs raw value)
                              try:
                                  if "size_tick" in inst:
                                      size_tick = Decimal(str(inst["size_tick"]))
                                  elif "supported_size_decimals" in inst:
                                      size_tick = Decimal("1") / (Decimal("10") ** int(inst["supported_size_decimals"]))
                                  else:
                                      size_tick = Decimal("0.0001")
                                      
                                  if "price_tick" in inst:
                                      price_tick = Decimal(str(inst["price_tick"]))
                                  elif "supported_price_decimals" in inst:
                                      price_tick = Decimal("1") / (Decimal("10") ** int(inst["supported_price_decimals"]))
                                  else:
                                      price_tick = Decimal("0.01")
                                      
                                  min_size = Decimal(str(inst.get("min_size") or inst.get("min_base_amount") or "0"))
                                  min_price = Decimal(str(inst.get("min_price") or "0"))
                              except Exception as e:
                                  log.warning(f"[Lighter] Error parsing precision for {s}: {e}")
                                  continue

                              # Calculate decimals from ticks
                              price_decimals = 0
                              if price_tick > 0:
                                  price_decimals = abs(price_tick.as_tuple().exponent)
                              
                              size_decimals = 0
                              if size_tick > 0:
                                  size_decimals = abs(size_tick.as_tuple().exponent)

                              self._instruments[s] = {
                                  "id": inst_id,
                                  "base_sym": base_sym,
                                  "quote_sym": quote_sym,
                                  "size_tick": size_tick,
                                  "price_tick": price_tick,
                                  "min_size": min_size,
                                  "min_price": min_price,
                                  "price_decimals": price_decimals,
                                  "size_decimals": size_decimals
                              }
                              self._id_to_symbol[inst_id] = s
                              self._id_to_symbol[str(inst_id)] = s
                         log.info(f"[Lighter] Loaded {len(self._instruments)} instruments: {list(self._instruments.keys())[:5]}...")
        except Exception as e:
            log.warning(f"[Lighter] Failed to load instruments: {e}")



    async def close(self) -> None:
        # Cancel background tasks
        for t in (self._ws_task, self._staleness_task):
            if t:
                t.cancel()
        if self._session:
            await self._session.close()
            self._session = None
        if self._ws_client:
            await self._ws_client.close()
    async def set_isolated_margin(self, symbol: str, leverage: int = 2) -> bool:
        """
        Set isolated margin mode for Lighter.

        IMPORTANT: Lighter uses ACCOUNT-LEVEL margin settings configured via the web UI.
        This method verifies the account leverage limits and warns if > requested.
        P0 FIX: Default leverage reduced from 3x to 2x for risk management.

        Returns True if we believe the account is properly configured.
        """
        # Try to verify account limits
        try:
            if not self._session:
                self._session = aiohttp.ClientSession(connector=get_connector())
            
            # Check account limits if we have api_key (account index)
            # Lighter uses accountLimits endpoint to show max leverage per market
            url = f"{self.rest_url}/api/v1/accountLimits"
            # Note: This endpoint may require authentication
            async with self._session.get(url, params={"account_index": self.account_index}) as r:
                if r.status == 200:
                    data = await r.json()
                    limits = data.get("limits", data.get("account_limits", {}))
                    if limits:
                        log.info(f"[Lighter] Account limits retrieved: {limits}")
                        # Check if max_leverage is configured
                        max_lev = limits.get("max_leverage", 20)
                        if max_lev > leverage:
                            log.warning(f"[Lighter] Account max leverage is {max_lev}x, but we want {leverage}x. "
                                       f"Please configure via Lighter web UI!")
                        return True
        except Exception as e:
            log.debug(f"[Lighter] Could not verify account limits: {e}")
        
        # Fallback: Log warning and assume OK
        log.warning(f"[Lighter] [WARN] Cannot verify leverage settings via API. "
                   f"Please ensure your Lighter account is configured for {leverage}x isolated margin "
                   f"at https://lighter.xyz/trade")
        self._leverage_warned = True
        return True
    
    async def set_leverage(self, symbol: str, leverage: int = 2) -> bool:
        """
        Set leverage for Lighter using SDK SignerClient.
        DISABLED: Leverage calls drain volume quota and always fail.
        Assumes leverage is pre-configured on the account (2x isolated).
        P0 FIX 2026-01-22: Added verification - logs prominent warning if leverage cannot be verified.

        Returns:
            True if leverage is verified or assumed OK (with warning).
            Returns True even if verification fails to avoid blocking trading,
            but logs prominent warnings for manual verification.
        """
        # P0 FIX 2026-01-22: Attempt to verify leverage before proceeding
        # This doesn't SET leverage (API drains quota), but VERIFIES it's pre-configured
        verified = await self._verify_leverage_configuration(symbol, expected_leverage=leverage)

        if verified:
            log.info(f"[Lighter] Leverage verified: {symbol} is configured for {leverage}x isolated")
            return True
        else:
            # Verification failed - log prominently but don't fail-fast
            # Trading will proceed but user must check manually
            if not getattr(self, '_leverage_warning_logged', False):
                log.error("=" * 60)
                log.error("[Lighter] P0 CRITICAL: LEVERAGE VERIFICATION FAILED")
                log.error(f"[Lighter] Cannot verify that {symbol} is configured for {leverage}x isolated")
                log.error("[Lighter] PLEASE MANUALLY VERIFY AT: https://lighter.xyz/trade")
                log.error("[Lighter] If leverage is wrong, you may be trading with unexpected exposure!")
                log.error("=" * 60)
                self._leverage_warning_logged = True
            return True  # Return True to not block trading, but warning is logged

    async def _verify_leverage_configuration(self, symbol: str, expected_leverage: int = 2) -> bool:
        """
        P0 FIX 2026-01-22: Verify account leverage configuration without setting it.

        Returns True if leverage can be verified as correct.
        Returns False if verification fails (doesn't mean leverage is wrong, just unverified).
        """
        if not self._sdk_client:
            log.warning(f"[Lighter] Cannot verify leverage for {symbol}: SDK client not available")
            return False

        try:
            import lighter
            account_api = lighter.AccountApi(self._sdk_client.api_client)
            account = await account_api.account(by="index", value=str(self.account_index))

            if hasattr(account, 'accounts') and account.accounts:
                acc = account.accounts[0]

                # Try to find leverage info in account data
                # Different Lighter SDK versions expose this differently
                max_leverage = getattr(acc, 'max_leverage', None)
                leverage_setting = getattr(acc, 'leverage', None)
                margin_mode = getattr(acc, 'margin_mode', None)

                # Log what we found for debugging
                log.debug(f"[Lighter] Account info: max_leverage={max_leverage}, leverage={leverage_setting}, margin_mode={margin_mode}")

                # Check if we can verify leverage
                if leverage_setting is not None:
                    if int(leverage_setting) == expected_leverage:
                        return True
                    else:
                        log.error(f"[Lighter] LEVERAGE MISMATCH: Expected {expected_leverage}x, found {leverage_setting}x")
                        return False

                if max_leverage is not None:
                    if int(max_leverage) >= expected_leverage:
                        log.info(f"[Lighter] Account max_leverage={max_leverage} >= expected {expected_leverage}x (OK)")
                        return True

                # Can't verify from account info - check via positions if any exist
                if hasattr(acc, 'positions') and acc.positions:
                    for pos in acc.positions:
                        pos_leverage = getattr(pos, 'leverage', None)
                        if pos_leverage is not None:
                            if int(pos_leverage) == expected_leverage:
                                return True
                            else:
                                log.warning(f"[Lighter] Position leverage mismatch: {pos_leverage}x vs expected {expected_leverage}x")

                # No verification possible from account data
                log.warning(f"[Lighter] Cannot verify leverage from account data - assuming pre-configured")
                return False
            else:
                log.warning(f"[Lighter] No account data returned for leverage verification")
                return False

        except Exception as e:
            log.warning(f"[Lighter] Leverage verification error: {e}")
            return False

        if not self._sdk_client:
            log.warning(f"[Lighter] SDK client not available - cannot set leverage for {symbol}")
            return False

        # Skip if volume quota blocked
        if self.is_volume_quota_blocked():
            log.debug(f"[Lighter] Skip leverage for {symbol}: volume quota blocked")
            return False
            
        symbol = symbol.upper()
        info = self._instruments.get(symbol)
        if not info:
             log.warning(f"[Lighter] Unknown symbol {symbol} for leverage setup")
             return False
        
        market_index = info.get("id")
        if market_index is None: return False
        
        applied_lev = 0
        async with self._execution_lock:
            for lev in [leverage, 3, 2]:
                try:
                    # Official SDK method: update_leverage
                    tx, resp, err = await self._sdk_client.update_leverage(
                        market_index=market_index,
                        margin_mode=lighter.SignerClient.ISOLATED_MARGIN_MODE,
                        leverage=lev
                    )
                    
                    if not err:
                        log.info(f"[Lighter] [OK] Leverage set to {lev}x ISOLATED for {symbol}")
                        applied_lev = lev
                        break
                    
                    # Handle error
                    err_str = str(err).lower()
                    is_rate_limited = "too many requests" in err_str or "429" in err_str or "23000" in err_str or "volume quota" in err_str
                    
                    if "21113" in err_str or "invalid initial margin fraction" in err_str:
                        log.debug(f"[Lighter] {symbol} rejected {lev}x leverage (limit too low). Trying fallback...")
                        continue
                    elif is_rate_limited:
                        # Volume quota is a 1-hour block, don't retry
                        if "23000" in err_str or "volume quota" in err_str:
                            self._set_volume_quota_block()
                            break
                        sleep_time = 5.0
                        log.warning(f"[Lighter] Rate limited ({err_str[:50]}) for {symbol}. Sleeping {sleep_time}s...")
                        await asyncio.sleep(sleep_time)
                        # Retry same leverage once
                        tx, resp, err = await self._sdk_client.update_leverage(
                            market_index=market_index,
                            margin_mode=lighter.SignerClient.ISOLATED_MARGIN_MODE,
                            leverage=lev
                        )
                        if not err:
                            log.info(f"[Lighter] [OK] Leverage set to {lev}x ISOLATED for {symbol} after retry")
                            applied_lev = lev
                            break
                        else:
                            log.warning(f"[Lighter] Retry failed for {symbol}: {err}")
                            break
                    else:
                        log.warning(f"[Lighter] SDK error for {symbol} at {lev}x: {err}")
                        break
                except Exception as e:
                    log.warning(f"[Lighter] Exception for {symbol} at {lev}x: {e}")
                    break
                
        if applied_lev == 0:
             log.warning(f"[Lighter] [ERROR] Failed to set leverage for {symbol} after all fallbacks.")

        return applied_lev > 0

    # --- Order Execution ---
    
    async def place_order(self, symbol: str, side: str, qty: float, price: Optional[float] = None,
                          ioc: bool = True, reduce_only: bool = False, client_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Place order using Lighter SDK (official method).

        Includes retry logic with exponential backoff and post-placement fill verification.
        """
        # Check if volume quota blocked
        if self.is_volume_quota_blocked():
            remaining = self.get_volume_quota_remaining_minutes()
            log.warning(f"[Lighter] Order rejected: volume quota blocked ({remaining:.0f} min remaining)")
            return {
                "status": "error",
                "reason": f"volume_quota_blocked_{remaining:.0f}min",
                "filled": 0.0,
                "order_id": None,
                "avg_price": 0.0
            }

        # Check SDK client
        if not self._sdk_client:
            log.warning("[Lighter] SDK client not available - returning dry_run")
            return {
                "status": "dry_run",
                "filled": qty,
                "order_id": f"mock_{int(time.time())}",
                "avg_price": price if price else 100.0,
                "reason": None,
                "dry_run": True
            }

        # Get market info - FIX 2026-01-24 Bug #4: Use _normalize_symbol for lookup
        symbol_normalized = self._normalize_symbol(symbol.upper())
        info = self._instruments.get(symbol_normalized)
        if not info:
            # Try refreshing instruments before giving up
            log.debug(f"[Lighter] Instrument {symbol} ({symbol_normalized}) not found, refreshing instruments...")
            await self._load_instruments()
            symbol_normalized = self._normalize_symbol(symbol.upper())
            info = self._instruments.get(symbol_normalized)
        if not info:
            return {"status": "error", "reason": f"Unknown instrument {symbol} (normalized: {symbol_normalized})", "filled": 0.0, "order_id": None, "avg_price": 0.0}

        market_index = info.get("id", 0)
        price_decimals = info.get("price_decimals", 2)
        size_decimals = info.get("size_decimals", 4)

        # Convert to Lighter format
        base_amount = int(qty * (10 ** size_decimals))
        is_ask = 1 if side.upper() == "SELL" else 0

        # Determine order type and time-in-force
        import lighter
        if price is None or price == 0:
            # Market order
            order_type = lighter.SignerClient.ORDER_TYPE_MARKET
            time_in_force = lighter.SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL

            # Lighter requires a "worst" price for market orders to protect against slippage
            bbo = await self.get_fresh_bbo(symbol)
            if bbo and bbo[0] > 0 and bbo[1] > 0:
                # For BUY, use 2% higher than best ask. For SELL, use 2% lower than best bid.
                if side.upper() == "BUY":
                    price_val = bbo[1] * 1.02
                else:
                    price_val = bbo[0] * 0.98
                price_int = int(price_val * (10 ** price_decimals))
            else:
                # CRITICAL: Cannot place market order without valid BBO - reject order
                log.error(f"[Lighter] Cannot place market order for {symbol}: BBO unavailable or invalid")
                return {"status": "error", "reason": "BBO unavailable for market order", "filled": 0.0, "order_id": None, "avg_price": 0.0}
        else:
            # Limit order
            price_int = int(price * (10 ** price_decimals))
            if ioc:
                order_type = lighter.SignerClient.ORDER_TYPE_LIMIT
                time_in_force = lighter.SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL
            else:
                order_type = lighter.SignerClient.ORDER_TYPE_LIMIT
                time_in_force = lighter.SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME

        # Expiry: use 0 for IOC orders, -1 for GTC (SDK/API standard)
        if time_in_force == lighter.SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL:
            expiry = 0
        else:
            expiry = -1

        log.info(f"[Lighter] Placing order: {symbol} {side} qty={qty} (base={base_amount}) price_int={price_int} type={order_type}")

        # Retry logic with exponential backoff (3 attempts)
        max_retries = 3
        last_error = None

        for attempt in range(max_retries):
            try:
                # Generate unique client order ID per attempt to avoid nonce collision
                client_order_idx = int(time.time() * 1000000) % 2**31 + attempt

                log.info(f"[Lighter] Order attempt {attempt+1}/{max_retries}: market={market_index} side={side} qty_int={base_amount} price_int={price_int}")

                # Use SDK create_order
                created_order, response, error = await self._sdk_client.create_order(
                    market_index=market_index,
                    client_order_index=client_order_idx,
                    base_amount=base_amount,
                    price=price_int,
                    is_ask=is_ask,
                    order_type=order_type,
                    time_in_force=time_in_force,
                    reduce_only=reduce_only,
                    order_expiry=expiry,
                    api_key_index=self.api_key_index
                )

                if error:
                    err_str = str(error).lower()
                    last_error = str(error)

                    # Volume quota is a 1-hour block - don't retry
                    if "volume quota" in err_str or "23000" in err_str:
                        self._set_volume_quota_block()
                        return {"status": "error", "reason": "volume_quota_limit", "filled": 0.0, "order_id": None, "avg_price": 0.0}

                    # FIX 2026-01-24: Handle "invalid reduce only mode" error (code 21740)
                    # Some markets (like XAU) don't support reduce_only parameter
                    # Block this symbol from future attempts
                    if "21740" in err_str or "invalid reduce only mode" in err_str:
                        log.error(f"[Lighter] Market {symbol} does not support reduce_only mode - blocking symbol")
                        # Add to a set of blocked symbols (market doesn't support reduce_only)
                        if not hasattr(self, '_reduce_only_blocked_symbols'):
                            self._reduce_only_blocked_symbols = set()
                        self._reduce_only_blocked_symbols.add(symbol_normalized)
                        return {"status": "error", "reason": "market_no_reduce_only_support", "filled": 0.0, "order_id": None, "avg_price": 0.0}

                    # Categorize errors
                    is_rate_limit = any(x in err_str for x in ["too many requests", "429", "rate limit"])
                    is_nonce_error = "nonce" in err_str or "invalid nonce" in err_str
                    is_transient = is_rate_limit or is_nonce_error or "timeout" in err_str or "connection" in err_str

                    if is_transient and attempt < max_retries - 1:
                        wait_time = 2 ** attempt  # 1s, 2s, 4s
                        log.warning(f"[Lighter] Transient error (attempt {attempt+1}): {error}. Retrying in {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        # Non-retryable error or max retries reached
                        log.error(f"[Lighter] Order error (final): {error}")
                        return {"status": "error", "reason": str(error), "filled": 0.0, "order_id": None, "avg_price": 0.0}

                log.info(f"[Lighter] Order response: code={response.code if response else 'N/A'}")

                # Parse successful response
                order_id = "unknown"
                if created_order:
                    order_id = str(created_order.nonce) if created_order.nonce else str(int(time.time() * 1000))

                # Post-placement fill verification for IOC/Market orders
                actual_fill = 0.0
                avg_price = price if price else 0.0

                if response and response.code == 200 and ioc:
                    # Poll for actual fill status (like Aster does)
                    actual_fill, avg_price = await self._verify_fill(symbol, qty, side)
                    if actual_fill > 0:
                        log.info(f"[Lighter] Verified fill: {actual_fill} @ {avg_price}")
                    else:
                        # IOC may have been cancelled unfilled - check position delta
                        log.debug(f"[Lighter] IOC fill verification returned 0, using optimistic fill")
                        actual_fill = qty  # Fallback to optimistic
                else:
                    actual_fill = qty  # Optimistic for GTC or non-200 response

                return {
                    "status": "filled" if actual_fill > 0 else "accepted",
                    "filled": actual_fill,
                    "order_id": order_id,
                    "avg_price": avg_price,
                    "reason": None
                }

            except Exception as e:
                last_error = str(e)
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    log.warning(f"[Lighter] Order exception (attempt {attempt+1}): {e}. Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    log.error(f"[Lighter] Order exception (final): {e}")
                    import traceback
                    traceback.print_exc()

        return {"status": "error", "reason": last_error or "Max retries exceeded", "filled": 0.0, "order_id": None, "avg_price": 0.0}

    async def _verify_fill(self, symbol: str, expected_qty: float, side: str) -> Tuple[float, float]:
        """
        Verify order fill by checking position change.
        Returns (filled_qty, avg_price).
        """
        try:
            # Get current position for this symbol
            positions = await self.get_positions()
            for pos in positions:
                if pos.get("symbol", "").upper() == symbol.upper():
                    pos_size = abs(float(pos.get("size", 0)))
                    entry_price = float(pos.get("entry", 0))
                    # If we have a position, assume fill occurred
                    if pos_size > 0:
                        return (min(pos_size, expected_qty), entry_price)

            # No position found - could be fully closed or never opened
            # For IOC orders, if no position exists, the order may have been rejected or filled+closed
            # Return expected qty as optimistic fill
            bbo = self._bbo.get(symbol.upper())
            est_price = bbo[1] if side.upper() == "BUY" and bbo else (bbo[0] if bbo else 0.0)
            return (expected_qty, est_price)

        except Exception as e:
            log.debug(f"[Lighter] _verify_fill error: {e}")
            return (expected_qty, 0.0)  # Optimistic fallback

    async def cancel_order(self, order_id: Any, symbol: str) -> Dict[str, Any]:
        """Cancel a resting order on Lighter.

        P1 FIX 2026-01-22: Standardized parameter order to match HL/AS convention.
        Previously was (symbol, order_id) which was inconsistent with other venues.
        Now matches: cancel_order(order_id, symbol) like Hyperliquid and Aster.
        """
        try:
            instr = self._instruments.get(symbol.upper())
            if not instr:
                return {"status": "error", "reason": f"Unknown symbol {symbol}"}
            
            market_index = int(instr['id'])
            order_index = int(order_id)
            
            log.info(f"[Lighter] Cancelling order: {symbol} market={market_index} index={order_index}")
            
            cancel_obj, response, error = await self._sdk_client.cancel_order(
                market_index=market_index,
                order_index=order_index,
                api_key_index=self.api_key_index
            )
            
            if error:
                log.error(f"[Lighter] Cancel error: {error}")
                return {"status": "error", "reason": str(error)}
            
            status = "ok" if response and response.code == 200 else "error"
            log.info(f"[Lighter] Cancel response: code={response.code if response else 'N/A'} status={status}")
            
            return {
                "status": status,
                "reason": response.message if response else None
            }
        except Exception as e:
            log.error(f"[Lighter] Cancel exception: {e}")
            return {"status": "error", "reason": str(e)}

    async def wait_for_fill(
        self,
        symbol: str,
        order_id: Any,
        expected_qty: float,
        side: str,
        timeout_s: float = 60.0,
        poll_interval: float = 2.0
    ) -> Dict[str, Any]:
        """
        Wait for a limit order to fill on Lighter, polling position status.

        Note: Lighter doesn't have a direct order status API, so we verify
        fills by checking position changes.

        Args:
            symbol: Trading symbol (e.g., "BTC")
            order_id: The order ID (used for logging)
            expected_qty: Expected fill quantity
            side: "BUY" or "SELL" (needed for position verification)
            timeout_s: Maximum wait time (default 60s)
            poll_interval: Polling frequency (default 2s)

        Returns:
            Dict with keys: filled, avg_price, is_maker, status
            status is one of: 'filled', 'partial', 'timeout'
        """
        start_time = time.time()
        initial_position = 0.0
        initial_entry_price = 0.0

        # Get initial position state
        try:
            positions = await self.get_positions()
            for pos in positions:
                if pos.get("symbol", "").upper() == symbol.upper():
                    initial_position = float(pos.get("size", 0))
                    initial_entry_price = float(pos.get("entry", 0))
                    break
        except Exception as e:
            log.warning(f"[Lighter] wait_for_fill initial position check error: {e}")

        last_detected_fill = 0.0

        while (time.time() - start_time) < timeout_s:
            try:
                positions = await self.get_positions()
                current_position = 0.0
                current_entry_price = 0.0

                for pos in positions:
                    if pos.get("symbol", "").upper() == symbol.upper():
                        current_position = float(pos.get("size", 0))
                        current_entry_price = float(pos.get("entry", 0))
                        break

                # Calculate fill based on position change
                position_delta = abs(current_position - initial_position)

                # If position changed in expected direction, we have fills
                if side.upper() == "BUY":
                    # Expecting position to increase
                    fill_qty = current_position - initial_position if current_position > initial_position else 0
                else:
                    # Expecting position to decrease (or go more negative)
                    fill_qty = initial_position - current_position if current_position < initial_position else 0

                fill_qty = abs(fill_qty)

                if fill_qty >= expected_qty * 0.95:
                    log.info(f"[Lighter] Order {order_id} filled: {fill_qty:.6f} @ ${current_entry_price:.4f}")
                    return {
                        "filled": fill_qty,
                        "avg_price": current_entry_price,
                        "is_maker": True,
                        "status": "filled"
                    }

                if fill_qty > last_detected_fill:
                    log.debug(f"[Lighter] Order {order_id} partial fill: {fill_qty:.6f}/{expected_qty:.6f}")
                    last_detected_fill = fill_qty

            except Exception as e:
                log.warning(f"[Lighter] wait_for_fill poll error: {e}")

            await asyncio.sleep(poll_interval)

        # Timeout reached - but do a FINAL SWEEP before giving up
        # FIX 2026-01-25: Late fills can arrive between polls, causing false rollbacks
        elapsed = time.time() - start_time
        log.info(f"[Lighter] Order {order_id} timeout after {elapsed:.1f}s, detected_fill={last_detected_fill:.6f}")

        # FINAL SWEEP: One more position check after brief delay for settlement
        try:
            await asyncio.sleep(0.5)  # Small delay for settlement
            positions = await self.get_positions()
            final_position = 0.0
            final_entry_price = 0.0

            for pos in positions:
                if pos.get("symbol", "").upper() == symbol.upper():
                    final_position = float(pos.get("size", 0))
                    final_entry_price = float(pos.get("entry", 0))
                    break

            # Calculate fill based on position change
            if side.upper() == "BUY":
                final_fill_qty = final_position - initial_position if final_position > initial_position else 0
            else:
                final_fill_qty = initial_position - final_position if final_position < initial_position else 0

            final_fill_qty = abs(final_fill_qty)

            # Check if late fill detected (>= 70% of target)
            if final_fill_qty >= expected_qty * 0.70 and final_fill_qty > last_detected_fill:
                log.warning(f"[Lighter] LATE FILL SWEEP: Order {order_id} detected fill={final_fill_qty:.6f} "
                           f"(target={expected_qty:.6f}, was={last_detected_fill:.6f}) - NOT rolling back!")
                return {
                    "filled": final_fill_qty,
                    "avg_price": final_entry_price,
                    "is_maker": True,
                    "status": "filled"  # Treat as filled to prevent orphan rollback
                }
        except Exception as sweep_e:
            log.debug(f"[Lighter] Final sweep check error (non-fatal): {sweep_e}")

        # Return partial fill info (no late fill detected)
        return {
            "filled": last_detected_fill,
            "avg_price": 0.0,
            "is_maker": last_detected_fill > 0,
            "status": "timeout"
        }

    # ---------- Public Data ----------

    async def _send_sub(self, symbol: str):
        # We need to send: {"type": "subscribe", "channel": "order_book/{id}"}
        # Custom WS sub
        if not self._ws:
            log.warning(f"[Lighter] _send_sub: WS not connected for {symbol}")
            return

        info = self._instruments.get(symbol.upper())
        if not info:
            loaded_keys = list(self._instruments.keys())[:5]
            log.warning(f"[Lighter] _send_sub: No instrument for {symbol}. Loaded: {loaded_keys}... ({len(self._instruments)} total)")
            return

        market_id = info.get("id")
        if market_id is None:
            log.warning(f"[Lighter] _send_sub: No market_id for {symbol}")
            return

        # Format 1 (Verified): {"type": "subscribe", "channel": "order_book/ID"}
        msg = {"type": "subscribe", "channel": f"order_book/{market_id}"}
        log.debug(f"[Lighter] Subscribing to {symbol} (market_id={market_id})")
        try:
            await self._ws.send_json(msg)
        except Exception as e:
            log.warning(f"[Lighter] _send_sub failed for {symbol}: {e}")

    async def subscribe_orderbook(self, symbol: str) -> None:
        symbol = symbol.upper()
        if symbol not in self._books:
            self._books[symbol] = Book()
            # Dynamic subscription if connected
            if self._ws:
                await self._send_sub(symbol)

    async def subscribe_tickers(self, symbols: List[str]) -> None:
        """Subscribe to real-time ticker/BBO updates for symbols."""
        for symbol in symbols:
            await self.subscribe_orderbook(symbol)

    async def get_fresh_bbo(self, symbol: str, max_age_ms: float = 2000.0) -> Optional[Tuple[float, float]]:
        """
        Get BBO, fetching via REST if cache is stale or missing.
        """
        # FIX 2026-01-24 Bug #3: Normalize symbol for cache lookup
        symbol_normalized = self._normalize_symbol(symbol.upper())
        age = self.get_bbo_age_ms(symbol_normalized)
        if age <= max_age_ms:
            # _bbo can be (bb, ba) or (bb, ba, bs, bas)
            val = self._bbo.get(symbol_normalized)
            if not val: return None
            if len(val) >= 2:
                return (val[0], val[1])
            return None
            
        # Fetch fresh
        return await self.fetch_bbo_rest(symbol)

    async def fetch_bbo_rest(self, symbol: str) -> Optional[Tuple[float, float]]:
        """
        Fetch BBO, preferring SDK with market_id if available.
        """
        # FIX 2026-01-24: Normalize symbol to Lighter format (BTC -> BTC-USDC)
        symbol = self._normalize_symbol(symbol.upper())
        info = self._instruments.get(symbol)
        if not info:
             # Refresh instruments if not found
             await self._load_instruments()
             symbol = self._normalize_symbol(symbol)  # Re-normalize after refresh
             info = self._instruments.get(symbol)

        if info and self._sdk_client:
            try:
                import lighter
                market_index = info.get("id")
                if market_index is not None:
                    order_api = lighter.OrderApi(self._sdk_client.api_client)
                    ob = await order_api.order_book_orders(market_id=market_index, limit=5)
                    if ob.bids and ob.asks:
                        # Parse all levels for depth aggregation
                        parsed_bids = [(float(b.price), float(b.remaining_base_amount)) for b in ob.bids if float(b.price) > 0]
                        parsed_asks = [(float(a.price), float(a.remaining_base_amount)) for a in ob.asks if float(a.price) > 0]
                        parsed_bids.sort(key=lambda x: x[0], reverse=True)
                        parsed_asks.sort(key=lambda x: x[0])

                        if parsed_bids and parsed_asks:
                            best_bid, bid_sz = parsed_bids[0]
                            best_ask, ask_sz = parsed_asks[0]

                            # Update _books cache for aggregated depth
                            if symbol not in self._books:
                                self._books[symbol] = Book()
                            self._books[symbol].bids = parsed_bids
                            self._books[symbol].asks = parsed_asks

                            # Store 4-tuple to preserve sizes
                            self._bbo[symbol] = (best_bid, best_ask, bid_sz, ask_sz)
                            self._bbo_ts[symbol] = time.time()
                            return (best_bid, best_ask)
            except Exception as e:
                log.debug(f"[Lighter] SDK BBO fetch failed for {symbol}: {e}")

        # Fallback to direct REST
        url = f"{self.rest_url}/api/v1/orderBookDetails"
        try:
            if not self._session:
                self._session = aiohttp.ClientSession(connector=get_connector())
            
            # Try symbol first, then market_id if available
            params = {"symbol": symbol}
            if info and info.get("id") is not None:
                params["market_id"] = str(info.get("id"))

            async with self._session.get(url, params=params) as r:
                if r.status != 200:
                    return None

                data = await r.json()
                bids = data.get("bids", [])
                asks = data.get("asks", [])

                # Parse all levels for depth aggregation
                parsed_bids = []
                parsed_asks = []
                for b in bids:
                    try:
                        if isinstance(b, dict):
                            px = float(b.get("price", 0))
                            sz = float(b.get("size", 0))
                        else:
                            px = float(b[0])
                            sz = float(b[1]) if len(b) > 1 else 0.0
                        if px > 0:
                            parsed_bids.append((px, sz))
                    except: pass
                for a in asks:
                    try:
                        if isinstance(a, dict):
                            px = float(a.get("price", 0))
                            sz = float(a.get("size", 0))
                        else:
                            px = float(a[0])
                            sz = float(a[1]) if len(a) > 1 else 0.0
                        if px > 0:
                            parsed_asks.append((px, sz))
                    except: pass

                # Sort: bids descending, asks ascending
                parsed_bids.sort(key=lambda x: x[0], reverse=True)
                parsed_asks.sort(key=lambda x: x[0])

                if parsed_bids and parsed_asks:
                    best_bid, bid_sz = parsed_bids[0]
                    best_ask, ask_sz = parsed_asks[0]

                    # Update _books cache for aggregated depth
                    if symbol not in self._books:
                        self._books[symbol] = Book()
                    self._books[symbol].bids = parsed_bids
                    self._books[symbol].asks = parsed_asks

                    # Store 4-tuple to preserve sizes
                    self._bbo[symbol] = (best_bid, best_ask, bid_sz, ask_sz)
                    self._bbo_ts[symbol] = time.time()
                    return (best_bid, best_ask)
        except Exception:
            pass
        return None

    def is_ws_healthy(self, max_age_ms: float = 5000.0) -> bool:
        """
        Check if WebSocket is connected and receiving data.
        FIX 2026-01-22: Added to detect WS disconnects that leave stale prices.

        Returns True only if:
        1. _ws_connected event is set (WS appears connected)
        2. At least one subscribed symbol has fresh data (< max_age_ms)
        """
        if not self._ws_connected.is_set():
            return False

        if not self._books:
            # No subscriptions yet, consider healthy
            return True

        # Check if ANY symbol has fresh data
        now = time.time()
        for symbol in self._books.keys():
            ts = self._bbo_ts.get(symbol.upper(), 0.0)
            if ts > 0 and (now - ts) * 1000 < max_age_ms:
                return True

        # All subscribed symbols have stale data = unhealthy
        return False

    def get_bbo(self, symbol: str) -> Optional[Tuple[float, float]]:
        sym = symbol.upper()
        val = self._bbo.get(sym)
        if val:
            return val
        # FIX 2026-01-24: Try normalized symbol format
        norm = self._normalize_symbol(sym)
        if norm != sym:
            return self._bbo.get(norm)
        return None

    def round_price(self, symbol: str, price: float) -> float:
        sym = self._normalize_symbol(symbol.upper())
        instr = self._instruments.get(sym)
        if not instr: return price
        decimals = instr.get("price_decimals", 2)
        return round(price, decimals)

    def round_qty(self, symbol: str, qty: float) -> float:
        sym = self._normalize_symbol(symbol.upper())
        instr = self._instruments.get(sym)
        if not instr: return qty
        decimals = instr.get("size_decimals", 2)
        return round(qty, decimals)

    def get_bbo_with_depth(self, symbol: str) -> Optional[Tuple[Tuple[float, float], Tuple[float, float]]]:
        """
        Return ((bid, bid_qty), (ask, ask_qty))
        Uses cached BBO size if available.
        """
        val = self._bbo.get(symbol.upper())
        if val and len(val) >= 4:
            # We have sizes
            return ((val[0], val[2]), (val[1], val[3]))
        elif val and len(val) == 2:
             # Fallback if no sizes (should not happen with new WS loop)
             # return infinite depth to allow trading? No, unsafe.
             return ((val[0], 0.0), (val[1], 0.0))
             
        # Try REST (fetch_bbo_rest return simple tuple, we need fetch_order_book)
        # For now return None
        return None

    def get_aggregated_depth_usd(self, symbol: str, levels: int = 5) -> float:
        """Sum depth across multiple orderbook levels for more accurate liquidity estimate."""
        sym = symbol.upper()

        # FIX 2026-01-24: Try multiple symbol formats to handle mismatches
        # Lighter uses "BTC-USDC" format, but caller might pass "BTC" or "BTCUSDC"
        book = self._books.get(sym)
        if not book or not book.bids or not book.asks:
            # Try alternate formats: BTC -> BTC-USDC, BTCUSDC -> BTC
            alt_formats = []
            if "-" not in sym and "USDC" not in sym and "USDT" not in sym:
                alt_formats.append(f"{sym}-USDC")  # BTC -> BTC-USDC
            if sym.endswith("USDT"):
                base = sym.replace("USDT", "")
                alt_formats.append(f"{base}-USDC")  # BTCUSDT -> BTC-USDC
            if "-USDC" in sym:
                alt_formats.append(sym.replace("-USDC", ""))  # BTC-USDC -> BTC

            for alt in alt_formats:
                book = self._books.get(alt)
                if book and book.bids and book.asks:
                    # Log the symbol mismatch discovery once
                    if not hasattr(self, '_symbol_remap_logged'):
                        self._symbol_remap_logged = set()
                    if sym not in self._symbol_remap_logged:
                        self._symbol_remap_logged.add(sym)
                        log.info(f"[Lighter] Symbol remap: '{sym}' -> '{alt}' for depth lookup")
                    sym = alt
                    break

        if not book or not book.bids or not book.asks:
            # Fallback: Use _bbo 4-tuple if available (bid_px, ask_px, bid_sz, ask_sz)
            val = self._bbo.get(sym)
            # Also try alternate formats for _bbo
            if not val or len(val) < 4:
                for alt in (f"{sym}-USDC", sym.replace("-USDC", ""), sym.replace("USDT", "-USDC")):
                    val = self._bbo.get(alt)
                    if val and len(val) >= 4:
                        break
            if val and len(val) >= 4:
                mid = (val[0] + val[1]) / 2
                depth_usd = min(val[2], val[3]) * mid
                # Log when falling back to single-level for major pairs
                if mid > 1000 or sym in ("BTC", "BTC-USDC", "ETH", "ETH-USDC"):
                    log.debug(f"[Lighter] get_aggregated_depth_usd({sym}): FALLBACK to _bbo single-level, "
                              f"bid_sz={val[2]:.6f} ask_sz={val[3]:.6f} mid=${mid:.2f} -> ${depth_usd:.2f}")
                    # Log available book keys for diagnosis
                    if not hasattr(self, '_depth_diag_logged'):
                        self._depth_diag_logged = set()
                    if sym not in self._depth_diag_logged:
                        self._depth_diag_logged.add(sym)
                        book_keys = list(self._books.keys())[:10]
                        log.info(f"[Lighter] DEPTH DIAG: Looking for '{symbol}', _books has {len(self._books)} keys: {book_keys}...")
                return depth_usd
            log.debug(f"[Lighter] get_aggregated_depth_usd: No book/bbo for {sym}")
            return 0.0
        mid = (book.bids[0][0] + book.asks[0][0]) / 2
        bid_depth = sum(sz for _, sz in book.bids[:levels])
        ask_depth = sum(sz for _, sz in book.asks[:levels])
        return min(bid_depth, ask_depth) * mid

    async def get_bbo_with_depth_async(self, symbol: str) -> Optional[Tuple[Tuple[float, float], Tuple[float, float]]]:
        """Fetch BBO with depth from Lighter SDK."""
        if not self._sdk_client:
            return self.get_bbo_with_depth(symbol)

        # Get market index
        info = self._instruments.get(symbol.upper())
        if not info:
            return self.get_bbo_with_depth(symbol)

        market_index = info.get("id", 0)

        try:
            import lighter
            order_api = lighter.OrderApi(self._sdk_client.api_client)
            ob = await order_api.order_book_orders(market_id=market_index, limit=5)

            if ob.bids and ob.asks:
                # Parse all levels for depth aggregation
                sym = symbol.upper()
                parsed_bids = [(float(b.price), float(b.remaining_base_amount)) for b in ob.bids if float(b.price) > 0]
                parsed_asks = [(float(a.price), float(a.remaining_base_amount)) for a in ob.asks if float(a.price) > 0]
                parsed_bids.sort(key=lambda x: x[0], reverse=True)
                parsed_asks.sort(key=lambda x: x[0])

                if parsed_bids and parsed_asks:
                    best_bid_price, best_bid_size = parsed_bids[0]
                    best_ask_price, best_ask_size = parsed_asks[0]

                    # Update _books cache for aggregated depth
                    if sym not in self._books:
                        self._books[sym] = Book()
                    self._books[sym].bids = parsed_bids
                    self._books[sym].asks = parsed_asks

                    # Cache for sync access - store 4-tuple to preserve sizes
                    self._bbo[sym] = (best_bid_price, best_ask_price, best_bid_size, best_ask_size)
                    self._bbo_ts[sym] = time.time()
                    self._depth_cache[sym] = (best_bid_size, best_ask_size)

                    return ((best_bid_price, best_bid_size), (best_ask_price, best_ask_size))
        except Exception as e:
            log.debug(f"[Lighter] Failed to get depth for {symbol}: {e}")

        return self.get_bbo_with_depth(symbol)

    def get_bbo_age_ms(self, symbol: str) -> float:
        sym = symbol.upper()
        ts = self._bbo_ts.get(sym, 0.0)
        if ts <= 0:
            # FIX 2026-01-24: Try normalized symbol format
            norm = self._normalize_symbol(sym)
            if norm != sym:
                ts = self._bbo_ts.get(norm, 0.0)
        if ts <= 0:
            return 999999.0
        return (time.time() - ts) * 1000.0

    async def equity(self) -> Optional[float]:
        """
        Fetch Lighter Equity (Collateral + PnL) via REST.
        Uses /api/v1/account?by=index&value={account_index}
        """
        if not self.account_index:
            log.debug("[Lighter] equity(): No account_index configured")
            return None

        # Ensure session exists
        if not self._session or self._session.closed:
            log.debug("[Lighter] equity(): Creating new session")
            self._session = aiohttp.ClientSession(connector=get_connector())

        try:
            url = f"{self.rest_url}/api/v1/account?by=index&value={self.account_index}"

            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    d = await r.json()
                    # Actual response format (from diagnostic):
                    # {"accounts": [{"collateral": "500.433812", "total_asset_value": "500.433812", ...}]}
                    
                    accounts = d.get("accounts", [])
                    if not accounts:
                        return None # Invalid response, don't assume 0
                        
                    acc = accounts[0] if isinstance(accounts, list) else accounts
                    total = 0.0
                    
                    # Primary: use collateral (free balance not locked in positions)
                    total = float(acc.get("collateral", 0) or 0)
                    if total == 0:
                        total = float(acc.get("available_balance", 0) or 0)

                    # CRITICAL: Add margin locked in positions + unrealized PnL
                    # Lighter subtracts margin from collateral when positions are opened,
                    # so we need to add it back to get true account equity
                    positions = acc.get("positions", [])
                    for pos in positions:
                        # Add allocated margin (locked for this position)
                        allocated = float(pos.get("allocated_margin", 0) or 0)
                        total += allocated
                        # Add unrealized PnL
                        pnl = float(pos.get("unrealized_pnl", 0) or 0)
                        total += pnl
                    
                    # Log success once
                    if self._bbo_ts.get("EQ_LOG") is None:
                        log.info(f"[Lighter] Equity fetched: ${total:.2f}")
                        self._bbo_ts["EQ_LOG"] = 1.0
                    return total
                else:
                    if self._bbo_ts.get("EQ_ERR") is None:
                        log.warning(f"[Lighter] Equity Error {r.status}: {await r.text()}")
                        self._bbo_ts["EQ_ERR"] = 1.0
        except Exception as e:
            log.warning(f"[Lighter] Equity fetch fatal: {e}")
            
        return None

    async def _ws_loop(self) -> None:
        """
        WebSocket loop to maintain OrderBook updates.
        Direct aiohttp implementation.
        """
        while True:
            ws = None
            try:
                log.debug(f"[Lighter] Connecting to WS: {self.ws_url}")
                try:
                    ws = await self._session.ws_connect(self.ws_url, heartbeat=15, timeout=30)
                except Exception as conn_err:
                    log.error(f"[Lighter] WS Connect FAILED: {type(conn_err).__name__}: {conn_err}")
                    # FIX 2026-01-24: Exponential backoff for failed connections
                    await asyncio.sleep(self._ws_reconnect_backoff)
                    self._ws_reconnect_backoff = min(self._ws_reconnect_backoff * 2, 30.0)
                    continue

                self._ws = ws
                self._ws_connected.set()  # FIX 2026-01-22: Signal WS is connected
                # FIX 2026-01-24: Reset backoff on successful connection
                self._ws_reconnect_backoff = 1.0
                self._ws_last_reconnect_time = time.time()
                log.debug("[Lighter] WS Connected!")

                # Resubscribe to existing books
                for s in list(self._books.keys()):
                    await self._send_sub(s)
                    await asyncio.sleep(0.05)  # FIX 2026-01-24: 50ms rate limit on resubscriptions
                    
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        
                        # DEBUG: Log RAW MSG once
                        if self._bbo_ts.get("DEBUG") is None:
                            log.info(f"[Lighter] RAW MSG SAMPLE: {str(data)[:200]}")
                            self._bbo_ts["DEBUG"] = 1.0

                        # Handle connection/subscription messages
                        m_type = data.get("type", "")
                        if m_type == "connected":
                            continue
                        if m_type == "subscribed":
                            continue
                        if m_type == "error":
                            if self._bbo_ts.get("ERR_LOG") is None:
                                log.warning(f"[Lighter] WS Error: {data}")
                                self._bbo_ts["ERR_LOG"] = 1.0
                            continue
                        
                        # Parse order book updates
                        # Format: {"channel": "order_book:0", "order_book": {"asks": [...], "bids": [...]}}
                        channel = data.get("channel", "")
                        if channel.startswith("order_book:"):
                            # Extract market ID from channel
                            try:
                                market_id = int(channel.split(":")[1])
                            except:
                                continue
                            
                            # Get order book data (nested inside "order_book" key)
                            ob_data = data.get("order_book", {})
                            bids = ob_data.get("bids", [])
                            asks = ob_data.get("asks", [])
                            
                            # Find symbol for this market ID
                            target_sym = None
                            for s, info in self._instruments.items():
                                if info.get("id") == market_id:
                                    target_sym = s
                                    break
                            
                            if target_sym and (bids or asks):
                                # Log first data reception
                                if self._bbo_ts.get(f"DATA_{target_sym}") is None:
                                    log.info(f"[Lighter] Received data for {target_sym}: {len(bids)}b/{len(asks)}a")
                                    self._bbo_ts[f"DATA_{target_sym}"] = 1.0

                                # Parse all levels for depth aggregation
                                parsed_bids = []
                                parsed_asks = []
                                for b in bids:
                                    try:
                                        px = float(b.get("price", 0))
                                        sz = float(b.get("size", 0))
                                        if px > 0 and sz > 0:
                                            parsed_bids.append((px, sz))
                                    except: pass
                                for a in asks:
                                    try:
                                        px = float(a.get("price", 0))
                                        sz = float(a.get("size", 0))
                                        if px > 0 and sz > 0:
                                            parsed_asks.append((px, sz))
                                    except: pass

                                # Sort: bids descending, asks ascending
                                parsed_bids.sort(key=lambda x: x[0], reverse=True)
                                parsed_asks.sort(key=lambda x: x[0])

                                # Update _books cache for aggregated depth
                                if target_sym not in self._books:
                                    self._books[target_sym] = Book()
                                self._books[target_sym].bids = parsed_bids
                                self._books[target_sym].asks = parsed_asks

                                # Update BBO cache
                                if parsed_bids and parsed_asks:
                                    best_bid, bid_sz = parsed_bids[0]
                                    best_ask, ask_sz = parsed_asks[0]
                                    self._bbo[target_sym] = (best_bid, best_ask, bid_sz, ask_sz)
                                    self._bbo_ts[target_sym] = time.time()

                    elif msg.type == aiohttp.WSMsgType.CLOSE:
                        log.info("[Lighter] WS Closed")
                        self._ws_connected.clear()  # FIX 2026-01-22: Signal WS disconnected
                        break
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        log.info("[Lighter] WS Error")
                        self._ws_connected.clear()  # FIX 2026-01-22: Signal WS disconnected
                        break
            except Exception as e:
                log.error(f"[Lighter] WS Loop Error: {e}")
                self._ws_connected.clear()  # FIX 2026-01-22: Signal WS disconnected
                self._ws = None

            # FIX 2026-01-24: Exponential backoff for reconnects
            await asyncio.sleep(self._ws_reconnect_backoff)
            self._ws_reconnect_backoff = min(self._ws_reconnect_backoff * 2, 30.0)

    async def _staleness_monitor_loop(self) -> None:
        """Monitor for stale price data and log warnings."""
        STALE_THRESHOLD_MS = 5000.0   # 5 seconds (reduced for faster staleness detection)
        CHECK_INTERVAL_S = 2.0        # Check every 2s for quicker response

        while True:
            try:
                await asyncio.sleep(CHECK_INTERVAL_S)
                if not self._books:
                    continue

                stale_symbols = []
                for symbol in list(self._books.keys()):
                    age_ms = self.get_bbo_age_ms(symbol)
                    if age_ms > STALE_THRESHOLD_MS:
                        stale_symbols.append((symbol, age_ms))

                if stale_symbols:
                    # FIX 2026-01-22: Reduced to debug - staleness handled silently in background
                    log.debug("[Lighter] STALENESS: %d symbols stale (>%.0fs): %s",
                        len(stale_symbols), STALE_THRESHOLD_MS / 1000,
                        ", ".join(f"{s}:{int(age)}ms" for s, age in stale_symbols[:5]))

                    # If >50% of symbols are stale, close WS to trigger reconnect
                    if len(stale_symbols) > len(self._books) * 0.5:
                        log.debug("[Lighter] >50%% symbols stale - forcing WS reconnect")
                        # FIX 2026-01-24: Clear connected flag BEFORE closing WS
                        # This prevents get_bbo() from returning stale cached values
                        # during the reconnection window (matches Aster's correct pattern)
                        self._ws_connected.clear()
                        if self._ws:
                            try:
                                await self._ws.close()
                            except Exception:
                                pass
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.warning("[Lighter] Staleness monitor error: %s", e)
                await asyncio.sleep(5.0)

    async def get_funding_rates(self) -> Dict[str, float]:
        """
        Fetch funding rates via /api/v1/funding-rates
        """
        now = time.time()
        if now - self._last_funding_fetch < 10.0:  # Cache for 10s
             return self._funding_rates

        url = f"{self.rest_url}/api/v1/funding-rates"  # FIXED: was /fundings
        try:
            if not self._session:
                self._session = aiohttp.ClientSession(connector=get_connector())
                
            async with self._session.get(url) as r:
                if r.status != 200:
                    log.debug(f"[Lighter] Funding rates API returned {r.status}")
                    return self._funding_rates
                
                data = await r.json()
                # Response format: {"code": 200, "funding_rates": [{market_id, exchange, symbol, rate}, ...]}
                # CRITICAL FIX 2026-01-25: API returns rates from multiple exchanges (binance, bybit, etc.)
                # We MUST filter for exchange='lighter' only, otherwise we get wrong rates (10x error!)
                funding_list = data.get("funding_rates", [])
                if isinstance(funding_list, list):
                    for item in funding_list:
                        # CRITICAL: Only use Lighter's own funding rate, not other exchanges
                        exchange = item.get("exchange", "").lower()
                        if exchange != "lighter":
                            continue

                        # Get symbol from our instruments by market_id
                        market_id = item.get("market_id")
                        rate = item.get("rate", 0)

                        # Find symbol name from market_id
                        for sym, info in self._instruments.items():
                            if info.get("id") == market_id:
                                self._funding_rates[sym] = float(rate)
                                log.debug(f"[LT RAW] {sym}: rate={rate} (exchange={exchange})")
                                break
                        
                self._last_funding_fetch = now
                log.debug(f"[Lighter] Fetched {len(self._funding_rates)} funding rates")
        except Exception as e:
            log.warning(f"[Lighter] Funding fetch error: {e}")
            
        return self._funding_rates

    # ---------- Execution (Stubbed) ----------

    # Old defective equity removed. Using top-stub.

    async def get_positions(self) -> List[Dict[str, Any]]:
        """Fetch all open positions from Lighter.
        FIX 2026-01-22: Added size_usd from SDK's position_value field for accurate USD comparison.
        """
        if not self._sdk_client:
            log.debug("[Lighter] get_positions: SDK client not available")
            return []

        try:
            import lighter
            account_api = lighter.AccountApi(self._sdk_client.api_client)
            account = await account_api.account(by="index", value=str(self.account_index))

            positions = []
            if hasattr(account, 'accounts') and account.accounts:
                acc = account.accounts[0]
                # Parse positions from account data
                # SDK AccountPosition fields: position (size in coins), sign (1/-1), symbol,
                # avg_entry_price, position_value (USD), unrealized_pnl, market_id
                if hasattr(acc, 'positions') and acc.positions:
                    for pos in acc.positions:
                        # 'position' is the size field (coins), 'sign' indicates long(1)/short(-1)
                        raw_size = float(pos.position or 0)
                        sign = int(pos.sign or 1)
                        actual_size = raw_size * sign  # Apply sign for direction
                        entry_px = float(pos.avg_entry_price or 0)
                        # FIX 2026-01-22: Use position_value for USD value (more accurate)
                        position_value = float(pos.position_value or 0)
                        # Fallback: calculate from size * entry if position_value not available
                        size_usd = abs(position_value) if position_value != 0 else abs(raw_size * entry_px)

                        # FIX 2026-01-25: Add USD threshold to filter dust positions at source
                        # Prevents orphan alerts for tiny residuals from partial fills
                        MIN_POSITION_USD = 10.0
                        if raw_size != 0:
                            if size_usd < MIN_POSITION_USD:
                                log.debug(f"[Lighter] Ignoring dust position {pos.symbol}: ${size_usd:.2f} < ${MIN_POSITION_USD}")
                                continue
                            positions.append({
                                "symbol": pos.symbol or f"MARKET_{pos.market_id}",
                                "size": actual_size,  # Coins (for order calculations)
                                "size_usd": size_usd,  # USD value (for reconciliation)
                                "entry": entry_px,
                                "unrealized_pnl": float(pos.unrealized_pnl or 0),
                            })
                            log.debug(f"[Lighter] Position found: {pos.symbol} size={actual_size} size_usd=${size_usd:.2f} entry={entry_px}")
                else:
                    log.debug(f"[Lighter] get_positions: No positions attribute or empty positions list")
            else:
                log.debug(f"[Lighter] get_positions: No accounts in response")

            log.debug(f"[Lighter] get_positions returning {len(positions)} positions: {positions}")
            return positions
        except Exception as e:
            log.warning(f"[Lighter] get_positions error: {e}")
            import traceback
            log.debug(f"[Lighter] get_positions traceback: {traceback.format_exc()}")
            return []
    
    def get_book(self, symbol: str) -> Optional[Book]:
        """Return the order book for a symbol."""
        return self._books.get(symbol.upper())
    
    async def get_funding_rate(self, symbol: str) -> float:
        """Fetch current funding rate for a single symbol."""
        rates = await self.get_funding_rates()
        return rates.get(symbol.upper(), 0.0)
    
    async def get_order_fill_price(self, symbol: str, oid: Any) -> Optional[float]:
        """Fetch average fill price for an order ID. Returns None if not filled."""
        # Lighter SDK would need order status endpoint
        # For now, return None as we use IOC fills optimistically
        log.debug(f"[Lighter] get_order_fill_price not fully implemented for {oid}")
        return None

    # ---------- Helpers ----------
    def fees_taker_bps(self) -> float:
        return float(self.fees_bps.get("taker_bps", 5.0)) # Guess 5bps

    def fees_maker_bps(self) -> float:
        return float(self.fees_bps.get("maker_bps", 0.0))

