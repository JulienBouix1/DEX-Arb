# -*- coding: utf-8 -*-
# Hyperliquid Exchange injector (hotfix):
# - Sanitizes REST URL to avoid using a private key string as base URL.
# - Minimal CLI when executed as a module: prints sdk readiness and address.

from __future__ import annotations

import os
from typing import Any, Dict, Optional, Tuple

_DEFAULT_REST = "https://api.hyperliquid.xyz"

def _sanitize_url(u: Optional[str]) -> str:
    if not isinstance(u, str):
        return _DEFAULT_REST
    s = u.strip()
    if not s or s.lower().startswith("0x") or "://" not in s:
        return _DEFAULT_REST
    return s

def build_exchange_and_wallet(creds: Optional[Dict[str, Any]] = None) -> Tuple[Optional[object], Optional[str]]:
    """Returns (exchange, user_address) or (None, None) if missing secrets."""
    creds = dict(creds or {})
    rest_url = _sanitize_url(creds.get("rest_url") or os.getenv("HL_REST_URL"))
    pk_env  = creds.get("private_key_env") or "HL_PRIVATE_KEY"
    pk      = os.getenv(pk_env)
    addr    = creds.get("user_address") or os.getenv("HL_ACCOUNT_ADDRESS") or os.getenv("HL_ADDRESS")

    try:
        from hyperliquid.exchange import Exchange  # type: ignore
    except Exception:
        return (None, addr)

    try:
        ex = Exchange(rest_url, pk)  # pk may be None for public endpoints
        return (ex, addr)
    except Exception:
        return (None, addr)

if __name__ == "__main__":
    ex, addr = build_exchange_and_wallet({})
    print({"sdk_ready": bool(ex), "address": addr})
