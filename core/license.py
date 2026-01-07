# -*- coding: utf-8 -*-
"""
License System for Bot_V3.
Simple license key validation for live mode access.

Paper mode is always available without license.
Live mode requires a valid license key.
"""
import hashlib
import os
import time
import logging
from typing import Tuple, Optional

log = logging.getLogger(__name__)

# License file location
LICENSE_FILE = "data/license.key"

# Salt for license key generation (change this for your deployment)
_SALT = "Bot_V3_2025_Julien"


def _generate_license_hash(email: str, expiry_ts: int) -> str:
    """Generate license hash from email and expiry timestamp."""
    raw = f"{email.lower().strip()}:{expiry_ts}:{_SALT}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def generate_license_key(email: str, days_valid: int = 365) -> str:
    """
    Generate a license key for a given email.

    Format: EMAIL:EXPIRY_TS:HASH

    Args:
        email: User email address
        days_valid: Number of days the license is valid

    Returns:
        License key string
    """
    expiry_ts = int(time.time()) + (days_valid * 86400)
    hash_val = _generate_license_hash(email, expiry_ts)
    return f"{email}:{expiry_ts}:{hash_val}"


def validate_license_key(license_key: str) -> Tuple[bool, str, Optional[str]]:
    """
    Validate a license key.

    Args:
        license_key: The license key string

    Returns:
        Tuple of (is_valid, message, email_or_none)
    """
    if not license_key or not license_key.strip():
        return False, "Empty license key", None

    parts = license_key.strip().split(":")
    if len(parts) != 3:
        return False, "Invalid license format", None

    email, expiry_str, provided_hash = parts

    try:
        expiry_ts = int(expiry_str)
    except ValueError:
        return False, "Invalid expiry timestamp", None

    # Check expiry
    if time.time() > expiry_ts:
        return False, f"License expired on {time.strftime('%Y-%m-%d', time.localtime(expiry_ts))}", email

    # Verify hash
    expected_hash = _generate_license_hash(email, expiry_ts)
    if provided_hash != expected_hash:
        return False, "Invalid license signature", None

    # Valid!
    days_left = int((expiry_ts - time.time()) / 86400)
    return True, f"Valid until {time.strftime('%Y-%m-%d', time.localtime(expiry_ts))} ({days_left} days)", email


def load_license() -> Tuple[bool, str, Optional[str]]:
    """
    Load and validate license from file or environment variable.

    Returns:
        Tuple of (is_valid, message, email_or_none)
    """
    # 1. Check environment variable first
    env_key = os.environ.get("BOT_LICENSE_KEY", "").strip()
    if env_key:
        log.debug("[LICENSE] Checking license from BOT_LICENSE_KEY env var")
        return validate_license_key(env_key)

    # 2. Check license file
    if os.path.exists(LICENSE_FILE):
        try:
            with open(LICENSE_FILE, "r", encoding="utf-8") as f:
                file_key = f.read().strip()
            if file_key:
                log.debug(f"[LICENSE] Checking license from {LICENSE_FILE}")
                return validate_license_key(file_key)
        except Exception as e:
            log.warning(f"[LICENSE] Failed to read license file: {e}")

    return False, "No license found", None


def save_license(license_key: str) -> bool:
    """Save license key to file."""
    try:
        os.makedirs(os.path.dirname(LICENSE_FILE), exist_ok=True)
        with open(LICENSE_FILE, "w", encoding="utf-8") as f:
            f.write(license_key.strip())
        log.info(f"[LICENSE] License saved to {LICENSE_FILE}")
        return True
    except Exception as e:
        log.error(f"[LICENSE] Failed to save license: {e}")
        return False


def check_live_mode_access() -> Tuple[bool, str]:
    """
    Check if live mode is allowed.

    Returns:
        Tuple of (allowed, message)
    """
    is_valid, msg, email = load_license()

    if is_valid:
        return True, f"Live mode authorized for {email}. {msg}"
    else:
        return False, f"Live mode BLOCKED: {msg}. Use paper mode or provide valid license."


# Default authorized email for this bot instance
AUTHORIZED_EMAIL = "github@jbouix.com"


def generate_default_license(days_valid: int = 365) -> str:
    """Generate a license for the default authorized email."""
    return generate_license_key(AUTHORIZED_EMAIL, days_valid)


# CLI utility for generating licenses
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        # Default: generate for authorized email
        email = AUTHORIZED_EMAIL
        days = 365
        print(f"No email specified, using default: {AUTHORIZED_EMAIL}")
    else:
        email = sys.argv[1]
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 365

    key = generate_license_key(email, days)
    print(f"\nGenerated License Key for {email}:")
    print(f"Valid for {days} days\n")
    print("=" * 60)
    print(key)
    print("=" * 60)
    print(f"\nTo activate, either:")
    print(f"1. Save to {LICENSE_FILE}")
    print(f"2. Set BOT_LICENSE_KEY environment variable")
    print(f"3. Add to .env file as BOT_LICENSE_KEY=<key>")

    # Offer to save directly
    save = input("\nSave to data/license.key now? [y/N]: ").strip().lower()
    if save == 'y':
        save_license(key)
        print("License saved!")
