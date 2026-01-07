# Project Guidelines - Carry Trading Bot

## Pre-Live Checklist

**CRITICAL**: Before declaring the bot ready for live trading, you MUST:

1. Run E2E venue roundtrip test:
   ```bash
   python tests/test_e2e_venue_roundtrip.py --auto-confirm
   ```
2. Verify all 3 venues pass (HL, AS, LT)
3. Check no open positions remain after test
4. Run unit tests: `pytest tests/test_carry.py -v`

## Venue Symbol Conventions

| Venue | Symbol Format | Example |
|-------|---------------|---------|
| Hyperliquid (HL) | Coin only | `BTC`, `ETH` |
| Aster (AS) | Symbol + USDT | `BTCUSDT`, `ETHUSDT` |
| Lighter (LT) | Coin only | `BTC`, `ETH` |

## Key Configuration Files

- `config/venues.yaml` - Venue endpoints and credentials
- `config/risk.yaml` - Risk parameters and carry strategy config
- `config/pairs.yaml` - Trading pairs mapping
- `.env` - API keys (never commit!)

## Testing Modes

| Mode | Config | Behavior |
|------|--------|----------|
| Paper | `paper_mode: true` | Simulated orders, tracks paper equity |
| Dry-Run Live | `dry_run_live: true` | Connects to venues, logs "WOULD PLACE" |
| Live | Both `false` | Real money trading |

## Common Issues

- **DNS timeouts**: Bot uses custom DNS resolver (8.8.8.8/1.1.1.1)
- **Lighter rate limit**: Wait 1h if you see "volume quota" error
- **Aster reduce_only**: Don't use `reduce_only=True` on exit orders
