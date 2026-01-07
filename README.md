# Bot_V3 - Multi-Venue Perpetual Futures Arbitrage Bot

A professional-grade trading bot for perpetual futures arbitrage across decentralized exchanges (DEXs): **Hyperliquid**, **Aster**, and **Lighter**.

## Overview

This bot implements two distinct trading strategies:

### 1. Scalp Strategy (Perp-Perp Arbitrage)
- **Objective**: Capture instantaneous price discrepancies between venues
- **Execution**: Simultaneous dual-leg IOC (Immediate-Or-Cancel) orders
- **Hold Time**: Seconds to minutes
- **Target Edge**: 25-50+ bps spread

### 2. Carry Strategy (Funding Rate Arbitrage)
- **Objective**: Collect funding rate differentials while maintaining delta-neutral positions
- **Execution**: Open opposite positions on two venues when funding rates diverge
- **Hold Time**: 8-24 hours
- **Target Edge**: 50+ bps funding differential per 8h period

## Project Structure

```
Bot_V3/
├── config/
│   ├── risk.yaml           # Risk parameters & strategy settings
│   ├── venues.yaml         # Exchange API endpoints
│   ├── pairs.yaml          # Trading pairs configuration
│   └── blocklist.yaml      # Blocked symbols
├── core/
│   ├── license.py          # License management for live mode
│   ├── notifier.py         # Discord/webhook notifications
│   ├── position_manager.py # Centralized position tracking
│   ├── orderbook.py        # Order book management
│   └── dns_utils.py        # Network utilities
├── strategies/
│   ├── perp_perp_arbitrage.py  # Scalp strategy implementation
│   ├── carry_strategy.py       # Carry strategy implementation
│   └── carry_storage.py        # Carry position persistence
├── venues/
│   ├── hyperliquid.py      # Hyperliquid connector
│   ├── aster.py            # Aster connector
│   └── lighter.py          # Lighter connector
├── run/
│   ├── perp_perp.py        # Main runner/orchestrator
│   └── flat.py             # Position flattening utility
├── logs/                   # Runtime logs (gitignored)
├── data/                   # Persistent data (gitignored)
├── .env                    # Credentials (gitignored)
├── .env.example            # Credential template
├── requirements.txt        # Python dependencies
└── start_bot.ps1           # Windows launch script
```

## Installation

### Prerequisites
- Python 3.11+
- Windows 10/11 (tested) or Linux
- API credentials for at least 2 venues

### Setup

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/Bot_V3.git
cd Bot_V3
```

2. **Create virtual environment**
```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
# or: source .venv/bin/activate  # Linux
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure credentials**
```bash
cp .env.example .env
# Edit .env with your API keys
```

5. **Generate license (for live mode)**
```bash
python -m core.license github@jbouix.com 365
# Save the generated key to data/license.key or .env
```

## Configuration

### `.env` - API Credentials

```env
# Hyperliquid
HL_PRIVATE_KEY=your_private_key_hex
HL_USER_ADDRESS=0xYourWalletAddress

# Aster
ASTER_API_KEY=your_api_key
ASTER_API_SECRET=your_api_secret

# Lighter
LIGHTER_API_KEY=your_api_key
LIGHTER_PRIVATE_KEY=your_private_key
LIGHTER_ACCOUNT_INDEX=your_account_index

# License (required for live mode)
BOT_LICENSE_KEY=your_license_key
```

### `config/risk.yaml` - Strategy Parameters

Key parameters to adjust:

```yaml
# Global
paper_mode: true              # Start in paper mode for testing

# Scalp Strategy
spread_entry_bps: 40.0        # Minimum edge to enter (bps)
slippage_bps_buffer: 2.0      # Safety buffer for slippage
alloc_frac_per_trade: 0.05    # 5% of equity per trade

# Carry Strategy
carry:
  paper_mode: true
  min_funding_bps: 50.0       # Minimum funding differential
  hold_min_hours: 8.0         # Minimum hold time
  max_carry_total_alloc_pct: 0.50  # Max 50% in carry positions
```

## Usage

### Quick Start (Windows)

```powershell
# Paper mode (both strategies)
.\start_bot.ps1

# Carry LIVE, Scalp PAPER
.\start_bot.ps1 --carry-live

# Scalp LIVE, Carry PAPER
.\start_bot.ps1 --scalp-live

# Auto-start without prompts
.\start_bot.ps1 --auto --carry-live
```

### Manual Start

```bash
python -m run.perp_perp --auto
```

### Dashboard

The bot includes a web dashboard accessible at `http://localhost:8081` showing:
- Real-time equity across all venues
- Active positions and P&L
- Opportunity scanner with live edges
- Strategy performance metrics

## Strategies Deep Dive

### Scalp Strategy Flow

```
1. Discovery  -> Map trading pairs across venues
2. Subscribe  -> WebSocket feeds for real-time prices
3. Scan       -> Calculate edges: (V1_bid - V2_ask) / mid
4. Validate   -> Check: edge > fees + slippage + buffer
5. Execute    -> Dual IOC orders (parallel)
6. Verify     -> Confirm fills, flatten residuals if mismatch
7. Monitor    -> PositionManager tracks for exit conditions
```

**Exit Conditions:**
- Take Profit: Captured 15%+ of entry spread
- Stop Loss: -100 bps basis move
- Timeout: 10 minutes max hold

### Carry Strategy Flow

```
1. Fetch Rates  -> Get funding rates from all venues
2. Calculate    -> Normalize rates, find best differential
3. Validate     -> net_yield = diff - fees - slippage > threshold
4. Enter        -> Open delta-neutral position (Long V1 / Short V2)
5. Accrue       -> Track funding collected over time
6. Monitor      -> MTM check every 30s
7. Exit         -> On reversal, stop-loss, take-profit, or timeout
```

**Exit Conditions:**
- Funding Reversal: Rate differential flipped
- MTM Stop: -100 bps basis widening
- MTM Take Profit: +400 bps basis improvement
- Trailing Stop: Dropped 150 bps from peak
- Timeout: 24 hours max hold

## Risk Management

### Built-in Protections

1. **Kill Switch**: Auto-flat if 3 consecutive equity drops
2. **Symbol Cooldown**: 10s between trades on same symbol
3. **Max Allocation**: Limits per strategy and per symbol
4. **Orphan Detection**: Auto-close unhedged positions
5. **Stale Data Check**: Reject trades if BBO > 500ms old

### Recommended Settings (Conservative)

```yaml
# Start with these for testing
spread_entry_bps: 35.0
slippage_bps_buffer: 5.0
max_scalp_total_alloc_pct: 0.20
carry.max_carry_total_alloc_pct: 0.30
scalp_management.stop_loss_bps: 50.0
```

## Known Limitations

1. **Lighter**: Order execution may be slow; verify fills carefully
2. **Weekend Gaps**: Reduced liquidity can cause slippage
3. **Flash Crashes**: Parallel execution can't fully protect against microsecond moves
4. **API Limits**: Each venue has rate limits; bot includes backoff logic

## Troubleshooting

### Common Issues

**"No pairs discovered"**
- Check API credentials in .env
- Verify network connectivity to exchanges
- Check `min_volume_24h_usd` threshold in risk.yaml

**"Live mode blocked"**
- Generate and install a valid license key
- Verify BOT_LICENSE_KEY in .env or data/license.key

**"Order failed: BBO unavailable"**
- WebSocket connection may be stale
- Restart the bot to reconnect

**High memory usage**
- Reduce `--hb-rows` parameter
- Limit number of subscribed pairs

## Development

### Running Tests

```bash
python -m pytest tests/
```

### Code Structure

- **Venues**: Implement `place_order`, `cancel_order`, `get_positions`, `equity`, `get_bbo`
- **Strategies**: Implement `tick()` async method called by runner
- **Runner**: Orchestrates discovery, heartbeat, strategy ticks, dashboard updates

## License

This software requires a valid license key for live trading mode.
Paper mode is available without restriction for testing.

Contact: github@jbouix.com

## Disclaimer

**USE AT YOUR OWN RISK**

Trading cryptocurrencies and derivatives carries significant risk. This software is provided "as-is" without warranty. The authors are not responsible for any financial losses incurred through the use of this software.

Always:
- Test thoroughly in paper mode first
- Start with small position sizes
- Monitor the bot actively when live
- Keep API keys secure and never commit them to version control
