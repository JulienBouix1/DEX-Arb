#!/bin/bash
# ============================================================================
# Trading Bot Launcher - Mac/Linux/ARM
# ============================================================================
# Equivalent to start_bot.ps1 for Unix systems (including Mac M1/M2)
#
# USAGE:
#   ./start_bot.sh                    # Start with default config (paper mode)
#   ./start_bot.sh --carry-live       # Start with carry in live mode
#   ./start_bot.sh --scalp-live       # Start with scalp in live mode
#   ./start_bot.sh --auto             # Skip confirmation prompt
#
# ============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo ""
echo "============================================"
echo "     TRADING BOT LAUNCHER (Unix/ARM)"
echo "============================================"
echo ""

# Parse arguments
CARRY_LIVE=false
SCALP_LIVE=false
AUTO_MODE=false

for arg in "$@"; do
    case $arg in
        --carry-live)
            CARRY_LIVE=true
            shift
            ;;
        --scalp-live)
            SCALP_LIVE=true
            shift
            ;;
        --auto)
            AUTO_MODE=true
            shift
            ;;
    esac
done

# Check Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}ERROR: Python 3 not found!${NC}"
    echo "Install with: brew install python3"
    exit 1
fi

PYTHON_VERSION=$(python3 --version 2>&1)
echo -e "Python: ${GREEN}$PYTHON_VERSION${NC}"

# Check if we're in the right directory
if [ ! -f "run/perp_perp.py" ]; then
    echo -e "${RED}ERROR: run/perp_perp.py not found!${NC}"
    echo "Please run this script from the bot root directory."
    exit 1
fi

# Load .env if exists
if [ -f ".env" ]; then
    echo "Loading .env file..."
    set -a
    source .env
    set +a
fi

# Check credentials
echo ""
echo "Checking credentials..."

if [ -z "$HL_PRIVATE_KEY" ]; then
    echo -e "${RED}ERROR: HL_PRIVATE_KEY not set in .env${NC}"
    exit 1
fi
echo -e "  HL_PRIVATE_KEY: ${GREEN}Set${NC}"

if [ -z "$ASTER_API_KEY" ]; then
    echo -e "${YELLOW}WARNING: ASTER_API_KEY not set${NC}"
fi

# Kill existing process on port 8080 (dashboard)
if lsof -i:8080 &> /dev/null; then
    echo ""
    echo -e "${YELLOW}Killing existing process on port 8080...${NC}"
    lsof -ti:8080 | xargs kill -9 2>/dev/null || true
fi

# Update risk.yaml for live mode if requested
if [ "$CARRY_LIVE" = true ] || [ "$SCALP_LIVE" = true ]; then
    echo ""
    echo -e "${YELLOW}Updating config/risk.yaml for LIVE mode...${NC}"

    if [ "$SCALP_LIVE" = true ]; then
        # Set paper_mode: false for scalp
        if [[ "$OSTYPE" == "darwin"* ]]; then
            sed -i '' 's/^paper_mode: true/paper_mode: false/' config/risk.yaml
        else
            sed -i 's/^paper_mode: true/paper_mode: false/' config/risk.yaml
        fi
        echo -e "  Scalp: ${RED}LIVE${NC}"
    fi

    if [ "$CARRY_LIVE" = true ]; then
        # Set carry.paper_mode: false (this is trickier with sed, using Python)
        python3 -c "
import yaml
with open('config/risk.yaml') as f:
    cfg = yaml.safe_load(f)
if 'carry' not in cfg:
    cfg['carry'] = {}
cfg['carry']['paper_mode'] = False
with open('config/risk.yaml', 'w') as f:
    yaml.dump(cfg, f, default_flow_style=False)
print('  carry.paper_mode set to false')
"
        echo -e "  Carry: ${RED}LIVE${NC}"
    fi
fi

# Display mode
echo ""
echo "============================================"
if [ "$CARRY_LIVE" = true ] || [ "$SCALP_LIVE" = true ]; then
    echo -e "${RED}   MODE: LIVE TRADING${NC}"
else
    echo -e "${GREEN}   MODE: PAPER TRADING${NC}"
fi
echo "============================================"
echo ""

# Confirmation
if [ "$AUTO_MODE" = false ]; then
    read -p "Press Enter to start the bot (Ctrl+C to cancel)..."
fi

# Start bot
echo ""
echo "Starting bot..."
echo ""

# Create logs directory
mkdir -p logs/scalp logs/carry

# Run
exec python3 -m run.perp_perp "$@"
