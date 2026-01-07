# start_bot.ps1 - Launch the trading bot
# Usage: .\start_bot.ps1 [--auto] [--scalp-live] [--carry-live]
#
# IMPORTANT: This script reads .env but NEVER modifies it.
# Edit .env manually if you need to change credentials.

param(
    [switch]$auto,
    [switch]$scalpLive,
    [switch]$carryLive,
    [switch]$help
)

$ErrorActionPreference = "Stop"

if ($help) {
    Write-Host "Usage: .\start_bot.ps1 [options]"
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  --auto        Skip confirmations and start immediately"
    Write-Host "  --scalp-live  Enable LIVE mode for Scalp strategy"
    Write-Host "  --carry-live  Enable LIVE mode for Carry strategy"
    Write-Host "  --help        Show this help message"
    Write-Host ""
    Write-Host "Examples:"
    Write-Host "  .\start_bot.ps1                    # Paper mode for both strategies"
    Write-Host "  .\start_bot.ps1 --carry-live       # Carry LIVE, Scalp PAPER"
    Write-Host "  .\start_bot.ps1 --scalp-live       # Scalp LIVE, Carry PAPER"
    Write-Host "  .\start_bot.ps1 --auto --carry-live # Auto-start with Carry LIVE"
    exit 0
}

Write-Host "================================" -ForegroundColor Cyan
Write-Host "   BOT TRADING - LAUNCH SCRIPT  " -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

# Kill existing instances on port 8081
try {
    $existing = Get-NetTCPConnection -LocalPort 8081 -ErrorAction SilentlyContinue
    if ($existing) {
        $pid_to_kill = $existing.OwningProcess
        Write-Host "[INFO] Found existing bot process (PID: $pid_to_kill) on port 8081. Killing..." -ForegroundColor Yellow
        Stop-Process -Id $pid_to_kill -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 1
        Write-Host "[OK] Existing bot process terminated." -ForegroundColor Green
    }
}
catch {
    # Ignore errors if no process found
}

# Activate virtual environment
if (Test-Path ".venv\Scripts\Activate.ps1") {
    & .\.venv\Scripts\Activate.ps1
    Write-Host "[OK] Virtual environment activated" -ForegroundColor Green
}
else {
    Write-Host "[ERROR] .venv not found! Run: python -m venv .venv && pip install -r requirements.txt" -ForegroundColor Red
    exit 1
}

# Load .env file (READ-ONLY - never modified by this script)
if (Test-Path ".env") {
    Write-Host "[INFO] Loading .env file..." -ForegroundColor Gray
    Get-Content ".env" | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim()
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
    Write-Host "[OK] Environment variables loaded from .env" -ForegroundColor Green
}
else {
    Write-Host "[ERROR] .env file not found!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please create .env from the template:" -ForegroundColor Yellow
    Write-Host "  1. Copy .env.example to .env" -ForegroundColor Gray
    Write-Host "  2. Edit .env with your API credentials" -ForegroundColor Gray
    Write-Host ""
    exit 1
}

# Check required credentials (READ-ONLY - just verify, never prompt or modify)
Write-Host ""
Write-Host "[CREDENTIALS CHECK]" -ForegroundColor Cyan

$missing_required = $false

# Hyperliquid (required)
$hl_key = [Environment]::GetEnvironmentVariable("HL_PRIVATE_KEY")
$hl_addr = [Environment]::GetEnvironmentVariable("HL_USER_ADDRESS")
if (-not $hl_key -or $hl_key -match "your_|example|here" -or $hl_key.Length -lt 10) {
    Write-Host "[MISSING] HL_PRIVATE_KEY - Edit .env to add your Hyperliquid private key" -ForegroundColor Red
    $missing_required = $true
}
else {
    Write-Host "[OK] Hyperliquid credentials" -ForegroundColor Green
}

# Aster (optional)
$aster_key = [Environment]::GetEnvironmentVariable("ASTER_API_KEY")
if (-not $aster_key -or $aster_key -match "your_|example|here") {
    Write-Host "[SKIP] Aster not configured (optional)" -ForegroundColor Gray
}
else {
    Write-Host "[OK] Aster credentials" -ForegroundColor Green
}

# Lighter (optional)
$lt_key = [Environment]::GetEnvironmentVariable("LIGHTER_API_KEY")
if (-not $lt_key -or $lt_key -match "your_|example|here") {
    Write-Host "[SKIP] Lighter not configured (optional)" -ForegroundColor Gray
}
else {
    Write-Host "[OK] Lighter credentials" -ForegroundColor Green
}

# License check for live mode
if ($scalpLive -or $carryLive) {
    $license = [Environment]::GetEnvironmentVariable("BOT_LICENSE_KEY")
    if (-not $license -or $license -match "example|your_") {
        Write-Host "[ERROR] Live mode requires BOT_LICENSE_KEY in .env" -ForegroundColor Red
        Write-Host "  Generate with: python -m core.license github@jbouix.com 365" -ForegroundColor Gray
        $missing_required = $true
    }
    else {
        Write-Host "[OK] License key found" -ForegroundColor Green
    }
}

# Exit if required credentials missing
if ($missing_required) {
    Write-Host ""
    Write-Host "[ABORT] Missing required credentials. Edit .env and try again." -ForegroundColor Red
    exit 1
}

# Update risk.yaml based on flags
if ($scalpLive -or $carryLive) {
    Write-Host ""
    Write-Host "[CONFIG] Updating strategy modes..." -ForegroundColor Cyan

    $riskPath = "config\risk.yaml"
    if (Test-Path $riskPath) {
        $riskContent = Get-Content $riskPath -Raw

        if ($scalpLive) {
            $riskContent = $riskContent -replace "paper_mode:\s*true", "paper_mode: false"
            Write-Host "  [OK] Scalp mode set to LIVE" -ForegroundColor Green
        }
        else {
            Write-Host "  [INFO] Scalp mode: PAPER" -ForegroundColor Gray
        }

        if ($carryLive) {
            # Update carry section
            $riskContent = $riskContent -replace "(carry:[\s\S]*?)paper_mode:\s*true", '$1paper_mode: false'
            Write-Host "  [OK] Carry mode set to LIVE" -ForegroundColor Green
        }
        else {
            Write-Host "  [INFO] Carry mode: PAPER" -ForegroundColor Gray
        }

        Set-Content $riskPath $riskContent
    }
}

# Display mode summary
Write-Host ""
Write-Host "================================" -ForegroundColor Cyan
Write-Host "   STRATEGY MODES SUMMARY       " -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
if ($scalpLive) {
    Write-Host "   Scalp: " -NoNewline; Write-Host "LIVE" -ForegroundColor Red
}
else {
    Write-Host "   Scalp: " -NoNewline; Write-Host "PAPER" -ForegroundColor Green
}
if ($carryLive) {
    Write-Host "   Carry: " -NoNewline; Write-Host "LIVE" -ForegroundColor Red
}
else {
    Write-Host "   Carry: " -NoNewline; Write-Host "PAPER" -ForegroundColor Green
}
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

# Confirmation before start
if (-not $auto) {
    $confirm = Read-Host "Press Enter to start the bot (or Ctrl+C to cancel)"
}

# Launch command
$cmd = ".\.venv\Scripts\python -m run.perp_perp"
if ($auto) {
    $cmd += " --auto"
}

Write-Host ""
Write-Host "Launching: $cmd" -ForegroundColor Yellow
Write-Host ""

# Execute
if (-not (Test-Path "logs")) { New-Item -ItemType Directory -Path "logs" | Out-Null }
$log_file = "logs\terminal_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
Write-Host "[INFO] Logging terminal output to: $log_file" -ForegroundColor Gray

# Use & to execute the command string and Tee-Object to split output
Invoke-Expression "$cmd *>&1 | Tee-Object -FilePath `"$log_file`""
