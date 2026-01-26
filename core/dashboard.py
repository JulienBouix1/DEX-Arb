# core/dashboard.py
import asyncio
import logging
import json
import time
from typing import Dict, Any, List, Optional, Callable, Awaitable
from aiohttp import web

log = logging.getLogger("dashboard")

class Dashboard:
    def __init__(self, port: int = 8080):
        self.port = port
        self.app = web.Application()
        self.app.router.add_get("/", self._handle_index)
        self.app.router.add_get("/api/state", self._handle_state)
        self.app.router.add_post("/api/emergency_stop", self._handle_emergency_stop)
        self.app.router.add_post("/api/close_all_positions", self._handle_close_positions)
        self.app.router.add_post("/api/close_position", self._handle_close_position)
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None

        # Callbacks for emergency actions (set by runner)
        self._emergency_stop_callback: Optional[Callable[[], Awaitable[None]]] = None
        self._close_positions_callback: Optional[Callable[[], Awaitable[dict]]] = None
        self._close_position_callback: Optional[Callable[[str], Awaitable[dict]]] = None

        # State
        self.state: Dict[str, Any] = {
            "equity": {"hl": 0.0, "as": 0.0, "lt": 0.0, "tot": 0.0, "ts": 0.0},  # Real venue equity
            "start_equity": 0.0,  # Real equity at bot start
            "equity_history": [], # List of {ts, val, hl, as}
            "history": [],  # List of Scalp trades
            "scalp_stats": {"trades": 0, "wins": 0, "losses": 0, "pnl_bps": 0.0, "session_pnl_usd": 0.0},
            "scalp_paper_equity": 1000.0,  # Paper equity for Scalp (separate from real)
            "open_positions": [],
            "carry_positions": [],  # List of Carry positions
            "carry_stats": {"entries": 0, "exits": 0, "wins": 0, "losses": 0, "paper_pnl_usd": 0.0, "session_pnl_usd": 0.0, "equity": 1000.0, "realized_pnl": 0.0, "unrealized_mtm": 0.0, "session_realized_pnl": 0.0, "session_start_equity": 0.0, "session_equity_mtm": 0.0},
            "scalp_pairs": [],  # List of symbols active for Scalp
            "carry_pairs": [],  # List of symbols active for Carry
            "funding_rates": [],  # List of {symbol, hl_rate, as_rate, lt_rate, spread, direction}
            "blocked_pairs": [],  # List of {symbol, reason, until_ts} for auto-blocklisted pairs
            "entry_attempts": [],  # List of carry entry attempts (failures, partial fills, orphans)
            "session_pnl": {"scalp_usd": 0.0, "carry_usd": 0.0, "global_usd": 0.0},  # Session PnL
            "strategy_modes": {"scalp": "PAPER", "carry": "PAPER"},
            "strategy_allocs": {"scalp": 0.50, "carry": 0.80},
            "equity_hl": 0.0,
            "status": "booting"
        }

    async def start(self) -> None:
        self.runner = web.AppRunner(self.app, access_log=None)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "0.0.0.0", self.port)
        await self.site.start()
        log.info(f"[Dashboard] Listening on http://localhost:{self.port}")

    async def stop(self) -> None:
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()

    def update_equity(self, hl: float, ash: float, lighter: float, tot: float) -> None:
        ts = time.time()
        self.state["equity"] = {
            "hl": hl,
            "as": ash,
            "lt": lighter,
            "tot": tot,
            "ts": ts
        }
        # Append to history (keep last 1440 points = 4h at 10s interval, or adjust as needed)
        self.state["equity_history"].append({
            "t": ts,
            "v": tot,
            "h": hl,
            "a": ash,
            "l": lighter
        })
        MAX_HIST = 2000
        if len(self.state["equity_history"]) > MAX_HIST:
             self.state["equity_history"] = self.state["equity_history"][-MAX_HIST:]

    def add_trade(self, trade: Dict[str, Any]) -> None:
        # trade: {timestamp, symbol, direction, net_adj_bps, filled_hl, filled_as, ...}
        self.state["history"].insert(0, trade)
        self.state["history"] = self.state["history"][:50]  # Keep last 50
        
        # Update scalp stats
        pnl = float(trade.get("net_adj_bps", 0))
        self.state["scalp_stats"]["trades"] += 1
        self.state["scalp_stats"]["pnl_bps"] += pnl
        if pnl > 0:
            self.state["scalp_stats"]["wins"] += 1
        elif pnl < 0:
            self.state["scalp_stats"]["losses"] += 1

    def update_positions(self, positions: List[Dict[str, Any]]) -> None:
        self.state["open_positions"] = positions

    def update_pairs(self, pairs_data: List[Dict[str, Any]]) -> None:
        """
        pairs_data: list of dict {symbol, hl_price, as_price, edge_bps, status}
        """
        self.state["pairs"] = pairs_data

    def update_carry_positions(self, positions: List[Dict[str, Any]]) -> None:
        """Update Carry strategy paper positions for display."""
        self.state["carry_positions"] = positions

    def update_carry_stats(self, entries: int, equity: float, realized_pnl: float = 0.0,
                           exits: int = 0, wins: int = 0, losses: int = 0,
                           unrealized_mtm: float = 0.0, session_realized_pnl: float = 0.0,
                           session_start_equity: float = 0.0, session_equity_mtm: float = 0.0) -> None:
        """Update stats for Carry strategy.

        PHASE 2 FIX (2026-01-22): Added unrealized_mtm and session_realized_pnl
        to show true realized PNL separate from open position MTM.
        FIX 2026-01-24: Added session_start_equity and session_equity_mtm tracking.
        """
        self.state["carry_stats"]["entries"] = entries
        self.state["carry_stats"]["exits"] = exits
        self.state["carry_stats"]["wins"] = wins
        self.state["carry_stats"]["losses"] = losses
        self.state["carry_stats"]["equity"] = equity
        self.state["carry_stats"]["realized_pnl"] = realized_pnl
        self.state["carry_stats"]["unrealized_mtm"] = unrealized_mtm
        self.state["carry_stats"]["session_realized_pnl"] = session_realized_pnl
        self.state["carry_stats"]["session_start_equity"] = session_start_equity
        self.state["carry_stats"]["session_equity_mtm"] = session_equity_mtm

    def update_pair_lists(self, scalp_pairs: List[str], carry_pairs: List[str]) -> None:
        """Update lists of pairs active for each strategy (for color coding)."""
        self.state["scalp_pairs"] = scalp_pairs
        self.state["carry_pairs"] = carry_pairs

    def update_blocked_pairs(self, blocked: List[Dict[str, Any]]) -> None:
        """Update list of auto-blocklisted pairs for Carry display.

        Args:
            blocked: List of dicts with {symbol, reason, until_ts, remaining_hours}
        """
        self.state["blocked_pairs"] = blocked

    def update_entry_attempts(self, attempts: List[Dict[str, Any]]) -> None:
        """Update carry entry attempts for dashboard display (failures, partial fills, orphans).

        Args:
            attempts: List of dicts with {timestamp, symbol, v1, v2, v1_status, v2_status, orphan_created, orphan_closed, reason}
        """
        self.state["entry_attempts"] = attempts

    def update_session_pnl(self, scalp_usd: float = None, carry_usd: float = None,
                           carry_realized_usd: float = None, carry_unrealized_usd: float = None) -> None:
        """Update session PnL values. Pass None to keep existing value.

        PHASE 2 FIX (2026-01-22): Added separate realized and unrealized tracking.
        - carry_usd: Legacy total (realized + unrealized) for backwards compatibility
        - carry_realized_usd: Only closed trade PNL
        - carry_unrealized_usd: Open position MTM
        """
        if scalp_usd is not None:
            self.state["session_pnl"]["scalp_usd"] = scalp_usd
            self.state["scalp_stats"]["session_pnl_usd"] = scalp_usd
        if carry_usd is not None:
            self.state["session_pnl"]["carry_usd"] = carry_usd
            self.state["carry_stats"]["session_pnl_usd"] = carry_usd
        if carry_realized_usd is not None:
            self.state["carry_stats"]["session_realized_pnl"] = carry_realized_usd
        if carry_unrealized_usd is not None:
            self.state["carry_stats"]["unrealized_mtm"] = carry_unrealized_usd
        # Auto-calculate global
        s = self.state["session_pnl"]["scalp_usd"]
        c = self.state["session_pnl"]["carry_usd"]
        self.state["session_pnl"]["global_usd"] = s + c

    def set_strategy_info(self, scalp_mode: str, carry_mode: str, scalp_alloc: float, carry_alloc: float) -> None:
        """Set the display mode and allocation limits for each strategy."""
        self.state["strategy_modes"]["scalp"] = scalp_mode.upper()
        self.state["strategy_modes"]["carry"] = carry_mode.upper()
        self.state["strategy_allocs"]["scalp"] = scalp_alloc
        self.state["strategy_allocs"]["carry"] = carry_alloc

    def update_scalp_paper_equity(self, equity: float) -> None:
        """Update Scalp paper equity (separate from real venue equity)."""
        self.state["scalp_paper_equity"] = equity

    def update_funding_rates(self, rates: List[Dict[str, Any]]) -> None:
        """Update funding rates for Carry tab display."""
        self.state["funding_rates"] = rates

    def set_emergency_stop_callback(self, callback: Callable[[], Awaitable[None]]) -> None:
        """Set callback for emergency stop button."""
        self._emergency_stop_callback = callback

    def set_close_positions_callback(self, callback: Callable[[], Awaitable[dict]]) -> None:
        """Set callback for close all positions button."""
        self._close_positions_callback = callback

    def set_close_position_callback(self, callback: Callable[[str], Awaitable[dict]]) -> None:
        """Set callback for manual close single position button."""
        self._close_position_callback = callback

    async def _handle_state(self, request: web.Request) -> web.Response:
        return web.json_response(self.state)

    async def _handle_emergency_stop(self, request: web.Request) -> web.Response:
        """Handle emergency stop request - closes positions and stops bot."""
        log.warning("[Dashboard] EMERGENCY STOP triggered from web interface!")
        self.state["status"] = "stopping"

        result = {"success": False, "message": "No callback configured"}

        try:
            # First close all positions
            if self._close_positions_callback:
                log.info("[Dashboard] Closing all positions...")
                close_result = await self._close_positions_callback()
                result["close_result"] = close_result

            # Then trigger stop
            if self._emergency_stop_callback:
                log.info("[Dashboard] Triggering emergency stop...")
                await self._emergency_stop_callback()
                result["success"] = True
                result["message"] = "Emergency stop triggered. Positions closed, bot stopping."
            else:
                result["message"] = "Stop callback not configured"

        except Exception as e:
            log.error(f"[Dashboard] Emergency stop error: {e}")
            result["success"] = False
            result["message"] = f"Error: {e}"

        return web.json_response(result)

    async def _handle_close_positions(self, request: web.Request) -> web.Response:
        """Handle close all positions request (without stopping bot)."""
        log.warning("[Dashboard] CLOSE ALL POSITIONS triggered from web interface!")

        result = {"success": False, "message": "No callback configured", "closed": []}

        try:
            if self._close_positions_callback:
                close_result = await self._close_positions_callback()
                result["success"] = True
                result["closed"] = close_result.get("closed", [])
                result["message"] = f"Closed {len(result['closed'])} positions"
            else:
                result["message"] = "Close callback not configured"

        except Exception as e:
            log.error(f"[Dashboard] Close positions error: {e}")
            result["success"] = False
            result["message"] = f"Error: {e}"

        return web.json_response(result)

    async def _handle_close_position(self, request: web.Request) -> web.Response:
        """Handle manual close single position request."""
        result = {"success": False, "message": "No callback configured"}

        try:
            data = await request.json()
            symbol = data.get("symbol", "")
            if not symbol:
                result["message"] = "No symbol provided"
                return web.json_response(result)

            log.warning(f"[Dashboard] MANUAL CLOSE requested for {symbol}")

            if self._close_position_callback:
                close_result = await self._close_position_callback(symbol)
                result["success"] = close_result.get("success", False)
                result["message"] = close_result.get("message", "Position closed")
                result["symbol"] = symbol
            else:
                result["message"] = "Close position callback not configured"

        except Exception as e:
            log.error(f"[Dashboard] Close position error: {e}")
            result["success"] = False
            result["message"] = f"Error: {e}"

        return web.json_response(result)

    async def _handle_index(self, request: web.Request) -> web.Response:
        html = """
<!DOCTYPE html>
<html>
<head>
    <title>Arb Bot Dashboard</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <meta name="apple-mobile-web-app-capable" content="yes">
    <meta name="mobile-web-app-capable" content="yes">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: 'Segoe UI', sans-serif; background: #0f0f13; color: #e0e0e0; padding: 10px; margin: 0; }
        .card { background: #1e1e24; padding: 20px; margin-bottom: 20px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
        h1, h2 { color: #00e676; margin-top: 0; font-weight: 300; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }
        .metric { text-align: center; }
        .metric div:first-child { color: #888; font-size: 0.9em; text-transform: uppercase; }
        .val { font-size: 1.8em; font-weight: 600; color: #fff; }
        table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
        th, td { text-align: left; padding: 10px; border-bottom: 1px solid #333; }
        th { color: #888; font-weight: normal; }
        .red { color: #ff5252; } .green { color: #00e676; } .yellow { color: #ffcc00; }
        .scroll-box { max-height: 400px; overflow-y: auto; }
        ::-webkit-scrollbar { width: 8px; }
        ::-webkit-scrollbar-thumb { background: #333; border-radius: 4px; }
        #chartContainer { position: relative; height: 300px; width: 100%; }

        /* Tab Styles */
        .tabs { display: flex; gap: 0; margin-bottom: 0; }
        .tab { padding: 12px 30px; background: #2a2a32; color: #888; cursor: pointer; border: none;
               font-size: 1em; transition: all 0.2s; border-radius: 8px 8px 0 0; margin-right: 4px; }
        .tab:hover { background: #353540; color: #ccc; }
        .tab.active { background: #1e1e24; color: #fff; font-weight: 500; }
        .tab.active.scalp { border-top: 3px solid #36a2eb; }
        .tab.active.carry { border-top: 3px solid #ff9800; }
        .tab.active.global { border-top: 3px solid #00e676; }
        .tab-content { display: none; }
        .tab-content.active { display: block; }
        .tab-panel { background: #1e1e24; border-radius: 0 12px 12px 12px; padding: 20px; margin-bottom: 20px; }

        /* Emergency Controls */
        .emergency-panel {
            position: fixed; bottom: 0; left: 0; right: 0;
            background: linear-gradient(to top, rgba(15,15,19,0.98), rgba(15,15,19,0.9));
            padding: 15px; z-index: 1000;
            display: flex; gap: 10px; justify-content: center; align-items: center;
            border-top: 1px solid #333;
        }
        .btn-emergency {
            padding: 15px 30px; font-size: 1.1em; font-weight: bold; border: none; border-radius: 8px;
            cursor: pointer; transition: all 0.2s; text-transform: uppercase;
        }
        .btn-stop { background: #d32f2f; color: white; }
        .btn-stop:hover { background: #f44336; transform: scale(1.05); }
        .btn-stop:active { transform: scale(0.95); }
        .btn-close-pos { background: #ff9800; color: white; }
        .btn-close-pos:hover { background: #ffa726; }
        .btn-disabled { background: #555 !important; cursor: not-allowed; }
        #emergency_status { color: #888; font-size: 0.9em; }
        .btn-close-single {
            background: #e53935; color: white; border: none; border-radius: 4px;
            padding: 4px 10px; cursor: pointer; font-size: 0.8em; transition: all 0.2s;
        }
        .btn-close-single:hover { background: #f44336; transform: scale(1.05); }
        .btn-close-single:disabled { background: #555; cursor: not-allowed; }

        /* Mobile responsive */
        @media (max-width: 768px) {
            body { padding: 5px; }
            h1 { font-size: 1.3em; }
            .card { padding: 12px; }
            .val { font-size: 1.4em; }
            .grid { grid-template-columns: repeat(2, 1fr); gap: 10px; }
            .tabs { flex-wrap: wrap; }
            .tab { padding: 10px 15px; font-size: 0.9em; }
            table { font-size: 0.8em; }
            th, td { padding: 6px 4px; }
            #chartContainer { height: 200px; }
            .emergency-panel { flex-wrap: wrap; }
            .btn-emergency { padding: 12px 20px; font-size: 1em; flex: 1; min-width: 120px; }
        }

        /* Add padding at bottom for fixed emergency panel */
        #app { padding-bottom: 80px; }
    </style>
</head>
<body>
    <div id="app">
        <h1>Perp-Perp Arb Bot <span style="font-size:0.5em; color:#666">v3.0</span></h1>

        <!-- Global Equity Summary (Always Visible) -->
        <div class="card grid">
            <div class="metric">
                <div style="color:#00e676">Total Equity</div>
                <div class="val" id="eq_tot" style="color:#00e676">--</div>
            </div>
            <div class="metric">
                <div style="color:#36a2eb">Hyperliquid</div>
                <div class="val" id="eq_hl" style="color:#36a2eb">--</div>
            </div>
            <div class="metric">
                <div style="color:#ff6384">Aster</div>
                <div class="val" id="eq_as" style="color:#ff6384">--</div>
            </div>
            <div class="metric">
                <div style="color:#ba68c8">Lighter</div>
                <div class="val" id="eq_lt" style="color:#ba68c8">--</div>
            </div>
        </div>

        <!-- TABS Navigation -->
        <div class="tabs">
            <button class="tab global active" onclick="showTab('global')">🌐 Global</button>
            <button class="tab scalp" onclick="showTab('scalp')">⚡ Scalp</button>
            <button class="tab carry" onclick="showTab('carry')">📊 Carry</button>
        </div>

        <!-- GLOBAL TAB -->
        <div id="tab-global" class="tab-content active">
            <div class="tab-panel">
                <!-- Equity Chart -->
                <div class="card" style="margin-bottom:0;">
                    <h2>Equity History</h2>
                    <div id="chartContainer">
                        <canvas id="equityChart"></canvas>
                    </div>
                </div>
            </div>

            <!-- PnL Summary -->
            <div class="card" style="padding:15px 20px;">
                <div style="display:flex;gap:30px;font-size:1em;flex-wrap:wrap;justify-content:center;">
                    <div><span style="color:#888">Start:</span> <span style="color:#fff">$<span id="start_eq">--</span></span></div>
                    <div><span style="color:#888">Current:</span> <span style="color:#fff">$<span id="current_eq">--</span></span></div>
                    <div style="border-left:1px solid #444;padding-left:30px;">
                        <span style="color:#36a2eb">Scalp:</span> <span id="scalp_session_pnl">--</span>
                    </div>
                    <div><span style="color:#ff9800">Carry:</span> <span id="carry_session_pnl">--</span></div>
                    <div style="border-left:1px solid #444;padding-left:30px;">
                        <span style="color:#4caf50;font-weight:bold">TOTAL:</span>
                        <span id="global_session_pnl" style="font-weight:bold;font-size:1.1em">--</span>
                    </div>
                </div>
            </div>

            <!-- Strategies Side by Side (Summary) -->
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                <div class="card" style="border-left: 4px solid #36a2eb;">
                    <h2 style="color:#36a2eb">⚡ SCALP <span id="scalp_mode_label" style="font-size:0.6em;color:#888">[-]</span></h2>
                    <div style="display:flex;gap:20px;">
                        <div><span style="color:#888">Trades:</span> <span id="scalp_trades">0</span></div>
                        <div><span style="color:#888">W/L:</span> <span id="scalp_wl" style="color:#00e676">0</span>/<span id="scalp_losses" style="color:#ff5252">0</span> (<span id="scalp_winrate">-</span>)</div>
                        <div><span style="color:#888">PnL:</span> <span id="scalp_pnl" class="green">0 bps</span></div>
                    </div>
                </div>
                <div class="card" style="border-left: 4px solid #ff9800;">
                    <h2 style="color:#ff9800">📊 CARRY <span id="carry_mode_label" style="font-size:0.6em;color:#888">[-]</span></h2>
                    <div style="display:flex;gap:20px;">
                        <div><span style="color:#888">Positions:</span> <span id="carry_count">0</span></div>
                        <div><span style="color:#888">W/L:</span> <span id="carry_wl" style="color:#00e676">0</span>/<span id="carry_losses" style="color:#ff5252">0</span></div>
                        <div><span style="color:#888">Equity:</span> <span id="carry_equity" class="green">$0.00</span></div>
                        <div><span style="color:#888">Unrealized:</span> <span id="carry_unrealized">--</span></div>
                    </div>
                </div>
            </div>
        </div>

        <!-- SCALP TAB -->
        <div id="tab-scalp" class="tab-content">
            <div class="tab-panel" style="border-top: 3px solid #36a2eb;">
                <h2 style="color:#36a2eb;margin-bottom:20px;">⚡ SCALP Strategy Details <span id="scalp_mode_label2" style="font-size:0.6em;color:#888">[-]</span></h2>

                <!-- Scalp Metrics -->
                <div class="grid" style="margin-bottom:20px;">
                    <div class="metric">
                        <div style="color:#888">Paper Equity</div>
                        <div class="val" id="scalp_paper_equity" style="color:#36a2eb">$--</div>
                    </div>
                    <div class="metric">
                        <div style="color:#888">Trades Today</div>
                        <div class="val" id="scalp_trades2">0</div>
                    </div>
                    <div class="metric">
                        <div style="color:#888">W / L</div>
                        <div class="val"><span id="scalp_wl2" style="color:#00e676">0</span> / <span id="scalp_losses2" style="color:#ff5252">0</span></div>
                    </div>
                    <div class="metric">
                        <div style="color:#888">Session PnL</div>
                        <div class="val" id="scalp_session_pnl2" style="color:#00e676">$0.00</div>
                    </div>
                </div>

                <div><span style="color:#888">Allocation:</span> <span id="scalp_alloc" style="color:#36a2eb">--</span> |
                     <span style="color:#888">Active Pairs:</span> <span id="scalp_pair_count">--</span></div>

                <h3 style="color:#888;margin-top:20px;">Recent Trades</h3>
                <div class="scroll-box" style="max-height:300px;">
                    <table id="trades">
                        <thead><tr><th>Time</th><th>Pair</th><th>Direction</th><th>Edge (bps)</th><th>Result</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- CARRY TAB -->
        <div id="tab-carry" class="tab-content">
            <div class="tab-panel" style="border-top: 3px solid #ff9800;">
                <h2 style="color:#ff9800;margin-bottom:20px;">📊 CARRY Strategy Details <span id="carry_mode_label2" style="font-size:0.6em;color:#888">[-]</span></h2>

                <!-- Carry Metrics -->
                <div class="grid" style="margin-bottom:20px;">
                    <div class="metric">
                        <div style="color:#888">Total Equity</div>
                        <div class="val" id="carry_total_equity" style="color:#ff9800">$--</div>
                    </div>
                    <div class="metric">
                        <div style="color:#888">Open Positions</div>
                        <div class="val" id="carry_count2">0</div>
                    </div>
                    <div class="metric">
                        <div style="color:#888">W / L</div>
                        <div class="val"><span id="carry_wl2" style="color:#00e676">0</span> / <span id="carry_losses2" style="color:#ff5252">0</span></div>
                    </div>
                    <div class="metric">
                        <div style="color:#888">Realized PnL</div>
                        <div class="val" id="carry_realized">$0.00</div>
                    </div>
                    <div class="metric">
                        <div style="color:#888">Unrealized MTM</div>
                        <div class="val" id="carry_unrealized2">$0.00</div>
                    </div>
                    <div class="metric">
                        <div style="color:#888">Session Start</div>
                        <div class="val" id="carry_session_start" style="color:#888">$0.00</div>
                    </div>
                    <div class="metric">
                        <div style="color:#888">Session MTM</div>
                        <div class="val" id="carry_session_mtm">$0.00</div>
                    </div>
                </div>

                <!-- Live Mode: Real Equity by Venue -->
                <div id="carry_live_equity" style="display:none;margin-bottom:20px;">
                    <h3 style="color:#888;">Live Equity by Venue</h3>
                    <div class="grid">
                        <div class="metric">
                            <div style="color:#36a2eb">Hyperliquid</div>
                            <div class="val" id="carry_eq_hl" style="color:#36a2eb;font-size:1.4em">$--</div>
                        </div>
                        <div class="metric">
                            <div style="color:#ff6384">Aster</div>
                            <div class="val" id="carry_eq_as" style="color:#ff6384;font-size:1.4em">$--</div>
                        </div>
                        <div class="metric">
                            <div style="color:#ba68c8">Lighter</div>
                            <div class="val" id="carry_eq_lt" style="color:#ba68c8;font-size:1.4em">$--</div>
                        </div>
                    </div>
                </div>

                <div><span style="color:#888">Allocation:</span> <span id="carry_alloc" style="color:#ff9800">--</span> |
                     <span style="color:#888">Entries:</span> <span id="carry_entries">0</span> |
                     <span style="color:#888">Exits:</span> <span id="carry_exits">0</span></div>

                <h3 style="color:#888;margin-top:20px;">Open Positions</h3>
                <div class="scroll-box" style="max-height:250px;">
                    <table id="carry_positions">
                        <thead><tr>
                            <th>Pair</th>
                            <th>Direction</th>
                            <th>Size (V1/V2)</th>
                            <th>Hold</th>
                            <th>Yield</th>
                            <th>Funding</th>
                            <th title="Price impact from entry to now">Price Δ</th>
                            <th>Fees (Paid/Exit)</th>
                            <th>Net MTM</th>
                            <th>Action</th>
                        </tr></thead>
                        <tbody></tbody>
                    </table>
                </div>

                <h3 style="color:#888;margin-top:20px;">Blocked Pairs <span id="blocked_count" style="font-size:0.7em;color:#ff5252;">(0)</span></h3>
                <div class="scroll-box" style="max-height:120px;">
                    <table id="blocked_pairs_table">
                        <thead><tr><th>Pair</th><th>Reason</th><th>Remaining</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>

                <h3 style="color:#ff5252;margin-top:20px;">Entry Attempts <span id="entry_attempts_count" style="font-size:0.7em;">(0)</span></h3>
                <div class="scroll-box" style="max-height:180px;">
                    <table id="entry_attempts_table">
                        <thead><tr><th>Time</th><th>Pair</th><th>V1</th><th>V2</th><th>Orphan</th><th>Reason</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>

                <h3 style="color:#888;margin-top:20px;">Funding Rates (bps/8h)</h3>
                <div class="scroll-box" style="max-height:250px;">
                    <table id="funding_rates_table">
                        <thead><tr><th>Pair</th><th style="color:#36a2eb">HL</th><th style="color:#ff6384">AS</th><th style="color:#ba68c8">LT</th><th>Best Spread</th><th>Direction</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
        </div>

    </div>

    <!-- Emergency Controls (Fixed at Bottom) -->
    <div class="emergency-panel">
        <button id="btn_close_all" class="btn-emergency btn-close-pos" onclick="closeAllPositions()">
            Close All Positions
        </button>
        <button id="btn_emergency_stop" class="btn-emergency btn-stop" onclick="emergencyStop()">
            EMERGENCY STOP
        </button>
        <span id="emergency_status"></span>
    </div>

    <script>
        // Emergency Stop Functions
        async function closeAllPositions() {
            if (!confirm('Are you sure you want to CLOSE ALL POSITIONS?')) return;

            const btn = document.getElementById('btn_close_all');
            const status = document.getElementById('emergency_status');
            btn.classList.add('btn-disabled');
            btn.disabled = true;
            status.innerText = 'Closing positions...';

            try {
                const resp = await fetch('/api/close_all_positions', { method: 'POST' });
                const data = await resp.json();
                status.innerText = data.message;
                status.style.color = data.success ? '#00e676' : '#ff5252';
            } catch (e) {
                status.innerText = 'Error: ' + e.message;
                status.style.color = '#ff5252';
            }

            setTimeout(() => {
                btn.classList.remove('btn-disabled');
                btn.disabled = false;
            }, 3000);
        }

        async function emergencyStop() {
            if (!confirm('EMERGENCY STOP will close all positions and SHUT DOWN the bot. Are you sure?')) return;
            if (!confirm('FINAL CONFIRMATION: This will STOP THE BOT. Continue?')) return;

            const btn = document.getElementById('btn_emergency_stop');
            const status = document.getElementById('emergency_status');
            btn.classList.add('btn-disabled');
            btn.disabled = true;
            status.innerText = 'Stopping bot...';
            status.style.color = '#ff9800';

            try {
                const resp = await fetch('/api/emergency_stop', { method: 'POST' });
                const data = await resp.json();
                status.innerText = data.message;
                status.style.color = data.success ? '#00e676' : '#ff5252';

                if (data.success) {
                    document.body.style.background = '#1a0000';
                    status.innerText = 'BOT STOPPED - Refresh page to reconnect';
                }
            } catch (e) {
                status.innerText = 'Error: ' + e.message;
                status.style.color = '#ff5252';
            }
        }

        async function closePosition(symbol) {
            if (!confirm(`Close position for ${symbol}?`)) return;

            const status = document.getElementById('emergency_status');
            status.innerText = `Closing ${symbol}...`;
            status.style.color = '#ff9800';

            try {
                const resp = await fetch('/api/close_position', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ symbol: symbol })
                });
                const data = await resp.json();
                status.innerText = data.message;
                status.style.color = data.success ? '#00e676' : '#ff5252';
            } catch (e) {
                status.innerText = 'Error: ' + e.message;
                status.style.color = '#ff5252';
            }
        }

        // Chart Setup
        const ctx = document.getElementById('equityChart').getContext('2d');
        const chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    { label: 'Total', data: [], borderColor: '#00e676', borderWidth: 2, tension: 0.1, yAxisID: 'y' },
                    { label: 'HL', data: [], borderColor: '#36a2eb', borderWidth: 1, borderDash: [5,5], tension: 0.1, yAxisID: 'y' },
                    { label: 'Aster', data: [], borderColor: '#ff6384', borderWidth: 1, borderDash: [5,5], tension: 0.1, yAxisID: 'y' },
                    { label: 'Lighter', data: [], borderColor: '#ba68c8', borderWidth: 1, borderDash: [5,5], tension: 0.1, yAxisID: 'y' }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                interaction: { mode: 'index', intersect: false },
                plugins: { legend: { labels: { color: '#ccc' } } },
                scales: {
                    x: { ticks: { color: '#666', maxTicksLimit: 10 } },
                    y: { ticks: { color: '#666' }, grid: { color: '#333' } }
                }
            }
        });

        function update() {
            fetch('/api/state').then(r => r.json()).then(data => {
                // Metrics
                document.getElementById('eq_tot').innerText = '$' + (data.equity.tot || 0).toFixed(2);
                document.getElementById('eq_hl').innerText = '$' + (data.equity.hl || 0).toFixed(2);
                document.getElementById('eq_as').innerText = '$' + (data.equity.as || 0).toFixed(2);
                document.getElementById('eq_lt').innerText = '$' + (data.equity.lt || 0).toFixed(2);
                
                // Chart
                if (data.equity_history && data.equity_history.length > 0) {
                    const times = data.equity_history.map(d => new Date(d.t * 1000).toLocaleTimeString());
                    const tots = data.equity_history.map(d => d.v);
                    const hls = data.equity_history.map(d => d.h);
                    const ass = data.equity_history.map(d => d.a);
                    const lts = data.equity_history.map(d => d.l);
                    
                    chart.data.labels = times;
                    chart.data.datasets[0].data = tots;
                    chart.data.datasets[1].data = hls;
                    chart.data.datasets[2].data = ass;
                    chart.data.datasets[3].data = lts;
                    chart.update();
                }


                // Scalp Trades
                const tbody_trades = document.querySelector('#trades tbody');
                tbody_trades.innerHTML = data.history.map(t => `
                    <tr>
                        <td>${new Date(t.timestamp * 1000).toLocaleTimeString()}</td>
                        <td>${t.symbol}</td>
                        <td>${t.direction}</td>
                        <td class="${t.net_adj_bps > 0 ? 'green' : 'red'}">${t.net_adj_bps.toFixed(1)}</td>
                        <td>${t.v1_label}/${t.v2_label}: ${t.hl_ok && t.as_ok ? 'OK' : 'FAIL'}</td>
                    </tr>
                `).join('');

                // Carry Positions Table (enhanced for carry tab)
                // FIX 2026-01-22: Updated to show per-leg sizes and fee breakdown
                const carry_pos = data.carry_positions || [];
                const tbody_carry = document.querySelector('#carry_positions tbody');
                tbody_carry.innerHTML = carry_pos.map(p => {
                    const mtm = p.mtm_bps || 0;
                    const mtmClass = mtm >= 0 ? 'green' : 'red';
                    const sizeUsd = p.size_usd || 0;
                    const sizeV1 = p.size_v1_usd || (sizeUsd / 2);
                    const sizeV2 = p.size_v2_usd || (sizeUsd / 2);
                    const fundingPnl = p.funding_pnl_usd || 0;
                    const pricePnl = p.price_pnl_usd || 0;
                    const feeCost = p.fee_cost_usd || 0;
                    const totalMtm = p.total_mtm_usd || (fundingPnl + pricePnl - feeCost);

                    // FIX 2026-01-22: Fee breakdown - paid fees vs projected exit fees
                    const feesPaid = p.total_fees_paid_usd || 0;
                    const exitFeeProj = p.exit_fee_projected_usd || 0;
                    const makerLegs = p.maker_legs || 0;
                    const takerLegs = p.taker_legs || 0;
                    const scaleCount = p.scale_up_count || 0;

                    // Show leg venues
                    const leg1 = p.leg1_venue || 'V1';
                    const leg2 = p.leg2_venue || 'V2';

                    // FIX 2026-01-25: Yield display with correct sign handling
                    // Keep raw value for sign detection, show absolute for magnitude
                    const currYieldRaw = p.current_funding_diff_bps ?? p.entry_funding_diff_bps ?? 0;
                    const currYield = Math.abs(currYieldRaw);
                    const yieldReceiving = p.yield_is_receiving !== false && currYieldRaw >= 0;
                    const yieldClass = yieldReceiving ? 'green' : 'red';
                    const yieldSign = yieldReceiving ? '+' : '-';
                    const entryYield = p.entry_funding_diff_bps || 0;

                    // FIX 2026-01-25: Get current prices for tooltip
                    const px1 = p.current_px_leg1 || 0;
                    const px2 = p.current_px_leg2 || 0;

                    return `
                    <tr>
                        <td>${p.symbol}${scaleCount > 0 ? ' <span style="color:#ba68c8;font-size:0.8em">(+' + scaleCount + ')</span>' : ''}</td>
                        <td style="font-size:0.85em">${p.direction}</td>
                        <td style="color:#ffcc00">$${sizeUsd.toFixed(0)} <span style="color:#666;font-size:0.75em">(${leg1}:$${sizeV1.toFixed(0)}/${leg2}:$${sizeV2.toFixed(0)})</span></td>
                        <td>${p.hold_hours.toFixed(1)}h</td>
                        <td class="${yieldClass}">${yieldSign}${currYield.toFixed(1)}bps <span style="color:#666;font-size:0.75em">(e:${entryYield.toFixed(0)})</span></td>
                        <td class="${fundingPnl >= 0 ? 'green' : 'red'}">$${fundingPnl.toFixed(2)}</td>
                        <td class="${pricePnl >= 0 ? 'green' : 'red'}" title="Current: ${leg1}=$${px1.toFixed(4)} / ${leg2}=$${px2.toFixed(4)}">$${pricePnl.toFixed(2)}</td>
                        <td style="color:#ff9800" title="Paid: $${feesPaid.toFixed(2)} (${makerLegs}M/${takerLegs}T legs)&#10;Exit proj: $${exitFeeProj.toFixed(2)} (taker)">-$${feeCost.toFixed(2)} <span style="font-size:0.7em">(${feesPaid.toFixed(2)}+${exitFeeProj.toFixed(2)})</span></td>
                        <td class="${mtmClass}">$${totalMtm.toFixed(2)} <span style="font-size:0.75em">(${mtm.toFixed(1)}bps)</span></td>
                        <td><button class="btn-close-single" onclick="closePosition('${p.symbol}')">Close</button></td>
                    </tr>`;
                }).join('');

                // Funding Rates Table (Carry tab)
                const funding_rates = data.funding_rates || [];
                const tbody_funding = document.querySelector('#funding_rates_table tbody');
                if (tbody_funding) {
                    tbody_funding.innerHTML = funding_rates.map(r => {
                        const hl = r.hl_rate || 0;
                        const as_r = r.as_rate || 0;
                        const lt = r.lt_rate || 0;
                        const spread = r.spread || 0;
                        const dir = r.direction || '-';
                        const spreadClass = Math.abs(spread) >= 50 ? 'green' : (Math.abs(spread) >= 20 ? 'yellow' : '');
                        return `
                        <tr>
                            <td>${r.symbol}</td>
                            <td style="color:#36a2eb">${hl.toFixed(1)}</td>
                            <td style="color:#ff6384">${as_r.toFixed(1)}</td>
                            <td style="color:#ba68c8">${lt ? lt.toFixed(1) : '-'}</td>
                            <td class="${spreadClass}">${spread.toFixed(1)}</td>
                            <td>${dir}</td>
                        </tr>`;
                    }).join('');
                }

                // Calculate total unrealized MTM
                let totalUnrealized = 0;
                carry_pos.forEach(p => { totalUnrealized += (p.total_mtm_usd || 0); });

                // Get strategy modes
                const sm = data.strategy_modes || {scalp: 'PAPER', carry: 'PAPER'};
                const scalp_is_paper = (sm.scalp === 'PAPER');
                const carry_is_live = (sm.carry === 'LIVE');

                // Scalp Stats
                const ss = data.scalp_stats || {};
                const wins = ss.wins || 0;
                const losses = ss.losses || 0;
                document.getElementById('scalp_trades').innerText = ss.trades || 0;
                const wr = ss.trades > 0 ? ((wins / ss.trades) * 100).toFixed(0) + '%' : '-';
                document.getElementById('scalp_winrate').innerText = wr;
                document.getElementById('scalp_wl').innerText = wins;
                document.getElementById('scalp_losses').innerText = losses;
                const scalp_pnl = ss.pnl_bps || 0;
                const scalp_el = document.getElementById('scalp_pnl');
                scalp_el.innerText = scalp_pnl.toFixed(1) + ' bps';
                scalp_el.className = scalp_pnl >= 0 ? 'green' : 'red';

                // Scalp Tab extra fields
                const scalp_trades2 = document.getElementById('scalp_trades2');
                if (scalp_trades2) scalp_trades2.innerText = ss.trades || 0;
                const scalp_wl2 = document.getElementById('scalp_wl2');
                if (scalp_wl2) scalp_wl2.innerText = wins;
                const scalp_losses2 = document.getElementById('scalp_losses2');
                if (scalp_losses2) scalp_losses2.innerText = losses;

                // Scalp Paper Equity (always use paper equity for scalp tab)
                const scalp_paper_eq = data.scalp_paper_equity || 1000.0;
                const scalp_paper_el = document.getElementById('scalp_paper_equity');
                if (scalp_paper_el) {
                    scalp_paper_el.innerText = '$' + scalp_paper_eq.toFixed(2);
                }

                // Carry Stats - Use REAL equity if LIVE, paper if PAPER
                const cs = data.carry_stats || {};
                const carry_wins = cs.wins || 0;
                const carry_losses_val = cs.losses || 0;
                document.getElementById('carry_count').innerText = carry_pos.length;
                document.getElementById('carry_entries').innerText = cs.entries || 0;
                document.getElementById('carry_exits').innerText = cs.exits || 0;
                document.getElementById('carry_wl').innerText = carry_wins;
                document.getElementById('carry_losses').innerText = carry_losses_val;

                // Carry equity: real (tot) if LIVE, paper if PAPER
                const carry_eq = carry_is_live ? (data.equity.tot || 0) : (cs.equity || 1000.0);
                const carry_eq_base = carry_is_live ? (data.start_equity || carry_eq) : 1000.0;
                const carry_el = document.getElementById('carry_equity');
                carry_el.innerText = '$' + carry_eq.toFixed(2);
                carry_el.className = carry_eq >= carry_eq_base ? 'green' : 'red';

                // Carry unrealized on global tab
                const carry_unreal = document.getElementById('carry_unrealized');
                if (carry_unreal) {
                    carry_unreal.innerText = (totalUnrealized >= 0 ? '+' : '') + '$' + totalUnrealized.toFixed(2);
                    carry_unreal.className = totalUnrealized >= 0 ? 'green' : 'red';
                }

                // Carry Tab extra fields
                const carry_count2 = document.getElementById('carry_count2');
                if (carry_count2) carry_count2.innerText = carry_pos.length;
                const carry_wl2 = document.getElementById('carry_wl2');
                if (carry_wl2) carry_wl2.innerText = carry_wins;
                const carry_losses2 = document.getElementById('carry_losses2');
                if (carry_losses2) carry_losses2.innerText = carry_losses_val;
                const carry_total_eq = document.getElementById('carry_total_equity');
                if (carry_total_eq) {
                    carry_total_eq.innerText = '$' + carry_eq.toFixed(2);
                }
                const carry_unreal2 = document.getElementById('carry_unrealized2');
                if (carry_unreal2) {
                    carry_unreal2.innerText = (totalUnrealized >= 0 ? '+' : '') + '$' + totalUnrealized.toFixed(2);
                    carry_unreal2.className = totalUnrealized >= 0 ? 'green' : 'red';
                }
                const carry_realized = document.getElementById('carry_realized');
                if (carry_realized) {
                    const realized = cs.realized_pnl || 0;
                    carry_realized.innerText = (realized >= 0 ? '+' : '') + '$' + realized.toFixed(2);
                    carry_realized.className = realized >= 0 ? 'green' : 'red';
                }

                // FIX 2026-01-24: Session equity display
                const sessionStart = cs.session_start_equity || 0;
                const sessionMtm = cs.session_equity_mtm || 0;

                const carry_session_start = document.getElementById('carry_session_start');
                if (carry_session_start) {
                    carry_session_start.innerText = '$' + sessionStart.toFixed(2);
                }

                const carry_session_mtm = document.getElementById('carry_session_mtm');
                if (carry_session_mtm) {
                    carry_session_mtm.innerText = (sessionMtm >= 0 ? '+' : '') + '$' + sessionMtm.toFixed(2);
                    carry_session_mtm.className = sessionMtm >= 0 ? 'green' : 'red';
                }

                // Show live equity panel only if carry is LIVE mode (use carry_is_live from above)
                const livePanel = document.getElementById('carry_live_equity');
                if (livePanel) {
                    livePanel.style.display = carry_is_live ? 'block' : 'none';
                    if (carry_is_live) {
                        document.getElementById('carry_eq_hl').innerText = '$' + (data.equity.hl || 0).toFixed(2);
                        document.getElementById('carry_eq_as').innerText = '$' + (data.equity.as || 0).toFixed(2);
                        document.getElementById('carry_eq_lt').innerText = '$' + (data.equity.lt || 0).toFixed(2);
                    }
                }

                // Blocked Pairs Table
                const blocked = data.blocked_pairs || [];
                const blocked_tbody = document.querySelector('#blocked_pairs_table tbody');
                if (blocked_tbody) {
                    blocked_tbody.innerHTML = blocked.map(b => {
                        const remaining = b.remaining_hours || 0;
                        const remainStr = remaining > 1 ? remaining.toFixed(1) + 'h' : (remaining * 60).toFixed(0) + 'm';
                        return `<tr style="color:#ff5252;">
                            <td>${b.symbol}</td>
                            <td>${b.reason || 'Reversals'}</td>
                            <td>${remainStr}</td>
                        </tr>`;
                    }).join('');
                }
                const blocked_count = document.getElementById('blocked_count');
                if (blocked_count) blocked_count.innerText = '(' + blocked.length + ')';

                // Entry Attempts Table (failures, partial fills, orphans)
                const attempts = data.entry_attempts || [];
                const attempts_tbody = document.querySelector('#entry_attempts_table tbody');
                if (attempts_tbody) {
                    attempts_tbody.innerHTML = attempts.map(a => {
                        const v1_class = a.v1_status.includes("CONFIRMED") || a.v1_status.includes("FILLED") ? "green" : "red";
                        const v2_class = a.v2_status.includes("CONFIRMED") || a.v2_status.includes("FILLED") ? "green" : "red";
                        const orphan_text = a.orphan_created ? (a.orphan_closed ? "CLOSED" : "OPEN!") : "-";
                        const orphan_style = a.orphan_created && !a.orphan_closed ? "color:#ff5252;font-weight:bold;" : "";
                        const time_str = new Date(a.timestamp * 1000).toLocaleTimeString();
                        const age_str = a.age_hours < 1 ? (a.age_hours * 60).toFixed(0) + 'm' : a.age_hours.toFixed(1) + 'h';
                        return `<tr>
                            <td>${time_str} (${age_str} ago)</td>
                            <td>${a.symbol || a.hl_coin}</td>
                            <td class="${v1_class}">${a.v1}: ${a.v1_status}</td>
                            <td class="${v2_class}">${a.v2}: ${a.v2_status}</td>
                            <td style="${orphan_style}">${orphan_text}</td>
                            <td style="color:#888;font-size:0.85em;">${a.reason}</td>
                        </tr>`;
                    }).join('');
                }
                const attempts_count = document.getElementById('entry_attempts_count');
                if (attempts_count) {
                    const open_orphans = attempts.filter(a => a.orphan_created && !a.orphan_closed).length;
                    attempts_count.innerText = '(' + attempts.length + (open_orphans > 0 ? ', ' + open_orphans + ' OPEN!' : '') + ')';
                    attempts_count.style.color = open_orphans > 0 ? '#ff5252' : '#888';
                }

                // Equity Info Bar
                const start = data.start_equity || 0;
                const curr = data.equity.tot || 0;
                document.getElementById('start_eq').innerText = start.toFixed(2);
                document.getElementById('current_eq').innerText = curr.toFixed(2);

                // Session PnL (using data from session_pnl object)
                const sp = data.session_pnl || {scalp_usd: 0, carry_usd: 0, global_usd: 0};

                const scalp_pnl_el = document.getElementById('scalp_session_pnl');
                scalp_pnl_el.innerText = (sp.scalp_usd >= 0 ? '+' : '') + '$' + sp.scalp_usd.toFixed(2);
                scalp_pnl_el.className = sp.scalp_usd >= 0 ? 'green' : 'red';

                const scalp_session2 = document.getElementById('scalp_session_pnl2');
                if (scalp_session2) {
                    scalp_session2.innerText = (sp.scalp_usd >= 0 ? '+' : '') + '$' + sp.scalp_usd.toFixed(2);
                    scalp_session2.className = sp.scalp_usd >= 0 ? 'green' : 'red';
                }

                const carry_pnl_el = document.getElementById('carry_session_pnl');
                carry_pnl_el.innerText = (sp.carry_usd >= 0 ? '+' : '') + '$' + sp.carry_usd.toFixed(2);
                carry_pnl_el.className = sp.carry_usd >= 0 ? 'green' : 'red';

                const global_pnl_el = document.getElementById('global_session_pnl');
                global_pnl_el.innerText = (sp.global_usd >= 0 ? '+' : '') + '$' + sp.global_usd.toFixed(2);
                global_pnl_el.className = sp.global_usd >= 0 ? 'green' : 'red';

                // Pair counts (scalp tab only - carry doesn't use this)
                document.getElementById('scalp_pair_count').innerText = (data.scalp_pairs || []).length;

                // Modes (both tabs) - sm already defined above
                document.getElementById('scalp_mode_label').innerText = `[${sm.scalp}]`;
                document.getElementById('carry_mode_label').innerText = `[${sm.carry}]`;
                const scalp_mode2 = document.getElementById('scalp_mode_label2');
                if (scalp_mode2) scalp_mode2.innerText = `[${sm.scalp}]`;
                const carry_mode2 = document.getElementById('carry_mode_label2');
                if (carry_mode2) carry_mode2.innerText = `[${sm.carry}]`;

                // Allocs
                document.getElementById('scalp_alloc').innerText = `${(data.strategy_allocs.scalp * 100).toFixed(0)}%`;
                document.getElementById('carry_alloc').innerText = `${(data.strategy_allocs.carry * 100).toFixed(0)}%`;
            });
        }

        // Tab switching function
        function showTab(tabName) {
            // Hide all tab contents
            document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
            // Deactivate all tabs
            document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
            // Show selected tab content
            document.getElementById('tab-' + tabName).classList.add('active');
            // Activate selected tab button
            document.querySelector('.tab.' + tabName).classList.add('active');
        }

        // Initial + Interval
        update();
        setInterval(update, 3000);
    </script>
</body>
</html>
"""
        return web.Response(text=html, content_type='text/html')

