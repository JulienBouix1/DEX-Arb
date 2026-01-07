ARB HOTFIX
==========
Created: 20251105-210637

This tar overwrites three modules with the known-good versions:
  - venues/hyperliquid.py  (WS bbo + l2Book, constructor: Hyperliquid(ws_url, rest_url, creds=None))
  - venues/aster.py        (bookTicker warmup + fast poller + stale watchdog)
  - run/arb_runner.py      (imports SUB_RATE_LIMIT_HZ from venues.hyperliquid and passes ws_url/rest_url)
  - run/hl_inject.py       (unchanged, included for completeness)

Quick apply on Windows PowerShell:
  1) Stop the runner.
  2) In repo root (contains folders 'run' and 'venues') run:
     tar -xvf arb_hotfix_20251105-210637.tar
  3) Clean bytecode:
     Get-ChildItem -Recurse -Filter *.pyc | Remove-Item -Force
     Get-ChildItem -Recurse -Directory -Filter __pycache__ | Remove-Item -Force -Recurse
  4) Verify the Hyperliquid API surface:
     python - <<'PY'
import importlib, inspect
m = importlib.import_module('venues.hyperliquid')
print('SUB_RATE_LIMIT_HZ =', getattr(m,'SUB_RATE_LIMIT_HZ', None))
print('Hyperliquid.__init__ =', list(inspect.signature(m.Hyperliquid.__init__).parameters.keys())[:4])
PY

     Expected:
       SUB_RATE_LIMIT_HZ = 8.0
       Hyperliquid.__init__ = ['self','ws_url','rest_url','creds']

  5) Run:
     python -u -m run.arb_runner --auto --top 200 --max-pairs 200 --hb-rows 60 --hb-interval 7

Notes:
  - If equity shows 1000, it means equity seeding did not pull balances yet. With the fixed Hyperliquid WS and REST, it should print '[EQUITY]' lines and update.
  - To make Aster extra responsive, you can set ASTER_STALE_MS=4000 in the environment before launching.