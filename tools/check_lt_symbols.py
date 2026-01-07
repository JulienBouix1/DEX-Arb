# -*- coding: utf-8 -*-
"""Check Lighter BTC symbols."""
import asyncio
import aiohttp

async def main():
    async with aiohttp.ClientSession() as s:
        async with s.get('https://mainnet.zklighter.elliot.ai/api/v1/orderBooks') as r:
            d = await r.json()
            print("BTC instruments on Lighter:")
            for inst in d.get('order_books', []):
                sym = inst.get('symbol', '')
                if 'BTC' in sym.upper():
                    inst_id = inst.get('id', inst.get('market_id', '?'))
                    print(f"  {sym} (id={inst_id})")

if __name__ == "__main__":
    asyncio.run(main())
