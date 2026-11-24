"""
Quick end to end poke at a running stack (docker compose up first).

    pip install httpx websockets
    python scripts/smoke_test.py [base_url] [api_key]
"""

import asyncio
import json
import sys

import httpx
import websockets

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
KEY = sys.argv[2] if len(sys.argv) > 2 else "dev-key-1"
H = {"Authorization": f"Bearer {KEY}"}


async def main():
    async with httpx.AsyncClient(base_url=BASE, headers=H, timeout=10) as c:
        res = await c.post("/calls", json={"from": "+14155550100", "to": "+14155550199", "metadata": {"src": "smoke"}})
        print("POST /calls ->", res.status_code, res.json())
        call = res.json()

        # ws auth via query param here, header works too
        async with websockets.connect(f"{call['websocket_url']}?api_key={KEY}") as ws:
            async for raw in ws:
                ev = json.loads(raw)
                print("  ws:", ev["state"], ev["at"])
                if ev["state"] == "COMPLETED":
                    break

        # upload runs in the worker, give it a sec
        for _ in range(10):
            got = (await c.get(f"/calls/{call['call_id']}")).json()
            if got["recording_url"]:
                break
            await asyncio.sleep(1)
        print("GET /calls/{id} ->", got)

        # fire a burst, should see some 429s
        codes = await asyncio.gather(*[c.post("/calls", json={"from": "1", "to": "2"}) for _ in range(6)])
        print("burst of 6 ->", [r.status_code for r in codes])

        print("metrics ->", (await c.get("/metrics")).json())


asyncio.run(main())
