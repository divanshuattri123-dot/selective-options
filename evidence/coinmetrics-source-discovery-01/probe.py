"""Bounded public no-account Coin Metrics discovery; raw responses preserved."""
import asyncio
import hashlib
import json
import time
from pathlib import Path
import aiohttp

ROOT = Path(__file__).parent
BASE = "https://community-api.coinmetrics.io/v4"
REQUESTS = (
    ("reference", "/reference-data/asset-metrics", {"metrics": "CapMrktCurUSD,CapRealUSD,CapMVRVCur,SOPR", "page_size": "100"}),
    ("catalog", "/catalog-v2/asset-metrics", {"assets": "btc,eth", "metrics": "CapMrktCurUSD,CapRealUSD,CapMVRVCur,SOPR", "page_size": "100"}),
    ("series", "/timeseries/asset-metrics", {"assets": "btc,eth", "metrics": "CapMrktCurUSD,CapRealUSD,CapMVRVCur,SOPR", "frequency": "1d", "page_size": "10", "limit_per_asset": "2", "paging_from": "end"}),
)

async def probe(session, name, route, params):
    meta = {"url": BASE + route, "parameters": params, "started_at_ms": time.time_ns() // 1000000, "authentication": "none", "max_bytes": 2000000}
    raw = bytearray()
    try:
        async with session.get(BASE + route, params=params, allow_redirects=False) as response:
            meta["status"] = response.status
            meta["headers"] = {k: v for k,v in response.headers.items() if k.lower() in ("date", "content-type", "x-ratelimit-plan", "x-ratelimit-limit", "x-ratelimit-remaining", "x-ratelimit-reset")}
            async for chunk in response.content.iter_chunked(65536):
                if len(raw) + len(chunk) > 2000000:
                    meta["truncated"] = True
                    break
                raw.extend(chunk)
    except (aiohttp.ClientError, OSError, TimeoutError) as exc:
        meta["error_type"] = type(exc).__name__
    meta["received_at_ms"] = time.time_ns() // 1000000
    meta["bytes"] = len(raw)
    meta["sha256"] = hashlib.sha256(raw).hexdigest()
    (ROOT / (name + ".body.json")).write_bytes(raw)
    (ROOT / (name + ".receipt.json")).write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(json.dumps({"name": name, **meta}))

async def main():
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
        async with asyncio.timeout(65):
            for request in REQUESTS:
                await probe(session, *request)

asyncio.run(main())
