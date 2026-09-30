import asyncio
import os
import time
from collections import defaultdict, deque
from typing import Dict, List

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

try:
    from BinaryOptionsToolsV2 import PocketOptionAsync
except Exception as exc:
    PocketOptionAsync = None
    IMPORT_ERROR = str(exc)
else:
    IMPORT_ERROR = None

app = FastAPI(title="CTAI Pocket Option OTC Bridge", version="6.2.11")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

SSID = os.getenv("POCKET_OPTION_SSID", "").strip()
BRIDGE_API_KEY = os.getenv("BRIDGE_API_KEY", "").strip()

SYMBOLS = {
    "EUR/USD OTC": "EURUSD_otc",
    "AUD/CAD OTC": "AUDCAD_otc",
    "AUD/CHF OTC": "AUDCHF_otc",
    "USD/SGD OTC": "USDSGD_otc",
    "GBP/USD OTC": "GBPUSD_otc",
    "USD/JPY OTC": "USDJPY_otc",
    "BTC/USD OTC": "BTCUSD_otc",
    "ETH/USD OTC": "ETHUSD_otc",
    "XAU/USD OTC": "XAUUSD_otc",
}

cache: Dict[str, deque] = defaultdict(lambda: deque(maxlen=180))
tasks: Dict[str, asyncio.Task] = {}
client = None
client_lock = asyncio.Lock()

def auth_ok(key: str | None) -> bool:
    return bool(BRIDGE_API_KEY) and key == BRIDGE_API_KEY

def normalize_symbol(symbol: str) -> str:
    s = symbol.strip().upper()
    if s in SYMBOLS:
        return SYMBOLS[s]
    if s.endswith(" OTC"):
        compact = s[:-4].replace("/", "")
        return compact + "_otc"
    return s.replace("/", "") + "_otc"

def clean_candle(c: dict) -> dict | None:
    try:
        ts = int(float(c.get("time", c.get("timestamp", 0))))
        if ts > 10_000_000_000:
            ts //= 1000
        o = float(c["open"])
        h = float(c["high"])
        l = float(c["low"])
        close = float(c["close"])
        if ts <= 0 or min(o, h, l, close) <= 0:
            return None
        return {
            "datetime": ts,
            "open": o,
            "high": h,
            "low": l,
            "close": close,
        }
    except Exception:
        return None

async def ensure_client():
    global client
    if client is not None:
        return client
    if PocketOptionAsync is None:
        raise RuntimeError(f"BinaryOptionsToolsV2 import failed: {IMPORT_ERROR}")
    if not SSID:
        raise RuntimeError("POCKET_OPTION_SSID is not configured")
    async with client_lock:
        if client is None:
            client = PocketOptionAsync(SSID)
            await asyncio.sleep(2)
    return client

async def stream_asset(broker_symbol: str):
    backoff = 3
    while True:
        try:
            api = await ensure_client()
            # get_candles_live performs a historical backfill and then maintains
            # a gap-free live candle feed.
            async for closed, forming in api.get_candles_live(
                broker_symbol, 60, hours=2.0, max_rows=150
            ):
                merged = list(closed or [])
                if forming:
                    merged.append(forming)

                cleaned = []
                for raw in merged:
                    c = clean_candle(raw)
                    if c:
                        cleaned.append(c)

                cleaned.sort(key=lambda x: x["datetime"])
                for c in cleaned:
                    cache[broker_symbol].append(c)

                # Deduplicate and keep chronological order.
                unique = {}
                for c in cache[broker_symbol]:
                    unique[c["datetime"]] = c
                cache[broker_symbol].clear()
                for ts in sorted(unique):
                    cache[broker_symbol].append(unique[ts])

            backoff = 3
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[OTC] {broker_symbol} stream error: {exc}", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def start_symbol(broker_symbol: str):
    task = tasks.get(broker_symbol)
    if task and not task.done():
        return
    tasks[broker_symbol] = asyncio.create_task(stream_asset(broker_symbol))

@app.on_event("startup")
async def startup():
    # Start only the two first validation symbols.
    for name in ("EUR/USD OTC", "AUD/CAD OTC"):
        await start_symbol(SYMBOLS[name])

@app.on_event("shutdown")
async def shutdown():
    for task in tasks.values():
        task.cancel()
    if client is not None:
        try:
            await client.shutdown()
        except Exception:
            pass

@app.get("/health")
async def health():
    return {
        "ok": True,
        "service": "ctai-pocket-option-otc-bridge",
        "version": "6.2.11",
        "ssid_configured": bool(SSID),
        "bridge_key_configured": bool(BRIDGE_API_KEY),
        "symbols": {k: len(cache[v]) for k, v in SYMBOLS.items()},
        "time": int(time.time()),
    }

@app.get("/api/otc/candles")
async def otc_candles(
    platform: str = Query("Pocket Option"),
    symbol: str = Query(...),
    interval: str = Query("1min"),
    outputsize: int = Query(120, ge=1, le=180),
    x_ctai_bridge_key: str | None = Header(default=None),
):
    if not auth_ok(x_ctai_bridge_key):
        raise HTTPException(status_code=401, detail="Unauthorized")

    if platform.strip().lower() != "pocket option":
        raise HTTPException(status_code=400, detail="Unsupported platform")

    if interval != "1min":
        raise HTTPException(status_code=400, detail="Bridge currently serves 1min candles only")

    broker_symbol = normalize_symbol(symbol)
    await start_symbol(broker_symbol)

    rows = list(cache.get(broker_symbol, []))[-outputsize:]
    if len(rows) < 5:
        return {
            "ok": False,
            "source": "pocket_option_otc",
            "platform": "Pocket Option",
            "symbol": symbol,
            "broker_symbol": broker_symbol,
            "interval": interval,
            "values": [],
            "status": "WAITING",
            "message": "Waiting for verified OTC candles",
        }

    return {
        "ok": True,
        "source": "pocket_option_otc",
        "platform": "Pocket Option",
        "symbol": symbol,
        "broker_symbol": broker_symbol,
        "interval": interval,
        "values": rows,
        "status": "CONNECTED",
        "server_time": int(time.time()),
    }
