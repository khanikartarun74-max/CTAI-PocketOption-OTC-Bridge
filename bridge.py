import asyncio
import os
import time
from collections import defaultdict, deque
from typing import Dict, Optional

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

try:
    from BinaryOptionsToolsV2 import PocketOptionAsync
except Exception as exc:
    PocketOptionAsync = None
    IMPORT_ERROR = str(exc)
else:
    IMPORT_ERROR = None


APP_VERSION = "6.3.0"

app = FastAPI(
    title="CTAI Pocket Option OTC Bridge",
    version=APP_VERSION,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)


# ============================================================
# ENVIRONMENT
# ============================================================

SSID = os.getenv("POCKET_OPTION_SSID", "").strip()
BRIDGE_API_KEY = os.getenv("BRIDGE_API_KEY", "").strip()


# ============================================================
# POCKET OPTION OTC SYMBOL MAP
# ============================================================

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


# ============================================================
# RUNTIME STATE
# ============================================================

cache: Dict[str, deque] = defaultdict(
    lambda: deque(maxlen=180)
)

tasks: Dict[str, asyncio.Task] = {}

client = None
client_lock = asyncio.Lock()

client_ready = False
asset_load_error: Optional[str] = None
client_error: Optional[str] = None


# ============================================================
# AUTH
# ============================================================

def auth_ok(key: Optional[str]) -> bool:
    return bool(BRIDGE_API_KEY) and key == BRIDGE_API_KEY


# ============================================================
# SYMBOL NORMALIZATION
# ============================================================

def normalize_symbol(symbol: str) -> str:
    raw = symbol.strip()

    if raw in SYMBOLS:
        return SYMBOLS[raw]

    upper = raw.upper()

    for display_name, broker_name in SYMBOLS.items():
        if upper == display_name.upper():
            return broker_name

    if upper.endswith(" OTC"):
        compact = upper[:-4].replace("/", "")
        return compact + "_otc"

    compact = upper.replace("/", "")

    if not compact.endswith("_OTC"):
        return compact + "_otc"

    return compact


# ============================================================
# CANDLE CLEANING
# ============================================================

def clean_candle(c: dict) -> Optional[dict]:
    try:
        ts = int(
            float(
                c.get(
                    "time",
                    c.get("timestamp", 0)
                )
            )
        )

        if ts > 10_000_000_000:
            ts //= 1000

        o = float(c["open"])
        h = float(c["high"])
        l = float(c["low"])
        close = float(c["close"])

        if ts <= 0:
            return None

        if min(o, h, l, close) <= 0:
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


# ============================================================
# CACHE MERGE
# ============================================================

def merge_candles(
    broker_symbol: str,
    candles: list,
) -> None:

    for raw in candles:
        candle = clean_candle(raw)

        if candle is not None:
            cache[broker_symbol].append(candle)

    unique = {}

    for candle in cache[broker_symbol]:
        unique[candle["datetime"]] = candle

    cache[broker_symbol].clear()

    for timestamp in sorted(unique):
        cache[broker_symbol].append(
            unique[timestamp]
        )


# ============================================================
# CLIENT INITIALIZATION
# ============================================================

async def ensure_client():

    global client
    global client_ready
    global asset_load_error
    global client_error

    if client is not None and client_ready:
        return client

    if PocketOptionAsync is None:
        raise RuntimeError(
            "BinaryOptionsToolsV2 import failed: "
            + str(IMPORT_ERROR)
        )

    if not SSID:
        raise RuntimeError(
            "POCKET_OPTION_SSID is not configured"
        )

    async with client_lock:

        if client is not None and client_ready:
            return client

        client_ready = False
        asset_load_error = None
        client_error = None

        try:

            print(
                "[CTAI] Creating PocketOptionAsync client...",
                flush=True,
            )

            new_client = PocketOptionAsync(
                ssid=SSID
            )

            print(
                "[CTAI] PocketOptionAsync client created.",
                flush=True,
            )

            print(
                "[CTAI] Waiting for Pocket Option assets...",
                flush=True,
            )

            # Public API documented by BinaryOptionsToolsV2.
            await new_client.wait_for_assets()

            print(
                "[CTAI] Pocket Option assets loaded.",
                flush=True,
            )

            client = new_client
            client_ready = True

            return client

        except Exception as exc:

            error_text = repr(exc)

            asset_load_error = error_text
            client_error = error_text

            print(
                "[CTAI] Pocket Option initialization failed: "
                + error_text,
                flush=True,
            )

            try:
                if "new_client" in locals():
                    await new_client.shutdown()
            except Exception:
                pass

            client = None
            client_ready = False

            raise


# ============================================================
# LIVE OTC STREAM
# ============================================================

async def stream_asset(
    broker_symbol: str,
):

    backoff = 3

    while True:

        try:

            api = await ensure_client()

            print(
                f"[OTC] Starting candle stream: "
                f"{broker_symbol}",
                flush=True,
            )

            async for closed, forming in api.get_candles_live(
                broker_symbol,
                period=60,
                hours=2.0,
                max_rows=150,
            ):

                merged = list(closed or [])

                if forming:
                    merged.append(forming)

                if merged:

                    merge_candles(
                        broker_symbol,
                        merged,
                    )

                    print(
                        f"[OTC] {broker_symbol}: "
                        f"{len(cache[broker_symbol])} "
                        f"candles cached",
                        flush=True,
                    )

            backoff = 3

        except asyncio.CancelledError:
            raise

        except Exception as exc:

            error_text = repr(exc)

            print(
                f"[OTC] {broker_symbol} stream error: "
                f"{error_text}",
                flush=True,
            )

            global client_ready
            global client_error

            client_ready = False
            client_error = error_text

            await asyncio.sleep(backoff)

            backoff = min(
                backoff * 2,
                60,
            )


# ============================================================
# START SYMBOL
# ============================================================

async def start_symbol(
    broker_symbol: str,
):

    task = tasks.get(broker_symbol)

    if task and not task.done():
        return

    tasks[broker_symbol] = asyncio.create_task(
        stream_asset(broker_symbol)
    )


# ============================================================
# STARTUP
# ============================================================

@app.on_event("startup")
async def startup():

    print(
        "[CTAI] CTAI Pocket Option OTC Bridge starting...",
        flush=True,
    )

    print(
        f"[CTAI] Version: {APP_VERSION}",
        flush=True,
    )

    print(
        f"[CTAI] SSID configured: {bool(SSID)}",
        flush=True,
    )

    print(
        f"[CTAI] Bridge key configured: "
        f"{bool(BRIDGE_API_KEY)}",
        flush=True,
    )

    try:

        await ensure_client()

        print(
            "[CTAI] Pocket Option connection ready.",
            flush=True,
        )

    except Exception as exc:

        print(
            "[CTAI] Startup asset initialization failed: "
            f"{repr(exc)}",
            flush=True,
        )

        return

    await start_symbol(
        SYMBOLS["EUR/USD OTC"]
    )

    await start_symbol(
        SYMBOLS["AUD/CAD OTC"]
    )

    print(
        "[CTAI] Initial OTC streams started.",
        flush=True,
    )


# ============================================================
# SHUTDOWN
# ============================================================

@app.on_event("shutdown")
async def shutdown():

    print(
        "[CTAI] Shutting down...",
        flush=True,
    )

    for task in tasks.values():

        if not task.done():
            task.cancel()

    if tasks:

        await asyncio.gather(
            *tasks.values(),
            return_exceptions=True,
        )

    tasks.clear()

    try:

        if client is not None:
            await client.shutdown()

    except Exception as exc:

        print(
            f"[CTAI] Shutdown warning: {exc}",
            flush=True,
        )


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
async def health():

    return {
        "ok": True,
        "service": "ctai-pocketoption-otc-bridge",
        "version": APP_VERSION,

        "ssid_configured": bool(SSID),
        "bridge_key_configured": bool(BRIDGE_API_KEY),

        "client_created": client is not None,
        "client_ready": client_ready,

        "asset_load_error": asset_load_error,
        "client_error": client_error,

        "symbols": {
            display_name: len(cache[broker_name])
            for display_name, broker_name in SYMBOLS.items()
        },

        "time": int(time.time()),
    }


# ============================================================
# OTC CANDLES API
# ============================================================

@app.get("/api/otc/candles")
async def otc_candles(

    platform: str = Query(
        "Pocket Option"
    ),

    symbol: str = Query(...),

    interval: str = Query(
        "1min"
    ),

    outputsize: int = Query(
        120,
        ge=1,
        le=180,
    ),

    x_ctai_bridge_key: Optional[str] = Header(
        default=None
    ),
):

    if not auth_ok(x_ctai_bridge_key):

        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
        )

    if (
        platform.strip().lower()
        != "pocket option"
    ):

        raise HTTPException(
            status_code=400,
            detail="Unsupported platform",
        )

    if interval != "1min":

        raise HTTPException(
            status_code=400,
            detail=(
                "Bridge currently serves "
                "1min candles only"
            ),
        )

    broker_symbol = normalize_symbol(symbol)

    await start_symbol(
        broker_symbol
    )

    rows = list(
        cache.get(
            broker_symbol,
            [],
        )
    )[-outputsize:]

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
            "client_ready": client_ready,
            "asset_load_error": asset_load_error,
            "client_error": client_error,
            "server_time": int(time.time()),
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
