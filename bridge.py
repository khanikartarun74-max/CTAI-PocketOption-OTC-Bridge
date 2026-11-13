import asyncio
import math
import os
import time
from collections import defaultdict, deque
from typing import Dict, Optional, List

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

try:
    from BinaryOptionsToolsV2 import PocketOptionAsync
except Exception as exc:
    PocketOptionAsync = None
    IMPORT_ERROR = str(exc)
else:
    IMPORT_ERROR = None


APP_VERSION = "6.4.0"

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
# POCKET OPTION CLIENT INITIALIZATION
# ============================================================

async def ensure_client():

    global client
    global client_ready
    global client_error

    async with client_lock:

        # Reuse an already connected client.
        if client is not None and client_ready:
            return client

        # Validate required configuration.
        if not SSID:
            raise RuntimeError(
                "POCKET_OPTION_SSID is not configured"
            )

        if PocketOptionAsync is None:
            raise RuntimeError(
                "BinaryOptionsToolsV2 import failed: "
                + str(IMPORT_ERROR)
            )

        new_client = None

        try:

            print(
                "[CTAI] Connecting to Pocket Option...",
                flush=True,
            )

            # Create a new Pocket Option client.
            new_client = PocketOptionAsync(
                ssid=SSID
            )

            # Wait for broker assets to become available.
            await new_client.wait_for_assets(60.0)

            # Load the current dynamic OTC asset registry.
            await refresh_assets(new_client)

            # Publish the client only after initialization succeeds.
            client = new_client
            client_ready = True
            client_error = None

            print(
                "[CTAI] Pocket Option client initialized successfully.",
                flush=True,
            )

            return client

        except Exception as exc:

            client = None
            client_ready = False
            client_error = repr(exc)

            print(
                "[CTAI] Pocket Option initialization failed: "
                + repr(exc),
                flush=True,
            )

            if new_client is not None:

                try:
                    await new_client.shutdown()

                except Exception as shutdown_error:

                    print(
                        "[CTAI] Failed client cleanup warning: "
                        + repr(shutdown_error),
                        flush=True,
                    )

            raise

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

# Dynamic Pocket Option asset registry
asset_lock = asyncio.Lock()
asset_registry: Dict[str, dict] = {}
otc_registry: Dict[str, dict] = {}


# ============================================================
# AUTH
# ============================================================

def auth_ok(key: Optional[str]) -> bool:
    return bool(BRIDGE_API_KEY) and key == BRIDGE_API_KEY


# ============================================================
# ASSET HELPERS
# ============================================================

def asset_symbol(asset: dict) -> str:
    return str(asset.get("symbol", "")).strip()


def asset_name(asset: dict) -> str:
    return str(
        asset.get("name")
        or asset.get("symbol")
        or ""
    ).strip()


def is_otc_asset(asset: dict) -> bool:
    value = asset.get("is_otc")

    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        return value.strip().lower() in (
            "true",
            "1",
            "yes",
            "y",
        )

    if isinstance(value, int):
        return value == 1

    return False


def is_active_asset(asset: dict) -> bool:
    value = asset.get("is_active")

    if value is None:
        return True

    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        return value.strip().lower() in (
            "true",
            "1",
            "yes",
            "y",
        )

    if isinstance(value, int):
        return value == 1

    return bool(value)


def asset_to_public(asset: dict) -> dict:
    return {
        "symbol": asset_symbol(asset),
        "name": asset_name(asset),
        "asset_type": asset.get("asset_type"),
        "payout": asset.get("payout"),
        "is_otc": bool(is_otc_asset(asset)),
        "is_active": bool(is_active_asset(asset)),
        "allowed_candles": asset.get(
            "allowed_candles",
            [],
        ),
    }


# ============================================================
# REFRESH DYNAMIC ASSET LIST
# ============================================================

async def refresh_assets(
    api=None,
) -> List[dict]:

    global asset_load_error

    if api is None:
        api = await ensure_client()

    async with asset_lock:

        try:

            print(
                "[CTAI] Reading active Pocket Option assets...",
                flush=True,
            )

            assets = await api.active_assets()

            if not isinstance(assets, list):
                raise RuntimeError(
                    "Pocket Option active_assets() "
                    "returned an unexpected format"
                )

            new_registry: Dict[str, dict] = {}
            new_otc_registry: Dict[str, dict] = {}

            for raw in assets:

                if not isinstance(raw, dict):
                    continue

                symbol = asset_symbol(raw)

                if not symbol:
                    continue

                normalized = dict(raw)

                new_registry[symbol] = normalized

                if (
                    is_otc_asset(normalized)
                    and is_active_asset(normalized)
                ):
                    new_otc_registry[symbol] = normalized

            asset_registry.clear()
            asset_registry.update(new_registry)

            otc_registry.clear()
            otc_registry.update(new_otc_registry)

            asset_load_error = None

            print(
                "[CTAI] Active assets loaded: "
                f"{len(asset_registry)} total, "
                f"{len(otc_registry)} OTC",
                flush=True,
            )

            return list(otc_registry.values())

        except Exception as exc:

            asset_load_error = repr(exc)

            print(
                "[CTAI] Dynamic asset discovery failed: "
                f"{asset_load_error}",
                flush=True,
            )

            raise


# ============================================================
# SYMBOL RESOLUTION
# ============================================================

def normalize_symbol_from_registry(
    symbol: str,
) -> Optional[str]:

    raw = symbol.strip()

    if not raw:
        return None

    # Exact broker symbol
    if raw in otc_registry:
        return raw

    upper = raw.upper()

    # Case-insensitive broker symbol
    for broker_symbol in otc_registry:

        if broker_symbol.upper() == upper:
            return broker_symbol

    # Match human-readable asset name
    for broker_symbol, asset in otc_registry.items():

        name = asset_name(asset)

        if (
            name
            and name.upper() == upper
        ):
            return broker_symbol

    # Common display format:
    # EUR/USD OTC -> EURUSD_otc
    compact = (
        upper
        .replace("/", "")
        .replace(" ", "")
    )

    if compact.endswith("_OTC"):
        candidate = compact[:-4] + "_otc"
    elif compact.endswith("OTC"):
        candidate = compact[:-3] + "_otc"
    else:
        candidate = compact + "_otc"

    for broker_symbol in otc_registry:

        if broker_symbol.upper() == candidate.upper():
            return broker_symbol

    return None


# ============================================================
# DISPLAY NAME
# ============================================================

def display_symbol(
    broker_symbol: str,
) -> str:

    asset = otc_registry.get(
        broker_symbol
    )

    if asset:
        name = asset_name(asset)

        if name:
            return name

    # Fallback formatting
    if broker_symbol.lower().endswith("_otc"):

        base = broker_symbol[:-4]

        if len(base) == 6:
            return (
                f"{base[:3]}/{base[3:]} OTC"
            )

        return f"{base} OTC"

    return broker_symbol



# ============================================================
# CANDLE CLEANING
# ============================================================

def clean_candle(c: dict) -> Optional[dict]:
    try:
        if not isinstance(c, dict):
            return None

        
        ts = int(float(c.get("time", c.get("timestamp", c.get("datetime", 0)))))
        

        if ts > 10_000_000_000:
            ts //= 1000

        o = float(c["open"])
        h = float(c["high"])
        l = float(c["low"])
        close = float(c["close"])

        prices = (o, h, l, close)

        # Reject non-finite or non-positive prices.
        if not all(math.isfinite(v) and v > 0 for v in prices):
            return None

        # Reject invalid timestamps.
        if ts <= 0:
            return None

        # Validate OHLC candle structure.
        if h < max(o, close, l):
            return None

        if l > min(o, close, h):
            return None

        return {
            "datetime": ts,
            "open": o,
            "high": h,
            "low": l,
            "close": close,
        }

    except (TypeError, ValueError, OverflowError, KeyError):
        return None


# ============================================================
# CACHE MERGE
# ============================================================

def merge_candles(
    broker_symbol: str,
    candles: list,
) -> None:

    # Keep existing candles and merge incoming candles
    # by timestamp before applying the cache limit.
    existing = {}

    for candle in cache[broker_symbol]:
        cleaned = clean_candle(candle)

        if cleaned is not None:
            existing[cleaned["datetime"]] = cleaned

    for raw in candles or []:
        cleaned = clean_candle(raw)

        if cleaned is not None:
            existing[cleaned["datetime"]] = cleaned

    # Sort unique timestamps and retain the newest 150.
    timestamps = sorted(existing)

    latest_timestamps = timestamps[-150:]

    cache[broker_symbol].clear()

    for timestamp in latest_timestamps:
        cache[broker_symbol].append(
            existing[timestamp]
        )

# ============================================================
# CLIENT INVALIDATION / SAFE RECONNECTION
# ============================================================

async def invalidate_client(failed_client, reason: str):

    global client
    global client_ready
    global client_error

    if failed_client is None:
        return

    async with client_lock:

        # Another asset may already have replaced this client.
        if client is not failed_client:
            return

        old_client = client

        client = None
        client_ready = False
        client_error = reason

        print(
            "[CTAI] Invalidating Pocket Option client: "
            + reason,
            flush=True,
        )

        try:

            await old_client.shutdown()

            print(
                "[CTAI] Old Pocket Option client shut down.",
                flush=True,
            )

        except Exception as shutdown_error:

            print(
                "[CTAI] Old client shutdown warning: "
                + repr(shutdown_error),
                flush=True,
            )
            
# ============================================================
# LIVE OTC STREAM WITH AUTOMATIC CLIENT RECOVERY
# ============================================================

async def stream_asset(
    broker_symbol: str,
):

    global client_ready
    global client_error

    backoff = 3

    while True:

        api = None

        try:

            # Get a valid Pocket Option client.
            api = await ensure_client()

            # Verify symbol is currently active.
            if broker_symbol not in otc_registry:

                await refresh_assets(api)

            if broker_symbol not in otc_registry:

                raise RuntimeError(
                    "OTC asset is not currently active: "
                    + broker_symbol
                )

            print(
                f"[OTC-DIAG] {broker_symbol}: "
                f"cache_before_live_backfill="
                f"{len(cache.get(broker_symbol, []))}",
                flush=True,
            )

            # Consume live candle updates continuously.
            async for closed, forming in api.get_candles_live(
                broker_symbol,
                period=60,
                hours=2.0,
                max_rows=150,
            ):

                # Another asset may have replaced this client.
                if client is not api:
                    break

                merged = list(closed or [])

                if forming:
                    merged.append(forming)

                if merged:

                    merge_candles(
                        broker_symbol,
                        merged,
                    )

                    # Only the current client can mark itself ready.
                    if client is api:

                        client_ready = True
                        client_error = None

                    backoff = 3

                    print(
                        f"[OTC] {broker_symbol}: "
                        f"{len(cache.get(broker_symbol, []))} "
                        "candles cached",
                        flush=True,
                    )

            # If this client is still active but its stream ended,
            # treat it as a connection failure.
            if client is api:

                raise RuntimeError(
                    "Live candle stream ended unexpectedly: "
                    + broker_symbol
                )

        except asyncio.CancelledError:

            raise

        except Exception as exc:

            error_text = repr(exc)

            print(
                f"[OTC] {broker_symbol}: "
                f"stream_error={error_text}",
                flush=True,
            )

            
            # Check whether the shared Pocket Option
            # WebSocket connection is actually disconnected.
            connection_alive = True

            if api is not None:

                try:

                    connection_alive = api.is_connected()

                except Exception as status_error:

                    print(
                        "[OTC-DIAG] Connection status check failed: "
                        + repr(status_error),
                        flush=True,
                    )

            # Reset the shared client only when the
            # underlying WebSocket is actually disconnected.
            if (
                api is not None
                and broker_symbol in otc_registry
                and not connection_alive
            ):

                await invalidate_client(
                    api,
                    error_text,
                )

            else:

                print(
                    f"[OTC] {broker_symbol}: "
                    "stream retry without resetting shared client",
                    flush=True,
                )

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

    task = tasks.get(
        broker_symbol
    )

    if (
        task
        and not task.done()
    ):
        return

    tasks[
        broker_symbol
    ] = asyncio.create_task(
        stream_asset(
            broker_symbol
        )
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
        "[CTAI] SSID configured: "
        f"{bool(SSID)}",
        flush=True,
    )

    print(
        "[CTAI] Bridge key configured: "
        f"{bool(BRIDGE_API_KEY)}",
        flush=True,
    )

    try:

        api = await ensure_client()

        print(
            "[CTAI] Pocket Option connection ready.",
            flush=True,
        )

        # Temporary single-stream diagnostic test.
        for preferred in (
            "EURUSD_otc",
        ):

            if preferred in otc_registry:

                await start_symbol(
                    preferred
                )
            
        print(
            "[CTAI] Dynamic OTC discovery ready.",
            flush=True,
        )

    except Exception as exc:

        print(
            "[CTAI] Startup initialization failed: "
            f"{repr(exc)}",
            flush=True,
        )

        return

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
            "[CTAI] Shutdown warning: "
            f"{exc}",
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
        "bridge_key_configured": bool(
            BRIDGE_API_KEY
        ),

        "client_created": (
            client is not None
        ),

        "client_ready": client_ready,

        "asset_load_error": asset_load_error,
        "client_error": client_error,

        "asset_count": len(
            asset_registry
        ),

        "otc_asset_count": len(
            otc_registry
        ),

        "otc_assets": [
            asset_to_public(asset)
            for asset in sorted(
                otc_registry.values(),
                key=lambda x: asset_symbol(x),
            )
        ],

        "active_streams": [
            symbol
            for symbol, task in tasks.items()
            if task
            and not task.done()
        ],

        "cached_candles": {
            symbol: len(cache[symbol])
            for symbol in tasks.keys()
        },

        "time": int(
            time.time()
        ),
    }

# ============================================================
# OTC ASSETS API
# ============================================================

@app.get("/api/otc/assets")
async def otc_assets(
    x_ctai_bridge_key: Optional[str] = Header(
        default=None
    ),
):

    if not auth_ok(
        x_ctai_bridge_key
    ):

        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
        )

    api = await ensure_client()

    # Refresh before returning list so
    # CTAI receives current Pocket Option assets.
    assets = await refresh_assets(
        api
    )

    result = []

    for asset in sorted(
        assets,
        key=lambda x: (
            asset_name(x).upper(),
            asset_symbol(x).upper(),
        ),
    ):

        result.append(
            asset_to_public(
                asset
            )
        )

    return {
        "ok": True,
        "source": "pocket_option_otc",
        "platform": "Pocket Option",
        "count": len(result),
        "assets": result,
        "server_time": int(
            time.time()
        ),
    }

# ============================================================
# TEMPORARY BROWSER TEST API
# ============================================================

@app.get("/api/otc/test")
async def otc_test(
    symbol: str = Query("EURUSD_otc")
):
    test_symbol = symbol.strip()

    try:
        api = await ensure_client()

        # Refresh the current Pocket Option asset registry
        await refresh_assets(api)

        # Check whether the requested symbol is available
        if test_symbol not in otc_registry:
            return {
                "ok": False,
                "version": APP_VERSION,
                "source": "pocket_option_otc",
                "symbol": test_symbol,
                "display_name": display_symbol(test_symbol),
                "asset_found": False,
                "stream_active": False,
                "candle_count": len(cache.get(test_symbol, [])),
                "client_ready": client_ready,
                "asset_load_error": asset_load_error,
                "client_error": client_error,
                "status": "NOT_FOUND",
                "message": (
                    f"{display_symbol(test_symbol)} "
                    "is not currently available"
                ),
                "server_time": int(time.time()),
            }

        # Start or reuse the stream for the requested asset
        await start_symbol(test_symbol)

        rows = list(cache.get(test_symbol, []))

        stream_task = tasks.get(test_symbol)
        stream_active = bool(
            stream_task and not stream_task.done()
        )

        return {
            "ok": True,
            "version": APP_VERSION,
            "source": "pocket_option_otc",
            "symbol": test_symbol,
            "display_name": display_symbol(test_symbol),
            "asset_found": True,
            "stream_active": stream_active,
            "candle_count": len(rows),
            "client_ready": client_ready,
            "asset_load_error": asset_load_error,
            "client_error": client_error,
            "status": (
                "CONNECTED"
                if len(rows) >= 60
                else "WAITING"
            ),
            "server_time": int(time.time()),
        }

    except Exception as exc:
        return {
            "ok": False,
            "version": APP_VERSION,
            "source": "pocket_option_otc",
            "symbol": test_symbol,
            "display_name": display_symbol(test_symbol),
            "status": "ERROR",
            "error": str(exc),
            "client_ready": client_ready,
            "asset_load_error": asset_load_error,
            "client_error": client_error,
            "server_time": int(time.time()),
        }


# ============================================================
# OTC CANDLES API
# ============================================================


@app.get("/api/otc/candles")
async def otc_candles(
    platform: str = Query("Pocket Option"),
    symbol: str = Query(...),
    interval: str = Query("1min"),
    outputsize: int = Query(120, ge=1, le=180),
    x_ctai_bridge_key: Optional[str] = Header(
        default=None
    ),
):

    # --------------------------------------------------------
    # 1. Authentication
    # --------------------------------------------------------
    if not auth_ok(x_ctai_bridge_key):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
        )

    # --------------------------------------------------------
    # 2. Platform validation
    # --------------------------------------------------------
    if platform.strip().lower() != "pocket option":
        raise HTTPException(
            status_code=400,
            detail="Unsupported platform",
        )

    # --------------------------------------------------------
    # 3. Current bridge serves 1-minute OTC candles
    # --------------------------------------------------------
    if interval != "1min":
        raise HTTPException(
            status_code=400,
            detail=(
                "Bridge currently serves "
                "1min candles only"
            ),
        )

    try:
        # ----------------------------------------------------
        # 4. Ensure Pocket Option client is ready
        # ----------------------------------------------------
        api = await ensure_client()

        # ----------------------------------------------------
        # 5. Resolve symbol from live OTC registry
        # ----------------------------------------------------
        broker_symbol = (
            normalize_symbol_from_registry(
                symbol
            )
        )

        # ----------------------------------------------------
        # 6. Refresh registry once if symbol is not found
        # ----------------------------------------------------
        if not broker_symbol:
            await refresh_assets(api)

            broker_symbol = (
                normalize_symbol_from_registry(
                    symbol
                )
            )

        # ----------------------------------------------------
        # 7. Asset not found
        # ----------------------------------------------------
        if not broker_symbol:
            return {
                "ok": False,
                "version": APP_VERSION,
                "source": "pocket_option_otc",
                "platform": "Pocket Option",
                "symbol": symbol,
                "interval": interval,
                "status": "ASSET_NOT_FOUND",
                "client_ready": client_ready,
                "asset_load_error": asset_load_error,
                "client_error": client_error,
                "server_time": int(time.time()),
            }

        # ----------------------------------------------------
        # 8. Start / confirm live candle stream
        # ----------------------------------------------------
        await start_symbol(broker_symbol)

        # ----------------------------------------------------
        # 9. Read current candle cache
        # ----------------------------------------------------
        rows = list(
            cache.get(
                broker_symbol,
                [],
            )
        )

        # ----------------------------------------------------
        # 10. Give the live stream a short time to populate
        #     if cache is still empty
        # ----------------------------------------------------
        if len(rows) < 1:

            for _ in range(10):

                await asyncio.sleep(0.5)

                rows = list(
                    cache.get(
                        broker_symbol,
                        [],
                    )
                )

                if len(rows) >= 1:
                    break

        # ----------------------------------------------------
        # 11. Return latest requested candles
        # ----------------------------------------------------
        latest = rows[-outputsize:]

        return {
            "ok": True,
            "version": APP_VERSION,
            "source": "pocket_option_otc",
            "platform": "Pocket Option",
            "symbol": broker_symbol,
            "display_name": display_symbol(
                broker_symbol
            ),
            "interval": interval,
            "asset_found": True,
            "stream_active": bool(
                tasks.get(broker_symbol)
                and not tasks[broker_symbol].done()
            ),
            "candle_count": len(rows),
            "returned": len(latest),
            "values": latest,
            "client_ready": client_ready,
            "asset_load_error": asset_load_error,
            "client_error": client_error,
            "status": (
                "CONNECTED"
                if len(latest) >= 60
                else "WAITING"
            ),
            "server_time": int(time.time()),
        }

    # --------------------------------------------------------
    # 12. Unexpected error
    # --------------------------------------------------------
    except Exception as exc:

        return {
            "ok": False,
            "version": APP_VERSION,
            "source": "pocket_option_otc",
            "platform": "Pocket Option",
            "symbol": symbol,
            "interval": interval,
            "status": "ERROR",
            "error": str(exc),
            "client_ready": client_ready,
            "asset_load_error": asset_load_error,
            "client_error": client_error,
            "server_time": int(time.time()),
        }
    
# ============================================================
# TEMPORARY CANDLE DATA TEST API
# ============================================================

@app.get("/api/otc/candles-test")
async def otc_candles_test(
    symbol: str = Query("EURUSD_otc"),
    outputsize: int = Query(10, ge=1, le=20),
):
    try:
        # ----------------------------------------------------
        # Ensure Pocket Option client is ready
        # ----------------------------------------------------
        api = await ensure_client()

        # ----------------------------------------------------
        # Resolve requested symbol from live OTC registry
        # ----------------------------------------------------
        test_symbol = normalize_symbol_from_registry(symbol)

        # If symbol is not found, refresh assets once
        if not test_symbol:
            await refresh_assets(api)
            test_symbol = normalize_symbol_from_registry(symbol)

        # ----------------------------------------------------
        # Asset not found
        # ----------------------------------------------------
        if not test_symbol:
            return {
                "ok": False,
                "version": APP_VERSION,
                "source": "pocket_option_otc",
                "symbol": symbol,
                "status": "ASSET_NOT_FOUND",
                "client_ready": client_ready,
                "asset_load_error": asset_load_error,
                "client_error": client_error,
                "server_time": int(time.time()),
            }

        # ----------------------------------------------------
        # Start / confirm live candle stream
        # ----------------------------------------------------
        await start_symbol(test_symbol)

        # ----------------------------------------------------
        # Read cached candles
        # ----------------------------------------------------
        rows = list(
            cache.get(
                test_symbol,
                [],
            )
        )

        # Return only requested number of latest candles
        latest = rows[-outputsize:]

        # ----------------------------------------------------
        # Success response
        # ----------------------------------------------------
        return {
            "ok": True,
            "version": APP_VERSION,
            "source": "pocket_option_otc",
            "symbol": test_symbol,
            "display_name": display_symbol(test_symbol),
            "asset_found": True,
            "stream_active": bool(
                tasks.get(test_symbol)
                and not tasks[test_symbol].done()
            ),
            "candle_count": len(rows),
            "returned": len(latest),
            "values": latest,
            "client_ready": client_ready,
            "asset_load_error": asset_load_error,
            "client_error": client_error,
            "status": (
                "CONNECTED"
                if len(latest) >= 60
                else "WAITING"
            ),
            "server_time": int(time.time()),
        }

    # --------------------------------------------------------
    # Unexpected error
    # --------------------------------------------------------
    except Exception as exc:
        return {
            "ok": False,
            "version": APP_VERSION,
            "source": "pocket_option_otc",
            "symbol": symbol,
            "status": "ERROR",
            "error": str(exc),
            "client_ready": client_ready,
            "asset_load_error": asset_load_error,
            "client_error": client_error,
            "server_time": int(time.time()),
    }

    # If not found, refresh the live asset
    # list once before rejecting the s
