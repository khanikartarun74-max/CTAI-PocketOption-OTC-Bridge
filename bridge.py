import asyncio
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

def clean_candle(
    c: dict,
) -> Optional[dict]:

    try:

        ts = int(
            float(
                c.get(
                    "time",
                    c.get(
                        "timestamp",
                        0,
                    ),
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

        if min(
            o,
            h,
            l,
            close,
        ) <= 0:
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
            cache[
                broker_symbol
            ].append(candle)

    unique = {}

    for candle in cache[
        broker_symbol
    ]:
        unique[
            candle["datetime"]
        ] = candle

    cache[
        broker_symbol
    ].clear()

    for timestamp in sorted(unique):

        cache[
            broker_symbol
        ].append(
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

    if (
        client is not None
        and client_ready
    ):
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

        if (
            client is not None
            and client_ready
        ):
            return client

        client_ready = False
        asset_load_error = None
        client_error = None

        new_client = None

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

            await new_client.wait_for_assets(
                60.0
            )

            print(
                "[CTAI] Pocket Option assets loaded.",
                flush=True,
            )

            client = new_client
            client_ready = True

            # Immediately discover assets
            await refresh_assets(
                client
            )

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

                if new_client is not None:
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

            # Verify symbol still exists in
            # the currently discovered OTC registry.
            if broker_symbol not in otc_registry:

                await refresh_assets(api)

            if broker_symbol not in otc_registry:

                raise RuntimeError(
                    "OTC asset is not currently active: "
                    + broker_symbol
                )

            print(
                "[OTC] Starting candle stream: "
                f"{broker_symbol}",
                flush=True,
            )

            async for closed, forming in api.get_candles_live(
                broker_symbol,
                period=60,
                hours=2.0,
                max_rows=150,
            ):

                merged = list(
                    closed or []
                )

                if forming:
                    merged.append(
                        forming
                    )

                if merged:

                    merge_candles(
                        broker_symbol,
                        merged,
                    )

                    print(
                        f"[OTC] {broker_symbol}: "
                        f"{len(cache[broker_symbol])} "
                        "candles cached",
                        flush=True,
                    )

            backoff = 3

        except asyncio.CancelledError:

            raise

        except Exception as exc:

            error_text = repr(exc)

            print(
                f"[OTC] {broker_symbol} "
                "stream error: "
                f"{error_text}",
                flush=True,
            )

            global client_ready
            global client_error

            client_ready = (
                client is not None
            )

            client_error = error_text

            await asyncio.sleep(
                backoff
            )

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

        # Keep the two streams that are already
        # proven to work in 6.3.0.
        for preferred in (
            "EURUSD_otc",
            "AUDCAD_otc",
        ):

            if preferred in otc_registry:

                await start_symbol(
                    preferred
                )

                print(
                    "[CTAI] Initial OTC stream started: "
                    f"{preferred}",
                    flush=True,
                )

            else:

                print(
                    "[CTAI] Preferred startup asset "
                    "not currently active: "
                    f"{preferred}",
                    flush=True,
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
async def otc_test():

    test_symbol = "EURUSD_otc"

    try:
        api = await ensure_client()

        # Refresh live Pocket Option assets
        await refresh_assets(api)

        # Confirm the test asset exists
        if test_symbol not in otc_registry:

            return {
                "ok": False,
                "version": APP_VERSION,
                "source": "pocket_option_otc",
                "symbol": test_symbol,
                "asset_found": False,
                "client_ready": client_ready,
                "asset_load_error": asset_load_error,
                "client_error": client_error,
                "message": "EUR/USD OTC is not currently available",
                "server_time": int(time.time()),
            }

        # Start the existing proven stream
        await start_symbol(test_symbol)

        rows = list(
            cache.get(
                test_symbol,
                [],
            )
        )

        return {
            "ok": True,
            "version": APP_VERSION,
            "source": "pocket_option_otc",
            "symbol": test_symbol,
            "display_name": display_symbol(
                test_symbol
            ),
            "asset_found": True,
            "stream_active": bool(
                tasks.get(test_symbol)
                and not tasks[test_symbol].done()
            ),
            "candle_count": len(rows),
            "client_ready": client_ready,
            "asset_load_error": asset_load_error,
            "client_error": client_error,
            "status": (
                "CONNECTED"
                if len(rows) >= 5
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

    if not auth_ok(
        x_ctai_bridge_key
    ):

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

    api = await ensure_client()

    broker_symbol = (
        normalize_symbol_from_registry(
            symbol
        )
    )

    # If not found, refresh the live asset
    # list once before rejecting the s
