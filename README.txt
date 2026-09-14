CTAI V6.2.11 — Pocket Option OTC Live Bridge
================================================

Purpose
-------
This package adds the server-side OTC data bridge needed by CTAI V6.2.10.
It is DATA-ONLY: it does not place trades.

Architecture
------------
Pocket Option OTC WebSocket
        ↓
BinaryOptionsToolsV2 connector
        ↓
FastAPI bridge (this package)
        ↓
GET /api/otc/candles
        ↓
CTAI Cloudflare Worker
        ↓
CTAI Android/PWA

Important
---------
- The Pocket Option connector is unofficial and may stop working if Pocket
  Option changes its private WebSocket protocol.
- The SSID/session credential is a secret. NEVER paste it into ChatGPT.
- Put the SSID only in the server environment variable POCKET_OPTION_SSID.
- Keep trading disabled. This bridge exposes market candles only.
- No regular-market provider is used as an OTC substitute.
- The bridge returns WAITING until verified OTC candles arrive.

Supported first assets
----------------------
EUR/USD OTC -> EURUSD_otc
AUD/CAD OTC -> AUDCAD_otc
AUD/CHF OTC -> AUDCHF_otc
USD/SGD OTC -> USDSGD_otc
GBP/USD OTC -> GBPUSD_otc
USD/JPY OTC -> USDJPY_otc
BTC/USD OTC -> BTCUSD_otc
ETH/USD OTC -> ETHUSD_otc
XAU/USD OTC -> XAUUSD_otc

The catalog can be expanded after the first live candle test.

Environment
-----------
POCKET_OPTION_SSID = complete Pocket Option authentication payload
BRIDGE_API_KEY     = long random secret used by the CTAI Worker
HOST               = 0.0.0.0
PORT               = 8000

Run
---
pip install -r requirements.txt
export POCKET_OPTION_SSID='...'
export BRIDGE_API_KEY='...'
uvicorn bridge:app --host 0.0.0.0 --port 8000

Health:
GET /health

Data:
GET /api/otc/candles?platform=Pocket%20Option&symbol=AUD/CAD%20OTC&interval=1min&outputsize=120

Header:
X-CTAI-Bridge-Key: <BRIDGE_API_KEY>

Expected success response:
{
  "ok": true,
  "source": "pocket_option_otc",
  "platform": "Pocket Option",
  "symbol": "AUD/CAD OTC",
  "broker_symbol": "AUDCAD_otc",
  "interval": "1min",
  "values": [
    {"datetime": 177..., "open": ..., "high": ..., "low": ..., "close": ...}
  ]
}

Cloudflare integration
----------------------
The CTAI Worker should call this bridge from /api/otc/candles and attach the
BRIDGE_API_KEY server-side. Do NOT put the bridge key in index.html/app.js.

This package does not contain your SSID or bridge key.

Source notes
------------
BinaryOptionsToolsV2 documents Pocket Option OTC symbols such as EURUSD_otc
and real-time subscriptions. It is an unofficial integration, so production
use should be treated as connector-dependent.

CTAI signal engine remains unchanged. Once valid candles arrive, V6.2.10
can analyze them without using regular-market fallback.
