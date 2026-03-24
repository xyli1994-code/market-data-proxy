import os
import json
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Query, Header
import httpx

app = FastAPI(title="market-data-proxy", version="1.0.0")

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
PROXY_API_KEY = os.getenv("PROXY_API_KEY", "")

def check_auth(x_api_key: str | None):
    if not PROXY_API_KEY:
        raise HTTPException(status_code=500, detail="Server missing PROXY_API_KEY")
    if x_api_key != PROXY_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.get("/")
def root():
    return {"ok": True, "service": "market-data-proxy"}

@app.get("/quote")
async def get_quote(
    symbol: str = Query(...),
    x_api_key: str | None = Header(default=None)
):
    check_auth(x_api_key)

    if not POLYGON_API_KEY:
        raise HTTPException(status_code=500, detail="Server missing POLYGON_API_KEY")

    symbol = symbol.upper()

    last_trade_url = f"https://api.polygon.io/v2/last/trade/{symbol}?apiKey={POLYGON_API_KEY}"
    prev_close_url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev?adjusted=true&apiKey={POLYGON_API_KEY}"

    async with httpx.AsyncClient(timeout=20) as client:
        r1 = await client.get(last_trade_url)
        r2 = await client.get(prev_close_url)

    if r1.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Polygon last trade failed: {r1.text}")
    if r2.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Polygon prev close failed: {r2.text}")

    trade_data = r1.json()
    prev_data = r2.json()

    if "results" not in trade_data or "results" not in prev_data or not prev_data["results"]:
        raise HTTPException(status_code=400, detail="No market data returned")

    price = trade_data["results"]["p"]
    timestamp = trade_data["results"]["t"]
    prev_close = prev_data["results"][0]["c"]

    change = price - prev_close
    change_percent = (change / prev_close * 100) if prev_close else 0

    return {
        "symbol": symbol,
        "price": price,
        "change": round(change, 4),
        "change_percent": round(change_percent, 4),
        "previous_close": prev_close,
        "timestamp": str(timestamp),
        "source": "polygon"
    }

@app.get("/bars")
async def get_bars(
    symbol: str = Query(...),
    interval: str = Query("1d"),
    limit: int = Query(120),
    x_api_key: str | None = Header(default=None)
):
    check_auth(x_api_key)

    if not POLYGON_API_KEY:
        raise HTTPException(status_code=500, detail="Server missing POLYGON_API_KEY")

    symbol = symbol.upper()

    interval_map = {
        "1m": (1, "minute"),
        "5m": (5, "minute"),
        "15m": (15, "minute"),
        "1h": (1, "hour"),
        "1d": (1, "day")
    }

    if interval not in interval_map:
        raise HTTPException(status_code=400, detail="interval must be one of 1m, 5m, 15m, 1h, 1d")

    multiplier, timespan = interval_map[interval]

    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/"
        f"{multiplier}/{timespan}/2025-01-01/2026-12-31"
        f"?adjusted=true&sort=desc&limit={limit}&apiKey={POLYGON_API_KEY}"
    )

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url)

    if r.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Polygon bars failed: {r.text}")

    data = r.json()
    rows = data.get("results", [])

    bars = []
    for row in rows:
        bars.append({
            "time": row["t"],
            "open": row["o"],
            "high": row["h"],
            "low": row["l"],
            "close": row["c"],
            "volume": row["v"]
        })

    bars.reverse()

    return {
        "symbol": symbol,
        "interval": interval,
        "bars": bars,
        "source": "polygon"
    }

@app.get("/positions")
async def get_positions(
    x_api_key: str | None = Header(default=None)
):
    check_auth(x_api_key)

    try:
        with open("positions.json", "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"positions.json read failed: {str(e)}")

    total_market_value = 0

    async with httpx.AsyncClient(timeout=20) as client:
        for p in data["positions"]:
            symbol = p["symbol"].upper()
            prev_close_url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev?adjusted=true&apiKey={POLYGON_API_KEY}"
            last_trade_url = f"https://api.polygon.io/v2/last/trade/{symbol}?apiKey={POLYGON_API_KEY}"

            r1 = await client.get(last_trade_url)
            r2 = await client.get(prev_close_url)

            if r1.status_code != 200 or r2.status_code != 200:
                continue

            trade_data = r1.json()
            if "results" not in trade_data:
                continue

            market_price = trade_data["results"]["p"]
            qty = p["quantity"]
            avg_cost = p["avg_cost"]

            market_value = market_price * qty
            unrealized_pnl = (market_price - avg_cost) * qty
            unrealized_pnl_percent = ((market_price - avg_cost) / avg_cost * 100) if avg_cost else 0

            p["market_price"] = round(market_price, 4)
            p["market_value"] = round(market_value, 2)
            p["unrealized_pnl"] = round(unrealized_pnl, 2)
            p["unrealized_pnl_percent"] = round(unrealized_pnl_percent, 2)

            total_market_value += market_value

    equity = total_market_value + data.get("cash", 0)

    for p in data["positions"]:
        mv = p.get("market_value", 0)
        p["weight"] = round(mv / equity, 4) if equity else 0

    data["equity"] = round(equity, 2)
    data["timestamp"] = datetime.now(timezone.utc).isoformat()

    return data
