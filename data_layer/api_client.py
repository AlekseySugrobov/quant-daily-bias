import os
from datetime import datetime, timezone

from dotenv import load_dotenv
import requests
import pandas as pd

load_dotenv()

BASE_URL = os.getenv("BASE_URL")

DAY_ENDPOINT = "/market-data/day"
RANGE_ENDPOINT = "/market-data/range"
HEALTH_ENDPOINT = "/health"

def check_api() -> dict:
    r = requests.get(f"{BASE_URL}{HEALTH_ENDPOINT}", timeout=5)
    r.raise_for_status()
    return r.json()

def _get(endpoint: str, params: dict) -> dict:
    url = f"{BASE_URL}{endpoint}"
    r = requests.get(url=url, params=params, timeout=120)
    r.raise_for_status()
    return r.json()

def _normalize_candles(candles: list) -> list[dict]:
    rows = []
    for c in candles:
        rows.append({
            "timestamp": c["t_open"],
            "open": c["o"],
            "high": c["h"],
            "low": c["l"],
            "close": c["c"],
            "volume": c.get("v")
        })    
    return rows

def _to_dataframe(rows: list) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.set_index("timestamp").sort_index()
    return df

def fetch_range(symbol: str, timeframe: str, start: str, end: str | None = None) -> pd.DataFrame:
    if end is None:
        end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    params = {
        "symbol": symbol,
        "tf": timeframe, 
        "from": start, 
        "to": end
    }
    data = _get(RANGE_ENDPOINT, params)
    rows = _normalize_candles(data["candles"])
    return _to_dataframe(rows)

def fetch_day(symbol: str, timeframe: str, date: str | None=None) -> tuple[pd.DataFrame, dict]:
    params = {
        "symbol": symbol,
        "tf": timeframe,
    }
    data = _get(DAY_ENDPOINT, params)
    rows = _normalize_candles(data["candles"])
    df = _to_dataframe(rows)
    meta = {
        "session_id": data.get("session_id"),
        "requested_session_id": data.get("requested_session_id"),
        "timezone": data.get("timezone")
    }
    return df, meta
