from datetime import datetime, timezone
from dotenv import load_dotenv
import os

import requests
import pandas as pd

load_dotenv()

BASE_URL = os.getenv("BASE_URL")

DAY_ENDPOINT = "/market-data/day"
RANGE_ENDPOINT = "/market-data/range"
HEALTH_ENDPOINT = "/health"

def check_api():
    r = requests.get(BASE_URL + HEALTH_ENDPOINT, timeout=5)
    r.raise_for_status()
    return r.json


def _normalize_candles(candles):
    rows = []

    for c in candles:
        rows.append({
            "timestamp": c["t_open"],
            "open": c["o"],
            "high": c["h"],
            "low": c["l"],
            "close": c["c"],
            "volume": c.get("v", None)
        })
    
    return rows

def _to_dataframe(rows):
    df = pd.DataFrame(rows)

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    df = df.set_index("timestamp").sort_index()

    return df

def fetch_range(symbol, timeframe, start, end = None) -> pd.DataFrame:
    url = f"{BASE_URL}/market-data/range"

    if end is None:
        end = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    params = {
        "symbol": symbol,
        "tf": timeframe, 
        "from": start, 
        "to": end
    }

    r = requests.get(url=url, params=params)

    r.raise_for_status()

    data = r.json()

    rows = _normalize_candles(data["candles"])

    return _to_dataframe(rows)

def fetch_day(symbol, timeframe, date=None):
    url = f"{BASE_URL}/market-data/day"

    params = {
        "symbol": symbol,
        "tf": timeframe,
    }

    if date is not None:
        params["date"] = date

    r = requests.get(url=url, params=params)

    r.raise_for_status()

    data = r.json()

    rows = _normalize_candles(data["candles"])
    
    df = _to_dataframe(rows)

    meta = {
        "session_id": data.get("session_id"),
        "requested_session_id": data.get("requested_session_id"),
        "timezone": data.get("timezone")
    }

    return df, meta
