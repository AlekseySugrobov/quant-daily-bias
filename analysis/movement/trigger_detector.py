from __future__ import annotations
import pandas as pd

TIMEFRAMES = {
    "1h": "1h",
    "4h": "4h"
}

TF_PRIORITY = {
    "4h": 0,
    "1h": 1
}

def _resample_to_tf(intraday_data: pd.DataFrame, tf: str) -> pd.DataFrame:
    ohlc = (
        intraday_data.set_index("ny_time")
        .resample(tf, label="left", closed="left")
        .agg(
            open = ("open", "first"),
            high = ("high", "max"),
            low = ("low", "min"),
            close = ("close", "last")
        )
        .dropna()
    )
    return ohlc

def _classify_trigger(
        bar: pd.Series,
        level: float,
        side: str
) -> str | None:
    if side == "High":
        if bar["high"] >= level and bar["close"] <= level:
            return "sweep"
        if bar["close"] > level:
            return "breakout"
    elif side == "Low":
        if bar["low"] <= level and bar["close"] >= level:
            return "sweep"
        if bar["close"] < level:
            return "breakout"
    return None

def find_trigger(
        intraday_data: pd.DataFrame,
        target: str,
        movement_start_time: pd.Timestamp
) -> tuple[str | None, str | None, str | None]:
    if target not in ("PDH", "PDL"):
        return None, None, None
    
    side = "Low" if target == "PDH" else "High"

    pre_start = intraday_data[intraday_data["ny_time"] < movement_start_time]
    if pre_start.empty:
        return None, None, None
    
    check_window = intraday_data[
        (intraday_data["ny_time"] >= movement_start_time - pd.Timedelta(minutes=5)) &
        (intraday_data["ny_time"] <= movement_start_time + pd.Timedelta(minutes=5))
    ]

    results = []

    for tf_label, tf_rule in TIMEFRAMES.items():
        resampled = _resample_to_tf(check_window, tf_rule)

        if resampled.empty:
            continue

        last_candle = resampled.iloc[-1]
        level = last_candle["low"] if side == "Low" else last_candle["high"]

        for _, bar in check_window.iterrows():
            trigger_type = _classify_trigger(bar, level, side)
            if trigger_type is not None:
                results.append((tf_label, side, trigger_type, bar["ny_time"]))
                break
    
    if not results:
        return None, None, None
        
    results.sort(key=lambda x: (TF_PRIORITY.get(x[0], 99)))
    tf, side, ttype, _ = results[0]
    return tf, side, ttype