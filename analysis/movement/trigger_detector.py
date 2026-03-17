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
    df = intraday_data.copy()

    shift = pd.Timedelta(hours=18)
    df["shifted_time"] = df["ny_time"] - shift


    ohlc = (
        df.set_index("shifted_time")
        .resample(tf, label="left", closed="left")
        .agg(
            open = ("open", "first"),
            high = ("high", "max"),
            low = ("low", "min"),
            close = ("close", "last")
        )
        .dropna()
    )
    ohlc.index = ohlc.index + shift
    return ohlc

def _detect_trigger_sequence(
        bars: pd.DataFrame,
        level: float,
        side: str
):
    last_trigger = None
    bars = bars.sort_values("ny_time")

    for i in range(1, len(bars)):
        prev = bars.iloc[i-1]
        curr = bars.iloc[i]

        if side == "Low":
            touched = prev["low"] > level and curr["low"] <= level
            if touched:
                if curr["close"] > level:
                    last_trigger = ("sweep", curr["ny_time"])
                elif curr["close"] < level:
                    last_trigger = ("breakout", curr["ny_time"])
        elif side == "High":
            touched = prev["high"] < level and curr["high"] >= level
            if touched:
                if curr["close"] < level:
                    last_trigger = ("sweep", curr["ny_time"])
                elif curr["close"] > level:
                    last_trigger = ("breakout", curr["ny_time"])
    return last_trigger

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

    results = []

    for tf_label, tf_rule in TIMEFRAMES.items():
        resampled = _resample_to_tf(pre_start, tf_rule)
        resampled = resampled[
            resampled.index + pd.Timedelta(tf_rule) <= movement_start_time
        ]

        if resampled.empty:
            continue

        last_candle = resampled.iloc[-1]
        level = last_candle["low"] if side == "Low" else last_candle["high"]
        level = float(level)

        trigger_window = pre_start

        trigger = _detect_trigger_sequence(trigger_window, level, side)
        if trigger:
            ttype, ttime = trigger
            results.append((tf_label, side, ttype, ttime))
    
    if not results:
        return None, None, None
        
    results.sort(key=lambda x: x[3], reverse=True)
    tf, side, ttype, _ = results[0]
    return tf, side, ttype