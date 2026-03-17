from __future__ import annotations

import pandas as pd

def _find_pivots(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["pivot_low"] = (
        (df["low"] < df["low"].shift(1)) &
        (df["low"] < df["low"].shift(-1))
    )
    df["pivot_high"] = (
        (df["high"] > df["high"].shift(1)) &
        (df["high"] > df["high"].shift(-1))
    )
    return df

def _resample_to_1h(intraday_data: pd.DataFrame) -> pd.DataFrame:
    return (
        intraday_data.set_index("ny_time")
        .resample("1h", label="left", closed="left")
        .agg(
            open = ("open", "first"),
            high = ("high", "max"),
            low = ("low", "min"),
            close = ("close", "last")
        )
        .dropna()
        .reset_index()
    )


def find_movement_start(
        intraday_data: pd.DataFrame,
        target: str,
        target_hit_time: pd.Timestamp
) -> pd.Timestamp | None:
    required_columns = {"ny_time", "open", "high", "low", "close"}
    missing_columns = required_columns - set(intraday_data.columns)

    if missing_columns:
        raise ValueError(f"Missing columns in intraday data: {missing_columns}")
    
    if target not in ("PDH", "PDL"):
        return None
    
    is_long = target == "PDH"

    h1 = _resample_to_1h(intraday_data)
    h1 = h1[h1["ny_time"] < target_hit_time]
    if h1.empty:
        return None
    
    h1 = _find_pivots(h1)

    if is_long:
        pivots = h1[h1["pivot_low"]]
    else:
        pivots = h1[h1["pivot_high"]]

    if pivots.empty:
        return intraday_data["ny_time"].min()
    
    pivots = pivots.sort_values("ny_time", ascending=False)

    for _, pivot in pivots.iterrows():
        level = pivot["low"] if is_long else pivot["high"]

        after = h1[h1["ny_time"] > pivot["ny_time"]]
        if after.empty:
            continue

        if is_long:
            broken = (after["low"] <= level).any()
        else:
            broken = (after["high"] >= level).any()

        if not broken:
            return pivot["ny_time"]
    
    return None

def adjust_movement_start_after_2h(
        intraday_data: pd.DataFrame,
        target: str,
        target_hit_time: pd.Timestamp
) -> pd.Timestamp | None:
    if target not in ("PDH", "PDL"):
        return None
    
    is_long = target == "PDH"

    pre_hit = intraday_data[intraday_data["ny_time"] < target_hit_time].copy()
    if pre_hit.empty:
        return None
    
    session_start = pre_hit["ny_time"].dt.normalize() + pd.Timedelta(hours=18)
    session_start = session_start.where(
        pre_hit["ny_time"] >= session_start,
        session_start - pd.Timedelta(days=1)
    )

    pre_hit["session_time"] = pre_hit["ny_time"] - session_start

    post_2h = pre_hit[pre_hit["session_time"] >= pd.Timedelta(hours=8)]
    if post_2h.empty:
        return None
    
    df = _find_pivots(post_2h)

    if is_long:
        pivots = df[df["pivot_low"]]
    else:
        pivots = df[df["pivot_high"]]
    
    if pivots.empty:
        return None
    
    for _, pivot in pivots.iterrows():
        level = pivot["low"] if is_long else pivot["high"]

        after = post_2h[post_2h["ny_time"] > pivot["ny_time"]]
        if after.empty:
            continue

        if is_long:
            broken = (after["low"] <= level).any()
        else:
            broken = (after["high"] >= level).any()

        if not broken:
            return pivot["ny_time"]
    
    return None
    


    

