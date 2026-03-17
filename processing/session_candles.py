import pandas as pd
import numpy as np

def build_session_candles(df: pd.DataFrame) -> pd.DataFrame:
    sessions = df.groupby("session").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        bars=("close", "count")
    )

    sessions["range"] = sessions["high"] - sessions["low"]

    conditions = [
        sessions["close"] < sessions["open"],
        sessions["close"] > sessions["open"]
    ]

    sessions["type"] = np.select(conditions, ["Bear", "Bull"], default="Doji")

    return sessions.reset_index()