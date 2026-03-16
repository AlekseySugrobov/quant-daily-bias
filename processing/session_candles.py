import pandas as pd

def build_session_candles(df:pd.DataFrame) -> pd.DataFrame:
    sessions = df.groupby("session").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        bars=("close", "count")
    )

    sessions["range"] = sessions["high"] - sessions["low"]

    sessions["type"] = "Doji"
    sessions.loc[sessions["close"] < sessions["open"], "type"] = "Bear"
    sessions.loc[sessions["close"] > sessions["open"], "type"] = "Bull"

    return sessions.reset_index()