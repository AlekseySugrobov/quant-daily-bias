import pandas as pd

NY_TIMEZONE = "America/New_York"

def add_sessions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    ny = df.index.tz_convert(NY_TIMEZONE)

    df["ny_time"] = ny
    df["session"] = (ny + pd.Timedelta(hours=6)).date


    return df