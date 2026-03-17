import pandas as pd

NY_TIMEZONE = "America/New_York"
SESSION_OFFSET = pd.Timedelta(hours=6)

def add_sessions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    ny = df.index.tz_convert(NY_TIMEZONE)

    df["ny_time"] = ny
    df["session"] = (ny + SESSION_OFFSET).date


    return df