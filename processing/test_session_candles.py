
from data_layer.api_client import fetch_range
from processing.session_builder import add_sessions
from processing.session_candles import build_session_candles

df = fetch_range(
    "ES",
    "1m",
    "2023-12-30T00:00:00Z"
)

df = add_sessions(df)

sessions = build_session_candles(df)

print(sessions.head(20))