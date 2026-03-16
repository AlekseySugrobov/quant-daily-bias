from data_layer.api_client import fetch_range
from processing.session_builder import add_sessions
from processing.session_candles import build_session_candles
from analysis.feature_table import build_feature_table


df = fetch_range(
    "ES",
    "1m",
    "2023-12-28T00:00:00Z",
    "2024-02-01T00:00:00Z"
)

df = add_sessions(df)
sessions = build_session_candles(df)
features = build_feature_table(sessions)

print(features)