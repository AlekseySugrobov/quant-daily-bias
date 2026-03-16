from data_layer.api_client import fetch_range
from processing.session_builder import add_sessions

df = fetch_range(
    "ES", 
    "1h", 
    "2026-02-10T00:00:00Z"
)

df = add_sessions(df)

print(df[["ny_time", "session"]].head(50))