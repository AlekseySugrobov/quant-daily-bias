from data_layer.api_client import fetch_day

df, meta = fetch_day(
    symbol = "ES",
    timeframe = "1m",
    date = "2026-03-12"
)

print(meta)
print(df.head())