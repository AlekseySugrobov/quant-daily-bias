import numpy as np
import pandas as pd

from analysis.handlers.base import FeatureHandler

LOW_ZONE_MAX = 0.3
HIGH_ZONE_MIN = 0.7

LOW_ZONE_LABEL = "0-30%"
MID_ZONE_LABEL = "30-70%"
HIGH_ZONE_LABEL = "70-100%"

NEUTRAL_BIAS_LABEL = "Neutral"
LONG_BIAS_LABEL = "Long"
SHORT_BIAS_LABEL = "Short"

def classify_sweep_type(
        prev_high: float,
        prev_low: float,
        prev2_high: float,
        prev2_low: float
) -> str | float:
    if (
        pd.isna(prev_high)
        or pd.isna(prev_low)
        or pd.isna(prev2_high)
        or pd.isna(prev2_low)
    ):
        return pd.NA
    
    took_high = prev_high >= prev2_high
    took_low = prev_low <= prev2_low

    if took_high and took_low:
        return "Both"
    if took_high:
        return "PDH"
    if took_low:
        return "PDL"
    return "None"

def calculate_prev_close_vs_range(
        prev_close: float,
        n_high: float,
        n_low: float
) -> float | None:
    if pd.isna(prev_close) or pd.isna(n_high) or pd.isna(n_low):
        return np.nan

    n_range = n_high - n_low

    if n_range == 0:
        return 0.5

    raw_position = (prev_close - n_low) / n_range
    position = min(max(raw_position, 0.0), 1.0)
    return position

def classify_prev_close_vs_range(
        position: float
) -> str | float:
    if pd.isna(position):
        return pd.NA
    if position <= LOW_ZONE_MAX:
        return LOW_ZONE_LABEL
    if position < HIGH_ZONE_MIN:
        return MID_ZONE_LABEL
    return HIGH_ZONE_LABEL

def classify_bias(prev_close_vs_prev2_range: str,
                  prev_close_vs_prev_range: str) -> str | float:
    if pd.isna(prev_close_vs_prev2_range) or pd.isna(prev_close_vs_prev_range):
        return pd.NA
    if prev_close_vs_prev2_range == HIGH_ZONE_LABEL and prev_close_vs_prev_range == HIGH_ZONE_LABEL:
        return LONG_BIAS_LABEL
    if prev_close_vs_prev2_range == LOW_ZONE_LABEL and prev_close_vs_prev_range == LOW_ZONE_LABEL:
        return SHORT_BIAS_LABEL
    return NEUTRAL_BIAS_LABEL


class PreviousDayPositionHandler(FeatureHandler):
    REQUIRED_COLUMNS = {"high", "low", "close"}

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["prev_high"] = result["high"].shift(1)
        result["prev_low"] = result["low"].shift(1)
        result["prev_close"] = result["close"].shift(1)

        result["prev2_high"] = result["high"].shift(2)
        result["prev2_low"] = result["low"].shift(2)
        result["prev2_range"] = result["prev2_high"] - result["prev2_low"]

        result["sweep_type"] = [
            classify_sweep_type(prev_high, prev_low, prev2_high, prev2_low)
            for prev_high, prev_low, prev2_high, prev2_low in zip(
                result["prev_high"],
                result["prev_low"],
                result["prev2_high"],
                result["prev2_low"]
            )
        ]

        result["prev_close_vs_prev2_range_position"] = [
            calculate_prev_close_vs_range(prev_close, prev2_high, prev2_low)
            for prev_close, prev2_high, prev2_low in zip(
                result["prev_close"],
                result["prev2_high"],
                result["prev2_low"]
            )
        ]

        result["prev_close_vs_prev2_range"] = [
            classify_prev_close_vs_range(position)
            for position in result["prev_close_vs_prev2_range_position"]
        ]

        result["prev_close_vs_prev_range"] = [
            calculate_prev_close_vs_range(prev_close, prev_high, prev_low)
            for prev_close, prev_high, prev_low in zip(
                result["prev_close"],
                result["prev_high"],
                result["prev_low"]
            )
        ]

        result["prev_close_vs_prev_range"] = [
            classify_prev_close_vs_range(position)
            for position in result["prev_close_vs_prev_range"]
        ]

        result["close_position_synced"] = result["prev_close_vs_prev_range"] == result["prev_close_vs_prev2_range"]

        result["bias_prediction"] = [
            classify_bias(prev_close_vs_prev2_range, prev_close_vs_prev_range)
            for prev_close_vs_prev2_range, prev_close_vs_prev_range in zip(
                result["prev_close_vs_prev2_range"],
                result["prev_close_vs_prev_range"]
            )
        ]

        return result