import re

import pandas as pd

from analysis.handlers.base import FeatureHandler

def build_target_from_bias_prediction(bias_prediction: pd.Series) -> pd.Series:
    target_map = {
        "Long": "PDH",
        "Short": "PDL",
        "Neutral": "NO_TARGET"
    }
    return bias_prediction.map(target_map)

def build_target_hit(
        target: pd.Series,
        current_high: pd.Series,
        current_low: pd.Series,
        prev_high: pd.Series,
        prev_low: pd.Series
) -> tuple[pd.Series, pd.Series]:
    target_hit = pd.Series(pd.NA, index=target.index, dtype="boolean")
    opposite_target_hit = pd.Series(pd.NA, index=target.index, dtype="boolean")

    pdh_mask = target == "PDH"
    pdl_mask = target == "PDL"

    target_hit.loc[pdh_mask] = current_high.loc[pdh_mask] >= prev_high.loc[pdh_mask]
    opposite_target_hit.loc[pdh_mask] = current_low.loc[pdh_mask] <= prev_low.loc[pdh_mask]

    target_hit.loc[pdl_mask] = current_low.loc[pdl_mask] <= prev_low.loc[pdl_mask]
    opposite_target_hit.loc[pdl_mask] = current_high.loc[pdl_mask] >= prev_high.loc[pdl_mask]

    return target_hit, opposite_target_hit



class TargetFeatureHandler(FeatureHandler):
    REQUIRED_COLUMNS = {"bias_prediction", "high", "low"}

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["target"] = build_target_from_bias_prediction(result["bias_prediction"])

        result["prev_high"] = result["high"].shift(1)
        result["prev_low"] = result["low"].shift(1)

        result["target_hit"], result["opposite_target_hit"] = build_target_hit(
            result["target"],
            result["high"],
            result["low"],
            result["prev_high"],
            result["prev_low"]
        )

        return result