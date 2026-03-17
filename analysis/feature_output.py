from dataclasses import dataclass
from collections.abc import Sequence
import pandas as pd

@dataclass(frozen=True)
class FeatureOutputSpec:
    rename_map: dict[str, str]
    columns: Sequence[str]
    dropna_subset: Sequence[str]

DEFAULT_FEATURE_OUTPUT = FeatureOutputSpec(
    rename_map = {
        "session": "date",
        "type": "today_type"
    }, columns = [
        "date",
        "weekday",
        "week_of_month",
        "week_part",
        "prev_type",
        "prev_range",
        "sweep_type",
        "prev_close_vs_prev2_range",
        "prev_close_vs_prev_range",
        "close_position_synced",
        "bias_prediction",
        "target",
        "target_hit",
        "opposite_target_hit", 
        "target_hit_time",
        "target_hit_1h",
        "target_hit_15m",
        "opposite_target_hit_time",
        "opposite_target_hit_1h",
        "opposite_target_hit_15m",
        "first_hit_side"
    ],
    dropna_subset= [
        "prev_type",
        "prev_range",
        "sweep_type",
        "prev_close_vs_prev2_range",
        "prev_close_vs_prev_range"
    ]
)

def format_feature_output(
        df: pd.DataFrame,
        spec: FeatureOutputSpec = DEFAULT_FEATURE_OUTPUT
) -> pd.DataFrame:
    renamed = df.rename(columns=spec.rename_map)
    missing = [col for col in spec.columns if col not in renamed.columns]
    if missing:
        raise ValueError(f"Missing columns in Dataframe: {missing}")

    return renamed[spec.columns].dropna(subset=spec.dropna_subset)