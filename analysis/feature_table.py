import pandas as pd
from analysis.feature_output import format_feature_output
from analysis.feature_pipeline import build_feature_chain
from analysis.target_timing import build_target_timing_features

def build_feature_table(sessions: pd.DataFrame, intraday_data: pd.DataFrame | None = None) -> pd.DataFrame:
    chain = build_feature_chain()
    features_df = chain.handle(sessions.copy())

    if intraday_data is not None:
        features_df = build_target_timing_features(features_df, intraday_data)

    return format_feature_output(features_df)