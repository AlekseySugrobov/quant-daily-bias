import pandas as pd
from analysis.feature_output import format_feature_output
from analysis.feature_pipeline import build_feature_chain

def build_feature_table(sessions: pd.DataFrame) -> pd.DataFrame:
    chain = build_feature_chain()
    result = chain.handle(sessions.copy())
    return format_feature_output(result)