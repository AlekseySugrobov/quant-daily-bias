import pandas as pd

from analysis.handlers.base import FeatureHandler

class PrevFeaturesHandler(FeatureHandler):
    REQUIRED_COLUMNS = {"range", "type"}

    def process(self, df:pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["prev_range"] = result["range"].shift(1)
        result["prev_type"] = result["type"].shift(1)
        return result