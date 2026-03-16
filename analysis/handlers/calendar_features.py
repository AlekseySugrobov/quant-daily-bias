import pandas as pd
import numpy as np
from analysis.handlers.base import FeatureHandler

def week_of_month(date_series: pd.Series) -> pd.Series:
    dates = pd.to_datetime(date_series)
    first_day = dates.dt.to_period("M").dt.start_time
    return ((dates.dt.day + first_day.dt.weekday) // 7) + 1

def classify_week_part(wom: pd.Series) -> pd.Series:
    max_week = wom.max()

    def classify(x: int) -> str:
        if x == 1: 
            return "first"
        if x == max_week:
            return "last"
        return "middle"
    
    return wom.apply(classify)

class CalendarFeaturesHandler(FeatureHandler):
    REQUIRED_COLUMNS = {"session"}

    def process(self, df):
        result = df.copy()

        dates = pd.to_datetime(result["session"])
        result["weekday"] = dates.dt.strftime("%a")

        result["week_of_month"] = week_of_month(dates)
        
        month_end = dates.dt.to_period("M").dt.end_time.dt.normalize()
        last_week_of_month = week_of_month(month_end)

        is_first = result["week_of_month"] == 1
        is_last = result["week_of_month"] == last_week_of_month

        result["week_part"] = np.select(
            [is_first, is_last],
            ["first", "last"],
            default="middle"
        )

        return result