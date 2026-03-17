import pandas as pd
import numpy as np
from analysis.handlers.base import FeatureHandler

def week_of_month(dates: pd.Series) -> pd.Series:
    first_day = dates.dt.to_period("M").dt.start_time
    return ((dates.dt.day + first_day.dt.weekday) // 7) + 1

class CalendarFeaturesHandler(FeatureHandler):
    REQUIRED_COLUMNS = {"session"}

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
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