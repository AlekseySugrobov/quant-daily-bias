from analysis.handlers.calendar_features import CalendarFeaturesHandler
from analysis.handlers.previous_day_position import PreviousDayPositionHandler
from analysis.handlers.prev_features import PrevFeaturesHandler

def build_feature_chain():
    prev_handler = PrevFeaturesHandler()
    calendar_handler = CalendarFeaturesHandler()
    previous_day_position_handler = PreviousDayPositionHandler()

    prev_handler.set_next(calendar_handler).set_next(previous_day_position_handler)

    return prev_handler