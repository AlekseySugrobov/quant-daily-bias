from analysis.handlers.calendar_features import CalendarFeaturesHandler
from analysis.handlers.previous_day_position import PreviousDayPositionHandler
from analysis.handlers.prev_features import PrevFeaturesHandler
from analysis.handlers.target_features import TargetFeatureHandler

def build_feature_chain():
    handlers = [
        PrevFeaturesHandler(),
        CalendarFeaturesHandler(),
        PreviousDayPositionHandler(),
        TargetFeatureHandler()
    ]

    for current, next_ in zip(handlers, handlers[1:]):
        current.set_next(next_)

    return handlers[0]