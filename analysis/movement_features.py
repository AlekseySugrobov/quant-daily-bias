from __future__ import annotations
import pandas as pd

from analysis.movement.models import MovementResult
from analysis.movement.start_detector import adjust_movement_start_after_2h, find_movement_start
from analysis.movement.trigger_detector import find_trigger

def _compute_movement(
        target: str, 
        target_hit_time: pd.Timestamp,
        session_bars: pd.DataFrame
) -> MovementResult:
    if target == "NO_TARGET":
        return MovementResult(None, None, None, None, None, None)
    
    movement_start_time = find_movement_start(session_bars, target, target_hit_time)

    if movement_start_time is None:
        return MovementResult(None, None, None, None, None, None)
    
    movement_start_hour = movement_start_time.hour
    movement_start_after_2h = adjust_movement_start_after_2h(session_bars, target, target_hit_time)

    target_tf, trigger_side, trigger_type = find_trigger(session_bars, target, movement_start_time)

    return MovementResult(
        movement_start_time=movement_start_time,
        movement_start_hour=movement_start_hour,
        movement_start_after_2h=movement_start_after_2h,
        trigger_tf=target_tf,
        trigger_side=trigger_side,
        trigger_type=trigger_type
    )

def build_movement_features(
        sessions: pd.DataFrame,
        intraday_data: pd.DataFrame
) -> pd.DataFrame:
    required_session_columns = {"session", "target", "target_hit_time"}
    required_intraday_columns = {"session", "ny_time", "open", "high", "low", "close"}

    missing_s = required_session_columns - set(sessions.columns)
    missing_i = required_intraday_columns - set(intraday_data.columns)

    if missing_s:
        raise ValueError(f"Missing session columns: {missing_s}")
    if missing_i:
        raise ValueError(f"Missing intraday columns: {missing_i}")
    
    intraday_groups = {
        sid: group.sort_values("ny_time")
        for sid, group in intraday_data.groupby("session")
    }

    results: list[MovementResult] = []

    for row in sessions[["session", "target", "target_hit_time"]].itertuples(index=False):
        session_bars = intraday_groups.get(row.session, pd.DataFrame())
        if session_bars.empty:
            results.append(MovementResult(None, None, None, None, None, None))
            continue
        results.append(_compute_movement(row.target, row.target_hit_time, session_bars))

    result_df = pd.DataFrame(
        [
            {
                "movement_start_time": res.movement_start_time,
                "movement_start_hour": res.movement_start_hour,
                "movement_start_after_2h": res.movement_start_after_2h,
                "trigger_tf": res.trigger_tf,
                "trigger_side": res.trigger_side,
                "trigger_type": res.trigger_type
            }
            for res in results
        ],
        index=sessions.index
    )

    return pd.concat([sessions, result_df], axis=1)