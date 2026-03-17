from __future__ import annotations

from dataclasses import dataclass
import pandas as pd

FIRST_HIT_TARGET = "TARGET"
FIRST_HIT_OPPOSITE = "OPPOSITE"
FIRST_HIT_BOTH_SAME_BAR = "BOTH_SAME_BAR"
FIRST_HIT_NO_HIT = "NO_HIT"
FIRST_HIT_NO_TARGET = "NO_TARGET"

@dataclass(frozen=True)
class TimingResult:
    target_hit_time: pd.Timestamp | None
    opposite_target_hit_time: pd.Timestamp | None
    first_hit_side: str

def _first_hit_time_pdh(bars: pd.DataFrame, level: float) -> pd.Timestamp | pd.NaT:
    hit_mask = bars["high"] >= level
    if not hit_mask.any():
        return pd.NaT
    return bars.loc[hit_mask, "ny_time"].iloc[0]

def _first_hit_time_pdl(bars: pd.DataFrame, level: float) -> pd.Timestamp | pd.NaT:
    hit_mask = bars["low"] <= level
    if not hit_mask.any():
        return pd.NaT
    return bars.loc[hit_mask, "ny_time"].iloc[0]

def _resolve_first_hit_side(
        target_hit_time: pd.Timestamp | pd.NaT,
        opposite_target_hit_time: pd.Timestamp | pd.NaT,
        has_target: bool
) -> str:
    if not has_target:
        return FIRST_HIT_NO_TARGET
    
    target_is_missing = pd.isna(target_hit_time)
    opposite_is_missing = pd.isna(opposite_target_hit_time)

    if target_is_missing and opposite_is_missing:
        return FIRST_HIT_NO_HIT
    
    if not target_is_missing and opposite_is_missing:
        return FIRST_HIT_TARGET
    
    if target_is_missing and not opposite_is_missing:
        return FIRST_HIT_OPPOSITE
    
    if target_hit_time < opposite_target_hit_time:
        return FIRST_HIT_TARGET
    
    if opposite_target_hit_time < target_hit_time:
        return FIRST_HIT_OPPOSITE
    
    return FIRST_HIT_BOTH_SAME_BAR

def _compute_session_timing(
        target: str,
        prev_high: float,
        prev_low: float,
        session_bars: pd.DataFrame,
) -> TimingResult:
    
    if session_bars.empty:
        return TimingResult(pd.NaT, pd.NaT, FIRST_HIT_NO_TARGET if target == "NO_TARGET" else FIRST_HIT_NO_HIT)

    if target == "NO_TARGET":
        return TimingResult(pd.NaT, pd.NaT, FIRST_HIT_NO_TARGET)

    if pd.isna(prev_high) or pd.isna(prev_low):
        return TimingResult(pd.NaT, pd.NaT, FIRST_HIT_NO_HIT)

    if target == "PDH":
        target_hit_time = _first_hit_time_pdh(session_bars, prev_high)
        opposite_target_hit_time = _first_hit_time_pdl(session_bars, prev_low)
    elif target == "PDL":
        target_hit_time = _first_hit_time_pdl(session_bars, prev_low)
        opposite_target_hit_time = _first_hit_time_pdh(session_bars, prev_high)
    else:
        return TimingResult(pd.NaT, pd.NaT, FIRST_HIT_NO_TARGET)
    
    first_hit_side = _resolve_first_hit_side(target_hit_time, opposite_target_hit_time, has_target=True)

    return TimingResult(target_hit_time, opposite_target_hit_time, first_hit_side)

def _timing_result_to_dict(item: TimingResult) -> dict:

    def extract_hours(ts):
        return ts.hour if pd.notna(ts) else pd.NA
    
    def extract_15m(ts):
        return (ts.minute // 15) + 1 if pd.notna(ts) else pd.NA

    return {
        "target_hit_time": item.target_hit_time,
        "target_hit_1h": extract_hours(item.target_hit_time),
        "target_hit_15m": extract_15m(item.target_hit_time),
        "opposite_target_hit_time": item.opposite_target_hit_time,
        "opposite_target_hit_1h": extract_hours(item.opposite_target_hit_time),
        "opposite_target_hit_15m": extract_15m(item.opposite_target_hit_time),
        "first_hit_side": item.first_hit_side
    }

def build_target_timing_features(
        sessions: pd.DataFrame,
        intraday_data: pd.DataFrame
) -> pd.DataFrame:
    required_session_columns = {"session", "target", "high", "low", "prev_high", "prev_low"}
    required_intraday_columns = {"session", "high", "low", "ny_time"}

    missing_session_columns = required_session_columns - set(sessions.columns)
    missing_intraday_columns = required_intraday_columns - set(intraday_data.columns)
    
    if missing_session_columns:
        raise ValueError(f"Missing required session columns: {sorted(missing_session_columns)}")

    if missing_intraday_columns:
        raise ValueError(f"Missing required intraday columns: {sorted(missing_intraday_columns)}")
    
    result = sessions.copy()

    intraday_groups = {
        session_id: group.sort_values("ny_time")
        for session_id, group in intraday_data.groupby("session", sort=True)
    }

    timing_input = result[["session", "target", "prev_high", "prev_low"]]

    timing_results: list[TimingResult] = []

    for row in timing_input.itertuples(index=False):

        session_bars = intraday_groups.get(row.session, pd.DataFrame())
        timing_results.append(_compute_session_timing(row.target, row.prev_high, row.prev_low, session_bars))
        
    timing_df = pd.DataFrame(
        [
            _timing_result_to_dict(item)
            for item in timing_results
        ],
        index=result.index
    )

    result = pd.concat([result, timing_df], axis=1)

    result = result.drop(columns=["prev_high", "prev_low"])

    return result