from __future__ import annotations
from dataclasses import dataclass
import pandas as pd

from analysis import movement

@dataclass(frozen=True)
class MovementResult:
    movement_start_time: pd.Timestamp | None
    movement_start_hour: int | None
    movement_start_after_2h: int | None
    trigger_tf: str | None
    trigger_side: str | None
    trigger_type: str | None