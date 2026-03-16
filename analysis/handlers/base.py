from __future__ import annotations

from abc import ABC, abstractmethod
import pandas as pd

class FeatureHandler(ABC):

    REQUIRED_COLUMNS: set[str] = set()

    def __init__(self, next_handler: "FeatureHandler | None" = None) -> None:
        self.next_handler = next_handler

    def set_next(self, handler: "FeatureHandler") -> "FeatureHandler":
        self.next_handler = handler
        return handler
    
    def validate_columns(self, df:pd.DataFrame) -> None:
        missing = self.REQUIRED_COLUMNS - set(df.columns)
        if missing:
            class_name = self.__class__.__name__
            raise ValueError(
                f"{class_name}: missing required columns: {sorted(missing)}"
            )

    def handle(self, df: pd.DataFrame) -> pd.DataFrame:
        self.validate_columns(df)
        result = self.process(df)
        if self.next_handler is not None:
            return self.next_handler.handle(result)
        return result
    
    @abstractmethod
    def process(self, df:pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError