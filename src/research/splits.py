from dataclasses import dataclass
from typing import Iterator, Tuple

import pandas as pd


@dataclass
class Split:
    train: pd.DataFrame
    val: pd.DataFrame
    test: pd.DataFrame


def chrono_split(df: pd.DataFrame, train_end: pd.Timestamp, val_end: pd.Timestamp) -> Split:
    train = df[df["game_date"] <= train_end].copy()
    val = df[(df["game_date"] > train_end) & (df["game_date"] <= val_end)].copy()
    test = df[df["game_date"] > val_end].copy()
    return Split(train=train, val=val, test=test)


def rolling_origin_splits(
    df: pd.DataFrame,
    train_days: int,
    test_days: int,
    step_days: int,
) -> Iterator[Tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
    df_sorted = df.sort_values("game_date")
    start = df_sorted["game_date"].min()
    end = df_sorted["game_date"].max()
    current = start + pd.Timedelta(days=train_days)
    while current + pd.Timedelta(days=test_days) <= end:
        train_end = current
        test_end = current + pd.Timedelta(days=test_days)
        yield start, train_end, test_end
        current = current + pd.Timedelta(days=step_days)
