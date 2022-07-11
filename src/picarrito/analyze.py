from __future__ import annotations

import datetime
import functools
import operator
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, List, Mapping, Optional, Tuple

import pandas as pd

from . import database


@dataclass
class Filter:
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    allow_only: Optional[List[Any]] = None
    disallow: Optional[List[Any]] = None


def _is_excluded_by_filter(values: pd.Series, filter_: Filter) -> pd.Series:
    exclusion_criteria = []

    if filter_.disallow is not None:
        exclusion_criteria.append(values.isin(filter_.disallow))
    if filter_.allow_only is not None:
        exclusion_criteria.append(~values.isin(filter_.allow_only))
    if filter_.min_value is not None:
        exclusion_criteria.append(values < filter_.min_value)
    if filter_.max_value is not None:
        exclusion_criteria.append(values > filter_.max_value)

    excluded = functools.reduce(operator.or_, exclusion_criteria, False)
    return excluded


def filter_db(
    db: pd.DataFrame, filters: Mapping[database.Colname, Filter]
) -> pd.DataFrame:
    db.reset_index(inplace=True)
    exclusion_votes = pd.DataFrame(
        {
            colname: _is_excluded_by_filter(db[colname], filter_)
            for colname, filter_ in filters.items()
        }
    )
    db.set_index(database.TIMESTAMP_COLUMN, inplace=True)
    return db[~exclusion_votes.any(axis=1).values]


def iter_measurements(
    db: pd.DataFrame,
    chamber_column: database.Colname,
    max_gap: datetime.timedelta,
    min_duration: datetime.timedelta,
    max_duration: datetime.timedelta,
) -> Iterator[pd.DataFrame]:
    chamber_changed = db[chamber_column] != db[chamber_column].shift(1)
    gap_exceeded = db.index.to_series().diff() > max_gap
    measurement_number = (chamber_changed | gap_exceeded).cumsum()
    for _, measurement in db.groupby(measurement_number):
        duration = measurement.index[-1] - measurement.index[0]
        if min_duration <= duration <= max_duration:
            yield measurement
