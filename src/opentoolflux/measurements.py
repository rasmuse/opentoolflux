from __future__ import annotations

import datetime
import functools
import logging
import operator
from dataclasses import dataclass
from typing import Any, Iterator, List, Mapping, Optional, TypedDict

import numpy as np
import pandas as pd

from . import database

logger = logging.getLogger(__name__)


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
    exclusions = _get_filter_exclusions(db, filters)
    logger.info(f"Database has {len(db):,} rows.")
    logger.info(_get_exclusions_summary(exclusions))
    db = _apply_filter_exclusions(db, exclusions)
    logger.info(f"Filtered database has {len(db):,} rows.")
    return db


def _get_filter_exclusions(
    db: pd.DataFrame, filters: Mapping[database.Colname, Filter]
) -> pd.DataFrame:
    db = db.reset_index()
    return pd.DataFrame(
        {
            colname: _is_excluded_by_filter(db[colname], filter_)
            for colname, filter_ in filters.items()
        }
    )


def _apply_filter_exclusions(
    db: pd.DataFrame, exclusions: pd.DataFrame
) -> pd.DataFrame:
    return db[~exclusions.any(axis=1).values]


def _get_exclusions_summary(exclusions: pd.DataFrame) -> str:
    exclusions = exclusions.assign(**{"All filters combined": exclusions.any(axis=1)})
    summary = pd.DataFrame(
        {
            "Number rejected": exclusions.sum().apply("{:,}".format),
            "Share rejected": exclusions.mean().apply("{:.1%}".format),
        }
    )
    return f"Data excluded by filters:\n{summary}\n"


class MeasurementMeta(TypedDict):
    duration: datetime.timedelta
    accept: bool


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

    measurement_metas: list[MeasurementMeta] = []

    for _, measurement in db.groupby(measurement_number):
        assert isinstance(measurement.index, pd.DatetimeIndex)
        duration = measurement.index[-1] - measurement.index[0]  # type: ignore
        assert isinstance(duration, datetime.timedelta), measurement.index
        accept = min_duration <= duration <= max_duration
        measurement_metas.append({"duration": duration, "accept": accept})

        if accept:
            yield _ensure_float64_floats(measurement)

    logger.info(f"\n{_get_measurements_summary(measurement_metas)}\n")


def _ensure_float64_floats(df: pd.DataFrame) -> pd.DataFrame:
    def replace_float_by_float64(dtype):
        if (
            (isinstance(dtype, str) and dtype.startswith("float"))
            or (isinstance(dtype, np.dtype) and dtype.kind == "f")
            or (dtype is float)
        ):
            return np.float64
        return dtype

    new_dtypes = {
        key: replace_float_by_float64(value) for key, value in df.dtypes.items()
    }

    return df.astype(new_dtypes)  # type: ignore


def _get_measurements_summary(metas: list[MeasurementMeta]) -> str:
    if metas:
        data = pd.DataFrame.from_records(metas)
    else:
        data = pd.DataFrame({"duration": [], "accept": []})

    summary = pd.DataFrame(
        {
            key: {
                "Number of segments": f"{len(subset):,}",
                "Average duration": _format_duration(
                    subset["duration"].mean() if len(subset) else None
                ),
                "Total duration": _format_duration(
                    subset["duration"].sum() if len(subset) else datetime.timedelta()
                ),
            }
            for key, subset in [
                ("All segments", data),
                ("Rejected segments", data[~data["accept"]]),
                ("Final measurements", data[data["accept"]]),
            ]
        }
    ).T

    return str(summary)


def _format_duration(duration: Optional[datetime.timedelta]) -> str:
    if duration is None:
        return "-"
    remaining_seconds = duration.total_seconds()
    days = remaining_seconds // (3600 * 24)
    remaining_seconds -= days * (3600 * 24)
    hours = remaining_seconds // 3600
    remaining_seconds -= hours * 3600
    minutes = remaining_seconds // 60
    remaining_seconds -= minutes * 60
    days_str = f"{days:.0f} days " if days else ""
    return f"{days_str}{hours:02.0f}:{minutes:02.0f}:{remaining_seconds:02.0f}"
