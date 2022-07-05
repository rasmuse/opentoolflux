from __future__ import annotations

from io import StringIO
from pathlib import Path
from typing import Iterable, Mapping, Union

import pandas as pd

TIMESTAMP_COLUMN = "__TIMESTAMP__"
EXCLUDE_COLUMN = "__EXCLUDE__"
DEFAULT_SEP = r"\s+"
MILLISECOND_NUMPY_UNIT = "datetime64[ms]"
MILLISECONDS_PER_SECOND = 1000


def read_src_file(
    path_or_buffer: Union[Path, StringIO],
    dtypes: Mapping[str, str],
    timestamp_col: str,
    sep=DEFAULT_SEP,
) -> pd.DataFrame:
    f"""
    Read the data file at `path`, keeping only the columns listed as keys in `dtypes`
    and using `timestamp_col` as index.

    Rename the index to `{TIMESTAMP_COLUMN}`.

    Add a boolean column `{EXCLUDE_COLUMN}` indicating exclusion status (default False).

    Return data sorted by the index col ascending.
    """
    data = pd.read_csv(path_or_buffer, sep=sep, dtype=dtypes, usecols=list(dtypes))
    data[TIMESTAMP_COLUMN] = _convert_datetime(data[timestamp_col])
    del data[timestamp_col]
    data.set_index(TIMESTAMP_COLUMN, inplace=True)
    data.dropna(inplace=True)
    data.sort_index(inplace=True)
    if not data.index.is_unique:
        first_duplicate = data.index[data.index.duplicated()][0]
        raise ValueError(f"Duplicate timestamp {first_duplicate}")
    data[EXCLUDE_COLUMN] = False
    return data


def update(db_1: pd.DataFrame, db_2: pd.DataFrame):
    if set(db_1.columns) != set(db_2.columns):
        raise ValueError(
            f"Unequal set of columns: {set(db_1.columns)}, {set(db_2.columns)}"
        )
    if not (db_1.dtypes == db_2.dtypes).all():
        diff = pd.DataFrame({"lhs": db_1.dtypes, "rhs": db_2.dtypes}).loc[
            lambda x: x["lhs"] != x["rhs"]
        ]
        raise ValueError(f"Conflicting dtypes: {diff}")
    concatenated = pd.concat([db_1, db_2]).sort_index()
    return concatenated[~concatenated.index.duplicated(keep="last")]


def read_db(path: Path) -> pd.DataFrame:
    return pd.read_feather(path).set_index(TIMESTAMP_COLUMN)


def save_db(db: pd.DataFrame, path: Path):
    db.reset_index().to_feather(path)


def _convert_datetime(s: pd.Series) -> pd.Series:
    if s.dtype.kind in {"i", "u", "f"}:
        return s.mul(MILLISECONDS_PER_SECOND).round().astype(MILLISECOND_NUMPY_UNIT)
