from __future__ import annotations

import functools
import glob
import itertools
from io import StringIO
from pathlib import Path
from typing import Iterable, Iterator, Literal, Mapping, Sequence, Union

import pandas as pd

TIMESTAMP_COLUMN = "__TIMESTAMP__"
MICROSECONDS_PER_SECOND = 1e6
MICROSECOND_NUMPY_TIMESTAMP = "datetime64[us]"

Colname = str
DTypeName = Literal[
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "int8",
    "int16",
    "int32",
    "int64",
    "float16",
    "float32",
    "float64",
    "bool",
]
DTypes = Mapping[Colname, DTypeName]


def create_empty_db(dtypes: DTypes, timestamp_col: Colname) -> pd.DataFrame:
    return _dataframe_to_db(
        pd.DataFrame(
            {colname: pd.Series([], dtype=dtype) for colname, dtype in dtypes.items()}
        ),
        timestamp_col,
    )


def read_src_file(
    path_or_buffer: Union[Path, StringIO],
    dtypes: DTypes,
    timestamp_col: Colname,
    sep: str,
) -> pd.DataFrame:
    f"""
    Read the data file at `path`, keeping only the columns listed as keys in `dtypes`
    and using `timestamp_col` as index.

    Rename the index to `{TIMESTAMP_COLUMN}`.

    Return data sorted by the index col ascending.
    """
    columns = list(dtypes)
    data = pd.read_csv(path_or_buffer, sep=sep, dtype=dtypes, usecols=columns)
    return _dataframe_to_db(data, timestamp_col)


def read_src_files(
    glob_patterns: list[str],
    dtypes: DTypes,
    timestamp_col: Colname,
    sep: str,
) -> pd.DataFrame:
    paths = sorted(_find_files(glob_patterns))
    datasets = (read_src_file(path, dtypes, timestamp_col, sep) for path in paths)
    return update(create_empty_db(dtypes, timestamp_col), *datasets)


def _find_files(glob_patterns: list[str]) -> Iterator[Path]:
    return map(
        Path,
        itertools.chain(
            *(glob.iglob(pattern, recursive=True) for pattern in glob_patterns)
        ),
    )


def _dataframe_to_db(data: pd.DataFrame, timestamp_col: Colname) -> pd.DataFrame:
    data[TIMESTAMP_COLUMN] = _convert_datetime(data[timestamp_col])
    del data[timestamp_col]
    data.set_index(TIMESTAMP_COLUMN, inplace=True)
    data.sort_index(inplace=True)
    if not data.index.is_unique:
        first_duplicate = data.index[data.index.duplicated()][0]
        raise ValueError(f"Duplicate timestamp {first_duplicate}")
    return data


def _convert_datetime(s: pd.Series) -> pd.Series:
    if s.dtype.kind in {"f"}:
        # Floating point troubles!
        #
        # Numeric input data are interpreted as Unix timestamps in seconds.
        #
        # pandas timestamps are int64 Unix timestamps in nanoseconds.
        #
        # float64 can exactly represent integers up to 2**53, which in microseconds
        # is in year 2255, but in nanoseconds is only in April 1970.
        #
        # To ensure that float input data is reproduced to the 6th decimal (microsecond)
        # in the output, we take the following steps:
        # (1) ensure that we are using float64 (exact integers to 2**53)
        # (2) multiply by 10**6 (i.e., to microseconds)
        # (3) round to closest microsecond
        # (4) convert to int64
        return (
            s.astype("float64")
            .mul(MICROSECONDS_PER_SECOND)
            .round()
            .astype(MICROSECOND_NUMPY_TIMESTAMP)
        )
    elif s.dtype.kind in {"i", "u"}:
        # Integers can be converted directly
        return s.astype("datetime64")
    elif s.dtype.kind in {"O"}:
        return pd.to_datetime(s, utc=True).dt.tz_localize(None).astype("datetime64")
    else:
        raise NotImplementedError(f"Cannot make datetime from dtype {s.dtype}.")


def update(original: pd.DataFrame, *databases: pd.DataFrame) -> pd.DataFrame:
    for db in databases:
        if set(original.columns) != set(db.columns):
            raise ValueError(
                f"Unequal set of columns: {set(original.columns)}, {set(db.columns)}"
            )
        if original.dtypes.to_dict() != db.dtypes.to_dict():
            diff = pd.DataFrame({"lhs": original.dtypes, "rhs": db.dtypes}).loc[
                lambda x: x["lhs"] != x["rhs"]
            ]
            raise ValueError(f"Conflicting dtypes: {diff}")
    concatenated = pd.concat([original, *databases]).sort_index()
    return concatenated[~concatenated.index.duplicated(keep="last")]


def read_db(path: Path) -> pd.DataFrame:
    return pd.read_feather(path).set_index(TIMESTAMP_COLUMN)


def save_db(db: pd.DataFrame, path: Path):
    db.reset_index().to_feather(path)
