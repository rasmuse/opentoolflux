from __future__ import annotations

import functools
import glob
import itertools
from io import StringIO
from pathlib import Path
from typing import Iterable, Iterator, Literal, Mapping, Sequence, Union

import pandas as pd

TIMESTAMP_COLUMN = "__TIMESTAMP__"
EXCLUDE_COLUMN = "__EXCLUDE__"
MILLISECOND_NUMPY_UNIT = "datetime64[ms]"
MILLISECONDS_PER_SECOND = 1000

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

    Add a boolean column `{EXCLUDE_COLUMN}` indicating exclusion status (default False).

    Return data sorted by the index col ascending.
    """
    data = pd.read_csv(path_or_buffer, sep=sep, dtype=dtypes).dropna()[list(dtypes)]
    return _dataframe_to_db(data, timestamp_col)


def read_src_files(
    glob_patterns: list[str],
    dtypes: DTypes,
    timestamp_col: Colname,
    sep: str,
) -> pd.DataFrame:
    paths = _find_files(glob_patterns)
    datasets = (read_src_file(path, dtypes, timestamp_col, sep) for path in paths)
    return functools.reduce(
        update,
        datasets,
        create_empty_db(dtypes, timestamp_col),
    )


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
    data[EXCLUDE_COLUMN] = False
    return data


def _convert_datetime(s: pd.Series) -> pd.Series:
    if s.dtype.kind in {"i", "u", "f"}:
        return s.mul(MILLISECONDS_PER_SECOND).round().astype(MILLISECOND_NUMPY_UNIT)


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
