import io
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional, Sequence, Tuple, Type, Union

import pandas as pd
import pandas.testing
import pytest

import opentoolflux.database

from .util import build_db

DATA_DIR = Path(__file__).parent / "data"

DB_DTYPES = {
    "ALARM_STATUS": "int8",
    "N2O": "float64",
}

SRC_DTYPES = {
    "EPOCH_TIME": "float64",
    **DB_DTYPES,
}


@dataclass
class SourceParseCase:
    src_text: str
    dtypes: Mapping[str, str]
    timestamp_col: str
    expected_result: Union[pd.DataFrame, Type[Exception]]
    sep: str = r"\s+"


source_parse_cases = [
    SourceParseCase(  # simple, straightforward case; should sort data by timestamp (A)
        "\n".join(
            [
                "A    B    C",
                "3.1  3.2  3",
                "2.1  2.2  2",
                "1.1  1.2  1",
            ]
        ),
        {
            "A": "float64",
            "B": "float32",
            "C": "uint8",
        },
        "A",
        build_db(
            [1.1, 2.1, 3.1],
            {
                "B": ("float32", [1.2, 2.2, 3.2]),
                "C": ("uint8", [1, 2, 3]),
            },
        ),
    ),
    SourceParseCase(  # string data
        "\n".join(
            [
                "A    B    C",
                "3.1  B3   3",
                "2.1  B2   2",
                "1.1  B1   1",
            ]
        ),
        {
            "A": "float64",
            "B": "str",
            "C": "str",
        },
        "A",
        build_db(
            [1.1, 2.1, 3.1],
            {
                "B": ("str", ["B1", "B2", "B3"]),
                "C": ("str", ["1", "2", "3"]),
            },
        ),
    ),
    SourceParseCase(  # actual Picarro data
        "\n".join(
            [
                "EPOCH_TIME      ALARM_STATUS   solenoid_valves     N2O_dry",
                "1620345675.170  0              5.0000000000E+00    3.3926340875E-01",
                "1620345675.991  0              5.0000000000E+00    3.3928078030E-01",
                "1620345676.605  2              5.0000000000E+00    3.5087647532E-01",
            ]
        ),
        {
            "EPOCH_TIME": "float64",
            "ALARM_STATUS": "uint8",
            "solenoid_valves": "float16",
            "N2O_dry": "float64",
        },
        "EPOCH_TIME",
        build_db(
            [1620345675.170, 1620345675.991, 1620345676.605],
            {
                "ALARM_STATUS": ("uint8", [0, 0, 2]),
                "solenoid_valves": ("float16", [5.0, 5.0, 5.0]),
                "N2O_dry": ("float64", [0.33926340875, 0.33928078030, 0.35087647532]),
            },
        ),
    ),
    SourceParseCase(  # can also parse with other separator (e.g., comma)
        "\n".join(
            [
                "A,B,C",
                "3.1,3.2,3",
                "2.1,2.2,2",
                "1.1,1.2,1",
            ]
        ),
        {
            "A": "float64",
            "B": "float32",
            "C": "uint8",
        },
        "A",
        build_db(
            [1.1, 2.1, 3.1],
            {
                "B": ("float32", [1.2, 2.2, 3.2]),
                "C": ("uint8", [1, 2, 3]),
            },
        ),
        sep=",",
    ),
    SourceParseCase(  # rows with null values are NOT dropped
        "\n".join(
            [
                "A    B",
                "1.1  1.2",
                "2.1",
                "3.1  3.2",
            ]
        ),
        {
            "A": "float64",
            "B": "float32",
        },
        "A",
        build_db(
            [1.1, 2.1, 3.1],
            {
                "B": ("float32", [1.2, float("nan"), 3.2]),
            },
        ),
    ),
    SourceParseCase(  # can ignore column(s) in the source
        "\n".join(
            [
                "A    B    C",
                "3.1  3.2  3",
                "2.1  2.2  2",
                "1.1  1.2  1",
            ]
        ),
        {
            "A": "float64",
            "C": "uint8",
        },
        "A",
        build_db(
            [1.1, 2.1, 3.1],
            {
                "C": ("uint8", [1, 2, 3]),
            },
        ),
    ),
    SourceParseCase(  # parsing int data as float64
        "\n".join(
            [
                "A    B",
                "1.1  1",
                "2.1  2",
                "3.1  3",
            ]
        ),
        {
            "A": "float64",
            "B": "float64",
        },
        "A",
        build_db(
            [1.1, 2.1, 3.1],
            {
                "B": ("float64", [1.0, 2.0, 3.0]),
            },
        ),
    ),
    SourceParseCase(  # not allowing duplicate timestamps
        "\n".join(
            [
                "A",
                "1.1",
                "2.1",
                "2.1",
            ]
        ),
        {
            "A": "float64",
        },
        "A",
        ValueError,
    ),
    SourceParseCase(  # not allowing float data to be parsed as int
        "\n".join(
            [
                "A    B",
                "1.1  1.2",
                "2.1  2.2",
                "3.1  3.2",
            ]
        ),
        {
            "A": "float64",
            "B": "int8",
        },
        "A",
        ValueError,
    ),
    SourceParseCase(  # overflow numbers wrap around; not so nice but that's how it is
        "\n".join(
            [
                "A    B",
                "1.1  -3",
                "2.1  255",
                "3.1  256",
                "4.1  257",
            ]
        ),
        {
            "A": "float64",
            "B": "uint8",
        },
        "A",
        build_db(
            [1.1, 2.1, 3.1, 4.1],
            {
                "B": ("uint8", [253, 255, 0, 1]),
            },
        ),
    ),
    SourceParseCase(  # timestamps can be specified as strings
        "\n".join(
            [
                "A,B",
                "0,1970-01-01 00:00:00.123+0000",
                "1,1970-01-01 01:00:01.123+0100",
                "2,1970-01-01T00:00:02.123",
                "3,1970-01-01T00:00:03.123Z",
                "4,1969-12-31 16:00:04.123-0800",
                "5,1970-01-01 00:00:05.123",
            ]
        ),
        {
            "A": "uint8",
            "B": "str",
        },
        "B",
        build_db(
            [0.123, 1.123, 2.123, 3.123, 4.123, 5.123],
            {
                "A": ("uint8", [0, 1, 2, 3, 4, 5]),
            },
        ),
        sep=",",
    ),
    SourceParseCase(  # float timestamps must be float64
        "\n".join(
            [
                "t",
                "1.1",
                "2.1",
                "3.1",
            ]
        ),
        {
            "t": "float32",
        },
        "t",
        ValueError,
    ),
]


@pytest.mark.parametrize("case", source_parse_cases)
def test_source_file_parsing(case: SourceParseCase):
    buffer = io.StringIO(case.src_text)

    if isinstance(case.expected_result, pd.DataFrame):
        result = opentoolflux.database.read_src_file(
            buffer, case.dtypes, case.timestamp_col, case.sep
        )
        print(result)
        pandas.testing.assert_frame_equal(result, case.expected_result)
    else:
        with pytest.raises(case.expected_result):
            opentoolflux.database.read_src_file(
                buffer, case.dtypes, case.timestamp_col, case.sep
            )


@pytest.mark.parametrize("case", source_parse_cases)
def test_db_save_and_load(case: SourceParseCase, tmp_path: Path):
    if not isinstance(case.expected_result, pd.DataFrame):
        return
    db_path = tmp_path / "db"
    db = case.expected_result
    opentoolflux.database.save_db(db, db_path)
    db_roundtripped = opentoolflux.database.read_db(db_path)
    pandas.testing.assert_frame_equal(db, db_roundtripped)


def test_update_db():
    db_1 = build_db([1.1, 2.2, 4.4], {"B": ("uint8", [1, 2, 4])})
    db_2 = build_db([2.2, 3.3], {"B": ("uint8", [5, 3])})
    expected_result = build_db([1.1, 2.2, 3.3, 4.4], {"B": ("uint8", [1, 5, 3, 4])})
    result = opentoolflux.database.update(db_1, db_2)
    pandas.testing.assert_frame_equal(result, expected_result)

    with pytest.raises(ValueError):
        db_2_other_dtype = build_db([2.2, 3.3], {"B": ("uint16", [5, 3])})
        opentoolflux.database.update(db_1, db_2_other_dtype)

    with pytest.raises(ValueError):
        db_2_extra_column = build_db(
            [2.2, 3.3], {"B": ("uint8", [5, 3]), "C": ("uint8", [5, 3])}
        )
        opentoolflux.database.update(db_1, db_2_extra_column)

    with pytest.raises(ValueError):
        db_2_missing_column = build_db([2.2, 3.3], {})
        opentoolflux.database.update(db_1, db_2_missing_column)


def test_create_empty_db():
    dtypes = {"x": "float32", "y": "uint8"}
    result = opentoolflux.database.create_empty_db({"t": "float64", **dtypes}, "t")
    assert len(result) == 0
    assert result.dtypes.to_dict() == dtypes
