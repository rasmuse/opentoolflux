import io
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional, Sequence, Tuple, Type, Union

import pandas as pd
import pandas.testing
import pytest

import picarrito.db

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


def build_db(
    timestamps: Sequence[float], cols: Mapping[str, Tuple[str, Sequence]]
) -> pd.DataFrame:
    return (
        pd.DataFrame(
            {
                col: pd.Series(dtype=dtype, data=values)
                for col, (dtype, values) in cols.items()
            }
        )
        .assign(
            **{
                picarrito.db.TIMESTAMP_COLUMN: _build_timestamps(timestamps),
                picarrito.db.EXCLUDE_COLUMN: False,
            }
        )
        .set_index(picarrito.db.TIMESTAMP_COLUMN)
    )


def _build_timestamps(values: list[float]):
    return pd.Series(values).mul(1e3).astype("datetime64[ms]")


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
    SourceParseCase(  # rows with null values are dropped
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
            [1.1, 3.1],
            {
                "B": ("float32", [1.2, 3.2]),
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
]


@pytest.mark.parametrize("case", source_parse_cases)
def test_source_file_parsing(case: SourceParseCase):
    buffer = io.StringIO(case.src_text)

    if isinstance(case.expected_result, pd.DataFrame):
        result = picarrito.db.read_src_file(
            buffer, case.dtypes, case.timestamp_col, case.sep
        )
        print(result)
        pandas.testing.assert_frame_equal(result, case.expected_result)
    else:
        with pytest.raises(case.expected_result):
            picarrito.db.read_src_file(
                buffer, case.dtypes, case.timestamp_col, case.sep
            )


@pytest.mark.parametrize("case", source_parse_cases)
def test_db_save_and_load(case: SourceParseCase, tmp_path: Path):
    if not isinstance(case.expected_result, pd.DataFrame):
        return
    db_path = tmp_path / "db"
    db = case.expected_result
    picarrito.db.save_db(db, db_path)
    db_roundtripped = picarrito.db.read_db(db_path)
    pandas.testing.assert_frame_equal(db, db_roundtripped)


def test_update_db():
    db_1 = build_db([1.1, 2.2, 4.4], {"B": ("uint8", [1, 2, 4])})
    db_2 = build_db([2.2, 3.3], {"B": ("uint8", [5, 3])})
    expected_result = build_db([1.1, 2.2, 3.3, 4.4], {"B": ("uint8", [1, 5, 3, 4])})
    result = picarrito.db.update(db_1, db_2)
    pandas.testing.assert_frame_equal(result, expected_result)

    with pytest.raises(ValueError):
        db_2_other_dtype = build_db([2.2, 3.3], {"B": ("uint16", [5, 3])})
        picarrito.db.update(db_1, db_2_other_dtype)

    with pytest.raises(ValueError):
        db_2_extra_column = build_db(
            [2.2, 3.3], {"B": ("uint8", [5, 3]), "C": ("uint8", [5, 3])}
        )
        picarrito.db.update(db_1, db_2_extra_column)

    with pytest.raises(ValueError):
        db_2_missing_column = build_db([2.2, 3.3], {})
        picarrito.db.update(db_1, db_2_missing_column)


def test_read_files(tmp_path: Path):
    files = {
        "dir/subdir1/file1.dat": "\n".join(
            [
                "A,B",
                "2.2,2",
            ]
        ),
        "dir/subdir2/another_file2.dat": "\n".join(
            [
                "A,B",
                "3.3,3",
            ]
        ),
        "dir/subdir3/file3.dat": "\n".join(
            [
                "A,B",
                "4.4,4",
                "1.1,1",
            ]
        ),
    }

    expected_result = build_db([1.1, 2.2, 3.3, 4.4], {"B": ("uint8", [1, 2, 3, 4])})

    for relpath, contents in files.items():
        dst_path = tmp_path / relpath
        dst_path.parent.mkdir(parents=True)
        with open(dst_path, "w") as f:
            f.write(contents)

    cwd = Path.cwd()
    os.chdir(tmp_path)
    result = picarrito.db.read_src_files(
        ["dir/**/file*.dat", "dir/subdir*/another*.dat"],
        {"A": "float32", "B": "uint8"},
        "A",
        ",",
    )
    os.chdir(cwd)

    pandas.testing.assert_frame_equal(result, expected_result)


def test_read_files_empty():
    dtypes = {"x": "float32", "y": "uint8"}
    result = picarrito.db.read_src_files([], {"t": "float64", **dtypes}, "t", ",")
    assert len(result) == 0
    assert result.dtypes.to_dict() == {picarrito.db.EXCLUDE_COLUMN: "bool", **dtypes}
