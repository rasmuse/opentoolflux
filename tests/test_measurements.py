from __future__ import annotations

import datetime
import itertools
from dataclasses import dataclass

import numpy as np
import opentoolflux.database
import pandas.testing
from opentoolflux.measurements import Filter, filter_db, iter_measurements

from tests.util import build_db


def test_filter():
    db_before = build_db(
        [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8],
        {
            "A": ("str", ["A", "B", "C", "D", "E", "F", "G", "H"]),
            "B": ("uint8", [1, 2, 3, 4, 5, 6, 7, 8]),
            "C": ("float32", [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]),
            "D": ("bool", [True, True, True, True, False, False, False, False]),
        },
    )

    filters = {
        opentoolflux.database.TIMESTAMP_COLUMN: Filter(
            min_value=np.datetime64("1970-01-01 00:00:02.53")
        ),
        "A": Filter(disallow=["A"]),
        "B": Filter(min_value=1, max_value=4),
        "C": Filter(min_value=2.0, max_value=5.0),
        "D": Filter(allow_only=[True]),
    }

    expected_result = db_before.iloc[[2, 3]]

    pandas.testing.assert_frame_equal(filter_db(db_before, filters), expected_result)


def test_split():
    max_gap = 1.4
    min_duration = 2.5
    max_duration = 4.0

    @dataclass
    class Part:
        included: bool
        identifier: str
        label: str
        time_increments: list[float]

    parts = (
        Part(True, "A1", "A", [100.0, 1.4, 1.4]),  # OK duration (2.8), OK gaps
        Part(False, "B1", "B", [0.1, 1.4, 1.4, 1.4]),  # too long duration, OK gaps
        Part(False, "C1", "C", [0.1, 1.5, 1.1]),  # OK duration, too long gap
        Part(True, "C2", "C", [1.5, 1.1, 1.1, 1.1]),  # OK duration (3.3), OK gaps
        Part(False, "D1", "D", [0.1]),  # too short duration (0)
        Part(False, "E1", "E", [0.1, 0.1]),  # too short duration (0.1)
        Part(False, "F1", "F", [0.1, 1.0, 1.0, 1.0, 1.1]),  # too long duration (4.1)
        Part(True, "G1", "G", [0.1, 1.0, 1.0, 1.0, 1.0]),  # OK duration (4.0), OK gaps
    )

    times = np.cumsum(list(itertools.chain(*(part.time_increments for part in parts))))
    identifiers = list(
        itertools.chain(
            *([part.identifier] * len(part.time_increments) for part in parts)
        )
    )
    labels = list(
        itertools.chain(*([part.label] * len(part.time_increments) for part in parts))
    )

    db = build_db(
        list(times),
        {
            "A": ("str", labels),
            "I": ("str", identifiers),
        },
    )

    measurements = list(
        iter_measurements(
            db,
            "A",
            max_gap=datetime.timedelta(seconds=max_gap),
            min_duration=datetime.timedelta(seconds=min_duration),
            max_duration=datetime.timedelta(seconds=max_duration),
        )
    )

    expected_measurements = [
        db.loc[db["I"] == part.identifier] for part in parts if part.included
    ]

    for measeurement, expected_measurement in zip(measurements, expected_measurements):
        print(measeurement)
        print(expected_measurement)
        pandas.testing.assert_frame_equal(measeurement, expected_measurement)
