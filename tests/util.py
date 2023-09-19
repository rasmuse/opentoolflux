from __future__ import annotations

from typing import Mapping, Sequence, Tuple

import pandas as pd

import opentoolflux.database


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
                opentoolflux.database.TIMESTAMP_COLUMN: opentoolflux.database.convert_datetime(
                    pd.Series(timestamps)
                ),
            }
        )
        .set_index(opentoolflux.database.TIMESTAMP_COLUMN)
    )
