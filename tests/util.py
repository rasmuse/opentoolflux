from __future__ import annotations

from typing import Mapping, Sequence, Tuple

import opentoolflux.database
import pandas as pd


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
                opentoolflux.database.TIMESTAMP_COLUMN: _build_timestamps(timestamps),
            }
        )
        .set_index(opentoolflux.database.TIMESTAMP_COLUMN)
    )


def _build_timestamps(values: list[float]):
    return pd.Series(values).mul(1e3).astype("datetime64[ms]")
