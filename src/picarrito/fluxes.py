from __future__ import annotations

import datetime
import logging

import numpy as np
import numpy.linalg
import pandas as pd

logger = logging.getLogger(__name__)


def estimate_vol_flux(
    measurement: pd.Series,
    t0_delay: datetime.timedelta,
    t0_margin: datetime.timedelta,
    tau_s: float,
    h: float,
):
    data_start = measurement.index[0]
    t0 = data_start + t0_delay
    data_analyze = measurement[measurement.index >= data_start + t0_delay + t0_margin]
    elapsed_seconds = (data_analyze.index - t0).total_seconds()
    concentrations = data_analyze.values
    c0, vol_flux = _calculate_vol_flux_from_cleaned_data(
        elapsed_seconds, concentrations, tau_s, h
    )
    return pd.Series(
        dict(
            data_start=data_start,
            t0=t0,
            c0=c0,
            vol_flux=vol_flux,
        )
    )


def _calculate_vol_flux_from_cleaned_data(
    elapsed: np.array, concentrations: np.array, tau: float, h: float
) -> float:
    # The differential equation solution is
    # c(t) == c(0) + F * (tau/h) * (1 - exp(-elapsed/tau))
    #
    # We know tau (= V / Q) and h (= V / A).
    # c(0) and F are unknown; F is the interesting quantity to be estimated from data.
    #
    # Rename things as follows:
    # x0 = c(0)
    # x1 = F
    # z = (tau / h) * (1 - exp(-elapsed/tau))
    # b = c(t)
    #
    # Then it is clear that this is an equation system,
    # 1 * x0 + z * x1 = b,
    # which can be solved using linear regression (least squares).

    z = (tau / h) * (1 - np.exp(-elapsed / tau))
    a = np.vstack([np.ones(len(z)), z]).T
    b = concentrations
    (c0, F), _, _, _ = numpy.linalg.lstsq(a, b, rcond=None)
    return (c0, F)
