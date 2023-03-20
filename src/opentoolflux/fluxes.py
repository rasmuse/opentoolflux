from __future__ import annotations

import datetime
import logging
from typing import Any, Mapping, TypedDict, Union

import numpy as np
import numpy.linalg
import pandas as pd

logger = logging.getLogger(__name__)


class VolFluxEstimate(TypedDict):
    data_start: datetime.datetime
    t0: datetime.datetime
    tau_s: float
    h: float
    fit_start: datetime.datetime
    fit_end: datetime.datetime
    c0: float
    vol_flux: float


def estimate_vol_flux(
    measurement: pd.Series,
    t0_delay: datetime.timedelta,
    t0_margin: datetime.timedelta,
    tau_s: float,
    h: float,
) -> VolFluxEstimate:
    assert isinstance(measurement.index, pd.DatetimeIndex)
    data_start = measurement.index[0]
    assert isinstance(data_start, datetime.datetime)
    t0 = data_start + t0_delay
    data_analyze = measurement[
        measurement.index >= data_start + t0_delay + t0_margin  # type: ignore
    ]
    assert isinstance(data_analyze.index, pd.DatetimeIndex)
    elapsed_seconds = (data_analyze.index - t0).total_seconds()  # type: ignore
    concentrations = data_analyze.values
    assert isinstance(concentrations, np.ndarray)
    c0, vol_flux = _calculate_vol_flux_from_cleaned_data(
        elapsed_seconds, concentrations, tau_s, h
    )
    return {
        "data_start": data_start,
        "t0": t0,
        "tau_s": tau_s,
        "h": h,
        "fit_start": data_analyze.index[0],
        "fit_end": data_analyze.index[-1],
        "c0": c0,
        "vol_flux": vol_flux,
    }


def predict_concentration(
    vol_flux_estimate: Mapping[str, Any], times: Union[np.ndarray, pd.DatetimeIndex]
) -> np.ndarray:
    elapsed_s = (times - vol_flux_estimate["t0"]).total_seconds()
    c0 = vol_flux_estimate["c0"]
    vol_flux = vol_flux_estimate["vol_flux"]
    tau_s = vol_flux_estimate["tau_s"]
    h = vol_flux_estimate["h"]
    return c0 + vol_flux * tau_s / h * (1 - np.exp(-elapsed_s / tau_s))


def _calculate_vol_flux_from_cleaned_data(
    elapsed: np.ndarray, concentrations: np.ndarray, tau: float, h: float
) -> tuple[float, float]:
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
