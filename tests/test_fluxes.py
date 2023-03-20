import datetime

import numpy as np
import opentoolflux.fluxes
import pandas as pd


def test_recover_flux():
    F = 0.0001234
    c0 = 0.3
    h = 0.2
    tau_s = 10000.0
    t0_delay = 300
    t0_margin = 100
    measurement_duration = 1200.0
    num_samples = 1000
    times_s = np.linspace(0, measurement_duration, num_samples)
    elapsed_s = times_s - t0_delay
    is_before_t0 = elapsed_s < 0
    concentrations = c0 + F * tau_s / h * (1 - np.exp(-elapsed_s / tau_s))
    concentrations[is_before_t0] = c0 * 1000  # what happens before t0 is irrelevant
    measurement = pd.Series(
        data=concentrations,
        index=(times_s * 1e3).astype("datetime64[ms]"),  # type: ignore
    )

    # With t0_delay and tau correctly specified; should work perfectly
    result = opentoolflux.fluxes.estimate_vol_flux(
        measurement,
        t0_delay=datetime.timedelta(seconds=t0_delay),
        t0_margin=datetime.timedelta(seconds=0),
        tau_s=tau_s,
        h=h,
    )
    print(result)
    assert _rel_error(result["c0"], c0) < 1e-3
    assert _rel_error(result["vol_flux"], F) < 1e-3

    # With t0_delay and tau correctly specified and an extra margin; still works
    result = opentoolflux.fluxes.estimate_vol_flux(
        measurement,
        t0_delay=datetime.timedelta(seconds=t0_delay),
        t0_margin=datetime.timedelta(seconds=t0_margin),
        tau_s=tau_s,
        h=h,
    )
    assert _rel_error(result["c0"], c0) < 1e-3
    assert _rel_error(result["vol_flux"], F) < 1e-3

    # With t0_delay specified too low, picks up bad data and gets an error
    result = opentoolflux.fluxes.estimate_vol_flux(
        measurement,
        t0_delay=datetime.timedelta(seconds=0),
        t0_margin=datetime.timedelta(seconds=0),
        tau_s=tau_s,
        h=h,
    )
    assert _rel_error(result["c0"], c0) > 0.5
    assert _rel_error(result["vol_flux"], F) > 0.5


def _rel_error(estimate, true_value):
    return abs(estimate / true_value - 1)
