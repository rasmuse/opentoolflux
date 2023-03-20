from __future__ import annotations

import importlib.resources as resources
from multiprocessing.sharedctypes import Value
from typing import Any, Iterable, Mapping, Optional, Sequence

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.figure import Figure

from opentoolflux import fluxes

from . import database

# Matplotlib TkAgg backend hogs memory and crashes with too many figures:
# https://github.com/matplotlib/matplotlib/issues/21950
mpl.use("agg")

with resources.path("opentoolflux.resources", "matplotlib-style") as path:
    mpl.style.use(path)  # pyright: reportGeneralTypeIssues=false


_SECONDS_PER_MINUTE = 60

prop_cycle = mpl.rc_params()["axes.prop_cycle"]
colors = prop_cycle.by_key()["color"]
_MEASUREMENT_KWS = dict(color="k", lw=0, marker=".", markersize=2)
_TIME_SERIES_KWS = dict(color="k", lw=0.5, ls="--", marker=".", markersize=5)
_ESTIMATOR_COLOR = colors[1]


def _subplot_title(column):
    return column


def plot_measurement(
    measurement: pd.DataFrame,
    gases: Sequence[database.Colname],
    flux_estimates: Mapping[database.Colname, Mapping[str, Any]],
    title: Optional[str] = None,
) -> Figure:
    # Rough calculation of height depending on number of panels;
    # nothing scientific at all and probably will break down for large numbers.
    height_per_column = 1.7
    height_extra = 1.3
    height_total = height_per_column * len(gases) + height_extra
    share_extra = height_extra / height_total
    fig, axs = plt.subplots(
        nrows=len(gases),
        sharex=True,
        gridspec_kw=dict(
            top=1 - 0.4 * share_extra,
            hspace=0.3,
            bottom=0.6 * share_extra,
        ),
        figsize=(6.4, height_total),
    )

    fig.suptitle(title)

    measurement_start = measurement.index[0]

    if len(gases) > 1:
        ax_by_column = dict(zip(gases, axs))  # type: ignore
    else:
        ax_by_column = {gases[0]: axs}

    def calculate_elapsed(time):
        return (time - measurement_start).total_seconds() / _SECONDS_PER_MINUTE

    for column in gases:
        ax = ax_by_column[column]
        ax.set_title(_subplot_title(column))
        ax.plot(
            calculate_elapsed(measurement.index),
            measurement[column],
            **_MEASUREMENT_KWS,
        )

    for gas, flux_estimate in flux_estimates.items():
        if flux_estimate["data_start"] != measurement_start:
            raise ValueError(
                "Flux estimate and measurement start at different times: "
                f"{flux_estimate['data_start']} {measurement_start}"
            )

        estimator_times = measurement.index[
            (flux_estimate["fit_start"] <= measurement.index)
            & (measurement.index <= flux_estimate["fit_end"])
        ]
        estimated_values = fluxes.predict_concentration(flux_estimate, estimator_times)
        ax = ax_by_column[gas]

        # Draw fitted function
        ax.plot(
            calculate_elapsed(estimator_times),
            estimated_values,
            lw=2,
            color=_ESTIMATOR_COLOR,
            label=f"Estimator fit",
        )

        # Draw vertical line at t0
        ax.axvline(
            [calculate_elapsed(flux_estimate["t0"])],
            lw=1,
            color=_ESTIMATOR_COLOR,
            linestyle="--",
            label="t0",
        )

    for ax in ax_by_column.values():
        ax.legend(loc="lower left", bbox_to_anchor=(0, 0))

    last_ax = ax_by_column[gases[-1]]
    last_ax.set_xlabel(
        f"Time elapsed (minutes) since\n{measurement_start:%Y-%m-%d %H:%M:%S}"
    )

    return fig


def plot_time_series(
    fluxes: pd.DataFrame,
    gases: Sequence[database.Colname],
    title: Optional[str] = None,
) -> Figure:
    fluxes = fluxes.set_index(["gas", "t0"])["vol_flux"].sort_index()

    # Rough calculation of height depending on number of panels;
    # nothing scientific at all and probably will break down for large numbers.
    height_per_column = 1.7
    height_extra = 1.3
    height_total = height_per_column * len(gases) + height_extra
    share_extra = height_extra / height_total
    fig, axs = plt.subplots(
        nrows=len(gases),
        sharex=True,
        gridspec_kw=dict(
            top=1 - 0.4 * share_extra,
            hspace=0.3,
            bottom=0.6 * share_extra,
        ),
        figsize=(6.4, height_total),
    )

    fig.suptitle(title)

    if len(gases) > 1:
        ax_by_column = dict(zip(gases, axs))  # type: ignore
    else:
        ax_by_column = {gases[0]: axs}

    for column in gases:
        ax = ax_by_column[column]
        ax.set_title(_subplot_title(column))
        ax.set_ylabel("flux")

        ax.plot(
            fluxes.xs(column),
            **_TIME_SERIES_KWS,
        )

    last_ax = ax_by_column[gases[-1]]
    last_ax.set_xlabel(f"t0")
    last_ax.tick_params("x", labelrotation=30)

    return fig
