from __future__ import annotations

import collections
import datetime
import functools
import logging
import os
import shutil
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    TypeVar,
    Union,
    overload,
)

import click
import matplotlib.pyplot as plt
import pandas as pd
import pydantic
import tomli

import opentoolflux
from opentoolflux.fluxes import estimate_vol_flux
from opentoolflux.plot import plot_measurement, plot_time_series

from . import database, logging_config, measurements

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = Path("opentoolflux.toml")
_DEFAULT_OUTDIR = Path("opentoolflux")
_DB_FILENAME = "database.feather"
_PLOTS_SUBDIR = "plots"
_FLUXES_FILENAME = "fluxes.csv"


def nicely_repackage_config_problems(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (BadConfig, pydantic.ValidationError) as e:
            raise click.ClickException(str(e)) from e

    return wrapper


def require_database_file(func):
    @functools.wraps(func)
    def wrapper(ctx, *args, **kwargs):
        conf: Config = ctx.obj["config"]
        db_path = _get_db_path(conf)
        if not db_path.exists():
            raise click.ClickException(
                f"No database found at {db_path}. \n"
                f"First run the import command or copy a database file to {db_path}."
            )
        return func(ctx, *args, **kwargs)

    return wrapper


@click.group()
@click.pass_context
@click.option(
    "config_path",
    "--config",
    type=click.Path(dir_okay=False, path_type=Path),
    default=_DEFAULT_CONFIG_PATH,
)
@click.version_option(opentoolflux.__version__)
@nicely_repackage_config_problems
def main(ctx: click.Context, config_path: Path):
    ctx.ensure_object(dict)
    conf = Config.from_toml(config_path) if config_path.exists() else Config()
    ctx.obj["config"] = conf

    work_dir = config_path.parent
    os.chdir(work_dir)

    logging_config.setup_logging(conf.logging, conf.general.outdir)


@main.command(name="import")
@click.pass_context
def import_(ctx: click.Context):
    """
    Import data, creating or updating a database file.

    This command can be run multiple times without problems. The column specified as
    `timestamp_col` in the configuration file is used as key. Import data with
    timestamps a already existing in the database file are left unchanged, while
    new data are added to the database.
    """
    conf: Config = ctx.obj["config"]
    if conf.import_ is None:
        raise click.ClickException("The config has no section [import].")

    db_path = _get_db_path(conf)

    try:
        db = database.read_db(db_path)
    except FileNotFoundError:
        logger.info(f"No existing database at '{db_path}'.")
        db = database.create_empty_db(conf.import_.columns, conf.import_.timestamp_col)

    new_data = _read_src_files(
        conf.import_.src,
        conf.import_.columns,
        conf.import_.timestamp_col,
        conf.import_.sep,
    )

    summary_rows = {
        "Before": _build_db_summary_row(db),
        "New data": _build_db_summary_row(new_data),
    }

    db = database.update(db, new_data)

    summary_rows["After"] = _build_db_summary_row(db)
    logger.info(f"Database updated:\n{pd.DataFrame(summary_rows).T}")

    database.save_db(db, db_path)


def _read_src_files(
    glob_patterns: List[str],
    dtypes: database.DTypes,
    timestamp_col: database.Colname,
    sep: str,
) -> pd.DataFrame:
    paths = database.find_files(glob_patterns)
    with click.progressbar(paths, label="Reading source files", show_pos=True) as paths:
        datasets = [
            database.read_src_file(path, dtypes, timestamp_col, sep) for path in paths
        ]
    result = database.update(database.create_empty_db(dtypes, timestamp_col), *datasets)
    return result


def _build_db_summary_row(db: pd.DataFrame):
    return {
        "Size in memory (MB)": f"{(db.memory_usage().sum() / (1024 * 1024)):.1f}",
        "Rows": f"{len(db):,}",
    }


@main.command()
@click.pass_context
@require_database_file
def info(ctx: click.Context):
    """
    Print some info about the database.
    """
    conf: Config = ctx.obj["config"]
    n_measurements = collections.defaultdict(int)
    for m in _iter_measurements(conf):
        assert conf.measurements
        (chamber_value,) = m[conf.measurements.chamber_col].unique()
        chamber_label = _get_chamber_label(chamber_value, conf.chamber_labels)
        n_measurements[chamber_label] += 1

    n_measurements = (
        pd.Series(n_measurements).rename_axis("Chamber").rename("Count").sort_index()
    )
    logger.info(
        f"Found {n_measurements.sum()} measurement(s) "
        f"from {len(n_measurements)} chamber(s):\n\n{n_measurements.to_frame()}"
    )


@main.command()
@click.pass_context
@require_database_file
def fluxes(ctx: click.Context):
    """
    Estimate fluxes and save to a csv file in the output directory.

    This command overwrites previous flux estimates.
    """
    conf: Config = ctx.obj["config"]
    result = _estimate_fluxes_result_table(_iter_measurements(conf), conf)
    fluxes_path = conf.general.outdir / _FLUXES_FILENAME
    result.to_csv(
        fluxes_path,
        index=False,
    )
    logger.info(f"Saved fluxes to '{fluxes_path}'.")


@main.group()
@click.pass_context
def plot(ctx: click.Context):
    """
    Create figures for diagnostics (subcommands available).
    """
    pass


@plot.command()
@click.pass_context
@require_database_file
def flux_fits(ctx: click.Context):
    """
    Estimate fluxes and create figures showing time series and curve fits.

    These figures are useful to identify potential problems in the data, and to
    ensure that `t0_delay` and `t0_margin` parameters are set correctly.

    Pre-existing flux-fits figures are automatically removed by this command.
    """
    conf: Config = ctx.obj["config"]
    measurements = list(_iter_measurements(conf))
    plot_dir = conf.general.outdir / _PLOTS_SUBDIR / "flux-fits"
    if plot_dir.exists():
        shutil.rmtree(plot_dir)
    plot_dir.mkdir(parents=True, exist_ok=False)
    with click.progressbar(
        measurements, label="Plotting measurements", show_pos=True
    ) as measurements:
        for m in measurements:
            _plot_flux_fit(m, plot_dir, conf)


def _plot_flux_fit(measurement: pd.DataFrame, dst_dir: Path, conf: Config):
    if conf.measurements is None:
        raise click.ClickException("The config file has no section [measurements].")
    if conf.fluxes is None:
        raise click.ClickException("The config file has no section [fluxes].")
    (chamber_value,) = measurement[conf.measurements.chamber_col].unique()

    flux_estimates_by_gas = {
        gas: _estimate_vol_flux(measurement, gas, conf) for gas in conf.fluxes.gases
    }

    fig = plot_measurement(
        measurement,
        conf.fluxes.gases,
        flux_estimates_by_gas,
        title=_get_chamber_label(chamber_value, conf.chamber_labels),
    )
    plot_path = dst_dir / _build_measurement_file_name(measurement, conf, ".png")
    fig.savefig(plot_path)
    plt.close(fig)


@plot.command()
@click.pass_context
@require_database_file
def flux_time_series(ctx: click.Context):
    """
    Estimate fluxes and create time-series figures with fluxes for each chamber.

    These figures are useful to get a first view of flux variations over time
    by chamber. Further plotting and analysis can be made based on results created
    using the `fluxes` command.

    Pre-existing flux time series figures are automatically removed by this command.
    """
    conf: Config = ctx.obj["config"]
    if conf.fluxes is None:
        raise click.ClickException("The config file has no section [fluxes].")
    fluxes = _estimate_fluxes_result_table(_iter_measurements(conf), conf)
    plot_dir = conf.general.outdir / _PLOTS_SUBDIR / "flux-time-series"
    if plot_dir.exists():
        shutil.rmtree(plot_dir)
    plot_dir.mkdir(parents=True, exist_ok=False)

    for chamber_value, chamber_data in fluxes.groupby("chamber_value"):
        chamber_label = _get_chamber_label(chamber_value, conf.chamber_labels)
        title = f"Volumetric fluxes, chamber {chamber_label}"
        plot_path = plot_dir / f"{chamber_label}.png"
        fig = plot_time_series(chamber_data, conf.fluxes.gases, title=title)
        fig.savefig(plot_path)
        plt.close(fig)


def _get_chamber_labels_series(
    chamber_values: pd.Series, chamber_labels: Optional[ChamberLabels]
) -> pd.Series:
    if chamber_labels is None:
        return chamber_values.astype(str)

    replacements = pd.Series(chamber_labels)
    replacements.index = replacements.index.astype(chamber_values.dtype)

    requested_values = (
        set(chamber_values.unique())
        if isinstance(chamber_values, pd.Series)
        else {chamber_values}
    )
    values_with_labels = set(replacements.index)
    missing = requested_values - values_with_labels
    if missing:
        raise click.UsageError(f"No chamber label specified for chambers {missing!r}")

    if isinstance(chamber_values, pd.Series):
        return chamber_values.replace(replacements).astype(str)
    else:
        return replacements[chamber_values]


def _get_chamber_label(
    chamber_value: Union[int, float, bool, str],
    chamber_labels: Optional[ChamberLabels],
):
    return _get_chamber_labels_series(pd.Series([chamber_value]), chamber_labels)[0]


def _build_measurement_file_name(measurement: pd.DataFrame, conf: Config, suffix: str):
    if conf.measurements is None:
        raise click.ClickException("The config file has no section [measurements].")
    (chamber_value,) = measurement[conf.measurements.chamber_col].unique()
    chamber_label = _get_chamber_label(chamber_value, conf.chamber_labels)
    data_start = measurement.index[0]
    return f"{chamber_label}-{data_start:%Y%m%d-%H%M%S}{suffix}"


def _estimate_vol_flux(measurement: pd.DataFrame, gas: str, conf: Config):
    assert conf.measurements is not None
    assert conf.fluxes is not None

    if isinstance(conf.fluxes.t0_delay, datetime.timedelta):
        # If a single t0_delay is used for all chambers
        t0_delay = conf.fluxes.t0_delay
    else:
        (chamber_value,) = measurement[conf.measurements.chamber_col].unique()
        assert isinstance(chamber_value, (float, int, bool, str))
        type_ = type(chamber_value)
        t0_delays = _convert_str_keys(conf.fluxes.t0_delay, type_)
        if chamber_value not in t0_delays:
            raise click.ClickException(
                f"t0_delay not defined for chamber {chamber_value}"
            )
        t0_delay = t0_delays[chamber_value]

    return estimate_vol_flux(
        measurement[gas],
        t0_delay=t0_delay,
        t0_margin=conf.fluxes.t0_margin,
        tau_s=conf.fluxes.tau_s,
        h=conf.fluxes.h,
    )


def _estimate_fluxes_result_table(measurements: Iterable[pd.DataFrame], conf: Config):
    if conf.measurements is None:
        raise click.ClickException("The config file has no section [measurements].")
    if conf.fluxes is None:
        raise click.ClickException("The config file has no section [fluxes].")

    def build_row(measurement: pd.DataFrame, gas: database.Colname):
        flux_est = _estimate_vol_flux(measurement, gas, conf)

        assert conf.measurements
        (chamber_value,) = measurement[conf.measurements.chamber_col].unique()

        result_row = {
            **flux_est,
            "chamber_value": chamber_value,
            "chamber_label": _get_chamber_label(chamber_value, conf.chamber_labels),
            "gas": gas,
        }

        return result_row

    # The click.progressbar(list(measurements), ...) makes a full list
    # of all measurements, which increases memory consumption compared the iterator,
    # for the purpose of being able to know the progress.
    # However, this is only one of several places in the source code that
    # requires 2x full database in memory.
    with click.progressbar(
        list(measurements), label="Analyzing measurements", show_pos=True
    ) as measurements:
        result_table = pd.DataFrame.from_records(
            [
                build_row(measurement, gas)
                for measurement in measurements
                for gas in conf.fluxes.gases
            ]
        )

    result_table = result_table[_FLUXES_COLUMNS_ORDER]

    logger.info(
        f"Estimated {len(result_table)} fluxes ({', '.join(conf.fluxes.gases)}) "
        f"in {result_table['t0'].nunique()} measurements."
    )

    return result_table


_FLUXES_COLUMNS_ORDER = [
    "data_start",
    "t0",
    "chamber_value",
    "chamber_label",
    "gas",
    "c0",
    "vol_flux",
]


def _iter_measurements(conf: Config):
    if conf.measurements is None:
        raise click.ClickException("The config file has no section [measurements].")
    db = database.read_db(_get_db_path(conf))
    db = measurements.filter_db(db, conf.filters)
    yield from measurements.iter_measurements(
        db,
        conf.measurements.chamber_col,
        conf.measurements.max_gap,
        conf.measurements.min_duration,
        conf.measurements.max_duration,
    )


def _get_db_path(conf: Config) -> Path:
    return conf.general.outdir / _DB_FILENAME


class BadConfig(Exception):
    pass


class General(pydantic.BaseModel):
    outdir: Path = _DEFAULT_OUTDIR


class Import(pydantic.BaseModel):
    src: List[str]
    timestamp_col: str
    sep: str = r"\s+"
    columns: Mapping[database.Colname, database.DTypeName]

    @pydantic.validator("columns")
    def timestamp_float_must_be_float64(cls, columns, values):
        timestamp_col = values["timestamp_col"]
        assert isinstance(timestamp_col, str)
        if timestamp_col not in columns:
            raise BadConfig(f"No data type given for timestamp_col '{timestamp_col}'.")
        timestamp_dtype = columns[timestamp_col]
        if timestamp_dtype.startswith("float") and timestamp_dtype != "float64":
            raise BadConfig(
                f"Floating-point timestamp_col '{timestamp_col}' must be float64."
            )
        return columns


class Measurements(pydantic.BaseModel):
    chamber_col: str
    max_gap: datetime.timedelta
    min_duration: datetime.timedelta
    max_duration: datetime.timedelta


ChamberLabel = str
ChamberLabels = Mapping[str, ChamberLabel]


class Fluxes(pydantic.BaseModel):
    gases: List[str]
    t0_delay: Union[datetime.timedelta, Dict[str, datetime.timedelta]]
    t0_margin: datetime.timedelta
    A: float
    Q: float
    V: float

    @property
    def tau_s(self) -> float:
        return self.V / self.Q

    @property
    def h(self) -> float:
        return self.V / self.A


class Config(pydantic.BaseModel):
    general: General = pydantic.Field(default_factory=General)
    import_: Optional[Import] = pydantic.Field(alias="import", default=None)
    filters: Mapping[database.Colname, measurements.Filter] = pydantic.Field(
        default_factory=dict
    )
    measurements: Optional[Measurements] = pydantic.Field(default=None)
    chamber_labels: Optional[ChamberLabels] = pydantic.Field(default=None)
    fluxes: Optional[Fluxes] = pydantic.Field(default=None)
    logging: Dict[str, Any] = logging_config.DEFAULT_LOG_SETTINGS

    @pydantic.validator("chamber_labels")
    def chamber_labels_must_be_one_to_one(cls, chamber_labels):
        label_to_key = {v: k for k, v in chamber_labels.items()}
        violator_keys = set(chamber_labels) - set(label_to_key.values())
        violator_labels = {v for k, v in chamber_labels.items() if k in violator_keys}
        violations = {k: v for k, v in chamber_labels.items() if v in violator_labels}
        if violations:
            raise BadConfig(f"chamber labels collide: {violations}")
        return chamber_labels

    @classmethod
    def from_toml(cls, path: Path) -> Config:
        logger.debug(f"Reading config file {path}")
        with open(path, "rb") as f:
            obj = tomli.load(f)
        return cls.parse_obj(obj)


KT = TypeVar("KT")
VT = TypeVar("VT")


def _convert_str_keys(
    d: Mapping[str, VT], convert: Callable[[Any], KT]
) -> Mapping[KT, VT]:
    return {convert(k): v for k, v in d.items()}
