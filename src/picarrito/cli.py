from __future__ import annotations

import datetime
import logging
import os
from pathlib import Path
from typing import List, Mapping

import click
import pydantic
import toml

from . import analyze, database, logging_config

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = Path("picarrito.toml")
_DEFAULT_DB_PATH = Path("picarrito_db.feather")


@click.group()
@click.pass_context
@click.option(
    "config_path",
    "--config",
    type=click.Path(dir_okay=False, path_type=Path, exists=True),
    default=_DEFAULT_CONFIG_PATH,
)
def main(ctx: click.Context, config_path: Path):
    ctx.ensure_object(dict)
    conf = Config.from_toml(config_path)
    ctx.obj["config"] = conf

    logging_config.setup_logging(conf.logging)

    work_dir = config_path.parent
    os.chdir(work_dir)


@main.command(name="import")
@click.pass_context
def import_(ctx: click.Context):
    conf: Config = ctx.obj["config"]
    new_data = database.read_src_files(
        conf.import_.src,
        conf.import_.columns,
        conf.import_.timestamp_col,
        conf.import_.sep,
    )
    try:
        old_db = database.read_db(conf.general.db_path)
        updated_db = database.update(old_db, new_data)
    except FileNotFoundError:
        updated_db = new_data

    database.save_db(updated_db, conf.general.db_path)


@main.command()
@click.pass_context
def info(ctx: click.Context):
    conf: Config = ctx.obj["config"]
    for _ in _iter_measurements(conf):
        pass


def _iter_measurements(conf: Config):
    db = database.read_db(conf.general.db_path)
    db = analyze.filter_db(db, conf.filters)
    yield from analyze.iter_measurements(
        db,
        conf.measurements.chamber_col,
        conf.measurements.max_gap,
        conf.measurements.min_duration,
        conf.measurements.max_duration,
    )


class General(pydantic.BaseModel):
    db_path: Path = _DEFAULT_DB_PATH


class Import(pydantic.BaseModel):
    src: List[str]
    timestamp_col: str
    sep: str = r"\s"
    columns: Mapping[database.Colname, database.DTypeName]


class Measurements(pydantic.BaseModel):
    chamber_col: str
    max_gap: datetime.timedelta
    min_duration: datetime.timedelta
    max_duration: datetime.timedelta


class Config(pydantic.BaseModel):
    general: General
    import_: Import = pydantic.Field(alias="import")
    filters: Mapping[database.Colname, analyze.Filter] = pydantic.Field(
        default_factory=dict
    )
    measurements: Measurements
    logging: Mapping = logging_config.DEFAULT_LOG_SETTINGS

    @classmethod
    def from_toml(cls, path: Path) -> Config:
        logger.debug(f"Reading config file {path}")
        with open(path, "r") as f:
            obj = toml.load(f)
        return cls.parse_obj(obj)
