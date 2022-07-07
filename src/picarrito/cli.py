from __future__ import annotations

import os
from pathlib import Path
from typing import List, Mapping

import click
import pydantic
import toml

from . import db

_DEFAULT_CONFIG_PATH = Path("picarrito.toml")


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
    ctx.obj["config"] = Config.from_toml(config_path)

    work_dir = config_path.parent
    os.chdir(work_dir)


@main.command(name="import")
@click.pass_context
def import_(ctx: click.Context):
    conf: Config = ctx.obj["config"]
    new_data = db.read_src_files(
        conf.import_.src,
        conf.import_.columns,
        conf.import_.timestamp_col,
        conf.import_.sep,
    )
    try:
        old_db = db.read_db(conf.general.db_path)
        updated_db = db.update(old_db, new_data)
    except FileNotFoundError:
        updated_db = new_data

    db.save_db(updated_db, conf.general.db_path)


class General(pydantic.BaseModel):
    db_path: Path


class Import(pydantic.BaseModel):
    src: List[str]
    timestamp_col: str
    sep: str = r"\s"
    columns: Mapping[db.Colname, db.DTypeName]


class Config(pydantic.BaseModel):
    general: General
    import_: Import = pydantic.Field(alias="import")

    @classmethod
    def from_toml(cls, path: Path) -> Config:
        with open(path, "r") as f:
            obj = toml.load(f)
        return cls.parse_obj(obj)
