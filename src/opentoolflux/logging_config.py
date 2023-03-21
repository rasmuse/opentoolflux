# https://docs.python.org/3/library/logging.config.html#configuration-dictionary-schema
import logging.config
import os
from pathlib import Path
from typing import Any, Dict, Optional

LogSettingsDict = Dict[str, Any]

DEFAULT_LOG_SETTINGS = {
    "formatters": {
        "detailed": {
            "format": "%(asctime)s %(levelname)-8s %(name)-25s %(message)s",
        },
        "brief": {
            "format": "%(levelname)-8s %(message)s",
        },
    },
    "filters": {
        "allow_opentoolflux": {"name": "opentoolflux"},
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "brief",
            "filters": ["allow_opentoolflux"],
        },
        "file_debug": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "filename": "log.debug.txt",
            "formatter": "detailed",
            "maxBytes": 1e6,
            "backupCount": 5,
            "filters": ["allow_opentoolflux"],
        },
        "file_info": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "filename": "log.info.txt",
            "formatter": "detailed",
            "maxBytes": 1e6,
            "backupCount": 5,
            "filters": ["allow_opentoolflux"],
        },
    },
    "root": {
        "level": "DEBUG",
        "handlers": [
            "console",
            "file_debug",
            "file_info",
        ],
    },
    "version": 1,
    "disable_existing_loggers": False,
}


def setup_logging(settings: LogSettingsDict, logging_dir: Optional[Path] = None):
    if logging_dir is None:
        logging_dir = Path.cwd()
    cwd = Path.cwd()
    logging_dir.mkdir(parents=True, exist_ok=True)
    os.chdir(logging_dir)
    logging.config.dictConfig(settings)
    os.chdir(cwd)
