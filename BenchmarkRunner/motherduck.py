import duckdb
from duckdb import DuckDBPyConnection

import os
import logging

from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def get_config_from_env():
    config = {
        "allow_unsigned_extensions": os.environ["MDTEST_ALLOW_UNSIGNED_EXTENSIONS"],
        "motherduck_host": os.environ["MDTEST_HOST"],
        "motherduck_token": os.environ["MDTEST_TOKEN"],
    }
    return config


def connect(connection_string=None, additional_config={}):
    config = get_config_from_env() | additional_config
    try:
        return duckdb.connect(connection_string, config=config)
    except Exception as e:
        print(e)


def attach(conn: DuckDBPyConnection, path: str, alias: str):
    logger.info(f"Attaching {path} as {alias}")
    path = path.strip()
    conn.sql("ATTACH '" + path + "' AS " + alias)
