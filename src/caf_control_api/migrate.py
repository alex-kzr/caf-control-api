"""Explicit one-step schema initialization for the P0 database."""

import os
from importlib.resources import files

import psycopg


def main() -> None:
    dsn = os.environ["CAF_DATABASE_URL"]
    schema = files("caf_control_api").joinpath("schema.sql").read_text(encoding="utf-8")
    with psycopg.connect(dsn) as connection:
        connection.execute(schema)


if __name__ == "__main__":
    main()
