"""Database helper functions using the built‑in ``sqlite3`` module.

This module avoids an external dependency on SQLAlchemy by leveraging
Python’s built‑in ``sqlite3`` library.  It provides simple wrappers to
create a database connection, write DataFrames to SQL tables, and read
tables back into pandas.
"""

from __future__ import annotations

import sqlite3
from typing import Optional

import pandas as pd


def create_connection(db_path: str = "insight2profit.db") -> sqlite3.Connection:
    """Create a SQLite connection.

    Parameters
    ----------
    db_path: str, default "insight2profit.db"
        Path to the SQLite database file.  Use ``":memory:"`` to create an
        in‑memory database.

    Returns
    -------
    sqlite3.Connection
        A connection object that can be used with pandas ``to_sql`` and
        ``read_sql``.
    """
    # By default, SQLite will create the database file if it does not exist
    conn = sqlite3.connect(db_path)
    return conn


def write_df_to_sql(
    df: pd.DataFrame,
    table_name: str,
    conn: sqlite3.Connection,
    if_exists: str = "replace",
    index: bool = False,
) -> None:
    """Write a pandas DataFrame to a SQL table using a SQLite connection.

    Parameters
    ----------
    df: pandas.DataFrame
        The data to write.
    table_name: str
        Name of the SQL table.
    conn: sqlite3.Connection
        Connection to the SQLite database.
    if_exists: str, default "replace"
        Behaviour when the table already exists.  Options: "fail", "replace", "append".
    index: bool, default False
        Whether to write the DataFrame’s index as a column.
    """
    df.to_sql(table_name, con=conn, if_exists=if_exists, index=index)


def read_sql_table(table_name: str, conn: sqlite3.Connection) -> pd.DataFrame:
    """Read an entire table from the SQL database into a DataFrame.

    Since pandas’ ``read_sql_table`` relies on SQLAlchemy, this function
    instead constructs a simple ``SELECT *`` query using ``read_sql_query``.

    Parameters
    ----------
    table_name: str
        Name of the table to read.
    conn: sqlite3.Connection
        Connection to the SQLite database.

    Returns
    -------
    pandas.DataFrame
        The contents of the SQL table.
    """
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql_query(query, con=conn)