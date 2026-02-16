"""Data loading and initial storage logic.

This module reads the provided CSV files into pandas DataFrames, stores them
in a SQLite database with a ``raw_`` prefix, and then performs basic type
coercions before storing the results with a ``store_`` prefix.  It also
identifies primary and foreign keys in the process.
"""

from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import Dict, Tuple

from .database import write_df_to_sql
import sqlite3



def load_raw_tables(conn: sqlite3.Connection, data_dir: Path) -> Dict[str, pd.DataFrame]:
    """Load CSV files into DataFrames and write them as `raw_` tables.

    Parameters
    ----------
    engine: sqlalchemy.engine.Engine
        Database engine where the raw tables will be stored.
    data_dir: pathlib.Path
        Directory containing the provided CSV files.  The function expects
        `products.csv`, `sales_order_header.csv` and `sales_order_detail.csv`.

    Returns
    -------
    dict[str, pandas.DataFrame]
        A dictionary mapping logical table names (without the `raw_` prefix) to
        their loaded DataFrame representations.
    """
    tables = {}
    # Define mapping from logical name to filename
    files = {
        "products": "products.csv",
        "sales_order_header": "sales_order_header.csv",
        "sales_order_detail": "sales_order_detail.csv",
    }
    for name, filename in files.items():
        path = data_dir / filename
        df = pd.read_csv(path)
        tables[name] = df
        # Write to raw table
        write_df_to_sql(df, table_name=f"raw_{name}", conn=conn, if_exists="replace", index=False)
    return tables


def transform_store_tables(conn: sqlite3.Connection, raw_tables: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Coerce data types for the raw tables and write them as `store_` tables.

    This function converts date strings to datetime objects, handles nullable
    integer columns, and ensures booleans and floats have appropriate dtypes.

    Parameters
    ----------
    engine: sqlalchemy.engine.Engine
        Database engine where the transformed tables will be stored.
    raw_tables: dict[str, pandas.DataFrame]
        The raw tables returned by :func:`load_raw_tables`.

    Returns
    -------
    dict[str, pandas.DataFrame]
        A dictionary mapping logical table names to their transformed
        DataFrames.
    """
    store_tables: Dict[str, pd.DataFrame] = {}
    # Transform products
    products = raw_tables["products"].copy()
    # Ensure correct dtypes
    # ProductID is int; it's already int
    products["ProductID"] = products["ProductID"].astype(int)
    products["MakeFlag"] = products["MakeFlag"].astype(bool)
    # Numeric columns
    numeric_cols = ["SafetyStockLevel", "ReorderPoint"]
    for col in numeric_cols:
        products[col] = products[col].astype(int)
    float_cols = ["StandardCost", "ListPrice", "Weight"]
    for col in float_cols:
        products[col] = pd.to_numeric(products[col], errors="coerce")
    # Write store_products
    store_tables["products"] = products
    write_df_to_sql(products, table_name="store_products", conn=conn, if_exists="replace", index=False)

    # Transform sales_order_header
    header = raw_tables["sales_order_header"].copy()
    header["SalesOrderID"] = header["SalesOrderID"].astype(int)
    # OrderDate: parse year-month to datetime
    header["OrderDate"] = pd.to_datetime(header["OrderDate"], format='mixed')
    # ShipDate: parse full date strings
    header["ShipDate"] = pd.to_datetime(header["ShipDate"], errors="coerce")
    header["OnlineOrderFlag"] = header["OnlineOrderFlag"].astype(bool)
    header["AccountNumber"] = header["AccountNumber"].astype(str)
    header["CustomerID"] = header["CustomerID"].astype(int)
    # SalesPersonID can have missing values -> use pandas nullable integer
    header["SalesPersonID"] = header["SalesPersonID"].astype('Int64')
    header["Freight"] = pd.to_numeric(header["Freight"], errors="coerce")
    store_tables["sales_order_header"] = header
    write_df_to_sql(header, table_name="store_sales_order_header", conn=conn, if_exists="replace", index=False)

    # Transform sales_order_detail
    detail = raw_tables["sales_order_detail"].copy()
    detail["SalesOrderID"] = detail["SalesOrderID"].astype(int)
    detail["SalesOrderDetailID"] = detail["SalesOrderDetailID"].astype(int)
    detail["OrderQty"] = detail["OrderQty"].astype(int)
    detail["ProductID"] = detail["ProductID"].astype(int)
    detail["UnitPrice"] = pd.to_numeric(detail["UnitPrice"], errors="coerce")
    detail["UnitPriceDiscount"] = pd.to_numeric(detail["UnitPriceDiscount"], errors="coerce")
    store_tables["sales_order_detail"] = detail
    write_df_to_sql(detail, table_name="store_sales_order_detail", conn=conn, if_exists="replace", index=False)

    return store_tables