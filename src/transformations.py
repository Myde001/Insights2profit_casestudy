"""Transformations to create publish tables from the store tables.

This module implements the logic described in the case study for the
``publish_product`` and ``publish_orders`` tables.
"""

from __future__ import annotations

import pandas as pd
from typing import Dict

from .database import write_df_to_sql
import sqlite3
from .utils import business_days_between


def transform_publish_product(conn: sqlite3.Connection, store_tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Perform product master transformations and write to `publish_product`.

    The function replaces missing colours with "N/A" and fills missing
    categories based on the product subcategory according to the specified
    mapping rules.  The result is written to the SQL database.

    Parameters
    ----------
    engine: sqlalchemy.engine.Engine
        Database engine to which the publish table will be written.
    store_tables: dict[str, pandas.DataFrame]
        The transformed tables returned by :func:`data_loading.transform_store_tables`.

    Returns
    -------
    pandas.DataFrame
        The publish_product DataFrame.
    """
    products = store_tables["products"].copy()
    # Replace NULL/NaN colours with "N/A"
    products["Color"] = products["Color"].fillna("N/A")
    # Fill missing ProductCategoryName based on ProductSubCategoryName
    def derive_category(row):
        """Derive a product category from subcategory when the category is missing.

        The initial mapping follows the case study instructions: certain
        subcategories are mapped directly to ``Clothing``, ``Accessories`` or
        ``Components``.  If a subcategory is not covered by those rules but a
        category is still missing, we perform a further aggregation based on
        domain knowledge.  This additional mapping assigns remaining
        subcategories to the appropriate high‑level category (e.g. frames
        components or apparel).  If the subcategory is still unrecognised,
        ``NaN`` will be left unchanged.
        """
        # If the category is already populated, use it
        if pd.notna(row["ProductCategoryName"]):
            return row["ProductCategoryName"]
        # Normalise subcategory to a string for comparisons
        sub = str(row["ProductSubCategoryName"]) if pd.notna(row["ProductSubCategoryName"]) else ""
        # Case‑study defined mappings
        clothing = {"Gloves", "Shorts", "Socks", "Tights", "Vests"}
        accessories = {"Locks", "Lights", "Headsets", "Helmets", "Pedals", "Pumps"}
        components_exact = {"Wheels", "Saddles"}
        if sub in clothing:
            return "Clothing"
        if sub in accessories:
            return "Accessories"
        if "Frames" in sub or sub in components_exact:
            return "Components"
        # Additional aggregations for remaining subcategories
        additional_mapping = {
            # Clothing
            "Socks": "Clothing",
            "Caps": "Clothing",
            "Jerseys": "Clothing",
            "Shorts": "Clothing",
            "Tights": "Clothing",
            "Bib-Shorts": "Clothing",
            "Gloves": "Clothing",
            "Vests": "Clothing",
            # Accessories
            "Helmets": "Accessories",
            "Headsets": "Accessories",
            "Panniers": "Accessories",
            "Locks": "Accessories",
            "Pumps": "Accessories",
            "Lights": "Accessories",
            "Bottles and Cages": "Accessories",
            "Bike Racks": "Accessories",
            "Cleaners": "Accessories",
            "Bike Stands": "Accessories",
            "Hydration Packs": "Accessories",
            "Pedals": "Accessories",
            "Fenders": "Accessories",
            # Components
            "Road Frames": "Components",
            "Mountain Frames": "Components",
            "Touring Frames": "Components",
            "Forks": "Components",
            "Derailleurs": "Components",
            "Brakes": "Components",
            "Saddles": "Components",
            "Cranksets": "Components",
            "Chains": "Components",
            "Bottom Brackets": "Components",
            "Tires and Tubes": "Components",
            # The original 'Wheels' and 'Saddles' are handled above
        }
        if sub in additional_mapping:
            return additional_mapping[sub]
        # If nothing matches, leave as NaN
        return row["ProductCategoryName"]

    products["ProductCategoryName"] = products.apply(derive_category, axis=1)
    # Write to publish_product
    write_df_to_sql(products, table_name="publish_product", conn=conn, if_exists="replace", index=False)
    return products


def transform_publish_orders(conn: sqlite3.Connection, store_tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Join sales order detail and header, calculate lead time and total price.

    The resulting table contains all columns from detail, all columns from
    header except ``SalesOrderID`` (renamed ``Freight`` to ``TotalOrderFreight``),
    plus ``LeadTimeInBusinessDays`` and ``TotalLineExtendedPrice``.  The
    result is written to the SQL database.

    Parameters
    ----------
    engine: sqlalchemy.engine.Engine
        Database engine to which the publish table will be written.
    store_tables: dict[str, pandas.DataFrame]
        The transformed tables returned by :func:`data_loading.transform_store_tables`.

    Returns
    -------
    pandas.DataFrame
        The publish_orders DataFrame.
    """
    detail = store_tables["sales_order_detail"]
    header = store_tables["sales_order_header"]
    # Join on SalesOrderID
    merged = detail.merge(header, on="SalesOrderID", how="left", suffixes=("", "_header"))
    # Calculate lead time in business days
    merged["LeadTimeInBusinessDays"] = business_days_between(merged["OrderDate"], merged["ShipDate"])
    # Total line extended price
    merged["TotalLineExtendedPrice"] = merged["OrderQty"] * (merged["UnitPrice"] - merged["UnitPriceDiscount"])
    # Rename Freight to TotalOrderFreight
    if "Freight" in merged.columns:
        merged = merged.rename(columns={"Freight": "TotalOrderFreight"})
    # We should drop the duplicate SalesOrderID from header? Actually SalesOrderID from header is duplicate; keep one
    # All columns from detail are kept; All columns from header except SalesOrderID (since we still have SalesOrderID from detail) -> drop the duplicate column if exists.
    # Our merge used same name; we will drop column 'SalesOrderID_header' if present.
    # Check for duplicates
    if "SalesOrderID_header" in merged.columns:
        merged = merged.drop(columns=["SalesOrderID_header"])
    # Write to publish_orders
    write_df_to_sql(merged, table_name="publish_orders", conn=conn, if_exists="replace", index=False)
    return merged