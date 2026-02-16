"""Entry point for the Insight2Profit case study solution.

Running this module executes the entire data pipeline: it loads the CSV
files into a SQLite database, applies the required transformations to
produce the publish tables, and answers the two analytical questions.

Usage:

```bash
python main.py
```
"""

from __future__ import annotations

import os
from pathlib import Path
import pandas as pd

from src.database import create_connection
from src.data_loading import load_raw_tables, transform_store_tables
from src.transformations import transform_publish_product, transform_publish_orders
from src.analysis import (
    color_highest_revenue_each_year,
    average_lead_time_by_category,
)


def main() -> None:
    # Determine the project root and data directory
    project_root = Path(__file__).resolve().parent
    data_dir = project_root / "data"
    # Ensure data directory exists
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory {data_dir} does not exist.")

    # Create database engine (SQLite file within the project)
    conn = create_connection(db_path=str(project_root / "insight2profit.db"))

    # 1. Load raw tables
    raw_tables = load_raw_tables(conn=conn, data_dir=data_dir)

    # 2. Transform store tables
    store_tables = transform_store_tables(conn=conn, raw_tables=raw_tables)

    # 3. Publish product master transformations
    publish_product = transform_publish_product(conn=conn, store_tables=store_tables)

    #publish_product.to_csv('publish_product2.csv')

    # 4. Publish orders transformations
    publish_orders = transform_publish_orders(conn=conn, store_tables=store_tables)

    # 5. Analysis questions
    # (a) Which colour generated the highest revenue each year?
    colour_revenue = color_highest_revenue_each_year(publish_orders, publish_product)
    print("\nHighest revenue by colour per year:")
    print(colour_revenue.to_string(index=False))
    colour_revenue.to_csv("output/colour_revenue.csv", index= False)

    # (b) Average lead time by product category
    avg_lead = average_lead_time_by_category(publish_orders, publish_product)
    print("\nAverage lead time (business days) by product category:")
    print(avg_lead.to_string(index=False))
    avg_lead.to_csv("output/average_lead.csv", index= False)


if __name__ == "__main__":
    main()