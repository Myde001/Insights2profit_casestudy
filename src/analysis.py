"""Analytical queries on the publish tables.

This module contains functions that answer the questions posed in the
interview case study using pandas.  The functions accept DataFrames and
return the results as DataFrames, suitable for printing or further
processing.
"""

from __future__ import annotations

import pandas as pd


def color_highest_revenue_each_year(
    publish_orders: pd.DataFrame, publish_product: pd.DataFrame
) -> pd.DataFrame:
    """Determine which colour generated the highest revenue for each year.

    Revenue is defined as the sum of ``TotalLineExtendedPrice`` across all
    orders.  The year is derived from the ``OrderDate`` column on the
    `publish_orders` DataFrame.

    Parameters
    ----------
    publish_orders: pandas.DataFrame
        The joined and transformed orders DataFrame with revenue and lead time.
    publish_product: pandas.DataFrame
        The transformed product master data.

    Returns
    -------
    pandas.DataFrame
        A DataFrame with columns ``year``, ``Color`` and ``revenue`` showing
        the colour that generated the highest revenue in each year.
    """
    # Ensure OrderDate is datetime
    orders = publish_orders.copy()
    orders["OrderDate"] = pd.to_datetime(orders["OrderDate"])
    # Join to get colour information
    merged = orders.merge(
        publish_product[["ProductID", "Color"]], on="ProductID", how="left"
    )
    # Extract year
    merged["year"] = merged["OrderDate"].dt.year
    # Aggregate revenue by year and colour
    revenue_by_color = (
        merged.groupby(["year", "Color"], dropna=False)["TotalLineExtendedPrice"]
        .sum()
        .reset_index(name="revenue")
    )
    # Find the colour with the max revenue per year
    # Use idxmax for each year to pick the row with the largest revenue
    idx = revenue_by_color.groupby("year")["revenue"].idxmax()
    result = revenue_by_color.loc[idx].reset_index(drop=True)
    return result


def average_lead_time_by_category(
    publish_orders: pd.DataFrame, publish_product: pd.DataFrame
) -> pd.DataFrame:
    """Compute the average lead time (business days) by product category.

    Parameters
    ----------
    publish_orders: pandas.DataFrame
        The joined and transformed orders DataFrame with lead time and revenue.
    publish_product: pandas.DataFrame
        The transformed product master data containing the product category.

    Returns
    -------
    pandas.DataFrame
        A DataFrame with columns ``ProductCategoryName`` and
        ``average_lead_time``.
    """
    # Join to get category information
    merged = publish_orders.merge(
        publish_product[["ProductID", "ProductCategoryName"]], on="ProductID", how="left"
    )
    # Group by category and compute mean lead time (ignoring NaN)
    avg_lead = (
        merged.groupby("ProductCategoryName", dropna=False)["LeadTimeInBusinessDays"]
        .mean()
        .reset_index(name="average_lead_time")
    )
    return avg_lead