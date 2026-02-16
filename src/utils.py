"""Utility functions for the case study solution.

This module provides helper functions such as conversion of partial dates
to proper timestamps and computation of business‑day differences.  These
functions are used throughout the pipeline.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Iterable, Optional


def business_days_between(start: pd.Series, end: pd.Series) -> pd.Series:
    """Calculate the number of business days between two date series.

    This function counts the number of weekdays (Monday through Friday) between
    the dates in ``start`` (inclusive) and ``end`` (exclusive) on a row‑by‑row
    basis.  Missing values in either the start or end date produce
    ``NaN`` in the result.

    Parameters
    ----------
    start: pandas.Series
        A series of start dates.
    end: pandas.Series
        A series of end dates.

    Returns
    -------
    pandas.Series
        A series of floats where each element is the number of business days
        between the corresponding elements of ``start`` and ``end``.  Missing
        values propagate to missing results.
    """
    # Convert to datetime
    start_dates = pd.to_datetime(start)
    end_dates = pd.to_datetime(end)

    # Compute row by row to gracefully handle NaT
    results = []
    for s, e in zip(start_dates, end_dates):
        if pd.isna(s) or pd.isna(e):
            results.append(np.nan)
        else:
            # Convert to date (datetime64[D]) for busday_count
            s_date = s.date()
            e_date = e.date()
            results.append(np.busday_count(s_date, e_date))
    return pd.Series(results, index=start.index, dtype=float)