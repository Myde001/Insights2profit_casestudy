# Insight2Profit Case Study Solution

This repository contains a Python solution for the **Insight2Profit interview case study**.  
The goal of the exercise is to load and transform three data sets (products, sales order headers, and sales order details), perform several data transformations and calculations, and answer a couple of analytical questions.

## Project layout

```
insight2profit_case_study/
├── data/
│   ├── products.csv                # original product data (provided)
│   ├── sales_order_header.csv      # original sales order headers (provided)
│   └── sales_order_detail.csv      # original sales order details (provided)
├── src/
│   ├── __init__.py
│   ├── database.py                 # helper functions for working with SQLite
│   ├── data_loading.py             # functions for loading raw data and storing it with a `store_` prefix
│   ├── transformations.py          # functions for transforming product and order data into publish tables
│   ├── analysis.py                 # functions that compute the answers to the analytical questions
│   └── utils.py                    # utility functions (e.g. business‑day calculations)
├── main.py                         # entry point that orchestrates the workflow
└── requirements.txt                # list of Python dependencies
```

## How to run

1. **Install the dependencies** (ideally in a virtual environment):

   ```bash
   pip install -r requirements.txt
   ```

2. **Run the pipeline**:

   ```bash
   python main.py
   ```

   This will:

   * Load the raw CSV files into pandas DataFrames and store them in a SQLite database with a `raw_` prefix.
   * Coerce the columns to more appropriate data types and save these into tables prefixed with `store_`.
   * Perform the required transformations on the product master data (`publish_product`) and the joined order data (`publish_orders`).
   * Answer the analytical questions and print the results to the console.

## Analytical questions answered

1. **Which colour generated the highest revenue each year?**  
   The solution aggregates revenue (`TotalLineExtendedPrice`) by year and colour, then selects the colour with the maximum revenue for each year.

2. **What is the average lead time in business days by product category?**  
   The solution calculates the business‑day difference between the order date and ship date for each order line and then reports the average by product category.

## Notes

* The provided `OrderDate` column contains only year–month values (e.g. `2021-06`).  
  The code interprets these values as the first day of the month (e.g. `2021-06-01`) in order to compute a lead time.
* Business days exclude Saturdays and Sundays.  
  The calculation uses numpy’s `busday_count` to count the number of weekdays between two dates.

Feel free to inspect the individual modules in the `src` directory for implementation details.