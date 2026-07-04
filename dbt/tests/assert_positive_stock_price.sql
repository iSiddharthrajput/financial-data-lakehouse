-- This test verifies that all closing prices in our Gold layer are positive numbers.
-- dbt tests fail if the query returns any rows.
select ticker, date, close
from {{ ref('gold_daily_stock_summary') }}
where close <= 0
