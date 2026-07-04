-- This test verifies that the primary key combination (ticker, date) is unique
-- in the Gold daily stock summary table.
-- dbt tests fail if the query returns any duplicate rows.
select ticker, date, count(*) as num_records
from {{ ref('gold_daily_stock_summary') }}
group by ticker, date
having count(*) > 1
