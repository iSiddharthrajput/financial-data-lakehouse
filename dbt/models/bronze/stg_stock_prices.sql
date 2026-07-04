-- ══════════════════════════════════════════════════════════════════════════════
-- STAGING: Stock Prices
-- Source: BRONZE.RAW_STOCK_PRICES (loaded by Airflow DAG: ingest_stock_prices)
-- Purpose: Thin passthrough that standardizes column names to lowercase
--          and makes the raw table part of the dbt lineage graph.
-- ══════════════════════════════════════════════════════════════════════════════

select
    ticker,
    to_timestamp(date / 1000)::date as date,
    open,
    high,
    low,
    close,
    volume,
    to_timestamp(ingested_at / 1000000) as ingested_at

from {{ source('bronze', 'RAW_STOCK_PRICES') }}
