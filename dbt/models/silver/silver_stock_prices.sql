-- ══════════════════════════════════════════════════════════════════════════════
-- SILVER: Stock Prices (Cleaned & Deduplicated)
-- Source: bronze.stg_stock_prices
-- Purpose: Deduplicate by (ticker, date), compute daily return & price range,
--          filter out bad records. Safe to query directly.
-- ══════════════════════════════════════════════════════════════════════════════

with ranked as (
    -- If the same ticker+date appears multiple times (e.g., DAG re-runs),
    -- keep only the LATEST ingested row
    select
        *,
        row_number() over (
            partition by ticker, date
            order by ingested_at desc
        ) as row_num
    from {{ ref('stg_stock_prices') }}
)

select
    ticker,
    date,
    open,
    high,
    low,
    close,
    volume,

    -- Computed: How much the price moved from open to close (percentage)
    round(((close - open) / nullif(open, 0)) * 100, 4) as daily_return_pct,

    -- Computed: Intraday price range (volatility indicator)
    round(high - low, 4) as price_range,

    ingested_at

from ranked
where row_num = 1          -- Keep only the latest version of each (ticker, date)
  and volume > 0           -- Filter out days with no trading activity
  and close is not null     -- Filter out incomplete records
