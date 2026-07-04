-- ══════════════════════════════════════════════════════════════════════════════
-- GOLD: Ticker Performance (Rolling Metrics)
-- Source: silver_stock_prices
-- Purpose: Rolling 7-day and 30-day averages per ticker for comparative
--          analysis across the 12 tracked stocks.
-- Materialized as TABLE for fast dashboard reads.
-- ══════════════════════════════════════════════════════════════════════════════

select
    ticker,
    date,
    close,
    volume,

    -- Rolling 7-day average closing price
    round(avg(close) over (
        partition by ticker
        order by date
        rows between 6 preceding and current row
    ), 4) as avg_close_7d,

    -- Rolling 30-day average closing price
    round(avg(close) over (
        partition by ticker
        order by date
        rows between 29 preceding and current row
    ), 4) as avg_close_30d,

    -- Rolling 7-day total volume
    sum(volume) over (
        partition by ticker
        order by date
        rows between 6 preceding and current row
    ) as total_volume_7d,

    -- Rolling 7-day average daily return
    round(avg(daily_return_pct) over (
        partition by ticker
        order by date
        rows between 6 preceding and current row
    ), 4) as avg_daily_return_7d

from {{ ref('silver_stock_prices') }}
