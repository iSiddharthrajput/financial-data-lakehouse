-- ══════════════════════════════════════════════════════════════════════════════
-- GOLD: Daily Stock Summary
-- Sources: silver_stock_prices + silver_news_articles
-- Purpose: One row per ticker per day with all price metrics + news count.
--          This is the primary table the Streamlit dashboard will query.
-- Materialized as TABLE for fast dashboard reads.
-- ══════════════════════════════════════════════════════════════════════════════

with stock_data as (
    select * from {{ ref('silver_stock_prices') }}
),

news_counts as (
    -- Count how many news articles were published per ticker per day
    select
        ticker,
        published_date,
        count(*) as news_article_count
    from {{ ref('silver_news_articles') }}
    group by ticker, published_date
)

select
    s.ticker,
    s.date,
    s.open,
    s.high,
    s.low,
    s.close,
    s.volume,
    s.daily_return_pct,
    s.price_range,

    -- Enrichment: How many news articles appeared for this ticker on this day
    coalesce(n.news_article_count, 0) as news_article_count,

    s.ingested_at

from stock_data s
left join news_counts n
    on s.ticker = n.ticker
    and s.date = n.published_date

