-- ══════════════════════════════════════════════════════════════════════════════
-- GOLD: News Volume (Media Attention Trends)
-- Source: silver_news_articles
-- Purpose: Daily count of news articles per ticker — tracks media attention
--          trends, useful for correlation with price movements.
-- Materialized as TABLE for fast dashboard reads.
-- ══════════════════════════════════════════════════════════════════════════════

select
    ticker,
    published_date as date,
    count(*) as article_count,

    -- Collect all unique sources that covered this ticker on this day
    listagg(distinct source, ', ') within group (order by source) as sources,

    -- Earliest and latest article timestamps for this ticker on this day
    min(published_at) as earliest_article,
    max(published_at) as latest_article

from {{ ref('silver_news_articles') }}
group by ticker, published_date
