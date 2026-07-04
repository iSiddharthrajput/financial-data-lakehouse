-- ══════════════════════════════════════════════════════════════════════════════
-- SILVER: News Articles (Cleaned & Deduplicated)
-- Source: bronze.stg_news_articles
-- Purpose: Deduplicate by URL (each article appears once), trim whitespace,
--          filter out records with missing titles.
-- ══════════════════════════════════════════════════════════════════════════════

with ranked as (
    -- If the same article URL appears multiple times (e.g., DAG re-runs
    -- or same article matched to multiple tickers), keep the latest ingestion
    select
        *,
        row_number() over (
            partition by ticker, url
            order by ingested_at desc
        ) as row_num
    from {{ ref('stg_news_articles') }}
)

select
    ticker,
    trim(source) as source,
    trim(title) as title,
    trim(description) as description,
    trim(url) as url,
    published_at,
    ingested_at,

    -- Computed: Extract just the date for easier joining with stock prices
    published_at::date as published_date


from ranked
where row_num = 1              -- Keep only one version per (ticker, url)
  and title is not null        -- Filter out articles with no headline
  and trim(title) != ''        -- Filter out empty string titles
  and title != '[Removed]'     -- Filter out deleted/removed articles
