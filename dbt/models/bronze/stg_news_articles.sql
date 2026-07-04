-- ══════════════════════════════════════════════════════════════════════════════
-- STAGING: News Articles
-- Source: BRONZE.RAW_NEWS_ARTICLES (loaded by Airflow DAG: ingest_news_sentiment)
-- Purpose: Thin passthrough that standardizes column names to lowercase
--          and makes the raw table part of the dbt lineage graph.
-- ══════════════════════════════════════════════════════════════════════════════

select
    ticker,
    source,
    title,
    description,
    url,
    to_timestamp(published_at / 1000000) as published_at,
    to_timestamp(ingested_at / 1000000) as ingested_at

from {{ source('bronze', 'RAW_NEWS_ARTICLES') }}
