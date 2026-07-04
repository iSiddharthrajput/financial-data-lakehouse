from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta, timezone
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import requests
import os

# ── 1. DAG Setup ─────────────────────────────────────────────────────────────
default_args = {
    'owner': 'Siddharth',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── 2. Data Extraction & Load Function ─────────────────────────────────────────
def fetch_and_load_news():
    api_key = os.environ.get('NEWSAPI_KEY')
    if not api_key:
        raise ValueError("❌ NEWSAPI_KEY not found in environment variables!")

    tickers = ['NVDA', 'AAPL', 'MSFT', 'GOOGL', 'META', 'AMZN', 'TSLA', 'AMD', 'PLTR', 'SNOW', 'NET', 'JPM']

    # Map tickers to company names for better search results
    ticker_to_company = {
        'NVDA': 'NVIDIA',
        'AAPL': 'Apple',
        'MSFT': 'Microsoft',
        'GOOGL': 'Google OR Alphabet',
        'META': 'Meta Platforms',
        'AMZN': 'Amazon',
        'TSLA': 'Tesla',
        'AMD': 'AMD',
        'PLTR': 'Palantir',
        'SNOW': 'Snowflake',
        'NET': 'Cloudflare',
        'JPM': 'JPMorgan',
    }

    all_articles = []
    failed_tickers = []

    print(f"Starting news extraction for tickers: {tickers}")
    for ticker in tickers:
        company = ticker_to_company[ticker]
        # Search for articles mentioning the company name + "stock"
        query = f'"{company}" AND stock'
        print(f"Fetching news for {ticker} ({company})...")

        url = 'https://newsapi.org/v2/everything'
        params = {
            'q': query,
            'language': 'en',
            'sortBy': 'publishedAt',
            'pageSize': 20,          # Top 20 articles per ticker
            'apiKey': api_key,
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            # Scrub the NewsAPI API key if it's printed in the exception traceback URL
            safe_error_msg = str(e).replace(api_key, "***REDACTED***")
            print(f"⚠️ NewsAPI request failed for {ticker}: {safe_error_msg}")
            failed_tickers.append(ticker)
            continue

        if data.get('status') != 'ok':
            error_msg = str(data.get('message', 'unknown')).replace(api_key, "***REDACTED***")
            print(f"⚠️ NewsAPI error for {ticker}: {error_msg}")
            failed_tickers.append(ticker)
            continue

        articles = data.get('articles', [])
        print(f"  → Got {len(articles)} articles for {ticker}")

        for article in articles:
            all_articles.append({
                'TICKER': ticker,
                'SOURCE': article.get('source', {}).get('name', 'Unknown'),
                'TITLE': article.get('title', ''),
                'DESCRIPTION': article.get('description', ''),
                'URL': article.get('url', ''),
                'PUBLISHED_AT': article.get('publishedAt', ''),
                'INGESTED_AT': datetime.now(timezone.utc).replace(tzinfo=None),
            })

    if failed_tickers:
        print(f"⚠️ Completed news sentiment ingestion with partial failures for: {failed_tickers}")

    if not all_articles:
        raise ValueError("❌ No news articles were successfully fetched for any ticker!")

    # Build a DataFrame from all collected articles
    final_df = pd.DataFrame(all_articles)

    # Parse the ISO 8601 timestamp from NewsAPI into a proper datetime
    final_df['PUBLISHED_AT'] = pd.to_datetime(final_df['PUBLISHED_AT'], utc=True).dt.tz_localize(None)

    print(f"Successfully compiled {len(final_df)} total news articles.")

    # Connect to Snowflake using the secure Connection we created in Airflow
    print("Connecting to Snowflake...")
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()

    try:
        # Write DataFrame directly to Snowflake BRONZE schema
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=final_df,
            table_name='RAW_NEWS_ARTICLES',
            database='FINANCIAL_DB',
            schema='BRONZE',
            auto_create_table=True,
        )
        print(f"✅ Write status: {success}, written {nrows} rows in {nchunks} chunks.")
    finally:
        conn.close()

# ── 3. Define the DAG Graph ───────────────────────────────────────────────────
with DAG(
    'ingest_news_sentiment',
    default_args=default_args,
    description='Fetch financial news headlines from NewsAPI and load into Snowflake BRONZE',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_and_load_news',
        python_callable=fetch_and_load_news,
    )
