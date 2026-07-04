from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta, timezone
import yfinance as yf
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

# ── 1. DAG Setup ─────────────────────────────────────────────────────────────
default_args = {
    'owner': 'Siddharth',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 25), # Starts a few days ago
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── 2. Data Extraction & Load Function ─────────────────────────────────────────
def fetch_and_load_stocks():
    tickers = ['NVDA', 'AAPL', 'MSFT', 'GOOGL', 'META', 'AMZN', 'TSLA', 'AMD', 'PLTR', 'SNOW', 'NET', 'JPM']
    all_data = []
    failed_tickers = []
    
    print(f"Starting extraction for tickers: {tickers}")
    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        try:
            ticker_obj = yf.Ticker(ticker)
            # Fetch last 30 days of data for initial load/backfill with a 30 second timeout
            df = ticker_obj.history(period='30d', timeout=30)
        except Exception as e:
            print(f"⚠️ Failed to fetch data for {ticker}: {e}")
            failed_tickers.append(ticker)
            continue
            
        if df.empty:
            print(f"⚠️ No data returned for {ticker}")
            failed_tickers.append(ticker)
            continue
            
        # Format the DataFrame index (Date) into a column
        df = df.reset_index()
        df['TICKER'] = ticker
        
        # Select and rename columns to match Snowflake case conventions (uppercase)
        df = df[['Date', 'TICKER', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df.columns = ['DATE', 'TICKER', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
        
        # Enforce exact data types
        df['DATE'] = pd.to_datetime(df['DATE']).dt.tz_localize(None)
        df['OPEN'] = df['OPEN'].astype(float)
        df['HIGH'] = df['HIGH'].astype(float)
        df['LOW'] = df['LOW'].astype(float)
        df['CLOSE'] = df['CLOSE'].astype(float)
        df['VOLUME'] = df['VOLUME'].astype(int)
        df['INGESTED_AT'] = datetime.now(timezone.utc).replace(tzinfo=None)
        
        all_data.append(df)
        
    if failed_tickers:
        print(f"⚠️ Completed stock price ingestion with partial failures for: {failed_tickers}")

    if not all_data:
        raise ValueError("❌ No stock data was successfully fetched for any ticker!")
        
    # Combine all individual ticker DataFrames into one large DataFrame
    final_df = pd.concat(all_data, ignore_index=True)
    print(f"Successfully compiled {len(final_df)} total rows of stock data.")
    
    # Connect to Snowflake using the secure Connection we created in Airflow
    print("Connecting to Snowflake...")
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    
    try:
        # Write pandas DataFrame directly to Snowflake BRONZE schema
        # auto_create_table=True will automatically compile the schema and build the table
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=final_df,
            table_name='RAW_STOCK_PRICES',
            database='FINANCIAL_DB',
            schema='BRONZE',
            auto_create_table=True
        )
        print(f"✅ Write status: {success}, written {nrows} rows in {nchunks} chunks.")
    finally:
        # Always close connection to avoid leakage
        conn.close()

# ── 3. Define the DAG Graph ───────────────────────────────────────────────────
with DAG(
    'ingest_stock_prices',
    default_args=default_args,
    description='Fetch daily stock prices from yfinance and load into Snowflake BRONZE',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Run the Python function as our ingestion task
    fetch_task = PythonOperator(
        task_id='fetch_and_load_stocks',
        python_callable=fetch_and_load_stocks,
    )
