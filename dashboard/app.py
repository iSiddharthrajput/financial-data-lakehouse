import streamlit as st
import pandas as pd
import os
import html
import snowflake.connector
from dotenv import load_dotenv

# Set page config for a premium, wide dashboard look
st.set_page_config(
    page_title="Financial Data Lakehouse Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load environment variables
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(project_root, ".env"))

# ── 1. Helper Function: Snowflake Connection ─────────────────────────────────
@st.cache_resource
def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.environ.get('SNOWFLAKE_ACCOUNT'),
        user=os.environ.get('SNOWFLAKE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        role=os.environ.get('SNOWFLAKE_ROLE'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
        database=os.environ.get('SNOWFLAKE_DATABASE'),
        schema='BRONZE_GOLD'  # Connect to the Gold schema
    )

# ── 2. Helper Function: Fetch Data ───────────────────────────────────────────
@st.cache_data(ttl=600)  # Cache data for 10 minutes to avoid credit burn
def fetch_data(query, params=None):
    conn = get_snowflake_connection()
    # Read the query into a pandas DataFrame using safe query parameters
    df = pd.read_sql(query, conn, params=params)
    # Convert column names to uppercase for consistency
    df.columns = [col.upper() for col in df.columns]
    return df

# ── 3. Page Header ───────────────────────────────────────────────────────────
st.title("📊 Financial Data Lakehouse Dashboard")
st.markdown("""
Welcome to the live performance dashboard. This dashboard queries the **GOLD** layer of our Medallion Data Lakehouse in Snowflake, 
combining daily stock price metrics from `yfinance` with sentiment signal volume from `NewsAPI`.
""")

# ── 4. Sidebar Controls ──────────────────────────────────────────────────────
st.sidebar.header("🛠️ Controls")

# Fetch available tickers
try:
    tickers_df = fetch_data("SELECT DISTINCT ticker FROM gold_daily_stock_summary ORDER BY ticker;")
    available_tickers = tickers_df['TICKER'].tolist()
except Exception as e:
    st.sidebar.error(f"Failed to connect to Snowflake: {e}")
    available_tickers = ['NVDA', 'AAPL', 'MSFT', 'GOOGL', 'META', 'AMZN', 'TSLA', 'AMD', 'PLTR', 'SNOW', 'NET', 'JPM']

selected_ticker = st.sidebar.selectbox(
    "Select Stock Ticker",
    options=available_tickers,
    index=0
)

# Defense in depth: validate that the selected ticker exists in our whitelist
if selected_ticker not in available_tickers:
    st.error("Invalid ticker selection.")
    st.stop()

st.sidebar.markdown("---")
st.sidebar.info("""
**Data Pipeline Info:**
- **Orchestration**: Apache Airflow
- **Transformations**: dbt Core (Bronze → Silver → Gold)
- **Data Quality**: Great Expectations
- **Database**: Snowflake (Azure)
""")

# ── 5. Main Dashboard Queries ────────────────────────────────────────────────
# Fetch daily summary for the selected ticker using parameter binding
daily_summary = fetch_data("""
    SELECT date, open, high, low, close, volume, daily_return_pct, price_range, news_article_count
    FROM gold_daily_stock_summary
    WHERE ticker = %(ticker)s
    ORDER BY date DESC;
""", params={"ticker": selected_ticker})

# Fetch rolling performance statistics using parameter binding
performance_stats = fetch_data("""
    SELECT date, close, avg_close_7d, avg_close_30d, avg_daily_return_7d
    FROM gold_ticker_performance
    WHERE ticker = %(ticker)s
    ORDER BY date DESC;
""", params={"ticker": selected_ticker})

# Fetch news volume timeline using parameter binding
news_volume = fetch_data("""
    SELECT date, article_count, sources
    FROM gold_news_volume
    WHERE ticker = %(ticker)s
    ORDER BY date DESC;
""", params={"ticker": selected_ticker})

# Fetch actual raw news articles for context from the Silver layer using parameter binding
raw_news_articles = fetch_data("""
    SELECT published_date, source, title, description, url
    FROM BRONZE_SILVER.silver_news_articles
    WHERE ticker = %(ticker)s
    ORDER BY published_date DESC
    LIMIT 10;
""", params={"ticker": selected_ticker})

# ── 6. Metrics Row ───────────────────────────────────────────────────────────
if not daily_summary.empty:
    latest_row = daily_summary.iloc[0]
    
    # Calculate previous day's metrics for delta
    prev_close = daily_summary.iloc[1]['CLOSE'] if len(daily_summary) > 1 else latest_row['CLOSE']
    price_delta = latest_row['CLOSE'] - prev_close
    percent_delta = (price_delta / prev_close) * 100

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            label="Latest Close Price", 
            value=f"${latest_row['CLOSE']:.2f}",
            delta=f"${price_delta:+.2f} ({percent_delta:+.2f}%)"
        )
    with col2:
        st.metric(
            label="Trading Volume", 
            value=f"{latest_row['VOLUME']:,}"
        )
    with col3:
        st.metric(
            label="Intraday Volatility (High-Low)", 
            value=f"${latest_row['PRICE_RANGE']:.2f}"
        )
    with col4:
        st.metric(
            label="Today's News Mention Count", 
            value=int(latest_row['NEWS_ARTICLE_COUNT']),
            delta="articles"
        )
else:
    st.warning(f"No summary data found for {selected_ticker}.")

st.markdown("---")

# ── 7. Charts Section ────────────────────────────────────────────────────────
tab1, tab2 = st.tabs(["📈 Stock Price Trends", "📰 Media Attention vs. Price"])

with tab1:
    st.subheader(f"{selected_ticker} Close Price & Moving Averages")
    if not performance_stats.empty:
        # Format date column for charts
        performance_stats['DATE'] = pd.to_datetime(performance_stats['DATE'])
        
        # Prepare data for plotting
        chart_data = performance_stats.set_index('DATE')[['CLOSE', 'AVG_CLOSE_7D', 'AVG_CLOSE_30D']]
        # Sort chronologically for charting
        chart_data = chart_data.sort_index()
        
        # Plot using Streamlit's built-in line chart
        st.line_chart(chart_data)
    else:
        st.warning("No rolling performance statistics found.")

with tab2:
    st.subheader("Price Movement Correlation with News Volume")
    if not daily_summary.empty:
        daily_summary['DATE'] = pd.to_datetime(daily_summary['DATE'])
        summary_sorted = daily_summary.sort_values('DATE')
        
        # Side-by-side comparison
        col_c1, col_c2 = st.columns(2)
        with col_c1:
            st.markdown("**Stock Close Price**")
            st.line_chart(summary_sorted.set_index('DATE')['CLOSE'])
        with col_c2:
            st.markdown("**News Article Count**")
            st.bar_chart(summary_sorted.set_index('DATE')['NEWS_ARTICLE_COUNT'])
    else:
        st.warning("No summary correlation data found.")

st.markdown("---")

# ── 8. News Articles Section ────────────────────────────────────────────────
st.subheader(f"📰 Latest News Coverage for {selected_ticker}")

if not raw_news_articles.empty:
    for idx, row in raw_news_articles.iterrows():
        # Clean formatting and HTML-escape raw text from third party API to prevent XSS
        date_str = pd.to_datetime(row['PUBLISHED_DATE']).strftime('%b %d, %Y')
        title_str = html.escape(str(row['TITLE']))
        source_str = html.escape(str(row['SOURCE']))
        desc_str = html.escape(str(row['DESCRIPTION']))
        url_str = html.escape(str(row['URL']))
        
        # Renders a neat list of clickable news widgets
        with st.container():
            st.markdown(f"### [{title_str}]({url_str})")
            st.markdown(f"**Source:** *{source_str}* | **Published:** *{date_str}*")
            st.markdown(desc_str)
            st.markdown("---")
else:
    st.info(f"No recent news articles logged for {selected_ticker}.")
