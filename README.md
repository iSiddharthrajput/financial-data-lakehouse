# 📊 Financial Data Lakehouse

A **production-grade Financial Data Lakehouse** built for real-world data engineering practice — ingesting stock prices and sentiment signals, transforming them through a Medallion Architecture, and surfacing insights via a live dashboard.

---

## 🏗️ Architecture Overview

```
Data Sources                  Ingestion          Storage & Transform              Serving
─────────────                 ─────────          ────────────────────             ───────
yfinance (Stock Prices)  ──►  Apache Airflow ──► Snowflake (Data Warehouse)   ──► Streamlit Dashboard
NewsAPI (News Sentiment) ──►  (Orchestrator) ──► dbt Core (Bronze→Silver→Gold)
                                   │
                               Great Expectations (Data Quality)
                               GitHub Actions (CI/CD)
```

## 🛠️ Tech Stack

| Layer | Tool | Status |
|---|---|---|
| **Orchestration** | Apache Airflow (Docker) | Active |
| **Data Warehouse** | Snowflake | Active |
| **Transformations** | dbt Core — Medallion Architecture | Active |
| **Data Quality** | Great Expectations | Active |
| **Dashboard** | Streamlit | Active |
| **CI/CD** | GitHub Actions | Active |
| **Data Sources** | yfinance, NewsAPI | Active |

## 📈 Tracked Tickers

`NVDA` `AAPL` `MSFT` `GOOGL` `META` `AMZN` `TSLA` `AMD` `PLTR` `SNOW` `NET` `JPM`

## 🗂️ Project Phases

- [x] **Phase 1** — Environment Setup (Docker, Snowflake, dbt, Airflow)
- [x] **Phase 2** — Data Ingestion (yfinance & NewsAPI pipelines)
- [x] **Phase 3** — Medallion Transformations (Bronze → Silver → Gold in dbt)
- [x] **Phase 4** — Data Quality (Great Expectations)
- [x] **Phase 5** — Dashboard (Streamlit)
- [x] **Phase 6** — CI/CD (GitHub Actions)
- [x] **Phase 7** — End-to-End Hardening & Testing (Pytest, parameter validation, dbt singular tests, master orchestration)

## 🔮 Future Roadmap & Extensions

- [ ] **Reddit Ingestion**: Add Reddit API scraper (via `praw`) to ingest WallStreetBets sentiment feeds (pending developer API key approval).
- [ ] **Observability**: Integrate **Elementary** dbt monitoring package to generate data lineage, schema drift anomalies, and test coverage dashboards.
- [ ] **Container Hardening**: Transition container dependencies from run-time pip installation (`_PIP_ADDITIONAL_REQUIREMENTS`) to pre-baked Custom Docker Images.

## 🚀 Getting Started

### 1. Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Snowflake Trial/Account

### 2. Local Setup
1. Clone the repository and configure your `.env` file from the placeholder variables:
   ```bash
   cp .env.example .env  # Update variables with your Snowflake & NewsAPI keys
   ```
2. Start the Airflow database and services:
   ```bash
   docker compose up -d --build
   ```
3. Run local unit tests (DAG smoke check):
   ```bash
   pip install -r requirements.txt
   pytest tests/
   ```
4. Run dbt locally or from the orchestrator:
   ```bash
   cd dbt
   dbt run --profiles-dir ../airflow/dbt-profile
   ```
5. Launch the Streamlit dashboard:
   ```bash
   streamlit run dashboard/app.py
   ```

---

*Built as a portfolio project demonstrating modern, secure, and production-grade data engineering practices.*
