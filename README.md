# 📊 Financial Data Lakehouse

A **production-grade Financial Data Lakehouse** built for real-world data engineering practice — ingesting stock prices and sentiment signals, transforming them through a Medallion Architecture, and surfacing insights via a live dashboard.

---

## 🏗️ Architecture Overview

```
Data Sources                  Ingestion          Storage & Transform              Serving
─────────────                 ─────────          ────────────────────             ───────
yfinance (Stock Prices)  ──►                ──► Snowflake (Data Warehouse)   ──► Streamlit Dashboard
Reddit WSB (Sentiment)   ──►  Apache Airflow ──► dbt Core (Bronze→Silver→Gold)
NewsAPI (News Sentiment) ──►                
                                   │
                               Great Expectations (Data Quality)
                               Elementary (dbt Monitoring)
                               GitHub Actions (CI/CD)
```

## 🛠️ Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow (Docker) |
| Data Warehouse | Snowflake |
| Transformations | dbt Core — Medallion Architecture |
| Data Quality | Great Expectations |
| Monitoring | Elementary |
| Dashboard | Streamlit |
| CI/CD | GitHub Actions |
| Data Sources | yfinance, Reddit WSB, NewsAPI |

## 📈 Tracked Tickers

`NVDA` `AAPL` `MSFT` `GOOGL` `META` `AMZN` `TSLA` `AMD` `PLTR` `SNOW` `NET` `JPM`

## 🗂️ Project Phases

- [x] **Phase 1** — Environment Setup (Docker ✅, Python ✅, Snowflake ✅, dbt ✅, Airflow ✅)
- [/] **Phase 2** — Data Ingestion (yfinance & NewsAPI pipelines ✅, Reddit pending API approval ⏳)
- [x] **Phase 3** — Medallion Transformations (Bronze → Silver → Gold in dbt ✅)
- [x] **Phase 4** — Data Quality (Great Expectations ✅, Elementary pending dbt setup)
- [x] **Phase 5** — Dashboard (Streamlit ✅)
- [x] **Phase 6** — CI/CD (GitHub Actions ✅)

## 🚀 Getting Started

> Setup instructions coming as the project progresses.

---

*Built as a portfolio project demonstrating modern data engineering practices.*
