from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ── 1. DAG Setup ─────────────────────────────────────────────────────────────
default_args = {
    'owner': 'Siddharth',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'lakehouse_pipeline',
    default_args=default_args,
    description='Master pipeline: ingest data, run dbt runs/tests, and execute Great Expectations validation',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # ── 2. Ingestion Tasks (Parallel) ──────────────────────────────────────────
    trigger_stocks = TriggerDagRunOperator(
        task_id='trigger_ingest_stock_prices',
        trigger_dag_id='ingest_stock_prices',
        wait_for_completion=True,
        poke_interval=15,
    )

    trigger_news = TriggerDagRunOperator(
        task_id='trigger_ingest_news_sentiment',
        trigger_dag_id='ingest_news_sentiment',
        wait_for_completion=True,
        poke_interval=15,
    )

    # ── 3. dbt Transformation & Testing (Sequential) ───────────────────────────
    # Run all dbt models in Snowflake (Bronze -> Silver -> Gold)
    dbt_run = BashOperator(
        task_id='dbt_run_transformations',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/.dbt',
    )

    # Run dbt test to verify schema constraints and relationships
    dbt_test = BashOperator(
        task_id='dbt_test_assertions',
        bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/.dbt',
    )

    # ── 4. Great Expectations Quality Checks ──────────────────────────────────
    trigger_validation = TriggerDagRunOperator(
        task_id='trigger_validate_gold_data',
        trigger_dag_id='validate_gold_data',
        wait_for_completion=True,
        poke_interval=15,
    )

    # Chaining the tasks: Ingestion -> Transform -> dbt Test -> Great Expectations
    [trigger_stocks, trigger_news] >> dbt_run >> dbt_test >> trigger_validation
