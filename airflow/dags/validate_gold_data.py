from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import great_expectations as gx
import sys

from sqlalchemy.engine import URL

# ── 1. DAG Setup ─────────────────────────────────────────────────────────────
default_args = {
    'owner': 'Siddharth',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── 2. Validation Function ───────────────────────────────────────────────────
def run_great_expectations_validation():
    print("🚀 Fetching Snowflake connection from Airflow Vault...")
    # Retrieve the connection URI dynamically from Airflow's connection manager
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Airflow hook.get_uri() creates a SQLAlchemy URI, but it might use 'snowflake://'
    # and default to the default schema. Let's force the schema to BRONZE_GOLD.
    conn = hook.get_connection('snowflake_conn')
    
    sf_user = conn.login
    sf_password = conn.password
    sf_account = conn.extra_dejson.get('account')
    sf_database = conn.extra_dejson.get('database', 'FINANCIAL_DB')
    sf_warehouse = conn.extra_dejson.get('warehouse', 'FINANCIAL_WH')
    sf_role = conn.extra_dejson.get('role', 'DBT_ROLE')
    sf_schema = 'BRONZE_GOLD'  # Target the dbt Gold schema
    
    connection_url = URL.create(
        drivername="snowflake",
        username=sf_user,
        password=sf_password,
        host=sf_account,
        database=f"{sf_database}/{sf_schema}",
        query={"warehouse": sf_warehouse, "role": sf_role}
    )

    print("🚀 Initializing Great Expectations Ephemeral Context...")
    context = gx.get_context()

    print("🔌 Connecting Snowflake to Great Expectations...")
    # Connect Snowflake using add_snowflake safely to avoid leaking credentials
    try:
        datasource = context.data_sources.add_snowflake(
            name="snowflake_gold",
            connection_string=connection_url.render_as_string(hide_password=False)
        )
    except Exception as e:
        raise RuntimeError(
            "❌ Failed to connect Great Expectations to Snowflake. "
            "Please check your Snowflake connection credentials and network access."
        ) from None

    # Reference the Gold daily stock summary table
    asset_name = "daily_stock_summary_asset"
    table_name = "GOLD_DAILY_STOCK_SUMMARY"
    
    try:
        data_asset = datasource.add_table_asset(name=asset_name, table_name=table_name)
    except Exception:
        data_asset = datasource.get_asset(asset_name)

    batch_request = data_asset.build_batch_request()

    print("📐 Creating Expectation Suite...")
    suite_name = "stock_data_quality_suite"
    try:
        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    except Exception:
        suite = context.suites.get(suite_name)

    print("✍️ Defining Validation Rules...")
    # 1. Non-null columns check
    for col in ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'TICKER']:
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=col))

    # 2. Volume must be positive
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column='VOLUME', 
        min_value=0
    ))

    # 3. Daily return bounds (-50% to +50%)
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column='DAILY_RETURN_PCT',
        min_value=-50.0,
        max_value=50.0
    ))

    # 4. Logical check: High >= Low
    suite.add_expectation(gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A='HIGH',
        column_B='LOW',
        or_equal=True
    ))

    # 5. Logical check: High >= Close
    suite.add_expectation(gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A='HIGH',
        column_B='CLOSE',
        or_equal=True
    ))

    print("🏃 Running Validation...")
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite
    )
    
    results = validator.validate()

    print("\n===========================================")
    print("📊 DATA QUALITY REPORT SUMMARY")
    print("===========================================")
    print(f"Suite: {results.meta.get('expectation_suite_name')}")
    print(f"Status: {'✅ PASSED' if results.success else '❌ FAILED'}")
    print(f"Total Rules Checked: {results.statistics.get('evaluated_expectations')}")
    print(f"Passed Rules: {results.statistics.get('successful_expectations')}")
    print(f"Failed Rules: {results.statistics.get('unsuccessful_expectations')}")
    print(f"Success Rate: {results.statistics.get('success_percent'):.2f}%")
    print("===========================================\n")

    if not results.success:
        print("🚨 Detail of Failed Expectations:")
        for r in results.results:
            if not r.success:
                print(f"  - {r.expectation_config.expectation_type} on {r.expectation_config.kwargs}")
        # Raising an exception here will mark the Airflow task as FAILED!
        raise ValueError("❌ Great Expectations validation failed! Data quality issues detected.")
    else:
        print("🎉 All data quality checks passed successfully!")

# ── 3. Define the DAG Graph ───────────────────────────────────────────────────
with DAG(
    'validate_gold_data',
    default_args=default_args,
    description='Run Great Expectations data quality checks on Snowflake GOLD tables',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    run_validation_task = PythonOperator(
        task_id='run_great_expectations_validation',
        python_callable=run_great_expectations_validation,
    )
