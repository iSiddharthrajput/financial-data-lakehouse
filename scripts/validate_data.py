import os
import sys
from dotenv import load_dotenv
import great_expectations as gx

from sqlalchemy.engine import URL

# Load project environment variables
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(project_root, ".env"))

def run_validation():
    print("🚀 Initializing Great Expectations Context...")
    # Get an ephemeral context (in-memory context, no complex setup needed!)
    context = gx.get_context()

    # Validate that all required environment variables are set
    required_vars = [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_ROLE', 'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE'
    ]
    missing = [v for v in required_vars if not os.environ.get(v)]
    if missing:
        raise EnvironmentError(f"❌ Missing required environment variables: {', '.join(missing)}")

    # Build connection string from env
    sf_account = os.environ.get('SNOWFLAKE_ACCOUNT')
    sf_user = os.environ.get('SNOWFLAKE_USER')
    sf_password = os.environ.get('SNOWFLAKE_PASSWORD')
    sf_role = os.environ.get('SNOWFLAKE_ROLE')
    sf_warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
    sf_database = os.environ.get('SNOWFLAKE_DATABASE')
    sf_schema = 'BRONZE_GOLD'  # The actual schema created by dbt

    connection_url = URL.create(
        drivername="snowflake",
        username=sf_user,
        password=sf_password,
        host=sf_account,
        database=f"{sf_database}/{sf_schema}",
        query={"warehouse": sf_warehouse, "role": sf_role}
    )

    print("🔌 Connecting to Snowflake Data Source...")
    # Connect Snowflake to Great Expectations safely
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

    # Add our Gold daily stock summary table as an asset
    asset_name = "daily_stock_summary_asset"
    table_name = "GOLD_DAILY_STOCK_SUMMARY"
    
    # In SQLAlcheme context, we add it as a table asset
    # (Since we are using Postgres datasource handler, it works fine with SQLAlchemy connection strings)
    try:
        data_asset = datasource.add_table_asset(name=asset_name, table_name=table_name)
    except Exception:
        # If asset already exists in context
        data_asset = datasource.get_asset(asset_name)

    # Get batch request
    batch_request = data_asset.build_batch_request()

    print("📐 Creating Expectation Suite...")
    # Create or retrieve expectation suite
    suite_name = "stock_data_quality_suite"
    try:
        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    except Exception:
        suite = context.suites.get(suite_name)

    # Add our rules (expectations)
    print("✍️ Defining Validation Rules...")
    
    # 1. Open, High, Low, Close should not be null
    for col in ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'TICKER']:
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=col))

    # 2. Volume must be positive or zero
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column='VOLUME', 
        min_value=0
    ))

    # 3. Daily return percentage must be within standard bounds (-50% to +50%)
    # Prevents ingestion spikes or dirty data anomalies
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column='DAILY_RETURN_PCT',
        min_value=-50.0,
        max_value=50.0
    ))

    # 4. Check logic: High must be greater than or equal to Low
    suite.add_expectation(gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A='HIGH',
        column_B='LOW',
        or_equal=True
    ))

    # 5. Check logic: High must be >= Close
    suite.add_expectation(gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A='HIGH',
        column_B='CLOSE',
        or_equal=True
    ))

    print("🏃 Running Validation...")
    # Run the validation
    # Create definition and run
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite
    )
    
    results = validator.validate()

    # Print validation summary
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
        sys.exit(1)
    else:
        print("🎉 All data quality checks passed successfully!")

if __name__ == "__main__":
    run_validation()
