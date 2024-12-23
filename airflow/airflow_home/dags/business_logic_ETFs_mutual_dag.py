from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': days_ago(1),
}

# Define the DAG
with DAG(
    dag_id='business_logic_ETFs_mutual_dag',
    default_args=default_args,
    description='A DAG to merge ETFs and Mutual Funds data',
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task 1: Merge on Shared Columns
    merge_on_shared_columns = PostgresOperator(
        task_id='merge_on_shared_columns',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS merged_funds_etfs AS
            SELECT
                COALESCE(etfs.fund_symbol, mutual_funds.fund_symbol) AS fund_symbol,
                COALESCE(etfs.region, mutual_funds.region) AS region,
                etfs.fund_long_name AS etf_long_name,
                mutual_funds.fund_long_name AS mutual_fund_long_name,
                etfs.currency AS etf_currency,
                mutual_funds.currency AS mutual_fund_currency,
                etfs.total_net_assets AS etf_net_assets,
                mutual_funds.total_net_assets AS mutual_fund_net_assets,
                etfs.fund_yield AS etf_yield,
                mutual_funds.fund_yield AS mutual_fund_yield,
                etfs.investment_strategy AS etf_investment_strategy,
                mutual_funds.investment_strategy AS mutual_fund_investment_strategy,
                etfs.avg_vol_3month,
                etfs.avg_vol_10day
            FROM
                transformed_etfs etfs
            FULL OUTER JOIN
                transformed_mutual_funds mutual_funds
            ON
                etfs.fund_symbol = mutual_funds.fund_symbol
                AND etfs.region = mutual_funds.region;
        """,
    )

    # Task 2: Combine Net Assets
    combine_net_assets = PostgresOperator(
        task_id='combine_net_assets',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS combined_net_assets AS
            SELECT
                COALESCE(etfs.fund_symbol, mutual_funds.fund_symbol) AS fund_symbol,
                COALESCE(etfs.region, mutual_funds.region) AS region,
                SUM(COALESCE(etfs.total_net_assets, 0) + COALESCE(mutual_funds.total_net_assets, 0)) AS total_combined_assets
            FROM
                transformed_etfs etfs
            FULL OUTER JOIN
                transformed_mutual_funds mutual_funds
            ON
                etfs.fund_symbol = mutual_funds.fund_symbol
                AND etfs.region = mutual_funds.region
            GROUP BY
                COALESCE(etfs.fund_symbol, mutual_funds.fund_symbol),
                COALESCE(etfs.region, mutual_funds.region);
        """,
    )

    # Task 3: Overlapping Categories
    overlapping_categories = PostgresOperator(
        task_id='overlapping_categories',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS overlapping_categories AS
            SELECT
                etfs.fund_symbol AS etf_symbol,
                mutual_funds.fund_symbol AS mutual_fund_symbol,
                etfs.fund_category AS etf_category,
                mutual_funds.fund_category AS mutual_fund_category,
                etfs.total_net_assets AS etf_net_assets,
                mutual_funds.total_net_assets AS mutual_fund_net_assets
            FROM
                transformed_etfs etfs
            INNER JOIN
                transformed_mutual_funds mutual_funds
            ON
                etfs.fund_category = mutual_funds.fund_category;
        """,
    )

    # Task 4: Performance Comparison
    performance_comparison = PostgresOperator(
        task_id='performance_comparison',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS performance_comparison AS
            SELECT
                COALESCE(etfs.fund_symbol, mutual_funds.fund_symbol) AS fund_symbol,
                etfs.fund_yield AS etf_yield,
                mutual_funds.fund_yield AS mutual_fund_yield,
                etfs.week52_high AS etf_52_week_high,
                mutual_funds.week52_high AS mutual_fund_52_week_high,
                etfs.week52_low AS etf_52_week_low,
                mutual_funds.week52_low AS mutual_fund_52_week_low
            FROM
                transformed_etfs etfs
            FULL OUTER JOIN
                transformed_mutual_funds mutual_funds
            ON
                etfs.fund_symbol = mutual_funds.fund_symbol;
        """,
    )

    # Task dependencies
    merge_on_shared_columns >> combine_net_assets >> overlapping_categories >> performance_comparison
