from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'start_date': datetime(2024, 12, 20),
    'catchup': False,
}

# Define the DAG
with DAG(
    'remove_duplicates_dag',
    default_args=default_args,
    description='Check and remove duplicates from all tables',
    schedule_interval=None,
    catchup=False,
) as dag:

    # Define tasks for each table
    remove_duplicates_mutual_funds = PostgresOperator(
        task_id='remove_duplicates_mutual_funds',
        postgres_conn_id='my_postgres_conn',
        sql="""
            DELETE FROM mutual_funds
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM mutual_funds
                GROUP BY fund_symbol, fund_long_name, fund_family
            );
        """,
    )

    remove_duplicates_etfs = PostgresOperator(
        task_id='remove_duplicates_etfs',
        postgres_conn_id='my_postgres_conn',
        sql="""
            DELETE FROM etfs
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM etfs
                GROUP BY fund_symbol, fund_long_name, exchange_code
            );
        """,
    )

    remove_duplicates_etf_prices = PostgresOperator(
        task_id='remove_duplicates_etf_prices',
        postgres_conn_id='my_postgres_conn',
        sql="""
            DELETE FROM etf_prices
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM etf_prices
                GROUP BY fund_symbol, price_date
            );
        """,
    )

    # A-E group operators
    remove_duplicates_mutual_fund_prices_a_b = PostgresOperator(
        task_id='remove_duplicates_mutual_fund_prices_a_b',
        postgres_conn_id='my_postgres_conn',
        sql="""
            DELETE FROM mutual_fund_prices_a_b
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM mutual_fund_prices_a_b
                GROUP BY fund_symbol, price_date
            );
        """,
    )

    remove_duplicates_mutual_fund_prices_c_d = PostgresOperator(
        task_id='remove_duplicates_mutual_fund_prices_c_d',
        postgres_conn_id='my_postgres_conn',
        sql="""
            DELETE FROM mutual_fund_prices_c_d
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM mutual_fund_prices_c_d
                GROUP BY fund_symbol, price_date
            );
        """,
    )

    remove_duplicates_mutual_fund_prices_e = PostgresOperator(
        task_id='remove_duplicates_mutual_fund_prices_e',
        postgres_conn_id='my_postgres_conn',
        sql="""
            DELETE FROM mutual_fund_prices_e
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM mutual_fund_prices_e
                GROUP BY fund_symbol, price_date
            );
        """,
    )

    # F-K group operators
    remove_duplicates_mutual_fund_prices_f_g = PostgresOperator(
        task_id='remove_duplicates_mutual_fund_prices_f_g',
        postgres_conn_id='my_postgres_conn',
        sql="""
            DELETE FROM mutual_fund_prices_f_g
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM mutual_fund_prices_f_g
                GROUP BY fund_symbol, price_date
            );
        """,
    )

    remove_duplicates_mutual_fund_prices_h_i = PostgresOperator(
        task_id='remove_duplicates_mutual_fund_prices_h_i',
        postgres_conn_id='my_postgres_conn',
        sql="""
            DELETE FROM mutual_fund_prices_h_i
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM mutual_fund_prices_h_i
                GROUP BY fund_symbol, price_date
            );
        """,
    )

    remove_duplicates_mutual_fund_prices_j_k = PostgresOperator(
        task_id='remove_duplicates_mutual_fund_prices_j_k',
        postgres_conn_id='my_postgres_conn',
        sql="""
            DELETE FROM mutual_fund_prices_j_k
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM mutual_fund_prices_j_k
                GROUP BY fund_symbol, price_date
            );
        """,
    )

    remove_duplicates_mutual_fund_prices_l_p = PostgresOperator(
        task_id='remove_duplicates_mutual_fund_prices_l_p',
        postgres_conn_id='my_postgres_conn',
        sql="""
            DELETE FROM mutual_fund_prices_l_p
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM mutual_fund_prices_l_p
                GROUP BY fund_symbol, price_date
            );
        """,
    )

    remove_duplicates_mutual_fund_prices_q_z = PostgresOperator(
        task_id='remove_duplicates_mutual_fund_prices_q_z',
        postgres_conn_id='my_postgres_conn',
        sql="""
            DELETE FROM mutual_fund_prices_q_z
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM mutual_fund_prices_q_z
                GROUP BY fund_symbol, price_date
            );
        """,
    )

    # Define task dependencies for parallel execution
    [
        remove_duplicates_mutual_funds,
        remove_duplicates_etfs,
        remove_duplicates_etf_prices,
        remove_duplicates_mutual_fund_prices_a_b,
        remove_duplicates_mutual_fund_prices_c_d,
        remove_duplicates_mutual_fund_prices_e,
        remove_duplicates_mutual_fund_prices_f_g,
        remove_duplicates_mutual_fund_prices_h_i,
        remove_duplicates_mutual_fund_prices_j_k,
        remove_duplicates_mutual_fund_prices_l_p,
        remove_duplicates_mutual_fund_prices_q_z
    ]