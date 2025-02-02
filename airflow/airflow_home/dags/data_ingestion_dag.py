from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from sqlalchemy import create_engine

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='data_ingestion_dag',
    default_args=default_args,
    description='A DAG to ingest CSV files into PostgreSQL',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def ingest_file_to_postgres(filename, table_name):
        # Set up the database connection
        engine = create_engine('postgresql+psycopg2://airflow:airflow@localhost:5432/airflow')

        # Read the CSV file into a Pandas DataFrame
        df = pd.read_csv(f'/home/tku/Projects/dataproject/dataproject-financial/airflow/airflow_home/dags/data/{filename}')

        # Write the DataFrame to PostgreSQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Successfully ingested {filename} into {table_name}")

    # Define tasks for each file
    ingest_etf_prices = PythonOperator(
        task_id='ingest_etf_prices',
        python_callable=ingest_file_to_postgres,
        op_kwargs={'filename': 'ETF_prices_2021_to_2022.csv', 'table_name': 'etf_prices'},
    )

    ingest_etfs = PythonOperator(
        task_id='ingest_etfs',
        python_callable=ingest_file_to_postgres,
        op_kwargs={'filename': 'ETFs.csv', 'table_name': 'etfs'},
    )

    # A-E group operators
    ingest_mutual_fund_prices_a_b = PythonOperator(
        task_id='ingest_mutual_fund_prices_a_b',
        python_callable=ingest_file_to_postgres,
        op_kwargs={'filename': 'MutualFund_prices_2021_to_2022_A_B.csv', 'table_name': 'mutual_fund_prices_a_b'},
    )

    ingest_mutual_fund_prices_c_d = PythonOperator(
        task_id='ingest_mutual_fund_prices_c_d',
        python_callable=ingest_file_to_postgres,
        op_kwargs={'filename': 'MutualFund_prices_2021_to_2022_C_D.csv', 'table_name': 'mutual_fund_prices_c_d'},
    )

    ingest_mutual_fund_prices_e = PythonOperator(
        task_id='ingest_mutual_fund_prices_e',
        python_callable=ingest_file_to_postgres,
        op_kwargs={'filename': 'MutualFund_prices_2021_to_2022_E.csv', 'table_name': 'mutual_fund_prices_e'},
    )

    # F-K group operators
    ingest_mutual_fund_prices_f_g = PythonOperator(
        task_id='ingest_mutual_fund_prices_f_g',
        python_callable=ingest_file_to_postgres,
        op_kwargs={'filename': 'MutualFund_prices_2021_to_2022_F_G.csv', 'table_name': 'mutual_fund_prices_f_g'},
    )

    ingest_mutual_fund_prices_h_i = PythonOperator(
        task_id='ingest_mutual_fund_prices_h_i',
        python_callable=ingest_file_to_postgres,
        op_kwargs={'filename': 'MutualFund_prices_2021_to_2022_H_I.csv', 'table_name': 'mutual_fund_prices_h_i'},
    )

    ingest_mutual_fund_prices_j_k = PythonOperator(
        task_id='ingest_mutual_fund_prices_j_k',
        python_callable=ingest_file_to_postgres,
        op_kwargs={'filename': 'MutualFund_prices_2021_to_2022_J_K.csv', 'table_name': 'mutual_fund_prices_j_k'},
    )

    ingest_mutual_fund_prices_l_p = PythonOperator(
        task_id='ingest_mutual_fund_prices_l_p',
        python_callable=ingest_file_to_postgres,
        op_kwargs={'filename': 'MutualFund_prices_2021_to_2022 - L-P.csv', 'table_name': 'mutual_fund_prices_l_p'},
    )

    ingest_mutual_fund_prices_q_z = PythonOperator(
        task_id='ingest_mutual_fund_prices_q_z',
        python_callable=ingest_file_to_postgres,
        op_kwargs={'filename': 'MutualFund_prices_2021_to_2022 - Q-Z.csv', 'table_name': 'mutual_fund_prices_q_z'},
    )

    ingest_mutual_funds = PythonOperator(
        task_id='ingest_mutual_funds',
        python_callable=ingest_file_to_postgres,
        op_kwargs={'filename': 'MutualFunds.csv', 'table_name': 'mutual_funds'},
    )

    # Update dependency chain
    (
        ingest_etf_prices
        >> ingest_etfs
        >> [
            ingest_mutual_fund_prices_a_b,
            ingest_mutual_fund_prices_c_d,
            ingest_mutual_fund_prices_e,
            ingest_mutual_fund_prices_f_g,
            ingest_mutual_fund_prices_h_i,
            ingest_mutual_fund_prices_j_k,
            ingest_mutual_fund_prices_l_p,
            ingest_mutual_fund_prices_q_z,
        ]
        >> ingest_mutual_funds
    )