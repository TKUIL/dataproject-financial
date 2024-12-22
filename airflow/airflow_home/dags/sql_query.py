from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 12, 20),
}

with DAG('list_all_tables_dag',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    list_tables = PostgresOperator(
        task_id='list_all_tables',
        postgres_conn_id='my_postgres_conn',  # Connection ID in Airflow
        sql="""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public';
        """
    )
