from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

default_args = {'owner': 'airflow', 'retries': 1, 'start_date': days_ago(1)}

with DAG(
    'full_pipeline',
    default_args=default_args,
    description='Master DAG to orchestrate other DAGs',
    schedule_interval=None,
    catchup=False,
    max_active_tasks=1  # Ensures tasks execute sequentially
) as dag:
    # Task 1: Trigger Data Ingestion DAG
    trigger_ingestion = TriggerDagRunOperator(
        task_id='trigger_data_ingestion',
        trigger_dag_id='data_ingestion_dag',
        trigger_rule=TriggerRule.ALL_SUCCESS,  # Proceed only if ingestion succeeds
    )

    # Task 2: Trigger Remove Duplicates DAG
    trigger_remove_duplicates = TriggerDagRunOperator(
        task_id='trigger_remove_duplicates',
        trigger_dag_id='remove_duplicates_dag',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Task 3: Trigger Transform Data DAG
    trigger_transform_data = TriggerDagRunOperator(
        task_id='trigger_transform_data',
        trigger_dag_id='transform_data_dag',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Task 4: Trigger Business Logic DAG
    trigger_business_logic = TriggerDagRunOperator(
        task_id='trigger_business_logic',
        trigger_dag_id='business_logic_ETFs_mutual_dag',
    )

    # Setting up dependencies
    trigger_ingestion >> trigger_remove_duplicates >> trigger_transform_data >> trigger_business_logic
