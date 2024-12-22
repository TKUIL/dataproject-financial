import os
from kaggle.api.kaggle_api_extended import KaggleApi

api = KaggleApi()
api.authenticate()

try:
    target_path = os.path.abspath('./dataproject-financial/airflow/airflow_home/dags/data')
    print(f"Downloading files to: {target_path}")

    api.dataset_download_files(
        'stefanoleone992/mutual-funds-and-etfs', 
        path=target_path,
        unzip=True  # Automatically unzips
    )
except Exception as e:
    print("An error occurred:", e)