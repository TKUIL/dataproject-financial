from kaggle.api.kaggle_api_extended import KaggleApi

api = KaggleApi()
api.authenticate()

try:
    api.dataset_download_files(
        'stefanoleone992/mutual-funds-and-etfs', 
        path='./data',
        unzip=True  # This parameter handles automatic unzipping
    )
except Exception as e:
    print("An error occurred:", e)