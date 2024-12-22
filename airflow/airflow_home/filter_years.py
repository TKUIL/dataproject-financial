import pandas as pd
from datetime import datetime
import os

# Define function to filter data by date range
def filter_data(file_path, output_file, date_column, start_date, end_date):
    print(f"Processing: {file_path}")
    # Load the CSV file
    df = pd.read_csv(file_path)

    # Convert the date column to datetime format
    df[date_column] = pd.to_datetime(df[date_column], dayfirst=True, errors='coerce')

    # Drop rows where the date conversion failed
    df = df.dropna(subset=[date_column])

    # Filter the rows for the date range
    filtered_df = df[(df[date_column] >= start_date) & (df[date_column] <= end_date)]
    print(f"Number of rows after filtering for {file_path}: {len(filtered_df)}")

    # Save the filtered data to a new CSV file
    filtered_df.to_csv(output_file, index=False)
    print(f"Filtered data saved to {output_file}\n")

# Define start and end dates for 2021-2022
start_date = datetime(2021, 1, 1)
end_date = datetime(2022, 12, 31)

# File paths for ETF prices
etf_file_path = 'dags/data/ETF prices.csv'
etf_output_file = 'dags/data/ETF_prices_2021_to_2022.csv'

# Filter ETF prices
filter_data(etf_file_path, etf_output_file, 'price_date', start_date, end_date)

# File paths for MutualFund prices
mutual_fund_files = [
    'dags/data/MutualFund prices - A-E.csv',
    'dags/data/MutualFund prices - F-K.csv',
    'dags/data/MutualFund prices - L-P.csv',
    'dags/data/MutualFund prices - Q-Z.csv',
]

# Process each MutualFund file
for file_path in mutual_fund_files:
    # Generate output file path
    output_file = file_path.replace('MutualFund prices', 'MutualFund_prices_2021_to_2022')
    filter_data(file_path, output_file, 'price_date', start_date, end_date)
