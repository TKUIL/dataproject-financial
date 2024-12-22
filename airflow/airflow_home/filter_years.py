import pandas as pd
from datetime import datetime
import os

def filter_data(file_path, output_file, date_column, start_date, end_date):
    print(f"Processing: {file_path}")
    df = pd.read_csv(file_path)
    df[date_column] = pd.to_datetime(df[date_column], dayfirst=True, errors='coerce')
    df = df.dropna(subset=[date_column])
    filtered_df = df[(df[date_column] >= start_date) & (df[date_column] <= end_date)]
    print(f"Number of rows after filtering for {file_path}: {len(filtered_df)}")
    filtered_df.to_csv(output_file, index=False)
    print(f"Filtered data saved to {output_file}\n")

def filter_data_ae(file_path, output_prefix, date_column, start_date, end_date):
    print(f"Processing A-E file: {file_path}")
    df = pd.read_csv(file_path)
    df[date_column] = pd.to_datetime(df[date_column], dayfirst=True, errors='coerce')
    df = df.dropna(subset=[date_column])
    filtered_df = df[(df[date_column] >= start_date) & (df[date_column] <= end_date)]
    
    # Split into groups
    a_b = filtered_df[filtered_df['fund_symbol'].str[0].str.upper().isin(['A', 'B'])]
    c_d = filtered_df[filtered_df['fund_symbol'].str[0].str.upper().isin(['C', 'D'])]
    e = filtered_df[filtered_df['fund_symbol'].str[0].str.upper() == 'E']
    
    # Save each group
    a_b.to_csv(f'{output_prefix}_A_B.csv', index=False)
    c_d.to_csv(f'{output_prefix}_C_D.csv', index=False)
    e.to_csv(f'{output_prefix}_E.csv', index=False)
    
    print(f"A-B rows: {len(a_b)}")
    print(f"C-D rows: {len(c_d)}")
    print(f"E rows: {len(e)}\n")

def filter_data_fk(file_path, output_prefix, date_column, start_date, end_date):
    print(f"Processing F-K file: {file_path}")
    df = pd.read_csv(file_path)
    df[date_column] = pd.to_datetime(df[date_column], dayfirst=True, errors='coerce')
    df = df.dropna(subset=[date_column])
    filtered_df = df[(df[date_column] >= start_date) & (df[date_column] <= end_date)]
    
    # Split into groups
    f_g = filtered_df[filtered_df['fund_symbol'].str[0].str.upper().isin(['F', 'G'])]
    h_i = filtered_df[filtered_df['fund_symbol'].str[0].str.upper().isin(['H', 'I'])]
    j_k = filtered_df[filtered_df['fund_symbol'].str[0].str.upper().isin(['J', 'K'])]
    
    # Save each group
    f_g.to_csv(f'{output_prefix}_F_G.csv', index=False)
    h_i.to_csv(f'{output_prefix}_H_I.csv', index=False)
    j_k.to_csv(f'{output_prefix}_J_K.csv', index=False)
    
    print(f"F-G rows: {len(f_g)}")
    print(f"H-I rows: {len(h_i)}")
    print(f"J-K rows: {len(j_k)}\n")

# Define start and end dates for 2021-2022
start_date = datetime(2021, 1, 1)
end_date = datetime(2022, 12, 31)

# Filter ETF prices
etf_file_path = 'dags/data/ETF prices.csv'
etf_output_file = 'dags/data/ETF_prices_2021_to_2022.csv'
filter_data(etf_file_path, etf_output_file, 'price_date', start_date, end_date)

# Process A-E file separately
ae_file = 'dags/data/MutualFund prices - A-E.csv'
filter_data_ae(ae_file, 'dags/data/MutualFund_prices_2021_to_2022', 'price_date', start_date, end_date)

# Process F-K file separately
fk_file = 'dags/data/MutualFund prices - F-K.csv'
filter_data_fk(fk_file, 'dags/data/MutualFund_prices_2021_to_2022', 'price_date', start_date, end_date)

# Process remaining files
remaining_files = [
    'dags/data/MutualFund prices - L-P.csv',
    'dags/data/MutualFund prices - Q-Z.csv',
]

for file_path in remaining_files:
    output_file = file_path.replace('MutualFund prices', 'MutualFund_prices_2021_to_2022')
    filter_data(file_path, output_file, 'price_date', start_date, end_date)