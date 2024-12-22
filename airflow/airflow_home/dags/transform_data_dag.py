from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='transform_financial_data',
    default_args=default_args,
    description='Transform ETF and mutual fund data',
    schedule_interval=None, 
    start_date=datetime(2023, 12, 22),
    catchup=False,
) as dag:

    transform_etf_prices = PostgresOperator(
        task_id='transform_etf_prices',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS transformed_etf_prices AS
            SELECT
                CONCAT(fund_symbol, '_', price_date) as etf_price_id,
                fund_symbol,
                CAST(price_date AS DATE) AS price_date,
                CAST(open AS NUMERIC(12, 2)) AS open,
                CAST(high AS NUMERIC(12, 2)) AS high,
                CAST(low AS NUMERIC(12, 2)) AS low,
                CAST(close AS NUMERIC(12, 2)) AS close,
                CAST(adj_close AS NUMERIC(12, 2)) AS adj_close,
                CAST(volume AS BIGINT) AS volume
            FROM etf_prices
            WHERE fund_symbol IS NOT NULL;
        """
    )

    transform_etfs = PostgresOperator(
        task_id='transform_etfs',
        postgres_conn_id='my_postgres_conn',
        sql="""
        CREATE TABLE IF NOT EXISTS transformed_etfs AS
        SELECT
            CONCAT(fund_symbol, '_', region) AS etf_id,
            fund_symbol,
            quote_type,
            region,
            fund_short_name,
            fund_long_name,
            currency,
            fund_category,
            fund_family,
            exchange_code,
            exchange_name,
            exchange_timezone,
            CAST(avg_vol_3month AS BIGINT) AS avg_vol_3month,
            CAST(avg_vol_10day AS BIGINT) AS avg_vol_10day,
            CAST(total_net_assets AS NUMERIC(20, 2)) AS total_net_assets,
            CAST(day50_moving_average AS NUMERIC(15, 2)) AS day50_moving_average,
            CAST(day200_moving_average AS NUMERIC(15, 2)) AS day200_moving_average,
            CAST(week52_high_low_change AS NUMERIC(15, 2)) AS week52_high_low_change,
            CAST(week52_high_low_change_perc AS NUMERIC(10, 2)) AS week52_high_low_change_perc,
            CAST(week52_high AS NUMERIC(15, 2)) AS week52_high,
            CAST(week52_high_change AS NUMERIC(15, 2)) AS week52_high_change,
            CAST(week52_high_change_perc AS NUMERIC(10, 2)) AS week52_high_change_perc,
            CAST(week52_low AS NUMERIC(15, 2)) AS week52_low,
            CAST(week52_low_change AS NUMERIC(15, 2)) AS week52_low_change,
            CAST(week52_low_change_perc AS NUMERIC(10, 2)) AS week52_low_change_perc,
            investment_strategy,
            CAST(fund_yield AS NUMERIC(10, 2)) AS fund_yield,
            CAST(inception_date AS DATE) AS inception_date,
            CAST(annual_holdings_turnover AS NUMERIC(10, 2)) AS annual_holdings_turnover,
            investment_type,
            size_type,
            CAST(fund_annual_report_net_expense_ratio AS NUMERIC(10, 2)) AS fund_annual_report_net_expense_ratio,
            CAST(category_annual_report_net_expense_ratio AS NUMERIC(10, 2)) AS category_annual_report_net_expense_ratio,
            CAST(asset_stocks AS NUMERIC(15, 2)) AS asset_stocks,
            CAST(asset_bonds AS NUMERIC(15, 2)) AS asset_bonds,
            CAST(fund_sector_basic_materials AS NUMERIC(10, 2)) AS fund_sector_basic_materials,
            CAST(fund_sector_communication_services AS NUMERIC(10, 2)) AS fund_sector_communication_services,
            CAST(fund_sector_consumer_cyclical AS NUMERIC(10, 2)) AS fund_sector_consumer_cyclical,
            CAST(fund_sector_consumer_defensive AS NUMERIC(10, 2)) AS fund_sector_consumer_defensive,
            CAST(fund_sector_energy AS NUMERIC(10, 2)) AS fund_sector_energy,
            CAST(fund_sector_financial_services AS NUMERIC(10, 2)) AS fund_sector_financial_services,
            CAST(fund_sector_healthcare AS NUMERIC(10, 2)) AS fund_sector_healthcare,
            CAST(fund_sector_industrials AS NUMERIC(10, 2)) AS fund_sector_industrials,
            CAST(fund_sector_real_estate AS NUMERIC(10, 2)) AS fund_sector_real_estate,
            CAST(fund_sector_technology AS NUMERIC(10, 2)) AS fund_sector_technology,
            CAST(fund_sector_utilities AS NUMERIC(10, 2)) AS fund_sector_utilities,
            CAST(fund_price_book_ratio AS NUMERIC(15, 2)) AS fund_price_book_ratio,
            CAST(fund_price_cashflow_ratio AS NUMERIC(15, 2)) AS fund_price_cashflow_ratio,
            CAST(fund_price_earning_ratio AS NUMERIC(15, 2)) AS fund_price_earning_ratio,
            CAST(fund_price_sales_ratio AS NUMERIC(15, 2)) AS fund_price_sales_ratio,
            CAST(fund_bond_maturity AS NUMERIC(15, 2)) AS fund_bond_maturity,
            CAST(fund_bond_duration AS NUMERIC(15, 2)) AS fund_bond_duration,
            CAST(fund_bonds_us_government AS NUMERIC(15, 2)) AS fund_bonds_us_government,
            CAST(fund_bonds_aaa AS NUMERIC(15, 2)) AS fund_bonds_aaa,
            CAST(fund_bonds_aa AS NUMERIC(15, 2)) AS fund_bonds_aa,
            CAST(fund_bonds_a AS NUMERIC(15, 2)) AS fund_bonds_a,
            CAST(fund_bonds_bbb AS NUMERIC(15, 2)) AS fund_bonds_bbb,
            CAST(fund_bonds_bb AS NUMERIC(15, 2)) AS fund_bonds_bb,
            CAST(fund_bonds_b AS NUMERIC(15, 2)) AS fund_bonds_b,
            CAST(fund_bonds_below_b AS NUMERIC(15, 2)) AS fund_bonds_below_b,
            CAST(fund_bonds_others AS NUMERIC(15, 2)) AS fund_bonds_others,
            top10_holdings,
            CAST(top10_holdings_total_assets AS NUMERIC(15, 2)) AS top10_holdings_total_assets,
            CAST(returns_as_of_date AS DATE) AS returns_as_of_date,
            CAST(fund_return_ytd AS NUMERIC(15, 2)) AS fund_return_ytd,
            CAST(category_return_ytd AS NUMERIC(15, 2)) AS category_return_ytd
        FROM etfs
        WHERE fund_symbol IS NOT NULL;
        """
    )

    transform_mutual_fund_prices_a_b = PostgresOperator(
        task_id='transform_mutual_fund_prices_a_b',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS transformed_mutual_fund_prices_a_b AS
            SELECT 
                CONCAT(fund_symbol, '_', price_date) as mutual_fund_price_id,
                fund_symbol,
                CAST(price_date AS DATE) AS price_date,
                CAST(nav_per_share AS NUMERIC(10, 2)) AS nav_per_share
            FROM mutual_fund_prices_a_b
            WHERE fund_symbol IS NOT NULL;
        """
    )

    transform_mutual_fund_prices_c_d = PostgresOperator(
        task_id='transform_mutual_fund_prices_c_d',
        postgres_conn_id='my_postgres_conn', 
        sql="""
            CREATE TABLE IF NOT EXISTS transformed_mutual_fund_prices_c_d AS
            SELECT
                CONCAT(fund_symbol, '_', price_date) as mutual_fund_price_id,
                fund_symbol,
                CAST(price_date AS DATE) AS price_date,
                CAST(nav_per_share AS NUMERIC(10, 2)) AS nav_per_share
            FROM mutual_fund_prices_c_d
            WHERE fund_symbol IS NOT NULL;
        """
    )

    transform_mutual_fund_prices_e = PostgresOperator(
        task_id='transform_mutual_fund_prices_e',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS transformed_mutual_fund_prices_e AS 
            SELECT
                CONCAT(fund_symbol, '_', price_date) as mutual_fund_price_id,
                fund_symbol,
                CAST(price_date AS DATE) AS price_date,
                CAST(nav_per_share AS NUMERIC(10, 2)) AS nav_per_share
            FROM mutual_fund_prices_e
            WHERE fund_symbol IS NOT NULL;
        """
    )

    transform_mutual_fund_prices_f_g = PostgresOperator(
        task_id='transform_mutual_fund_prices_f_g',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS transformed_mutual_fund_prices_f_g AS
            SELECT
                CONCAT(fund_symbol, '_', price_date) as mutual_fund_price_id,
                fund_symbol,
                CAST(price_date AS DATE) AS price_date,
                CAST(nav_per_share AS NUMERIC(10, 2)) AS nav_per_share
            FROM mutual_fund_prices_f_g
            WHERE fund_symbol IS NOT NULL;
        """
    )

    transform_mutual_fund_prices_h_i = PostgresOperator(
        task_id='transform_mutual_fund_prices_h_i',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS transformed_mutual_fund_prices_h_i AS
            SELECT
                CONCAT(fund_symbol, '_', price_date) as mutual_fund_price_id,
                fund_symbol,
                CAST(price_date AS DATE) AS price_date,
                CAST(nav_per_share AS NUMERIC(10, 2)) AS nav_per_share
            FROM mutual_fund_prices_h_i
            WHERE fund_symbol IS NOT NULL;
        """
    )

    transform_mutual_fund_prices_j_k = PostgresOperator(
        task_id='transform_mutual_fund_prices_j_k',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS transformed_mutual_fund_prices_j_k AS
            SELECT
                CONCAT(fund_symbol, '_', price_date) as mutual_fund_price_id,
                fund_symbol,
                CAST(price_date AS DATE) AS price_date,
                CAST(nav_per_share AS NUMERIC(10, 2)) AS nav_per_share
            FROM mutual_fund_prices_j_k
            WHERE fund_symbol IS NOT NULL;
        """
    )

    transform_mutual_fund_prices_l_p = PostgresOperator(
        task_id='transform_mutual_fund_prices_l_p',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS transformed_mutual_fund_prices_l_p AS
            SELECT
                CONCAT(fund_symbol, '_', price_date) as mutual_fund_price_id,
                fund_symbol,
                CAST(price_date AS DATE) AS price_date,
                CAST(nav_per_share AS NUMERIC(10, 2)) AS nav_per_share
            FROM mutual_fund_prices_l_p
            WHERE fund_symbol IS NOT NULL;
        """
    )

    transform_mutual_fund_prices_q_z = PostgresOperator(
        task_id='transform_mutual_fund_prices_q_z',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS transformed_mutual_fund_prices_q_z AS
            SELECT
                CONCAT(fund_symbol, '_', price_date) as mutual_fund_price_id,
                fund_symbol,
                CAST(price_date AS DATE) AS price_date,
                CAST(nav_per_share AS NUMERIC(10, 2)) AS nav_per_share
            FROM mutual_fund_prices_q_z
            WHERE fund_symbol IS NOT NULL;
        """
    )

    transform_mutual_funds = PostgresOperator(
        task_id='transform_mutual_funds',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS transformed_mutual_funds AS
            SELECT
                CONCAT(fund_symbol, '_', region, '_', fund_long_name) AS mutual_funds_id,
                fund_symbol,
                quote_type,
                region,
                fund_short_name,
                fund_long_name,
                currency,
                CAST(initial_investment AS NUMERIC(15, 2)) AS initial_investment,
                CAST(subsequent_investment AS NUMERIC(15, 2)) AS subsequent_investment,
                fund_category,
                fund_family,
                exchange_code,
                exchange_name,
                exchange_timezone,
                management_name,
                management_bio,
                CAST(management_start_date AS DATE) AS management_start_date,
                CAST(total_net_assets AS NUMERIC(20, 2)) AS total_net_assets,
                CAST(year_to_date_return AS NUMERIC(10, 2)) AS year_to_date_return,
                CAST(day50_moving_average AS NUMERIC(15, 2)) AS day50_moving_average,
                CAST(day200_moving_average AS NUMERIC(15, 2)) AS day200_moving_average,
                CAST(week52_high_low_change AS NUMERIC(15, 2)) AS week52_high_low_change,
                CAST(week52_high_low_change_perc AS NUMERIC(10, 2)) AS week52_high_low_change_perc,
                CAST(week52_high AS NUMERIC(15, 2)) AS week52_high,
                CAST(week52_high_change AS NUMERIC(15, 2)) AS week52_high_change,
                CAST(week52_high_change_perc AS NUMERIC(10, 2)) AS week52_high_change_perc,
                CAST(week52_low AS NUMERIC(15, 2)) AS week52_low,
                CAST(week52_low_change AS NUMERIC(15, 2)) AS week52_low_change,
                CAST(week52_low_change_perc AS NUMERIC(10, 2)) AS week52_low_change_perc,
                investment_strategy,
                CAST(fund_yield AS NUMERIC(10, 2)) AS fund_yield,
                CAST(morningstar_overall_rating AS INTEGER) AS morningstar_overall_rating,
                CAST(morningstar_risk_rating AS INTEGER) AS morningstar_risk_rating,
                CAST(inception_date AS DATE) AS inception_date,
                CAST(last_dividend AS NUMERIC(15, 2)) AS last_dividend,
                CAST(last_cap_gain AS NUMERIC(15, 2)) AS last_cap_gain,
                CAST(annual_holdings_turnover AS NUMERIC(10, 2)) AS annual_holdings_turnover,
                investment_type,
                size_type,
                CAST(fund_annual_report_net_expense_ratio AS NUMERIC(10, 2)) AS fund_annual_report_net_expense_ratio,
                CAST(category_annual_report_net_expense_ratio AS NUMERIC(10, 2)) AS category_annual_report_net_expense_ratio,
                CAST(fund_prospectus_net_expense_ratio AS NUMERIC(10, 2)) AS fund_prospectus_net_expense_ratio,
                CAST(fund_prospectus_gross_expense_ratio AS NUMERIC(10, 2)) AS fund_prospectus_gross_expense_ratio,
                CAST(fund_max_12b1_fee AS NUMERIC(10, 2)) AS fund_max_12b1_fee,
                CAST(fund_max_front_end_sales_load AS NUMERIC(10, 2)) AS fund_max_front_end_sales_load,
                CAST(category_max_front_end_sales_load AS NUMERIC(10, 2)) AS category_max_front_end_sales_load,
                CAST(fund_max_deferred_sales_load AS NUMERIC(10, 2)) AS fund_max_deferred_sales_load,
                CAST(category_max_deferred_sales_load AS NUMERIC(10, 2)) AS category_max_deferred_sales_load,
                CAST(fund_year3_expense_projection AS NUMERIC(20, 2)) AS fund_year3_expense_projection,
                CAST(fund_year5_expense_projection AS NUMERIC(20, 2)) AS fund_year5_expense_projection,
                CAST(fund_year10_expense_projection AS NUMERIC(20, 2)) AS fund_year10_expense_projection
            FROM mutual_funds
            WHERE fund_symbol IS NOT NULL;
        """
    )

        

transform_etf_prices >> transform_etfs >> [transform_mutual_funds, transform_mutual_fund_prices_a_b, 
                                         transform_mutual_fund_prices_c_d, transform_mutual_fund_prices_e, 
                                         transform_mutual_fund_prices_f_g, transform_mutual_fund_prices_h_i, 
                                         transform_mutual_fund_prices_j_k, transform_mutual_fund_prices_l_p, 
                                         transform_mutual_fund_prices_q_z]