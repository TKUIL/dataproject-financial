# Apache Airflow and PostgreSQL Data Pipeline for Mutual Funds and ETFs

## Project Overview
This project builds a data pipeline using **Apache Airflow** and **PostgreSQL** to process, transform, and analyze data for mutual funds and ETFs. It includes data ingestion, transformation, and business logic implementation, with the potential to connect to BI tools for visualization.

## Features
- **Data Ingestion:** Load large financial datasets into PostgreSQL.
- **Data Transformation:** Clean and transform data using DAGs in Apache Airflow.
- **Business Logic:** Perform advanced SQL operations, such as merging datasets and calculating combined metrics.
- **Scalability:** Designed for integration with BI tools like PowerBI, Tableau

## Technologies Used
- **Apache Airflow:** Workflow orchestration and DAG management.
- **PostgreSQL:** Relational database for data storage and transformation.
- **Git LFS:** For managing large dataset files.
- **Python:** For pipeline logic and SQL execution.

## Installation

### Prerequisites
- Python >= 3.8
- PostgreSQL
- Apache Airflow
- Git LFS (for managing large files)

### Setup Instructions
1. **Clone the repository:**
   ```bash
   git clone https://github.com/TKUIL/dataproject-financial
   cd /dataproject-financial/airflow/airflow_home

2. **Set up Python environment:**
   ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
   
2. **Set up Git LFS (if required):**
   ```bash
   git lfs install
   git lfs pull

2. **Set up Git LFS (if required):**
   ```bash
   Initialize Airflow DB:
    
    airflow db migrate
    
   Start Airflow:
    
    airflow scheduler &
    airflow webserver

   Access the UI at http://localhost:8080.

   Set up PostgreSQL connection:

    Update Airflow's connections with PostgreSQL details (my_postgres_conn).

   Run the DAGs:

    Trigger DAGs via the Airflow UI or CLI.
    Available DAGs in order of operation:
    1. data_ingestion_dag
    2. remove_duplicates_dag
    3. transform_data_dag
    4. business_logic_dag
  

