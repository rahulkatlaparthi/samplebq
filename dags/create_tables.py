from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

PROJECT_ID = "qwiklabs-gcp-00-38c5c3a6722c"
DATASET_ID = "sales_dataset"

with DAG(
    dag_id="create_sales_tables",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    create_sales_tables = BigQueryInsertJobOperator(
        task_id="create_sales_tables",
        configuration={
            "query": {
                "query": f"""
                CREATE SCHEMA IF NOT EXISTS
                `{PROJECT_ID}.{DATASET_ID}`;

                CREATE TABLE IF NOT EXISTS
                `{PROJECT_ID}.{DATASET_ID}.products`
                (
                    product_id INT64,
                    product_name STRING,
                    category STRING,
                    price FLOAT64,
                    stock_quantity INT64
                );

                CREATE TABLE IF NOT EXISTS
                `{PROJECT_ID}.{DATASET_ID}.sales`
                (
                    sale_id INT64,
                    product_id INT64,
                    quantity INT64,
                    sale_amount FLOAT64,
                    sale_date DATE
                );

                CREATE TABLE IF NOT EXISTS
                `{PROJECT_ID}.{DATASET_ID}.sales_summary`
                (
                    summary_date DATE,
                    total_sales INT64,
                    total_quantity INT64,
                    total_revenue FLOAT64
                );
                """,
                "useLegacySql": False,
            }
        },
        location="US",
    ) 
