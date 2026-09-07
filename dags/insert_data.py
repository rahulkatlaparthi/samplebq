from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

PROJECT_ID = "qwiklabs-gcp-00-38c5c3a6722c"
DATASET_ID = "sales_dataset"

with DAG(
    dag_id="insert_sales_data",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    insert_data = BigQueryInsertJobOperator(
        task_id="insert_data",
        configuration={
            "query": {
                "query": f"""
                INSERT INTO `{PROJECT_ID}.{DATASET_ID}.products`
                (product_id, product_name, category, price, stock_quantity)
                VALUES
                    (101, 'Laptop', 'Electronics', 75000.00, 20),
                    (102, 'Mobile Phone', 'Electronics', 25000.00, 50),
                    (103, 'Headphones', 'Accessories', 5000.00, 100),
                    (104, 'Keyboard', 'Accessories', 2500.00, 75);

                INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sales`
                (sale_id, product_id, quantity, sale_amount, sale_date)
                VALUES
                    (1001, 101, 1, 75000.00, CURRENT_DATE()),
                    (1002, 102, 2, 50000.00, CURRENT_DATE()),
                    (1003, 103, 3, 15000.00, CURRENT_DATE()),
                    (1004, 104, 2, 5000.00, CURRENT_DATE());

                INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sales_summary`
                (summary_date, total_sales, total_quantity, total_revenue)

                SELECT
                    sale_date,
                    COUNT(*) AS total_sales,
                    SUM(quantity) AS total_quantity,
                    SUM(sale_amount) AS total_revenue
                FROM `{PROJECT_ID}.{DATASET_ID}.sales`
                GROUP BY sale_date;
                """,
                "useLegacySql": False,
            }
        },
        location="US",
    )
