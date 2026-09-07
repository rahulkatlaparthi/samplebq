from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime


# Read values from Airflow Variables
PROJECT_ID = Variable.get("gcp_project_id")
DATASET_ID = Variable.get("bq_dataset_id")

TABLE_ID = "inventory"


with DAG(
    dag_id="inventory",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    create_inventory_table = BigQueryInsertJobOperator(
        task_id="create_inventory_table",
        configuration={
            "query": {
                "query": f"""
                CREATE TABLE IF NOT EXISTS
                `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
                (
                    inventory_id INT64,
                    product_id INT64,
                    warehouse STRING,
                    quantity INT64,
                    last_updated TIMESTAMP
                )
                """,
                "useLegacySql": False,
            }
        },
        location="US",
    )

    insert_inventory_data = BigQueryInsertJobOperator(
        task_id="insert_inventory_data",
        configuration={
            "query": {
                "query": f"""
                INSERT INTO
                `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
                (
                    inventory_id,
                    product_id,
                    warehouse,
                    quantity,
                    last_updated
                )
                VALUES
                    (1, 101, 'Hyderabad', 25, CURRENT_TIMESTAMP()),
                    (2, 102, 'Bangalore', 40, CURRENT_TIMESTAMP()),
                    (3, 103, 'Vijayawada', 60, CURRENT_TIMESTAMP()),
                    (4, 104, 'Chennai', 35, CURRENT_TIMESTAMP())
                """,
                "useLegacySql": False,
            }
        },
        location="US",
    )

    create_inventory_table >> insert_inventory_data
