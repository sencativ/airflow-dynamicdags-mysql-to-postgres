import yaml
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from resources.scripts.case_study import (
    extract_logic,
    load_logic,
    update_ingest_type_logic,
)

# Load konfigurasi dari file YAML
with open("dags/resources/config/case_study.yaml", "r") as f:
    config = yaml.safe_load(f)


@dag(default_args={"retries": 3, "retry_delay": 30})
def case_study():
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")
    wait_el_task = EmptyOperator(task_id="wait_el_task")
    wait_transform_task = EmptyOperator(task_id="wait_transform_task")

    # ========== Membuat task EL (Extract & Load) secara dinamis berdasarkan konfigurasi ==========
    for item in config.get("ingestion", []):

        # --- Ekstrak data dari mysql ke staging area ---
        @task(task_id=f"extract_to_staging_area.{item['table']}")
        def extract(item, **kwargs):
            extract_logic(item, **kwargs)

        # --- Load data dari staging area ke postgres ---
        @task(task_id=f"load_to_bronze.{item['table']}")
        def load(item, **kwargs):
            load_logic(item, **kwargs)

        # --- Update status ingest type di airflow variable ---
        @task(task_id=f"update_ingest_type.{item['table']}")
        def update_ingest_type(item, **kwargs):
            update_ingest_type_logic(item, **kwargs)

        (
            start_task
            >> extract(item)
            >> load(item)
            >> update_ingest_type(item)
            >> wait_el_task
        )

    # ========== Membuat task transformasi secara dinamis berdasarkan konfigurasi ==========
    for filepath in config.get("transformation", []):
        name = filepath.split("/")[-1].replace(".sql", "")

        # --- Transform data bronze ke silver ---
        transform_to_silver = SQLExecuteQueryOperator(
            task_id=f"transform_to_silver.{name}",
            conn_id="postgres_dibimbing",
            sql=filepath,
        )

        wait_el_task >> transform_to_silver >> wait_transform_task

    # ========== Membuat task datamart secara dinamis berdasarkan konfigurasi ==========
    for filepath in config.get("datamart", []):
        name = filepath.split("/")[-1].replace(".sql", "")

        # --- Membuat datamart di gold berdasarkan data silver ---
        datamart_to_gold = SQLExecuteQueryOperator(
            task_id=f"datamart_to_gold.{name}",
            conn_id="postgres_dibimbing",
            sql=filepath,
        )

        wait_transform_task >> datamart_to_gold >> end_task


# Mendefinisikan DAG
case_study()
