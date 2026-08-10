from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

BRONZE_DATASETS = [
    ("events.csv", "events"),
    ("customers.csv", "customers"),
    ("orders.csv", "orders"),
    ("order_items.csv", "order_items"),
    ("products.csv", "products"),
    ("reviews.csv", "reviews"),
    ("sessions.csv", "sessions"),
]

SPARK_SUBMIT_BASE = " ".join([
    "spark-submit",
    "--master spark://spark-master:7077",
    "--driver-memory 1g",
    "--conf spark.cores.max=2",
    "--conf spark.executor.instances=1",
    "--conf spark.executor.cores=2",
    "--conf spark.executor.memory=1g",
    "--conf spark.serializer=org.apache.spark.serializer.JavaSerializer",
    "--jars /opt/project/drivers/delta-core_2.12-2.4.0.jar,/opt/project/drivers/delta-storage-2.4.0.jar",
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "--conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.HDFSLogStore",
])


with DAG (
    dag_id="medallion_pipeline_dag",
    description="DAG for Bronze ingestion in the medallion pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 7, 28),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    dagrun_timeout=timedelta(hours=2),
    tags=["lakehouse", "medallion"],
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="bronze_ingestion") as bronze_ingestion:
        for csv_file, dataset_name in BRONZE_DATASETS:
            BashOperator(
                task_id=f"ingest_{dataset_name}",
                bash_command=(
                    f"{SPARK_SUBMIT_BASE} "
                    f"--name bronze_ingestion_{dataset_name} "
                    "/opt/project/scripts/bronze/ingest_bronze.py "
                    "{{ ds }} "
                    f"{csv_file} "
                    f"{dataset_name}"
                ),
                execution_timeout=timedelta(minutes=45),
                do_xcom_push=False,
            )

    end = EmptyOperator(task_id="end")

    start >> bronze_ingestion >> end
