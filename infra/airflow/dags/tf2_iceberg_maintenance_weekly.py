from airflow import DAG
from airflow.operators.python import PythonOperator

from tf2_common import (
    DEFAULT_ARGS,
    PIPELINE_TIMEOUT,
    START_DATE,
    TF2_AIRFLOW_MAINTENANCE_CRON,
    run_iceberg_maintenance,
    validate_runtime_config,
)

with DAG(
    dag_id="tf2_iceberg_maintenance_weekly",
    description="Run weekly Iceberg compaction and snapshot expiry.",
    default_args=DEFAULT_ARGS,
    start_date=START_DATE,
    schedule=TF2_AIRFLOW_MAINTENANCE_CRON,
    catchup=False,
    max_active_runs=1,
    tags=["tf2", "maintenance"],
) as dag:
    validate_config = PythonOperator(
        task_id="validate_runtime_config",
        python_callable=validate_runtime_config,
        retries=0,
    )

    iceberg_maintenance = PythonOperator(
        task_id="iceberg_maintenance",
        python_callable=run_iceberg_maintenance,
        execution_timeout=PIPELINE_TIMEOUT,
    )

    validate_config >> iceberg_maintenance
