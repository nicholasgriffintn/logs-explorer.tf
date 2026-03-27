from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from tf2_common import (
    CHECK_TIMEOUT,
    DEFAULT_ARGS,
    PIPELINE_TIMEOUT,
    REFRESH_DAYS,
    SPARK_APPLICATION,
    START_DATE,
    TF2_AIRFLOW_FEATURE_CRON,
    TF2_TRINO_CONN_ID,
    assert_sql_task_has_no_failures,
    load_sql,
    spark_driver_memory,
    spark_executor_memory,
    spark_conf,
    spark_master,
    spark_packages,
    validate_runtime_config,
)

with DAG(
    dag_id="tf2_feature_serving_daily",
    description="Run feature-serving Spark refresh and serving quality checks.",
    default_args=DEFAULT_ARGS,
    start_date=START_DATE,
    schedule=TF2_AIRFLOW_FEATURE_CRON,
    catchup=False,
    max_active_runs=1,
    tags=["tf2", "feature-serving"],
) as dag:
    validate_config = PythonOperator(
        task_id="validate_runtime_config",
        python_callable=validate_runtime_config,
        retries=0,
    )

    spark_feature_serving_incremental = SparkSubmitOperator(
        task_id="spark_feature_serving_incremental",
        conn_id="spark_default",
        application=SPARK_APPLICATION,
        application_args=["--mode", "incremental", "--refresh-days", REFRESH_DAYS, "--pipeline", "feature-serving"],
        packages=spark_packages(),
        conf=spark_conf(),
        executor_memory=spark_executor_memory(),
        driver_memory=spark_driver_memory(),
        env_vars={"SPARK_MASTER": spark_master()},
        execution_timeout=PIPELINE_TIMEOUT,
    )

    serving_quality_checks_sql = SQLExecuteQueryOperator(
        task_id="serving_quality_checks_sql",
        conn_id=TF2_TRINO_CONN_ID,
        sql=load_sql("infra/trino/queries/quality/serving_quality_checks.sql"),
        execution_timeout=CHECK_TIMEOUT,
    )

    serving_quality_checks_assert = PythonOperator(
        task_id="serving_quality_checks_assert",
        python_callable=assert_sql_task_has_no_failures,
        op_kwargs={"sql_task_id": "serving_quality_checks_sql"},
    )

    validate_config >> spark_feature_serving_incremental >> serving_quality_checks_sql >> serving_quality_checks_assert
