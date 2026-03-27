from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from tf2_common import (
    CHECK_TIMEOUT,
    DEFAULT_ARGS,
    PIPELINE_TIMEOUT,
    REFRESH_DAYS,
    SPARK_APPLICATION,
    START_DATE,
    TF2_TRINO_CONN_ID,
    assert_sql_task_has_no_failures,
    dag_conf_string,
    load_sql,
    spark_driver_memory,
    spark_executor_memory,
    spark_conf,
    spark_master,
    spark_packages,
    validate_runtime_config,
)


def should_run_serving_quality_checks(**context) -> bool:
    pipeline = dag_conf_string(context, "pipeline", "all")
    return pipeline in {"all", "feature-serving"}


def should_run_ml_readiness_checks(**context) -> bool:
    pipeline = dag_conf_string(context, "pipeline", "all")
    return pipeline in {"all", "ml"}


with DAG(
    dag_id="tf2_backfill_manual",
    description="Manually trigger full/incremental backfills through Spark pipelines.",
    default_args=DEFAULT_ARGS,
    start_date=START_DATE,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["tf2", "backfill"],
) as dag:
    validate_config = PythonOperator(
        task_id="validate_runtime_config",
        python_callable=validate_runtime_config,
        retries=0,
    )

    spark_processing_backfill = SparkSubmitOperator(
        task_id="spark_processing_backfill",
        conn_id="spark_default",
        application=SPARK_APPLICATION,
        application_args=[
            "--mode",
            "{{ dag_run.conf.get('mode', 'full') }}",
            "--refresh-days",
            REFRESH_DAYS,
            "--pipeline",
            "{{ dag_run.conf.get('pipeline', 'all') }}",
        ],
        packages=spark_packages(),
        conf=spark_conf(),
        executor_memory=spark_executor_memory(),
        driver_memory=spark_driver_memory(),
        env_vars={"SPARK_MASTER": spark_master()},
        execution_timeout=PIPELINE_TIMEOUT,
    )

    maybe_serving_quality_checks = ShortCircuitOperator(
        task_id="maybe_serving_quality_checks",
        python_callable=should_run_serving_quality_checks,
    )

    serving_quality_checks_sql = SQLExecuteQueryOperator(
        task_id="serving_quality_checks_sql",
        conn_id=TF2_TRINO_CONN_ID,
        sql=load_sql("infra/trino/queries/quality/data_quality_checks.sql"),
        execution_timeout=CHECK_TIMEOUT,
    )

    serving_quality_checks_assert = PythonOperator(
        task_id="serving_quality_checks_assert",
        python_callable=assert_sql_task_has_no_failures,
        op_kwargs={"sql_task_id": "serving_quality_checks_sql"},
    )

    maybe_ml_readiness_checks = ShortCircuitOperator(
        task_id="maybe_ml_readiness_checks",
        python_callable=should_run_ml_readiness_checks,
    )

    ml_readiness_checks_sql = SQLExecuteQueryOperator(
        task_id="ml_readiness_checks_sql",
        conn_id=TF2_TRINO_CONN_ID,
        sql=load_sql("infra/trino/queries/ml/ml_data_readiness_check.sql"),
        execution_timeout=CHECK_TIMEOUT,
    )

    ml_readiness_checks_assert = PythonOperator(
        task_id="ml_readiness_checks_assert",
        python_callable=assert_sql_task_has_no_failures,
        op_kwargs={"sql_task_id": "ml_readiness_checks_sql"},
    )

    validate_config >> spark_processing_backfill
    spark_processing_backfill >> maybe_serving_quality_checks >> serving_quality_checks_sql >> serving_quality_checks_assert
    spark_processing_backfill >> maybe_ml_readiness_checks >> ml_readiness_checks_sql >> ml_readiness_checks_assert
