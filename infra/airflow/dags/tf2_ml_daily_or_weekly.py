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
    TF2_AIRFLOW_ML_CRON,
    TF2_TRINO_CONN_ID,
    assert_sql_task_has_no_failures,
    dag_conf_bool,
    load_sql,
    run_ml_baseline_training,
    spark_driver_memory,
    spark_executor_memory,
    spark_conf,
    spark_master,
    spark_packages,
    validate_runtime_config,
    variable_bool,
)

RUN_BASELINE_TRAINING_DEFAULT = variable_bool("TF2_AIRFLOW_RUN_BASELINE_TRAINING_DEFAULT", False)


def should_run_baseline_training(**context) -> bool:
    return dag_conf_bool(context, "run_baseline_training", RUN_BASELINE_TRAINING_DEFAULT)


with DAG(
    dag_id="tf2_ml_daily_or_weekly",
    description="Run ML refresh plus readiness checks and optional baseline training.",
    default_args=DEFAULT_ARGS,
    start_date=START_DATE,
    schedule=TF2_AIRFLOW_ML_CRON,
    catchup=False,
    max_active_runs=1,
    tags=["tf2", "ml"],
) as dag:
    validate_config = PythonOperator(
        task_id="validate_runtime_config",
        python_callable=validate_runtime_config,
        retries=0,
    )

    spark_ml_incremental = SparkSubmitOperator(
        task_id="spark_ml_incremental",
        conn_id="spark_default",
        application=SPARK_APPLICATION,
        application_args=["--mode", "incremental", "--refresh-days", REFRESH_DAYS, "--pipeline", "ml"],
        packages=spark_packages(),
        conf=spark_conf(),
        executor_memory=spark_executor_memory(),
        driver_memory=spark_driver_memory(),
        env_vars={"SPARK_MASTER": spark_master()},
        execution_timeout=PIPELINE_TIMEOUT,
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

    maybe_run_baseline_training = ShortCircuitOperator(
        task_id="maybe_run_baseline_training",
        python_callable=should_run_baseline_training,
    )

    ml_baseline_training = PythonOperator(
        task_id="ml_baseline_training",
        python_callable=run_ml_baseline_training,
        execution_timeout=PIPELINE_TIMEOUT,
    )

    validate_config >> spark_ml_incremental >> ml_readiness_checks_sql >> ml_readiness_checks_assert
    ml_readiness_checks_assert >> maybe_run_baseline_training >> ml_baseline_training
