from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.trino.hooks.trino import TrinoHook

TF2_REPO_ROOT = Variable.get("TF2_REPO_ROOT", default_var="/workspace")
TF2_TRINO_CONN_ID = Variable.get("TF2_TRINO_CONN_ID", default_var="trino_default")
TF2_TRINO_CATALOG = Variable.get("TF2_TRINO_CATALOG", default_var="tf2")
TF2_TRINO_SCHEMA = Variable.get("TF2_TRINO_SCHEMA", default_var="default")

TF2_AIRFLOW_FEATURE_CRON = Variable.get("TF2_AIRFLOW_FEATURE_CRON", default_var="15 * * * *")
TF2_AIRFLOW_ML_CRON = Variable.get("TF2_AIRFLOW_ML_CRON", default_var="0 4 * * *")
TF2_AIRFLOW_E2E_CRON = Variable.get("TF2_AIRFLOW_E2E_CRON", default_var="30 2 * * *")
TF2_AIRFLOW_MAINTENANCE_CRON = Variable.get("TF2_AIRFLOW_MAINTENANCE_CRON", default_var="0 5 * * 0")

REFRESH_DAYS = Variable.get("REFRESH_DAYS", default_var="7")
SPARK_NLP_VERSION = Variable.get("SPARK_NLP_VERSION", default_var="5.5.3")
SPARK_APPLICATION = f"{TF2_REPO_ROOT}/infra/spark/jobs/build_processing.py"
SPARK_BASE_PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "software.amazon.awssdk:bundle:2.20.160",
    "software.amazon.awssdk:url-connection-client:2.20.160",
    f"com.johnsnowlabs.nlp:spark-nlp_2.12:{SPARK_NLP_VERSION}",
]

START_DATE = datetime(2025, 1, 1)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

PIPELINE_TIMEOUT = timedelta(minutes=90)
CHECK_TIMEOUT = timedelta(minutes=30)

MAINTENANCE_TABLES = [
    "logs",
    "summaries",
    "messages",
    "ops_pipeline_runs",
    "features_player_match",
    "features_player_recent_form",
    "serving_player_profiles",
    "serving_map_overview_daily",
    "serving_player_match_deep_dive",
    "ml_training_dataset_snapshots",
    "ml_training_player_match",
    "ml_model_registry",
    "ml_model_stage_history",
    "serving_ml_model_registry",
    "serving_ml_pipeline_progress_daily",
    "serving_ml_prediction_quality_daily",
]

REQUIRED_RUNTIME_VARIABLES = [
    "CATALOG_URI",
    "WAREHOUSE",
    "R2_CATALOG_TOKEN",
    "R2_ENDPOINT",
    "R2_ACCESS_KEY_ID",
    "R2_SECRET_ACCESS_KEY",
]


def _required_variable(name: str) -> str:
    value = Variable.get(name, default_var="")
    if not value:
        raise AirflowException(f"Missing required Airflow Variable: {name}")
    return value


def _variable(name: str, default: str) -> str:
    return Variable.get(name, default_var=default)


def _parse_bool(value: object, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0

    lowered = str(value).strip().lower()
    if lowered in {"1", "true", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "no", "n", "off"}:
        return False
    return default


def variable_bool(name: str, default: bool) -> bool:
    return _parse_bool(Variable.get(name, default_var=str(default).lower()), default)


def env_bool(name: str, default: bool) -> bool:
    # Backwards-compatible alias for older DAG imports.
    return variable_bool(name, default)


def dag_conf_bool(context: dict, key: str, default: bool) -> bool:
    dag_run = context.get("dag_run")
    if dag_run is None or dag_run.conf is None:
        return default
    return _parse_bool(dag_run.conf.get(key), default)


def dag_conf_string(context: dict, key: str, default: str) -> str:
    dag_run = context.get("dag_run")
    if dag_run is None or dag_run.conf is None:
        return default

    value = dag_run.conf.get(key)
    if value is None:
        return default
    return str(value).strip()


def spark_conf() -> dict[str, str]:
    conf = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.shuffle.partitions": _variable("SPARK_SQL_SHUFFLE_PARTITIONS", "512"),
        "spark.default.parallelism": _variable("SPARK_DEFAULT_PARALLELISM", "256"),
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": "tf2",
        "spark.sql.catalog.tf2": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.tf2.type": "rest",
        "spark.sql.catalog.tf2.uri": _required_variable("CATALOG_URI"),
        "spark.sql.catalog.tf2.warehouse": _required_variable("WAREHOUSE"),
        "spark.sql.catalog.tf2.token": _required_variable("R2_CATALOG_TOKEN"),
        "spark.sql.catalog.tf2.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.iceberg.vectorization.enabled": _variable("SPARK_ICEBERG_VECTORIZATION_ENABLED", "false"),
        "spark.sql.parquet.enableVectorizedReader": _variable("SPARK_PARQUET_VECTORIZED_READER_ENABLED", "false"),
        "spark.sql.parquet.enableNestedColumnVectorizedReader": _variable(
            "SPARK_PARQUET_NESTED_VECTORIZED_READER_ENABLED", "false"
        ),
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.endpoint": _required_variable("R2_ENDPOINT"),
        "spark.hadoop.fs.s3a.access.key": _required_variable("R2_ACCESS_KEY_ID"),
        "spark.hadoop.fs.s3a.secret.key": _required_variable("R2_SECRET_ACCESS_KEY"),
        "spark.hadoop.fs.s3a.region": "auto",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
    }

    if variable_bool("OPENLINEAGE_SPARK_ENABLED", True):
        conf["spark.extraListeners"] = "io.openlineage.spark.agent.OpenLineageSparkListener"
        conf["spark.openlineage.namespace"] = _variable("OPENLINEAGE_SPARK_NAMESPACE", "tf2-spark")

    return conf


def spark_packages() -> str:
    packages = list(SPARK_BASE_PACKAGES)
    if variable_bool("OPENLINEAGE_SPARK_ENABLED", True):
        openlineage_package_version = _variable("OPENLINEAGE_SPARK_PACKAGE_VERSION", "1.36.0")
        packages.append(f"io.openlineage:openlineage-spark_2.12:{openlineage_package_version}")
    return ",".join(packages)


def spark_master() -> str:
    return _variable("SPARK_MASTER", "local[4]")


def spark_driver_memory() -> str:
    return _variable("SPARK_DRIVER_MEMORY", "6g")


def spark_executor_memory() -> str:
    return _variable("SPARK_EXECUTOR_MEMORY", "6g")


def _is_unset_runtime_value(value: str) -> bool:
    cleaned = value.strip()
    if not cleaned:
        return True
    return cleaned.startswith("<") and cleaned.endswith(">")


def validate_runtime_config() -> None:
    missing_or_placeholder = [
        name
        for name in REQUIRED_RUNTIME_VARIABLES
        if _is_unset_runtime_value(Variable.get(name, default_var=""))
    ]
    if missing_or_placeholder:
        missing_str = ", ".join(sorted(missing_or_placeholder))
        raise AirflowException(
            f"Missing or placeholder Airflow Variables: {missing_str}. "
            "Set real values in Airflow Variables (Admin > Variables) and re-run."
        )


def load_sql(relative_path: str) -> str:
    sql_path = Path(TF2_REPO_ROOT) / relative_path
    return sql_path.read_text(encoding="utf-8")


def _contains_fail_token(payload: object) -> bool:
    if payload is None:
        return False
    if isinstance(payload, (list, tuple)):
        return any(_contains_fail_token(item) for item in payload)
    return str(payload).strip().upper() == "FAIL"


def assert_sql_task_has_no_failures(sql_task_id: str, **context) -> None:
    ti = context["ti"]
    records = ti.xcom_pull(task_ids=sql_task_id)
    if _contains_fail_token(records):
        raise AirflowException(f"{sql_task_id} returned one or more FAIL statuses")


def maintenance_statements() -> list[str]:
    retention = _variable("TF2_MAINTENANCE_RETENTION", "14d")
    threshold = _variable("TF2_MAINTENANCE_OPTIMIZE_THRESHOLD", "256MB")

    statements: list[str] = []
    for table_name in MAINTENANCE_TABLES:
        fqtn = f"{TF2_TRINO_CATALOG}.{TF2_TRINO_SCHEMA}.{table_name}"
        statements.append(
            f"ALTER TABLE {fqtn} EXECUTE optimize(file_size_threshold => '{threshold}')"
        )
        statements.append(
            f"ALTER TABLE {fqtn} EXECUTE expire_snapshots(retention_threshold => '{retention}')"
        )

    return statements


def run_iceberg_maintenance() -> None:
    retention = _variable("TF2_MAINTENANCE_RETENTION", "14d")
    threshold = _variable("TF2_MAINTENANCE_OPTIMIZE_THRESHOLD", "256MB")
    hook = TrinoHook(trino_conn_id=TF2_TRINO_CONN_ID)
    logger = logging.getLogger(__name__)

    for table_name in MAINTENANCE_TABLES:
        table_check_sql = (
            f"SELECT COUNT(*) "
            f"FROM {TF2_TRINO_CATALOG}.information_schema.tables "
            f"WHERE table_schema = '{TF2_TRINO_SCHEMA}' "
            f"AND table_name = '{table_name}'"
        )
        rows = hook.get_records(table_check_sql)
        table_exists = bool(rows and rows[0] and rows[0][0] != 0)
        fqtn = f"{TF2_TRINO_CATALOG}.{TF2_TRINO_SCHEMA}.{table_name}"

        if not table_exists:
            logger.info("Skipping %s (table not found)", fqtn)
            continue

        logger.info("Running Iceberg maintenance for %s", fqtn)
        hook.run(f"ALTER TABLE {fqtn} EXECUTE optimize(file_size_threshold => '{threshold}')")
        hook.run(f"ALTER TABLE {fqtn} EXECUTE expire_snapshots(retention_threshold => '{retention}')")


def run_ml_baseline_training(**context) -> None:
    import sys

    dag_run = context.get("dag_run")
    dag_conf = dag_run.conf if dag_run and dag_run.conf else {}

    model_version = str(dag_conf.get("model_version", _variable("MODEL_VERSION", "v1.0.0")))
    snapshot_id = str(dag_conf.get("snapshot_id", Variable.get("SNAPSHOT_ID", default_var=""))).strip()

    training_code_version = _variable("TRAINING_CODE_VERSION", "unknown")
    feature_sql_version = _variable("FEATURE_SQL_VERSION", training_code_version)

    trino_conn = BaseHook.get_connection(TF2_TRINO_CONN_ID)
    trino_host = trino_conn.host or "tf2-trino"
    trino_port = str(trino_conn.port or 8080)
    trino_user = trino_conn.login or "airflow"
    trino_schema = trino_conn.schema or TF2_TRINO_SCHEMA
    trino_extra = trino_conn.extra_dejson or {}
    trino_catalog = trino_extra.get("catalog", TF2_TRINO_CATALOG)
    trino_http_scheme = trino_extra.get("protocol", _variable("TRINO_HTTP_SCHEME", "http"))

    trainer_args = [
        "--trino-host",
        trino_host,
        "--trino-port",
        trino_port,
        "--trino-user",
        trino_user,
        "--trino-catalog",
        trino_catalog,
        "--trino-schema",
        trino_schema,
        "--trino-http-scheme",
        trino_http_scheme,
        "--model-version",
        model_version,
        "--train-ratio",
        _variable("TRAIN_RATIO", "0.8"),
        "--artifact-root",
        _variable("ARTIFACT_ROOT", "artifacts/ml"),
        "--report-path",
        _variable("ML_REPORT_PATH", "docs/ml-offline-evaluation-report.md"),
        "--training-code-version",
        training_code_version,
        "--feature-sql-version",
        feature_sql_version,
        "--min-fold-train-rows",
        _variable("MIN_FOLD_TRAIN_ROWS", "20000"),
        "--min-fold-val-rows",
        _variable("MIN_FOLD_VAL_ROWS", "5000"),
        "--win-policy-min-precision",
        _variable("WIN_POLICY_MIN_PRECISION", "0.60"),
        "--win-policy-min-recall",
        _variable("WIN_POLICY_MIN_RECALL", "0.55"),
        "--tilt-policy-min-precision",
        _variable("TILT_POLICY_MIN_PRECISION", "0.75"),
        "--tilt-policy-min-recall",
        _variable("TILT_POLICY_MIN_RECALL", "0.95"),
        "--gate-win-min-f1",
        _variable("GATE_WIN_MIN_F1", "0.66"),
        "--gate-win-max-brier",
        _variable("GATE_WIN_MAX_BRIER", "0.20"),
        "--gate-win-min-fold-f1",
        _variable("GATE_WIN_MIN_FOLD_F1", "0.60"),
        "--gate-impact-max-rmse",
        _variable("GATE_IMPACT_MAX_RMSE", "20.00"),
        "--gate-impact-max-mae",
        _variable("GATE_IMPACT_MAX_MAE", "16.00"),
        "--gate-impact-max-fold-rmse",
        _variable("GATE_IMPACT_MAX_FOLD_RMSE", "22.00"),
        "--gate-tilt-min-f1",
        _variable("GATE_TILT_MIN_F1", "0.85"),
        "--gate-tilt-max-brier",
        _variable("GATE_TILT_MAX_BRIER", "0.02"),
        "--gate-tilt-min-recall",
        _variable("GATE_TILT_MIN_RECALL", "0.95"),
        "--gate-tilt-max-fold-f1-std",
        _variable("GATE_TILT_MAX_FOLD_F1_STD", "0.03"),
    ]
    if snapshot_id:
        trainer_args.extend(["--snapshot-id", snapshot_id])

    repo_root = str(Path(TF2_REPO_ROOT).resolve())
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    try:
        from infra.ml import train_baselines
    except ModuleNotFoundError as exc:
        raise AirflowException(
            "Could not import infra.ml.train_baselines from TF2_REPO_ROOT. "
            "Set Airflow Variable TF2_REPO_ROOT to the checked-out repository path."
        ) from exc

    exit_code = train_baselines.main(trainer_args)
    if exit_code != 0:
        raise AirflowException(f"ML baseline training failed with exit code {exit_code}")
