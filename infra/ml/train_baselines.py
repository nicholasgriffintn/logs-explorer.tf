#!/usr/bin/env python3
"""Train baseline models from the latest ML snapshot and publish rich reports.

This trainer is designed to run inside a dedicated Docker image with managed
ML dependencies. It loads snapshot data from Trino, trains regularised sklearn
pipelines, writes model artefacts, emits a detailed markdown report, and
registers candidate versions in tf2.default.ml_model_registry.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    confusion_matrix,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from trino.dbapi import connect


def sql_escape(value: str) -> str:
    return value.replace("'", "''")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_now_human() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


@dataclass
class TrinoConfig:
    host: str
    port: int
    user: str
    catalog: str
    schema: str
    http_scheme: str


class TrinoClient:
    def __init__(self, cfg: TrinoConfig) -> None:
        self._conn = connect(
            host=cfg.host,
            port=cfg.port,
            user=cfg.user,
            catalog=cfg.catalog,
            schema=cfg.schema,
            http_scheme=cfg.http_scheme,
        )

    def fetch_df(self, sql: str) -> pd.DataFrame:
        cur = self._conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        return pd.DataFrame(rows, columns=cols)

    def execute(self, sql: str) -> None:
        cur = self._conn.cursor()
        cur.execute(sql)


def latest_snapshot_id(trino: TrinoClient) -> str:
    df = trino.fetch_df(
        """
SELECT snapshot_id
FROM tf2.default.ml_training_dataset_snapshots
ORDER BY created_at DESC
LIMIT 1
""".strip()
    )
    if df.empty:
        raise RuntimeError("No snapshot found in tf2.default.ml_training_dataset_snapshots")
    return str(df.iloc[0]["snapshot_id"])


def load_snapshot_rows(trino: TrinoClient, snapshot_id: str) -> pd.DataFrame:
    sql = f"""
SELECT
  snapshot_id,
  steamid,
  match_time,
  match_date,
  map,
  team,
  momentum_label,
  duration_seconds,
  team_score,
  opponent_score,
  score_delta,
  kills,
  assists,
  deaths,
  damage_dealt,
  healing_done,
  ubers_used,
  classes_played_count,
  kill_share_of_team,
  damage_share_of_team,
  healing_share_of_team,
  impact_index,
  damage_per_minute,
  kda_ratio,
  chat_messages,
  avg_message_length,
  all_caps_messages,
  intense_punctuation_messages,
  negative_lexicon_hits,
  negative_chat_ratio,
  rolling_5_avg_kills,
  rolling_10_avg_damage,
  rolling_10_avg_impact,
  rolling_10_kda_ratio,
  rolling_10_win_rate,
  rolling_10_negative_chat_ratio,
  career_avg_kills,
  career_avg_damage,
  career_avg_impact,
  form_delta_kills,
  form_delta_damage,
  form_delta_impact,
  games_played_to_date,
  label_win,
  label_impact_percentile,
  label_tilt
FROM tf2.default.ml_training_player_match
WHERE snapshot_id = '{sql_escape(snapshot_id)}'
ORDER BY match_time
""".strip()
    df = trino.fetch_df(sql)
    if df.empty:
        raise RuntimeError(f"No rows found for snapshot_id={snapshot_id}")
    df["match_time"] = pd.to_datetime(df["match_time"], utc=True)
    return df


def split_time(df: pd.DataFrame, train_ratio: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    ordered = df.sort_values("match_time").reset_index(drop=True)
    split_idx = max(1, min(len(ordered) - 1, int(len(ordered) * train_ratio)))
    return ordered.iloc[:split_idx].copy(), ordered.iloc[split_idx:].copy()


def build_preprocessor(numeric_cols: Sequence[str], categorical_cols: Sequence[str]) -> ColumnTransformer:
    numeric = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    categorical = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )
    return ColumnTransformer(
        transformers=[
            ("num", numeric, list(numeric_cols)),
            ("cat", categorical, list(categorical_cols)),
        ],
        remainder="drop",
    )


def train_win_model(
    x_train: pd.DataFrame,
    y_train: pd.Series,
    numeric_cols: Sequence[str],
    categorical_cols: Sequence[str],
) -> Pipeline:
    return Pipeline(
        steps=[
            ("prep", build_preprocessor(numeric_cols, categorical_cols)),
            (
                "model",
                LogisticRegression(
                    C=0.25,  # stronger regularisation than default
                    max_iter=250,
                    solver="lbfgs",
                ),
            ),
        ]
    ).fit(x_train, y_train)


def train_tilt_model(
    x_train: pd.DataFrame,
    y_train: pd.Series,
    numeric_cols: Sequence[str],
    categorical_cols: Sequence[str],
) -> Pipeline:
    return Pipeline(
        steps=[
            ("prep", build_preprocessor(numeric_cols, categorical_cols)),
            (
                "model",
                LogisticRegression(
                    C=0.15,  # stronger regularisation + class balancing
                    max_iter=300,
                    solver="lbfgs",
                    class_weight="balanced",
                ),
            ),
        ]
    ).fit(x_train, y_train)


def train_impact_model(
    x_train: pd.DataFrame,
    y_train: pd.Series,
    numeric_cols: Sequence[str],
    categorical_cols: Sequence[str],
) -> Pipeline:
    return Pipeline(
        steps=[
            ("prep", build_preprocessor(numeric_cols, categorical_cols)),
            ("model", Ridge(alpha=40.0)),
        ]
    ).fit(x_train, y_train)


def binary_metrics(y_true: np.ndarray, probs: np.ndarray, threshold: float = 0.5) -> Dict[str, float]:
    preds = (probs >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, preds, labels=[0, 1]).ravel()
    return {
        "precision": float(precision_score(y_true, preds, zero_division=0)),
        "recall": float(recall_score(y_true, preds, zero_division=0)),
        "f1": float(f1_score(y_true, preds, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_true, probs)),
        "pr_auc": float(average_precision_score(y_true, probs)),
        "brier": float(brier_score_loss(y_true, probs)),
        "true_negatives": float(tn),
        "false_positives": float(fp),
        "false_negatives": float(fn),
        "true_positives": float(tp),
        "positive_rate": float(np.mean(y_true)),
    }


def regression_metrics(y_true: np.ndarray, preds: np.ndarray) -> Dict[str, float]:
    return {
        "rmse": float(np.sqrt(mean_squared_error(y_true, preds))),
        "mae": float(mean_absolute_error(y_true, preds)),
    }


def threshold_table(y_true: np.ndarray, probs: np.ndarray, thresholds: Sequence[float]) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    for t in thresholds:
        preds = (probs >= t).astype(int)
        rows.append(
            {
                "threshold": float(t),
                "precision": float(precision_score(y_true, preds, zero_division=0)),
                "recall": float(recall_score(y_true, preds, zero_division=0)),
                "f1": float(f1_score(y_true, preds, zero_division=0)),
                "predicted_positive_rate": float(np.mean(preds)),
            }
        )
    return rows


def calibration_table(y_true: np.ndarray, probs: np.ndarray, bins: int = 10) -> List[Dict[str, float]]:
    df = pd.DataFrame({"y_true": y_true, "probs": probs})
    df["bin"] = pd.qcut(df["probs"], q=min(bins, len(df)), labels=False, duplicates="drop")
    summary = (
        df.groupby("bin", dropna=True)
        .agg(
            rows=("y_true", "count"),
            avg_predicted=("probs", "mean"),
            avg_observed=("y_true", "mean"),
            min_prob=("probs", "min"),
            max_prob=("probs", "max"),
        )
        .reset_index()
    )
    out: List[Dict[str, float]] = []
    for _, row in summary.iterrows():
        out.append(
            {
                "bin": float(row["bin"]),
                "rows": float(row["rows"]),
                "avg_predicted": float(row["avg_predicted"]),
                "avg_observed": float(row["avg_observed"]),
                "min_prob": float(row["min_prob"]),
                "max_prob": float(row["max_prob"]),
            }
        )
    return out


def top_coefficients(model: Pipeline, top_n: int = 12) -> Tuple[List[Tuple[str, float]], List[Tuple[str, float]]]:
    prep: ColumnTransformer = model.named_steps["prep"]
    estimator = model.named_steps["model"]
    names = prep.get_feature_names_out()
    coef = estimator.coef_
    if isinstance(coef, np.ndarray) and coef.ndim > 1:
        coef = coef[0]
    coef = np.asarray(coef, dtype=float)
    pairs = list(zip(names.tolist(), coef.tolist()))
    pairs_sorted = sorted(pairs, key=lambda x: x[1], reverse=True)
    return pairs_sorted[:top_n], sorted(pairs, key=lambda x: x[1])[:top_n]


def ensure_dir(path: pathlib.Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_json(path: pathlib.Path, payload: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def register_model(
    trino: TrinoClient,
    model_name: str,
    model_version: str,
    task_type: str,
    snapshot_id: str,
    training_code_version: str,
    feature_sql_version: str,
    artifact_uri: str,
    metrics: Dict[str, float],
    calibration_notes: str,
) -> None:
    metrics_json = json.dumps(metrics, sort_keys=True)
    delete_sql = f"""
DELETE FROM tf2.default.ml_model_registry
WHERE model_name = '{sql_escape(model_name)}'
  AND model_version = '{sql_escape(model_version)}'
""".strip()
    insert_sql = f"""
INSERT INTO tf2.default.ml_model_registry (
  model_name,
  model_version,
  task_type,
  stage,
  snapshot_id,
  training_code_version,
  feature_sql_version,
  artifact_uri,
  metrics_json,
  calibration_notes,
  created_at,
  promoted_at,
  is_active
)
VALUES (
  '{sql_escape(model_name)}',
  '{sql_escape(model_version)}',
  '{sql_escape(task_type)}',
  'candidate',
  '{sql_escape(snapshot_id)}',
  '{sql_escape(training_code_version)}',
  '{sql_escape(feature_sql_version)}',
  '{sql_escape(artifact_uri)}',
  '{sql_escape(metrics_json)}',
  '{sql_escape(calibration_notes)}',
  CURRENT_TIMESTAMP,
  NULL,
  FALSE
)
""".strip()
    trino.execute(delete_sql)
    trino.execute(insert_sql)


def format_table(rows: List[Dict[str, Any]], columns: Sequence[str]) -> List[str]:
    lines = []
    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join(["---"] * len(columns)) + " |"
    lines.append(header)
    lines.append(divider)
    for row in rows:
        vals = []
        for col in columns:
            v = row.get(col, "")
            if isinstance(v, float):
                vals.append(f"{v:.4f}")
            else:
                vals.append(str(v))
        lines.append("| " + " | ".join(vals) + " |")
    return lines


def write_report(
    path: pathlib.Path,
    snapshot_id: str,
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    metric_by_model: Dict[str, Dict[str, float]],
    win_threshold_rows: List[Dict[str, float]],
    tilt_threshold_rows: List[Dict[str, float]],
    win_calibration_rows: List[Dict[str, float]],
    tilt_calibration_rows: List[Dict[str, float]],
    win_top_pos: List[Tuple[str, float]],
    win_top_neg: List[Tuple[str, float]],
    tilt_top_pos: List[Tuple[str, float]],
    tilt_top_neg: List[Tuple[str, float]],
    impact_top_pos: List[Tuple[str, float]],
    impact_top_neg: List[Tuple[str, float]],
) -> None:
    lines: List[str] = []
    lines.append("# ML offline evaluation report")
    lines.append("")
    lines.append(f"- Generated at: `{utc_now_human()}`")
    lines.append(f"- Snapshot ID: `{snapshot_id}`")
    lines.append(f"- Train rows: `{len(train_df)}`")
    lines.append(f"- Validation rows: `{len(val_df)}`")
    lines.append(f"- Validation date range: `{val_df['match_time'].min()}` to `{val_df['match_time'].max()}`")
    lines.append("")
    lines.append("## Dataset profile")
    lines.append("")
    dataset_rows = [
        {"metric": "players_total", "value": float(val_df["steamid"].nunique())},
        {"metric": "distinct_maps", "value": float(val_df["map"].nunique())},
        {"metric": "distinct_match_days", "value": float(val_df["match_date"].nunique())},
        {"metric": "label_win_positive_rate", "value": float(val_df["label_win"].mean())},
        {"metric": "label_tilt_positive_rate", "value": float(val_df["label_tilt"].mean())},
    ]
    lines.extend(format_table(dataset_rows, ["metric", "value"]))
    lines.append("")
    lines.append("## Model summary")
    lines.append("")
    summary_rows = [
        {
            "model": "win_probability_baseline",
            "precision": metric_by_model["win_probability_baseline"]["precision"],
            "recall": metric_by_model["win_probability_baseline"]["recall"],
            "f1": metric_by_model["win_probability_baseline"]["f1"],
            "roc_auc": metric_by_model["win_probability_baseline"]["roc_auc"],
            "pr_auc": metric_by_model["win_probability_baseline"]["pr_auc"],
            "brier": metric_by_model["win_probability_baseline"]["brier"],
        },
        {
            "model": "tilt_risk_baseline",
            "precision": metric_by_model["tilt_risk_baseline"]["precision"],
            "recall": metric_by_model["tilt_risk_baseline"]["recall"],
            "f1": metric_by_model["tilt_risk_baseline"]["f1"],
            "roc_auc": metric_by_model["tilt_risk_baseline"]["roc_auc"],
            "pr_auc": metric_by_model["tilt_risk_baseline"]["pr_auc"],
            "brier": metric_by_model["tilt_risk_baseline"]["brier"],
        },
    ]
    lines.extend(format_table(summary_rows, ["model", "precision", "recall", "f1", "roc_auc", "pr_auc", "brier"]))
    lines.append("")
    impact_rows = [
        {
            "model": "impact_percentile_baseline",
            "rmse": metric_by_model["impact_percentile_baseline"]["rmse"],
            "mae": metric_by_model["impact_percentile_baseline"]["mae"],
        }
    ]
    lines.extend(format_table(impact_rows, ["model", "rmse", "mae"]))
    lines.append("")
    lines.append("## Threshold trade-offs")
    lines.append("")
    lines.append("### Win model")
    lines.append("")
    lines.extend(format_table(win_threshold_rows, ["threshold", "precision", "recall", "f1", "predicted_positive_rate"]))
    lines.append("")
    lines.append("### Tilt model")
    lines.append("")
    lines.extend(format_table(tilt_threshold_rows, ["threshold", "precision", "recall", "f1", "predicted_positive_rate"]))
    lines.append("")
    lines.append("## Calibration tables")
    lines.append("")
    lines.append("### Win model")
    lines.append("")
    lines.extend(format_table(win_calibration_rows, ["bin", "rows", "min_prob", "max_prob", "avg_predicted", "avg_observed"]))
    lines.append("")
    lines.append("### Tilt model")
    lines.append("")
    lines.extend(format_table(tilt_calibration_rows, ["bin", "rows", "min_prob", "max_prob", "avg_predicted", "avg_observed"]))
    lines.append("")
    lines.append("## Top coefficients")
    lines.append("")

    def coef_rows(pairs: List[Tuple[str, float]]) -> List[Dict[str, Any]]:
        return [{"feature": f, "weight": w} for f, w in pairs]

    lines.append("### Win positive weights")
    lines.extend(format_table(coef_rows(win_top_pos), ["feature", "weight"]))
    lines.append("")
    lines.append("### Win negative weights")
    lines.extend(format_table(coef_rows(win_top_neg), ["feature", "weight"]))
    lines.append("")
    lines.append("### Tilt positive weights")
    lines.extend(format_table(coef_rows(tilt_top_pos), ["feature", "weight"]))
    lines.append("")
    lines.append("### Tilt negative weights")
    lines.extend(format_table(coef_rows(tilt_top_neg), ["feature", "weight"]))
    lines.append("")
    lines.append("### Impact positive weights")
    lines.extend(format_table(coef_rows(impact_top_pos), ["feature", "weight"]))
    lines.append("")
    lines.append("### Impact negative weights")
    lines.extend(format_table(coef_rows(impact_top_neg), ["feature", "weight"]))
    lines.append("")
    lines.append("## Calibration notes")
    lines.append("")
    lines.append(
        f"- Win Brier score: `{metric_by_model['win_probability_baseline']['brier']:.4f}`. "
        "If production thresholding matters, tune threshold against business costs."
    )
    lines.append(
        f"- Tilt Brier score: `{metric_by_model['tilt_risk_baseline']['brier']:.4f}`. "
        "Class imbalance is handled with `class_weight=balanced`; calibrate with isotonic regression before production promotion."
    )
    lines.append(
        "- Impact RMSE/MAE are baseline quality only; assess residuals by map and class usage slices before staging promotion."
    )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Train baseline ML models and register candidates.")
    parser.add_argument("--trino-host", default=os.environ.get("TRINO_HOST", "tf2-trino"))
    parser.add_argument("--trino-port", type=int, default=int(os.environ.get("TRINO_PORT", "8080")))
    parser.add_argument("--trino-user", default=os.environ.get("TRINO_USER", "ml_trainer"))
    parser.add_argument("--trino-catalog", default=os.environ.get("TRINO_CATALOG", "tf2"))
    parser.add_argument("--trino-schema", default=os.environ.get("TRINO_SCHEMA", "default"))
    parser.add_argument("--trino-http-scheme", default=os.environ.get("TRINO_HTTP_SCHEME", "http"))
    parser.add_argument("--model-version", default=os.environ.get("MODEL_VERSION", "v1.0.0"))
    parser.add_argument("--snapshot-id", default=os.environ.get("SNAPSHOT_ID"))
    parser.add_argument("--train-ratio", type=float, default=float(os.environ.get("TRAIN_RATIO", "0.8")))
    parser.add_argument("--artifact-root", default=os.environ.get("ARTIFACT_ROOT", "artifacts/ml"))
    parser.add_argument("--report-path", default=os.environ.get("ML_REPORT_PATH", "docs/ml-offline-evaluation-report.md"))
    parser.add_argument("--training-code-version", default=os.environ.get("TRAINING_CODE_VERSION", "unknown"))
    parser.add_argument("--feature-sql-version", default=os.environ.get("FEATURE_SQL_VERSION", "unknown"))
    args = parser.parse_args()

    trino_cfg = TrinoConfig(
        host=args.trino_host,
        port=args.trino_port,
        user=args.trino_user,
        catalog=args.trino_catalog,
        schema=args.trino_schema,
        http_scheme=args.trino_http_scheme,
    )
    trino = TrinoClient(trino_cfg)

    requested_snapshot = (args.snapshot_id or "").strip()
    snapshot_id = requested_snapshot if requested_snapshot else latest_snapshot_id(trino)
    df = load_snapshot_rows(trino, snapshot_id)
    train_df, val_df = split_time(df, args.train_ratio)
    if len(train_df) < 1000 or len(val_df) < 200:
        raise RuntimeError(f"Insufficient rows after split: train={len(train_df)} val={len(val_df)}")

    common_numeric = [
        "duration_seconds",
        "team_score",
        "opponent_score",
        "score_delta",
        "kills",
        "assists",
        "deaths",
        "damage_dealt",
        "healing_done",
        "ubers_used",
        "classes_played_count",
        "kill_share_of_team",
        "damage_share_of_team",
        "healing_share_of_team",
        "impact_index",
        "damage_per_minute",
        "kda_ratio",
        "rolling_5_avg_kills",
        "rolling_10_avg_damage",
        "rolling_10_avg_impact",
        "rolling_10_kda_ratio",
        "rolling_10_win_rate",
        "career_avg_kills",
        "career_avg_damage",
        "career_avg_impact",
        "form_delta_kills",
        "form_delta_damage",
        "form_delta_impact",
        "games_played_to_date",
    ]
    common_cat = ["map", "team", "momentum_label"]

    tilt_numeric = [
        "deaths",
        "impact_index",
        "score_delta",
        "chat_messages",
        "avg_message_length",
        "all_caps_messages",
        "intense_punctuation_messages",
        "negative_lexicon_hits",
        "negative_chat_ratio",
        "rolling_10_negative_chat_ratio",
        "rolling_10_win_rate",
        "form_delta_impact",
        "games_played_to_date",
    ]
    tilt_cat = ["map", "team", "momentum_label"]

    x_train_common = train_df[common_numeric + common_cat]
    x_val_common = val_df[common_numeric + common_cat]
    x_train_tilt = train_df[tilt_numeric + tilt_cat]
    x_val_tilt = val_df[tilt_numeric + tilt_cat]

    y_train_win = train_df["label_win"].astype(int)
    y_val_win = val_df["label_win"].astype(int)
    y_train_tilt = train_df["label_tilt"].astype(int)
    y_val_tilt = val_df["label_tilt"].astype(int)
    y_train_impact = train_df["label_impact_percentile"].astype(float)
    y_val_impact = val_df["label_impact_percentile"].astype(float)

    win_model = train_win_model(x_train_common, y_train_win, common_numeric, common_cat)
    tilt_model = train_tilt_model(x_train_tilt, y_train_tilt, tilt_numeric, tilt_cat)
    impact_model = train_impact_model(x_train_common, y_train_impact, common_numeric, common_cat)

    win_probs = win_model.predict_proba(x_val_common)[:, 1]
    tilt_probs = tilt_model.predict_proba(x_val_tilt)[:, 1]
    impact_preds = np.clip(impact_model.predict(x_val_common), 1.0, 100.0)

    win_metrics = binary_metrics(y_val_win.to_numpy(), win_probs)
    tilt_metrics = binary_metrics(y_val_tilt.to_numpy(), tilt_probs)
    impact_metrics = regression_metrics(y_val_impact.to_numpy(), impact_preds)

    win_threshold_rows = threshold_table(y_val_win.to_numpy(), win_probs, [0.3, 0.4, 0.5, 0.6, 0.7])
    tilt_threshold_rows = threshold_table(y_val_tilt.to_numpy(), tilt_probs, [0.2, 0.3, 0.4, 0.5, 0.6])
    win_calibration_rows = calibration_table(y_val_win.to_numpy(), win_probs, bins=10)
    tilt_calibration_rows = calibration_table(y_val_tilt.to_numpy(), tilt_probs, bins=10)

    win_top_pos, win_top_neg = top_coefficients(win_model)
    tilt_top_pos, tilt_top_neg = top_coefficients(tilt_model)
    impact_top_pos, impact_top_neg = top_coefficients(impact_model)

    artifact_root = pathlib.Path(args.artifact_root).resolve()
    report_path = pathlib.Path(args.report_path).resolve()
    metric_by_model: Dict[str, Dict[str, float]] = {
        "win_probability_baseline": win_metrics,
        "impact_percentile_baseline": impact_metrics,
        "tilt_risk_baseline": tilt_metrics,
    }

    model_bundle = [
        ("win_probability_baseline", win_model, "classification", win_metrics, "Win calibration included in report."),
        ("impact_percentile_baseline", impact_model, "regression", impact_metrics, "Ridge baseline with strong regularisation."),
        (
            "tilt_risk_baseline",
            tilt_model,
            "classification",
            tilt_metrics,
            "Trained with class_weight=balanced; calibration table included in report.",
        ),
    ]

    trained_at = utc_now_iso()
    for model_name, model_obj, task_type, metrics, notes in model_bundle:
        model_dir = artifact_root / snapshot_id / model_name / args.model_version
        ensure_dir(model_dir)
        model_path = model_dir / "model.joblib"
        metadata_path = model_dir / "metadata.json"
        joblib.dump(model_obj, model_path)
        write_json(
            metadata_path,
            {
                "model_name": model_name,
                "model_version": args.model_version,
                "snapshot_id": snapshot_id,
                "task_type": task_type,
                "trained_at": trained_at,
                "metrics": metrics,
                "train_rows": int(len(train_df)),
                "validation_rows": int(len(val_df)),
            },
        )
        register_model(
            trino=trino,
            model_name=model_name,
            model_version=args.model_version,
            task_type=task_type,
            snapshot_id=snapshot_id,
            training_code_version=args.training_code_version,
            feature_sql_version=args.feature_sql_version,
            artifact_uri=str(model_path),
            metrics=metrics,
            calibration_notes=notes,
        )

    write_report(
        path=report_path,
        snapshot_id=snapshot_id,
        train_df=train_df,
        val_df=val_df,
        metric_by_model=metric_by_model,
        win_threshold_rows=win_threshold_rows,
        tilt_threshold_rows=tilt_threshold_rows,
        win_calibration_rows=win_calibration_rows,
        tilt_calibration_rows=tilt_calibration_rows,
        win_top_pos=win_top_pos,
        win_top_neg=win_top_neg,
        tilt_top_pos=tilt_top_pos,
        tilt_top_neg=tilt_top_neg,
        impact_top_pos=impact_top_pos,
        impact_top_neg=impact_top_neg,
    )

    print(f"Snapshot: {snapshot_id}")
    print(f"Train rows: {len(train_df)} | Validation rows: {len(val_df)}")
    print(
        "Win metrics:",
        json.dumps(
            {k: round(v, 6) for k, v in win_metrics.items() if isinstance(v, float)},
            sort_keys=True,
        ),
    )
    print(
        "Impact metrics:",
        json.dumps({k: round(v, 6) for k, v in impact_metrics.items()}, sort_keys=True),
    )
    print(
        "Tilt metrics:",
        json.dumps(
            {k: round(v, 6) for k, v in tilt_metrics.items() if isinstance(v, float)},
            sort_keys=True,
        ),
    )
    print(f"Report: {report_path}")
    print(f"Artifacts root: {artifact_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
