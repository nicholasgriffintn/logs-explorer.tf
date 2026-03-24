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
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Sequence, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.calibration import CalibratedClassifierCV
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
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
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, StandardScaler
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


@dataclass
class TemporalFold:
    name: str
    train_df: pd.DataFrame
    val_df: pd.DataFrame


@dataclass
class ThresholdPolicy:
    threshold: float
    min_precision: float
    min_recall: float
    selected_by_constraints: bool
    precision: float
    recall: float
    f1: float


class FeatureQualityTransformer(BaseEstimator, TransformerMixin):
    """Reduce noisy cardinality and cap numeric outliers using train-only stats."""

    def __init__(
        self,
        numeric_cols: Sequence[str],
        map_col: str = "map",
        map_min_frequency: int = 100,
        lower_quantile: float = 0.01,
        upper_quantile: float = 0.99,
    ) -> None:
        self.numeric_cols = numeric_cols
        self.map_col = map_col
        self.map_min_frequency = map_min_frequency
        self.lower_quantile = lower_quantile
        self.upper_quantile = upper_quantile
        self.frequent_maps_: set[str] = set()
        self.numeric_bounds_: Dict[str, Tuple[float, float]] = {}

    def fit(self, x: pd.DataFrame, y: Any = None) -> "FeatureQualityTransformer":
        df = x.copy()
        if self.map_col in df.columns:
            map_values = df[self.map_col].where(pd.notna(df[self.map_col]), "__MISSING__").astype(str)
            counts = map_values.value_counts(dropna=False)
            self.frequent_maps_ = set(counts[counts >= self.map_min_frequency].index.tolist())

        bounds: Dict[str, Tuple[float, float]] = {}
        for col in list(self.numeric_cols):
            if col not in df.columns:
                continue
            values = pd.to_numeric(df[col], errors="coerce")
            if values.notna().sum() == 0:
                continue
            lower = float(values.quantile(self.lower_quantile))
            upper = float(values.quantile(self.upper_quantile))
            if lower > upper:
                lower, upper = upper, lower
            bounds[col] = (lower, upper)
        self.numeric_bounds_ = bounds
        return self

    def transform(self, x: pd.DataFrame) -> pd.DataFrame:
        df = x.copy()
        if self.map_col in df.columns and self.frequent_maps_:
            map_values = df[self.map_col].where(pd.notna(df[self.map_col]), "__MISSING__").astype(str)
            df[self.map_col] = np.where(map_values.isin(self.frequent_maps_), map_values, "__OTHER__")

        for col, (lower, upper) in self.numeric_bounds_.items():
            if col not in df.columns:
                continue
            values = pd.to_numeric(df[col], errors="coerce")
            df[col] = values.clip(lower=lower, upper=upper)

        return df


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


def build_temporal_folds(
    df: pd.DataFrame,
    min_train_rows: int = 20000,
    min_val_rows: int = 5000,
) -> List[TemporalFold]:
    ordered = df.sort_values("match_time").reset_index(drop=True)
    fold_windows = (
        ("fold_60_10", 0.60, 0.10),
        ("fold_70_10", 0.70, 0.10),
        ("fold_80_20", 0.80, 0.20),
    )
    folds: List[TemporalFold] = []
    n_rows = len(ordered)
    for name, train_frac, val_frac in fold_windows:
        train_end = int(n_rows * train_frac)
        val_end = min(n_rows, train_end + int(n_rows * val_frac))
        train_df = ordered.iloc[:train_end].copy()
        val_df = ordered.iloc[train_end:val_end].copy()
        if len(train_df) < min_train_rows or len(val_df) < min_val_rows:
            continue
        folds.append(TemporalFold(name=name, train_df=train_df, val_df=val_df))
    return folds


def assert_no_leakage_features(task_name: str, selected_features: Sequence[str], disallowed_features: set[str]) -> None:
    overlap = sorted(set(selected_features).intersection(disallowed_features))
    if overlap:
        joined = ", ".join(overlap)
        raise RuntimeError(f"{task_name} contains leakage-prone features: {joined}")


def build_linear_preprocessor(numeric_cols: Sequence[str], categorical_cols: Sequence[str]) -> ColumnTransformer:
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


def build_tree_preprocessor(numeric_cols: Sequence[str], categorical_cols: Sequence[str]) -> ColumnTransformer:
    numeric = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]
    )
    categorical = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            (
                "ordinal",
                OrdinalEncoder(
                    handle_unknown="use_encoded_value",
                    unknown_value=-1,
                ),
            ),
        ]
    )
    return ColumnTransformer(
        transformers=[
            ("num", numeric, list(numeric_cols)),
            ("cat", categorical, list(categorical_cols)),
        ],
        remainder="drop",
    )


def fit_calibrated_classifier(
    base_estimator: Pipeline,
    x_train: pd.DataFrame,
    y_train: pd.Series,
    calibration_fraction: float = 0.15,
) -> Any:
    if len(x_train) < 2000:
        return base_estimator.fit(x_train, y_train)

    split_idx = int(len(x_train) * (1.0 - calibration_fraction))
    split_idx = max(1000, min(len(x_train) - 500, split_idx))
    x_fit = x_train.iloc[:split_idx]
    y_fit = y_train.iloc[:split_idx]
    x_cal = x_train.iloc[split_idx:]
    y_cal = y_train.iloc[split_idx:]

    fitted = base_estimator.fit(x_fit, y_fit)
    if len(np.unique(y_cal)) < 2:
        return fitted

    return CalibratedClassifierCV(
        estimator=fitted,
        method="sigmoid",
        cv="prefit",
    ).fit(x_cal, y_cal)


def train_win_model(
    x_train: pd.DataFrame,
    y_train: pd.Series,
    numeric_cols: Sequence[str],
    categorical_cols: Sequence[str],
    calibrate: bool = True,
) -> Any:
    # Fit once, calibrate on a holdout split to avoid expensive full-CV retraining.
    base = Pipeline(
        steps=[
            ("quality", FeatureQualityTransformer(numeric_cols=numeric_cols, map_min_frequency=120)),
            ("prep", build_tree_preprocessor(numeric_cols, categorical_cols)),
            (
                "model",
                HistGradientBoostingClassifier(
                    loss="log_loss",
                    learning_rate=0.05,
                    max_iter=320,
                    max_depth=8,
                    max_leaf_nodes=63,
                    min_samples_leaf=250,
                    l2_regularization=0.1,
                    random_state=42,
                ),
            ),
        ],
    )
    if calibrate:
        return fit_calibrated_classifier(base, x_train, y_train)
    return base.fit(x_train, y_train)


def train_tilt_model(
    x_train: pd.DataFrame,
    y_train: pd.Series,
    numeric_cols: Sequence[str],
    categorical_cols: Sequence[str],
    calibrate: bool = True,
) -> Any:
    base = Pipeline(
        steps=[
            ("quality", FeatureQualityTransformer(numeric_cols=numeric_cols, map_min_frequency=120)),
            ("prep", build_linear_preprocessor(numeric_cols, categorical_cols)),
            (
                "model",
                LogisticRegression(
                    C=0.15,  # stronger regularisation + class balancing
                    max_iter=300,
                    solver="lbfgs",
                    class_weight="balanced",
                ),
            ),
        ],
    )
    if calibrate:
        return fit_calibrated_classifier(base, x_train, y_train)
    return base.fit(x_train, y_train)


def train_impact_model(
    x_train: pd.DataFrame,
    y_train: pd.Series,
    numeric_cols: Sequence[str],
    categorical_cols: Sequence[str],
) -> Pipeline:
    return Pipeline(
        steps=[
            ("quality", FeatureQualityTransformer(numeric_cols=numeric_cols, map_min_frequency=120)),
            ("prep", build_tree_preprocessor(numeric_cols, categorical_cols)),
            (
                "model",
                HistGradientBoostingRegressor(
                    loss="squared_error",
                    learning_rate=0.05,
                    max_iter=350,
                    max_depth=8,
                    max_leaf_nodes=63,
                    min_samples_leaf=250,
                    l2_regularization=0.1,
                    random_state=42,
                ),
            ),
        ]
    ).fit(x_train, y_train)


def binary_metrics(y_true: np.ndarray, probs: np.ndarray, threshold: float = 0.5) -> Dict[str, float]:
    preds = (probs >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, preds, labels=[0, 1]).ravel()
    roc_auc = float("nan")
    pr_auc = float("nan")
    if len(np.unique(y_true)) > 1:
        roc_auc = float(roc_auc_score(y_true, probs))
        pr_auc = float(average_precision_score(y_true, probs))
    return {
        "precision": float(precision_score(y_true, preds, zero_division=0)),
        "recall": float(recall_score(y_true, preds, zero_division=0)),
        "f1": float(f1_score(y_true, preds, zero_division=0)),
        "roc_auc": roc_auc,
        "pr_auc": pr_auc,
        "brier": float(brier_score_loss(y_true, probs)),
        "true_negatives": float(tn),
        "false_positives": float(fp),
        "false_negatives": float(fn),
        "true_positives": float(tp),
        "positive_rate": float(np.mean(y_true)),
        "predicted_positive_rate": float(np.mean(preds)),
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


def choose_threshold_policy(
    y_true: np.ndarray,
    probs: np.ndarray,
    thresholds: Sequence[float],
    min_precision: float,
    min_recall: float,
) -> ThresholdPolicy:
    rows = threshold_table(y_true, probs, thresholds)
    constrained = [
        row
        for row in rows
        if row["precision"] >= min_precision and row["recall"] >= min_recall
    ]
    if constrained:
        best = max(constrained, key=lambda row: (row["f1"], row["precision"], row["recall"]))
        return ThresholdPolicy(
            threshold=float(best["threshold"]),
            min_precision=min_precision,
            min_recall=min_recall,
            selected_by_constraints=True,
            precision=float(best["precision"]),
            recall=float(best["recall"]),
            f1=float(best["f1"]),
        )
    fallback = max(rows, key=lambda row: (row["f1"], row["precision"], row["recall"]))
    return ThresholdPolicy(
        threshold=float(fallback["threshold"]),
        min_precision=min_precision,
        min_recall=min_recall,
        selected_by_constraints=False,
        precision=float(fallback["precision"]),
        recall=float(fallback["recall"]),
        f1=float(fallback["f1"]),
    )


def with_selected_threshold(rows: List[Dict[str, float]], selected_threshold: float) -> List[Dict[str, float]]:
    out: List[Dict[str, float]] = []
    for row in rows:
        copied = dict(row)
        copied["policy_selected"] = 1.0 if abs(copied["threshold"] - selected_threshold) < 1e-9 else 0.0
        out.append(copied)
    return out


def calibration_table(y_true: np.ndarray, probs: np.ndarray, bins: int = 10) -> List[Dict[str, float]]:
    df = pd.DataFrame({"y_true": y_true, "probs": probs})
    clipped = np.clip(df["probs"].to_numpy(dtype=float), 0.0, 1.0)
    df["bin"] = pd.cut(
        clipped,
        bins=np.linspace(0.0, 1.0, bins + 1),
        labels=False,
        include_lowest=True,
    )
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
    for bin_id in range(bins):
        bin_df = summary[summary["bin"] == bin_id]
        if bin_df.empty:
            continue
        row = bin_df.iloc[0]
        out.append(
            {
                "bin": float(bin_id),
                "rows": float(row["rows"]),
                "avg_predicted": float(row["avg_predicted"]),
                "avg_observed": float(row["avg_observed"]),
                "min_prob": float(row["min_prob"]),
                "max_prob": float(row["max_prob"]),
            }
        )
    return out


def expected_calibration_error(y_true: np.ndarray, probs: np.ndarray, bins: int = 10) -> float:
    if len(y_true) == 0:
        return float("nan")
    df = pd.DataFrame({"y_true": y_true, "probs": probs})
    clipped = np.clip(df["probs"].to_numpy(dtype=float), 0.0, 1.0)
    df["bin"] = pd.cut(
        clipped,
        bins=np.linspace(0.0, 1.0, bins + 1),
        labels=False,
        include_lowest=True,
    )
    total = float(len(df))
    ece = 0.0
    for _, bin_df in df.groupby("bin", dropna=True):
        weight = len(bin_df) / total
        ece += weight * abs(float(bin_df["probs"].mean()) - float(bin_df["y_true"].mean()))
    return float(ece)


def top_feature_effects(
    model: Any,
    x_eval: pd.DataFrame,
    y_eval: np.ndarray,
    *,
    is_classification: bool,
    top_n: int = 12,
    sample_size: int = 5000,
) -> List[Tuple[str, float]]:
    scoring = "roc_auc" if is_classification else "neg_root_mean_squared_error"
    if len(x_eval) == 0:
        return []
    y_series = pd.Series(y_eval, index=x_eval.index)
    sample_n = min(sample_size, len(x_eval))
    if sample_n < len(x_eval):
        sample_idx = x_eval.sample(n=sample_n, random_state=42).index
        x_sample = x_eval.loc[sample_idx]
        y_sample = y_series.loc[x_sample.index].to_numpy()
    else:
        x_sample = x_eval
        y_sample = y_series.to_numpy()

    importance = permutation_importance(
        estimator=model,
        X=x_sample,
        y=y_sample,
        n_repeats=2,
        random_state=42,
        scoring=scoring,
    )
    pairs = list(zip(x_sample.columns.tolist(), importance.importances_mean.tolist()))
    return sorted(pairs, key=lambda x: x[1], reverse=True)[:top_n]


def classification_metrics_by_segment(
    y_true: np.ndarray,
    probs: np.ndarray,
    segments: pd.Series,
    segment_name: str,
    threshold: float = 0.5,
    min_rows: int = 200,
) -> List[Dict[str, Any]]:
    scored = pd.DataFrame({"y_true": y_true, "probs": probs, "segment": segments})
    rows: List[Dict[str, Any]] = []
    for segment_value, segment_df in scored.groupby("segment", dropna=False):
        if len(segment_df) < min_rows:
            continue
        metrics = binary_metrics(
            segment_df["y_true"].to_numpy(dtype=int),
            segment_df["probs"].to_numpy(dtype=float),
            threshold=threshold,
        )
        rows.append(
            {
                segment_name: str(segment_value) if pd.notna(segment_value) else "null",
                "rows": float(len(segment_df)),
                "precision": metrics["precision"],
                "recall": metrics["recall"],
                "f1": metrics["f1"],
                "roc_auc": metrics["roc_auc"],
                "pr_auc": metrics["pr_auc"],
                "brier": metrics["brier"],
                "positive_rate": metrics["positive_rate"],
            }
        )
    return sorted(rows, key=lambda row: row["rows"], reverse=True)


def regression_metrics_by_segment(
    y_true: np.ndarray,
    preds: np.ndarray,
    segments: pd.Series,
    segment_name: str,
    min_rows: int = 200,
) -> List[Dict[str, Any]]:
    scored = pd.DataFrame({"y_true": y_true, "preds": preds, "segment": segments})
    rows: List[Dict[str, Any]] = []
    for segment_value, segment_df in scored.groupby("segment", dropna=False):
        if len(segment_df) < min_rows:
            continue
        metrics = regression_metrics(
            segment_df["y_true"].to_numpy(dtype=float),
            segment_df["preds"].to_numpy(dtype=float),
        )
        rows.append(
            {
                segment_name: str(segment_value) if pd.notna(segment_value) else "null",
                "rows": float(len(segment_df)),
                "rmse": metrics["rmse"],
                "mae": metrics["mae"],
            }
        )
    return sorted(rows, key=lambda row: row["rows"], reverse=True)


def temporal_backtest_rows(
    folds: Sequence[TemporalFold],
    model_kind: str,
    numeric_cols: Sequence[str],
    categorical_cols: Sequence[str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    feature_cols = list(numeric_cols) + list(categorical_cols)
    for fold in folds:
        x_train = fold.train_df[feature_cols]
        x_val = fold.val_df[feature_cols]
        if model_kind == "win":
            y_train = fold.train_df["label_win"].astype(int)
            y_val = fold.val_df["label_win"].astype(int)
            model = train_win_model(x_train, y_train, numeric_cols, categorical_cols, calibrate=False)
            probs = model.predict_proba(x_val)[:, 1]
            metrics = binary_metrics(y_val.to_numpy(), probs)
            rows.append(
                {
                    "fold": fold.name,
                    "train_rows": float(len(fold.train_df)),
                    "val_rows": float(len(fold.val_df)),
                    "precision": metrics["precision"],
                    "recall": metrics["recall"],
                    "f1": metrics["f1"],
                    "roc_auc": metrics["roc_auc"],
                    "pr_auc": metrics["pr_auc"],
                    "brier": metrics["brier"],
                }
            )
        elif model_kind == "tilt":
            y_train = fold.train_df["label_tilt"].astype(int)
            y_val = fold.val_df["label_tilt"].astype(int)
            model = train_tilt_model(x_train, y_train, numeric_cols, categorical_cols, calibrate=False)
            probs = model.predict_proba(x_val)[:, 1]
            metrics = binary_metrics(y_val.to_numpy(), probs)
            rows.append(
                {
                    "fold": fold.name,
                    "train_rows": float(len(fold.train_df)),
                    "val_rows": float(len(fold.val_df)),
                    "precision": metrics["precision"],
                    "recall": metrics["recall"],
                    "f1": metrics["f1"],
                    "roc_auc": metrics["roc_auc"],
                    "pr_auc": metrics["pr_auc"],
                    "brier": metrics["brier"],
                }
            )
        else:
            y_train = fold.train_df["label_impact_percentile"].astype(float)
            y_val = fold.val_df["label_impact_percentile"].astype(float)
            model = train_impact_model(x_train, y_train, numeric_cols, categorical_cols)
            preds = np.clip(model.predict(x_val), 1.0, 100.0)
            metrics = regression_metrics(y_val.to_numpy(), preds)
            rows.append(
                {
                    "fold": fold.name,
                    "train_rows": float(len(fold.train_df)),
                    "val_rows": float(len(fold.val_df)),
                    "rmse": metrics["rmse"],
                    "mae": metrics["mae"],
                }
            )
    return rows


def gate_result(
    model_name: str,
    gate_name: str,
    comparator: str,
    actual: float,
    target: float,
) -> Dict[str, Any]:
    if np.isnan(actual):
        passed = False
    elif comparator == ">=":
        passed = actual >= target
    else:
        passed = actual <= target
    return {
        "model_name": model_name,
        "gate_name": gate_name,
        "comparator": comparator,
        "actual": actual,
        "target": target,
        "status": "PASS" if passed else "FAIL",
    }


def fold_metric(rows: Sequence[Dict[str, Any]], key: str, fn: str) -> float:
    values = [float(row[key]) for row in rows if key in row and pd.notna(row[key])]
    if not values:
        return float("nan")
    if fn == "min":
        return float(min(values))
    if fn == "max":
        return float(max(values))
    if len(values) == 1:
        return 0.0
    return float(np.std(values, ddof=1))


def build_promotion_gates(
    win_metrics: Dict[str, float],
    impact_metrics: Dict[str, float],
    tilt_metrics: Dict[str, float],
    win_backtest_rows: Sequence[Dict[str, Any]],
    impact_backtest_rows: Sequence[Dict[str, Any]],
    tilt_backtest_rows: Sequence[Dict[str, Any]],
    *,
    win_min_f1: float,
    win_max_brier: float,
    win_min_fold_f1: float,
    impact_max_rmse: float,
    impact_max_mae: float,
    impact_max_fold_rmse: float,
    tilt_min_f1: float,
    tilt_max_brier: float,
    tilt_min_recall: float,
    tilt_max_fold_f1_std: float,
) -> Tuple[List[Dict[str, Any]], Dict[str, bool]]:
    rows: List[Dict[str, Any]] = []
    rows.append(gate_result("win_probability_baseline", "f1", ">=", float(win_metrics["f1"]), win_min_f1))
    rows.append(gate_result("win_probability_baseline", "brier", "<=", float(win_metrics["brier"]), win_max_brier))
    rows.append(
        gate_result(
            "win_probability_baseline",
            "min_fold_f1",
            ">=",
            fold_metric(win_backtest_rows, "f1", "min"),
            win_min_fold_f1,
        )
    )

    rows.append(gate_result("impact_percentile_baseline", "rmse", "<=", float(impact_metrics["rmse"]), impact_max_rmse))
    rows.append(gate_result("impact_percentile_baseline", "mae", "<=", float(impact_metrics["mae"]), impact_max_mae))
    rows.append(
        gate_result(
            "impact_percentile_baseline",
            "max_fold_rmse",
            "<=",
            fold_metric(impact_backtest_rows, "rmse", "max"),
            impact_max_fold_rmse,
        )
    )

    rows.append(gate_result("tilt_risk_baseline", "f1", ">=", float(tilt_metrics["f1"]), tilt_min_f1))
    rows.append(gate_result("tilt_risk_baseline", "brier", "<=", float(tilt_metrics["brier"]), tilt_max_brier))
    rows.append(gate_result("tilt_risk_baseline", "recall", ">=", float(tilt_metrics["recall"]), tilt_min_recall))
    rows.append(
        gate_result(
            "tilt_risk_baseline",
            "fold_f1_stddev",
            "<=",
            fold_metric(tilt_backtest_rows, "f1", "std"),
            tilt_max_fold_f1_std,
        )
    )

    readiness: Dict[str, bool] = {}
    for model_name in ("win_probability_baseline", "impact_percentile_baseline", "tilt_risk_baseline"):
        model_rows = [row for row in rows if row["model_name"] == model_name]
        readiness[model_name] = bool(model_rows) and all(row["status"] == "PASS" for row in model_rows)
    return rows, readiness


def ensure_dir(path: pathlib.Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_json(path: pathlib.Path, payload: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (float, np.floating)) and np.isnan(value):
        return "NULL"
    if isinstance(value, (np.integer, int)):
        return str(int(value))
    if isinstance(value, (np.floating, float)):
        return f"{float(value):.10f}"
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return "NULL"
        return f"DATE '{value.strftime('%Y-%m-%d')}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    return f"'{sql_escape(str(value))}'"


def ensure_validation_metrics_table(trino: TrinoClient) -> None:
    trino.execute(
        """
CREATE TABLE IF NOT EXISTS tf2.default.ml_model_validation_metrics_daily (
  model_name VARCHAR,
  model_version VARCHAR,
  task_type VARCHAR,
  snapshot_id VARCHAR,
  progress_date DATE,
  rows_total BIGINT,
  observed_positive_rate DOUBLE,
  predicted_positive_rate DOUBLE,
  precision DOUBLE,
  recall DOUBLE,
  f1 DOUBLE,
  roc_auc DOUBLE,
  pr_auc DOUBLE,
  brier DOUBLE,
  rmse DOUBLE,
  mae DOUBLE,
  created_at TIMESTAMP
)
""".strip()
    )


def upsert_validation_metrics_daily(
    trino: TrinoClient,
    model_name: str,
    model_version: str,
    task_type: str,
    snapshot_id: str,
    rows: Sequence[Dict[str, Any]],
) -> None:
    trino.execute(
        f"""
DELETE FROM tf2.default.ml_model_validation_metrics_daily
WHERE model_name = '{sql_escape(model_name)}'
  AND model_version = '{sql_escape(model_version)}'
  AND snapshot_id = '{sql_escape(snapshot_id)}'
""".strip()
    )
    if not rows:
        return

    values_sql: List[str] = []
    for row in rows:
        values_sql.append(
            "("
            + ", ".join(
                [
                    sql_literal(model_name),
                    sql_literal(model_version),
                    sql_literal(task_type),
                    sql_literal(snapshot_id),
                    sql_literal(row.get("progress_date")),
                    sql_literal(row.get("rows_total")),
                    sql_literal(row.get("observed_positive_rate")),
                    sql_literal(row.get("predicted_positive_rate")),
                    sql_literal(row.get("precision")),
                    sql_literal(row.get("recall")),
                    sql_literal(row.get("f1")),
                    sql_literal(row.get("roc_auc")),
                    sql_literal(row.get("pr_auc")),
                    sql_literal(row.get("brier")),
                    sql_literal(row.get("rmse")),
                    sql_literal(row.get("mae")),
                    "CURRENT_TIMESTAMP",
                ]
            )
            + ")"
        )

    trino.execute(
        """
INSERT INTO tf2.default.ml_model_validation_metrics_daily (
  model_name,
  model_version,
  task_type,
  snapshot_id,
  progress_date,
  rows_total,
  observed_positive_rate,
  predicted_positive_rate,
  precision,
  recall,
  f1,
  roc_auc,
  pr_auc,
  brier,
  rmse,
  mae,
  created_at
)
VALUES
"""
        + ",\n".join(values_sql)
    )


def classification_daily_metrics_rows(
    val_df: pd.DataFrame,
    y_true: pd.Series,
    probs: np.ndarray,
    threshold: float = 0.5,
) -> List[Dict[str, Any]]:
    scored = val_df[["match_date"]].copy()
    scored["y_true"] = y_true.to_numpy(dtype=int)
    scored["probs"] = probs
    scored["pred"] = (probs >= threshold).astype(int)
    rows: List[Dict[str, Any]] = []
    for progress_date, day_df in scored.groupby("match_date", dropna=False):
        if pd.isna(progress_date):
            continue
        metrics = binary_metrics(
            day_df["y_true"].to_numpy(dtype=int),
            day_df["probs"].to_numpy(dtype=float),
            threshold=threshold,
        )
        rows.append(
            {
                "progress_date": progress_date,
                "rows_total": int(len(day_df)),
                "observed_positive_rate": metrics["positive_rate"],
                "predicted_positive_rate": float(day_df["pred"].mean()),
                "precision": metrics["precision"],
                "recall": metrics["recall"],
                "f1": metrics["f1"],
                "roc_auc": metrics["roc_auc"],
                "pr_auc": metrics["pr_auc"],
                "brier": metrics["brier"],
                "rmse": None,
                "mae": None,
            }
        )
    return rows


def regression_daily_metrics_rows(val_df: pd.DataFrame, y_true: pd.Series, preds: np.ndarray) -> List[Dict[str, Any]]:
    scored = val_df[["match_date"]].copy()
    scored["y_true"] = y_true.to_numpy(dtype=float)
    scored["preds"] = preds
    rows: List[Dict[str, Any]] = []
    for progress_date, day_df in scored.groupby("match_date", dropna=False):
        if pd.isna(progress_date):
            continue
        metrics = regression_metrics(day_df["y_true"].to_numpy(dtype=float), day_df["preds"].to_numpy(dtype=float))
        rows.append(
            {
                "progress_date": progress_date,
                "rows_total": int(len(day_df)),
                "observed_positive_rate": None,
                "predicted_positive_rate": None,
                "precision": None,
                "recall": None,
                "f1": None,
                "roc_auc": None,
                "pr_auc": None,
                "brier": None,
                "rmse": metrics["rmse"],
                "mae": metrics["mae"],
            }
        )
    return rows


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
    win_threshold_policy: ThresholdPolicy,
    tilt_threshold_policy: ThresholdPolicy,
    win_threshold_rows: List[Dict[str, float]],
    tilt_threshold_rows: List[Dict[str, float]],
    win_calibration_rows: List[Dict[str, float]],
    tilt_calibration_rows: List[Dict[str, float]],
    win_top_features: List[Tuple[str, float]],
    tilt_top_features: List[Tuple[str, float]],
    impact_top_features: List[Tuple[str, float]],
    win_backtest_rows: List[Dict[str, Any]],
    tilt_backtest_rows: List[Dict[str, Any]],
    impact_backtest_rows: List[Dict[str, Any]],
    win_segment_rows: List[Dict[str, Any]],
    tilt_segment_rows: List[Dict[str, Any]],
    impact_segment_rows: List[Dict[str, Any]],
    promotion_gate_rows: List[Dict[str, Any]],
) -> None:
    model_order = [
        "win_probability_baseline",
        "impact_percentile_baseline",
        "tilt_risk_baseline",
    ]
    gate_rows_by_model = {
        model_name: [row for row in promotion_gate_rows if row["model_name"] == model_name]
        for model_name in model_order
    }

    def gate_failure_text(row: Dict[str, Any]) -> str:
        comparator = str(row["comparator"])
        fail_symbol = "<" if comparator == ">=" else ">"
        return f"{row['gate_name']} ({float(row['actual']):.4f} {fail_symbol} {float(row['target']):.4f})"

    decision_rows: List[Dict[str, Any]] = []
    approved_models: List[str] = []
    blocked_models: List[str] = []
    for model_name in model_order:
        model_gate_rows = gate_rows_by_model.get(model_name, [])
        failures = [row for row in model_gate_rows if row.get("status") == "FAIL"]
        if model_gate_rows and not failures:
            approved_models.append(model_name)
            decision_rows.append(
                {
                    "model": model_name,
                    "decision": "approved",
                    "reason": "all promotion gates pass",
                }
            )
        else:
            blocked_models.append(model_name)
            decision_rows.append(
                {
                    "model": model_name,
                    "decision": "blocked",
                    "reason": "; ".join(gate_failure_text(row) for row in failures) or "missing gate results",
                }
            )

    if approved_models == ["tilt_risk_baseline"]:
        outcome_line = "Promote `tilt_risk_baseline` only."
    elif approved_models:
        outcome_line = "Promote: " + ", ".join(f"`{name}`" for name in approved_models) + "."
    else:
        outcome_line = "Do not promote any model in this run."

    threshold_rows_with_policy: List[Dict[str, Any]] = []
    for row in win_threshold_rows:
        copied = dict(row)
        copied["meets_precision_target"] = float(copied["precision"] >= win_threshold_policy.min_precision)
        copied["meets_recall_target"] = float(copied["recall"] >= win_threshold_policy.min_recall)
        copied["meets_policy_constraints"] = float(
            copied["meets_precision_target"] == 1.0 and copied["meets_recall_target"] == 1.0
        )
        threshold_rows_with_policy.append(copied)
    for row in tilt_threshold_rows:
        copied = dict(row)
        copied["meets_precision_target"] = float(copied["precision"] >= tilt_threshold_policy.min_precision)
        copied["meets_recall_target"] = float(copied["recall"] >= tilt_threshold_policy.min_recall)
        copied["meets_policy_constraints"] = float(
            copied["meets_precision_target"] == 1.0 and copied["meets_recall_target"] == 1.0
        )
        threshold_rows_with_policy.append(copied)

    def effect_rows(pairs: List[Tuple[str, float]]) -> List[Dict[str, Any]]:
        return [{"feature": f, "permutation_importance": w} for f, w in pairs]

    lines: List[str] = []
    lines.append("# ML offline evaluation report")
    lines.append("")
    lines.append(f"- Generated at: `{utc_now_human()}`")
    lines.append(f"- Snapshot ID: `{snapshot_id}`")
    lines.append(f"- Train rows: `{len(train_df)}`")
    lines.append(f"- Validation rows: `{len(val_df)}`")
    lines.append(f"- Validation date range: `{val_df['match_time'].min()}` to `{val_df['match_time'].max()}`")
    lines.append("")
    lines.append("## Executive summary")
    lines.append("")
    lines.append(f"- Outcome: {outcome_line}")
    for row in decision_rows:
        lines.append(f"- `{row['model']}`: {row['decision']} ({row['reason']}).")
    lines.append(
        f"- Win operational rate: at threshold `{win_threshold_policy.threshold:.2f}`, predicted-positive volume is "
        f"`{100.0 * metric_by_model['win_probability_baseline']['predicted_positive_rate']:.2f}%` of validation rows."
    )
    lines.append(
        f"- Tilt operational rate: at threshold `{tilt_threshold_policy.threshold:.2f}`, predicted-positive volume is "
        f"`{100.0 * metric_by_model['tilt_risk_baseline']['predicted_positive_rate']:.2f}%` of validation rows."
    )
    lines.append("")
    lines.append("### Decision block")
    lines.append("")
    lines.extend(format_table(decision_rows, ["model", "decision", "reason"]))
    lines.append("")
    lines.append("## Promotion gates")
    lines.append("")
    lines.extend(
        format_table(
            promotion_gate_rows,
            ["model_name", "gate_name", "comparator", "target", "actual", "status"],
        )
    )
    lines.append("")
    lines.append("## Key operating numbers")
    lines.append("")
    lines.append("### Temporal fold minima and variance")
    lines.append("")
    temporal_rows = [
        {
            "model": "win_probability_baseline",
            "min_fold_f1": fold_metric(win_backtest_rows, "f1", "min"),
            "fold_f1_stddev": fold_metric(win_backtest_rows, "f1", "std"),
        },
        {
            "model": "tilt_risk_baseline",
            "min_fold_f1": fold_metric(tilt_backtest_rows, "f1", "min"),
            "fold_f1_stddev": fold_metric(tilt_backtest_rows, "f1", "std"),
        },
        {
            "model": "impact_percentile_baseline",
            "max_fold_rmse": fold_metric(impact_backtest_rows, "rmse", "max"),
            "fold_rmse_stddev": fold_metric(impact_backtest_rows, "rmse", "std"),
        },
    ]
    lines.extend(
        format_table(
            temporal_rows,
            ["model", "min_fold_f1", "fold_f1_stddev", "max_fold_rmse", "fold_rmse_stddev"],
        )
    )
    lines.append("")
    lines.append("### Selected thresholds and expected volume")
    lines.append("")
    policy_rows = [
        {
            "model": "win_probability_baseline",
            "threshold": win_threshold_policy.threshold,
            "min_precision_target": win_threshold_policy.min_precision,
            "min_recall_target": win_threshold_policy.min_recall,
            "selected_by_constraints": float(win_threshold_policy.selected_by_constraints),
            "precision": win_threshold_policy.precision,
            "recall": win_threshold_policy.recall,
            "f1": win_threshold_policy.f1,
            "predicted_positive_rate": metric_by_model["win_probability_baseline"]["predicted_positive_rate"],
        },
        {
            "model": "tilt_risk_baseline",
            "threshold": tilt_threshold_policy.threshold,
            "min_precision_target": tilt_threshold_policy.min_precision,
            "min_recall_target": tilt_threshold_policy.min_recall,
            "selected_by_constraints": float(tilt_threshold_policy.selected_by_constraints),
            "precision": tilt_threshold_policy.precision,
            "recall": tilt_threshold_policy.recall,
            "f1": tilt_threshold_policy.f1,
            "predicted_positive_rate": metric_by_model["tilt_risk_baseline"]["predicted_positive_rate"],
        },
    ]
    lines.extend(
        format_table(
            policy_rows,
            [
                "model",
                "threshold",
                "min_precision_target",
                "min_recall_target",
                "selected_by_constraints",
                "precision",
                "recall",
                "f1",
                "predicted_positive_rate",
            ],
        )
    )
    lines.append("")
    lines.append("### Calibration quality")
    lines.append("")
    lines.extend(
        format_table(
            [
                {
                    "model": "win_probability_baseline",
                    "brier": metric_by_model["win_probability_baseline"]["brier"],
                    "ece": metric_by_model["win_probability_baseline"]["ece"],
                },
                {
                    "model": "tilt_risk_baseline",
                    "brier": metric_by_model["tilt_risk_baseline"]["brier"],
                    "ece": metric_by_model["tilt_risk_baseline"]["ece"],
                },
            ],
            ["model", "brier", "ece"],
        )
    )
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
    lines.append("## Temporal backtesting")
    lines.append("")
    lines.append("### Win model folds")
    lines.append("")
    lines.extend(format_table(win_backtest_rows, ["fold", "train_rows", "val_rows", "precision", "recall", "f1", "roc_auc", "pr_auc", "brier"]))
    lines.append("")
    lines.append("### Tilt model folds")
    lines.append("")
    lines.extend(format_table(tilt_backtest_rows, ["fold", "train_rows", "val_rows", "precision", "recall", "f1", "roc_auc", "pr_auc", "brier"]))
    lines.append("")
    lines.append("### Impact model folds")
    lines.append("")
    lines.extend(format_table(impact_backtest_rows, ["fold", "train_rows", "val_rows", "rmse", "mae"]))
    lines.append("")
    lines.append("## Segment quality")
    lines.append("")
    lines.append("### Win model by momentum")
    lines.append("")
    lines.extend(
        format_table(
            win_segment_rows,
            ["momentum_label", "rows", "precision", "recall", "f1", "roc_auc", "pr_auc", "brier", "positive_rate"],
        )
    )
    lines.append("")
    lines.append("### Tilt model by momentum")
    lines.append("")
    lines.extend(
        format_table(
            tilt_segment_rows,
            ["momentum_label", "rows", "precision", "recall", "f1", "roc_auc", "pr_auc", "brier", "positive_rate"],
        )
    )
    lines.append("")
    lines.append("### Impact model by momentum")
    lines.append("")
    lines.extend(format_table(impact_segment_rows, ["momentum_label", "rows", "rmse", "mae"]))
    lines.append("")
    lines.append("## Threshold trade-offs")
    lines.append("")
    lines.append("### Win model")
    lines.append("")
    win_threshold_rows_only = threshold_rows_with_policy[: len(win_threshold_rows)]
    lines.extend(
        format_table(
            win_threshold_rows_only,
            [
                "threshold",
                "precision",
                "recall",
                "f1",
                "predicted_positive_rate",
                "meets_precision_target",
                "meets_recall_target",
                "meets_policy_constraints",
                "policy_selected",
            ],
        )
    )
    lines.append("")
    lines.append("### Tilt model")
    lines.append("")
    tilt_threshold_rows_only = threshold_rows_with_policy[len(win_threshold_rows):]
    lines.extend(
        format_table(
            tilt_threshold_rows_only,
            [
                "threshold",
                "precision",
                "recall",
                "f1",
                "predicted_positive_rate",
                "meets_precision_target",
                "meets_recall_target",
                "meets_policy_constraints",
                "policy_selected",
            ],
        )
    )
    lines.append("")
    lines.append("## Calibration tables")
    lines.append("")
    lines.append("- Bins are fixed-width probability buckets over `[0, 1]`; sparse bins indicate concentrated predictions.")
    lines.append("")
    lines.append("### Win model")
    lines.append("")
    lines.extend(format_table(win_calibration_rows, ["bin", "rows", "min_prob", "max_prob", "avg_predicted", "avg_observed"]))
    lines.append("")
    lines.append("### Tilt model")
    lines.append("")
    lines.extend(format_table(tilt_calibration_rows, ["bin", "rows", "min_prob", "max_prob", "avg_predicted", "avg_observed"]))
    lines.append("")
    lines.append("## Feature effects")
    lines.append("")
    lines.append(
        "- Permutation importance values are score deltas after shuffling a feature; they are ranking diagnostics, not directional causal effects."
    )
    lines.append("")
    lines.append("### Win top permutation importances")
    lines.extend(format_table(effect_rows(win_top_features), ["feature", "permutation_importance"]))
    lines.append("")
    lines.append("### Tilt top permutation importances")
    lines.extend(format_table(effect_rows(tilt_top_features), ["feature", "permutation_importance"]))
    lines.append("")
    lines.append("### Impact top permutation importances")
    lines.extend(format_table(effect_rows(impact_top_features), ["feature", "permutation_importance"]))
    lines.append("")
    lines.append("## Calibration notes")
    lines.append("")
    lines.append(
        "- Win and impact models now use train-time feature quality controls: rare map bucketing and quantile clipping on numeric outliers."
    )
    lines.append(
        "- Win uses a calibrated gradient-boosted classifier and impact uses a non-linear gradient-boosted regressor."
    )
    lines.append(
        "- Temporal backtesting rows above should be treated as the baseline promotion signal, not a single split metric."
    )
    lines.append(
        f"- Win Brier/ECE: `{metric_by_model['win_probability_baseline']['brier']:.4f}` / "
        f"`{metric_by_model['win_probability_baseline']['ece']:.4f}` at threshold `{win_threshold_policy.threshold:.2f}`."
    )
    lines.append(
        f"- Tilt Brier/ECE: `{metric_by_model['tilt_risk_baseline']['brier']:.4f}` / "
        f"`{metric_by_model['tilt_risk_baseline']['ece']:.4f}` at threshold `{tilt_threshold_policy.threshold:.2f}`."
    )
    lines.append(
        "- Promotion stage transitions are blocked when gate rows above include FAIL."
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
    parser.add_argument("--min-fold-train-rows", type=int, default=int(os.environ.get("MIN_FOLD_TRAIN_ROWS", "20000")))
    parser.add_argument("--min-fold-val-rows", type=int, default=int(os.environ.get("MIN_FOLD_VAL_ROWS", "5000")))
    parser.add_argument(
        "--win-policy-min-precision",
        type=float,
        default=float(os.environ.get("WIN_POLICY_MIN_PRECISION", "0.60")),
    )
    parser.add_argument(
        "--win-policy-min-recall",
        type=float,
        default=float(os.environ.get("WIN_POLICY_MIN_RECALL", "0.55")),
    )
    parser.add_argument(
        "--tilt-policy-min-precision",
        type=float,
        default=float(os.environ.get("TILT_POLICY_MIN_PRECISION", "0.75")),
    )
    parser.add_argument(
        "--tilt-policy-min-recall",
        type=float,
        default=float(os.environ.get("TILT_POLICY_MIN_RECALL", "0.95")),
    )
    parser.add_argument("--gate-win-min-f1", type=float, default=float(os.environ.get("GATE_WIN_MIN_F1", "0.66")))
    parser.add_argument("--gate-win-max-brier", type=float, default=float(os.environ.get("GATE_WIN_MAX_BRIER", "0.20")))
    parser.add_argument(
        "--gate-win-min-fold-f1",
        type=float,
        default=float(os.environ.get("GATE_WIN_MIN_FOLD_F1", "0.60")),
    )
    parser.add_argument(
        "--gate-impact-max-rmse",
        type=float,
        default=float(os.environ.get("GATE_IMPACT_MAX_RMSE", "20.00")),
    )
    parser.add_argument("--gate-impact-max-mae", type=float, default=float(os.environ.get("GATE_IMPACT_MAX_MAE", "16.00")))
    parser.add_argument(
        "--gate-impact-max-fold-rmse",
        type=float,
        default=float(os.environ.get("GATE_IMPACT_MAX_FOLD_RMSE", "22.00")),
    )
    parser.add_argument("--gate-tilt-min-f1", type=float, default=float(os.environ.get("GATE_TILT_MIN_F1", "0.85")))
    parser.add_argument("--gate-tilt-max-brier", type=float, default=float(os.environ.get("GATE_TILT_MAX_BRIER", "0.02")))
    parser.add_argument(
        "--gate-tilt-min-recall",
        type=float,
        default=float(os.environ.get("GATE_TILT_MIN_RECALL", "0.95")),
    )
    parser.add_argument(
        "--gate-tilt-max-fold-f1-std",
        type=float,
        default=float(os.environ.get("GATE_TILT_MAX_FOLD_F1_STD", "0.03")),
    )
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

    win_numeric = [
        "rolling_5_avg_kills",
        "rolling_10_avg_damage",
        "rolling_10_avg_impact",
        "rolling_10_kda_ratio",
        "rolling_10_win_rate",
        "rolling_10_negative_chat_ratio",
        "career_avg_kills",
        "career_avg_damage",
        "career_avg_impact",
        "form_delta_kills",
        "form_delta_damage",
        "form_delta_impact",
        "games_played_to_date",
    ]
    win_cat = ["map", "team", "momentum_label"]

    impact_numeric = [
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
    impact_cat = ["map", "team", "momentum_label"]

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

    assert_no_leakage_features(
        task_name="win_probability_baseline",
        selected_features=win_numeric + win_cat,
        disallowed_features={
            "team_score",
            "opponent_score",
            "score_delta",
            "kills",
            "assists",
            "deaths",
            "damage_dealt",
            "healing_done",
            "ubers_used",
            "kill_share_of_team",
            "damage_share_of_team",
            "healing_share_of_team",
            "impact_index",
            "damage_per_minute",
            "kda_ratio",
        },
    )
    assert_no_leakage_features(
        task_name="impact_percentile_baseline",
        selected_features=impact_numeric + impact_cat,
        disallowed_features={
            "kills",
            "assists",
            "deaths",
            "damage_dealt",
            "healing_done",
            "ubers_used",
            "kill_share_of_team",
            "damage_share_of_team",
            "healing_share_of_team",
            "impact_index",
            "damage_per_minute",
            "kda_ratio",
            "classes_played_count",
        },
    )

    x_train_win = train_df[win_numeric + win_cat]
    x_val_win = val_df[win_numeric + win_cat]
    x_train_impact = train_df[impact_numeric + impact_cat]
    x_val_impact = val_df[impact_numeric + impact_cat]
    x_train_tilt = train_df[tilt_numeric + tilt_cat]
    x_val_tilt = val_df[tilt_numeric + tilt_cat]

    y_train_win = train_df["label_win"].astype(int)
    y_val_win = val_df["label_win"].astype(int)
    y_train_tilt = train_df["label_tilt"].astype(int)
    y_val_tilt = val_df["label_tilt"].astype(int)
    y_train_impact = train_df["label_impact_percentile"].astype(float)
    y_val_impact = val_df["label_impact_percentile"].astype(float)

    win_model = train_win_model(x_train_win, y_train_win, win_numeric, win_cat)
    tilt_model = train_tilt_model(x_train_tilt, y_train_tilt, tilt_numeric, tilt_cat)
    impact_model = train_impact_model(x_train_impact, y_train_impact, impact_numeric, impact_cat)

    win_probs = win_model.predict_proba(x_val_win)[:, 1]
    tilt_probs = tilt_model.predict_proba(x_val_tilt)[:, 1]
    impact_preds = np.clip(impact_model.predict(x_val_impact), 1.0, 100.0)

    win_policy = choose_threshold_policy(
        y_val_win.to_numpy(),
        win_probs,
        thresholds=np.arange(0.20, 0.86, 0.05).tolist(),
        min_precision=args.win_policy_min_precision,
        min_recall=args.win_policy_min_recall,
    )
    tilt_policy = choose_threshold_policy(
        y_val_tilt.to_numpy(),
        tilt_probs,
        thresholds=np.arange(0.10, 0.81, 0.05).tolist(),
        min_precision=args.tilt_policy_min_precision,
        min_recall=args.tilt_policy_min_recall,
    )

    win_metrics = binary_metrics(y_val_win.to_numpy(), win_probs, threshold=win_policy.threshold)
    tilt_metrics = binary_metrics(y_val_tilt.to_numpy(), tilt_probs, threshold=tilt_policy.threshold)
    impact_metrics = regression_metrics(y_val_impact.to_numpy(), impact_preds)
    win_metrics["ece"] = expected_calibration_error(y_val_win.to_numpy(), win_probs)
    tilt_metrics["ece"] = expected_calibration_error(y_val_tilt.to_numpy(), tilt_probs)

    win_threshold_candidates = sorted({0.2, 0.3, 0.4, 0.5, 0.6, 0.7, round(win_policy.threshold, 4)})
    tilt_threshold_candidates = sorted({0.1, 0.2, 0.3, 0.4, 0.5, 0.6, round(tilt_policy.threshold, 4)})
    win_threshold_rows = with_selected_threshold(
        threshold_table(y_val_win.to_numpy(), win_probs, win_threshold_candidates),
        win_policy.threshold,
    )
    tilt_threshold_rows = with_selected_threshold(
        threshold_table(y_val_tilt.to_numpy(), tilt_probs, tilt_threshold_candidates),
        tilt_policy.threshold,
    )
    win_calibration_rows = calibration_table(y_val_win.to_numpy(), win_probs, bins=10)
    tilt_calibration_rows = calibration_table(y_val_tilt.to_numpy(), tilt_probs, bins=10)

    folds = build_temporal_folds(
        df,
        min_train_rows=args.min_fold_train_rows,
        min_val_rows=args.min_fold_val_rows,
    )
    win_backtest_rows = temporal_backtest_rows(folds, "win", win_numeric, win_cat)
    tilt_backtest_rows = temporal_backtest_rows(folds, "tilt", tilt_numeric, tilt_cat)
    impact_backtest_rows = temporal_backtest_rows(folds, "impact", impact_numeric, impact_cat)

    win_top_features = top_feature_effects(
        win_model,
        x_val_win,
        y_val_win.to_numpy(dtype=int),
        is_classification=True,
    )
    tilt_top_features = top_feature_effects(
        tilt_model,
        x_val_tilt,
        y_val_tilt.to_numpy(dtype=int),
        is_classification=True,
    )
    impact_top_features = top_feature_effects(
        impact_model,
        x_val_impact,
        y_val_impact.to_numpy(dtype=float),
        is_classification=False,
    )

    win_segment_rows = classification_metrics_by_segment(
        y_true=y_val_win.to_numpy(dtype=int),
        probs=win_probs,
        segments=val_df["momentum_label"],
        segment_name="momentum_label",
        threshold=win_policy.threshold,
    )
    tilt_segment_rows = classification_metrics_by_segment(
        y_true=y_val_tilt.to_numpy(dtype=int),
        probs=tilt_probs,
        segments=val_df["momentum_label"],
        segment_name="momentum_label",
        threshold=tilt_policy.threshold,
    )
    impact_segment_rows = regression_metrics_by_segment(
        y_true=y_val_impact.to_numpy(dtype=float),
        preds=impact_preds,
        segments=val_df["momentum_label"],
        segment_name="momentum_label",
    )

    artifact_root = pathlib.Path(args.artifact_root).resolve()
    report_path = pathlib.Path(args.report_path).resolve()
    metric_by_model: Dict[str, Dict[str, float]] = {
        "win_probability_baseline": win_metrics,
        "impact_percentile_baseline": impact_metrics,
        "tilt_risk_baseline": tilt_metrics,
    }
    promotion_gate_rows, promotion_ready_by_model = build_promotion_gates(
        win_metrics=win_metrics,
        impact_metrics=impact_metrics,
        tilt_metrics=tilt_metrics,
        win_backtest_rows=win_backtest_rows,
        impact_backtest_rows=impact_backtest_rows,
        tilt_backtest_rows=tilt_backtest_rows,
        win_min_f1=args.gate_win_min_f1,
        win_max_brier=args.gate_win_max_brier,
        win_min_fold_f1=args.gate_win_min_fold_f1,
        impact_max_rmse=args.gate_impact_max_rmse,
        impact_max_mae=args.gate_impact_max_mae,
        impact_max_fold_rmse=args.gate_impact_max_fold_rmse,
        tilt_min_f1=args.gate_tilt_min_f1,
        tilt_max_brier=args.gate_tilt_max_brier,
        tilt_min_recall=args.gate_tilt_min_recall,
        tilt_max_fold_f1_std=args.gate_tilt_max_fold_f1_std,
    )

    model_bundle = [
        (
            "win_probability_baseline",
            win_model,
            "classification",
            win_metrics,
            f"Calibrated win probabilities; operating threshold={win_policy.threshold:.2f}; promotion_ready={promotion_ready_by_model['win_probability_baseline']}.",
        ),
        (
            "impact_percentile_baseline",
            impact_model,
            "regression",
            impact_metrics,
            f"Non-linear impact baseline; promotion_ready={promotion_ready_by_model['impact_percentile_baseline']}.",
        ),
        (
            "tilt_risk_baseline",
            tilt_model,
            "classification",
            tilt_metrics,
            f"Calibrated tilt probabilities; operating threshold={tilt_policy.threshold:.2f}; promotion_ready={promotion_ready_by_model['tilt_risk_baseline']}.",
        ),
    ]

    ensure_validation_metrics_table(trino)
    validation_metrics_rows_by_model: Dict[str, List[Dict[str, Any]]] = {
        "win_probability_baseline": classification_daily_metrics_rows(
            val_df,
            y_val_win,
            win_probs,
            threshold=win_policy.threshold,
        ),
        "tilt_risk_baseline": classification_daily_metrics_rows(
            val_df,
            y_val_tilt,
            tilt_probs,
            threshold=tilt_policy.threshold,
        ),
        "impact_percentile_baseline": regression_daily_metrics_rows(val_df, y_val_impact, impact_preds),
    }

    trained_at = utc_now_iso()
    for model_name, model_obj, task_type, metrics, notes in model_bundle:
        model_gate_rows = [row for row in promotion_gate_rows if row["model_name"] == model_name]
        threshold_policy_payload: Dict[str, Any] | None = None
        if model_name == "win_probability_baseline":
            threshold_policy_payload = {
                "threshold": win_policy.threshold,
                "min_precision": win_policy.min_precision,
                "min_recall": win_policy.min_recall,
                "selected_by_constraints": win_policy.selected_by_constraints,
            }
        elif model_name == "tilt_risk_baseline":
            threshold_policy_payload = {
                "threshold": tilt_policy.threshold,
                "min_precision": tilt_policy.min_precision,
                "min_recall": tilt_policy.min_recall,
                "selected_by_constraints": tilt_policy.selected_by_constraints,
            }
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
                "threshold_policy": threshold_policy_payload,
                "promotion_ready": promotion_ready_by_model.get(model_name, False),
                "promotion_gates": model_gate_rows,
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
        upsert_validation_metrics_daily(
            trino=trino,
            model_name=model_name,
            model_version=args.model_version,
            task_type=task_type,
            snapshot_id=snapshot_id,
            rows=validation_metrics_rows_by_model.get(model_name, []),
        )

    write_report(
        path=report_path,
        snapshot_id=snapshot_id,
        train_df=train_df,
        val_df=val_df,
        metric_by_model=metric_by_model,
        win_threshold_policy=win_policy,
        tilt_threshold_policy=tilt_policy,
        win_threshold_rows=win_threshold_rows,
        tilt_threshold_rows=tilt_threshold_rows,
        win_calibration_rows=win_calibration_rows,
        tilt_calibration_rows=tilt_calibration_rows,
        win_top_features=win_top_features,
        tilt_top_features=tilt_top_features,
        impact_top_features=impact_top_features,
        win_backtest_rows=win_backtest_rows,
        tilt_backtest_rows=tilt_backtest_rows,
        impact_backtest_rows=impact_backtest_rows,
        win_segment_rows=win_segment_rows,
        tilt_segment_rows=tilt_segment_rows,
        impact_segment_rows=impact_segment_rows,
        promotion_gate_rows=promotion_gate_rows,
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
    print(f"Temporal folds evaluated: {len(folds)}")
    print("Promotion readiness:", json.dumps(promotion_ready_by_model, sort_keys=True))
    failed_gates = [row for row in promotion_gate_rows if row["status"] != "PASS"]
    print(f"Promotion gate failures: {len(failed_gates)}")
    print(f"Report: {report_path}")
    print(f"Artifacts root: {artifact_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
