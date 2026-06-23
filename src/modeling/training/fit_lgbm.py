"""LightGBM fit from search artifacts — single fit, no CV sweep.

Reads best_params.json + search_best.json (n_estimators), feature_set.json.
Loads training data via snap ⋈ model.__sample JOIN (DuckDB-native, plan 5.1):
  - source: snap."<snapshot_id>" (features) INNER JOIN model."<model_id>__sample"
    (open_time + target + fold_id) on open_time
  - uses ALL fold rows (fold_id 0 = train-only, fold_id 1..n = walk-forward folds)
    for the final production fit

Offline prediction is a separate pipeline step (modeling.predict) that scores the
full snapshot range into model."<id>__pred" — it is no longer fused into the
sample here (t315).
"""

from __future__ import annotations

import json
import logging
import pickle
from pathlib import Path

import lightgbm as lgb
import pandas as pd

import utils

logger = logging.getLogger(__name__)

_FIXED_PARAMS: dict = {
    "objective":      "regression",
    "boosting_type":  "gbdt",
    "metric":         "rmse",
    "subsample_freq": 1,
    "force_col_wise": True,
    "verbosity":      -1,
    "n_jobs":         4,
}


def fit_lightgbm_from_search(model_id: str) -> dict:
    """Fit a LightGBM model from search artifacts.

    Training data is loaded via the snap ⋈ model.__sample JOIN (DuckDB-native,
    plan 5.1) — all fold rows are used for the final production fit.  Offline
    scoring of the full snapshot range is the separate predict step.

    Args:
        model_id: Key from config/models.json.

    Returns:
        Dict with model_id, n_estimators, n_features, selected_features,
        artifact_dir.
    """
    models_cfg = utils.load_models_config()
    if model_id not in models_cfg.get("models", {}):
        raise ValueError(f"Model not found in config/models.json: {model_id}")

    meta         = models_cfg["models"][model_id]
    artifact_dir = Path(utils._resolve_path(meta["artifact_dir"]))
    target_name  = meta["target_name"]

    best_params: dict = json.loads(
        (artifact_dir / "search" / "best_params.json").read_text(encoding="utf-8")
    )
    search_best: dict = json.loads(
        (artifact_dir / "search" / "search_best.json").read_text(encoding="utf-8")
    )
    feature_set: dict = json.loads(
        (artifact_dir / "feature_engineering" / "feature_set.json").read_text(encoding="utf-8")
    )
    selected_features: list[str] = feature_set["selected"]

    best_iterations = [int(fold["best_iteration"]) for fold in search_best["fold_summary"]]
    n_estimators    = round(sum(best_iterations) / len(best_iterations) * 1.1)

    final_params: dict = {**_FIXED_PARAMS, **best_params, "n_estimators": n_estimators, "random_state": 42}

    X_train, y_train = _load_train_data(model_id, selected_features, target_name)

    logger.info(
        "[fit_lgbm] Fitting %s: n_samples=%d, n_features=%d, n_estimators=%d",
        model_id, len(y_train), len(selected_features), n_estimators,
    )

    model = lgb.LGBMRegressor(**final_params)
    model.fit(X_train, y_train)

    _save_artifacts(artifact_dir, model, selected_features, final_params, n_estimators)

    # Offline prediction is no longer fused into the sample (t315): the predict
    # step scores the full snapshot range into a separate model."<id>__pred" table
    # so the snapshot/sample stay clean. See modeling.predict.predict_offline.

    # Provenance: mark reg.models 'trained' (oos_metric + search_run link) and
    # register the produced artifact paths in reg.artifacts (plan 5 step 5).
    from modeling.training.artifacts import register_training_artifacts
    register_training_artifacts(
        model_id      = model_id,
        output_dir    = artifact_dir,
        oos_metric    = search_best.get("objective_score"),
        asset_id      = meta.get("asset_id"),
    )

    return {
        "model_id":          model_id,
        "n_estimators":      n_estimators,
        "n_features":        len(selected_features),
        "selected_features": selected_features,
        "artifact_dir":      str(artifact_dir),
    }


def _load_train_data(
    model_id         : str,
    selected_features: list[str],
    target_name      : str,
) -> tuple[pd.DataFrame, pd.Series]:
    """Load training matrix from snap ⋈ model.__sample JOIN (plan 5.1).

    Joins the immutable snapshot (feature columns) with the model's small
    sample table (open_time + target + fold_id) on open_time.  All fold rows
    are used for the final production fit (fold_id 0 = train-only rows,
    fold_id 1..n = walk-forward validation folds — both included).

    Args:
        model_id          : Model key from config/models.json (used to resolve
                            the snapshot_id and table names).
        selected_features : Feature columns to load from the snapshot.
        target_name       : Target column to load from the sample table.

    Returns:
        (X_train, y_train).
    """
    # --- resolve snapshot_id from registry or model config ---
    models_cfg  = utils.load_models_config()
    meta        = models_cfg["models"][model_id]
    asset_id    = meta.get("asset_id")
    snapshot_id = meta.get("sampling", {}).get("snapshot_id")

    conn = utils.open_lab_connection(asset_id)
    try:
        if not snapshot_id:
            # fall back to reg.models if config is missing snapshot_id
            row = conn.execute(
                "SELECT snapshot_id FROM reg.models WHERE model_id = ?", [model_id]
            ).fetchone()
            if row:
                snapshot_id = row[0]
        if not snapshot_id:
            raise ValueError(
                f"Cannot resolve snapshot_id for model {model_id} — set "
                "sampling.snapshot_id in config/models.json or run the sample step first."
            )

        feat_cols_sql = ", ".join(f's."{c}"' for c in selected_features)
        sql = f"""
            SELECT
                s.open_time,
                m."{target_name}",
                {feat_cols_sql}
            FROM snap."{snapshot_id}" AS s
            INNER JOIN model."{model_id}__sample" AS m ON s.open_time = m.open_time
            ORDER BY s.open_time
        """
        df = conn.execute(sql).df()

        # I2 invariant logging: joined rows should equal model.__sample rowcount.
        sample_row = conn.execute(
            f'SELECT COUNT(*) FROM model."{model_id}__sample"'
        ).fetchone()
        sample_count = int(sample_row[0]) if sample_row else -1
    finally:
        conn.close()

    logger.info(
        "[fit_lgbm] _load_train_data: model=%s  joined_rows=%d  sample_rows=%d"
        "  (I2: snap ⋈ model.__sample)",
        model_id, len(df), sample_count,
    )

    df["open_time"] = pd.to_datetime(df["open_time"])

    X_train = df[selected_features]
    y_train = df[target_name].astype(float)

    return pd.DataFrame(X_train), pd.Series(y_train)


def _save_artifacts(
    artifact_dir     : Path,
    model            : lgb.LGBMRegressor,
    selected_features: list[str],
    params           : dict,
    n_estimators     : int,
) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)

    with open(artifact_dir / "model.pkl", "wb") as f:
        pickle.dump(model, f)

    (artifact_dir / "features.json").write_text(
        json.dumps({"features": selected_features}, indent=4), encoding="utf-8"
    )
    (artifact_dir / "params.json").write_text(
        json.dumps({"params": params, "n_estimators": n_estimators}, indent=4), encoding="utf-8"
    )
