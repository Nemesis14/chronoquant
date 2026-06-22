"""Load champion models and generate unified long/short predictions from feature data.

Resolves the asset-specific champion long and short model, loads feature rows from
feat_ohlcv_quant, joins target labels from the target table, runs inference for each
model, and writes a single unified row per open_time with stable long_pred/short_pred
columns.  Idempotent by open_time — safe to re-run.

Deploy / cutover
----------------
The live sync loop additionally checks ``reg.deployments`` for a pending deployment
(status='pending') on each run.  When found, it executes an atomic backfill+swap:

    BEGIN;
        DELETE FROM predictions;
        INSERT INTO predictions SELECT ... FROM lab.model."<new_model>__pred" ...;
    COMMIT;

The trading service never sees a partial table (DuckDB MVCC guarantees a consistent
snapshot for concurrent readers).  After the swap the deployment row is promoted to
``active`` (active=True, activated_at=now(), previous_strategy_id=<prev>).

Design choice — registry-intent-based detection
------------------------------------------------
The sync writer detects pending deployments by querying ``reg.deployments`` instead of
a separate CLI trigger.  This avoids a second writer race and keeps the hot-path in one
process.  The CLI (``06_trigger_deploy.py``) inserts the pending row; the next sync cycle
picks it up and executes the swap atomically.  This matches the "live writer detects intent"
pattern from the architecture plan (section 5.2).

Data flow
---------
feat_ohlcv_quant + target  →  JOIN on open_time  →  inference  →  predictions
reg.deployments(pending)   →  backfill+swap  →  predictions  (atomic)
"""

import json
import logging
import os
import pickle
from datetime import timedelta
from typing import Any

import numpy as np
import polars as pl

import utils
from data_handling.store.duckdb_query import query_range_pl
from data_handling.store.duckdb_store import (
    backfill_predictions,
    ensure_tables,
    get_connection,
    insert_predictions,
)

logger = logging.getLogger(__name__)

_LONG_PRED_COL  = "long_pred"
_SHORT_PRED_COL = "short_pred"


# %% Public entry point


def sync_predictions(
    start_time : str,
    end_time   : str | None = None,
    asset_id   : str | None = None,
    backfill   : bool = False,
) -> None:
    """Sync predictions for the champion long and short models.

    Resolves the champion long and short model for the asset, loads feature
    rows once with both target columns, runs inference, and writes a single
    unified df per open_time with stable long_pred/short_pred columns.

    Additionally checks ``reg.deployments`` for a pending deployment.  When
    a pending deployment is found the predictions table is atomically replaced
    (backfill+swap) and the deployment row is promoted to active.

    Args:
        start_time : Start of the prediction window (UTC string).
        end_time   : End of the prediction window (UTC string, or None for latest).
        asset_id   : Asset ID from config/assets.json; uses default if None.
        backfill   : If True, use gap-fill insert (skip existing open_times).
    """
    # --- config ---
    resolved_asset = utils.resolve_asset_id(asset_id)
    db_cfg         = utils.load_asset_config(resolved_asset)
    feat_cfg       = utils.load_features_config(asset_id=resolved_asset)
    model_cfg      = utils.load_models_config()
    db_path        = db_cfg["database"]["db_path"]

    _targets     = feat_cfg["database"]["features"].get("targets", [])
    _fw_minutes  = _targets[0]["rolling_window"] if _targets else 60

    # --- champion model resolution ---
    try:
        long_id, long_meta, short_id, short_meta = utils.champion_models_for_asset(
            model_cfg, resolved_asset
        )
    except ValueError as exc:
        logger.warning("Champion model feloldas sikertelen: %s", exc)
        return

    long_target  = long_meta["target_name"]
    short_target = short_meta["target_name"]

    # --- load model artifacts ---
    long_artifacts = _load_model_artifacts(long_id, long_meta)
    if long_artifacts is None:
        return
    long_model, long_feat_list = long_artifacts

    short_artifacts = _load_model_artifacts(short_id, short_meta)
    if short_artifacts is None:
        return
    short_model, short_feat_list = short_artifacts

    # --- feature query from feat_ohlcv_quant ---
    all_features = list(dict.fromkeys(long_feat_list + short_feat_list))
    feat_select  = list(dict.fromkeys(["open_time", "close"] + all_features))

    feat_df = query_range_pl(db_path, "feat_ohlcv_quant", start=start_time, end=end_time, columns=feat_select)
    if feat_df.is_empty():
        logger.warning(
            "Nincs feature sor: asset_id=%s start_time=%s", resolved_asset, start_time
        )
        return

    # --- target query from target table ---
    target_select = list(dict.fromkeys(["open_time", long_target, short_target]))
    target_df = query_range_pl(db_path, "target", start=start_time, end=end_time, columns=target_select)

    # --- join features and targets on open_time ---
    if not target_df.is_empty():
        feat_df = feat_df.join(target_df, on="open_time", how="left")
    else:
        logger.warning(
            "Nincs target adat: asset_id=%s — predictions target oszlopok NULL lesznek",
            resolved_asset,
        )
        feat_df = feat_df.with_columns([
            pl.lit(None).cast(pl.Float64).alias(long_target),
            pl.lit(None).cast(pl.Float64).alias(short_target),
        ])

    df = feat_df.unique(subset=["open_time"], keep="last", maintain_order=True)

    # --- inference ---
    long_proba  = _run_inference(df, long_feat_list,  long_model,  long_meta)
    short_proba = _run_inference(df, short_feat_list, short_model, short_meta)

    # --- unified output (index-aligned, single upsert via Polars) ---
    df_out = pl.DataFrame({
        "open_time"      : df["open_time"],
        "close"          : df["close"],
        "label_end_ts"   : df["open_time"] + timedelta(minutes=_fw_minutes),
        long_target      : df[long_target],
        short_target     : df[short_target],
        _LONG_PRED_COL   : long_proba.tolist(),
        _SHORT_PRED_COL  : short_proba.tolist(),
        "long_model_id"  : [long_id]  * len(df),
        "short_model_id" : [short_id] * len(df),
    })

    conn = get_connection(db_path)
    ensure_tables(conn)
    try:
        # --- deploy/cutover: check for pending deployment ---
        pending = _detect_pending_deployment(resolved_asset)
        if pending is not None:
            _execute_cutover(conn, db_path, pending, resolved_asset)
        else:
            written = (
                backfill_predictions(conn, df_out)
                if backfill
                else insert_predictions(conn, df_out)
            )
            logger.info(
                "OK: %d predikció irva (long=%s, short=%s), %d uj sor DuckDB-ba",
                len(df_out), long_id, short_id, written,
            )
    finally:
        conn.close()


# %% Deploy / cutover helpers


def _detect_pending_deployment(asset_id: str) -> dict[str, Any] | None:
    """Query reg.deployments for a pending deployment for the given asset.

    Returns the pending deployment row dict, or None if there is none.

    Args:
        asset_id : Asset ID to check (e.g. 'solusdt').

    Returns:
        Deployment row as a dict (keys: deployment_id, asset_id, strategy_id,
        active, status, activated_at, previous_strategy_id, created_at, updated_at),
        or None when no pending deployment exists.
    """
    try:
        reg_path = utils.registry_path()
    except Exception as exc:
        logger.debug("Registry path resolution failed, skipping deploy check: %s", exc)
        return None

    try:
        import duckdb

        conn = duckdb.connect(reg_path)
        try:
            # Ensure the lifecycle columns exist (migration may be pending on this DB)
            for col_def in [
                "activated_at TIMESTAMP",
                "previous_strategy_id VARCHAR",
            ]:
                col_name = col_def.split()[0]
                try:
                    conn.execute(
                        f"ALTER TABLE deployments ADD COLUMN IF NOT EXISTS {col_def}"
                    )
                except Exception:
                    logger.debug("Migration skip in registry: %s", col_name)

            cursor = conn.execute(
                "SELECT * FROM deployments WHERE asset_id = ? AND status = 'pending'",
                [asset_id],
            )
            row = cursor.fetchone()
            if row is None:
                return None
            columns = [d[0] for d in cursor.description]
            return dict(zip(columns, row, strict=True))
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("Deploy check failed (registry unavailable): %s", exc)
        return None


def _execute_cutover(
    conn       : Any,
    db_path    : str,
    deployment : dict[str, Any],
    asset_id   : str,
    reg_path   : str | None = None,
    lab_path   : str | None = None,
) -> None:
    """Atomically replace the predictions table and activate the deployment.

    The swap is performed inside a single DuckDB transaction so the trading
    service never observes a partial table (DuckDB MVCC guarantees a consistent
    snapshot for concurrent readers).

    Steps inside the transaction:
      1. Resolve the new strategy from reg.strategies (strategy_id → model_id_long/short).
      2. Resolve the new model pred tables from lab.duckdb (model.__pred).
      3. DELETE all rows from predictions.
      4. INSERT INTO predictions SELECT ... FROM lab.model."<id>__pred" (both directions).
      5. COMMIT — the trading service sees the full new table from this point on.

    After the transaction, update reg.deployments: pending → active.

    Args:
        conn       : Open live DuckDB connection (the predictions writer).
        db_path    : Absolute path to the live .duckdb file.
        deployment : Pending deployment row dict from _detect_pending_deployment.
        asset_id   : Resolved asset ID.
        reg_path   : Override for the registry path (default: utils.registry_path()).
        lab_path   : Override for the lab DB path (default: utils.lab_db_path(asset_id)).
    """
    deployment_id    = deployment["deployment_id"]
    strategy_id      = deployment["strategy_id"]
    resolved_reg     = reg_path  or utils.registry_path()
    resolved_lab     = lab_path  or utils.lab_db_path(asset_id)

    logger.info(
        "Deploy/cutover indul: deployment_id=%s strategy_id=%s asset_id=%s",
        deployment_id, strategy_id, asset_id,
    )

    # --- resolve strategy → model ids ---
    strategy = _resolve_strategy(strategy_id, resolved_reg)
    if strategy is None:
        logger.error(
            "Deploy ABORT: strategy_id=%s not found in registry, deployment_id=%s",
            strategy_id, deployment_id,
        )
        return

    long_model_id  = strategy.get("model_id_long")
    short_model_id = strategy.get("model_id_short")
    if not long_model_id or not short_model_id:
        logger.error(
            "Deploy ABORT: strategy %s has no model_id_long/short", strategy_id
        )
        return

    # --- resolve lab pred tables and build full-range DataFrame ---
    df_new = _load_pred_table_as_df(resolved_lab, long_model_id, short_model_id, db_path)
    if df_new is None:
        logger.error(
            "Deploy ABORT: cannot load pred tables for strategy=%s", strategy_id
        )
        return

    # --- find current active deployment for rollback reference ---
    previous_strategy_id = _find_active_strategy_id(asset_id, resolved_reg)

    # --- atomic backfill+swap inside a single transaction ---
    # DuckDB MVCC: concurrent readers hold a consistent snapshot of the table
    # state at their transaction start; they will not see partial inserts.
    conn.execute("BEGIN")
    try:
        conn.execute("DELETE FROM predictions")
        col_list = ", ".join(f'"{c}"' for c in df_new.columns)
        conn.register("_cutover_batch", df_new)
        conn.execute(
            f"INSERT INTO predictions ({col_list}) SELECT {col_list} FROM _cutover_batch"
        )
        conn.unregister("_cutover_batch")
        conn.execute("COMMIT")
    except Exception as exc:
        conn.execute("ROLLBACK")
        logger.error("Deploy ROLLBACK: %s — predictions table unchanged", exc)
        return

    rows_after = conn.execute("SELECT COUNT(*) FROM predictions").fetchone()
    n_rows = int(rows_after[0]) if rows_after else 0
    logger.info(
        "Deploy/cutover OK: predictions replaced, rows=%d long=%s short=%s",
        n_rows, long_model_id, short_model_id,
    )

    # --- promote deployment: pending → active ---
    _activate_deployment(deployment_id, previous_strategy_id, resolved_reg)


def _resolve_strategy(
    strategy_id : str,
    reg_path    : str | None = None,
) -> dict[str, Any] | None:
    """Fetch a reg.strategies row by strategy_id.

    Args:
        strategy_id : PK of the strategy to look up.
        reg_path    : Path to registry.duckdb (default: utils.registry_path()).

    Returns the row as a dict, or None if not found.
    """
    try:
        from data_handling.store.registry import get, open_crud_connection

        path = reg_path or utils.registry_path()
        conn = open_crud_connection(path)
        try:
            return get(conn, "strategies", strategy_id)
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("_resolve_strategy failed: %s", exc)
        return None


def _load_pred_table_as_df(
    lab_path       : str,
    long_model_id  : str,
    short_model_id : str,
    live_path      : str,
) -> "pl.DataFrame | None":
    """Load the full-range offline predictions for both models from the lab database.

    Joins model."<long_id>__pred" and model."<short_id>__pred" on open_time,
    adds stamp columns (long_model_id, short_model_id), and projects the result
    to the predictions table schema.

    Returns a Polars DataFrame ready for insertion, or None on failure.

    Args:
        lab_path       : Absolute path to the lab .duckdb.
        long_model_id  : Long-direction model ID.
        short_model_id : Short-direction model ID.
        live_path      : Absolute path to the live .duckdb (for OHLCV close join).
    """
    import duckdb

    try:
        lab_conn = duckdb.connect(lab_path, read_only=True)
        try:
            # Validate pred tables exist
            for mid in (long_model_id, short_model_id):
                tbl = f'model."{mid}__pred"'
                row = lab_conn.execute(
                    "SELECT COUNT(*) FROM information_schema.tables"
                    " WHERE table_schema = 'model' AND table_name = ?",
                    [f"{mid}__pred"],
                ).fetchone()
                if not row or row[0] == 0:
                    logger.error("Deploy ABORT: pred table not found: %s", tbl)
                    return None

            # Join both pred tables on open_time; also attach live for close prices
            try:
                lab_conn.execute(f"ATTACH '{live_path}' AS _live_ro (READ_ONLY)")
                close_join = "LEFT JOIN _live_ro.ohlcv o ON o.open_time = pl.open_time"
                close_col  = "COALESCE(o.close, NULL) AS close"
            except Exception:
                close_join = ""
                close_col  = "NULL::DOUBLE AS close"

            result = lab_conn.execute(f"""
                SELECT
                    pl.open_time,
                    {close_col},
                    NULL::TIMESTAMP AS label_end_ts,
                    NULL::DOUBLE    AS long_mfe_fw60,
                    NULL::DOUBLE    AS short_mfe_fw60,
                    pl.pred         AS long_pred,
                    ps.pred         AS short_pred,
                    '{long_model_id}'  AS long_model_id,
                    '{short_model_id}' AS short_model_id
                FROM model."{long_model_id}__pred" pl
                JOIN model."{short_model_id}__pred" ps ON ps.open_time = pl.open_time
                {close_join}
                ORDER BY pl.open_time
            """).pl()

            return result
        finally:
            lab_conn.close()
    except Exception as exc:
        logger.error("_load_pred_table_as_df failed: %s", exc)
        return None


def _find_active_strategy_id(
    asset_id : str,
    reg_path : str | None = None,
) -> str | None:
    """Return the strategy_id of the currently active deployment, if any.

    Used to populate previous_strategy_id for rollback purposes.

    Args:
        asset_id : Asset ID to query.
        reg_path : Path to registry.duckdb (default: utils.registry_path()).
    """
    try:
        import duckdb

        path = reg_path or utils.registry_path()
        conn = duckdb.connect(path, read_only=True)
        try:
            row = conn.execute(
                "SELECT strategy_id FROM deployments"
                " WHERE asset_id = ? AND active = TRUE"
                " ORDER BY updated_at DESC LIMIT 1",
                [asset_id],
            ).fetchone()
            return str(row[0]) if row else None
        finally:
            conn.close()
    except Exception as exc:
        logger.debug("_find_active_strategy_id failed: %s", exc)
        return None


def _activate_deployment(
    deployment_id        : str,
    previous_strategy_id : str | None,
    reg_path             : str | None = None,
) -> None:
    """Promote a pending deployment to active in reg.deployments.

    Sets active=True, status='active', activated_at=now(), and records the
    previous_strategy_id for rollback.  All previously active deployments for
    the same asset are deactivated first (active=False, status='archived').

    Args:
        deployment_id        : PK of the deployment to activate.
        previous_strategy_id : strategy_id that was active before the swap.
        reg_path             : Path to registry.duckdb (default: utils.registry_path()).
    """
    try:
        import duckdb

        path = reg_path or utils.registry_path()
        conn = duckdb.connect(path)
        try:
            # Fetch asset_id for deactivation of old rows
            row = conn.execute(
                "SELECT asset_id FROM deployments WHERE deployment_id = ?",
                [deployment_id],
            ).fetchone()
            if row is None:
                logger.warning("_activate_deployment: deployment_id=%s not found", deployment_id)
                return
            asset_id = row[0]

            # Deactivate all currently active deployments for this asset
            conn.execute(
                "UPDATE deployments SET active = FALSE, status = 'archived',"
                " updated_at = now()"
                " WHERE asset_id = ? AND active = TRUE AND deployment_id != ?",
                [asset_id, deployment_id],
            )

            # Activate the new deployment
            conn.execute(
                "UPDATE deployments SET"
                "  active = TRUE,"
                "  status = 'active',"
                "  activated_at = now(),"
                "  previous_strategy_id = ?,"
                "  updated_at = now()"
                " WHERE deployment_id = ?",
                [previous_strategy_id, deployment_id],
            )
            logger.info(
                "reg.deployments updated: deployment_id=%s → active"
                " (previous_strategy_id=%s)",
                deployment_id, previous_strategy_id,
            )
        finally:
            conn.close()
    except Exception as exc:
        logger.error("_activate_deployment failed (predictions swap already committed): %s", exc)


# %% Private helpers


def _load_model_artifacts(
    model_id   : str,
    model_meta : dict,
) -> tuple[object, list[str]] | None:
    """Load the pickled model and feature list for a model config entry.

    Args:
        model_id   : Model ID string (used only for log messages).
        model_meta : Model metadata dict from models.json.

    Returns:
        (model, feature_list) on success, (None, None) if artifacts are missing.
    """
    model_dir = utils._resolve_path(model_meta["artifact_dir"])
    feat_path = os.path.join(model_dir, "features.json")
    mdl_path  = os.path.join(model_dir, "model.pkl")

    if not os.path.exists(feat_path) or not os.path.exists(mdl_path):
        logger.warning("Model artifact nem talalhato: model_id=%s, kihagyva", model_id)
        return None

    with open(feat_path, encoding="utf-8") as f:
        features_data = json.load(f)
    with open(mdl_path, "rb") as f:
        model = pickle.load(f)

    feature_list = _feature_list_for_prediction(
        features_data = features_data,
        model         = model,
        trainer       = model_meta.get("trainer", ""),
    )
    return model, feature_list


def _run_inference(
    df           : pl.DataFrame,
    feature_list : list[str],
    model        : object,
    model_meta   : dict,
) -> np.ndarray:
    """Run model inference and return a probability array.

    Args:
        df           : Feature DataFrame (all rows, all columns available).
        feature_list : Columns to use as model input.
        model        : Loaded model object.
        model_meta   : Model metadata dict from models.json.

    Returns:
        Array-like of probability values aligned to df rows.
    """
    trainer        = model_meta.get("trainer", "")
    predict_method = model_meta.get("predict", {}).get("method", "predict")

    X = df.select(feature_list).fill_nan(0.0).fill_null(0.0).to_numpy()
    if trainer.startswith("statsmodels") and "const" not in feature_list:
        X = np.column_stack([np.ones(len(X)), X])

    if predict_method == "predict_proba":
        proba: Any = model.predict_proba(X)  # type: ignore[union-attr]
        if hasattr(proba, "shape") and len(proba.shape) == 2:
            proba = proba[:, 1]
    else:
        proba = model.predict(X)  # type: ignore[union-attr]

    return np.asarray(proba, dtype=float)


def _feature_list_for_prediction(
    features_data : dict | list,
    model         : object,
    trainer       : str = "",
) -> list[str]:
    """Resolve the ordered feature list for prediction from artifact metadata.

    Args:
        features_data : Loaded features.json payload (dict or list).
        model         : Loaded model object (may expose feature_names_in_).
        trainer       : Trainer string from model metadata.

    Returns:
        Ordered list of feature column names.
    """
    payload_features = _features_from_payload(features_data)
    if trainer.startswith("statsmodels"):
        return payload_features

    input_features = (
        features_data.get("input_features")
        if isinstance(features_data, dict)
        else None
    )
    if input_features:
        return list(input_features)

    feature_names_in = getattr(model, "feature_names_in_", None)
    if feature_names_in is not None and len(feature_names_in) > 0:
        return list(feature_names_in)

    feature_name = getattr(model, "feature_name_", None)
    if feature_name is not None and len(feature_name) > 0:
        return list(feature_name)

    return payload_features


def _features_from_payload(features_data: dict | list) -> list[str]:
    """Extract the raw feature name list from a features.json payload.

    Args:
        features_data : Loaded features.json (dict with 'features' key, or list).

    Returns:
        List of feature column names.
    """
    if isinstance(features_data, dict):
        return list(features_data["features"])
    return list(features_data)
