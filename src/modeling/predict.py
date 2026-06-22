"""Offline predict step — score the full snapshot range into ``model."<id>__pred"``.

Plan 5, step 6: after training, the model scores the *entire* immutable snapshot
range with its ``model.pkl`` and writes the result to a separate
``model."<model_id>__pred"`` table (``open_time``, ``pred``).  The prediction is
**not** fused back into the snapshot — keeping the snapshot immutable and its
``content_sha256`` (in ``reg.snapshots``) intact, so ``snap ⋈ model.__pred`` is a
plain join on ``open_time`` and the snapshot stays reproducible.

The feature inputs come from the snapshot itself, projected to the model's
selected feature columns (``features.json`` -> the FE-selected feature_set).
Only this module owns the lab connection here; it reuses the t314 provenance
gateway to flip ``reg.models`` to ``predicted`` and the snapshot layer's hashing
to verify immutability.

Reference: _doc_/_plans_/data_process_architecture.md section 5 (step 6), 5.1, 7.
"""

from __future__ import annotations

import json
import logging
import pickle
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

import utils
from data_handling.store import registry
from modeling.sampling.snapshot_sampler import pred_table_fqn, snapshot_table_fqn

logger = logging.getLogger(__name__)

# reg.artifacts kind for the offline prediction table (a DuckDB-table output, not a file).
PRED_ARTIFACT_KIND = "pred_table"


def predict_offline(model_id: str, verify_snapshot: bool = True) -> dict[str, Any]:
    """Score the full snapshot range and write ``model."<model_id>__pred"``.

    Steps (plan 5, step 6):
      1. load ``model.pkl`` + the selected feature columns (``features.json``),
      2. resolve the immutable ``snapshot_id`` (reg.models, falling back to config),
      3. read ``open_time`` + the selected features for the *entire* snapshot range,
      4. score and write ``model."<model_id>__pred"`` (open_time, pred) — a separate
         table, never fused into the snapshot,
      5. verify the snapshot content hash is unchanged (immutability),
      6. flip ``reg.models`` to ``predicted`` and register the pred table in
         ``reg.artifacts``.

    Args:
        model_id        : Model key from config/models.json (a trained model).
        verify_snapshot : If True, recompute the snapshot content hash and assert
                          it still matches ``reg.snapshots.content_sha256``.

    Returns:
        Summary dict: ``model_id``, ``snapshot_id``, ``pred_table``, ``n_rows``,
        ``n_features``, ``snapshot_immutable``.

    Raises:
        ValueError: If the model/snapshot is unknown, artifacts are missing, the
                    snapshot has no rows, or (when verifying) the snapshot hash
                    changed.
    """
    models_cfg = utils.load_models_config()
    if model_id not in models_cfg.get("models", {}):
        raise ValueError(f"Model not found in config/models.json: {model_id}")

    meta         = models_cfg["models"][model_id]
    asset_id     = meta["asset_id"]
    artifact_dir = Path(utils._resolve_path(meta["artifact_dir"]))

    model            = _load_model(artifact_dir)
    selected_features = _load_selected_features(artifact_dir)

    conn = utils.open_lab_connection(asset_id)
    try:
        snapshot_id = _resolve_snapshot_id(conn, model_id, meta)
        if not _snapshot_exists(conn, snapshot_id):
            raise ValueError(
                f"Snapshot table snap.\"{snapshot_id}\" not found — create it first "
                "(src/data_handling/05_create_snapshot.py)."
            )

        hash_before = _snapshot_content_sha256(conn, snapshot_id) if verify_snapshot else None

        features_df = _read_snapshot_features(conn, snapshot_id, selected_features)
        if features_df.empty:
            raise ValueError(
                f"Snapshot snap.\"{snapshot_id}\" has no rows to score for {model_id}."
            )

        pred_df = _score(model, features_df, selected_features)
        n_rows  = _write_pred_table(conn, model_id, pred_df)

        snapshot_immutable = True
        if verify_snapshot:
            hash_after = _snapshot_content_sha256(conn, snapshot_id)
            snapshot_immutable = hash_after == hash_before
            if not snapshot_immutable:
                raise ValueError(
                    f"Snapshot snap.\"{snapshot_id}\" content hash changed during "
                    f"predict ({hash_before} -> {hash_after}) — immutability violated."
                )

        _register_pred(conn, model_id, snapshot_id)
    finally:
        conn.close()

    # Provenance: reg.models -> predicted (t314 gateway).
    from modeling import provenance
    provenance.set_model_status(model_id, "predicted", asset_id=asset_id)

    logger.info(
        "predict_offline: %s scored snapshot=%s rows=%d features=%d immutable=%s",
        model_id, snapshot_id, n_rows, len(selected_features), snapshot_immutable,
    )
    return {
        "model_id":           model_id,
        "snapshot_id":        snapshot_id,
        "pred_table":         pred_table_fqn(model_id),
        "n_rows":             n_rows,
        "n_features":         len(selected_features),
        "snapshot_immutable": snapshot_immutable,
    }


# %% Artifact loading


def _load_model(artifact_dir: Path) -> Any:
    """Load the fitted ``model.pkl`` from the artifact directory."""
    model_path = artifact_dir / "model.pkl"
    if not model_path.exists():
        raise ValueError(f"model.pkl not found — train the model first: {model_path}")
    with open(model_path, "rb") as f:
        return pickle.load(f)


def _load_selected_features(artifact_dir: Path) -> list[str]:
    """Read the model's input feature columns from ``features.json`` (``features`` key)."""
    features_path = artifact_dir / "features.json"
    if not features_path.exists():
        raise ValueError(f"features.json not found — train the model first: {features_path}")
    payload  = json.loads(features_path.read_text(encoding="utf-8"))
    features = payload.get("features") or []
    if not features:
        raise ValueError(f"features.json has no 'features' list: {features_path}")
    return list(features)


# %% Snapshot IO


def _resolve_snapshot_id(conn: duckdb.DuckDBPyConnection, model_id: str, meta: dict) -> str:
    """Resolve the model's snapshot_id from reg.models, falling back to config."""
    row = registry.get(conn, "models", model_id)
    if row is not None and row.get("snapshot_id"):
        return str(row["snapshot_id"])
    snapshot_id = meta.get("sampling", {}).get("snapshot_id")
    if not snapshot_id:
        raise ValueError(
            f"No snapshot_id for {model_id} (reg.models nor sampling.snapshot_id)."
        )
    return str(snapshot_id)


def _snapshot_exists(conn: duckdb.DuckDBPyConnection, snapshot_id: str) -> bool:
    """Return True if the snap.\"<snapshot_id>\" table exists in the lab database."""
    row = conn.execute(
        "SELECT 1 FROM information_schema.tables"
        " WHERE table_schema = 'snap' AND table_name = ?",
        [snapshot_id],
    ).fetchone()
    return row is not None


def _read_snapshot_features(
    conn              : duckdb.DuckDBPyConnection,
    snapshot_id       : str,
    selected_features : list[str],
) -> pd.DataFrame:
    """Read ``open_time`` + the selected feature columns for the full snapshot range.

    The features come from the immutable snapshot (read-only); the snapshot is not
    modified.  Rows are ordered by open_time so the written pred table is stable.
    """
    feat_sql = ", ".join(f'"{c}"' for c in selected_features)
    return conn.execute(
        f'SELECT open_time, {feat_sql} FROM {snapshot_table_fqn(snapshot_id)} ORDER BY open_time'
    ).df()


def _score(model: Any, features_df: pd.DataFrame, selected_features: list[str]) -> pd.DataFrame:
    """Score the model over the snapshot rows -> DataFrame(open_time, pred)."""
    x_pred = features_df[selected_features]
    preds  = model.predict(x_pred)
    return pd.DataFrame({"open_time": features_df["open_time"].to_numpy(), "pred": preds})


# %% Pred table write + immutability check


def _write_pred_table(conn: duckdb.DuckDBPyConnection, model_id: str, pred_df: pd.DataFrame) -> int:
    """CREATE OR REPLACE the ``model."<id>__pred"`` table from the scored DataFrame.

    Idempotent: a re-run replaces the table in place (deterministic for a fixed
    model + immutable snapshot).  Only ``open_time`` and ``pred`` are written.
    """
    conn.execute("CREATE SCHEMA IF NOT EXISTS model")
    fqn = pred_table_fqn(model_id)
    conn.register("_pred_df", pred_df)
    try:
        conn.execute(
            f"CREATE OR REPLACE TABLE {fqn} AS "
            "SELECT open_time, CAST(pred AS DOUBLE) AS pred FROM _pred_df ORDER BY open_time"
        )
    finally:
        conn.unregister("_pred_df")
    row = conn.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()
    return int(row[0]) if row else 0


def _snapshot_content_sha256(conn: duckdb.DuckDBPyConnection, snapshot_id: str) -> str:
    """Recompute sha256 over the snapshot table's ordered row content.

    Mirrors the snapshot layer's hash scheme (to_json per row, ordered by
    open_time) so it can be compared with the stored ``reg.snapshots`` value to
    confirm the snapshot was untouched by the predict step.
    """
    row = conn.execute(
        f"""
        WITH ordered AS (
            SELECT to_json(t) AS row_json
            FROM {snapshot_table_fqn(snapshot_id)} t
            ORDER BY open_time
        )
        SELECT COALESCE(sha256(string_agg(row_json, '\n')), sha256('')) FROM ordered
        """
    ).fetchone()
    return str(row[0]) if row else ""


# %% Provenance


def _register_pred(conn: duckdb.DuckDBPyConnection, model_id: str, snapshot_id: str) -> None:
    """Register the pred table as a reg.artifacts row (best-effort, owner = model).

    The offline prediction is a DuckDB-table output; reg.artifacts records its
    fully-qualified table name under ``kind='pred_table'`` so the catalog points
    to it the same way it points to file artifacts.
    """
    try:
        registry.upsert(
            conn,
            "artifacts",
            {
                "artifact_id": f"{model_id}__{PRED_ARTIFACT_KIND}",
                "owner_id":    model_id,
                "kind":        PRED_ARTIFACT_KIND,
                "path":        pred_table_fqn(model_id),
                "status":      "candidate",
            },
        )
    except Exception as exc:  # noqa: BLE001 — provenance must never abort a successful predict
        logger.warning("_register_pred: reg.artifacts write failed: %s", exc)
