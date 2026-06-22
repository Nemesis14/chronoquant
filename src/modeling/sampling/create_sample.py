"""Snapshot-native sampling orchestrator — snap source, model.__sample output, reg.feature_sets.

DuckDB-native sampling path (plan section 5, steps 2-3, and 5.1):

  * **Source** is an immutable ``snap."<snapshot_id>"`` table (frozen by the
    snapshot layer), *not* the mutable ``quant_train`` table — this is what makes
    a model reproducible.
  * **Output** is a small ``model."<model_id>__sample"`` table inside the lab
    database: ``open_time`` + the model's target column(s) + ``fold_id`` only.
    Features stay in the snapshot; downstream steps project them via the
    ``__train_input`` view (plan 5.1) — there is no per-model feature copy.
  * **Feature filtering** is a *logical* feature_set, registered in
    ``reg.feature_sets`` (selected_cols, n_input, n_selected); no physical column
    drop happens here.

The hourly select + walk-forward ``fold_id`` are a single deterministic SQL
statement over the snapshot (see ``snapshot_sampler``).  The walk-forward fold
semantics (train/valid/shift months, n_folds, 240-min purge, ``fold_id`` Int8
0..n with 0 = train-only) reproduce the previous Polars path exactly — only the
source and the output changed, never the methodology.

Only this module touches utils and the lab connection; ``snapshot_sampler`` and
``yearly_sampler`` are IO-free.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import duckdb

import utils
from data_handling.store import registry
from modeling.sampling.snapshot_sampler import (
    build_feature_set_id,
    build_sample_ctas_sql,
    sample_table_fqn,
)
from modeling.sampling.yearly_sampler import generate_walk_forward_folds

logger = logging.getLogger(__name__)

# Map a model target_name to the strategy direction token (plan 6).
_DIRECTION_BY_TARGET_PREFIX = {"long": "l", "short": "s"}


def create_model_sample(model_id: str, snapshot_id: str) -> dict[str, Any]:
    """Create the ``model."<model_id>__sample"`` table for a config-defined model.

    Resolves the model's asset, target column, walk-forward parameters and feature
    selection from config, then samples deterministically over the immutable
    ``snap."<snapshot_id>"`` and registers the logical feature_set.

    Steps (plan 5, steps 2-3):
      1. open the lab connection (lab default + live RO + reg),
      2. generate the walk-forward fold windows (existing semantics),
      3. CTAS ``model."<model_id>__sample"`` = hourly select + fold_id over the snap,
      4. register the feature selection in ``reg.feature_sets`` and link it on
         ``reg.models``.

    Args:
        model_id    : Model key from config/models.json.
        snapshot_id : Immutable snapshot id (``snap."<id>"``) to sample from.

    Returns:
        Summary dict: ``model_id``, ``snapshot_id``, ``sample_table``, ``n_rows``,
        ``fold_row_counts``, ``feature_set_id``, ``n_input``, ``n_selected``.

    Raises:
        ValueError: If the model is unknown or the snapshot has no usable rows.
    """
    models_cfg = utils.load_models_config()
    if model_id not in models_cfg.get("models", {}):
        raise ValueError(f"Model not found in config/models.json: {model_id}")

    meta          = models_cfg["models"][model_id]
    asset_id      = meta["asset_id"]
    target_name   = meta["target_name"]
    sampling_meta = meta.get("sampling", {})
    artifact_dir  = Path(utils._resolve_path(meta["artifact_dir"]))

    horizon      = _horizon_from_target(target_name)
    direction    = _direction_from_target(target_name)
    year         = _anchor_year(sampling_meta)
    seed         = 42 + year
    train_months = int(sampling_meta.get("train_months", 9))
    valid_months = int(sampling_meta.get("valid_months", 3))
    shift_months = int(sampling_meta.get("shift_months", 3))
    n_folds      = int(sampling_meta.get("n_folds", 4))
    purge_minutes = int(sampling_meta.get("purge_minutes", 240))

    selected_cols = _resolve_selected_features(artifact_dir, target_name)

    conn = utils.open_lab_connection(asset_id)
    try:
        return create_snapshot_sample(
            conn          = conn,
            model_id      = model_id,
            snapshot_id   = snapshot_id,
            asset_id      = asset_id,
            target_cols   = [target_name],
            horizon       = horizon,
            direction     = direction,
            year          = year,
            seed          = seed,
            train_months  = train_months,
            valid_months  = valid_months,
            shift_months  = shift_months,
            n_folds       = n_folds,
            purge_minutes = purge_minutes,
            selected_cols = selected_cols,
        )
    finally:
        conn.close()


def create_snapshot_sample(
    conn          : duckdb.DuckDBPyConnection,
    model_id      : str,
    snapshot_id   : str,
    asset_id      : str,
    target_cols   : list[str],
    horizon       : int = 60,
    direction     : str = "l",
    year          : int = 2023,
    seed          : int = 42,
    train_months  : int = 9,
    valid_months  : int = 3,
    shift_months  : int = 3,
    n_folds       : int = 4,
    purge_minutes : int = 240,
    selected_cols : list[str] | None = None,
) -> dict[str, Any]:
    """Build the sample table + reg.feature_sets row on an open lab connection.

    Pure orchestration over the IO-free SQL builders — owns the DuckDB execution
    and the registry write.  Deterministic: an immutable snapshot + fixed seed +
    fixed fold parameters yields a bit-identical sample table.

    Args:
        conn          : Open lab connection (lab default + ``snap``/``model`` + ``reg``).
        model_id      : Owning model id (sample table token + reg.models link).
        snapshot_id   : Immutable source snapshot id.
        asset_id      : Asset key (for the feature_set_id and reg links).
        target_cols   : Target column(s) to carry into the sample.
        horizon       : Forward-window bar count (feature_set_id token).
        direction     : ``l``/``s``/``combo`` (feature_set_id token).
        year          : Anchor calendar year for the walk-forward folds.
        seed          : Reproducibility seed for the per-hour pick.
        train_months  : Walk-forward training window length (months).
        valid_months  : Walk-forward validation window length (months).
        shift_months  : Months to shift between successive folds.
        n_folds       : Number of folds (validation windows 1..n; 0 = train-only).
        purge_minutes : Purge gap in minutes (metadata; applied at search time).
        selected_cols : Logical feature_set columns.  If None, all feat_* columns
                        of the snapshot are taken as the selection.

    Returns:
        Summary dict (see :func:`create_model_sample`).

    Raises:
        ValueError: If the snapshot table is missing or has no usable rows.
    """
    _ensure_model_schema(conn)

    if not _snapshot_exists(conn, snapshot_id):
        raise ValueError(
            f"Snapshot table snap.\"{snapshot_id}\" not found — create it first "
            "(src/data_handling/05_create_snapshot.py)."
        )

    all_feat_cols = _snapshot_feature_columns(conn, snapshot_id)
    n_input       = len(all_feat_cols)
    selected      = list(selected_cols) if selected_cols else all_feat_cols

    fold_time_windows = generate_walk_forward_folds(
        year          = year,
        train_months  = train_months,
        valid_months  = valid_months,
        shift_months  = shift_months,
        purge_minutes = purge_minutes,
        n_folds       = n_folds,
    )

    ctas_sql = build_sample_ctas_sql(
        model_id          = model_id,
        snapshot_id       = snapshot_id,
        seed              = seed,
        target_cols       = target_cols,
        fold_time_windows = fold_time_windows,
    )
    conn.execute(ctas_sql)

    sample_fqn = sample_table_fqn(model_id)
    n_rows_row = conn.execute(f"SELECT COUNT(*) FROM {sample_fqn}").fetchone()
    n_rows     = int(n_rows_row[0]) if n_rows_row else 0
    if n_rows == 0:
        raise ValueError(
            f"Sample is empty — snapshot snap.\"{snapshot_id}\" has no rows with "
            f"non-null targets {target_cols}."
        )

    fold_row_counts = _fold_counts(conn, sample_fqn)

    feature_set_id = build_feature_set_id(asset_id, horizon, direction, selected)
    registry.upsert(
        conn,
        "feature_sets",
        {
            "feature_set_id": feature_set_id,
            "snapshot_id":    snapshot_id,
            "n_input":        n_input,
            "n_selected":     len(selected),
            "selected_cols":  selected,
            "status":         "candidate",
        },
    )
    _link_model(conn, model_id, snapshot_id, feature_set_id)

    logger.info(
        "create_snapshot_sample: %s rows=%d folds=%s fs=%s (n_input=%d n_selected=%d)",
        sample_fqn, n_rows, fold_row_counts, feature_set_id, n_input, len(selected),
    )
    return {
        "model_id":        model_id,
        "snapshot_id":     snapshot_id,
        "sample_table":    sample_fqn,
        "n_rows":          n_rows,
        "fold_row_counts": fold_row_counts,
        "feature_set_id":  feature_set_id,
        "n_input":         n_input,
        "n_selected":      len(selected),
    }


# %% Internal — config resolution


def _horizon_from_target(target_name: str) -> int:
    """Parse the forward-window bar count from a target name (e.g. long_mfe_fw60 -> 60)."""
    token = target_name.rsplit("_fw", 1)
    if len(token) == 2 and token[1].isdigit():
        return int(token[1])
    return 60


def _direction_from_target(target_name: str) -> str:
    """Map a target prefix to the direction token (long -> l, short -> s)."""
    prefix = target_name.split("_", 1)[0]
    return _DIRECTION_BY_TARGET_PREFIX.get(prefix, "l")


def _anchor_year(sampling_meta: dict[str, Any]) -> int:
    """Resolve the anchor calendar year from sampling metadata (matches legacy logic)."""
    if "year" in sampling_meta:
        return int(sampling_meta["year"])
    sample_id = sampling_meta.get("sample_id", "")
    if "_yearly_" in sample_id:
        return int(sample_id.split("_yearly_")[-1])
    if sample_id:
        tail = sample_id.split("_")[-1]
        if tail.isdigit():
            return int(tail)
    return 2023


def _resolve_selected_features(artifact_dir: Path, target_name: str) -> list[str] | None:
    """Read the FE feature_set.json selection, if present, else None (= all feat_*).

    The feature_engineering step (plan 5, step 3) writes ``selected`` into
    ``<artifact_dir>/feature_engineering/feature_set.json``.  When that file is
    absent (sample created before FE), the full feat_* superset is the selection.
    """
    fe_path = artifact_dir / "feature_engineering" / "feature_set.json"
    if not fe_path.exists():
        return None
    payload  = json.loads(fe_path.read_text(encoding="utf-8"))
    selected = payload.get("selected")
    if not selected:
        return None
    return [c for c in selected if isinstance(c, str) and c.startswith("feat_")]


# %% Internal — DuckDB helpers


def _ensure_model_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the ``model`` schema in the (default) lab database if absent."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS model")


def _snapshot_exists(conn: duckdb.DuckDBPyConnection, snapshot_id: str) -> bool:
    """Return True if the snap.\"<snapshot_id>\" table exists in the lab database."""
    row = conn.execute(
        "SELECT 1 FROM information_schema.tables"
        " WHERE table_schema = 'snap' AND table_name = ?",
        [snapshot_id],
    ).fetchone()
    return row is not None


def _snapshot_feature_columns(conn: duckdb.DuckDBPyConnection, snapshot_id: str) -> list[str]:
    """Return the snapshot's feat_* columns, sorted (the logical feature superset)."""
    rows = conn.execute(f'SELECT * FROM snap."{snapshot_id}" LIMIT 0').description
    cols = [d[0] for d in rows]
    return sorted(c for c in cols if c.startswith("feat_"))


def _fold_counts(conn: duckdb.DuckDBPyConnection, sample_fqn: str) -> dict[str, int]:
    """Return per-fold row counts as a {fold_id_str: count} dict."""
    rows = conn.execute(
        f"SELECT fold_id, COUNT(*) FROM {sample_fqn} GROUP BY fold_id ORDER BY fold_id"
    ).fetchall()
    return {str(r[0]): int(r[1]) for r in rows}


def _link_model(
    conn           : duckdb.DuckDBPyConnection,
    model_id       : str,
    snapshot_id    : str,
    feature_set_id : str,
) -> None:
    """Upsert the model's snapshot + feature_set link into reg.models (idempotent)."""
    registry.upsert(
        conn,
        "models",
        {
            "model_id":       model_id,
            "snapshot_id":    snapshot_id,
            "feature_set_id": feature_set_id,
            "status":         "sampled",
        },
    )
