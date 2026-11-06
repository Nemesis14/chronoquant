"""Snapshot-native sampling orchestrator — snap source, model.__sample output, reg.feature_sets.

DuckDB-native sampling path (plan section 5, steps 2-3, and 5.1):

  * **Source** is an immutable ``snap."<snapshot_id>"`` table (frozen by the
    snapshot layer), *not* the mutable ``quant_train`` table — this is what makes
    a model reproducible.
  * **Output** is a small ``model."<model_id>__sample"`` table inside the lab
    database: ``open_time`` + the model's target column(s) + ``split``
    (0 = train, 1 = valid) only.
    Features stay in the snapshot; downstream steps project them via the
    ``__train_input`` view (plan 5.1) — there is no per-model feature copy.
  * **Feature filtering** is a *logical* feature_set, registered in
    ``reg.feature_sets`` (selected_cols, n_input, n_selected); no physical column
    drop happens here.

The only supported ``sampling_mode`` (config/models.json) is ``train_valid_split``:
single chronological split; ``split`` TINYINT 0 = train, 1 = valid.  Feature lookback
embargo applied at train start; target purge applied at train end; no embargo at valid
start.

Only this module touches utils and the lab connection; ``snapshot_sampler`` is IO-free.
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
    build_train_valid_split_ctas_sql,
    sample_table_fqn,
)
from utils import _snapshot_exists

logger = logging.getLogger(__name__)

# Map a model target_name to the strategy direction token (plan 6).
_DIRECTION_BY_TARGET_PREFIX = {"long": "l", "short": "s"}


def create_model_sample(model_id: str, snapshot_id: str) -> dict[str, Any]:
    """Create the ``model."<model_id>__sample"`` table for a config-defined model.

    Reads the ``sampling`` block of config/models.json (``sampling_mode`` must be
    ``train_valid_split``) and delegates to
    :func:`create_snapshot_sample_train_valid_split`.

    Args:
        model_id    : Model key from config/models.json.
        snapshot_id : Immutable snapshot id (``snap."<id>"``) to sample from.

    Returns:
        Summary dict — see :func:`create_snapshot_sample_train_valid_split`.

    Raises:
        ValueError: If the model is unknown, the snapshot has no usable rows, or
                    ``sampling_mode`` is not ``train_valid_split``.
    """
    models_cfg = utils.load_models_config()
    if model_id not in models_cfg.get("models", {}):
        raise ValueError(f"Model not found in config/models.json: {model_id}")

    meta          = models_cfg["models"][model_id]
    asset_id      = meta["asset_id"]
    target_name   = meta["target_name"]
    sampling_meta = meta.get("sampling", {})
    artifact_dir  = Path(utils._resolve_path(meta["artifact_dir"]))
    sampling_mode = sampling_meta.get("sampling_mode", "train_valid_split")

    if sampling_mode != "train_valid_split":
        raise ValueError(
            f"Unknown sampling_mode '{sampling_mode}' for model '{model_id}'. "
            "Supported: 'train_valid_split'."
        )

    selected_cols = _resolve_selected_features(artifact_dir, target_name)
    conn          = utils.open_lab_connection(asset_id)

    try:
        return create_snapshot_sample_train_valid_split(
            conn                             = conn,
            model_id                         = model_id,
            snapshot_id                      = snapshot_id,
            asset_id                         = asset_id,
            target_cols                      = [target_name],
            horizon                          = _horizon_from_target(target_name),
            direction                        = _direction_from_target(target_name),
            seed                             = int(sampling_meta.get("seed", 42)),
            train_start                      = sampling_meta["train_start"],
            train_end                        = sampling_meta["train_end"],
            valid_start                      = sampling_meta["valid_start"],
            valid_end                        = sampling_meta["valid_end"],
            feature_lookback_embargo_minutes = int(
                sampling_meta.get("feature_lookback_embargo_minutes", 240)
            ),
            target_purge_minutes             = int(sampling_meta.get("target_purge_minutes", 60)),
            selected_cols                    = selected_cols,
        )
    finally:
        conn.close()


def create_snapshot_sample_train_valid_split(
    conn                            : duckdb.DuckDBPyConnection,
    model_id                        : str,
    snapshot_id                     : str,
    asset_id                        : str,
    target_cols                     : list[str],
    horizon                         : int = 60,
    direction                       : str = "l",
    seed                            : int = 42,
    train_start                     : str = "2021-01-01",
    train_end                       : str = "2025-04-30",
    valid_start                     : str = "2025-05-01",
    valid_end                       : str = "2026-05-31",
    feature_lookback_embargo_minutes: int = 240,
    target_purge_minutes            : int = 60,
    selected_cols                   : list[str] | None = None,
) -> dict[str, Any]:
    """Build the sample table + reg.feature_sets row for a simple train/valid split.

    Selects one random row per hour from the snapshot, covering both train and
    valid windows.  The output ``model."<model_id>__sample"`` table contains
    ``open_time``, the model's target column(s), and a ``split`` TINYINT column
    (0 = train, 1 = valid).

    Two embargo rules are applied on the train side only:

    - ``feature_lookback_embargo_minutes``: rows within this many minutes of
      ``train_start`` are excluded (feature warmup guarantee).
    - ``target_purge_minutes``: rows within this many minutes of ``train_end``
      (counting back) are excluded (forward-looking target leak prevention).

    No embargo is applied at the start of the valid window.

    Deterministic: an immutable snapshot + fixed seed + fixed date boundaries
    yield a bit-identical sample table every run (``CREATE OR REPLACE``).

    Args:
        conn                            : Open lab connection.
        model_id                        : Owning model id (sample table token + reg.models link).
        snapshot_id                     : Immutable source snapshot id.
        asset_id                        : Asset key (for the feature_set_id and reg links).
        target_cols                     : Target column(s) to carry into the sample.
        horizon                         : Forward-window bar count (feature_set_id token).
        direction                       : ``l``/``s``/``combo`` (feature_set_id token).
        seed                            : Reproducibility seed for the per-hour pick.
        train_start                     : Inclusive start of train window (YYYY-MM-DD).
        train_end                       : Inclusive end of train window (YYYY-MM-DD).
        valid_start                     : Inclusive start of valid window (YYYY-MM-DD).
        valid_end                       : Inclusive end of valid window (YYYY-MM-DD).
        feature_lookback_embargo_minutes: Minutes excluded from the start of train.
        target_purge_minutes            : Minutes excluded from the end of train.
        selected_cols                   : Logical feature_set columns.  If None, all
                                          feat_* columns of the snapshot are taken.

    Returns:
        Summary dict: ``model_id``, ``snapshot_id``, ``sample_table``, ``n_rows``,
        ``split_row_counts``, ``feature_set_id``, ``n_input``, ``n_selected``.

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

    ctas_sql = build_train_valid_split_ctas_sql(
        model_id                         = model_id,
        snapshot_id                      = snapshot_id,
        seed                             = seed,
        target_cols                      = target_cols,
        train_start                      = train_start,
        train_end                        = train_end,
        valid_start                      = valid_start,
        valid_end                        = valid_end,
        feature_lookback_embargo_minutes = feature_lookback_embargo_minutes,
        target_purge_minutes             = target_purge_minutes,
    )
    conn.execute(ctas_sql)

    sample_fqn = sample_table_fqn(model_id)
    n_rows_row = conn.execute(f"SELECT COUNT(*) FROM {sample_fqn}").fetchone()
    n_rows     = int(n_rows_row[0]) if n_rows_row else 0
    if n_rows == 0:
        raise ValueError(
            f"Sample is empty — snapshot snap.\"{snapshot_id}\" has no rows with "
            f"non-null targets {target_cols} in the specified date range."
        )

    split_row_counts = _split_counts(conn, sample_fqn)

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
        "create_snapshot_sample_train_valid_split: %s rows=%d splits=%s fs=%s "
        "(n_input=%d n_selected=%d)",
        sample_fqn, n_rows, split_row_counts, feature_set_id, n_input, len(selected),
    )
    return {
        "model_id":         model_id,
        "snapshot_id":      snapshot_id,
        "sample_table":     sample_fqn,
        "n_rows":           n_rows,
        "split_row_counts": split_row_counts,
        "feature_set_id":   feature_set_id,
        "n_input":          n_input,
        "n_selected":       len(selected),
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


def _snapshot_feature_columns(conn: duckdb.DuckDBPyConnection, snapshot_id: str) -> list[str]:
    """Return the snapshot's feat_* columns, sorted (the logical feature superset)."""
    rows = conn.execute(f'SELECT * FROM snap."{snapshot_id}" LIMIT 0').description
    cols = [d[0] for d in rows]
    return sorted(c for c in cols if c.startswith("feat_"))


def _split_counts(conn: duckdb.DuckDBPyConnection, sample_fqn: str) -> dict[str, int]:
    """Return per-split row counts as a {split_str: count} dict (0=train, 1=valid)."""
    rows = conn.execute(
        f"SELECT split, COUNT(*) FROM {sample_fqn} GROUP BY split ORDER BY split"
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
