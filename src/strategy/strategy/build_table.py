"""Strategy scored-table builder for ChronoQuant (DuckDB-native).

Builds the strategy calibration input as the join

    snap."<snapshot_id>"
        ⋈ model_long."<model_id_long>__pred"
        ⋈ model_short."<model_id_short>__pred"
        ⋈ live.ohlcv          (optional, for close-derived trade prices)

on ``open_time`` (plan 5.1: ``strat."<session>__scored"``).  The two model
predictions come from the offline predict tables (t315), the realized targets
(``long_mfe_fw60`` / ``short_mfe_fw60``) come from the immutable snapshot, and the
``close`` column comes from the live ohlcv table (read-only) so trade artifacts
can carry entry/exit prices.

No model.pkl is loaded and no parquet is written here — the scoring already
happened offline; this step is a pure DuckDB join returning an in-memory
DataFrame.  Output ``strat.*`` tables are written downstream (optimize step).

Contract note — predict scope vs. sample scope
    The ``model."<model_id>__pred"`` tables cover the **full snapshot range**
    (every bar in ``snap."<snapshot_id>"``), not just the training sample rows.
    This is intentional: the offline predict step scores all historical bars so
    the strategy can calibrate and optimize over any sub-window of the snapshot.
    The ``model."<model_id>__sample"`` table (training scope) is irrelevant here
    and is never read by the strategy layer.

Callers reach the lab/live/registry topology through ``utils.open_lab_connection``
(config-gateway), never by opening DuckDB files directly.

Reference: _doc_/_plans_/data_process_architecture.md sections 5 (step 7), 5.1, 6.
"""

from __future__ import annotations

import logging
from typing import Any

import duckdb
import pandas as pd

import utils
from data_handling.store import registry
from modeling.sampling.snapshot_sampler import pred_table_fqn, snapshot_table_fqn

logger = logging.getLogger(__name__)

# Realized MFE target columns carried from the snapshot for both directions.
LONG_MFE_COL = "long_mfe_fw60"
SHORT_MFE_COL = "short_mfe_fw60"


# %% Internal helpers


def _resolve_snapshot_id(conn: duckdb.DuckDBPyConnection, model_id: str, meta: dict) -> str:
    """Resolve a model's snapshot_id from reg.models, falling back to config.

    Both the long and the short model are scored against the same snapshot range;
    the snapshot id is read from the model's registry row (the snapshot it was
    trained/predicted from) with a ``sampling.snapshot_id`` config fallback.
    """
    row = registry.get(conn, "models", model_id)
    if row is not None and row.get("snapshot_id"):
        return str(row["snapshot_id"])
    snapshot_id = meta.get("sampling", {}).get("snapshot_id")
    if not snapshot_id:
        raise ValueError(
            f"No snapshot_id for {model_id} (neither reg.models nor sampling.snapshot_id)."
        )
    return str(snapshot_id)


def _table_exists(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    """Return True if ``schema.table`` exists in the lab database."""
    row = conn.execute(
        "SELECT 1 FROM information_schema.tables"
        " WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchone()
    return row is not None


def _live_ohlcv_available(conn: duckdb.DuckDBPyConnection) -> bool:
    """Return True if the attached live database exposes an ohlcv table."""
    try:
        row = conn.execute(
            "SELECT 1 FROM information_schema.tables"
            " WHERE table_catalog = 'live' AND table_name = 'ohlcv' LIMIT 1"
        ).fetchone()
    except duckdb.Error:
        return False
    return row is not None


def _model_meta(model_id: str) -> dict[str, Any]:
    """Return the model's config entry from config/models.json (empty if absent)."""
    return utils.load_models_config().get("models", {}).get(model_id, {})


# %% Public API


def build_scored_table(
    long_model_id  : str,
    short_model_id : str,
    asset_id       : str | None = None,
    snapshot_id    : str | None = None,
) -> pd.DataFrame:
    """Build the strategy scored table from the snap+pred join.

    Joins on ``open_time``:
      * ``snap."<snapshot_id>"``                      -> realized MFE targets,
      * ``model_long."<long_model_id>__pred"``        -> raw long score,
      * ``model_short."<short_model_id>__pred"``      -> raw short score,
      * ``live.ohlcv`` (optional)                     -> open/high/low/close.

    No model.pkl is loaded: the offline predict tables (t315) already hold the
    scores for the full snapshot range.

    Args:
        long_model_id  : Long-direction model id (its ``__pred`` table is joined).
        short_model_id : Short-direction model id (its ``__pred`` table is joined).
        asset_id       : Asset key for the lab connection; resolved from the long
                         model's config (or the default asset) if None.
        snapshot_id    : Snapshot id both predict tables were scored against;
                         resolved from reg.models / config if None.

    Returns:
        DataFrame with columns:
            open_time, pred_long_raw, pred_short_raw,
            long_mfe_fw60, short_mfe_fw60,
            open, high, low, close   (the price columns are NULL if live.ohlcv
                                      is unavailable).

    Raises:
        ValueError: If the snapshot or either predict table is missing, or the
                    join returns no rows.
    """
    long_meta  = _model_meta(long_model_id)
    short_meta = _model_meta(short_model_id)
    asset_id   = asset_id or long_meta.get("asset_id") or short_meta.get("asset_id")

    conn = utils.open_lab_connection(asset_id)
    try:
        snap_id = snapshot_id or _resolve_snapshot_id(conn, long_model_id, long_meta)

        if not _table_exists(conn, "snap", snap_id):
            raise ValueError(
                f'Snapshot table snap."{snap_id}" not found — create it first '
                "(src/data_handling/05_create_snapshot.py)."
            )
        for mid in (long_model_id, short_model_id):
            if not _table_exists(conn, "model", f"{mid}__pred"):
                raise ValueError(
                    f'Prediction table model."{mid}__pred" not found — run the '
                    "offline predict step first (src/modeling/predict.py)."
                )

        snap_fqn       = snapshot_table_fqn(snap_id)
        long_pred_fqn  = pred_table_fqn(long_model_id)
        short_pred_fqn = pred_table_fqn(short_model_id)

        if _live_ohlcv_available(conn):
            price_select = "o.open AS open, o.high AS high, o.low AS low, o.close AS close"
            price_join   = "LEFT JOIN live.ohlcv o ON o.open_time = s.open_time"
        else:
            price_select = (
                "CAST(NULL AS DOUBLE) AS open, CAST(NULL AS DOUBLE) AS high, "
                "CAST(NULL AS DOUBLE) AS low, CAST(NULL AS DOUBLE) AS close"
            )
            price_join = ""
            logger.warning("build_scored_table: live.ohlcv unavailable — price columns NULL")

        sql = f"""
            SELECT
                s.open_time                  AS open_time,
                pl.pred                      AS pred_long_raw,
                ps.pred                      AS pred_short_raw,
                s."{LONG_MFE_COL}"           AS {LONG_MFE_COL},
                s."{SHORT_MFE_COL}"          AS {SHORT_MFE_COL},
                {price_select}
            FROM {snap_fqn} s
            JOIN {long_pred_fqn}  pl ON pl.open_time = s.open_time
            JOIN {short_pred_fqn} ps ON ps.open_time = s.open_time
            {price_join}
            WHERE s."{LONG_MFE_COL}"  IS NOT NULL
              AND s."{SHORT_MFE_COL}" IS NOT NULL
            ORDER BY s.open_time
        """
        scored_df = conn.execute(sql).df()
    finally:
        conn.close()

    if scored_df.empty:
        raise ValueError(
            f'Empty scored join for snapshot "{snap_id}" with models '
            f"{long_model_id} / {short_model_id} — no overlapping open_time rows."
        )

    logger.info(
        "build_scored_table: %d rows (snap=%s long=%s short=%s)",
        len(scored_df), snap_id, long_model_id, short_model_id,
    )
    return scored_df
