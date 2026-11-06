"""Snapshot-native sampler — deterministic hourly select + train/valid ``split`` in SQL.

This is the DuckDB-native sampling path (plan section 5, steps 2-3, and 5.1).
The source is an immutable ``snap."<snapshot_id>"`` table (frozen by the snapshot
layer); the output is a small ``model."<model_id>__sample"`` table inside the lab
database — hourly resolution + ``split`` only, no per-model feature copy.

The hourly select and the train/valid assignment are expressed as a single
deterministic SQL statement over the snapshot:

  * hourly select: ``QUALIFY ROW_NUMBER() OVER (PARTITION BY hour ORDER BY hash(...))``
    — exactly one row per distinct hour, content-addressed by the seed so the
    same (snapshot, seed) always picks the same minute (bit-identical, immutable
    source -> reproducible sample);
  * split: ``TINYINT`` 0 = train, 1 = valid, with the feature-lookback embargo at
    the train start and the target purge at the train end.

Pure SQL builders here are IO-free; ``modeling.sampling.create_sample`` owns the
lab connection and the ``reg.feature_sets`` write.
"""

from __future__ import annotations

import hashlib

MODEL_SCHEMA = "model"
SNAP_SCHEMA = "snap"


def sample_table_fqn(model_id: str) -> str:
    """Return the fully-qualified, quoted sample table name (plan 6)."""
    return f'{MODEL_SCHEMA}."{model_id}__sample"'


def snapshot_table_fqn(snapshot_id: str) -> str:
    """Return the fully-qualified, quoted snapshot table name (plan 6)."""
    return f'{SNAP_SCHEMA}."{snapshot_id}"'


def pred_table_fqn(model_id: str) -> str:
    """Return the fully-qualified, quoted offline-prediction table name (plan 6).

    The offline predict step (plan 5, step 6) writes ``model."<model_id>__pred"``
    (open_time, pred) for the full snapshot range — separate from the immutable
    snapshot so the snapshot hash stays intact.
    """
    return f'{MODEL_SCHEMA}."{model_id}__pred"'


def build_feature_set_id(asset_id: str, horizon: int, direction: str, selected_cols: list[str]) -> str:
    """Assemble the feature_set_id ``fs_{asset}_fw{h}_{dir}__{hash8}`` (plan 6).

    The hash8 is the first 8 hex chars of sha256 over the sorted selected column
    list — identical feature selection -> identical id (reuse detection).

    Args:
        asset_id      : Asset key (lower-case symbol), e.g. ``solusdt``.
        horizon       : Forward-window bar count, e.g. ``60`` -> ``fw60``.
        direction     : ``l`` (long) / ``s`` (short) / ``combo``.
        selected_cols : The logical feature_set columns (order-insensitive).

    Returns:
        The feature_set identifier.
    """
    joined = ",".join(sorted(selected_cols))
    hash8 = hashlib.sha256(joined.encode("utf-8")).hexdigest()[:8]
    return f"fs_{asset_id}_fw{horizon}_{direction}__{hash8}"


def build_train_valid_split_select_sql(
    snapshot_id                     : str,
    seed                            : int,
    target_cols                     : list[str],
    train_start                     : str,
    train_end                       : str,
    valid_start                     : str,
    valid_end                       : str,
    feature_lookback_embargo_minutes: int = 240,
    target_purge_minutes            : int = 60,
) -> str:
    """Build the deterministic hourly-select + split indicator SELECT for a train/valid split.

    Selects one random row per hour from the snapshot, covering both train and
    valid windows.  Two embargo rules are applied on the train side:

    - ``feature_lookback_embargo_minutes``: rows within this many minutes of
      ``train_start`` are excluded (feature warmup guarantee).
    - ``target_purge_minutes``: rows within this many minutes of ``train_end``
      (counting back) are excluded (forward-looking target leak prevention).

    No embargo is applied at the start of the valid window.

    The result carries an INT8 ``split`` column: 0 = train, 1 = valid.

    Args:
        snapshot_id                     : Source snapshot id (``snap."<id>"``).
        seed                            : Reproducibility seed for per-hour pick.
        target_cols                     : Target column(s) to carry into the sample.
        train_start                     : Inclusive start of train window (YYYY-MM-DD).
        train_end                       : Inclusive end of train window (YYYY-MM-DD).
        valid_start                     : Inclusive start of valid window (YYYY-MM-DD).
        valid_end                       : Inclusive end of valid window (YYYY-MM-DD).
        feature_lookback_embargo_minutes: Minutes excluded from the start of train.
        target_purge_minutes            : Minutes excluded from the end of train.

    Returns:
        A complete ``SELECT`` statement (no trailing semicolon).
    """
    fqn         = snapshot_table_fqn(snapshot_id)
    target_sql  = ", ".join(f'"{c}"' for c in target_cols)
    null_checks = " AND ".join(f'"{c}" IS NOT NULL' for c in target_cols)

    return f"""
        SELECT
            open_time,
            {target_sql},
            CAST(CASE
                WHEN CAST(open_time AS DATE) BETWEEN DATE '{valid_start}' AND DATE '{valid_end}'
                    THEN 1
                ELSE 0
            END AS TINYINT) AS split
        FROM {fqn}
        WHERE
            {null_checks}
            AND (
                -- train window: apply feature lookback embargo at start and target purge at end
                (
                    CAST(open_time AS DATE) BETWEEN DATE '{train_start}' AND DATE '{train_end}'
                    AND open_time >= (TIMESTAMP '{train_start} 00:00:00' + INTERVAL '{feature_lookback_embargo_minutes}' MINUTE)
                    AND open_time <= (TIMESTAMP '{train_end} 23:59:59' - INTERVAL '{target_purge_minutes}' MINUTE)
                )
                OR
                -- valid window: no embargo
                CAST(open_time AS DATE) BETWEEN DATE '{valid_start}' AND DATE '{valid_end}'
            )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY date_trunc('hour', open_time)
            ORDER BY hash(CAST(epoch_ms(open_time) AS BIGINT) + {seed}), open_time
        ) = 1
        ORDER BY open_time
    """


def build_train_valid_split_ctas_sql(
    model_id                        : str,
    snapshot_id                     : str,
    seed                            : int,
    target_cols                     : list[str],
    train_start                     : str,
    train_end                       : str,
    valid_start                     : str,
    valid_end                       : str,
    feature_lookback_embargo_minutes: int = 240,
    target_purge_minutes            : int = 60,
) -> str:
    """Build the full CTAS statement for a simple train/valid split sample.

    Wraps :func:`build_train_valid_split_select_sql`.  The output table has
    columns: ``open_time``, target column(s), and ``split`` (TINYINT 0/1).
    ``CREATE OR REPLACE`` makes re-runs idempotent and bit-identical for
    the same inputs.

    Args:
        model_id                        : Owning model id (table name token).
        snapshot_id                     : Immutable source snapshot id.
        seed                            : Reproducibility seed for per-hour pick.
        target_cols                     : Target column(s) to carry into the sample.
        train_start                     : Inclusive start of train window (YYYY-MM-DD).
        train_end                       : Inclusive end of train window (YYYY-MM-DD).
        valid_start                     : Inclusive start of valid window (YYYY-MM-DD).
        valid_end                       : Inclusive end of valid window (YYYY-MM-DD).
        feature_lookback_embargo_minutes: Minutes excluded from the start of train.
        target_purge_minutes            : Minutes excluded from the end of train.

    Returns:
        A complete ``CREATE OR REPLACE TABLE ... AS SELECT`` statement.
    """
    select_sql = build_train_valid_split_select_sql(
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
    return f"CREATE OR REPLACE TABLE {sample_table_fqn(model_id)} AS{select_sql}"
