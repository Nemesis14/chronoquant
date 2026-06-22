"""Snapshot-native sampler — deterministic hourly select + walk-forward fold_id in SQL.

This is the DuckDB-native sampling path (plan section 5, steps 2-3, and 5.1).
The source is an immutable ``snap."<snapshot_id>"`` table (frozen by the snapshot
layer); the output is a small ``model."<model_id>__sample"`` table inside the lab
database — hourly resolution + ``fold_id`` only, no per-model feature copy.

The hourly select and the walk-forward fold assignment are expressed as a single
deterministic SQL statement over the snapshot:

  * hourly select: ``QUALIFY ROW_NUMBER() OVER (PARTITION BY hour ORDER BY hash(...))``
    — exactly one row per distinct hour, content-addressed by the seed so the
    same (snapshot, seed) always picks the same minute (bit-identical, immutable
    source -> reproducible sample);
  * fold_id: a walk-forward ``CASE`` chain built from the fold validation windows,
    reproducing the existing ``assign_walk_forward_fold_ids`` semantics
    (``fold_id`` Int8 0..n, 0 = train-only).

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


def build_fold_case_sql(fold_time_windows: list[dict]) -> str:
    """Build the walk-forward ``fold_id`` CASE expression from fold windows.

    Reproduces ``assign_walk_forward_fold_ids``: a row whose ``open_time`` date is
    inside a fold's inclusive validation window gets that fold's id (1..n); all
    other rows get 0 (train-only).  Windows are emitted in order; the first match
    wins, matching the iterative Polars assignment's last-write-wins on
    non-overlapping windows.

    Args:
        fold_time_windows : Output of ``generate_walk_forward_folds`` — dicts with
                            ``fold_id``, ``valid_start``, ``valid_end`` (date strings).

    Returns:
        A SQL ``CASE ... END`` expression yielding an INT8-castable fold_id.
    """
    if not fold_time_windows:
        return "CAST(0 AS TINYINT)"

    whens: list[str] = []
    for fw in fold_time_windows:
        fold_id     = int(fw["fold_id"])
        valid_start = fw["valid_start"]
        valid_end   = fw["valid_end"]
        whens.append(
            "WHEN CAST(open_time AS DATE) BETWEEN DATE "
            f"'{valid_start}' AND DATE '{valid_end}' THEN {fold_id}"
        )
    case_body = "\n            ".join(whens)
    return f"CAST(CASE\n            {case_body}\n            ELSE 0\n        END AS TINYINT)"


def build_sample_select_sql(
    snapshot_id      : str,
    seed             : int,
    target_cols      : list[str],
    fold_time_windows: list[dict],
) -> str:
    """Build the deterministic hourly-select + fold_id SELECT over a snapshot.

    The projection is intentionally small (plan 5, step 2): ``open_time``, the
    model's target column(s), and ``fold_id``.  Features stay in the snapshot —
    the sample carries no feat_* copy; downstream steps project via the
    ``__train_input`` view (plan 5.1).

    Determinism: the per-hour winner is chosen by ``hash(epoch_ms, seed)``, so an
    immutable snapshot + fixed seed yields a bit-identical sample.

    Args:
        snapshot_id       : Source snapshot id (``snap."<id>"``).
        seed              : Reproducibility seed for the per-hour pick.
        target_cols       : Target column(s) to carry into the sample.
        fold_time_windows : Walk-forward windows for the fold_id CASE.

    Returns:
        A complete ``SELECT`` statement (no trailing semicolon).
    """
    fqn         = snapshot_table_fqn(snapshot_id)
    target_sql  = ", ".join(f'"{c}"' for c in target_cols)
    fold_case   = build_fold_case_sql(fold_time_windows)
    null_checks = " AND ".join(f'"{c}" IS NOT NULL' for c in target_cols)

    return f"""
        SELECT
            open_time,
            {target_sql},
            {fold_case} AS fold_id
        FROM {fqn}
        WHERE {null_checks}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY date_trunc('hour', open_time)
            ORDER BY hash(CAST(epoch_ms(open_time) AS BIGINT) + {seed}), open_time
        ) = 1
        ORDER BY open_time
    """


def build_sample_ctas_sql(
    model_id         : str,
    snapshot_id      : str,
    seed             : int,
    target_cols      : list[str],
    fold_time_windows: list[dict],
) -> str:
    """Build the full ``CREATE TABLE model."<id>__sample" AS <select>`` statement.

    Wraps :func:`build_sample_select_sql`.  ``CREATE OR REPLACE`` keeps the path
    deterministic: re-running with the same (snapshot, seed) yields a bit-identical
    table (plan 5, step 2).

    Args:
        model_id          : Owning model id (table name token).
        snapshot_id       : Immutable source snapshot id.
        seed              : Reproducibility seed for the per-hour pick.
        target_cols       : Target column(s) to carry into the sample.
        fold_time_windows : Walk-forward windows for the fold_id CASE.

    Returns:
        A complete ``CREATE OR REPLACE TABLE ... AS SELECT`` statement.
    """
    select_sql = build_sample_select_sql(snapshot_id, seed, target_cols, fold_time_windows)
    return f"CREATE OR REPLACE TABLE {sample_table_fqn(model_id)} AS{select_sql}"
