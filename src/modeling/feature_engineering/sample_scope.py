"""Sample-scoped feature engineering input builder.

Materialises a local ``quant_train`` working table from the immutable snapshot
and the model-specific ``model."<model_id>__sample"`` rows. This keeps the FE
notebook aligned with the same snapshot/sample contract that downstream
search/train/predict already use.
"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb


@dataclass(frozen=True)
class SampleScopeMeta:
    """Metadata for the local FE working set."""

    model_id: str
    snapshot_id: str
    sample_table: str
    row_count: int
    sample_row_count: int
    min_open_time: str | None
    max_open_time: str | None


def materialize_sample_scoped_quant_train(
    conn: duckdb.DuckDBPyConnection,
    model_id: str,
    snapshot_id: str,
) -> SampleScopeMeta:
    """Create a temp ``quant_train`` table from ``snap ⋈ model.__sample``.

    The created table intentionally keeps the historical ``quant_train`` name so
    the analysis modules can stay unchanged, but its rows are restricted to the
    exact model sample rather than a broad time window slice.

    Raises:
        RuntimeError: If the sample is empty or the snapshot/sample join drops rows.
    """
    sample_table = f'model."{model_id}__sample"'
    sample_row = conn.execute(f"SELECT COUNT(*) FROM {sample_table}").fetchone()
    sample_row_count = int(sample_row[0]) if sample_row else 0
    if sample_row_count == 0:
        raise RuntimeError(
            f'{sample_table} is empty; run the sample step before feature engineering.'
        )

    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE quant_train AS
        SELECT
            s.*
        FROM snap."{snapshot_id}" AS s
        INNER JOIN {sample_table} AS m
            ON s.open_time = m.open_time
        ORDER BY s.open_time
    """)

    row = conn.execute("""
        SELECT
            COUNT(*) AS n_rows,
            MIN(open_time)::VARCHAR AS min_open_time,
            MAX(open_time)::VARCHAR AS max_open_time
        FROM quant_train
    """).fetchone()
    row_count = int(row[0]) if row else 0
    min_open_time = str(row[1]) if row and row[1] is not None else None
    max_open_time = str(row[2]) if row and row[2] is not None else None

    if row_count != sample_row_count:
        raise RuntimeError(
            "Sample/snapshot join lost rows during feature engineering input "
            f"materialization: sample_rows={sample_row_count}, joined_rows={row_count}."
        )

    return SampleScopeMeta(
        model_id=model_id,
        snapshot_id=snapshot_id,
        sample_table=sample_table,
        row_count=row_count,
        sample_row_count=sample_row_count,
        min_open_time=min_open_time,
        max_open_time=max_open_time,
    )
