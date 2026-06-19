"""Redundancy and correlation analysis.

Groups feat_* columns in the 'quant_train' table (loaded from the model's sample
parquet into an in-memory DuckDB) into high-correlation clusters using Pearson
thresholds.  Recommends one representative per cluster and marks the rest for removal.
"""

import logging

import duckdb
import polars as pl

from .config import FeatureEngineeringConfig

logger = logging.getLogger(__name__)


def _find(parent: list[int], x: int) -> int:
    """Union-find: path-compressed root lookup."""
    root = x
    while parent[root] != root:
        root = parent[root]
    while parent[x] != root:
        parent[x], x = root, parent[x]
    return root


def analyze_redundancy(
    conn : duckdb.DuckDBPyConnection,
    cfg  : FeatureEngineeringConfig,
) -> pl.DataFrame:
    """Detect redundant feature groups in quant_train.

    Computes Pearson correlation pairs inside DuckDB (no Python-side matrix) to
    keep memory bounded.  For each feature, one aggregation query computes CORR
    against all other features over a fixed sample; only pairs above threshold
    are returned to Python.  Union-find clusters the resulting edge list.

    Args:
        conn : Open DuckDB connection to the asset database.
        cfg  : Analysis configuration with redundancy thresholds.

    Returns:
        DataFrame with one row per feature and columns:
            feature, cluster_id (int), is_representative (bool),
            max_pearson (float), decision ('keep' | 'drop'), drop_reason (str | None).
    """
    _SCHEMA: dict[str, type[pl.DataType]] = {
        "feature"          : pl.Utf8,
        "cluster_id"       : pl.Int32,
        "is_representative": pl.Boolean,
        "max_pearson"      : pl.Float64,
        "decision"         : pl.Utf8,
        "drop_reason"      : pl.Utf8,
    }

    # --- discover feat_* columns ---
    schema    = conn.execute("DESCRIBE quant_train").pl()
    feat_cols = [c for c in schema["column_name"].to_list() if c.startswith("feat_")]

    if not feat_cols:
        logger.warning("analyze_redundancy: no feat_* columns in quant_train")
        return pl.DataFrame(schema=_SCHEMA)

    n_rows: int = conn.execute("SELECT COUNT(*) FROM quant_train").fetchone()[0]  # type: ignore[index]
    sample_n = min(n_rows, cfg.redundancy_max_rows)
    n_feats  = len(feat_cols)
    logger.info(
        "analyze_redundancy: %d features, sample=%d / %d rows",
        n_feats, sample_n, n_rows,
    )

    # materialise a small sample table once — avoids re-scanning on every CORR query
    sample_clause = f"USING SAMPLE {sample_n} ROWS" if sample_n < n_rows else ""
    cols_sql = ", ".join(f'"{c}"' for c in feat_cols)
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE _redundancy_sample AS
        SELECT {cols_sql} FROM quant_train {sample_clause}
    """)

    # --- compute Pearson correlation for each feature pair inside DuckDB ---
    # For each feat_i, one query computes CORR(feat_i, feat_j) for all j != i.
    # We only pull high-correlation pairs back to Python — memory stays tiny.
    thr = cfg.pearson_cluster_thr
    edges: list[tuple[int, int, float]] = []   # (i, j, |r|) where j > i
    max_pearson_per_feat: list[float] = [0.0] * n_feats

    for i, col_i in enumerate(feat_cols):
        corr_selects = ", ".join(
            f'ABS(CORR("{col_i}", "{col_j}")) AS c{j}'
            for j, col_j in enumerate(feat_cols)
            if j != i
        )
        try:
            row = conn.execute(
                f"SELECT {corr_selects} FROM _redundancy_sample"
            ).fetchone()
        except Exception:
            continue
        if row is None:
            continue

        # map results back to feature indices (skip index i)
        result_idx = 0
        for j in range(n_feats):
            if j == i:
                continue
            r = row[result_idx] or 0.0
            result_idx += 1
            max_pearson_per_feat[i] = max(max_pearson_per_feat[i], r)
            if j > i and r >= thr:
                edges.append((i, j, r))

    conn.execute("DROP TABLE IF EXISTS _redundancy_sample")

    # --- union-find clustering over edge list ---
    parent = list(range(n_feats))
    for i, j, _ in edges:
        pi, pj = _find(parent, i), _find(parent, j)
        if pi != pj:
            parent[pi] = pj

    raw_roots    = [_find(parent, i) for i in range(n_feats)]
    unique_roots = sorted(set(raw_roots))
    root_map     = {r: idx for idx, r in enumerate(unique_roots)}
    cluster_ids  = [root_map[r] for r in raw_roots]

    # --- representative: lowest index in each cluster ---
    reps: dict[int, int] = {}
    for i, cid in enumerate(cluster_ids):
        if cid not in reps:
            reps[cid] = i

    # --- build result ---
    records: list[dict] = []
    for i, feat in enumerate(feat_cols):
        cid    = cluster_ids[i]
        is_rep = reps[cid] == i

        if is_rep:
            decision    = "keep"
            drop_reason: str | None = None
        else:
            decision    = "drop"
            drop_reason = (
                f"redundant in cluster_{cid}; "
                f"representative={feat_cols[reps[cid]]}"
            )

        records.append({
            "feature"          : feat,
            "cluster_id"       : cid,
            "is_representative": is_rep,
            "max_pearson"      : max_pearson_per_feat[i],
            "decision"         : decision,
            "drop_reason"      : drop_reason,
        })

    n_clusters = len(reps)
    n_dropped  = sum(1 for r in records if r["decision"] == "drop")
    logger.info(
        "analyze_redundancy: done — %d clusters, %d redundant features dropped",
        n_clusters, n_dropped,
    )
    return pl.DataFrame(records, schema=_SCHEMA)
