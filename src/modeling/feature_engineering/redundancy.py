"""Redundancy and correlation analysis.

Groups feat_* columns in quant_train into high-correlation clusters using
Pearson thresholds via numpy.  Recommends one representative per cluster
and marks the rest for removal.
"""

import logging

import duckdb
import numpy as np
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
    """Detect redundant feature groups in quant_train and recommend per-cluster representatives.

    Clusters are built from the Pearson correlation matrix computed in numpy.
    Connected components (union-find) identify groups where any pair exceeds
    cfg.pearson_cluster_thr.  The lowest-index feature in each cluster is
    designated representative; downstream steps (t110) can refine the choice
    using quality and target-relation metrics.

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
    logger.info(
        "analyze_redundancy: loading %d feature columns, sample=%d / %d rows",
        len(feat_cols), sample_n, n_rows,
    )

    # --- load feat_* into Polars; fill NaN/NULL with column mean ---
    cols_sql = ", ".join(f'"{c}"' for c in feat_cols)
    sample_clause = f"USING SAMPLE {sample_n} ROWS" if sample_n < n_rows else ""
    df = conn.execute(f"SELECT {cols_sql} FROM quant_train {sample_clause}").pl()

    filled = df.with_columns([
        pl.col(c).fill_nan(None).fill_null(strategy="mean")
        for c in feat_cols
    ])

    # --- Pearson correlation matrix via numpy ---
    mat: np.ndarray = filled.to_numpy()               # (n_rows, n_feats)
    corr: np.ndarray = np.corrcoef(mat.T)             # (n_feats, n_feats)
    np.nan_to_num(corr, nan=0.0, copy=False)

    n_feats = len(feat_cols)

    # --- union-find clustering: merge pairs above threshold ---
    parent = list(range(n_feats))

    for i in range(n_feats):
        for j in range(i + 1, n_feats):
            if abs(corr[i, j]) >= cfg.pearson_cluster_thr:
                pi, pj = _find(parent, i), _find(parent, j)
                if pi != pj:
                    parent[pi] = pj

    # --- normalise cluster IDs to sequential integers ---
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

        members     = [j for j, c in enumerate(cluster_ids) if c == cid and j != i]
        max_pearson = float(max((abs(corr[i, j]) for j in members), default=0.0))

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
            "max_pearson"      : max_pearson,
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
