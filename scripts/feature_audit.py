#!/usr/bin/env python3
# =============================================================================
# Feature Audit Script for SOLUSDT feature tables
# =============================================================================
# Purpose:
#  - Compute null rates, near-constant rates, and pairwise correlations
#  - Identify candidate redundant features and near-duplicate pairs
#  - Print summary suitable for profile freeze decision
# =============================================================================

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import numpy as np
import pandas as pd

import utils
from db.table_ops import sqlite_connect


# =============================================================================
# _load_features(db_path, table_feat, sample_rows) -> pd.DataFrame
# =============================================================================
def _load_features(db_path: str, table_feat: str, sample_rows: int) -> pd.DataFrame:
    with sqlite_connect(db_path) as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM {table_feat}").fetchone()[0]
        df = pd.read_sql_query(
            f"SELECT * FROM {table_feat} ORDER BY open_time DESC LIMIT {sample_rows}",
            conn,
        )
    print(f"INFO: Loaded {len(df):,} rows (of {total:,} total) from '{table_feat}'")
    return df


# =============================================================================
# audit_features(asset_id, sample_rows, corr_threshold) -> None
# =============================================================================
def audit_features(
    asset_id:       str | None = None,
    sample_rows:    int        = 100_000,
    corr_threshold: float      = 0.95,
) -> None:
    db_cfg     = utils.load_asset_config(asset_id)
    db_path    = db_cfg["database"]["db_path"]
    table_feat = db_cfg["database"]["tables"]["features"]

    df = _load_features(db_path, table_feat, sample_rows)

    feat_cols   = [c for c in df.columns if c.startswith("feat_")]
    target_cols = [c for c in df.columns if c.startswith("trg_")]

    print(f"\n{'='*70}")
    print(f"FEATURE AUDIT: {table_feat}")
    print(f"{'='*70}")
    print(f"Total feature columns: {len(feat_cols)}")
    print(f"Target columns:        {target_cols}")

    # -------------------------------------------------------------------------
    # Null rates
    # -------------------------------------------------------------------------
    print(f"\n--- Null rates (feat_ columns) ---")
    null_rates = df[feat_cols].isna().mean().sort_values(ascending=False)
    high_null  = null_rates[null_rates > 0.01]
    if high_null.empty:
        print("  All features have <1% null rate.")
    else:
        for col, rate in high_null.items():
            print(f"  {col:<50}  null={rate*100:.1f}%")

    low_null = null_rates[(null_rates > 0) & (null_rates <= 0.01)]
    if not low_null.empty:
        print(f"  {len(low_null)} features with 0-1% null rate (showing top 10):")
        for col, rate in low_null.head(10).items():
            print(f"    {col:<50}  null={rate*100:.2f}%")

    # -------------------------------------------------------------------------
    # Near-constant detection
    # -------------------------------------------------------------------------
    print(f"\n--- Near-constant features (std < 0.001) ---")
    stds = df[feat_cols].std()
    near_const = stds[stds < 0.001].sort_values()
    if near_const.empty:
        print("  None found.")
    else:
        for col, std in near_const.items():
            print(f"  {col:<50}  std={std:.6f}")

    # -------------------------------------------------------------------------
    # Pairwise correlation analysis
    # -------------------------------------------------------------------------
    print(f"\n--- High-correlation pairs (|r| >= {corr_threshold}) ---")
    # Drop cols with too many nulls before correlation
    valid_cols = [c for c in feat_cols if df[c].isna().mean() < 0.5]
    corr       = df[valid_cols].corr().abs()

    high_pairs = []
    cols       = list(corr.columns)
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            r = corr.iloc[i, j]
            if r >= corr_threshold:
                high_pairs.append((cols[i], cols[j], round(float(r), 4)))

    if not high_pairs:
        print(f"  No pairs above {corr_threshold}.")
    else:
        print(f"  Found {len(high_pairs)} pairs:")
        for a, b, r in sorted(high_pairs, key=lambda x: -x[2]):
            tag = "  PERFECT" if r >= 0.9999 else ""
            print(f"  {r:.4f}  {a} <-> {b}{tag}")

    # -------------------------------------------------------------------------
    # Summary by feature group
    # -------------------------------------------------------------------------
    print(f"\n--- Feature count by group ---")
    groups: dict[str, list[str]] = {}
    for c in feat_cols:
        # Extract group from column name (feat_<group>_...)
        parts = c.replace("feat_", "").split("_")
        group = parts[0]
        groups.setdefault(group, []).append(c)
    for group, cols_in_group in sorted(groups.items()):
        print(f"  {group:<25} {len(cols_in_group):3d} cols")

    # -------------------------------------------------------------------------
    # Univariate null coverage for activity-dependent features
    # -------------------------------------------------------------------------
    activity_feats = [c for c in feat_cols if any(
        kw in c for kw in [
            "quote_volume", "trade_count", "avg_trade", "taker_buy", "taker_flow"
        ]
    )]
    if activity_feats:
        print(f"\n--- Activity-field-dependent features ({len(activity_feats)}) ---")
        for c in activity_feats:
            null_pct = df[c].isna().mean() * 100
            print(f"  {c:<50}  null={null_pct:.1f}%")

    print(f"\n{'='*70}")
    print("Audit complete.")
    print(f"{'='*70}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit feature table: null rates, near-constant features, high correlations.",
    )
    parser.add_argument(
        "--asset-id",
        default = None,
        help    = "Asset ID from config/assets.json (e.g. solusdt_fw60); omit for default",
    )
    parser.add_argument(
        "--sample-rows",
        type    = int,
        default = 100_000,
        help    = "Number of most-recent rows to sample for analysis (default: 100000)",
    )
    parser.add_argument(
        "--corr-threshold",
        type    = float,
        default = 0.95,
        help    = "Pairwise correlation threshold for flagging pairs (default: 0.95)",
    )
    args = parser.parse_args()
    audit_features(
        asset_id       = args.asset_id,
        sample_rows    = args.sample_rows,
        corr_threshold = args.corr_threshold,
    )


if __name__ == "__main__":
    main()
