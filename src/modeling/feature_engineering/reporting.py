"""Feature engineering output generation.

Merges quality, target-relation, redundancy, and stability analysis results into:
  - analyst_report.md  — human-readable summary with decisions and rationale
  - feature_set.json   — machine-readable feature list consumable by sampling

Output path: database/<asset_id>/feature_engineering/<run_id>/
"""

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from .config import FeatureEngineeringConfig

logger = logging.getLogger(__name__)


def generate_outputs(
    quality_df    : pl.DataFrame,
    relation_df   : pl.DataFrame,
    redundancy_df : pl.DataFrame,
    stability_df  : pl.DataFrame,
    cfg           : FeatureEngineeringConfig,
    output_dir    : Path,
) -> None:
    """Write analyst_report.md and feature_set.json for a completed analysis run.

    A feature must survive all four checks to appear in the selected list:
      1. quality    : decision == 'keep'
      2. relation   : decision == 'keep' for at least one target
      3. redundancy : decision == 'keep'  (is_representative == True)
      4. stability  : no bucket with stability_flag == 'decayed'

    Drop decisions from any step propagate to the final dropped list with reasons.
    Features that fail only the review threshold appear in the review list.

    Args:
        quality_df    : Output of analyze_quality.
        relation_df   : Output of analyze_target_relation.
        redundancy_df : Output of analyze_redundancy.
        stability_df  : Output of analyze_stability.
        cfg           : Analysis configuration (asset_id, run_id, thresholds).
        output_dir    : Directory to write analyst_report.md and feature_set.json.
                        Must exist before calling.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    created_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    all_features: list[str] = quality_df["feature"].to_list()

    # --- aggregate decisions per feature ---
    selected : list[str]       = []
    dropped  : list[dict]      = []   # {"col": ..., "reason": ..., "step": ...}
    review   : list[str]       = []

    # quality drops
    quality_drop = {
        r["feature"]: r["drop_reason"]
        for r in quality_df.iter_rows(named=True)
        if r["decision"] == "drop"
    }
    quality_review = {
        r["feature"]
        for r in quality_df.iter_rows(named=True)
        if r["decision"] == "review"
    }

    # relation drops: drop if ALL targets say 'weak' or 'leakage'
    if len(relation_df) > 0:
        relation_keep = {
            r["feature"]
            for r in relation_df.iter_rows(named=True)
            if r["decision"] == "keep"
        }
        relation_leak = {
            r["feature"]
            for r in relation_df.iter_rows(named=True)
            if r["decision"] == "leakage"
        }
    else:
        relation_keep = set(all_features)
        relation_leak : set[str] = set()

    # redundancy drops
    redundancy_drop = {
        r["feature"]: r["drop_reason"]
        for r in redundancy_df.iter_rows(named=True)
        if r["decision"] == "drop"
    }

    # stability decayed
    if len(stability_df) > 0:
        decayed_features = {
            r["feature"]
            for r in stability_df.iter_rows(named=True)
            if r["stability_flag"] == "decayed"
        }
        unstable_features = {
            r["feature"]
            for r in stability_df.iter_rows(named=True)
            if r["stability_flag"] in ("unstable", "review")
        } - decayed_features
    else:
        decayed_features  : set[str] = set()
        unstable_features : set[str] = set()

    for feat in all_features:
        reasons: list[str] = []

        if feat in quality_drop:
            reasons.append(f"quality: {quality_drop[feat]}")
        if feat in relation_leak:
            reasons.append("relation: leakage suspect")
        if feat not in relation_keep and feat not in relation_leak:
            reasons.append("relation: weak signal across all targets")
        if feat in redundancy_drop:
            reasons.append(f"redundancy: {redundancy_drop[feat]}")
        if feat in decayed_features:
            reasons.append("stability: decayed in recent buckets")

        is_review = (
            feat in quality_review
            or feat in unstable_features
        ) and not reasons

        if reasons:
            dropped.append({"col": feat, "reason": " | ".join(reasons)})
        elif is_review:
            review.append(feat)
        else:
            selected.append(feat)

    # --- feature_set.json ---
    feature_set = {
        "run_id"     : cfg.run_id,
        "asset_id"   : cfg.asset_id,
        "created_at" : created_at,
        "target_cols": list(cfg.target_cols),
        "selected"   : selected,
        "dropped"    : dropped,
        "review"     : review,
        "thresholds" : {
            "max_null_rate"        : cfg.max_null_rate,
            "max_inf_rate"         : cfg.max_inf_rate,
            "min_variance"         : cfg.min_variance,
            "max_outlier_ratio"    : cfg.max_outlier_ratio,
            "min_spearman_abs"     : cfg.min_spearman_abs,
            "max_spearman_leakage" : cfg.max_spearman_leakage,
            "pearson_cluster_thr"  : cfg.pearson_cluster_thr,
            "spearman_cluster_thr" : cfg.spearman_cluster_thr,
            "stability_bucket_days": cfg.stability_bucket_days,
            "max_drift_threshold"  : cfg.max_drift_threshold,
        },
    }

    json_path = output_dir / "feature_set.json"
    json_path.write_text(json.dumps(feature_set, indent=2), encoding="utf-8")
    logger.info("generate_outputs: wrote %s", json_path)

    # --- analyst_report.md ---
    report_lines: list[str] = [
        "# Feature Engineering Report",
        "",
        f"**run_id:** {cfg.run_id}  ",
        f"**asset_id:** {cfg.asset_id}  ",
        f"**created_at:** {created_at}  ",
        f"**targets:** {', '.join(cfg.target_cols)}",
        "",
        "---",
        "",
        "## Summary",
        "",
        "| Outcome | Count |",
        "|---------|-------|",
        f"| Selected | {len(selected)} |",
        f"| Dropped | {len(dropped)} |",
        f"| Review | {len(review)} |",
        f"| Total | {len(all_features)} |",
        "",
        "---",
        "",
        f"## Selected Features ({len(selected)})",
        "",
    ]
    for feat in selected:
        report_lines.append(f"- `{feat}`")

    report_lines += [
        "",
        "---",
        "",
        f"## Dropped Features ({len(dropped)})",
        "",
    ]
    for entry in dropped:
        report_lines.append(f"- `{entry['col']}` — {entry['reason']}")

    if review:
        report_lines += [
            "",
            "---",
            "",
            f"## Review Features ({len(review)})",
            "",
            "These features passed all hard thresholds but show marginal quality or "
            "instability.  Inspect before including in modeling.",
            "",
        ]
        for feat in review:
            report_lines.append(f"- `{feat}`")

    report_lines += [
        "",
        "---",
        "",
        "## Analysis Parameters",
        "",
        "| Parameter | Value |",
        "|-----------|-------|",
    ]
    for k, v in feature_set["thresholds"].items():
        report_lines.append(f"| `{k}` | {v} |")

    report_path = output_dir / "analyst_report.md"
    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    logger.info("generate_outputs: wrote %s", report_path)

    logger.info(
        "generate_outputs: done — selected=%d dropped=%d review=%d",
        len(selected), len(dropped), len(review),
    )
