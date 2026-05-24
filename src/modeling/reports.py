# =============================================================================
# Shared model training reports
# =============================================================================
# Purpose:
#  - Generate consistent HTML reports for all model families
#  - Keep metrics, CV summaries, and feature tables model-independent
# =============================================================================

from __future__ import annotations

import base64
import html
from io import BytesIO
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


METRIC_COLUMNS = [
    "roc_auc",
    "pr_auc",
    "brier_score",
    "log_loss",
]


# =============================================================================
# write_training_report(...) -> None
# =============================================================================
# Purpose:
#  - Build a standalone HTML report from standard training artifacts
# =============================================================================
def write_training_report(
    output_dir: str | Path,
    model_id: str,
    target_col: str,
    sample: dict,
    cv_df: pd.DataFrame,
    sample_sizes: list[dict],
    artifacts: dict,
    tuning_param: str,
    tuning_label: str,
    feature_rows: list[dict],
    feature_table_title: str,
    tuning_xscale: str = "linear",
    auxiliary_columns: dict[str, str] | None = None,
) -> None:
    output_dir = Path(output_dir)
    auxiliary_columns = auxiliary_columns or {}
    cv_summary_df = cv_summary(
        cv_df=cv_df,
        tuning_param=tuning_param,
        auxiliary_columns=auxiliary_columns,
    )
    plot_uri = cv_plot_data_uri(
        cv_summary_df=cv_summary_df,
        tuning_param=tuning_param,
        tuning_label=tuning_label,
        xscale=tuning_xscale,
    )
    feature_df = pd.DataFrame(feature_rows)
    performance_df = pd.DataFrame(
        performance_rows(
            artifacts=artifacts,
            cv_summary_df=cv_summary_df,
            tuning_param=tuning_param,
        )
    )

    best_value = artifacts.get("best_tuning_value", artifacts.get(f"best_{tuning_param}"))
    html_text = f"""<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <title>{html.escape(model_id)} training report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 32px; color: #222; }}
        h1, h2 {{ margin-bottom: 8px; }}
        table {{ border-collapse: collapse; margin: 12px 0 28px 0; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 6px 8px; text-align: right; }}
        th:first-child, td:first-child {{ text-align: left; }}
        th {{ background: #f3f5f7; }}
        .meta {{ color: #555; margin-bottom: 24px; }}
        img {{ max-width: 100%; border: 1px solid #ddd; }}
    </style>
</head>
<body>
    <h1>{html.escape(model_id)} training report</h1>
    <div class="meta">
        trainer: <strong>{html.escape(str(artifacts.get("trainer", "")))}</strong><br>
        target: <strong>{html.escape(target_col)}</strong><br>
        sample: <strong>{html.escape(sample["sample_id"])}</strong><br>
        best {html.escape(tuning_label)}: <strong>{html.escape(str(best_value))}</strong><br>
        input features: <strong>{artifacts.get("n_features_input")}</strong><br>
        row stride: <strong>{artifacts.get("row_stride")}</strong>
    </div>

    <h2>Sample sizes</h2>
    {_df_to_html(pd.DataFrame(sample_sizes))}

    <h2>Train and CV metrics by {html.escape(tuning_label)}</h2>
    <img src="{plot_uri}" alt="Train and CV metrics by {html.escape(tuning_label)}">

    <h2>CV summary</h2>
    {_df_to_html(cv_summary_df)}

    <h2>{html.escape(feature_table_title)}</h2>
    {_df_to_html(feature_df)}

    <h2>Final performance</h2>
    {_df_to_html(performance_df)}
</body>
</html>
"""
    (output_dir / "report.html").write_text(html_text, encoding="utf-8")


# =============================================================================
# cv_summary(...) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Aggregate per-fold CV results by one tuning parameter
# =============================================================================
def cv_summary(
    cv_df: pd.DataFrame,
    tuning_param: str,
    auxiliary_columns: dict[str, str] | None = None,
) -> pd.DataFrame:
    auxiliary_columns = auxiliary_columns or {}
    aggregations = {}
    for col, method in auxiliary_columns.items():
        if col in cv_df.columns:
            aggregations[col] = (col, method)

    for metric in METRIC_COLUMNS:
        train_col = f"train_{metric}"
        valid_col = f"valid_{metric}"
        if train_col in cv_df.columns:
            aggregations[train_col] = (train_col, "mean")
        if valid_col in cv_df.columns:
            aggregations[valid_col] = (valid_col, "mean")

    return cv_df.groupby(tuning_param, as_index=False).agg(**aggregations)


# =============================================================================
# cv_plot_data_uri(...) -> str
# =============================================================================
# Purpose:
#  - Build base64 PNG for side-by-side train/CV metric plots
# =============================================================================
def cv_plot_data_uri(
    cv_summary_df: pd.DataFrame,
    tuning_param: str,
    tuning_label: str,
    xscale: str,
) -> str:
    fig, axes = plt.subplots(1, 3, figsize=(16, 4.5), sharex=True)
    plot_specs = [
        (axes[0], "ROC AUC", "roc_auc"),
        (axes[1], "PR AUC", "pr_auc"),
        (axes[2], "Brier score", "brier_score"),
    ]
    for ax, title, metric in plot_specs:
        train_col = f"train_{metric}"
        valid_col = f"valid_{metric}"
        if train_col in cv_summary_df.columns:
            ax.plot(cv_summary_df[tuning_param], cv_summary_df[train_col], marker="o", label="Train")
        if valid_col in cv_summary_df.columns:
            ax.plot(cv_summary_df[tuning_param], cv_summary_df[valid_col], marker="o", label="CV")
        ax.set_xscale(xscale)
        ax.set_xlabel(tuning_label)
        ax.set_ylabel("metric value")
        ax.set_title(title)
        ax.grid(True, alpha=0.3)
        ax.legend()

    fig.tight_layout()
    buffer = BytesIO()
    fig.savefig(buffer, format="png", dpi=140)
    plt.close(fig)
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


# =============================================================================
# performance_rows(...) -> list[dict]
# =============================================================================
# Purpose:
#  - Flatten final train/test and selected CV metrics for report table
# =============================================================================
def performance_rows(
    artifacts: dict,
    cv_summary_df: pd.DataFrame,
    tuning_param: str,
) -> list[dict]:
    rows = []
    for split, metrics in artifacts["final_metrics"].items():
        rows.append(
            {
                "split": split,
                "n": metrics["n"],
                "positive_rate": metrics["positive_rate"],
                "roc_auc": metrics["roc_auc"],
                "pr_auc": metrics["pr_auc"],
                "brier_score": metrics["brier_score"],
                "log_loss": metrics["log_loss"],
            }
        )

    best_value = artifacts.get("best_tuning_value", artifacts.get(f"best_{tuning_param}"))
    cv_row = cv_summary_df.loc[cv_summary_df[tuning_param] == best_value].iloc[0]
    rows.extend(
        [
            {
                "split": "cv_train_mean_at_best_param",
                "n": None,
                "positive_rate": None,
                "roc_auc": cv_row.get("train_roc_auc"),
                "pr_auc": cv_row.get("train_pr_auc"),
                "brier_score": cv_row.get("train_brier_score"),
                "log_loss": cv_row.get("train_log_loss"),
            },
            {
                "split": "cv_valid_mean_at_best_param",
                "n": None,
                "positive_rate": None,
                "roc_auc": cv_row.get("valid_roc_auc"),
                "pr_auc": cv_row.get("valid_pr_auc"),
                "brier_score": cv_row.get("valid_brier_score"),
                "log_loss": cv_row.get("valid_log_loss"),
            },
        ]
    )
    return rows


# =============================================================================
# _df_to_html(df: pd.DataFrame) -> str
# =============================================================================
# Purpose:
#  - Render compact HTML tables with numeric formatting
# =============================================================================
def _df_to_html(df: pd.DataFrame) -> str:
    return df.to_html(index=False, escape=True, float_format=lambda x: f"{x:.6f}")
