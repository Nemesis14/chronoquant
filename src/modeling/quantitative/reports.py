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

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

METRIC_COLUMNS = [
    "roc_auc",
    "pr_auc",
    "brier_score",
    "log_loss",
]

METRIC_PAIR_COLUMNS = [
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
    best_cv_df = pd.DataFrame([best_cv_performance_row(artifacts, cv_summary_df, tuning_param)])
    final_df = pd.DataFrame([final_performance_row(artifacts)])
    validation_calibration_html = validation_calibration_section(output_dir)

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

    <h2>CV summary by parameter</h2>
    {_df_to_html(cv_summary_df)}

    <h2>Best-parameter CV performance</h2>
    {_df_to_html(best_cv_df)}

    {validation_calibration_html}

    <h2>Final performance</h2>
    {_df_to_html(final_df)}

    <h2>{html.escape(feature_table_title)}</h2>
    {_df_to_html(feature_df)}
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

    return cv_df.groupby(tuning_param, as_index=False).agg(**aggregations)  # type: ignore[return-value]


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
# validation_calibration_section(output_dir: Path) -> str
# =============================================================================
# Purpose:
#  - Render a calibration/lift section from best-parameter validation predictions
# =============================================================================
def validation_calibration_section(output_dir: Path) -> str:
    prediction_path = output_dir / "validation_predictions.csv"
    if not prediction_path.exists():
        return ""

    predictions_df = pd.read_csv(prediction_path)
    summary_df = validation_calibration_summary(predictions_df, n_bins=20)
    if summary_df.empty:
        return ""

    plot_uri = validation_calibration_plot_data_uri(summary_df)
    return f"""
    <h2>Validation Probability Calibration</h2>
    <img src="{plot_uri}" alt="Validation probability calibration by bin">
    {_df_to_html(summary_df)}
"""


# =============================================================================
# validation_calibration_summary(predictions_df: pd.DataFrame, n_bins: int) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Split validation predictions into equal-count probability bins
#  - Compare mean predicted probability with realized target rate
# =============================================================================
def validation_calibration_summary(
    predictions_df: pd.DataFrame,
    n_bins: int = 20,
) -> pd.DataFrame:
    required = {"y_true", "y_pred"}
    if predictions_df.empty or not required.issubset(predictions_df.columns):
        return pd.DataFrame()

    df = predictions_df[["y_true", "y_pred"]].copy()
    df["y_true"] = pd.to_numeric(df["y_true"], errors="coerce")
    df["y_pred"] = pd.to_numeric(df["y_pred"], errors="coerce")
    df = df.dropna(subset=["y_true", "y_pred"]).sort_values("y_pred").reset_index(drop=True)  # type: ignore[call-overload]
    if df.empty:
        return pd.DataFrame()

    bin_count = min(n_bins, len(df))
    df["bin"] = pd.qcut(df.index, q=bin_count, labels=False, duplicates="drop") + 1  # type: ignore[operator]
    base_rate = float(df["y_true"].mean())  # type: ignore[arg-type]
    summary = df.groupby("bin", as_index=False).agg(
        n=("y_true", "size"),
        pred_min=("y_pred", "min"),
        pred_max=("y_pred", "max"),
        pred_mean=("y_pred", "mean"),
        target_rate=("y_true", "mean"),
    )
    summary["baseline_target_rate"] = base_rate  # type: ignore[index]
    summary["lift"] = summary["target_rate"] / base_rate if base_rate > 0 else None  # type: ignore[index]
    return summary[  # type: ignore[return-value]
        [
            "bin",
            "n",
            "pred_min",
            "pred_max",
            "pred_mean",
            "target_rate",
            "baseline_target_rate",
            "lift",
        ]
    ]


# =============================================================================
# validation_calibration_plot_data_uri(summary_df: pd.DataFrame) -> str
# =============================================================================
# Purpose:
#  - Build base64 PNG for prediction mean vs realized target-rate by bin
# =============================================================================
def validation_calibration_plot_data_uri(summary_df: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(10, 4.8))
    ax.plot(summary_df["bin"], summary_df["pred_mean"], marker="o", label="Mean predicted probability")
    ax.plot(summary_df["bin"], summary_df["target_rate"], marker="o", label="Realized target rate")
    if "baseline_target_rate" in summary_df.columns and not summary_df.empty:
        baseline = summary_df["baseline_target_rate"].iloc[0]
        ax.axhline(baseline, color="gray", linestyle="--", linewidth=1.4, label="Baseline target rate")

    ax.set_xlabel("Prediction bin (low to high probability)")
    ax.set_ylabel("rate")
    ax.set_title("Validation calibration by equal-count probability bin")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    buffer = BytesIO()
    fig.savefig(buffer, format="png", dpi=140)
    plt.close(fig)
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


# =============================================================================
# best_cv_performance_row(...) -> dict
# =============================================================================
# Purpose:
#  - Return train/valid metric pairs at the selected tuning value
# =============================================================================
def best_cv_performance_row(
    artifacts: dict,
    cv_summary_df: pd.DataFrame,
    tuning_param: str,
) -> dict:
    best_value = artifacts.get("best_tuning_value", artifacts.get(f"best_{tuning_param}"))
    cv_row = cv_summary_df.loc[cv_summary_df[tuning_param] == best_value].iloc[0]
    result = {
        tuning_param: best_value,
    }
    for metric in METRIC_PAIR_COLUMNS:
        result[f"train_{metric}"] = cv_row.get(f"train_{metric}")
        result[f"valid_{metric}"] = cv_row.get(f"valid_{metric}")
    return result


# =============================================================================
# final_performance_row(artifacts: dict) -> dict
# =============================================================================
# Purpose:
#  - Return final train/test metric pairs in one comparable report row
# =============================================================================
def final_performance_row(artifacts: dict) -> dict:
    final_train = artifacts["final_metrics"]["final_train"]
    final_test = artifacts["final_metrics"]["final_test"]
    result = {
        "train_n": final_train["n"],
        "test_n": final_test["n"],
        "train_positive_rate": final_train["positive_rate"],
        "test_positive_rate": final_test["positive_rate"],
    }
    for metric in METRIC_PAIR_COLUMNS:
        result[f"train_{metric}"] = final_train.get(metric)
        result[f"test_{metric}"] = final_test.get(metric)
    return result


# =============================================================================
# _df_to_html(df: pd.DataFrame) -> str
# =============================================================================
# Purpose:
#  - Render compact HTML tables with numeric formatting
# =============================================================================
def _df_to_html(df: pd.DataFrame) -> str:
    return df.to_html(index=False, escape=True, float_format=lambda x: f"{x:.6f}")
