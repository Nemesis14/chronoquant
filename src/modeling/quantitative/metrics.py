# =============================================================================
# Shared model metrics
# =============================================================================
# Purpose:
#  - Compute common binary classification metrics for all model families
#  - Keep metric definitions identical for long, short, and future models
# =============================================================================

import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)


# =============================================================================
# binary_classification_metrics(...) -> dict
# =============================================================================
# Purpose:
#  - Compute shared metrics from binary facts and predicted probabilities
# =============================================================================
def binary_classification_metrics(
    y_true,
    y_pred,
    lift_percentiles: tuple[float, ...] = (0.01, 0.05, 0.10),
    calibration_bins: int = 10,
) -> dict:
    y_true = pd.Series(y_true).astype(int).reset_index(drop=True)
    y_pred = pd.Series(y_pred).astype(float).reset_index(drop=True)

    valid_mask = y_true.notna() & y_pred.notna()
    y_true = y_true[valid_mask].reset_index(drop=True)  # type: ignore[union-attr]
    y_pred = y_pred[valid_mask].clip(1e-15, 1 - 1e-15).reset_index(drop=True)  # type: ignore[union-attr]

    if len(y_true) == 0:
        raise ValueError("Cannot compute metrics on an empty sample")

    positive_rate = float(y_true.mean())
    metrics = {
        "n": int(len(y_true)),
        "positive_count": int(y_true.sum()),
        "negative_count": int((1 - y_true).sum()),
        "positive_rate": positive_rate,
        "roc_auc": _safe_metric(roc_auc_score, y_true, y_pred),
        "pr_auc": _safe_metric(average_precision_score, y_true, y_pred),
        "log_loss": _safe_metric(log_loss, y_true, y_pred),
        "brier_score": _safe_metric(brier_score_loss, y_true, y_pred),
        "lift": lift_at_percentiles(y_true, y_pred, lift_percentiles),
        "calibration": calibration_table(y_true, y_pred, calibration_bins),
    }
    return metrics


# =============================================================================
# lift_at_percentiles(...) -> dict
# =============================================================================
# Purpose:
#  - Measure event-rate lift inside top prediction percentiles
# =============================================================================
def lift_at_percentiles(
    y_true,
    y_pred,
    percentiles: tuple[float, ...] = (0.01, 0.05, 0.10),
) -> dict:
    y_true = pd.Series(y_true).astype(int).reset_index(drop=True)
    y_pred = pd.Series(y_pred).astype(float).reset_index(drop=True)
    baseline_rate = float(y_true.mean())
    order = y_pred.sort_values(ascending=False).index

    result = {}
    for percentile in percentiles:
        if not 0 < percentile <= 1:
            raise ValueError(f"Percentile must be in (0, 1]: {percentile}")

        top_n = max(1, int(np.ceil(len(y_true) * percentile)))
        top_idx = order[:top_n]
        top_rate = float(y_true.loc[top_idx].mean())
        lift = None if baseline_rate == 0 else top_rate / baseline_rate
        key = _percentile_key(percentile)
        result[key] = {
            "top_n": int(top_n),
            "event_rate": top_rate,
            "lift": None if lift is None else float(lift),
        }

    return result


# =============================================================================
# calibration_table(...) -> list[dict]
# =============================================================================
# Purpose:
#  - Compare average predicted probability and actual event rate by bins
# =============================================================================
def calibration_table(y_true, y_pred, n_bins: int = 10) -> list[dict]:
    if n_bins < 2:
        raise ValueError("n_bins must be at least 2")

    df = pd.DataFrame(
        {
            "y_true": pd.Series(y_true).astype(int),
            "y_pred": pd.Series(y_pred).astype(float),
        }
    )
    df["bin"] = pd.cut(
        df["y_pred"],
        bins=np.linspace(0, 1, n_bins + 1),
        include_lowest=True,
        labels=False,
    )

    rows = []
    for bin_no, part in df.groupby("bin", dropna=False):
        if pd.isna(bin_no):  # type: ignore[arg-type]
            continue
        rows.append(
            {
                "bin": int(bin_no),  # type: ignore[arg-type]
                "n": int(len(part)),
                "mean_pred": float(part["y_pred"].mean()),  # type: ignore[arg-type]
                "event_rate": float(part["y_true"].mean()),  # type: ignore[arg-type]
            }
        )
    return rows


# =============================================================================
# _safe_metric(metric_fn, y_true, y_pred) -> float | None
# =============================================================================
# Purpose:
#  - Return None for metrics undefined on single-class samples
# =============================================================================
def _safe_metric(metric_fn, y_true, y_pred) -> float | None:
    try:
        value = float(metric_fn(y_true, y_pred))
        if np.isnan(value):
            return None
        return value
    except ValueError:
        return None


# =============================================================================
# _percentile_key(percentile: float) -> str
# =============================================================================
# Purpose:
#  - Convert 0.05 to 'top_5pct'
# =============================================================================
def _percentile_key(percentile: float) -> str:
    value = percentile * 100
    if value.is_integer():
        return f"top_{int(value)}pct"
    return f"top_{value:g}pct"
