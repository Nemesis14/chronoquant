"""Helpers for the 3200 target yearly regime analysis notebook."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

import analyst.db_utils as dbu

try:
    from scipy.stats import ks_2samp, wasserstein_distance
except ImportError:  # pragma: no cover - notebook fallback
    ks_2samp = None
    wasserstein_distance = None


@dataclass(frozen=True)
class CoverageCutoff:
    """Coverage cutoff used to compare the csonka 2026 period with prior years."""

    month: int
    day: int
    max_timestamp: pd.Timestamp


def load_target_frame(target_col: str = "long_mfe_fw60") -> pd.DataFrame:
    """Load the target table for one target column and add calendar fields.

    Args:
        target_col: Target column to analyze.

    Returns:
        DataFrame with ``open_time``, target values, and derived calendar columns.
    """
    df = dbu.load_table("target")
    df["open_time"] = pd.to_datetime(df["open_time"])
    df = df[["open_time", target_col]].copy()
    df[target_col] = pd.to_numeric(df[target_col], errors="coerce")

    out = df.copy()
    out["year"] = out["open_time"].dt.year
    out["month"] = out["open_time"].dt.month
    out["quarter"] = out["open_time"].dt.quarter
    out["year_month"] = out["open_time"].dt.to_period("M").dt.to_timestamp()
    out["day"] = out["open_time"].dt.day
    return out


def coverage_summary(df: pd.DataFrame, target_col: str) -> pd.DataFrame:
    """Return row coverage and valid-row coverage by year."""
    summary = (
        df.groupby("year")
        .agg(
            rows=("open_time", "size"),
            valid_rows=(target_col, lambda s: int(s.notna().sum())),
            min_time=("open_time", "min"),
            max_time=("open_time", "max"),
        )
        .reset_index()
    )
    summary["valid_rate"] = summary["valid_rows"] / summary["rows"]
    return summary


def yearly_stats(df: pd.DataFrame, target_col: str) -> pd.DataFrame:
    """Return annual distribution summary for the selected target."""
    valid = df.dropna(subset=[target_col]).copy()
    base = (
        valid.groupby("year")[target_col]
        .agg(["count", "mean", "std", "min", "median", "max"])
        .reset_index()
    )
    q = (
        valid.groupby("year")[target_col]
        .quantile([0.01, 0.05, 0.25, 0.75, 0.95, 0.99])
        .unstack()
        .reset_index()
    )
    q.columns = ["year", "q01", "q05", "q25", "q75", "q95", "q99"]
    return base.merge(q, on="year", how="left")


def monthly_summary(df: pd.DataFrame, target_col: str) -> pd.DataFrame:
    """Return monthly mean/std/median by year."""
    valid = df.dropna(subset=[target_col]).copy()
    return (
        valid.groupby(["year", "month", "year_month"])[target_col]
        .agg(["count", "mean", "std", "median"])
        .reset_index()
        .sort_values(["year", "month"])
    )


def quarterly_summary(df: pd.DataFrame, target_col: str) -> pd.DataFrame:
    """Return quarterly mean/std/median by year."""
    valid = df.dropna(subset=[target_col]).copy()
    return (
        valid.groupby(["year", "quarter"])[target_col]
        .agg(["count", "mean", "std", "median"])
        .reset_index()
        .sort_values(["year", "quarter"])
    )


def pairwise_distribution_distance(df: pd.DataFrame, target_col: str) -> pd.DataFrame:
    """Return pairwise annual distribution distances.

    Uses Wasserstein distance and KS statistic when SciPy is available.
    """
    valid = df.dropna(subset=[target_col]).copy()
    years = sorted(valid["year"].unique())
    rows: list[dict[str, float | int]] = []

    for idx, year1 in enumerate(years):
        arr1 = valid.loc[valid["year"] == year1, target_col].to_numpy()
        for year2 in years[idx + 1 :]:
            arr2 = valid.loc[valid["year"] == year2, target_col].to_numpy()
            rows.append(
                {
                    "year1": year1,
                    "year2": year2,
                    "wasserstein": (
                        float(wasserstein_distance(arr1, arr2))
                        if wasserstein_distance is not None
                        else np.nan
                    ),
                    "ks_stat": (
                        float(ks_2samp(arr1, arr2).statistic)
                        if ks_2samp is not None
                        else np.nan
                    ),
                }
            )

    return pd.DataFrame(rows).sort_values("wasserstein")


def year_cutoff_for_latest(df: pd.DataFrame) -> CoverageCutoff:
    """Return the cutoff month/day implied by the latest available year."""
    latest_year = int(df["year"].max())
    latest_ts = pd.Timestamp(df.loc[df["year"] == latest_year, "open_time"].max())
    return CoverageCutoff(
        month=int(latest_ts.month),
        day=int(latest_ts.day),
        max_timestamp=latest_ts,
    )


def same_span_comparison(df: pd.DataFrame, target_col: str) -> pd.DataFrame:
    """Compare each year with the same month/day span as the latest year."""
    valid = df.dropna(subset=[target_col]).copy()
    cutoff = year_cutoff_for_latest(valid)
    latest_year = int(valid["year"].max())

    same_span = valid[
        (valid["month"] < cutoff.month)
        | ((valid["month"] == cutoff.month) & (valid["day"] <= cutoff.day))
    ].copy()

    base = same_span.loc[same_span["year"] == latest_year, target_col].to_numpy()
    rows: list[dict[str, float | int]] = []
    for year in sorted(same_span["year"].unique()):
        arr = same_span.loc[same_span["year"] == year, target_col].to_numpy()
        rows.append(
            {
                "year": int(year),
                "rows": int(len(arr)),
                "mean": float(np.mean(arr)),
                "std": float(np.std(arr)),
                "median": float(np.median(arr)),
                "p95": float(np.quantile(arr, 0.95)),
                "p99": float(np.quantile(arr, 0.99)),
                "vs_latest_mean_diff": float(np.mean(base) - np.mean(arr)),
                "vs_latest_wasserstein": (
                    float(wasserstein_distance(base, arr))
                    if wasserstein_distance is not None
                    else np.nan
                ),
                "vs_latest_ks_stat": (
                    float(ks_2samp(base, arr).statistic)
                    if ks_2samp is not None
                    else np.nan
                ),
            }
        )
    return pd.DataFrame(rows).sort_values("year")


def monthly_profile_correlation(df: pd.DataFrame, target_col: str) -> pd.DataFrame:
    """Return correlation of yearly monthly profiles with the latest year."""
    valid = df.dropna(subset=[target_col]).copy()
    pivot = (
        valid.groupby(["year", "month"])[target_col]
        .mean()
        .reset_index()
        .pivot(index="month", columns="year", values=target_col)
    )

    latest_year = int(valid["year"].max())
    rows: list[dict[str, float | int]] = []
    for year in pivot.columns:
        if year == latest_year:
            continue
        common = pivot[[latest_year, year]].dropna()
        rows.append(
            {
                "year": int(year),
                "common_months": int(len(common)),
                "corr_with_latest": float(common[latest_year].corr(common[year]))
                if len(common) >= 2
                else np.nan,
            }
        )
    return pd.DataFrame(rows).sort_values("corr_with_latest", ascending=False)


def relative_seasonality(df: pd.DataFrame, target_col: str, period: str) -> pd.DataFrame:
    """Return seasonal effect relative to each year's average.

    Args:
        df: Target frame.
        target_col: Selected target column.
        period: ``"month"`` or ``"quarter"``.
    """
    if period not in {"month", "quarter"}:
        raise ValueError("period must be 'month' or 'quarter'")

    valid = df.dropna(subset=[target_col]).copy()
    grouped = (
        valid.groupby(["year", period])[target_col]
        .mean()
        .reset_index()
        .rename(columns={target_col: "period_mean"})
    )
    year_mean = (
        valid.groupby("year")[target_col].mean().reset_index().rename(columns={target_col: "year_mean"})
    )
    rel = grouped.merge(year_mean, on="year", how="left")
    rel["relative_to_year_mean"] = rel["period_mean"] / rel["year_mean"] - 1.0
    summary = (
        rel.groupby(period)["relative_to_year_mean"]
        .agg(["mean", "std", "min", "max", "count"])
        .reset_index()
    )
    return rel, summary


def recommendation_lines(
    same_span_df: pd.DataFrame,
    profile_corr_df: pd.DataFrame,
) -> list[str]:
    """Generate short recommendation bullets from the comparison outputs."""
    latest_year = int(same_span_df["year"].max())
    comparable = (
        same_span_df.loc[same_span_df["year"] != latest_year]
        .sort_values("vs_latest_wasserstein")
        .head(3)["year"]
        .astype(int)
        .tolist()
    )
    farthest = (
        same_span_df.loc[same_span_df["year"] != latest_year]
        .sort_values("vs_latest_wasserstein", ascending=False)
        .head(2)["year"]
        .astype(int)
        .tolist()
    )
    best_corr = (
        profile_corr_df.sort_values("corr_with_latest", ascending=False)
        .head(2)["year"]
        .astype(int)
        .tolist()
    )

    return [
        (
            f"A {latest_year}-os target szintje a legközelebb ezekhez az évekhez áll: "
            f"{', '.join(map(str, comparable))}."
        ),
        (
            f"A legtávolabbi rezsimek a {', '.join(map(str, farthest))} évek, "
            "ezeket azonos súllyal kezelni torzíthatja a kalibrációt."
        ),
        (
            f"A havi profil hasonlóság alapján a legközelebbi minták: "
            f"{', '.join(map(str, best_corr))}."
        ),
        (
            "A teljes history megtartható expanding-window tanításhoz, "
            "de a kalibrációt és a döntési küszöböket recency-aware módon érdemes hangolni."
        ),
    ]
