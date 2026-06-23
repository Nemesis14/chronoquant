"""Rank-first score calibration for ChronoQuant strategy sessions.

Fits percentile rank lookup tables and isotonic regression calibrators on a
calibration period of the scored table (the snap+pred join from
``build_table.build_scored_table``), applies them to the full scored table, and
persists the file artifacts.

The calibration **method is unchanged** from the parquet era — only the source is
now an in-memory DataFrame (the DuckDB join) instead of strategy_table.parquet,
and the calibrated table is returned in-memory instead of being written back to
parquet.  The rank_lookup_long/short.parquet and isotonic_long/short.pkl files
remain (the live service loads them).
"""

import logging
import pickle
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression

import utils

logger = logging.getLogger(__name__)

# %% Internal helpers


def _artifact_dir(session_id: str) -> Path:
    """Return the artifact directory path for a session.

    Args:
        session_id: Strategy session identifier.

    Returns:
        Path to artifacts/{session_id}/.
    """
    return Path(utils._resolve_path("artifacts")) / session_id


def _build_rank_lookup(
    calib_df: pd.DataFrame,
    score_col: str,
    mfe_col: str,
) -> pd.DataFrame:
    """Build a rank lookup parquet DataFrame for one direction.

    The lookup contains one row per calibration-period observation, sorted by
    score_raw ascending. Each row carries its percentile rank, decile bucket id,
    and the aggregate stats (mean_mfe, hit_rate) for its bucket — repeated /
    denormalized so the lookup can be applied via np.interp without a secondary
    join.

    Args:
        calib_df  : Calibration-period rows (already filtered).
        score_col : Column name for raw model score (e.g. "pred_long_raw").
        mfe_col   : Column name for realized MFE (e.g. "long_mfe_fw60").

    Returns:
        DataFrame with columns:
            score_raw, score_pct, bucket_id,
            bucket_mean_mfe, bucket_hit_rate
        sorted by score_raw ascending.
    """
    scores = calib_df[score_col].to_numpy(dtype=float)
    mfe    = calib_df[mfe_col].to_numpy(dtype=float)
    n      = len(scores)

    sort_idx   = np.argsort(scores, kind="stable")
    scores_s   = scores[sort_idx]
    mfe_s      = mfe[sort_idx]
    score_pct  = np.arange(1, n + 1, dtype=float) / n                           # 1/n .. 1.0
    bucket_id  = np.minimum(np.ceil(score_pct * 10).astype(int), 10)            # 1-10

    # Aggregate stats per bucket — vectorised over 10 possible buckets
    bucket_mean_mfe   = np.empty(n, dtype=float)
    bucket_hit_rate   = np.empty(n, dtype=float)
    bucket_median_mfe = np.empty(n, dtype=float)
    bucket_p75_mfe    = np.empty(n, dtype=float)

    for bid in range(1, 11):
        mask = bucket_id == bid
        if mask.any():
            bm     = float(mfe_s[mask].mean())
            bhr    = float((mfe_s[mask] > 0).mean())
            bmed   = float(np.median(mfe_s[mask]))
            bp75   = float(np.percentile(mfe_s[mask], 75))
        else:
            bm     = 0.0
            bhr    = 0.0
            bmed   = 0.0
            bp75   = 0.0
        bucket_mean_mfe[mask]   = bm
        bucket_hit_rate[mask]   = bhr
        bucket_median_mfe[mask] = bmed
        bucket_p75_mfe[mask]    = bp75

    return pd.DataFrame({
        "score_raw"        : scores_s,
        "score_pct"        : score_pct,
        "bucket_id"        : bucket_id,
        "bucket_mean_mfe"  : bucket_mean_mfe,
        "bucket_hit_rate"  : bucket_hit_rate,
        "bucket_median_mfe": bucket_median_mfe,
        "bucket_p75_mfe"   : bucket_p75_mfe,
    })


# %% Public API


def fit_calibration(
    session_id : str,
    scored_df  : pd.DataFrame,
    start      : str,
    end        : str,
) -> tuple[pd.DataFrame, IsotonicRegression, IsotonicRegression]:
    """Fit rank calibration and isotonic regression on the calibration period.

    Takes the scored table (the snap+pred join), filters to the [start, end]
    window, builds percentile rank lookup tables, fits one IsotonicRegression per
    direction, and applies all calibrations to the entire scored table.

    New columns added to the returned DataFrame:
        score_pct_long, score_pct_short         — interpolated percentile (0.0-1.0)
        bucket_long, bucket_short               — decile bucket int 1-10
        bucket_mean_mfe_long/short              — bucket mean realized MFE
        bucket_hit_rate_long/short              — bucket fraction(mfe > 0)
        bucket_median_mfe_long/short            — bucket median realized MFE
        bucket_p75_mfe_long/short               — bucket 75th-percentile MFE
        pred_long_cal, pred_short_cal           — isotonic-predicted MFE

    File artifacts written to artifacts/{session_id}/:
        rank_lookup_long.parquet
        rank_lookup_short.parquet
        isotonic_long.pkl
        isotonic_short.pkl

    Args:
        session_id : Strategy session identifier.
        scored_df  : The scored table from build_scored_table (open_time,
                     pred_long_raw, pred_short_raw, long_mfe_fw60, short_mfe_fw60,
                     and optional price columns).
        start      : Calibration start date YYYY-MM-DD (inclusive).
        end        : Calibration end date YYYY-MM-DD (inclusive).

    Returns:
        Tuple of (calibrated_df, isotonic_long, isotonic_short).

    Raises:
        ValueError: If the calibration period produces no rows.
    """
    artifact_dir = _artifact_dir(session_id)

    df = scored_df.copy()
    logger.info("fit_calibration: scored table has %d rows", len(df))

    # --- Filter calibration period ---
    start_ts = f"{start} 00:00:00"
    end_ts   = f"{end} 23:59:59"

    open_time_str = df["open_time"].astype(str)
    mask     = (open_time_str >= start_ts) & (open_time_str <= end_ts)
    calib_df = cast(pd.DataFrame, df[mask].copy())

    if calib_df.empty:
        raise ValueError(
            f"No rows in calibration period {start} to {end}. "
            f"Full table range: {df['open_time'].min()} to {df['open_time'].max()}"
        )
    logger.info("Calibration period rows: %d", len(calib_df))

    artifact_dir.mkdir(parents=True, exist_ok=True)

    # --- Build and save rank lookup tables ---
    lookup_long  = _build_rank_lookup(calib_df, "pred_long_raw",  "long_mfe_fw60")
    lookup_short = _build_rank_lookup(calib_df, "pred_short_raw", "short_mfe_fw60")

    lookup_long_path  = artifact_dir / "rank_lookup_long.parquet"
    lookup_short_path = artifact_dir / "rank_lookup_short.parquet"
    lookup_long.to_parquet(lookup_long_path,   compression="zstd", index=False)
    lookup_short.to_parquet(lookup_short_path, compression="zstd", index=False)
    logger.info("Rank lookup tables saved: %s, %s", lookup_long_path, lookup_short_path)

    # --- Apply rank lookup to entire table via np.interp ---
    # np.interp extrapolates by clipping to boundary values
    full_long_raw  = df["pred_long_raw"].to_numpy(dtype=float)
    full_short_raw = df["pred_short_raw"].to_numpy(dtype=float)

    score_pct_long  = np.interp(
        full_long_raw,
        lookup_long["score_raw"].to_numpy(),
        lookup_long["score_pct"].to_numpy(),
    ).clip(0.0, 1.0)
    score_pct_short = np.interp(
        full_short_raw,
        lookup_short["score_raw"].to_numpy(),
        lookup_short["score_pct"].to_numpy(),
    ).clip(0.0, 1.0)

    bucket_long_arr  = np.minimum(np.ceil(score_pct_long  * 10).astype(int), 10)
    bucket_short_arr = np.minimum(np.ceil(score_pct_short * 10).astype(int), 10)

    # Build bucket-level stat lookup dicts (bid -> stat) from the lookup tables
    def _bucket_stat_map(lookup: pd.DataFrame, stat_col: str) -> dict[int, float]:
        return (
            lookup.groupby("bucket_id")[stat_col]
            .first()  # all rows in a bucket have the same repeated value
            .to_dict()
        )

    long_mean_map   = _bucket_stat_map(lookup_long,  "bucket_mean_mfe")
    long_hr_map     = _bucket_stat_map(lookup_long,  "bucket_hit_rate")
    long_median_map = _bucket_stat_map(lookup_long,  "bucket_median_mfe")
    long_p75_map    = _bucket_stat_map(lookup_long,  "bucket_p75_mfe")
    short_mean_map  = _bucket_stat_map(lookup_short, "bucket_mean_mfe")
    short_hr_map    = _bucket_stat_map(lookup_short, "bucket_hit_rate")
    short_median_map = _bucket_stat_map(lookup_short, "bucket_median_mfe")
    short_p75_map    = _bucket_stat_map(lookup_short, "bucket_p75_mfe")

    df["score_pct_long"]             = score_pct_long
    df["score_pct_short"]            = score_pct_short
    df["bucket_long"]                = bucket_long_arr
    df["bucket_short"]               = bucket_short_arr
    df["bucket_mean_mfe_long"]       = pd.array(bucket_long_arr).map(long_mean_map)    # type: ignore[attr-defined]
    df["bucket_mean_mfe_short"]      = pd.array(bucket_short_arr).map(short_mean_map)  # type: ignore[attr-defined]
    df["bucket_hit_rate_long"]       = pd.array(bucket_long_arr).map(long_hr_map)      # type: ignore[attr-defined]
    df["bucket_hit_rate_short"]      = pd.array(bucket_short_arr).map(short_hr_map)    # type: ignore[attr-defined]
    df["bucket_median_mfe_long"]     = pd.array(bucket_long_arr).map(long_median_map)  # type: ignore[attr-defined]
    df["bucket_p75_mfe_long"]        = pd.array(bucket_long_arr).map(long_p75_map)     # type: ignore[attr-defined]
    df["bucket_median_mfe_short"]    = pd.array(bucket_short_arr).map(short_median_map) # type: ignore[attr-defined]
    df["bucket_p75_mfe_short"]       = pd.array(bucket_short_arr).map(short_p75_map)   # type: ignore[attr-defined]

    # --- Fit isotonic regression calibrators (secondary layer) ---
    iso_long  = IsotonicRegression(out_of_bounds="clip", increasing=True)
    iso_short = IsotonicRegression(out_of_bounds="clip", increasing=True)

    iso_long.fit(
        X = calib_df["pred_long_raw"].to_numpy(),   # type: ignore[union-attr]
        y = calib_df["long_mfe_fw60"].to_numpy(),   # type: ignore[union-attr]
    )
    iso_short.fit(
        X = calib_df["pred_short_raw"].to_numpy(),  # type: ignore[union-attr]
        y = calib_df["short_mfe_fw60"].to_numpy(),  # type: ignore[union-attr]
    )
    logger.info("IsotonicRegression fitted for long and short")

    long_pkl_path  = artifact_dir / "isotonic_long.pkl"
    short_pkl_path = artifact_dir / "isotonic_short.pkl"
    with open(long_pkl_path, "wb") as f:
        pickle.dump(iso_long, f)
    with open(short_pkl_path, "wb") as f:
        pickle.dump(iso_short, f)
    logger.info("Calibrators saved: %s, %s", long_pkl_path, short_pkl_path)

    # --- Apply isotonic to entire table ---
    df["pred_long_cal"]  = iso_long.predict(full_long_raw)
    df["pred_short_cal"] = iso_short.predict(full_short_raw)

    logger.info("fit_calibration: calibrated table ready (%d rows)", len(df))
    return df, iso_long, iso_short
