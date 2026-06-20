"""Sampling config dataclasses — yearly random-hour and walk-forward CV."""

from dataclasses import dataclass


@dataclass(frozen=True)
class WalkForwardSamplingConfig:
    """Immutable configuration for walk-forward CV sampling.

    Generates ``n_folds`` time-based folds with explicit train/valid windows.
    Each fold shifts by ``shift_months`` from the previous one.  The first
    fold's validation window starts at month ``year-10`` (anchor year October).

    Args:
        sample_id     : Unique identifier; becomes the sample directory name.
        asset_id      : Asset key from config/assets.json (e.g. 'solusdt').
        year          : Anchor calendar year (e.g. 2021).  First valid window
                        starts at ``year-10-01``.
        seed          : Fixed random seed for hourly observation selection.
        train_months  : Length of training window in months.
        valid_months  : Length of validation window in months.
        shift_months  : Months to shift between successive folds.  Set equal
                        to ``valid_months`` for non-overlapping validation.
        purge_minutes : Gap in minutes excluded around each fold boundary.
                        Default 240 conservatively covers max feature lookback.
        target_cols   : Target columns to include in sample output.
        feature_cols  : Feature columns to include.  Empty tuple = all feat_*
                        columns discovered from quant_train at runtime.
    """

    sample_id    : str
    asset_id     : str
    year         : int
    seed         : int
    train_months : int = 9
    valid_months : int = 3
    shift_months : int = 3
    purge_minutes: int = 240
    target_cols  : tuple[str, ...] = ("long_mfe_fw60", "short_mfe_fw60")
    feature_cols : tuple[str, ...] = ()


@dataclass(frozen=True)
class YearlySamplingConfig:
    """Immutable configuration for generating a yearly random-hour sample.

    Args:
        sample_id     : Unique identifier; becomes the sample directory name.
        asset_id      : Asset key from config/assets.json (e.g. 'solusdt').
        year          : Calendar year to sample (e.g. 2021).
        seed          : Fixed random seed. Controls both hourly selection and
                        validation week selection. Suggested default: 42 + year.
        purge_minutes : Gap in minutes to exclude around each validation week
                        boundary. Default 240 covers the max feature lookback.
        target_cols   : Target columns to include in sample output.
        feature_cols  : Feature columns to include. Empty tuple = all feat_*
                        columns discovered from quant_train at runtime.
    """

    sample_id    : str
    asset_id     : str
    year         : int
    seed         : int
    purge_minutes: int = 240
    n_folds      : int = 4
    target_cols  : tuple[str, ...] = ("long_mfe_fw60", "short_mfe_fw60")
    feature_cols : tuple[str, ...] = ()
