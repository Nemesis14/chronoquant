"""SamplingConfig dataclass — all parameters needed to generate a sample definition."""

from dataclasses import dataclass

# %% Config


@dataclass(frozen=True)
class SamplingConfig:
    """Immutable configuration for generating a time-based CV sample.

    Args:
        sample_id               : Unique identifier written to the sample directory name.
        asset_id                : Asset key from config/assets.json (e.g. 'solusdt').
        target_col              : Target column name (e.g. 'long_mfe_fw60').
        target_horizon_minutes  : Forward-return horizon in minutes; used as default embargo.
        min_train_days          : Minimum number of calendar days in the first training fold.
        valid_days              : Validation window length in calendar days.
        step_days               : Step between consecutive fold starts in calendar days.
        test_days               : Final holdout window length in calendar days.
        embargo_minutes         : Gap between train end and valid start (default: target_horizon_minutes).
        round_to_months         : If True (default), round data_start up and data_end down to whole
                                  calendar months before generating splits. Ensures clean month boundaries
                                  for easier analysis and cross-version comparability.
    """

    sample_id              : str
    asset_id               : str
    target_col             : str
    target_horizon_minutes : int
    min_train_days         : int        = 730
    valid_days             : int        = 180
    step_days              : int        = 180
    test_days              : int        = 365
    embargo_minutes        : int | None = None
    round_to_months        : bool       = True
