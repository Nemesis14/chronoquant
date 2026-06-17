"""YearlySamplingConfig dataclass — parameters for yearly random-hour sampling."""

from dataclasses import dataclass


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
        test_months   : Number of trailing months of the year to reserve as
                        the test holdout (segment='test'). 0 disables test rows.
    """

    sample_id    : str
    asset_id     : str
    year         : int
    seed         : int
    purge_minutes: int = 240
    target_cols  : tuple[str, ...] = ("long_mfe_fw60", "short_mfe_fw60")
    feature_cols : tuple[str, ...] = ()
    test_months  : int = 1
