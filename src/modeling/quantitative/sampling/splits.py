"""Pure date-math expanding-window split generator.

No IO, no database access, no project imports.  Input and output are plain
strings in YYYY-MM-DD HH:MM:SS format.
"""

from datetime import datetime, timedelta

# %% Helpers


def _format_time(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S")


# %% Public API


def build_expanding_window_splits(
    data_start      : str,
    data_end        : str,
    min_train_days  : int,
    valid_days      : int,
    step_days       : int,
    test_days       : int,
    embargo_minutes : int | None = None,
) -> dict:
    """Build expanding training windows with fixed validation windows and a final test set.

    Args:
        data_start      : Earliest usable timestamp, 'YYYY-MM-DD HH:MM:SS'.
        data_end        : Latest labeled timestamp, 'YYYY-MM-DD HH:MM:SS'.
        min_train_days  : Minimum training window length in calendar days.
        valid_days      : Validation window length in calendar days.
        step_days       : Step between consecutive fold starts in calendar days.
        test_days       : Final holdout window length in calendar days.
        embargo_minutes : Gap between train end and valid start; 0 when None.

    Returns:
        Dict with keys 'folds' (list of fold dicts, 1-indexed) and 'test'.
        Each fold contains: fold, train_start, train_end, valid_start, valid_end.
        test contains: start, end.

    Raises:
        ValueError: If data_end <= data_start or no folds can be generated.
    """
    # --- parse ---
    data_start_ts = datetime.fromisoformat(data_start)
    data_end_ts   = datetime.fromisoformat(data_end)

    if data_end_ts <= data_start_ts:
        raise ValueError("data_end must be after data_start")

    embargo_td  = timedelta(minutes=embargo_minutes) if embargo_minutes else timedelta(0)
    one_minute  = timedelta(minutes=1)
    test_start  = data_end_ts - timedelta(days=test_days)
    cv_end      = test_start - embargo_td
    valid_start = data_start_ts + timedelta(days=min_train_days)

    # --- rolling folds ---
    folds   = []
    fold_no = 1

    while valid_start + timedelta(days=valid_days) <= cv_end:
        valid_end = valid_start + timedelta(days=valid_days) - one_minute
        train_end = valid_start - embargo_td - one_minute

        if train_end > data_start_ts:
            folds.append(
                {
                    "fold"        : fold_no,
                    "train_start" : _format_time(data_start_ts),
                    "train_end"   : _format_time(train_end),
                    "valid_start" : _format_time(valid_start),
                    "valid_end"   : _format_time(valid_end),
                }
            )
            fold_no += 1

        valid_start = valid_start + timedelta(days=step_days)

    if not folds:
        raise ValueError(
            "No folds generated. Reduce min_train_days, valid_days, or test_days."
        )

    return {
        "folds": folds,
        "test" : {
            "start": _format_time(test_start),
            "end"  : _format_time(data_end_ts),
        },
    }
