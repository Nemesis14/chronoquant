"""Walk-forward cross-validation with purging and embargo for financial time series.

PurgedEmbargoCV implements a forward-only CV scheme where training always
precedes the test window. Purging removes training rows whose forward label
horizon overlaps with the test period. Embargo adds a safety gap between
training end and test start to eliminate short-term autocorrelation leakage.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generator

import numpy as np
import pandas as pd


@dataclass
class PurgedEmbargoCV:
    """Walk-forward CV with purging and embargo for overlapping labels.

    Each fold uses all available training data before the test window,
    minus rows whose label_end_ts leaks into the test period (purging)
    and minus a configurable embargo buffer gap (embargo).

    Args:
        n_splits: Number of test folds.
        embargo:  Minimum gap between the last training label end and the
                  first test event. Prevents short-term autocorrelation
                  contamination.
    """

    n_splits: int = 5
    embargo: pd.Timedelta = field(default_factory=lambda: pd.Timedelta("2D"))  # type: ignore[assignment]

    def split(
        self,
        event_ts: pd.Series,
        label_end_ts: pd.Series,
    ) -> Generator[tuple[np.ndarray, np.ndarray], None, None]:
        """Yield (train_idx, test_idx) integer index arrays for each fold.

        Folds are non-overlapping, equal-sized, forward-only time windows.
        Training for fold k = rows with event_ts strictly before the fold k
        test window, after purging label overlap and applying the embargo gap.

        Args:
            event_ts:     Series of event timestamps (open_time), length N.
            label_end_ts: Series of label end timestamps, same length and index.

        Yields:
            Tuple (train_idx, test_idx) of integer position arrays (iloc-based).
        """
        n = len(event_ts)
        ev_arr  = np.asarray(event_ts.values)
        lbl_arr = np.asarray(label_end_ts.values)
        order   = np.argsort(ev_arr, kind="stable")
        sorted_event_ts     = ev_arr[order]
        sorted_label_end_ts = lbl_arr[order]

        fold_size = n // self.n_splits

        for k in range(self.n_splits):
            test_start_pos = k * fold_size
            test_end_pos   = (k + 1) * fold_size if k < self.n_splits - 1 else n

            test_idx = order[test_start_pos:test_end_pos]

            test_start_ts = sorted_event_ts[test_start_pos]
            embargo_cutoff = pd.Timestamp(test_start_ts) - self.embargo

            # train = rows before the test window whose labels don't overlap
            # label_end_ts <= embargo_cutoff ensures the embargo gap
            pre_test_mask = (
                (sorted_event_ts < test_start_ts) &
                (sorted_label_end_ts <= embargo_cutoff)
            )
            train_idx = order[pre_test_mask]

            yield train_idx, test_idx

    def get_n_splits(self) -> int:
        return self.n_splits
