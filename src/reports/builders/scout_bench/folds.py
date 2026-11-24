# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportOptionalMemberAccess=false
"""Shared calendar purged CV folds (5300 "Közös protokoll"): identical for every model, hash-fixed.

Test = one calendar block (all assets together); train = every other block's events minus the purge
(full interval ``[t0 - B, t_c + H]`` = ``[event_start, label_end)`` overlapping the test block) and minus the embargo
(``H`` band on both edges of the test block). Because the blocks are calendar bands shared by all assets, a train event of
ANY asset that overlaps the test band in time is removed: the purge is cross-asset by construction.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np
import polars as pl

from reports.builders import wave_events as we


@dataclass(frozen=True)
class Fold:
    block: int
    train_idx: np.ndarray
    test_idx: np.ndarray
    n_purged: int
    n_embargoed: int


@dataclass(frozen=True)
class FoldSet:
    folds: tuple[Fold, ...]
    bounds: tuple[datetime, ...]
    embargo_min: int
    hash: str

    def __iter__(self) -> Iterator[Fold]:
        return iter(self.folds)

    def __len__(self) -> int:
        return len(self.folds)


def make_folds(frame: pl.DataFrame, bounds: list[datetime] | None = None, embargo_min: int | None = None) -> FoldSet:
    """Folds over a prepared frame (``prepare_frame``); ``embargo_min`` defaults to the largest ``h_min`` (H)."""
    bounds = bounds or we.cv_block_bounds(we.params_v2())
    emb_min = int(embargo_min if embargo_min is not None else frame["h_min"].max())
    emb = np.timedelta64(timedelta(minutes=emb_min))
    es = frame["event_start"].to_numpy().astype("datetime64[us]")
    le = frame["label_end"].to_numpy().astype("datetime64[us]")
    blk = frame["cv_block"].to_numpy()
    folds = []
    for k in range(len(bounds) - 1):
        lo, hi = np.datetime64(bounds[k], "us"), np.datetime64(bounds[k + 1], "us")
        test = blk == k
        overlap = (es < hi) & (le > lo)
        overlap_emb = (es < hi + emb) & (le > lo - emb)
        cand = ~test
        train = cand & ~overlap_emb
        folds.append(Fold(k, np.flatnonzero(train), np.flatnonzero(test),
                          int((cand & overlap).sum()), int((cand & overlap_emb & ~overlap).sum())))
    h = hashlib.sha256()
    h.update(repr([b.isoformat() for b in bounds]).encode())
    h.update(str(emb_min).encode())
    h.update(frame["asset_id"].to_numpy().astype(str).tobytes())
    h.update(frame["t0"].to_numpy().astype("datetime64[us]").tobytes())
    for f in folds:
        h.update(f.train_idx.tobytes())
        h.update(f.test_idx.tobytes())
    return FoldSet(tuple(folds), tuple(bounds), emb_min, h.hexdigest()[:16])
