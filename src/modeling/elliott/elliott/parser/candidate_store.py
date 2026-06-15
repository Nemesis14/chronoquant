# =============================================================================
# CandidateStore — top-K pattern candidate cache
# =============================================================================
# Purpose:
#  - Stores top-K PatternCandidate objects per (start_idx, end_idx, direction, degree)
#  - Avoids re-evaluating the same interval multiple times
#  - Enables top-K ranking across all intervals
# =============================================================================

from __future__ import annotations

from modeling.elliott.elliott.data import PatternCandidate


class CandidateStore:
    # ==========================================================================
    # CandidateStore(top_k) -> None
    # ==========================================================================
    # Purpose:
    #  - Initialize with top-K limit per interval key
    # ==========================================================================
    def __init__(self, top_k: int = 5) -> None:
        self.top_k = top_k
        self._store: dict[tuple, list[PatternCandidate]] = {}

    def add(self, candidate: PatternCandidate) -> None:
        """Add a candidate; maintain top-K by score for its key."""
        key = (
            candidate.start_idx,
            candidate.end_idx,
            candidate.direction,
            candidate.degree,
        )
        bucket = self._store.setdefault(key, [])
        bucket.append(candidate)
        bucket.sort(key=lambda c: c.score, reverse=True)
        if len(bucket) > self.top_k:
            del bucket[self.top_k:]

    def get(
        self,
        start_idx: int,
        end_idx:   int,
        direction: int,
        degree:    int,
    ) -> list[PatternCandidate]:
        """Return top-K candidates for this interval, or empty list."""
        key = (start_idx, end_idx, direction, degree)
        return list(self._store.get(key, []))

    def all_candidates(self) -> list[PatternCandidate]:
        """Return all stored candidates sorted by score descending."""
        flat = [c for bucket in self._store.values() for c in bucket]
        flat.sort(key=lambda c: c.score, reverse=True)
        return flat

    def top_n(self, n: int) -> list[PatternCandidate]:
        """Return global top-N candidates across all intervals."""
        return self.all_candidates()[:n]

    def clear(self) -> None:
        self._store.clear()
