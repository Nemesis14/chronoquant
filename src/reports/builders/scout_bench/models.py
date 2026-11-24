# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportOptionalMemberAccess=false
"""Model interface + registry of the Impulse Scout benchmark.

How to add a model (t932-t935)::

    from reports.builders import scout_bench as sb

    class MyModel(sb.ScoutModel):
        def fit(self, train, prefix_data, *, seed):          # train: polars frame (label_class, features, row_id ...)
            ...
        def predict_proba(self, df, prefix_data):            # -> (n, 3) in sb.CLASSES order, rows sum to 1
            ...

    sb.REGISTRY.register(sb.ModelSpec(
        name="scout_lgbm_r1", factory=MyModel, family="P1", hypothesis="H1", round=1,   # primary: pre-registered
        config={"n_estimators": 200, ...}))                                             # config is hashed

* ``hypothesis`` in ``H1`` / ``H2`` / ``H3`` marks the pre-registered primary comparison (5300; Holm family). It needs
  ``round == 1`` (epic decision 2026-11-21: round-1 fixed configs only; round-2 winners go through the SPA only) and a
  hypothesis is registered once. Every other model is exploratory.
* ``prefix_data`` is opaque to the harness: any object the model understands, indexed by the frame's ``row_id``
  (e.g. an ``(n_events, n_bars, n_channels)`` array). It is passed unchanged to ``fit`` / ``predict_proba``.
* Determinism: the harness passes ``seed`` to ``fit``; the model must derive all randomness from it.
* The binary impulse score defaults to ``proba[:, 0]``; override :meth:`ScoutModel.score` if needed.
"""

from __future__ import annotations

import hashlib
import json
import re
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import polars as pl

from reports.builders.scout_bench.frame import CLASSES, S0C_COLS, y_binary, y_class

BASELINE_NAME = "S0C_logit"
BASELINE_C = 0.1                     # same as t920 model A (5300: "ugyanaz a regularizáció")
HYPOTHESES = ("H1", "H2", "H3")
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


class ScoutModel(ABC):
    """One model: 3-class probabilities (impulse / false_start / sideways) from the event frame."""

    @abstractmethod
    def fit(self, train: pl.DataFrame, prefix_data: Any, *, seed: int) -> None: ...

    @abstractmethod
    def predict_proba(self, df: pl.DataFrame, prefix_data: Any) -> np.ndarray:
        """``(n, 3)`` probabilities in :data:`CLASSES` order."""

    def score(self, proba: np.ndarray) -> np.ndarray:
        """Binary impulse-vs-rest score (default: P(impulse))."""
        return proba[:, 0]


def three_class_from_binary(p_impulse: np.ndarray, train: pl.DataFrame) -> np.ndarray:
    """Binary P(impulse) -> 3 classes: the rest ``1 - p`` is split with the train-fold conditional share
    ``P(false_start | not impulse)`` (a constant). Keeps the binary score untouched."""
    rest = train.filter(pl.col("label_class") != "impulse")
    q = float((rest["label_class"] == "false_start").mean()) if rest.height else 0.5
    p = np.clip(np.asarray(p_impulse, dtype=float), 0.0, 1.0)
    return np.column_stack([p, (1 - p) * q, (1 - p) * (1 - q)])


class ScalarLogit(ScoutModel):
    """Baseline S0 + C: standardised (fold-train mean / sd) L2 logistic regression, fixed ``C`` (t920 model A)."""

    def __init__(self, c: float = BASELINE_C, cols: tuple[str, ...] = S0C_COLS) -> None:
        self.c, self.cols = c, tuple(cols)

    def _x(self, df: pl.DataFrame) -> np.ndarray:
        return (df.select(list(self.cols)).to_numpy().astype(float) - self.mu) / self.sd

    def fit(self, train: pl.DataFrame, prefix_data: Any, *, seed: int) -> None:
        from sklearn.linear_model import LogisticRegression

        x = train.select(list(self.cols)).to_numpy().astype(float)
        self.mu = x.mean(axis=0)
        sd = x.std(axis=0)
        self.sd = np.where(sd > 1e-12, sd, 1.0)
        self._train = train.select("label_class")
        self.clf = LogisticRegression(C=self.c, max_iter=2000).fit(self._x(train), y_binary(train))

    def predict_proba(self, df: pl.DataFrame, prefix_data: Any) -> np.ndarray:
        p = self.clf.predict_proba(self._x(df))[:, 1]
        return three_class_from_binary(p, self._train)


@dataclass(frozen=True)
class ModelSpec:
    name: str
    factory: Callable[[], ScoutModel]
    family: str
    hypothesis: str | None = None       # "H1" | "H2" | "H3" -> pre-registered primary; None -> exploratory
    round: int = 1                      # iteration number on the leaderboard (1 or 2)
    config: dict[str, Any] = field(default_factory=dict)
    description: str = ""
    is_baseline: bool = False

    @property
    def primary(self) -> bool:
        return self.hypothesis is not None

    @property
    def config_hash(self) -> str:
        blob = json.dumps({"name": self.name, "family": self.family, "round": self.round, "config": self.config},
                          sort_keys=True, default=str)
        return hashlib.sha256(blob.encode()).hexdigest()[:12]


class ModelRegistry:
    """Ordered, validated registry (every tried variant stays: 5300 requires a complete leaderboard)."""

    def __init__(self) -> None:
        self._specs: dict[str, ModelSpec] = {}

    def register(self, spec: ModelSpec) -> ModelSpec:
        if not _NAME_RE.fullmatch(spec.name):
            raise ValueError(f"invalid model name: {spec.name!r}")
        if spec.name in self._specs:
            raise ValueError(f"model already registered: {spec.name}")
        if spec.hypothesis is not None:
            if spec.hypothesis not in HYPOTHESES:
                raise ValueError(f"hypothesis must be one of {HYPOTHESES}")
            if spec.round != 1:
                raise ValueError("primary (H1-H3) tests run on round-1 pre-registered configs only")
            if any(s.hypothesis == spec.hypothesis for s in self._specs.values()):
                raise ValueError(f"hypothesis {spec.hypothesis} already registered")
        if spec.round not in (1, 2):
            raise ValueError("round must be 1 or 2 (5300: no further rounds)")
        self._specs[spec.name] = spec
        return spec

    def get(self, name: str) -> ModelSpec:
        return self._specs[name]

    @property
    def baseline(self) -> ModelSpec:
        return next(s for s in self._specs.values() if s.is_baseline)

    def specs(self, *, primary: bool | None = None, include_baseline: bool = True) -> list[ModelSpec]:
        out = [s for s in self._specs.values() if include_baseline or not s.is_baseline]
        if primary is not None:
            out = [s for s in out if s.primary == primary]
        return out

    def __contains__(self, name: str) -> bool:
        return name in self._specs

    def __len__(self) -> int:
        return len(self._specs)


def baseline_spec() -> ModelSpec:
    return ModelSpec(BASELINE_NAME, ScalarLogit, family="B0", round=1,
                     config={"c": BASELINE_C, "cols": list(S0C_COLS)}, is_baseline=True,
                     description="skalár logit S0 + C (t920 Modell A), C = 0,1, pooled")


def new_registry() -> ModelRegistry:
    """A registry holding only the baseline (tests, ad-hoc runs)."""
    r = ModelRegistry()
    r.register(baseline_spec())
    return r


#: the default module-level registry: t932-t935 register their round-1 / exploratory models here
REGISTRY = new_registry()


def y_for(df: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """(binary impulse, class index) labels of a frame."""
    return y_binary(df), y_class(df)


__all__ = ["BASELINE_C", "BASELINE_NAME", "CLASSES", "HYPOTHESES", "REGISTRY", "ModelRegistry", "ModelSpec", "ScalarLogit",
           "ScoutModel", "baseline_spec", "new_registry", "three_class_from_binary", "y_for"]
