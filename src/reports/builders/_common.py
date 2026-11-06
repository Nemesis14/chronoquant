"""Shared helpers of the model-report builders (paths, direction, DB access, figure helpers)."""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import plotly.graph_objects as go

import utils
from reports import theme

logger = logging.getLogger(__name__)


def direction_of(model_id: str) -> str:
    """``"long"`` / ``"short"`` from the model id (``..._l_...`` / ``..._s_...``)."""
    return "long" if "_l_" in model_id else "short"


def target_of(direction: str) -> str:
    return f"{direction}_mfe_fw60"


def artifact_dir_of(model_id: str) -> Path:
    """Artifact directory of a model from ``config/models.json``."""
    meta = utils.load_models_config()["models"][model_id]
    return Path(utils._resolve_path(meta["artifact_dir"]))  # noqa: SLF001


def git_sha() -> str | None:
    """Short git sha of the working tree (``None`` outside git)."""
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, cwd=utils.repo_root(), check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() or None


def source_info(step: str, generator: str) -> dict[str, Any]:
    """``manifest.source`` block of a pipeline-step report."""
    info: dict[str, Any] = {"kind": "pipeline_step", "step": step, "generator": generator}
    sha = git_sha()
    if sha:
        info["git_sha"] = sha
    return info


def read_live(sql: str, params: list[Any] | None = None) -> pd.DataFrame | None:
    """Run a query on the live DB read-only; ``None`` (and a log line) when it is locked/missing."""
    path = utils.load_asset_config()["database"]["db_path"]
    try:
        con = duckdb.connect(str(path), read_only=True)
    except duckdb.Error as exc:
        logger.warning("live db unavailable (%s): %s", path, exc)
        return None
    try:
        return con.execute(sql, params or []).fetchdf()
    finally:
        con.close()


def themed(fig: go.Figure, *, height: int = 420, **layout: Any) -> go.Figure:
    """Apply the neutral layout defaults of the report theme."""
    fig.update_layout(**theme.base_layout(**layout), height=height)
    return fig


def vrect(fig: go.Figure, x0: Any, x1: Any, color: str = theme.ORANGE, **kw: Any) -> None:
    """Shaded x-range (valid period)."""
    fig.add_vrect(x0=x0, x1=x1, fillcolor=color, opacity=0.08, line_width=0, **kw)


def fmt(value: Any, digits: int = 4) -> str:
    """Compact number formatting for markdown text (``-`` for missing)."""
    if value is None:
        return "-"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return "-" if not np.isfinite(number) else f"{number:.{digits}f}"
