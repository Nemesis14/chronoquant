# =============================================================================
# Multi-degree pivot builder
# =============================================================================
# Purpose:
#  - Build degree_0, degree_1, degree_2 pivot lists from the same OHLCV
#  - Each higher degree uses 2x the ZigZag threshold of the previous
#  - Returns dict {degree: list[Pivot]}
# Parameters:
#  - df: OHLCV DataFrame
#  - cfg: ElliottConfig (base: degree_0 uses cfg.zigzag_threshold)
#  - degrees: number of degrees to build (default 3)
# =============================================================================

from __future__ import annotations

import dataclasses

import pandas as pd

from elliott_waves.elliott.config import ElliottConfig
from elliott_waves.elliott.data import Pivot
from elliott_waves.elliott.pivots.zigzag import detect_zigzag


def build_multi_degree(
    df:      pd.DataFrame,
    cfg:     ElliottConfig,
    degrees: int = 3,
) -> dict[int, list[Pivot]]:
    """Build pivot lists for each degree level."""
    result: dict[int, list[Pivot]] = {}
    for d in range(degrees):
        scaled_threshold = cfg.zigzag_threshold * (2 ** d)
        d_cfg = dataclasses.replace(cfg, zigzag_threshold=scaled_threshold)
        result[d] = detect_zigzag(df, d_cfg, degree=d)
    return result
