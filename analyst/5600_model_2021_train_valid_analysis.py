"""Compatibility wrapper for the combined 5600 analysis helpers."""

from __future__ import annotations

from analyst.model_5600_train_valid_analysis import *  # noqa: F401,F403
from analyst.model_5600_train_valid_analysis import main


if __name__ == "__main__":
    raise SystemExit(main())
