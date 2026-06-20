"""ChronoQuant strategy module.

CLI entry points:
    00_build_strategy_table.py  — Build strategy table parquet from model predictions.
    01_calibrate_scores.py      — Fit and apply isotonic regression calibration.
    02_optimize_strategy.py     — Optuna sweep over entry/exit/cooldown parameters.

Library:
    strategy/  — Core logic modules (build_table, calibrate, optimize, artifacts).
"""
