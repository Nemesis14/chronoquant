"""Cluster regime research module (Epic 065).

Sandbox research code exploring K-means regime clustering for SOLUSDT.
Nothing here feeds the live trading pipeline (see project conventions:
`research/` is explicitly isolated from `src/`).

t872 (third iteration): CV-optimized cluster separation via walk-forward
ANOVA F-score search over k=2..30. Rebuilt from scratch after the fixed-k=10
approach (t863-t871) was shown to have no exploitable trading edge under
realistic execution, regardless of classification methodology or TP/SL
calibration (see `_doc_/models_doc/6501_k10_validation_realistic_backtest.md`).
"""
