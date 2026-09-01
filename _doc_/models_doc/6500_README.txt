Cluster Regime Strategy - OOS Validation Analysis
==================================================

FILES:
  - 6500_cluster_regime_analysis.ipynb (26 cells, Quarto-compatible)
  - 6500_cluster_regime_analysis.html (rendered HTML version)
  - 6500_README.txt (this file)

CONTENT:
The notebook provides a decision-oriented analysis of OOS validation results
from t867, focusing on Cluster 1 SHORT edge tradability assessment.

KEY SECTIONS:
  01. Data loading and summary
  02. Train period cluster profiles (2021)
  03. OOS validation results (2023-2025)
  04. Train vs OOS performance decay
  05. Year-by-year stability analysis
  06. Cluster size stability
  07. Edge significance and H0 survival probability
  08. Final decision framework (4 criteria)
  09. Visualizations (3 figures)
  10. Summary and recommendations

KEY FINDINGS:
  - Cluster 1 SHORT edge: SIGNIFICANT (p=0.0017, Bonferroni-corrected)
  - Mean OOS SHORT MFE: -0.022 (2.2 bp)
  - Effective N: 382 (Newey-West adjusted)
  - Year-by-year: 2/3 years significant (2025 strongest)
  - Cluster size stability: CV=0.30 (acceptable)
  - Sharpe decay: ~57% (expected, not suspicious)

DECISION: TRADABLE EDGE DETECTED (Conditional)
RECOMMENDATION: PROCEED to Phase 3 with caution
  - Start with reduced position size
  - Monitor quarterly for regime shifts
  - Target transaction cost: < 1 bp

Generated: 2026-08-31
Data source: t867 OOS validation run
