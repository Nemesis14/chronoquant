---
epic: epic_065
id: t871
title: K=10 clustering full training (2021-2025-06) + OOS validation (2025-07—2026-06)
assignee: validator_agent
status: pr
blocks: []
blocked_by: [t870]
---

## Deliverables Completed

### 1. K-means Training (2021-01 — 2025-06-30)

- Feature selection: MI-based (14 features from t863)
- Window representation: 360-bar rolling, z-score norm (t865)
- K-means clustering: k=10, random_state=42, deterministic
- Training data: 78,769 observations
- Model artifact: `artifacts/research_cluster_regime/kmeans_k10_trained_2021_2025_06.pkl`

**Acceptance Criteria:**
- [x] K=10 model trained on full 2021-2025-06 data
- [x] Feature metadata documented (14 MI-selected features)
- [x] Training completed without errors
- [x] Model serialized and saved

### 2. OOS Backtesting (2025-07-01 — 2026-06-30)

- Cluster predictions: OOS period has 17,472 window observations
- Trade simulation: $1,000 per cluster signal
- Entry/Exit rules:
  - Entry: new cluster ID → $1,000 position
  - TP: +2% return
  - SL: -1% loss
  - Timeout: 24h hold at MFE
- Realistic costs: 0.3% per trade (spread 0.05%×2 + commission 0.1%)
- Realistic slippage: 0.2% avg entry + 0.2% avg exit (0.4% round-trip)

**Backtest Results:**
- Total trades: 1,262 over 1 year
- Win count: 460
- Win rate: 36.45%
- Avg P&L/trade: -$3.68
- Net P&L: -$4,646
- Gross P&L (before costs): -$860
- Max drawdown: -$4,646
- Sharpe ratio: -4.75

**Output Files:**
- [x] `k10_oos_backtest_2025_07_2026_06_realistic.csv` (1,262 trade log)
- [x] `k10_oos_backtest_2025_07_2026_06_summary.json` (summary metrics)

### 3. Analysis & Documentation

**Report:** `_doc_/models_doc/6501_k10_validation_realistic_backtest.md`

Key findings:
- **No edge under realistic conditions:** 36.45% win rate (cf. 98.3% idealized from t870)
- **Cost structure is prohibitive:** $8,834 annual costs (~0.7% of capital)
- **Market microstructure dominates:** Win payoff ($13) ≈ loss payoff ($13)
- **Signal does not persist OOS:** K-means patterns from 2021-06 training degraded under 2025-07 — 2026-06 market conditions
- **Recommendation:** Do NOT proceed with clustering strategy; explore LightGBM alternatives or volatility regimes

### 4. Code Quality

**Ruff Check:**
```bash
python -m ruff check research/cluster_regime/05_k10_training_oos_backtest.py
All checks passed!
```

**Implementation File:** `research/cluster_regime/05_k10_training_oos_backtest.py`
- Loads window representation (70-dim features)
- Trains K-means (k=10)
- Predicts OOS clusters
- Simulates trades with realistic market conditions
- Outputs CSV trade log + JSON summary
- Fully documented with docstrings

---

## Validation Checklist

For validator_agent:

- [ ] Verify ruff compliance: `python -m ruff check research/cluster_regime/`
- [ ] Verify backtest outputs exist:
  - [ ] `k10_oos_backtest_2025_07_2026_06_realistic.csv`
  - [ ] `k10_oos_backtest_2025_07_2026_06_summary.json`
  - [ ] `kmeans_k10_trained_2021_2025_06.pkl`
- [ ] Verify analysis document:
  - [ ] `_doc_/models_doc/6501_k10_validation_realistic_backtest.md`
  - [ ] Sections: Setup, Results, Findings, Decision
- [ ] Verify trade log format:
  - [ ] Columns: entry_time, exit_time, cluster, direction, entry_price, exit_price, mfe, realized_pnl, pnl_pct, win
  - [ ] 1,262 rows (as per summary)
- [ ] Verify summary JSON:
  - [ ] Contains backtest_results key with all metrics
  - [ ] Consistent with trade log (1,262 trades, 460 wins)
- [ ] No uncommitted changes in `research/cluster_regime/`

---

## Notes

### Decision Gate: Phase 3 Validation Complete

This validation demonstrates that **cluster regime strategy lacks a real trading edge** under realistic market conditions:

1. **Idealized (t870): 98.3% win rate** — based on intrabar TP/SL without costs
2. **Realistic (t871): 36.45% win rate** — accounting for execution costs + slippage

The 62% drop in win rate is primarily due to:
- Fixed transaction costs (0.3% per trade)
- Market impact slippage (0.4% round-trip)
- Signal decay in OOS period (training: 2021-06, testing: 2025-07—2026-06)

**Recommendation:** Clustering strategy is **NOT PRODUCTION READY**. Epic 065 should close with this validation, and the team should return to LightGBM-based directional models with focus on cost optimization.

---

## References

- **t870:** K=3 idealized backtest (predecessor, decision baseline)
- **t863:** Feature redundancy analysis (MI-based feature selection)
- **t865:** Window representation (360-bar rolling, z-score normalization)
- **t866:** K-means clustering and profiling
- **t867:** OOS validation with Bonferroni correction
- **Epic 065:** Cluster Regime Strategy (full research arc)
