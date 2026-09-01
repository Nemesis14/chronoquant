---
epic: epic_065
id: t871
title: K=10 clustering full training (2021-2025-06) + OOS validation (2025-07—2026-06)
assignee: validator_agent
status: pr
blocks: []
blocked_by: [t870]
---

## Deliverables Completed (DEFINITIVE — Corrected Classification)

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

### 2. Corrected Cluster Direction Classification (Paired T-Test + Bonferroni)

**Critical bug found and fixed:** The initial classification approach used an
independent-sample comparison (`t_stat = (long_mfe_mean - short_mfe_mean) / SE`),
which is mathematically invalid here — since `long_mfe > 0` and `short_mfe < 0`,
the subtraction always *adds* their magnitudes, producing a spuriously large
positive `t_stat` for every cluster (all 10 clusters incorrectly classified as LONG).

**Fix:** Paired t-test on the per-observation difference, since `long_mfe` and
`short_mfe` come from the same observation window (correlated, not independent):
```
diff_i = long_mfe_i + short_mfe_i    (= |long_mfe_i| - |short_mfe_i|, since short_mfe_i < 0)
t_stat  = mean(diff) / (std(diff) / sqrt(n))
p_value = 2 * (1 - t.cdf(|t_stat|, df=n-1))
```
Bonferroni correction: alpha = 0.05 / 10 clusters = 0.005.

**Corrected classification (training period 2021-01 — 2025-06-30):**

| Cluster | n_obs | t_stat | p_value | Assignment |
|---------|-------|--------|---------|------------|
| 0 | 11,029 | -1.121 | 0.2622 | NEUTRAL |
| 1 | 16,597 | -4.309 | 0.0000165 | SHORT |
| 2 | 4,292 | -0.011 | 0.9915 | NEUTRAL |
| 3 | 9,409 | +2.966 | 0.0030 | LONG |
| 4 | 5,862 | -0.696 | 0.4865 | NEUTRAL |
| 5 | 10,441 | -5.987 | <0.0001 | SHORT |
| 6 | 4,200 | -8.505 | <0.0001 | SHORT |
| 7 | 3,599 | -5.662 | <0.0001 | SHORT |
| 8 | 17,083 | -4.525 | 0.0000061 | SHORT |
| 9 | 1,475 | +2.906 | 0.0037 | LONG |

**Final tradeable set:** LONG = {3, 9}, SHORT = {1, 5, 6, 7, 8}; NEUTRAL/excluded = {0, 2, 4}

Output: `artifacts/research_cluster_regime/paired_ttest_cluster_classification.json`

### 3. Definitive OOS Backtesting (2025-07-01 — 2026-06-30)

**Trading Rules:**
- LONG (clusters 3, 9): entry close, TP +2%, SL -1%
- SHORT (clusters 1, 5, 6, 7, 8): entry close, TP -2% (down), SL +1% (up)
- Timeout: 24h hold at MFE
- Realistic costs: 0.3% per trade + 0.4% slippage round-trip (~0.7% total)
- Clusters 9 (OOS n=0) and 7 (OOS n=2) kept per explicit user instruction, with documented small-sample caveats

**Definitive Backtest Results:**
- Total trades: 863
- Win count: 318
- Win rate: 36.85%
- Avg P&L/trade: -$3.55
- Net P&L: -$3,060.13
- Gross P&L (before costs): -$471.13
- Max drawdown: -$3,047.11
- Sharpe ratio: -4.541

**Per-cluster breakdown:**

| Cluster | Direction | Trades | Win Rate | Net P&L | Note |
|---------|-----------|--------|----------|---------|------|
| 1 | SHORT | 288 | 36.46% | -$1,058.21 | |
| 3 | LONG | 172 | 37.21% | -$574.47 | |
| 5 | SHORT | 115 | 36.52% | -$411.81 | |
| 6 | SHORT | 101 | 36.63% | -$369.77 | |
| 7 | SHORT | 2 | 0.00% | -$26.04 | Rare event, n=2, wide uncertainty |
| 8 | SHORT | 185 | 37.84% | -$619.84 | |
| 9 | LONG | 0 | N/A | $0.00 | Disappeared OOS (train n=1,475) |

**Per-direction breakdown:**

| Direction | Trades | Win Rate | Net P&L |
|-----------|--------|----------|---------|
| LONG | 172 | 37.21% | -$574.47 |
| SHORT | 691 | 36.76% | -$2,485.67 |

**Output Files:**
- [x] `k10_final_corrected_backtest_2025_07_2026_06.csv` (863-row trade log)
- [x] `k10_final_corrected_backtest_summary.json` (overall + per-cluster + per-direction metrics)
- [x] `kmeans_k10_trained_2021_2025_06.pkl` (trained model)

### 4. Full Version History (Audit Trail)

| Version | Clusters Traded | Classification | Trades | Win Rate | Net P&L |
|---------|------------------|-----------------|--------|----------|---------|
| t870 (idealized) | 2 LONG, 1 SHORT (k=3) | Independent-sample | ~2,235 | 98.3% | +$18.5k-24.8k (idealized) |
| t871 v1 (bug: LONG-only) | All 10 | N/A | 1,262 | 36.45% | -$4,646 |
| t871 v2 (all 10, LONG+SHORT) | All 10 | Ad hoc heuristic | 1,301 | 36.59% | -$4,698.83 |
| t871 v3 ("4 tradeable") | 0, 2, 3, 9 | **Invalid** independent-sample t-test | 502 | 38.05% | -$1,659.11 |
| **t871 DEFINITIVE** | **1, 3, 5, 6, 7, 8, 9** | **Correct paired t-test, Bonferroni** | **863** | **36.85%** | **-$3,060.13** |

Superseded (but retained for audit trail) outputs:
- `apriori_cluster_classification.json` — invalid independent-sample t-test (do not use)
- `k10_oos_backtest_2025_07_2026_06_realistic.csv` / `_summary.json` — v1/v2 runs

### 5. Analysis & Documentation

**Report:** `_doc_/models_doc/6501_k10_validation_realistic_backtest.md` (rewritten as DEFINITIVE version)

Sections: Corrected Classification Methodology, Definitive Setup, Definitive Results,
Full History Comparison, Key Findings, Phase 3 Decision & Recommendation.

**Key Findings:**
1. **Classification fix changed which clusters trade, not the outcome.** Win rate stays in the 36-38% band across all 5 backtest configurations tried (idealized excluded).
2. **No directional asymmetry.** LONG (37.21% WR) and SHORT (36.76% WR) perform statistically identically — ruling out "wrong direction" as the cause of poor realistic performance.
3. **30-point gap to breakeven.** Required win rate ~67% (given 0.7%/trade cost structure) vs. achieved 36.85%.
4. **Signal instability confirmed independently.** Companion analysis (`cluster_profile_train_vs_oos_all_clusters.json`) shows 35-100% MFE degradation train→OOS across all clusters, including complete disappearance of cluster 9.

### 6. Code Quality

**Ruff Check:** All checks passed on `research/cluster_regime/05_k10_training_oos_backtest.py` and `08_paired_ttest_classification.py`

**Implementation Files:**
- `research/cluster_regime/05_k10_training_oos_backtest.py` — corrected `TRADEABLE_SETUPS` (7-cluster), simplified `_simulate_trades` (removed dead `cluster_directions` param and now-unused `_profile_clusters_for_direction`), per-cluster/per-direction breakdown added to summary output
- `research/cluster_regime/08_paired_ttest_classification.py` — new: correct paired t-test classification methodology
- `research/cluster_regime/06_cluster_profile_comparison.py` — new: train vs OOS cluster profile comparison (all 10 clusters)
- `research/cluster_regime/07_apriori_cluster_classification.py` — retained for audit trail (documents the invalid independent-sample approach that was superseded)

---

## Validation Checklist

For validator_agent:

- [ ] Verify ruff compliance: `python -m ruff check research/cluster_regime/`
- [ ] Verify DEFINITIVE backtest outputs exist and correct:
  - [ ] `k10_final_corrected_backtest_2025_07_2026_06.csv` (863 rows)
  - [ ] `k10_final_corrected_backtest_summary.json` (863 trades, 36.85% win rate, per-cluster + per-direction breakdown present)
  - [ ] `kmeans_k10_trained_2021_2025_06.pkl` (saved model)
- [ ] Verify classification outputs:
  - [ ] `paired_ttest_cluster_classification.json` (correct methodology, 7 significant clusters)
  - [ ] `cluster_profile_train_vs_oos_all_clusters.json` (train vs OOS, all 10 clusters)
- [ ] Verify analysis document is the DEFINITIVE rewrite:
  - [ ] `_doc_/models_doc/6501_k10_validation_realistic_backtest.md`
  - [ ] Contains "Corrected Classification Methodology" section
  - [ ] Contains full version-history comparison table (t870 → t871 v1/v2/v3 → definitive)
- [ ] Verify trade log format:
  - [ ] Columns: entry_time, exit_time, cluster, direction, entry_price, exit_price, mfe, realized_pnl, pnl_pct, win
  - [ ] Contains clusters {1,3,5,6,7,8,9} only (no 0, 2, 4)
- [ ] No uncommitted changes in `research/cluster_regime/`

---

## Notes

### Decision Gate: Phase 3 Validation COMPLETE ✅ (Definitive)

Five independent backtest configurations (1 idealized, 4 realistic) all converge on the same
conclusion: **no exploitable trading edge exists for K-means cluster regime detection under
realistic execution conditions**, regardless of K (3 or 10) or cluster classification methodology
(even after fixing the statistical bug in direction assignment).

### Recommendation: **Do NOT Proceed**

Even the statistically correct 7-cluster tradeable set achieves only 36.85% win rate against a
required ~67% breakeven threshold — a 30-point gap that cluster-selection correctness alone cannot
close. LONG and SHORT directions perform identically, ruling out misclassification as an explanation.

**Epic 065 Phase 3 recommendation:** Close the clustering research track; return to LightGBM-based
directional modeling (`src/modeling/`) with explicit transaction-cost optimization in strategy
calibration (`src/strategy/`).

---

## References

- **t863:** Feature redundancy selection (MI-based)
- **t865:** Window representation (360-bar rolling, z-score normalization)
- **t866:** K-means clustering (k=3..8 elbow; t870 found k=3 optimal, t871 explores k=10)
- **t867:** OOS validation (original Bonferroni-corrected t-tests, k=3 model)
- **t870:** K=3 idealized backtest (98.3% win rate, predecessor decision baseline)
- **t871:** K=10 realistic backtest (THIS DOCUMENT) — DEFINITIVE result after classification bug fix, final verdict: NO EDGE
