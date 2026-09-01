# K=10 Clustering Validation: Realistic Backtest Results (DEFINITIVE — Corrected Classification + Calibrated TP)

**Task:** t871 — K=10 clustering full training (2021-06) + OOS validation (2025-07—2026-06)

**Date:** 2026-09-01

**STATUS: DEFINITIVE.** This document supersedes all prior versions of this analysis. It corrects a mathematical error in cluster direction classification discovered during review, tests whether cluster-specific TP/SL calibration recovers a profitable edge, and reports the final, statistically valid backtest for the Epic 065 Phase 3 decision.

**Executive Summary**

This document validates the K=10 clustering regime strategy under realistic market conditions using a corrected, statistically rigorous cluster classification (paired t-test with Bonferroni correction). The corrected-classification result: **36.85% win rate and -$3,060.13 net P&L** over the 1-year OOS period (2025-07-01 — 2026-06-30), trading the 7 clusters that show a statistically significant directional bias in the training period. A follow-up test then asked whether the fixed +2%/-1% TP/SL — mismatched to each cluster's actual MFE magnitude — was suppressing a real signal; calibrating TP/SL per cluster from training-period MFE **made results worse, not better** (35.58% win rate, -$4,231.64 net P&L). **No real trading edge exists at any TP/SL scale tested** — this holds across every version of the analysis: idealized or realistic, correctly or incorrectly classified, fixed or calibrated targets.

---

## Section 1: Corrected Classification Methodology

### The Bug That Was Found

All prior classification attempts in this epic (t870's original k=3 profiling, and t871's first two classification passes) used an **independent-sample comparison** to decide cluster direction:

```
t_stat = (mean(long_mfe) - mean(short_mfe)) / SE
```

This is mathematically broken for this data. `long_mfe` (typically positive, e.g. +0.02) and `short_mfe` (typically negative, e.g. -0.02) are subtracted, which **always adds their magnitudes** (`0.02 − (−0.02) = 0.04`). The result is a large positive `t_stat` for essentially every cluster — which is exactly what happened when this formula was applied to all 10 K=10 clusters: **all 10 came back "LONG"** with p<0.0001, an obviously invalid result.

### The Correct Test: Paired T-Test

`long_mfe` and `short_mfe` are computed from the **same observation window** (same 360-bar lookback, same timestamp) — they are paired, correlated measurements, not independent samples. The valid test is a **paired t-test** on the per-observation difference:

```
diff_i = long_mfe_i + short_mfe_i        (since short_mfe_i < 0, this equals |long_mfe_i| - |short_mfe_i|)

H0: E[diff] = 0   (no directional bias — long and short MFE are symmetric)
H1: E[diff] != 0

t_stat  = mean(diff) / (std(diff) / sqrt(n))
p_value = 2 * (1 - t.cdf(|t_stat|, df=n-1))
```

**Bonferroni correction:** `alpha = 0.05 / 10 clusters = 0.005` (two-tailed), `t_critical ≈ 2.807` for large df.

**Assignment rule:**
```
IF t_stat > t_critical AND mean(diff) > 0  -> LONG   (|long_mfe| significantly exceeds |short_mfe|)
IF t_stat < -t_critical AND mean(diff) < 0 -> SHORT  (|short_mfe| significantly exceeds |long_mfe|)
ELSE                                        -> NEUTRAL
```

### Corrected Classification Results (Training Period 2021-01-01 — 2025-06-30)

| Cluster | n_obs | long_mfe_mean | short_mfe_mean | diff_mean | t_stat | p_value | Assignment |
|---------|-------|---------------|-----------------|-----------|--------|---------|------------|
| 0 | 11,029 | +0.020170 | -0.020534 | -0.000364 | -1.121 | 0.2622 | **NEUTRAL** |
| 1 | 16,597 | +0.022341 | -0.023647 | -0.001306 | -4.309 | 0.0000165 | **SHORT** |
| 2 | 4,292 | +0.024026 | -0.024033 | -0.000007 | -0.011 | 0.9915 | **NEUTRAL** |
| 3 | 9,409 | +0.022616 | -0.021403 | +0.001213 | +2.966 | 0.0030 | **LONG** |
| 4 | 5,862 | +0.026589 | -0.026989 | -0.000399 | -0.696 | 0.4865 | **NEUTRAL** |
| 5 | 10,441 | +0.019384 | -0.021544 | -0.002160 | -5.987 | <0.0001 | **SHORT** |
| 6 | 4,200 | +0.023775 | -0.030898 | -0.007123 | -8.505 | <0.0001 | **SHORT** |
| 7 | 3,599 | +0.024004 | -0.029106 | -0.005103 | -5.662 | <0.0001 | **SHORT** |
| 8 | 17,083 | +0.022318 | -0.023762 | -0.001444 | -4.525 | 0.0000061 | **SHORT** |
| 9 | 1,475 | +0.029873 | -0.025960 | +0.003913 | +2.906 | 0.0037 | **LONG** |

**Final tradeable set (7 of 10 clusters significant):**
- **LONG:** clusters 3, 9
- **SHORT:** clusters 1, 5, 6, 7, 8
- **EXCLUDED (NEUTRAL, no significant directional bias):** clusters 0, 2, 4

Note that **cluster 3 flips direction** relative to every prior version of this analysis (previously traded as SHORT; correctly LONG), and **clusters 0 and 2** — previously traded as LONG and SHORT respectively — have **no statistical basis for trading at all**.

---

## Section 2: Definitive Backtest Setup

### K=10 Clustering Model

- **Training Period:** 2021-01-01 to 2025-06-30 (78,769 observations)
- **OOS Period:** 2025-07-01 to 2026-06-30 (1-year validation)
- **Feature Space:** 70-dimensional window representation (14 MI-selected features × 5 summary stats)
- **Algorithm:** K-means with k=10, random_state=42, deterministic
- **Artifact:** `artifacts/research_cluster_regime/kmeans_k10_trained_2021_2025_06.pkl`

### Realistic Trading Rules

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Position Size | $1,000 per signal | Fixed capital allocation |
| LONG Entry/Exit (clusters 3, 9) | close[t], TP +2%, SL -1% | Standard directional signal |
| SHORT Entry/Exit (clusters 1,5,6,7,8) | close[t], TP -2% (down), SL +1% (up) | Symmetric inverse logic |
| Timeout | 24 hours (hold at MFE) | Empirical max hold before signal decay |
| Total Costs | 0.3% per trade | Spread 0.05%×2 + Commission 0.1% (realistic Binance futures) |
| Entry Slippage | 0.2% average | Market impact from $1k position |
| Exit Slippage | 0.2% average | Execution friction on close |

Per explicit user instruction, clusters 9 (OOS n=0) and 7 (OOS n=2) were **kept** in the tradeable set despite very low/zero OOS sample size — they carry documented small-sample caveats rather than being silently dropped.

---

## Section 3: Definitive Backtest Results

### Overall Performance

| Metric | Value |
|--------|-------|
| **Total Trades** | 863 |
| **Win Count** | 318 |
| **Win Rate** | 36.85% |
| **Avg P&L/Trade** | -$3.55 |
| **Net P&L** | -$3,060.13 |
| **Gross P&L** (before costs) | -$471.13 |
| **Max Drawdown** | -$3,047.11 |
| **Sharpe Ratio** | -4.541 |

### Per-Cluster Breakdown

| Cluster | Direction | Trades | Win Rate | Net P&L | Note |
|---------|-----------|--------|----------|---------|------|
| 1 | SHORT | 288 | 36.46% | -$1,058.21 | |
| 3 | LONG | 172 | 37.21% | -$574.47 | |
| 5 | SHORT | 115 | 36.52% | -$411.81 | |
| 6 | SHORT | 101 | 36.63% | -$369.77 | |
| 7 | SHORT | 2 | 0.00% | -$26.04 | **Rare-event cluster** — only 2 OOS signals; result has wide statistical uncertainty, not representative of the training-period edge magnitude |
| 8 | SHORT | 185 | 37.84% | -$619.84 | |
| 9 | LONG | 0 | N/A | $0.00 | **Cluster disappeared OOS** — train n=1,475 but zero observations assigned to this cluster in 2025-07—2026-06; kept in tradeable set per training significance, but produced no trades |

### Per-Direction Breakdown

| Direction | Trades | Win Rate | Net P&L |
|-----------|--------|----------|---------|
| LONG | 172 | 37.21% | -$574.47 |
| SHORT | 691 | 36.76% | -$2,485.67 |

**Observation:** LONG and SHORT perform almost identically (win rate within 0.5pp of each other), both far below the ~67% breakeven threshold implied by the 0.7%/trade cost structure. Direction is not the differentiator — the underlying signal is simply too weak in both directions.

---

## Section 4: Full History Comparison (t870 → t871 v1/v2 → Definitive)

| Version | Method | Clusters Traded | Classification Basis | Trades | Win Rate | Net P&L |
|---------|--------|------------------|----------------------|--------|----------|---------|
| **t870** | K=3, idealized MFE (no costs/slippage) | 2 LONG, 1 SHORT (k=3 model) | Independent-sample profiling | ~2,235 | 98.3% | +$18.5k–24.8k (idealized) |
| **t871 v1** | K=10, realistic, LONG-only bug | All 10 (LONG-only, no SHORT logic) | N/A (bug) | 1,262 | 36.45% | -$4,646 |
| **t871 v2** | K=10, realistic, all clusters LONG+SHORT | All 10 | Ad hoc (long_mean vs -short_mean heuristic) | 1,301 | 36.59% | -$4,698.83 |
| **t871 v3** | K=10, realistic, "4 tradeable" | 0, 2, 3, 9 | **Invalid independent-sample t-test** (always → LONG; direction assumed via heuristic) | 502 | 38.05% | -$1,659.11 |
| **t871 DEFINITIVE** | K=10, realistic, 7 tradeable clusters | 1, 3, 5, 6, 7, 8, 9 | **Correct paired t-test**, Bonferroni α=0.005 | 863 | 36.85% | -$3,060.13 |

**Key correction vs. v3:** v3 traded cluster 3 as SHORT and clusters 0/2 as LONG/SHORT respectively, none of which had valid statistical support. The definitive run correctly identifies cluster 3 as LONG and excludes clusters 0, 2, 4 entirely, while adding clusters 1, 5, 6, 7, 8 (correctly SHORT) that v3 had wrongly excluded as "non-significant" (they were significant — just under the broken test they didn't reach the always-LONG threshold in the SHORT direction).

**Every version reaches the same qualitative conclusion:** win rate stays in the 36–38% band regardless of which cluster subset or classification method is used, and net P&L is negative in every realistic (non-idealized) run. The classification bug changed *which* clusters were traded and the exact P&L figure, but did **not** change the fundamental verdict.

---

## Section 5: Key Findings

### 1. Classification Bug Did Not Change the Outcome, Only the Path

Despite trading a different (and now statistically correct) set of clusters, the definitive win rate (36.85%) is nearly identical to every prior realistic run (36.45%, 36.59%, 38.05%). This convergence across differently-constructed cluster subsets is itself evidence that **the signal is not concentrated in any particular cluster subset** — it is uniformly weak (or absent) across the K=10 partition.

### 2. No Directional Edge, Corrected or Not

LONG (37.21% WR) and SHORT (36.76% WR) perform statistically indistinguishably in the definitive run. This rules out the hypothesis that the earlier misclassification (trading cluster 3 as SHORT instead of LONG) was responsible for the poor realistic performance — even trading the *correct* direction for every cluster still yields a losing strategy.

### 3. Market Microstructure Remains the Dominant Cost

Per-trade costs (spread 0.1% + commission 0.1% + slippage 0.4% = ~0.7% round-trip) require a >67% win rate to break even given the 2%/1% TP/SL structure. The definitive backtest achieves 36.85% — a **30-point gap** that no amount of cluster-selection correctness closes.

### 4. Small-Sample Clusters Confirm Fragility, Not Opportunity

- Cluster 9 (LONG, train n=1,475, one of only two LONG-significant clusters) produced **zero OOS trades** — the training-period pattern did not recur at all in the OOS window.
- Cluster 7 (SHORT, train n=3,599) produced only **2 OOS trades**, both losses. Per user instruction this was kept rather than dropped, but the result should not be read as evidence for or against the cluster's edge — n=2 carries no statistical weight.

Both cases illustrate the same underlying problem: statistically significant *training-period* patterns did not persist with any reliability into the OOS period, whether measured by MFE degradation (documented separately in `cluster_profile_train_vs_oos_all_clusters.json`) or by trade frequency collapse.

---

## Section 6: Calibrated TP/SL Test — Does Target Mismatch Explain the Loss?

### Motivation

The fixed TP/SL (+2%/-1%) used in every prior backtest was **not calibrated** to each cluster's actual MFE magnitude. For example, cluster 1's OOS short MFE mean is only -1.50% (per `cluster_profile_train_vs_oos_all_clusters.json`), well short of the fixed -2% TP — meaning the take-profit target was structurally close to unreachable for that cluster's typical excursion, forcing most trades toward SL or timeout regardless of underlying signal quality. This raised a legitimate question: is the strategy's poor performance an artifact of badly-sized targets, or a genuine absence of edge?

### Calibration Method

```
TP_cluster[i] = |train_mfe_mean[i]| x 0.7    (70% safety margin below the training-period average excursion)
SL_cluster[i] = TP_cluster[i] x 0.5           (fixed 2:1 risk/reward ratio, same as the fixed-TP backtest)
```

| Cluster | Direction | Train MFE Mean | Calibrated TP | Calibrated SL |
|---------|-----------|-----------------|----------------|----------------|
| 3 | LONG | +2.227% | 1.559% | 0.779% |
| 9 | LONG | +2.987% | 2.091% | 1.046% |
| 1 | SHORT | -2.342% | 1.640% | 0.820% |
| 5 | SHORT | -2.042% | 1.429% | 0.715% |
| 6 | SHORT | -3.095% | 2.167% | 1.083% |
| 7 | SHORT | -2.911% | 2.037% | 1.019% |
| 8 | SHORT | -2.293% | 1.605% | 0.803% |

All other parameters (24h timeout, 0.3% costs, 0.4% round-trip slippage, $1,000 position size) unchanged from the definitive fixed-TP backtest.

### Result: Calibration Made Performance WORSE

| Metric | Fixed TP (+2%/-1%) | Calibrated TP (per-cluster) | Delta |
|--------|---------------------|-------------------------------|-------|
| Total Trades | 863 | 1,082 | +219 (more entries reach the smaller TP) |
| Win Rate | 36.85% | **35.58%** | -1.27 pp |
| Net P&L | -$3,060.13 | **-$4,231.64** | -$1,171.51 (39% worse) |
| Gross P&L | -$471.13 | -$985.64 | -$514.51 |
| Sharpe Ratio | -4.541 | -6.364 | -1.823 |

Per-cluster (calibrated run):

| Cluster | Direction | TP | SL | Trades | Win Rate | Net P&L |
|---------|-----------|-----|-----|--------|----------|---------|
| 3 | LONG | 1.559% | 0.779% | 224 | 35.71% | -$873.32 |
| 9 | LONG | 2.091% | 1.046% | 0 | N/A | $0.00 |
| 1 | SHORT | 1.640% | 0.820% | 361 | 35.18% | -$1,430.67 |
| 5 | SHORT | 1.429% | 0.715% | 174 | 35.06% | -$706.55 |
| 6 | SHORT | 2.167% | 1.083% | 84 | 36.90% | -$303.02 |
| 7 | SHORT | 2.037% | 1.019% | 3 | 33.33% | -$13.01 |
| 8 | SHORT | 1.605% | 0.803% | 236 | 36.02% | -$905.07 |

Per-direction: LONG n=224, WR=35.71%, net=-$873.32; SHORT n=858, WR=35.55%, net=-$3,358.32 — direction symmetry persists.

### Interpretation: The Signal Is Too Weak at Any Target Scale

Shrinking TP proportionally shrinks SL (fixed 2:1 ratio), so a "more reachable" take-profit comes paired with a *more noise-sensitive* stop-loss. The calibrated run shows exactly this trade-off resolving unfavorably: trade count rose 25% (more entries technically reach TP) but win rate **fell**, because the tighter SL is clipped by ordinary intrabar price noise more often than the tighter TP is reached by genuine directional signal. Net result: more trades, lower win rate, larger loss.

This is the decisive test for the "badly-calibrated targets" hypothesis. If the underlying cluster signal were real but simply mis-targeted, calibrating TP/SL to the cluster's own historical MFE should have improved — or at minimum not worsened — the outcome. Instead it got **worse at every level** (trades, win rate, net P&L, gross P&L, Sharpe). This rules out target miscalibration as the explanation and confirms: **the directional signal itself, not the TP/SL sizing, is the limiting factor.**

### Key Question Answered

> Does the signal become profitable with properly calibrated (smaller, cluster-matched) TP targets, or is the edge still too small to overcome costs even with realistic targets?

**Answer: The edge remains absent (and the outcome is materially worse) even with cluster-matched, training-derived TP/SL targets.** This closes off target-calibration as a viable remediation path for Epic 065.

Output files: `k10_calibrated_tp_backtest_2025_07_2026_06.csv` (1,082-row trade log with per-trade TP/SL columns), `k10_calibrated_tp_backtest_summary.json` (overall + per-cluster + per-direction metrics, plus the calibration table itself).

---

## Section 7: Phase 3 Decision & Recommendation

### Final Recommendation: **Do NOT Proceed with Clustering-Based Strategy**

This conclusion is now supported by **six independent backtest configurations** (t870 idealized, four realistic t871 classification variants including the statistically corrected one, and the calibrated-TP follow-up), all converging on the same result: no exploitable edge survives realistic execution costs, at any TP/SL scale.

**Rationale (definitive):**

1. **Statistically correct classification changes which clusters trade, not whether the strategy is profitable.** Win rate remains in the 35–38% band across every methodology tried.
2. **Both directions fail equally.** There is no LONG-vs-SHORT asymmetry to exploit — ruling out "we traded the wrong direction" as an explanation for poor performance.
3. **Target calibration does not help — it hurts.** Sizing TP/SL to each cluster's own training-period MFE made the loss 39% larger, ruling out "badly-sized targets" as the explanation.
4. **Required win rate (>67%) vs. achieved (35–37%) is a 30+ point gap** — far too large to close via parameter tuning, position sizing, further cluster refinement, or target calibration.
5. **Signal instability across train→OOS** (documented in the companion cluster profile analysis) shows 35–100% degradation in MFE magnitude and up to complete disappearance of cluster assignments, indicating the K-means partition does not generalize.

### Epic 065 Closure

- **Phase 3 Conclusion:** Cluster regime strategy (K=3 and K=10 variants) is **NOT production-ready**, under any classification methodology or TP/SL calibration tested. Every remediation hypothesis tried (fix the direction bug, restrict to significant clusters, calibrate targets to actual MFE) either left the result unchanged or made it worse.
- **Recommended next step:** Return to the LightGBM-based directional modeling approach (`src/modeling/`), with explicit focus on transaction cost optimization in the strategy calibration layer (`src/strategy/`), where isotonic calibration and Optuna threshold search can directly account for realistic cost structures — something the clustering approach has no mechanism to do.

---

## Technical Appendix: Definitive Backtest Configuration

### Tradeable Setups (Corrected, Frozen)

```python
TRADEABLE_SETUPS = {
    3: "long",    # train n=9,409,  t=+2.966, p=0.0030
    9: "long",    # train n=1,475,  t=+2.906, p=0.0037  (OOS n=0)
    1: "short",   # train n=16,597, t=-4.309, p=0.0000165
    5: "short",   # train n=10,441, t=-5.987, p<0.0001
    6: "short",   # train n=4,200,  t=-8.505, p<0.0001
    7: "short",   # train n=3,599,  t=-5.662, p<0.0001  (OOS n=2)
    8: "short",   # train n=17,083, t=-4.525, p=0.0000061
}
```

### Entry/Exit Logic

**LONG (clusters 3, 9):**
- Entry: close[t] + 0.2% slippage
- TP: close >= entry × 1.02 → exit at entry × 1.018
- SL: low <= entry × 0.99 → exit at entry × 0.992
- Timeout: 24h → exit at max(high) × 0.998

**SHORT (clusters 1, 5, 6, 7, 8):**
- Entry: close[t] - 0.2% slippage
- TP: close <= entry × 0.98 → exit at entry × 0.982
- SL: high >= entry × 1.01 → exit at entry × 1.008
- Timeout: 24h → exit at min(low) × 1.002

### Cost Allocation

- Per-trade fixed: 0.3% of $1,000 = $3.00
- Entry slippage: 0.2% × $1,000 = $2.00
- Exit slippage: 0.2% × $1,000 = $2.00
- **Total per trade: ~$7.00 (0.7% of notional)**

---

## Output Files

- `k10_final_corrected_backtest_2025_07_2026_06.csv` — Definitive fixed-TP trade log (863 rows, 7 clusters, LONG+SHORT)
- `k10_final_corrected_backtest_summary.json` — Definitive fixed-TP summary (overall + per-cluster + per-direction)
- `k10_calibrated_tp_backtest_2025_07_2026_06.csv` — Calibrated-TP trade log (1,082 rows, per-trade TP/SL columns)
- `k10_calibrated_tp_backtest_summary.json` — Calibrated-TP summary (overall + per-cluster calibration + per-direction)
- `kmeans_k10_trained_2021_2025_06.pkl` — Trained model (archived)
- `paired_ttest_cluster_classification.json` — Corrected classification methodology results
- `cluster_profile_train_vs_oos_all_clusters.json` — Train-vs-OOS MFE degradation analysis (all 10 clusters)
- `apriori_cluster_classification.json` — **Superseded/invalid** independent-sample t-test (kept for audit trail; do not use)
- `k10_oos_backtest_2025_07_2026_06_realistic.csv` / `_summary.json` — **Superseded** v1/v2 (LONG-only, then all-10 LONG+SHORT) runs, kept for audit trail

---

## References & Related Tasks

- **t863:** Feature redundancy selection (MI-based)
- **t865:** Window representation (360-bar rolling, z-score normalization)
- **t866:** K-means clustering (k=3..8 elbow method)
- **t867:** OOS validation (original Bonferroni-corrected t-tests on k=3 model)
- **t870:** K=3 idealized backtest (98.3% win rate, zero-cost execution — predecessor baseline)
- **t871:** K=10 realistic backtest — **THIS DOCUMENT**, definitive result after classification bug fix
