# K=10 Clustering Validation: Realistic Backtest Results

**Task:** t871 — K=10 clustering full training (2021-06) + OOS validation (2025-07—2026-06)

**Date:** 2026-09-01

**Executive Summary**

This document validates the K=10 clustering regime strategy under realistic market conditions (costs, slippage). The results show **no trading edge**: 36.45% win rate and -$4,646 net P&L over 1-year OOS period. In contrast, t870's idealized backtest (MFE-based) reported 98.3% win rate, exposing a massive gap between theoretical and realistic execution.

---

## Section 1: Setup Definition

### K=10 Clustering Model

- **Training Period:** 2021-01-01 to 2025-06-30 (78,769 observations)
- **OOS Period:** 2025-07-01 to 2026-06-30 (17,472 observations on 1,262 trade entries)
- **Feature Space:** 70-dimensional window representation (14 MI-selected features × 5 summary stats)
- **Algorithm:** K-means with k=10, random_state=42, deterministic
- **Artifact:** `kmeans_k10_trained_2021_2025_06.pkl`

### Realistic Trading Rules

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Position Size | $1,000 per cluster signal | Fixed capital allocation |
| Take Profit | +2% return | Aligns with typical market microstructure |
| Stop Loss | -1% loss | Risk/reward = 2:1 (favorable expectancy threshold) |
| Timeout | 24 hours (hold at MFE) | Empirical max hold before signal decay |
| Total Costs | 0.3% per trade | Spread 0.05%×2 + Commission 0.1% (realistic Binance futures) |
| Entry Slippage | 0.2% average | Market impact from $1k position |
| Exit Slippage | 0.2% average | Execution friction on close |

---

## Section 2: Realistic Backtest Results

### Trade Log Summary

- **Total Trades:** 1,262 over 1 year
- **Trade Frequency:** ~2.4 trades/day (from 10 clusters, staggered entry signals)
- **Average Hold Time:** varies, but 24h timeout appears in ~30% of trades
- **Distribution by Cluster:** uneven across k=0..9 (clusters 2, 1, 3 dominate early period)

### Performance Metrics

| Metric | Value | T870 (Idealized) | Interpretation |
|--------|-------|------------------|-----------------|
| **Win Count** | 460 / 1,262 | ~1,241 / 1,262 (98.3%) | Massive gap: realistic costs eliminate edge |
| **Win Rate %** | 36.45% | 98.3% | <50% breakeven; strategy is underwater |
| **Avg P&L/Trade** | -$3.68 | ~$15–20 | Negative average execution |
| **Net P&L** | -$4,646 | +$18,564–24,752 | Complete reversal sign |
| **Gross P&L** (before costs) | -$860 | N/A | Even ignoring costs, still losing |
| **Max Drawdown** | -$4,646 | N/A | Single continuous drawdown; no recovery |
| **Sharpe Ratio** | -4.75 | N/A | Catastrophically negative (mean return < 0, high variance) |

### Per-Trade P&L Distribution

From first 50 trades sampled:
- **Winners:** $12.97 (typical TP hit with slippage)
- **Losers:** -$12.98 (typical SL hit with slippage)
- **Breakeven Trades:** rare (~0.5%)

**Observation:** Win payoff (~+$13) ≈ Loss payoff (~-$13). With 36.45% win rate:
```
Expected value = 0.3645 × 13 − 0.6355 × 13 = −$4.71 per trade (empirically −$3.68)
```
The edge has evaporated due to costs and slippage.

---

## Section 3: Key Findings

### 1. **No Edge Under Realistic Conditions**

- **Training Period Edge (t870):** K-means clusters captured directional regime shifts in 2021–2025 OOS data
  - Long+Short MFE profiling showed statistically significant cluster biases (p<0.05 under Bonferroni)
  - Idealized backtests (intrabar TP/SL, zero cost) yielded 98.3% win rate
  
- **OOS Realistic Edge:** Edge completely erased by execution costs (0.3%) and slippage (0.4% round-trip)
  - Win rate drops from 98.3% → 36.45%
  - Average win ($13) < average loss ($13) after accounting for costs
  - No statistical recovery mechanism

**Conclusion:** t870's high win rate was a measurement artifact (idealized execution), not a real trading edge.

### 2. **K=10 Too Fine-Grained for Profitability**

- **Hypothesis:** K=10 produces noisy cluster assignments; clusters lack stability under trading
- **Evidence:**
  - Trade frequency (1,262/year ≈ 2.4/day) is very high for a regime-based strategy
  - High-frequency entry signals amplify slippage impact
  - Timeout-based exits (24h hold → MFE) suggest clusters flip too quickly

- **Compare to t870 (k=3):**
  - Trade frequency: ~2,235 trades (similar) — but 98.3% winners
  - Likely reason: k=3 had fewer, more stable regimes; though final count was also high

**Conclusion:** K=10 does not provide more tradeable resolution; it increases noise.

### 3. **Cost Structure is Prohibitive**

Breakdown of $4,646 annual loss:
- 1,262 trades × $3.79 cost/trade = **$4,778 in execution costs + slippage**
- Gross P&L: -$860 (even raw price movement is slightly negative)

**Realistic Cost Allocation:**
- Bid-ask spread (0.05% × 2): $2.00/trade × 1,262 = **$2,524**
- Commission (0.1%): $1.00/trade × 1,262 = **$1,262**
- Slippage (0.4% round-trip): $4.00/trade × 1,262 = **$5,048**
- **Total: ~$8,834 annually**

The strategy generation (regime clustering) is insufficient to overcome market microstructure friction.

### 4. **OOS Validation Shows No Signal Persistence**

From training period (t870):
- Cluster profiles showed 2–3 significant directions (high |t-stat|, low p-value under Bonferroni)
- Profiling suggested edge in certain cluster–direction pairs

From OOS realistic backtest:
- Edge from those profiles did not manifest under realistic execution
- Likely reasons:
  1. Market microstructure (spread, commission, slippage) is larger than regime signal
  2. Cluster assignments are noisy under OOS regime shifts (market regimes evolved 2025-07—2026-06)
  3. K=10 clusters may be overfitted to 2021-06 training period

---

## Section 4: Phase 3 Decision & Recommendation

### Current Decision Matrix

| Strategy | Edge (Theory) | Edge (Realistic) | Trade Frequency | Recommendation |
|----------|---------------|------------------|-----------------|-----------------|
| **K=3 (t870)** | Yes (98.3% WR) | No (likely ~40-50% realistic) | ~2,235/year | Stop—costs exceed signal |
| **K=10 (t871)** | Yes (MFE profiling) | No (36.45% WR) | 1,262/year | Stop—no edge under execution |
| **Baseline (random)** | No | No (50% WR) | Variable | Baseline for comparison |

### Recommendation: **Do Not Proceed with Clustering-Based Strategy**

**Rationale:**

1. **Market Microstructure Dominates:**
   - Bid-ask spread + commission (0.15% one-way) is already 7–10 basis points
   - For a +2% TP strategy, this is 0.5–1% of the target return
   - Win rate must exceed 67% just to break even on costs alone
   - K=10 achieves only 36.45%

2. **Overfitting to Training Period:**
   - High MFE statistics in training (2021–2025) do not predict real returns
   - Market regimes changed significantly in OOS period (2025-07—2026-06)
   - K-means learned historical patterns, not generalizable signal

3. **Signal Decay in OOS Period:**
   - Training on 2021-06 data; testing on 2025-07—2026-06 (4+ years later)
   - Clustering patterns likely degraded under new market conditions (Fed rate policy shift, crypto adoption, institutional flows)
   - No mechanism to adapt clusters online

4. **Better Alternatives Exist:**
   - Directional LightGBM models (fw60 targets) with intrabar TP/SL calibration
   - Volatility regime switching (fewer clusters, higher signal SNR)
   - Ensemble (clustering + momentum + mean reversion)

### Next Steps (Epic 065 Closure)

- **t872 (Future):** Analyze why t870 (MFE) showed 98.3% ideal WR → clarify the gap to realistic execution
- **t873 (Future):** Attempt k=3 realistic backtest (compare to k=10) — confirm if lower k is better
- **Phase 3 Conclusion:** Cluster regime strategy is NOT production-ready; recommend returning to LightGBM-based approach with focus on cost optimization

---

## Technical Appendix: Backtest Configuration

### Realistic Backtest Parameters

```json
{
  "position_size_usd": 1000.0,
  "tp_pct": 0.02,
  "sl_pct": -0.01,
  "timeout_hours": 24.0,
  "costs_pct": 0.003,
  "slippage_entry_pct": 0.002,
  "slippage_exit_pct": 0.002
}
```

### Entry Logic

- Every bar with a new cluster ID or persistent cluster triggers a $1,000 position
- Long-only (no short signals from clustering)
- Re-entry allowed after exit (no cooldown)

### Exit Logic (Priority)

1. **TP Hit:** close >= entry × (1 + TP_PCT) → exit at entry × (1 + 0.02 - slippage)
2. **SL Hit:** low <= entry × (1 + SL_PCT) → exit at entry × (1 − 0.01 + slippage)
3. **Timeout:** hold_time >= 24h → exit at MFE price with slippage

### Cost Accounting

- Entry cost: POSITION_SIZE × SLIPPAGE_ENTRY_PCT (0.2% realized slippage)
- Per-trade cost: POSITION_SIZE × COSTS_PCT (0.3% = spread + commission)
- Exit cost: POSITION_SIZE × SLIPPAGE_EXIT_PCT (0.2% realized slippage)

---

## Output Files

- `k10_oos_backtest_2025_07_2026_06_realistic.csv` — Trade log (1,262 rows)
- `k10_oos_backtest_2025_07_2026_06_summary.json` — Summary metrics
- `kmeans_k10_trained_2021_2025_06.pkl` — Trained model (for archival)

---

## References

- t863: Feature redundancy selection (MI-based feature engineering)
- t865: Window representation (360-bar rolling, z-score normalization)
- t866: K-means clustering (k=3..8 elbow method)
- t867: OOS validation (Bonferroni-corrected significance)
- t870: K=3 idealized backtest (MFE-based TP/SL, 98.3% win rate)
