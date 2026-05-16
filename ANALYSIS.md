# Model Analysis & Cut-off Strategy

## Date: 2026-02-17

### Overview
Analysis of the prediction spread (long_p - short_p) to determine optimal trading signals.

---

## 1. Spread Distribution

The spread is calculated as: **`spread = lg_l_rw240_p90_base_sm_p - lg_s_rw240_p90_base_sm_p`**

### Statistics
| Metric | Value |
|--------|-------|
| Min | -0.244456 |
| Max | 0.999797 |
| Mean | -0.000051 |
| Median | -0.000967 |
| Std Dev | 0.015464 |

### Percentile Distribution
| Percentile | Value | Signal |
|------------|-------|--------|
| P1 | -0.035255 | Extreme Short |
| P5 | -0.019272 | Strong Short |
| **P10** | **-0.014181** | **← SHORT Cut-off** |
| P25 | -0.007474 | Weak Short |
| P50 | -0.000967 | Neutral |
| P75 | 0.005861 | Weak Long |
| **P90** | **+0.013927** | **← LONG Cut-off** |
| P95 | 0.021352 | Strong Long |
| P99 | 0.053112 | Extreme Long |

---

## 2. Predictive Power Analysis

### LONG Signal (spread >= +0.0139)
**Cases:** 327,059 (10% of data)

| Metric | Value |
|--------|-------|
| Actually had LONG move (trg_l=1) | 52,814 (16.15%) |
| No SHORT move (trg_s=0) | 265,104 (81.06%) |
| **LIFT** | **1.61x** ✅ |

**Interpretation:** When models predict LONG, they're 61% more likely to be right vs random.

---

### SHORT Signal (spread <= -0.0142)
**Cases:** 327,059 (10% of data)

| Metric | Value |
|--------|-------|
| Actually had SHORT move (trg_s=1) | 42,651 (13.04%) |
| No LONG move (trg_l=0) | 277,440 (84.83%) |
| **LIFT** | **1.30x** ✅ |

**Interpretation:** When models predict SHORT, they're 30% more likely to be right vs random.

---

## 3. Trading Strategy

### Signal Zones
```
SHORT Signal Zone     │      NEUTRAL Zone      │     LONG Signal Zone
(spread <= -0.0142)   │ (-0.0142 < x < 0.0139)│  (spread >= 0.0139)
     13% accuracy     │      No trade          │      16% accuracy
     1.30x LIFT       │                        │      1.61x LIFT
```

### Recommended Action
- **LONG**: When `spread >= +0.0139` → Signal for price increase
- **SHORT**: When `spread <= -0.0142` → Signal for price decrease
- **NEUTRAL**: When `-0.0142 < spread < +0.0139` → Do not trade

---

## 4. Model Composition

### Long Model (lg_l_rw240_p90_base_sm)
- **Target:** Top 10% price increases (rolling 240min window)
- **P-value threshold:** 0.01
- **Features selected:** 7 features
  - feat_rsi_14
  - feat_roc_14
  - feat_roc_140
  - feat_sma_ratio_14
  - feat_sma_ratio_140
  - feat_bb_width_14
  - feat_bb_width_140

### Short Model (lg_s_rw240_p90_base_sm)
- **Target:** Bottom 10% price decreases (rolling 240min window)
- **P-value threshold:** 0.01
- **Features selected:** 8 features
  - feat_rsi_14
  - feat_roc_14
  - feat_roc_140
  - feat_macd_diff (extra)
  - feat_sma_ratio_14
  - feat_sma_ratio_140
  - feat_bb_width_14
  - feat_bb_width_140

---

## 5. Data Integrity Check

### Target Correlation
| Scenario | Count | Percentage |
|----------|-------|-----------|
| Both LONG and SHORT | 19,775 | 0.60% |
| Only LONG signal | 307,361 | 9.40% |
| Only SHORT signal | 307,365 | 9.40% |
| Neither | 2,636,803 | 80.60% |

**Finding:** Long and Short targets are nearly inverse (0.60% overlap), validating the model design.

---

## 6. Last Updated
- **Timestamp:** 2026-02-17 02:00 UTC
- **Training Data:** 2019-11-28 → 2026-01-31
- **Total Predictions:** 3,270,567
- **Data Version:** v2 (corrected short target percentile)
