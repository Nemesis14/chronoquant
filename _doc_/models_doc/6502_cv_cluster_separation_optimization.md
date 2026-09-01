# CV-Optimized Cluster Separation: ANOVA F-Score Walk-Forward K-Search

**Task:** t872 — CV-optimalizált klaszter-szeparáció (ANOVA F-score alapú k keresés)

**Date:** 2026-09-01

**STATUS: EPIC 065 CLOSED.** Sections 1–7 cover the core t872 clustering-quality search (no trading backtest, per original ticket scope). Section 8 covers three coordinator-requested follow-up robustness diagnostics (cluster size, direction, and cross-fold identity stability). Section 9 is the final Epic 065 closure summary, combining this document with t871's realistic-backtest conclusion. See Section 9 for the bottom line.

**Executive Summary**

Epic 065's first two iterations (t863–t871) fixed k in advance (k=3, then k=10) and only afterward tried to determine which clusters were "tradeable" — an approach that was shown to have no exploitable trading edge under realistic execution, regardless of classification methodology or TP/SL calibration (`6501_k10_validation_realistic_backtest.md`). This third iteration inverts the process: **k itself is chosen by a walk-forward cross-validated search that directly optimizes cluster separation quality** (ANOVA F-statistic on the long/short MFE directional signal), always measured out-of-fold. The search over k=2..30 selected **k=13** as the most separative and stable configuration (cv_score=17.70, not dominated by any single fold). A companion out-of-fold direction report (Fold 4: train 2021–2025H1, validate 2025H2–2026H1) finds 4 of the 13 clusters show a statistically significant SHORT bias (Bonferroni-corrected), 0 show a significant LONG bias, 7 are NEUTRAL, and 2 clusters have zero members in the OOS validation window.

**Update (Section 8/9, Epic closure):** three follow-up diagnostics — per-cluster size stability, direction-assignment stability, and cross-fold cluster identity via centroid matching — were run to stress-test this result before any trading conclusion. They found the apparent k=13 separation quality does not translate into a robust, stable, tradeable signal: only 1 of 13 clusters survives as a genuinely cross-fold-confirmed, directionally consistent pattern, and it is too small (~1–2% of the data) to be a meaningful trading population. Combined with t871's already-negative realistic-backtest result, **Epic 065 is closed with no viable trading path found via cluster-based regime detection.**

---

## Section 1: Pipeline Rebuild

All `research/cluster_regime/` and `artifacts/research_cluster_regime/` files from the prior two iterations were deleted (repo cleanup, user request — only champion model artifacts retained). The pipeline was rewritten from scratch, preserving the validated logic from t863–t865 with one methodological fix:

| Step | Script | Change vs. t863–t865 |
|------|--------|------------------------|
| Feature selection | `03_feature_selection.py` | **Train-only fix:** correlation matrix now computed ONLY on 2021-01–2022-12 (Fold 1's training window — the one date range that is a training period in every CV fold and a validation period in none). The original version sampled from the full `quant_train` table, which would leak information about later (validation) periods into feature selection. |
| Target computation | `01_target_computation.py` | Unchanged: `long_mfe_fw360 = log(max(high[t+1:t+360])/close[t])`, `short_mfe_fw360 = log(min(low[t+1:t+360])/close[t])` — strictly t+1..t+360, no leakage into the observation itself. |
| Window representation | `02_window_representation.py` | Unchanged core logic (360-bar lookback, 30-min downsampling, within-window z-score normalization, 5 summary stats/feature). Now loads the selected-feature list dynamically from `03`'s JSON output instead of a hardcoded list. Computed once over the full 2021-01–2026-06 range; CV folds slice this single table by date rather than recomputing it four times (window construction is a purely row-local transform, so sharing one precomputed table across folds carries no leakage risk). |
| CV k-search | `04_cv_cluster_optimization.py` | **New.** Walk-forward ANOVA F-score search, described in Section 2. |

### Selected Features (14, train-only on 2021–2022)

| Dimension | Features |
|-----------|----------|
| candle_shape | `feat_doji`, `feat_body_ratio` |
| market_structure | `feat_lr_r2_30`, `feat_efficiency_ratio_10` |
| momentum | `feat_adx_14`, `feat_return_momentum_delta_10` |
| price_action | `feat_return_autocorr_lag1_30`, `feat_returns_skew_14` |
| trend | `feat_directional_agreement_10_30`, `feat_lr_slope_60` |
| volatility | `feat_bb_width_rank_20`, `feat_hist_vol_rank_60` |
| volume_liquidity | `feat_ad_line`, `feat_volume_ratio_14` |

Nearly identical to t863's original selection (`feat_body_ratio_sma_10` swapped for `feat_doji` in `candle_shape` — a minor consequence of the narrower train-only sampling window), confirming the feature set is not sensitive to this correction.

**Window representation output:** 104,011 rows × 75 columns (70 = 14 features × 5 stats, plus `open_time` + 4 target columns), covering 2021-01 through 2026-06-30.

---

## Section 2: CV Structure and ANOVA F-Score Methodology

### Walk-Forward CV (4 folds, not random k-fold)

```
Fold 1: Train 2021-2022      (n=34,993) -> Validate 2023            (n=17,473)
Fold 2: Train 2021-2023      (n=52,513) -> Validate 2024            (n=17,521)
Fold 3: Train 2021-2024      (n=70,081) -> Validate 2025 H1         (n=8,641)
Fold 4: Train 2021-2025 H1   (n=78,769) -> Validate 2025 H2-2026 H1 (n=17,472)
```

Random k-fold is invalid for time series (it would let the model "see the future" within a fold); the growing-window walk-forward scheme guarantees every validation period is strictly chronologically after its training period.

### ANOVA F-Score (validation-only)

Per-observation directional signal:
```
signal_i = long_mfe_fw360_i + short_mfe_fw360_i
```
(since `short_mfe_fw360 < 0`, this equals `|long_mfe_i| - |short_mfe_i|`: positive → long-biased excursion, negative → short-biased excursion, near-zero → directionally neutral.)

For each (k, fold) pair: fit a cluster model on the fold's **training** rows, predict cluster labels for the fold's **validation** rows, then compute a one-way ANOVA F-statistic on `signal` grouped by predicted cluster label — **on the validation rows only**. Fitting the F-test on train data would be trivially inflated (the clustering objective itself tends to separate points, so training-set separation is not evidence of a generalizable pattern); the validation-only measurement is the critical methodological choice here.

```
cv_score(k) = mean(F_fold1, F_fold2, F_fold3, F_fold4)
cv_std(k)   = std(F_fold1, F_fold2, F_fold3, F_fold4)
```

**Search vs. final algorithm:** the k=2..30 grid (29 values × 4 folds = 116 fits) uses `MiniBatchKMeans` (n_init=3, max_iter=100, batch_size=2048) for tractable runtime; the final selected-k model is refit with full-batch `KMeans` (n_init=10, max_iter=300) on the complete dataset for higher-quality centroids in the saved artifact.

**Statistical caveat:** the signal series is autocorrelated (documented in t867/t871's effective-N discussion), so classical ANOVA p-values are optimistic — effective N is well below nominal N. The F-statistic is used here strictly as a **relative ranking metric across k values and folds**, not as an absolute significance test; the same autocorrelation-driven inflation applies uniformly across every k tested, so relative comparisons remain valid even though absolute p-values should not be trusted.

### Stability Check (Single-Fold Dominance)

For each k, `single_fold_dominance = max(F_folds) / sum(F_folds)`. If one fold contributes more than 50% of the total F across the 4 folds, the k is flagged `unstable` — a sign the apparent separation is a one-period artifact (e.g., a single unusual regime in one validation year) rather than a persistent pattern. Unstable k's are excluded from the primary selection pool.

---

## Section 3: K-Search Results (k=2..30)

| k | cv_score | cv_std | CV (std/score) | dominance | unstable |
|---|----------|--------|-----------------|-----------|----------|
| 2 | 9.8245 | 14.0235 | 1.4274 | 0.779 | **True** |
| 3 | 10.7008 | 9.6652 | 0.9032 | 0.559 | **True** |
| 4 | 12.7541 | 7.9305 | 0.6218 | 0.447 | False |
| 5 | 25.1364 | 19.8503 | 0.7897 | 0.515 | **True** |
| 6 | 15.9019 | 5.9738 | 0.3757 | 0.361 | False |
| 7 | 17.4084 | 15.8769 | 0.9120 | 0.575 | **True** |
| 8 | 13.7092 | 5.2513 | 0.3831 | 0.342 | False |
| 9 | 15.9221 | 4.7436 | 0.2979 | 0.297 | False |
| 10 | 17.1058 | 9.0447 | 0.5287 | 0.395 | False |
| 11 | 13.6968 | 8.6900 | 0.6345 | 0.397 | False |
| 12 | 12.8727 | 5.0141 | 0.3895 | 0.386 | False |
| **13** | **17.7042** | **9.6207** | **0.5434** | **0.386** | **False ← SELECTED** |
| 14 | 16.0533 | 6.2749 | 0.3909 | 0.372 | False |
| 15 | 14.6783 | 4.6393 | 0.3161 | 0.341 | False |
| 16 | 16.1157 | 7.1776 | 0.4454 | 0.358 | False |
| 17 | 15.6079 | 5.4415 | 0.3486 | 0.370 | False |
| 18 | 11.9088 | 3.2039 | 0.2690 | 0.350 | False |
| 19 | 14.3276 | 3.1037 | 0.2166 | 0.284 | False |
| 20 | 12.1608 | 3.0998 | 0.2549 | 0.306 | False |
| 21 | 12.0720 | 4.7318 | 0.3920 | 0.372 | False |
| 22 | 13.1999 | 1.1703 | 0.0887 | 0.274 | False |
| 23 | 10.8579 | 4.0007 | 0.3685 | 0.372 | False |
| 24 | 13.8727 | 2.2459 | 0.1619 | 0.276 | False |
| 25 | 14.7467 | 3.4700 | 0.2353 | 0.308 | False |
| 26 | 11.7319 | 3.7898 | 0.3230 | 0.344 | False |
| 27 | 12.9553 | 2.5500 | 0.1968 | 0.323 | False |
| 28 | 11.3331 | 3.6404 | 0.3212 | 0.358 | False |
| 29 | 13.2817 | 2.0978 | 0.1579 | 0.301 | False |
| 30 | 11.9833 | 3.7284 | 0.3111 | 0.329 | False |

### Optimal K Selection Rationale

k=5 has the single highest raw cv_score (25.14), and k=2, 3, 7 also post high scores — but **all four are flagged unstable** (dominance 51.5–77.9%, i.e., over half of each of these k's total separation signal comes from a single one of the four folds). Manual inspection confirms the pattern: at low k, the ANOVA F-score is extremely sensitive to which regime happened to dominate one particular validation year, not to a persistent structural pattern.

Among the **stable** candidates (dominance ≤ 50%, no single fold dominates), **k=13 has the highest cv_score (17.70)**, ahead of k=10 (17.11, incidentally close to the previous iterations' arbitrarily-chosen k=10) and k=16 (16.12). k=13's dominance (0.386) is comfortably under the 50% instability threshold, and all four of its fold-level F-statistics are independently significant at conventional (uncorrected) levels — see per-fold detail below.

**Per-fold detail for k=13:**

| Fold | n_train | n_valid | n_realized_clusters | F-stat | p-value |
|------|---------|---------|----------------------|--------|---------|
| fold1 (valid=2023) | 34,993 | 17,473 | 13 | 27.30 | 3.0e-62 |
| fold2 (valid=2024) | 52,513 | 17,521 | 10 | 5.84 | 3.7e-08 |
| fold3 (valid=2025H1) | 70,081 | 8,641 | 12 | 23.43 | 3.9e-48 |
| fold4 (valid=2025H2-2026H1) | 78,769 | 17,472 | 12 | 14.25 | 8.4e-28 |

All four folds independently show F >> 1 with nominal p << 0.05 (recalling the autocorrelation caveat above — these p-values are optimistic in absolute terms, but their consistent presence across all four independently-fit folds is itself the relevant stability evidence, not any single p-value). Note `n_realized_clusters` is occasionally below 13 (fold2: 10, fold3 & fold4: 12) — a small number of the 13 fitted centroids receive zero validation-set members in some folds, consistent with the "clusters can vanish OOS" phenomenon already documented in the prior k=10 iteration, just less pronounced here.

**Selected: k=13.**

---

## Section 4: Final Model

`cv_optimal_k_model.pkl` contains the full-batch `KMeans(k=13)` fit on the entire 2021-01–2026-06-30 window representation (all 104,011 rows), plus the feature column list and the per-column NaN-imputation medians used at fit time (so the model can be applied consistently to new data). This is the artifact a future trading-validation task would load.

---

## Section 5: Per-Cluster Direction Report (CV-Validated, Out-of-Fold)

**Methodology note:** cluster label integers are not guaranteed aligned across independently-fit folds (KMeans labeling is arbitrary per fit), so per-cluster direction cannot be validly averaged across folds. Following the corrected paired t-test methodology validated in t871 (`08_paired_ttest_classification.py`), this report uses **Fold 4 only** — the model trained on 2021-01–2025-06-30 and evaluated on 2025-07-01–2026-06-30 — since it is both the most recent regime and a genuinely uncontaminated out-of-fold split relative to its own training window.

```
diff_i = long_mfe_fw360_i + short_mfe_fw360_i     (paired, per-observation)
t_stat  = mean(diff) / (std(diff) / sqrt(n))
Bonferroni alpha = 0.05 / 13 = 0.003846
```

| Cluster | n_obs (OOS) | long_mfe_mean | short_mfe_mean | diff_mean | t_stat | p_value | Assignment |
|---------|-------------|-----------------|------------------|-----------|--------|---------|------------|
| 0 | 1,393 | +0.01303 | -0.01502 | -0.00198 | -2.746 | 0.00612 | NEUTRAL |
| 1 | 2,277 | +0.01305 | -0.01409 | -0.00103 | -2.248 | 0.02469 | NEUTRAL |
| 2 | 68 | +0.01194 | -0.03460 | -0.02266 | -4.179 | 0.00009 | **SHORT** |
| 3 | 626 | +0.01316 | -0.01476 | -0.00160 | -1.796 | 0.07291 | NEUTRAL |
| 4 | 817 | +0.01436 | -0.01999 | -0.00563 | -6.459 | <0.0001 | **SHORT** |
| 5 | 2,389 | +0.01319 | -0.01337 | -0.00018 | -0.430 | 0.66716 | NEUTRAL |
| 6 | 0 | — | — | — | — | — | **MISSING** (0 OOS members) |
| 7 | 3,153 | +0.01241 | -0.01406 | -0.00165 | -2.937 | 0.00333 | **SHORT** |
| 8 | 266 | +0.01117 | -0.00999 | +0.00118 | +1.273 | 0.20423 | NEUTRAL |
| 9 | 0 | — | — | — | — | — | **MISSING** (0 OOS members) |
| 10 | 4,233 | +0.01262 | -0.01516 | -0.00254 | -7.605 | <0.0001 | **SHORT** |
| 11 | 955 | +0.01900 | -0.01904 | -0.00004 | -0.038 | 0.96972 | NEUTRAL |
| 12 | 1,295 | +0.01331 | -0.01464 | -0.00133 | -2.276 | 0.02299 | NEUTRAL |

**Summary:** 4 of 13 clusters (2, 4, 7, 10) show a statistically significant SHORT bias under Bonferroni correction; **0 clusters show a significant LONG bias**; 7 are NEUTRAL; 2 (clusters 6, 9) have zero members in the OOS validation window — the same "cluster disappears out-of-sample" phenomenon documented for the fixed-k=10 approach, occurring here for 2 of 13 clusters (15%) versus 1 of 10 (10%) previously, roughly comparable.

**This asymmetry (SHORT-only significant clusters, zero significant LONG clusters) mirrors the k=10 iteration's finding** (`6501_k10_validation_realistic_backtest.md`, Section 1: 5 of 7 significant clusters were SHORT there too), suggesting it reflects a genuine, k-independent regime characteristic of the 2025H2–2026H1 period rather than an artifact of the specific clustering choice.

---

## Section 6: Comparison to Prior Iterations

| Aspect | t863–t870 (fixed k=10) | t871 (fixed k=10, corrected) | **t872 (this task)** |
|--------|--------------------------|-------------------------------|----------------------|
| k selection | Arbitrary (elbow/silhouette on unrelated criteria) | Same fixed k=10 | **Data-driven: walk-forward CV search over k=2..30** |
| Direction classification | Post-hoc, initially with a mathematically invalid test | Post-hoc, corrected paired t-test | Post-hoc, same corrected paired t-test, applied to CV-selected k |
| Validation scope | Single train/OOS split | Single train/OOS split | **4-fold walk-forward, F-score always out-of-fold** |
| Stability check | None | None | **Single-fold-dominance flag excludes k's driven by one period** |
| Trading backtest | Yes (multiple variants) | Yes (multiple variants, all showed no edge) | **None — explicitly out of scope for this task** |
| Result | 4/10 clusters significant in training, degraded 40–45% OOS | 7/10 clusters correctly classified (2 LONG, 5 SHORT), no trading edge | 13 clusters; OOS (Fold 4) shows 4/13 significant, all SHORT; qualitatively consistent asymmetry with t871 |

The SHORT-dominant asymmetry recurring across two independent clustering configurations (k=10 fixed, k=13 CV-selected) is the most notable cross-validation signal in this task — it did not disappear when k was chosen by an objective, leakage-free search rather than picked arbitrarily.

---

## Section 7: Key Findings & Next Steps

1. **A data-driven, walk-forward-validated k does exist and is stable:** k=13 shows the highest ANOVA F-score among candidates whose separation is not dominated by a single validation period, with all 4 folds independently significant (nominal, autocorrelation-uncorrected).
2. **Low-k candidates (2, 3, 5, 7) are unstable artifacts**, not genuine structure — their high raw F-scores are driven by one anomalous fold each. This is exactly the failure mode the ticket's stability check was designed to catch.
3. **Directional asymmetry (SHORT >> LONG significance) persists across both the k=10 and k=13 configurations** — the most robust finding of this task, and worth carrying into any future trading-oriented follow-up.
4. **Cluster disappearance OOS persists** (2/13 clusters here vs. 1/10 previously) — a structural characteristic of this regime-clustering approach that any downstream trading design must account for (e.g., a fallback/no-signal state when a cluster has no members).
5. **No conclusion about trading profitability is drawn or implied here.** t871 already demonstrated that even correctly-classified, well-separated clusters (via a properly corrected statistical test) did not translate into a profitable strategy after realistic costs and slippage, across five separate backtest configurations. This task's improved cluster separation does not by itself indicate the underlying signal is now large enough to overcome the ~0.7%/trade cost structure identified in t871 — that would require a new, dedicated backtest task explicitly scoped to test whether k=13's stronger separation translates into better realized P&L than k=10's did.

### Recommended Next Step (as originally written — SUPERSEDED, see Section 8 below)

If Epic 065 continues, a follow-up task should backtest the k=13 model's 4 significant SHORT clusters (2, 4, 7, 10) under the same realistic-cost methodology validated in t871 (0.3% costs + 0.4% round-trip slippage, cluster-specific or fixed TP/SL — t871 showed calibrated TP/SL made things worse, so fixed TP/SL should be tried first). Given the qualitative similarity to t871's already-negative result, expectations for this follow-up should be modest — but it is the logical next validation step now that the clustering itself has been optimized rather than arbitrarily chosen.

**Note (added at Epic closure):** the three coordinator-requested follow-up diagnostics in Section 8 substantially revise this recommendation — the naive "4 significant clusters from Fold 4 alone" view undersold how fragile these clusters are (in size, in direction, and in cross-fold identity). Section 8's more rigorous cross-fold analysis found only **one** cluster (of the original 4) that holds up as a genuinely robust, non-coincidental recurring pattern. A follow-up backtest, if pursued, should target that one pattern specifically rather than all 4.

---

## Section 8: Epic Closure — Three Follow-Up Diagnostics (Size, Direction, and Cross-Fold Identity Stability)

**Added 2026-09-01, at Epic 065 closure.** After this document's initial publication, the coordinator requested three additional diagnostics to stress-test the k=13 result before deciding whether ANY cluster-based signal here is trading-viable. All three were run without any new training methodology or backtest — purely diagnostic re-use of the already-fit fold models and already-computed direction classifications. Together with t871's realistic-backtest conclusion, they form the basis for Epic 065's final closure decision (Section 9).

### 8.1 Per-Cluster Size Stability (Train vs. Valid, All 4 Folds)

**Script:** `research/cluster_regime/05_cv_per_cluster_counts.py` · **Output:** `artifacts/research_cluster_regime/cv_per_cluster_train_valid_counts.json`

**Question:** were the clusters that vanished/weakened in validation already small in training, or did they look normal in training and only disappear out-of-fold?

**Finding:** the 6 vanished/near-vanished clusters (fold2: clusters 4, 6, 10; fold3: cluster 12; fold4: clusters 2, 9) were consistently among the **smallest** clusters in their fold's training fit (train share 1.1%–1.9%; the bottom 2–3 clusters by size in fold2 and fold4 specifically). None were degenerate in training (smallest still ~600–900 raw observations) — this is a **base-rate effect**: rare-regime clusters have a material chance of zero occurrences in a 6–12 month validation window purely from sampling variance, not evidence that training itself produced spurious clusters.

### 8.2 Direction Assignment Stability (Train vs. Valid, All 4 Folds, All 13 Clusters)

**Script:** `research/cluster_regime/06_cv_direction_stability.py` · **Output:** `artifacts/research_cluster_regime/cv_direction_stability_train_vs_valid.json`

**Question:** does a cluster's directional signal (LONG/SHORT/NEUTRAL) — established via the corrected paired t-test — survive the transition from its own fold's training sample to that fold's validation sample?

**Finding:** only **16 of 52** cluster × fold combinations (30.77%) show a matching train/valid assignment. Of the 36 mismatches: **6 are outright FLIPS** (LONG↔SHORT reversal — fold1 clusters 6,7; fold3 clusters 0,2,8; fold4 cluster 11), 10 *weaken* (train-significant → valid-NEUTRAL), and — the largest bucket at 14 cases — *strengthen* (train-NEUTRAL → valid-significant, i.e. a direction "appears" only in validation with no training-period basis). The strengthen bucket being larger than the weaken bucket is itself diagnostic: a genuine, persistent signal should typically show regression toward NEUTRAL out-of-fold (weakening), not spontaneously *gain* significance more often than it loses it. This pattern is more consistent with validation-window-specific noise (compounded by the already-documented autocorrelation-driven optimism in the classical p-values) than with a stable underlying signal.

### 8.3 Cross-Fold Cluster Identity via Centroid Matching

**Script:** `research/cluster_regime/07_cross_fold_signal_identity.py` · **Output:** `artifacts/research_cluster_regime/cv_cross_fold_signal_identity.json`

**Question:** since each fold fits an independent KMeans model, is "Cluster 9" in Fold 1 even the same market pattern as "Cluster 9" in Fold 3? (8.2's same-numbered comparison assumes yes, which is not guaranteed.)

**Method:** centroid cosine similarity, mutual-nearest-neighbor matching across all 6 fold pairs, union-find grouping into candidate persistent patterns. **Before trusting this**, a within-fold baseline was computed (pairwise similarity between the 13 *admittedly different* clusters within one fold's own fit) as a noise floor — this validity check is itself a notable finding: raw centroid similarity in this 70-dimensional summary-statistic space is heavily ceiling-compressed (within-fold baseline median 0.963, cross-fold match median 0.998 — real but modest separation on a [0.80, 1.0] scale), consistent with the low silhouette scores (~0.15–0.30) seen as far back as the original t866 elbow search. The match distribution did clear a pre-registered reliability bar (all 53 mutual matches above the within-fold 75th percentile).

**Finding:** 11 candidate persistent patterns span 2–4 folds (5 clusters have no cross-fold match at all). Of the 11: 6 are labeled CONSISTENT_SHORT, 1 CONSISTENT_LONG, 4 MIXED_CONFLICTING. Critically, **strength varies enormously within the "consistent" label** — the classification only required *no opposite-direction conflict*, not that multiple folds be independently significant. Re-examining each component's member-level significance:

- **Only one pattern is genuinely robust:** `pattern_3` (fold1:cluster3 → fold2:cluster7 → fold4:cluster3; n_obs 620/693/790) has **all three** matched members independently significant SHORT (p<0.001 each), zero NEUTRAL, zero conflict.
- The other 5 "CONSISTENT_SHORT" patterns are weaker: 2–3 of their members are NEUTRAL, with only 1–2 significant members providing the actual signal (e.g. `pattern_0`, `pattern_4`, `pattern_7` each have only **one** significant member out of 4–8).
- The single CONSISTENT_LONG candidate (`pattern_10`) is not real evidence of a LONG pattern: only 1 of its 3 matched folds has any validation data at all (fold3, n=229); the other two folds' matched clusters had **zero** OOS observations. "Consistent" here means "not contradicted by data that doesn't exist," not genuine agreement.

**Bottom line of 8.3: at most one truly robust, cross-fold-identity-confirmed, directionally consistent pattern exists in the k=13 model (SHORT, ~620–790 observations per fold, roughly 1–2% of each fold's training data). No comparably robust LONG pattern exists.**

### 8.4 Synthesis of the Three Follow-Ups

All three diagnostics point the same direction: the k=13 clustering, despite winning the walk-forward ANOVA F-score search on separation *strength*, is **fragile** along every axis tested that matters for a trading application:

| Axis | Finding |
|------|---------|
| Size | Smallest clusters have material odds of vanishing in any given ~1-year OOS window |
| Direction | Only 31% of same-ID cluster/fold combinations keep their train-time direction; 11.5% flip sign outright |
| Identity | Even after correcting for arbitrary cross-fold labeling via centroid matching, only 1 of 13 clusters produces a fully corroborated, non-coincidental, directionally consistent signal — and it is too small (~1–2% of the data) to be a meaningful trading population on its own |

---

## Section 9: Epic 065 Final Closure

**Epic 065 is closed as of 2026-09-01.** Combining this document's k-search result with t871's realistic-backtest result and the three Section 8 follow-up diagnostics, the epic's final conclusion across BOTH clustering configurations tested (fixed k=10 and CV-optimized k=13) is:

> **Cluster-based regime detection (K-means on 360-bar windowed technical-indicator summary statistics) does not produce a trading-viable, stable edge — under any k tested, any direction-classification methodology (including the corrected paired t-test), any TP/SL calibration, or any cross-fold robustness check applied.**

Specifically:
1. **t871 (fixed k=10):** five independent realistic-backtest configurations (LONG-only bug, all-10 LONG+SHORT, invalid "4-tradeable", the statistically corrected 7-cluster run, and a cluster-specific calibrated-TP variant) all converged on a 35–38% win rate against a ~67% breakeven requirement — a 30-point gap that no reclassification or recalibration closed.
2. **t872 core (CV-optimized k=13):** replacing the arbitrary fixed-k choice with a leakage-free, walk-forward-validated ANOVA F-score search still only found k=13 as "best," and even k=13's own OOS direction report (Fold 4) showed 0 significant LONG clusters against 4 significant SHORT.
3. **t872 follow-ups (Section 8):** the improved clustering's apparent quality does not survive closer scrutiny — cluster size, cluster direction, and cluster *identity itself* are all unstable across time to varying degrees, leaving at most one small (~1–2% of the data), genuinely robust pattern (SHORT-only) after correcting for the arbitrary per-fold labeling that the naive same-cluster-ID comparisons in earlier sections implicitly assumed.

No further cluster-based trading validation is recommended under this Epic. Any future work in this direction would need a fundamentally different feature representation or clustering objective — not further tuning of k, direction classification, or TP/SL within the current window-summary-statistic approach — to have a reasonable chance of producing a different outcome.

**Recommended next direction:** return to the LightGBM-based directional modeling pipeline (`src/modeling/`), with explicit focus on transaction-cost-aware strategy calibration (`src/strategy/`) — isotonic calibration and Optuna threshold search there can directly incorporate the realistic cost structure this epic repeatedly found to be the dominant obstacle, which the clustering approach has no mechanism to account for.

---

## Output Files

### Core pipeline (Sections 1–7)
- `research/cluster_regime/01_target_computation.py` — fw360 target computation (rebuilt)
- `research/cluster_regime/02_window_representation.py` — 360-bar window features (rebuilt, dynamic feature loading)
- `research/cluster_regime/03_feature_selection.py` — train-only MI/correlation redundancy analysis (rebuilt, leakage fix)
- `research/cluster_regime/04_cv_cluster_optimization.py` — walk-forward CV + ANOVA F-score k-search (new)
- `artifacts/research_cluster_regime/feature_selection_report.json` — 14 selected features (train-only, 2021-2022)
- `artifacts/research_cluster_regime/solusdt_window_repr.parquet` — 104,011 rows × 75 cols, full 2021-01–2026-06 range
- `artifacts/research_cluster_regime/cv_k_search_results.json` — k=2..30 results, all fold-level F-stats, optimal-k rationale, Fold-4 direction report
- `artifacts/research_cluster_regime/cv_optimal_k_model.pkl` — final k=13 KMeans model (full data, plus feature columns + imputation medians)

### Section 8 follow-up diagnostics
- `research/cluster_regime/05_cv_per_cluster_counts.py` — per-fold, per-cluster train vs. valid n_obs
- `artifacts/research_cluster_regime/cv_per_cluster_train_valid_counts.json`
- `research/cluster_regime/06_cv_direction_stability.py` — train vs. valid direction assignment stability, all 4 folds
- `artifacts/research_cluster_regime/cv_direction_stability_train_vs_valid.json`
- `research/cluster_regime/07_cross_fold_signal_identity.py` — centroid-matching cross-fold cluster identity + consistent-signal extraction
- `artifacts/research_cluster_regime/cv_cross_fold_signal_identity.json`

---

## References & Related Tasks

- **t863–t865:** Original feature selection / target computation / window representation (logic preserved, feature-selection train-only fix applied)
- **t866–t870:** Fixed k=3 then k=10 clustering, post-hoc profiling — superseded methodology
- **t871:** Corrected paired t-test classification + realistic backtest — established that fixed-k clustering has no trading edge under realistic execution, at any TP/SL scale tested
- **t872:** CV-optimized cluster separation (THIS DOCUMENT) — k selection is data-driven and leakage-free; Section 8 follow-ups establish that even the improved k=13 clustering lacks a trading-viable, stable, robustly-identified signal. **Epic 065 closed.**
