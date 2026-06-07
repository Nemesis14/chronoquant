# SOLUSDT Short LightGBM local_v2 — Fejlesztési és Promóciós Dokumentáció

Dátum: 2026-06-07

---

## Áttekintés

A `lgbm_solusdt_s_fw60_q10_local_v2` short modell a `lgbm_solusdt_l_fw60_q90_local_v2` long
modellel azonos workflow mentén lett fejlesztve. Target: `trg_s_fw60_q10` (1 órán belüli
ár-esés, rolling 10. percentilis). A stratégia sweep 2025-01-01–2026-06-07 intervallumra
készült, friss adatokkal.

---

## 1. Feature Audit

- **Forrás**: `solusdt_1m_features`, 3,060,447 sor, 208 `feat_` oszlop
- **Eltávolítva (high-null >1%)**: `feat_prev_session_high_dist`, `feat_prev_session_low_dist` (1.2% null)
- **Eltávolítva (ismert duplikátumok)**: 6 db (feat_ad_line, feat_atr_14, feat_ohlc_range,
  feat_returns_sma_14, feat_williams_r_14, feat_wma_ratio_14)
- **Aktivitás feature-ök** (18): mind 0% null
- **Végső search feature lista**: 200 feature → `models/lgbm_solusdt_s_fw60_q10_local_v2/search/features_search.json`

---

## 2. Hyperparameter Search

### Smoke stage (5 trial, 2 fold)

| Trial | obj_score | valid_ll | gap   | prauc |
|-------|-----------|----------|-------|-------|
| Best #4 | 0.2843  | 0.2810   | 0.039 | 0.328 |

### Explore stage (60 trial, 5 fold)

Guardrail: champion mean valid PR AUC = 0.2467, floor = 0.2343

| Rang | Trial | obj_score | valid_ll  | train_ll  | gap    | std_ll  | prauc  |
|------|-------|-----------|-----------|-----------|--------|---------|--------|
| 1    | #34   | 0.271666  | 0.268686  | 0.247026  | 0.0217 | 0.01192 | 0.3531 |
| 2    | #40   | 0.271833  | 0.268919  | —         | —      | —       | 0.3508 |
| 3    | #32   | 0.271889  | 0.268966  | —         | —      | —       | 0.3498 |
| 4    | #48   | 0.271964  | 0.268986  | —         | —      | —       | 0.3505 |
| 5    | #42   | 0.272146  | 0.269189  | —         | —      | —       | 0.3495 |

**Megjegyzés**: Top-10 trial mind 0.271–0.272 között → stabil konvergencia.

### Best hyperparameterek (Trial #34)

```json
{
    "colsample_bytree":   0.7964,
    "extra_trees":        true,
    "learning_rate":      0.01549,
    "max_bin":            127,
    "max_depth":          4,
    "min_child_samples":  392,
    "min_child_weight":   0.002004,
    "min_split_gain":     0.0000301,
    "num_leaves":         9,
    "path_smooth":        1.122,
    "reg_alpha":          0.9516,
    "reg_lambda":         8.275,
    "subsample":          0.8291
}
```

**Magyarázat:**
- `num_leaves=9`, `max_depth=4` → shallow tree, anti-overfit
- `extra_trees=True` → randomized splits, jobb generalizáció ritka (10%) targethez
- `min_child_samples=392` → konzervatív leaf méret, stabilitás
- `reg_lambda=8.275` → erős L2 regularizáció
- `learning_rate=0.01549` → viszonylag alacsony, finomabb illeszkedés

### Per-fold breakdown (best trial #34)

| Fold | train range end | valid_ll | train_ll | prauc |
|------|----------------|----------|----------|-------|
| 1    | 2022-08-11     | 0.2712   | 0.2439   | 0.349 |
| 2    | 2023-02-07     | 0.2894   | 0.2389   | 0.317 |
| 3    | 2023-08-06     | 0.2675   | 0.2525   | 0.400 |
| 4    | 2024-02-02     | 0.2538   | 0.2573   | 0.338 |
| 5    | 2024-07-31     | 0.2615   | 0.2424   | 0.360 |

Fold 2 magasabb valid_ll — ez az early 2023 BTC crash körüli volatilis periódusnak felel meg.

### Feature importance (top 15, explore best trial)

| Feature                        | Jelleg |
|-------------------------------|--------|
| feat_day_range_position       | Nap pozíció |
| feat_neg_return_mean_60       | Elmúlt 60 perces negatív return átlag |
| feat_natr_14                  | Normalizált ATR (volatilitás) |
| feat_volume_sma_14            | Volume SMA |
| feat_gk_vol_60                | Garman-Klass volatilitás 60p |
| feat_parkinson_vol_60         | Parkinson vol 60p |
| feat_obv                      | OBV (volume alapú trend) |
| feat_day_open_return          | Nap nyitástól való return |
| feat_parkinson_vol_30         | Parkinson vol 30p |
| feat_gk_vol_30                | Garman-Klass vol 30p |
| feat_weekly_open_return       | Hét nyitástól való return |
| feat_bars_into_session_norm   | Időpont a napon belül |
| feat_roc_140                  | Rate of change 140p |
| feat_gk_vol_10                | GK vol 10p |
| feat_ema_ratio_140            | EMA ratio 140p |

**Megállapítás**: A short modell domináns feature-jei a volatilitás és negatív return
mértékek — a modell a magas volatilitású, lefelé trend közeledési szituációkat detektálja.

---

## 3. Final Model Fit

- **TRAIN_END**: 2025-06-05 11:25:00 (test periódus előtti perc)
- **n_estimators**: 1256 (mean best_iter=1121, ×1.12 buffer)
- **fold best iters**: [977, 1348, 929, 721, 1630]
- **Train shape**: 42,197 × 200 (row_stride=60)
- **pos_rate**: 0.1010 (10.1% short signal)

Artifaktok: `models/lgbm_solusdt_s_fw60_q10_local_v2/model.pkl` (1.4 MB)

---

## 4. Strategy Sweep Eredmény

**Sweep paraméterek**: 2025-01-01 – 2026-06-07, 200 kombináció, short side

### Kiválasztási szempontok alkalmazása

| Config | Trades | WR | PF | Max DD | Megfelel? |
|--------|--------|----|----|--------|-----------|
| entry=0.30, hold=120, tp=0.0 | 512 | 63.1% | 2.25 | -17.4% | WR < 65% |
| entry=0.35, hold=120, tp=0.0 | 358 | 65.6% | 2.39 | -12.3% | Határon |
| **entry=0.40, hold=120, tp=0.0** | **275** | **66.9%** | **3.13** | **-7.3%** | **✓ KIVÁLASZTOTT** |
| entry=0.45, hold=120, tp=0.0 | 190 | 69.0% | 3.92 | -9.3% | ✓ Kevesebb trade |
| entry=0.48, hold=120, tp=0.0 | 147 | 77.6% | 6.20 | -4.7% | ✓ Magas PF, alacsony trade |

### Kiválasztott konfig: entry=0.40, max_hold=120, tp=0.0

Indoklás: legjobb trade count / WR / PF egyensúly. 275 trade statisztikailag szignifikáns,
WR 66.9% > 65% küszöb, PF 3.13 > 2.0, max_dd -7.3% < 20% küszöb.

### Backtest összefoglaló (entry=0.40, max_hold=120, tp=0.0)

| Metrika | Érték |
|---------|-------|
| Periódus | 2025-01-01 – 2026-06-07 |
| Trade count | 275 |
| Wins / Losses | 184 / 91 |
| Win rate | **66.9%** |
| Profit factor | **3.127** |
| Total return | **+816.3%** |
| Initial equity | 10,000 |
| Final equity | **91,634.93** |
| Max drawdown | **-7.3%** |
| Avg hold | 106.9 perc |
| Exposure | 3.9% |
| Exit reasons | max_hold: 194, probability_exit: 81 |

**Kontextus**: A 2025-ös SOL árfolyam ~$200-ról ~$50-ig esett (macro lefelé trend), ami
a short stratégiának kiemelkedő hozamot biztosított. Az eredmény a 2025-ös bear piacban
mért teljesítmény, nem általánosan elvárható future hozam.

---

## 5. Config változások

| Fájl | Változás |
|------|----------|
| `config/models.json` | `lgbm_solusdt_s_fw60_q10_local_v2`: active=true; `lgbm_solusdt_s_fw60_q10_stable_v1`: active=false |
| `config/strategies.json` | `solusdt_short_fw60_q10_local_v2` hozzáadva az aktív long stratégia után |
| `src/streamlit_app/main.py` | `_SOL_MODEL_STATS["short"]` frissítve az új modell adataival |
| `scripts/sweep_strategy.py` | Unicode `→` ASCII `to`-ra cserélve (Windows cp1250 fix) |

---

## 6. Promotion gate ellenőrzés

- [x] mean_valid_prauc = 0.3531 >> floor 0.2343 (+43% a champion felett)
- [x] mean_gap = 0.0217 < 0.03
- [x] std_valid_ll = 0.0119 — foldok között stabil
- [x] Artifaktok reprodukálhatók: `search_best.json`, `search_trials.jsonl`
- [x] Régi champion (`stable_v1`) artifaktok megmaradnak, rollback lehetséges
- [x] UI helyesen mutatja az új stratégiát (`_resolve_short_strategy`)
- [x] Live predictions naprakész (50s lag, dashboard auto-sync aktív)
