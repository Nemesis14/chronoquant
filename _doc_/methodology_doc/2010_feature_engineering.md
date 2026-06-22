# Feature Engineering — Moduláris analízis layer

**Script:** `src/modeling/01_feature_engineering.ipynb`
**Library:** `src/modeling/feature_engineering/`
**Output:** `database/<asset_id>/feature_engineering/<run_id>/`

---

## Célja

A `quant_train` DuckDB tábla `feat_*` oszlopait négy egymástól független dimenzió
mentén vizsgálja. Az eredmény egy determinisztikusan generált `feature_set.json`,
amelyet a `00_create_sample.py` és a `02_hyper_param_search.py` fogyaszt.

---

## Input

| Forrás | Tartalom |
|--------|----------|
| `quant_train` DuckDB tábla | `feat_*` oszlopok + `long_mfe_fw60`, `short_mfe_fw60` target oszlopok |
| `FeatureEngineeringConfig` | Küszöbértékek minden analízis lépéshez |

A `quant_train` tábla csak NULL-mentes target sorokat tartalmaz (a build pipeline
garantálja). Az első ~1441 sor `feat_*` értékei NULL-ok lehetnek (t-1 lag warmup),
de a sampling lookback offset kizárja ezeket a tanítási ablakból.

---

## Négy analízis lépés

### 1. Quality — `analyze_quality()`

Univariáns minőségi ellenőrzés minden `feat_*` oszlopra.

| Metrika | Döntés | Feltétel |
|---------|--------|----------|
| `null_rate` | drop | > `max_null_rate` (0.01) |
| `inf_rate` | drop | > `max_inf_rate` (0.001) |
| `variance` | drop | < `min_variance` (1e-8) |
| `outlier_ratio` | review | > `max_outlier_ratio` (0.05) |
| — | keep | minden más |

Output schema: `feature, null_rate, inf_rate, variance, outlier_ratio, decision, drop_reason`

### 2. Target Relation — `analyze_target_relation()`

Pearson (`CORR`) és Spearman (RANK-alapú) korreláció minden `feat_*` × target párra.
`signal_proxy = |ρ_spearman|`

| Döntés | Feltétel |
|--------|----------|
| `leakage` | `|ρ| > 0.95` — jövőbeli adat szivárgás gyanú |
| `weak` | `|ρ| < 0.01` — nincs érdemi szignál |
| `keep` | minden más |

Egy feature csak akkor kerül ki, ha **mindkét** targetre `weak` (vagy `leakage`).

Output schema: `feature, target, pearson_r, spearman_rho, signal_proxy, leakage_flag, decision`

### 3. Redundancy — `analyze_redundancy()`

Pearson korrelációs mátrix alapján klaszterezés (union-find algoritmus).
Ha két feature `|Pearson r| ≥ pearson_cluster_thr` (0.95), egy klaszterbe kerülnek.
Klaszterenként a legkisebb indexű feature a reprezentatív (`keep`), a többi `drop`.

A korrelációs mátrix max `redundancy_max_rows` (500 000) véletlenszerű sorból számolódik
RAM-hatékonyság érdekében.

Output schema: `feature, cluster_id, is_representative, max_pearson, decision, drop_reason`

### 4. Stability — `analyze_stability()`

Az adatot `stability_bucket_days` (90) napos, nem-átfedő időablakokra osztja.
Minden (feature, bucket) párra Spearman korreláció mindkét targettel.
`drift = |ρ_bucket − ρ_baseline|`

| Flag | Feltétel | Hatás a végeredményre |
|------|----------|-----------------------|
| `stable` | drift ≤ 0.15 | selected |
| `review` | 0.15 < drift ≤ 0.30 | review lista |
| `unstable` | drift > 0.30, nem az utolsó 2 bucket | review lista |
| `decayed` | drift > 0.30 **és** az utolsó 2 bucket | **drop** |

Output schema: `feature, bucket_idx, bucket_start, bucket_end, n, null_rate, mean, std, spearman_long, spearman_short, drift_long, drift_short, stability_flag`

---

## Output — notebook inline logika

A `feature_set.json` generálása **kizárólag** a `01_feature_engineering.ipynb`
utolsó output celláiban történik — nincs külön reporting modul.

Egy feature a **selected** listába kerül, ha mind a négy feltétel teljesül:
1. quality → `keep`
2. target_relation → legalább egy targetre `keep`
3. redundancy → `keep` (reprezentatív)
4. stability → nincs `decayed` bucket

### `feature_set.json` séma

```json
{
  "run_id": "run_20240601_120000",
  "asset_id": "solusdt",
  "created_at": "2024-06-01 12:00:00",
  "target_cols": ["long_mfe_fw60", "short_mfe_fw60"],
  "selected": ["feat_rsi_14", "feat_roc_10", ...],
  "dropped": [
    {"col": "feat_foo", "reason": "quality: null_rate=0.05 > max=0.01"}
  ],
  "review": ["feat_bar"],
  "thresholds": {
    "max_null_rate": 0.01,
    "max_inf_rate": 0.001,
    "min_variance": 1e-8,
    "max_outlier_ratio": 0.05,
    "min_spearman_abs": 0.01,
    "max_spearman_leakage": 0.95,
    "pearson_cluster_thr": 0.95,
    "redundancy_max_rows": 500000,
    "stability_bucket_days": 90,
    "max_drift_threshold": 0.3
  }
}
```

### `analyst_report.md`

Human-readable összefoglaló: selected / dropped / review listák, drop okok, paraméterek.

---

## Futtatás

```bash
# Jupyter notebook interaktívan
uv run jupyter notebook src/modeling/01_feature_engineering.ipynb
```

A notebook automatikusan generál `run_id`-t (`run_YYYYMMDD_HHMMSS` formátumban).
Az output path kiíródik a konzolra futás végén.

---

## Kapcsolódó fájlok

| Fájl | Tartalom |
|------|----------|
| `src/modeling/feature_engineering/__init__.py` | Publikus API |
| `src/modeling/feature_engineering/config.py` | `FeatureEngineeringConfig` |
| `src/modeling/feature_engineering/quality.py` | Quality analízis |
| `src/modeling/feature_engineering/target_relation.py` | Target relation analízis |
| `src/modeling/feature_engineering/redundancy.py` | Redundancia klaszterezés |
| `src/modeling/feature_engineering/stability.py` | Időbeli stabilitás |
| `src/modeling/feature_engineering/reporting.py` | Output generálás |
| `src/modeling/feature_engineering/tests/smoke/test_package.py` | Smoke tesztek |
| `_doc_/2000_features.md` | Feature layer metodológia (lookahead, t-1 lag, csoportok) |
| `_doc_/5000_modelling.md` | Modeling pipeline áttekintés |
