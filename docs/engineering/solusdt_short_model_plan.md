# SOLUSDT Short Model — Implementation Plan

Párhuzamos terv a már meglévő SOLUSDT long modellel (`fw60_q90`).  
Short target: **`trg_s_fw60_q10`** — 60 bares előre tekintő ablakban a 10. percentilis alatti elmozdulás (lefelé).

---

## Összefüggések

| Dimenzió | Long (meglévő) | Short (tervezett) |
|---|---|---|
| Target col | `trg_l_fw60_q90` | `trg_s_fw60_q10` |
| Irány | felfelé | lefelé |
| Percentilis | 90. | 10. |
| Asset | `solusdt_fw60` | `solusdt_fw60` |
| Forward window | 60 bar (1h) | 60 bar (1h) |
| Modell naming | `*_l_fw60_q90_*` | `*_s_fw60_q10_*` |

---

## Task 1 — Target hozzáadása a features profile-hoz

**Fájl:** `config/features.json`

A `solusdt_fw60` asset profile `targets` listájához hozzá kell adni:

```json
{
  "col": "trg_s_fw60_q10",
  "direction": "short",
  "forward_window": 60,
  "quantile": 0.10
}
```

A BCH-nál (`bchusdt_fw240`) ez pontosan ugyanígy van `trg_s_fw240_q10` névvel — ugyanazt a struktúrát kell követni.

---

## Task 2 — 3 short modell config hozzáadása

**Fájl:** `config/models.json`

A BCH short modellek (`logit_s_fw240_q10_pval_v1`, `logit_s_fw240_q10_l1_v1`, `lgbm_s_fw240_q10_stable_v1`) struktúráját kell lemásolni és adaptálni.

### 2a. `logit_solusdt_s_fw60_q10_pval_v1`
- trainer: `statsmodels_pvalue_logreg`
- target: `trg_s_fw60_q10`
- asset_id: `solusdt_fw60`
- sample_id: `base_solusdt_fw60_dev`
- active: `false` (training után döntés)
- Hyperparams: p_value_threshold 0.05, max_iter 200 (BCH default-ot követni)

### 2b. `logit_solusdt_s_fw60_q10_l1_v1`
- trainer: `sklearn_lasso_logreg`
- target: `trg_s_fw60_q10`
- asset_id: `solusdt_fw60`
- sample_id: `base_solusdt_fw60_dev`
- active: `false`
- Hyperparams: alpha sweep [0.001, 0.01, 0.1, 1.0, 10.0], max_iter 1000

### 2c. `lgbm_solusdt_s_fw60_q10_stable_v1`
- trainer: `lightgbm_binary`
- target: `trg_s_fw60_q10`
- asset_id: `solusdt_fw60`
- sample_id: `base_solusdt_fw60_dev`
- active: `false`
- Hyperparams: num_leaves sweep [15, 31, 63], n_estimators 300, learning_rate 0.05

---

## Task 3 — Derived tables újraépítése (short target generálása)

Az `solusdt_fw60` features táblában még nincs `trg_s_fw60_q10` oszlop.  
Task 1 után újra kell futtatni:

```bash
python scripts/rebuild_derived_tables.py --features-only --asset-id solusdt_fw60
```

Ez hozzáadja a new target oszlopot a meglévő OHLCV adatokból anélkül, hogy az OHLCV szinkronizációt újra kellene futtatni.

---

## Task 4 — Sample splits (már megvan, ellenőrzés szükséges)

A `base_solusdt_fw60_dev` sample split valószínűleg már létezik (a long modell training-hez lett létrehozva).  
Ellenőrizni: `samples/base_solusdt_fw60_dev/` létezik-e.

Ha nem:
```bash
python scripts/create_sample_splits.py --sample-id base_solusdt_fw60_dev --asset-id solusdt_fw60 --target-horizon-minutes 60
```

A short modellekhez **ugyanaz a sample** használható mint a longnál — a target oszlop különbözik, a split logika nem.

---

## Task 5 — 3 modell betanítása

A Task 2–4 elvégzése után sorban:

```bash
python scripts/train_model.py --model-id logit_solusdt_s_fw60_q10_pval_v1
python scripts/train_model.py --model-id logit_solusdt_s_fw60_q10_l1_v1
python scripts/train_model.py --model-id lgbm_solusdt_s_fw60_q10_stable_v1
```

Outputok: `models/<model_id>/model.pkl`, `features.json`, `report.html`.

---

## Task 6 — Modell összehasonlítás és aktív modell kiválasztása

A long modell összevetéshez (`docs/analysis/solusdt_1h_model_comparison.md`) hasonlóan:

- ROC-AUC, PR-AUC, Brier score a 3 short modellre
- Kiválasztani a legjobb short modellt → `active: true` a config-ban
- Dokumentálni: `docs/analysis/solusdt_1h_short_model_comparison.md`

---

## Task 7 — Backtest strategy a short modellhez

**Fájl:** `config/strategies.json`

Új strategy: `solusdt_short_fw60_q10_managed_v1`

```json
{
  "strategy_id": "solusdt_short_fw60_q10_managed_v1",
  "model_id": "<kiválasztott aktív short modell>",
  "asset_id": "solusdt_fw60",
  "direction": "short",
  "entry_threshold": 0.5,
  "exit_threshold": 0.3,
  "take_profit_pct": 0.025,
  "stop_loss_pct": 0.015,
  "trailing_stop_pct": null,
  "max_hold_bars": 120,
  "fee_pct": 0.001
}
```

Futtatás:
```bash
python scripts/backtest_strategy.py solusdt_short_fw60_q10_managed_v1
```

---

## Task 8 — UI: Chart layout átrendezése

**Fájl:** `src/streamlit_app/components/charts.py`

### Jelenlegi layout
```
[price/candle chart — 70%]
[long prediction panel — 30%]
```

### Új layout
```
[long prediction panel  — 20%]
[price/candle chart     — 60%]
[short prediction panel — 20%]
```

Változtatások a `prediction_price_figure()` függvényben:
- `make_subplots(rows=3, ...)` — 3 sor
- `row_heights=[0.20, 0.60, 0.20]`
- Candlestick → `row=2`
- Long prediction trace → `row=1`
- Short prediction trace → `row=3`
- Threshold vonalak: long thresholdok `row=1`-re, short thresholdok `row=3`-ra
- Y tengelyek frissítése mindhárom sorra
- `_add_threshold_panel_legend` hívások mindkét prediction panelre

A függvény szignatúrája bővül:
```python
def prediction_price_figure(
    df: pd.DataFrame,
    # long thresholdok (meglévő)
    entry_threshold: float | None = None,
    rearm_threshold: float | None = None,
    exit_threshold: float | None = None,
    # short thresholdok (új)
    short_entry_threshold: float | None = None,
    short_rearm_threshold: float | None = None,
    short_exit_threshold: float | None = None,
):
```

**Streamlit hívások frissítése:** `src/streamlit_app/main.py` — a `prediction_price_figure()` híváshoz hozzáadni a short threshold paramétereket (az aktív short strategy config-ból tölteni).

**`data.py` bővítése:** a short modell predikciós oszlopát is lekérdezni (`{short_model_id}_p`), és a df-ben `short_prediction` névvel átadni.

---

## Task 9 — UI: Belépési jelek a candlestick barchart-on

**Fájl:** `src/streamlit_app/components/charts.py`

A közép-panel (candle chart, `row=2`) kiegészítése historikus belépési jelzőkkel.

### Logika
- **Zöld felfelé mutató háromszög** a bar tetején: ha az adott baren a long prediction >= long `entry_threshold`
- **Piros lefelé mutató háromszög** a bar tetején: ha az adott baren a short prediction >= short `entry_threshold`

### Implementáció — 2 új Scatter trace a `row=2`-re

```python
# Long entry signals
long_signals = plot_df[plot_df["prediction"] >= entry_threshold]
fig.add_trace(
    go.Scatter(
        x=long_signals["open_time"],
        y=long_signals["high"] * 1.001,  # bar teteje fölé kicsit
        mode="markers",
        name="long signal",
        marker={
            "symbol": "triangle-up",
            "color": "#16a34a",
            "size": 9,
            "line": {"width": 0},
        },
        hovertemplate="long signal<br>p=%{customdata:.3f}<extra></extra>",
        customdata=long_signals["prediction"],
        showlegend=False,
    ),
    row=2, col=1,
)

# Short entry signals
short_signals = plot_df[plot_df["short_prediction"] >= short_entry_threshold]
fig.add_trace(
    go.Scatter(
        x=short_signals["open_time"],
        y=short_signals["high"] * 1.001,
        mode="markers",
        name="short signal",
        marker={
            "symbol": "triangle-down",
            "color": "#dc2626",
            "size": 9,
            "line": {"width": 0},
        },
        hovertemplate="short signal<br>p=%{customdata:.3f}<extra></extra>",
        customdata=short_signals["short_prediction"],
        showlegend=False,
    ),
    row=2, col=1,
)
```

**Megjegyzés:** a jelzők csak akkor jelennek meg, ha a megfelelő prediction oszlop szerepel a df-ben és az entry_threshold be van állítva — mindkét irányban None-safe kezelés szükséges.

---

## Task 10 — Predictions szinkronizáció bővítése

**Fájl:** `src/data_pipeline/sync_predictions.py`

Jelenleg csak az `env.json`-ban megadott aktív modell predikcióját szinkronizálja.  
Ha a dashboard egyszerre mutatja a long ÉS short predikciót, mindkét modell live predikcióját fenn kell tartani.

Vizsgálat szükséges: a `sync_predictions.py` tud-e több modellt egyszerre szinkronizálni, vagy külön kell hívni.  
Ha szükséges: az `env.json`-t vagy a sync pipeline-t bővíteni, hogy mindkét aktív modell (`active: true` és az adott asset-hez tartozó) predikcióját frissítse.

---

## Végrehajtási sorrend

```
Task 1  → config/features.json (short target hozzáadása)
Task 2  → config/models.json (3 short modell config)
Task 3  → rebuild_derived_tables (short target kiszámítása)
Task 4  → sample splits ellenőrzés
Task 5  → 3 modell training
Task 6  → összehasonlítás, aktív modell kiválasztása
Task 7  → backtest strategy
Task 8  → UI chart layout (3 panel)
Task 9  → UI entry signal háromszögek
Task 10 → sync pipeline bővítés (ha szükséges)
```

Tasks 1–2 párhuzamosan elvégezhető.  
Task 3 blokkol Task 5-re.  
Tasks 8–9 elvégezhetők Task 6 előtt is (dummy/None short adatokkal tesztelve).
