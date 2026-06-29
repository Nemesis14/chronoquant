# 8200 — Analyst Template Rendszer

Forrás: `analyst/lib/` — megosztott Python segédmodulok az analysis notebookokhoz.

## Modulok

### `analyst/lib/plot_utils.py`

| Szimbólum | Típus | Leírás |
|-----------|-------|--------|
| `CQ_COLORS` | `dict[str, str]` | ChronoQuant notebook paletta (8 névesített szín: blue, black, gray_dark, gray, gray_light, yellow, orange, red) |
| `CQ_SEQUENCE` | `list[str]` | 5 elemű default plot sorrend a CQ_COLORS-ból |
| `setup_cq_theme()` | `-> None` | sns.set_theme whitegrid + CQ rc konfig + CQ_SEQUENCE palette — hívd a Setup cellában |
| `apply_theme()` | `-> None` | Alternatív UI-stílusú téma (Bootstrap színek, szélesebb fig) — dashboard és UI notebookokhoz |
| `style_df(df, ...)` | `-> Styler` | Pandas DataFrame Quarto-kompatibilis HTML stílusozás |
| `display_table(df, ...)` | `-> None` | Stílusozott DataFrame megjelenítés caption-nal |
| `timeseries(...)` | `-> (fig, ax)` | Idősor line plot |
| `bar_monthly(...)` | `-> (fig, ax)` | Havi bar chart |
| `kde_by_group(...)` | `-> (fig, ax)` | KDE csoportonként |
| `quarterly_boxplot(...)` | `-> (fig, ax)` | Negyedéves boxplot |
| `dual_axis(...)` | `-> (fig, ax1, ax2)` | Kéttengelyes plot |
| `stacked_panels(n, ...)` | `-> (fig, axes)` | N darab függőleges panel |
| `hbar_features(...)` | `-> (fig, ax)` | Vízszintes feature importance bar |

### `analyst/lib/db_utils.py`

| Szimbólum | Típus | Leírás |
|-----------|-------|--------|
| `find_repo_root()` | `-> Path` | Projekt root megkeresése `pyproject.toml` alapján — notebook setup cellában a `_root` változóhoz |
| `db_path()` | `-> str` | Live DuckDB útvonal (`solusdt.duckdb`) az asset config-ból |
| `lab_db_path()` | `-> str` | Lab DuckDB útvonal (`solusdt_lab.duckdb`) — levezetett a `db_path()`-ból |
| `connect(read_only)` | `-> Connection` | Live DuckDB kapcsolat |
| `lab_connect(read_only)` | `-> Connection` | Lab DuckDB kapcsolat (snap/model/strat sémák) |
| `load_table(table, ...)` | `-> DataFrame` | Tábla betöltés date-range szűréssel |
| `table_stats_df()` | `-> DataFrame` | Core táblák státusz összefoglalója |
| `monthly_agg(...)` | `-> DataFrame` | Havi aggregáció egy oszlopra |

### `analyst/lib/table_formatting.py`

| Szimbólum | Leírás |
|-----------|--------|
| `display_analysis_table(df)` | Quarto-kompatibilis táblázat megjelenítés |
| `analysis_table_html(df)` | HTML string output (asis cellákhoz) |
| `format_analysis_table(df)` | Styler object (manuális megjelenítéshez) |

---

## Analysis Notebook Template Konvenció

Minden modellhez (`artifacts/<model_id>/analysis/`) 4 standard notebook készül:

| Notebook | Cél |
|----------|-----|
| `01_sampling.ipynb` | Train/valid szétválasztás, target eloszlás, top10 decilis elemzés |
| `02_feature_engineering.ipynb` | Feature IV, MI, bin scatter — optbinning alapú feature szelekció |
| `03_hyperparameter_search.ipynb` | Optuna search review, trial progressz, paraméter hatás |
| `04_strategy.ipynb` | Strategy signal vizualizáció — napi/heti trigger ábrák az érvényes periódusra |

### HTML output elhelyezése

A renderelt HTML-ek a modell artifact root-jában élnek:
```
artifacts/<model_id>/
  01_sampling.html
  02_feature_engineering.html
  03_hyperparameter_search.html
  04_strategy.html
  analysis/
    01_sampling.ipynb
    02_feature_engineering.ipynb
    03_hyperparameter_search.ipynb
    04_strategy.ipynb
```

---

## Új modell notebookjainak elkészítése

1. Másold a legfrissebb champion modell mind a 4 notebookját az `analysis/` mappából.
2. Módosítsd a **Parameters cellát** (első kód cella minden notebookban):
   - `01_sampling.ipynb`, `02_fe.ipynb`: `MODEL_ID`, `DIRECTION` (`"l"` vagy `"s"`), `TARGET`
   - `03_search.ipynb`: `model_id` (papermill paraméter)
   - `04_strategy.ipynb`: `MODEL_ID`, `DIRECTION` (`"long"` vagy `"short"`)
3. Short modelleknél figyelj a direction-specifikus logikára a `04_strategy.ipynb`-ben:
   - Entry condition: `(1.0 - df[_pct_col]) >= ENTRY_CUTOFF` (invertált)
   - TP check: `future["low"] <= tp_price` (ár csökken)
   - TP_LOG: negatív értékű bucket_median_mfe a bottom percentile bucket-ből
4. Renderd a notebookokat Quarto-val: `quarto render <notebook>.ipynb --to html`
5. Másold a HTML outputot a modell artifact root-jába.

---

## Import minta (Setup cella template)

```python
import sys
import duckdb
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

from analyst.lib.db_utils import find_repo_root, lab_db_path, lab_connect
from analyst.lib.plot_utils import CQ_COLORS, CQ_SEQUENCE, setup_cq_theme
from analyst.lib.table_formatting import display_analysis_table, analysis_table_html

_root = find_repo_root()
sys.path.insert(0, str(_root))
sys.path.insert(0, str(_root / "src"))
setup_cq_theme()

# Model paraméterek (Parameters cellából)
LAB_DB = lab_db_path()
```

---

## Kapcsolódó fájlok

| Fájl | Tartalom |
|------|----------|
| `analyst/lib/plot_utils.py` | Plot stílus, DataFrame display |
| `analyst/lib/db_utils.py` | DB kapcsolatok, query helper-ek |
| `analyst/lib/table_formatting.py` | HTML tábla formázás |
| `analyst/quarto/_quarto.yml` | Quarto projekt konfig |
| `analyst/quarto/chronoquant_analysis.css` | Custom CSS az HTML notebookokhoz |
