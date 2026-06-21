# Analyst Skill

Quarto-renderable elemzési notebookok elkészítésének végrehajtási útmutatója.
Notebook struktúra, vizuális design, kód-konvenciók és workflow.

Quarto config és label szintaxis: → `.agent/tools/quarto_analysis_defaults.md`

---

## Notebook Struktúra

```text
Raw cell       → Quarto frontmatter
Markdown cell  → ## Cél
Markdown cell  → ## <1. Szekció>
Code cell      → lekérdezés + display / plot
...
Markdown cell  → ## <N. Szekció>
Code cell      → lekérdezés + display / plot
Markdown cell  → ## <Értelmező záró szekció>
Code cell      → programmatikusan generált interpretáció / decision table / metrika összefoglaló
```

Nincs `Findings`, `Conclusion`, `Summary` szekció. Az eredmények közvetlenül a
code cell-ek outputjában jelennek meg táblaként és/vagy ábrán, majd a notebook
végén kötelező egy rövid, döntést támogató értelmező szekció.

---

## Quarto Frontmatter (Raw Cell)

Az **első cell** minden notebookban Raw cell, a teljes Quarto config-gal:

```yaml
---
title: "<Emberi cím, pl. Targets Tábla — Eloszlás és Minőség>"
subtitle: "<Sorszám és kontextus, pl. Elemzés 3200 | solusdt 1m>"
date: "<YYYY-MM-DD>"
format:
  html:
    theme: cosmo
    css: ../analyst/chronoquant_analysis.css
    toc: true
    toc-title: "Tartalom"
    toc-location: left
    toc-depth: 3
    toc-expand: 2
    number-sections: true
    page-layout: article
    smooth-scroll: true
    code-fold: true
    code-tools: true
    code-summary: "code"
    code-copy: true
    code-overflow: wrap
    df-print: paged
    fig-align: center
    fig-width: 10
    fig-height: 5
    fig-format: retina
    embed-resources: true
    self-contained: true
    date-format: "YYYY-MM-DD"
    link-external-newwindow: true
    grid:
      sidebar-width: 320px
      body-width: 900px
      margin-width: 200px
      gutter-width: 2rem
execute:
  enabled: true
  echo: true
  warning: false
  message: false
  freeze: false
---
```

Ne használj `# Heading`-et dokumentumcímként. A Quarto a frontmatter `title`-t
rendereli fejlécként.

---

## Cél Fejezet

Mindig az első markdown cell a Raw cell után. Kötelező mezők:

```markdown
## Cél

Ez a notebook a `<tábla/modul>` elemzését végzi <asset>, <granularitás> bontásban.
Vizsgált területek: <rövid felsorolás>.

**Kapcsolódó dokumentáció:**
- Módszertan: `_doc_/<XXXX>_<slug>.md`
- Kód-dokumentáció: `_doc_/<XXXX>_<slug>.md`

**Asset:** SOLUSDT | **Granularitás:** 1m | **Dátum:** YYYY-MM-DD
```

Ha a user döntési kérdést tesz fel (pl. mely éveket érdemes használni),
kötelező ezt már a célfejezetben expliciten megfogalmazni.

---

## Setup Cell

A Raw cell után azonnal következő code cell minden notebookban kötelező.
Importok és teljes seaborn téma együtt, egyetlen cellában:

```python
import sys
import duckdb
import pandas as pd
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from IPython.display import display, Markdown

sys.path.insert(0, str(_root))
from analyst.table_formatting import format_analysis_table, display_analysis_table
import analyst.plot_utils as pu
import analyst.db_utils as dbu

CQ_COLORS = {
    "blue":       "#1696d2",
    "black":      "#000000",
    "gray_dark":  "#353535",
    "gray":       "#696969",
    "gray_light": "#d2d2d2",
    "yellow":     "#fdbf11",
    "orange":     "#f15a24",
    "red":        "#ec008b",
}
CQ_SEQUENCE = [
    CQ_COLORS["blue"],
    CQ_COLORS["yellow"],
    CQ_COLORS["orange"],
    CQ_COLORS["gray"],
    CQ_COLORS["red"],
]

sns.set_theme(
    style="whitegrid",
    rc={
        "figure.figsize": (9, 5.5),
        "figure.dpi": 120,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.edgecolor": CQ_COLORS["gray"],
        "axes.labelcolor": CQ_COLORS["gray_dark"],
        "xtick.color": CQ_COLORS["gray_dark"],
        "ytick.color": CQ_COLORS["gray_dark"],
        "grid.color": CQ_COLORS["gray_light"],
        "grid.linewidth": 0.8,
        "axes.axisbelow": True,
        "legend.frameon": False,
    },
)
sns.set_palette(CQ_SEQUENCE)
```

Ha van notebook-specifikus helper, itt importáld.

---

## Palette — Szín Szemantika

| Szín | Hex | Mikor |
|------|-----|-------|
| blue | `#1696d2` | fő mérési sorozat |
| gray / black | `#696969` / `#000000` | összehasonlítás, benchmark |
| yellow / orange | `#fdbf11` / `#f15a24` | másodlagos összehasonlítás |
| red | `#ec008b` | figyelmeztetés, hiba, kizárt szegmens |
| gray_light | `#d2d2d2` | semleges háttér, sávok |

Egy notebookon belül következetes szín-szemantika kötelező — szomszédos chartokban ne rendelj ugyanahhoz a színhez különböző jelentést.

---

## Szekció Cell-Minta

Minden elemzési szekció három részből áll:

### 1. Markdown leírás cell

```markdown
## <Szekció cím>

**Mi ez.** <Mit vizsgál ez a lekérdezés / ábra.>

**Forrás.** `<tábla>` tábla, `<oszlopok>` oszlopok.

**Módszer.** <Milyen aggregáció, split, szűrés, validáció vagy transzformáció történik.>

**Értelmezés.** <Mit jelent az eredmény. Milyen eltérés, kapcsolat vagy kockázat számít fontosnak.>
```

Minden mező külön bekezdés.

### 2. Code cell — lekérdezés és megjelenítés

```python
#| label: tbl-<kebab-case-nev>
#| tbl-cap: "Emberi felirat"

display_analysis_table(df)
```

```python
#| label: fig-<kebab-case-nev>
#| fig-cap: "Emberi felirat"
#| fig-alt: "Leírás"

fig, ax = plt.subplots()
sns.lineplot(data=df, x="x", y="y", ax=ax, color=CQ_COLORS["blue"])
ax.set_xlabel("...")
ax.set_ylabel("...")
plt.show()
```

### 3. Szabályok

- Minden szekció **legalább egy táblát vagy ábrát** tartalmaz
- Tábla: mindig `display_analysis_table(df)` — nincs bare `df`, `display(df)`, `df.head()`
- Ábra: mindig `plt.show()` a cell végén
- `ax.set_title()` tilos
- `print()` tilos — használj táblát, ábrát vagy `display(Markdown(...))`-t
- Minden fontos output után legyen közeli, programmatikusan generált rövid olvasat

---

## Chart Rules

- seaborn az elsődleges könyvtár; matplotlib a fallback és finomhangolás
- Plotly tilos, kivéve ha a user explicit interaktív HTML-t kér
- Minden charthoz legyen forrás dataframe vagy summary table
- Következtetések számított adatból folyjanak, ne csak vizuális benyomásból
- Tengelyfeliratok egységgel vagy skála kontextussal
- Top és right spine eltávolítva
- Könnyű gridlines az értéktengelyen
- Direct labels vagy kompakt legend

---

## Analysis-Driven Chart Selection

A kérdés dönti el a chart típust, nem a megszokás:

- **Időbeli alakulás:** line chart, rolling chart, éves overlay vagy faceted time series
- **Éves összehasonlítás:** külön panelek vagy aligned overlays, azonos skálák
- **Eloszlás:** histogram, KDE, box, violin, ECDF, quantile range, density comparison
- **Model performance:** train-valid metrics table + calibration vagy prediction-vs-target view
- **Seasonality:** monthly/quarterly aggregation, aligned panelek vagy heatmap
- **Split periódusok:** shading, bands, vagy explicit legend train/valid/OOS/regime-hez

Ha a user azt kérdezi, összehasonlíthatók-e évek vagy periódusok: tedd vizuálisan és numerikusan egyértelművé.

---

## Temporal And Split Presentation Rules

- Ha a chart több időszegmenst kever: különböztess meg shading-gel, facet panellel vagy stabil szín-szemantikával
- Ha train és valid periódusok fontosak: jelöld az időtengelyen vagy szeparáld panelekre
- Éves összehasonlításnál tartsd az y-tengelyeket azonos skálán
- Időrendezett multi-panel layouthoz `layout-ncol: 1` előnyben, hacsak egymás melletti nézet nem olvashatóbb
- Ha csonka évet hasonlítasz teljes évekhez: jelezd explicit szövegben

---

## Caption Examples

```python
# Egyetlen tábla
#| label: tbl-positive-rate-by-year
#| tbl-cap: "Positive target rate by year"

display_analysis_table(yearly)
```

```python
# Egyetlen ábra
#| label: fig-positive-rate-by-year
#| fig-cap: "Positive target rate by year"
#| fig-alt: "Line chart showing yearly positive target rate."

fig, ax = plt.subplots(figsize=(9, 5.5))
sns.lineplot(data=yearly, x="year", y="positive_rate", ax=ax, color=CQ_COLORS["blue"])
ax.axhline(0.08, linestyle="--", linewidth=1, color=CQ_COLORS["gray"])
ax.set_xlabel("Year")
ax.set_ylabel("Positive rate")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=2))
plt.show()
```

```python
# Multi-panel (Quarto layout)
#| label: fig-train-valid-panel
#| fig-cap: "Train-valid comparison"
#| fig-subcap:
#|   - "Train sample"
#|   - "Validation sample"
#| layout-ncol: 2

fig, ax = plt.subplots()
...
plt.show()

fig, ax = plt.subplots()
...
plt.show()
```

Quarto panel layout előnyben ha: train vs valid páros chartok, before vs after összehasonlítások,
ugyanaz a metrika több spliten vagy horizonton.

---

## Numeric Formatting

| Column type | Display format |
|---|---|
| `year` | Egész szám, pl. `2024` |
| `count`, `n`, `row_count`, `rows`, `trades`, `volume`, `*_count`, `*_n` | Egész, nulla tizedesjegy |
| `rate`, `ratio`, `share`, `pct`, `percent` | Percent string, 2 tizedesjegy; auto ×100 ha érték ≤ 1 |
| minden más float | 3 tizedesjegy string |

---

## Model és Target Kiértékelési Szabályok

Ha a notebook targetet, modellt vagy predikciót elemez:

**Mit vizsgálj (ha az adat elérhető):**
- Különítsd el a train és valid / in-sample és out-of-sample halmazt
- Mutasd meg az időbeli alakulást és a regime-váltásokat
- Mutasd meg az eloszlási különbségeket évente vagy szegmensenként
- Mutasd meg a predikció és a fact target kapcsolatát
- Záró állásfoglalás: a teljes időszak használható-e együtt, vagy vannak kizárandó évek

**Ha nincs kész `segment` mező:** rekonstruáld foldból, manifestből vagy metadata-ból.
**Ne használj leakage-gyanús vagy full-fit predikciót** validációs következtetésekre.

**Preferált vizualizációk:**
- Train vs valid metric table: `RMSE`, `MAE`, `R²` és sample counts
- Prediction vs fact scatter ideális reference line-nal
- Binned calibration chart: átlag predikció vs átlag fact
- Residual summary table vagy residual distribution
- Daily/periodic aggregation chart ha regime-követés fontos

---

## Értelmező Zárás

Minden notebook végén kötelező egy külön szekció.
Lehetséges cím: `## Értelmezés`, `## Modellolvasat`, `## Döntési szempontok`

**Minőségi elvárások:**
- Ne ismételd meg szó szerint a táblákat
- Állást kell foglalni — ne állj meg a "látható különbségnél"; számszerűsítsd ha az adat engedi
- A következtetés a futott számokra és ábrákra támaszkodjon
- Ha a user döntést akar hozni: adj döntést támogató választ, ne csak összefoglalót
- Ha az eredmény vegyes: mondd meg mi erős és mi gyenge
- Ha a valid gyengébb mint a train: mondd meg, a degradáció elfogadható-e

**Tipikus kérdések amikre válaszolni kell:**
- Mennyire különböznek az évek egymástól?
- Elég egységesek-e az évek a modellhez?
- Vannak-e kizárandó évek vagy rezsimek?
- A modell rangsorolásra vagy abszolút becslésre alkalmas inkább?
- A valid teljesítmény elég stabil-e?

---

## Tiltott Placeholder Szöveg

Soha ne írj:
- `futtatás után kitöltendő`
- `to be filled after running`
- `TODO after execution`
- üres értelmezést vagy üres záró szekciót

Ha az eredmény a futtatástól függ, generáld programmatikusan.

---

## Futtatás és Renderelés

```bash
quarto render _doc_\XXXX_<slug>.ipynb --execute
```

1. Írd meg az összes markdown és code cell-t
2. Minden eredmény programmatikusan generált legyen
3. Futtasd le a notebookot tiszta kernelből
4. Rendereld Quarto-val
5. Ellenőrizd, hogy a HTML létrejött
6. Olvasd vissza a fő outputokat
7. Ha kell, javítsd a notebookot, futtasd újra, és csak utána add át

---

## QA Checklist

- [ ] Raw cell: teljes Quarto frontmatter
- [ ] Setup cell: importok + seaborn theme + CQ_COLORS + table helper — egyetlen cellában
- [ ] Minden szekció: markdown (mi/forrás/módszer/értelmezés) → code → tábla/ábra
- [ ] `display_analysis_table(df)` minden tábla cell végén — nincs bare `df`, `display(df)`, `df.head()`
- [ ] Minden tábla: `#| label: tbl-...` és `#| tbl-cap: ...`
- [ ] Minden ábra: `#| label: fig-...`, `#| fig-cap: ...`, `#| fig-alt: ...`
- [ ] `plt.show()` minden plot cell végén
- [ ] Nincs `ax.set_title()`
- [ ] Nincs pandas index oszlop a rendered HTML táblákban
- [ ] Numeric formatting projekt konvenciókat követ
- [ ] Train-valid / in-sample-OOS / éves összehasonlítás vizuálisan jelölve, ha releváns
- [ ] Nincs placeholder szöveg
- [ ] Notebook tiszta kernelből lefuttatva
- [ ] Quarto render sikeres, HTML létezik `_doc_/<slug>.html` helyen
- [ ] Fő outputok visszaolvasva és ellenőrizve
- [ ] Notebook végén decision-oriented értelmezés a futott eredmények alapján
