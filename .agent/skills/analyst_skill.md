# Analyst Skill

Quarto-renderable elemzési notebookok elkészítésének végrehajtási útmutatója.
Notebook struktúra, cell-minták, kód-konvenciók és workflow.

---

## Notebook Struktúra

```
Raw cell       → Quarto frontmatter
Markdown cell  → ## Cél
Markdown cell  → ## <1. Szekció>
Code cell      → lekérdezés + display / plot
...
Markdown cell  → ## <N. Szekció>
Code cell      → lekérdezés + display / plot
```

Nincs `Findings`, `Conclusion`, `Summary` szekció. Az eredmények közvetlenül a
code cell-ek outputjában jelennek meg táblaként és/vagy ábrán.

---

## Quarto Frontmatter (Raw cell)

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

Ne használj `# Heading`-et dokumentumcímként — a Quarto a frontmatter `title`-t
rendereli fejlécként. Redundáns `#` fejléc duplikált látható címet okoz.

---

## Cél Fejezet

Mindig az első markdown cell (Raw cell után). Kötelező mezők:

```markdown
## Cél

Ez a notebook a `<tábla/modul>` elemzését végzi <asset>, <granularitás> bontásban.
Vizsgált területek: <rövid felsorolás>.

**Kapcsolódó dokumentáció:**
- Módszertan: `_doc_/<XXXX>_<slug>.md`
- Kód-dokumentáció: `_doc_/<XXXX>_<slug>.md`

**Asset:** SOLUSDT | **Granularitás:** 1m | **Dátum:** YYYY-MM-DD
```

---

## Szekció Cell-Minta

Minden elemzési szekció három részből áll:

### 1. Markdown leírás cell

```markdown
## <Szekció cím>

**Mi ez.** <Mit vizsgál ez a lekérdezés / ábra.>

**Forrás.** `<tábla>` tábla, `<oszlopok>` oszlopok.

**Értelmezés.** <Mit jelent az eredmény. Milyen érték az elfogadható.>
```

Minden mező külön bekezdés (üres sorral elválasztva), hogy Quarto külön sorokra
rendereli őket.

### 2. Code cell — lekérdezés és megjelenítés

```python
#| label: tbl-<kebab-case-nev>        # tábla esetén
#| tbl-cap: "Emberi felirat"

import duckdb, pandas as pd

con = duckdb.connect(db_path, read_only=True)
df = con.execute("""
    SELECT ...
    FROM ...
    WHERE ...
""").df()
con.close()

display_analysis_table(df)
```

```python
#| label: fig-<kebab-case-nev>        # ábra esetén
#| fig-cap: "Emberi felirat"
#| fig-alt: "Leírás"

fig, ax = plt.subplots()
sns.barplot(data=df, x="col", y="val", ax=ax, color=CQ_COLORS["blue"])
ax.set_xlabel("...")
ax.set_ylabel("...")
plt.show()
```

### 3. Szabályok

- Minden szekció **legalább táblát vagy ábrát** tartalmaz — mindkettő is lehet.
- Tábla: mindig `display_analysis_table(df)` — soha ne a bare `df` vagy `display(df)`.
- Ábra: mindig `plt.show()` a cell végén.
- `ax.set_title()` tilos — a caption-t Quarto generálja a `fig-cap`-ből.
- Nincs `print()` — csak tábla, ábra, vagy `display(Markdown(...))`.

---

## Setup Cell

A Raw cell után azonnal következő code cell — minden notebook-ban kötelező:

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

# Ha van notebook-specifikus helper:
# from analyst.XXXX_helpers import ...

# Adatbázis path — utils-on keresztül, ne hardcode
import utils
db_path = utils.load_asset_config()["database"]["db_path"]

# Design standard (részletek: analysis_presentation_skill.md)
CQ_COLORS = {
    "blue": "#1696d2",
    "black": "#000000",
    "gray_dark": "#353535",
    "gray": "#696969",
    "gray_light": "#d2d2d2",
    "yellow": "#fdbf11",
    "orange": "#f15a24",
    "red": "#ec008b",
}
CQ_SEQUENCE = [
    CQ_COLORS["blue"], CQ_COLORS["yellow"], CQ_COLORS["orange"],
    CQ_COLORS["gray"], CQ_COLORS["red"],
]

sns.set_theme(
    style="whitegrid",
    rc={
        "figure.figsize": (10, 5),
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

---

## Kód Stílus

- **Adat-hozzáférés:** DuckDB-first — SQL szűrésre, join-ra, aggregációra, window függvényekre.
- **Nagy in-memory transform:** Polars.
- **Display inputok:** pandas, kis végső dataframe-ekre.
- **Charting:** seaborn (elsődleges); matplotlib fallback/customization (axis formatters, referencia-vonalak).
- `duckdb.connect(db_path, read_only=True)` — soha ne írj az adatbázisba.
- Plotly tilos, kivéve ha a user explicit interaktív HTML-t kér.

---

## Tábla-megjelenítési Szabályok

Minden tábla cell **kötelezően** `display_analysis_table(df)`-fel zárul.

**Tilos:**
- `df` bare variable a cell végén
- `display(df)` vagy `display(df.head())`
- `df.style` `format_analysis_table` nélkül
- `df.head()` mint utolsó kifejezés

**Numerikus formázás:**

| Oszlop típus | Megjelenítés |
|---|---|
| `year` | Egész, pl. `2024` — soha nem `2,024` vagy `2024.0` |
| count, n, rows, violations, *_count, *_n | Egész, nulla tizedes |
| rate, ratio, share, pct, percent | Százalék string, 2 tizedes, pl. `23.24%`; ×100 auto ha érték ≤ 1 |
| többi float | 3 tizedes string, pl. `1.234` |

`analyst/table_formatting.py` — kanonikus implementáció. Ha még nem létezik,
hozd létre a `analysis_presentation_skill.md`-ben dokumentált kóddal, és tedd be
az `analyst/` könyvtárba.

---

## Segédmodulok

Újrafelhasználható logika: `analyst/XXXX_<name>.py`
ahol `XXXX` = a notebook sorszáma.

- Hozz létre helper-t ha ugyanaz a logika 2+ szekciót is érintene.
- Minden publikus függvény típusannotált, Google-style docstring-gel (`coding_skill.md`).
- Soha ne írjon DuckDB-be, ne módosítson produkciós táblát.

Import a Setup cell-ben:

```python
sys.path.insert(0, str(_root))
from analyst.XXXX_helpers import compute_target_distribution
```

---

## Tiltott Placeholder Szöveg

Soha ne írj:

- `futtatás után kitöltendő`
- `to be filled after running`
- `TODO after execution`
- üres finding vagy értelmezés szekciót

Ha az eredmény a futtatástól függ, generáld programmatikusan:

```python
display(Markdown(f"**Eredmény:** {violations:,} sor sérti a feltételt."))
```

---

## Futtatás és Renderelés

```bash
quarto render _doc_\XXXX_<slug>.ipynb --execute
```

1. Írd meg az összes markdown és code cell-t.
2. Minden eredmény programmatikusan generált — nincs placeholder.
3. Futtasd le a notebookot tiszta kernel-ből.
4. `quarto render _doc_\XXXX_<slug>.ipynb --execute`
5. Ellenőrizd: a HTML létezik `_doc_/XXXX_<slug>.html`-ként.
6. Ellenőrizd: nincs tiltott placeholder a rendered outputban.
7. Ellenőrizd: minden tábla és ábra cell rendelkezik `#| label:` és caption tag-gel.

---

## Rendering QA Checklist

- [ ] Raw cell: teljes Quarto frontmatter (cím, CSS, format config)
- [ ] Setup cell: CQ_COLORS, sns.set_theme, import-ok, db_path
- [ ] Minden szekció: markdown leírás → code → tábla/ábra
- [ ] `display_analysis_table(df)` minden tábla cell végén
- [ ] Nincs bare `df`, `display(df)`, `df.head()` a cell végén
- [ ] Minden tábla: `#| label: tbl-...` és `#| tbl-cap: ...`
- [ ] Minden ábra: `#| label: fig-...` és `#| fig-cap: ...`
- [ ] `plt.show()` minden plot cell végén
- [ ] Nincs `ax.set_title()`
- [ ] Nincs placeholder szöveg
- [ ] Numerikus formázás konvenciónak megfelelő
- [ ] HTML létezik és letölthető
