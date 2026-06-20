# Analyst Skill

Quarto-renderable elemzési notebookok elkészítésének végrehajtási útmutatója.
Notebook struktúra, cell-minták, kód-konvenciók és workflow.

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

Ha a user egy döntési kérdést tesz fel, például mely éveket érdemes használni,
kötelező ezt már a célfejezetben expliciten megfogalmazni.

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

- Minden szekció **legalább egy táblát vagy ábrát** tartalmaz.
- Tábla: mindig `display_analysis_table(df)`.
- Ábra: mindig `plt.show()` a cell végén.
- `ax.set_title()` tilos.
- `print()` tilos. Használj táblát, ábrát vagy `display(Markdown(...))`-t.
- Minden fontos output után legyen közeli, programmatikusan generált rövid olvasat.

---

## Setup Cell

A Raw cell után azonnal következő code cell minden notebookban kötelező:

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
```

Ha van notebook-specifikus helper, azt itt importáld.

---

## Kód Stílus

- **Adat-hozzáférés:** DuckDB-first SQL lekérdezésekre, ahol ez természetes.
- **Nagy in-memory transform:** Polars vagy pandas, a feladathoz illően.
- **Display inputok:** pandas, kis végső dataframe-ekre.
- **Charting:** seaborn elsődleges, matplotlib fallback és finomhangolás.
- Soha ne írj az adatbázisba.
- Plotly tilos, kivéve ha a user explicit interaktív HTML-t kér.

Az agentnek a helyes elemzési reprezentációt kell választania, nem a megszokottat:

- idősor esetén időtengelyes chart;
- éves összehasonlításnál faceted vagy overlay nézet;
- modellértékelésnél train-valid bontás;
- eloszlásoknál histogram, KDE, box, violin vagy kvantilis alapú összefoglaló;
- kalibrációnál binning, reference line, residual vagy pred-vs-fact nézet.

---

## Model And Target Evaluation Rules

Ha a notebook targetet, modellt vagy predikciót elemez, az alábbi szabályok kötelezők,
ha az adat elérhető:

- különítsd el a train és valid vagy in-sample és out-of-sample halmazt;
- mutasd meg az időbeli alakulást és a regime-váltásokat;
- mutasd meg az eloszlási különbségeket;
- mutasd meg a predikció és a fact target kapcsolatát;
- használj referencia-vonalat vagy ideális vonalat, ha ennek van értelme;
- írj metrikatáblát, ne csak ábrát;
- a notebook végén fogalmazd meg, hogy a modell mire használható és mire nem.

Ha a forrásban nincs kész `segment` mező:

- keresd meg, rekonstruálható-e foldból, metadata-ból, manifestből vagy sample-logikából;
- ha igen, rekonstruáld;
- ha nem, írd le explicit módon, hogy a kiértékelés milyen korlátozással olvasandó.

Ne használj leakage-gyanús vagy full-fit predikciót validációs következtetésekre,
ha fair splitből újra előállítható a train-valid nézet.

---

## Értelmező Zárás

Minden notebook végén kötelező egy külön szekció, például `## Értelmezés`,
`## Modellolvasat`, `## Döntési szempontok` vagy hasonló címmel.

Ebben a szekcióban:

- ne ismételd meg szó szerint a táblákat;
- állást kell foglalni a fontos kérdésekben;
- a következtetés a futott számokra és ábrákra támaszkodjon;
- ha a user döntést akar hozni, adj döntést támogató választ.

Példák:

- használható-e a teljes időszak a modellfejlesztéshez;
- vannak-e kizárandó évek vagy rezsimek;
- a valid teljesítmény elég stabil-e;
- a modell inkább rangsorolásra vagy pontbecslésre jó-e.

---

## Tiltott Placeholder Szöveg

Soha ne írj:

- `futtatás után kitöltendő`
- `to be filled after running`
- `TODO after execution`
- üres értelmezést vagy üres záró szekciót

Ha az eredmény a futtatástól függ, generáld programmatikusan.

---

## Futtatás És Renderelés

```bash
quarto render _doc_\XXXX_<slug>.ipynb --execute
```

1. Írd meg az összes markdown és code cell-t.
2. Minden eredmény programmatikusan generált legyen.
3. Futtasd le a notebookot tiszta kernelből.
4. Rendereld Quarto-val.
5. Ellenőrizd, hogy a HTML létrejött.
6. Olvasd vissza a fő outputokat.
7. Ha kell, javítsd a notebookot, futtasd újra, és csak utána add át.

---

## Rendering QA Checklist

- [ ] Raw cell: teljes Quarto frontmatter
- [ ] Setup cell: importok, theme, table helper
- [ ] Minden szekció: markdown leírás → code → tábla/ábra
- [ ] `display_analysis_table(df)` minden tábla cell végén
- [ ] Nincs bare `df`, `display(df)`, `df.head()` a cell végén
- [ ] Minden tábla: `#| label: tbl-...` és `#| tbl-cap: ...`
- [ ] Minden ábra: `#| label: fig-...` és `#| fig-cap: ...`
- [ ] `plt.show()` minden plot cell végén
- [ ] Nincs `ax.set_title()`
- [ ] Nincs placeholder szöveg
- [ ] A train-valid vagy más kritikus split explicit jelölve van, ha releváns
- [ ] A notebook végén van decision-oriented értelmezés
- [ ] A HTML létezik és a fő eredmények visszaellenőrzöttek
