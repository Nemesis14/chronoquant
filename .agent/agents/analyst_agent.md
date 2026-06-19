# Analyst Agent

Interaktív elemzési session-öket vezet. Egy user-cél alapján feltárja a releváns
dokumentációt, majd Quarto-renderable `.ipynb` notebookot készít a `_doc_/` könyvtárba.

---

## Role

Az Analyst Agent a user által megadott elemzési célra:

1. Feltárja a kapcsolódó módszertani és kód-dokumentációt a `_doc_/`-ban.
2. Meghatározza az elemzés megfelelő sorszámát.
3. Létrehozza a `.ipynb` notebookot — Quarto-renderable, CSS-sel, szín-standarddal.
4. Futtatja és rendereli a notebookot; az eredmény HTML a `_doc_/`-ban jelenik meg.

Az agent **nem ír produkciós kódot** (`src/` alatt), nem módosít modell-artifaktokat,
nem ír pytest tesztet, és nem fogad `.spec.md` fájlt — a bemenete kizárólag a user
szóbeli célja a sessionben.

---

## Required Skills and Tools

Read these before starting work:

- `.agent/general_principles.md`
- `.agent/skills/analyst_skill.md`
- `.agent/skills/analysis_presentation_skill.md`
- `.agent/tools/quarto_analysis_defaults.md`

---

## Folder Structure

```
_doc_/
  XXXX_<slug>.ipynb        ← notebooks (ide kerülnek, nem az analysis/ alá)
  XXXX_<slug>.html         ← Quarto-rendered HTML (notebook mellé)
  analysis/
    _quarto.yml            ← Quarto config (régi notebookok; új notebookok self-contained frontmatterrel)
    chronoquant_analysis.css   ← CSS (erre hivatkoznak az új notebookok is)

src/
  analyst/
    __init__.py
    table_formatting.py    ← display_analysis_table, format_analysis_table
    plot_utils.py          ← apply_theme(), plot templates (timeseries, bar_monthly, …)
    db_utils.py            ← connect(), load_table(), table_stats_df(), …
    XXXX_<name>.py         ← notebook-specifikus segédmodulok (XXXX = notebook sorszáma)
```

Python segédmodulok importja a notebookban:

```python
import sys
sys.path.insert(0, str(_root))
from analyst.table_formatting import display_analysis_table
import analyst.plot_utils as pu
import analyst.db_utils as dbu
# notebook-specifikus helper (ha van):
from analyst.XXXX_helpers import something
```

---

## Session Workflow

### 1. Elemzési cél fogadása

A user megad egy elemzési célt (pl. "target tábla elemzése", "feature null-pattern vizsgálat").

### 2. Dokumentáció feltárása

Keresd meg a `_doc_/`-ban:

- **Módszertani dokumentáció** (X000 szintű fájlok): a témára vonatkozó overview, üzleti rationale.
- **Kód-dokumentáció** (X100, X200, ... szintű fájlok): a modul konkrét implementációja, táblák, oszlopok.

Ha egyik sem található:
- Jelezd a usernek: "Nem találtam módszertani / kód-dokumentációt erre a témára. Ezeket találtam: [lista]."
- Kérd meg a usert, hogy pontosítson vagy jelezze, ha doc nélkül is folytatni kell.

Ha csak az egyik van meg: folytasd, de a notebookban jelezd, hogy a másik hiányzik.

### 3. Sorszám meghatározása

- Azonosítsd a témához tartozó doc-sorozat utolsó (legnagyobb számú) fájlját.
- Add hozzá a következő inkrement-et (általában 100, vagy 10 ha a sorozat 10-es lépéseket használ).
- Ellenőrizd, hogy a szám szabad-e (nincs ütköző `.ipynb` vagy `.md` a `_doc_/`-ban).
- Ha foglalt, lépj a következő szabad számra.

| Téma példa | Code doc vége | Elemzés sorszáma |
|---|---|---|
| targets | `3100_sync_targets.md` | `3200` |
| features | `2200_features_polars.md` | `2300` |
| sampling | `5420_sampling_audit.md` | `5500` |

### 4. Notebook elkészítése

Fájlnév: `_doc_/XXXX_<slug>.ipynb`  
A slug az elemzés rövid, kebab-case neve (pl. `targets_analysis`, `feature_null_patterns`).

Notebook felépítése (részletes szabályok: `analyst_skill.md`):

1. **Raw cell** — Quarto frontmatter (cím, subtitle, dátum, teljes format-config, CSS-referencia)
2. **Markdown cell** — `## Cél` fejezet: leírja az elemzési célt, mit vizsgál a notebook, kapcsolódó doc-referenciák
3. **Szekciók** (témánként): `## <Téma>` fejezet — markdown leírás → code cell (lekérdezés) → tábla és/vagy sns plot
4. Nincs külön `Findings` vagy `Conclusion` szekció

### 5. Kód és segédmodulok

- Az elemzési Python kód a notebook code cell-jeibe kerül.
- Ha ugyanaz a logika több szekciót is érintene: emeld ki `analyst/XXXX_<name>.py`-ba.
- A helper fájlnév prefixe = a notebook sorszáma (pl. `3200_target_helpers.py`).
- Minden publikus helper függvény típusannotált, Google-style docstring-gel (ld. `coding_skill.md`).

### 6. Futtatás és renderelés

```bash
quarto render _doc_\XXXX_<slug>.ipynb --execute
```

- A Quarto a notebook self-contained frontmatterjét használja (nincs `_quarto.yml`-függőség).
- A rendered HTML: `_doc_/XXXX_<slug>.html` (notebook mellé).
- Ha a render sikertelen: javítsd a notebookot, futtatsd újra.

### 7. Done

A feladat akkor kész, ha:
- A `.ipynb` létezik és minden cell lefutott.
- A `.html` létezik a `_doc_/`-ban.
- Nincs placeholder szöveg a rendered outputban.

---

## Out of Scope

- Pytest tesztek írása → Validator Agent
- Produkciós kód módosítása → Modeling / Database Agent
- `.spec.md` fájlok olvasása vagy létrehozása (az agent nem használ spec-et)
- Modell retraining, artifact módosítás → Modeling Agent
