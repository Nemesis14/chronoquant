# Analyst Skill — Riportok és kutatási összefoglalók

Végrehajtási útmutató az `analyst_agent` számára: report-formátum riportok készítése a `src/reports`
könyvtárral, valamint md-alapú elemzések/kutatási összefoglalók. **Nincs notebook, Quarto, papermill vagy HTML-render**
(epic_068-ban megszűnt; a régi anyagok a git historyban: commit `73f2156`).

Kontraktus: `_doc_/_plans_/068_bff_ui.md` §3. Kód-referencia: `_doc_/database_and_code_doc/8330_reports.md`.

---

## Mit hova

| Output | Hely | Formátum |
|---|---|---|
| Modell-/stratégia-specifikus riport | `artifacts/<model_id>/reports/<slug>/` | report-formátum (`scope: model`) |
| Általános riport (target, feature, sampling...) | `_doc_/reports/<slug>/` | report-formátum (`scope: general`) |
| Lezárt kutatás / kísérlet összefoglaló | `_doc_/research/<slug>.md` + index sor a `README.md`-ben | markdown, "Kód a git historyban" lábléc |
| Modell-elemzés szöveges összefoglaló | `_doc_/models_doc/<slug>.md` | markdown |

A pipeline-lépések (`sampling`, `feature_engineering`, `hyperparameter_search`, `strategy`, `target_regime`) riportjait a
`src/reports/builders/*` modulok állítják elő; a `pipeline.py --step feature_engineering|analyze` hívja őket. Ezek módosítása a builder
logikájának változtatása — tartsd a determinisztikus kimenetet és futtasd a `src/reports/tests`-et.

---

## Report-formátum

```
<root>/<slug>/
  manifest.json        schema_version 1: id, title, scope, model_id, created_at, order, summary, tags, source, content{markdown, charts[], tables[]}
  report.md            GFM; beágyazás önálló sorban: {{chart:<id>}} / {{table:<id>}}
  charts/<id>.json     Plotly figure {data, layout, config?}; max 5000 pont/trace, gyertya max 2000
  tables/<id>.json     {columns:[{key,label,type,digits?}], rows:[[...]], total_rows, truncated}; max 2000 sor
```

- `slug`: `^[a-z0-9][a-z0-9_-]{0,63}$`; a modell-riportok fix slugjai: `01_sampling`, `02_feature_engineering`, `03_hyperparameter_search`, `04_strategy`.
- Nincs nyers HTML, nincs képfájl; a téma (light/dark) a frontenden érvényesül, a figure JSON-ba nem kerül template.
- Paletta: `reports.theme` (`LONG` zöld, `SHORT` piros, `ACCENT`, `NEUTRAL`, `BLUE`, `ORANGE`); szomszédos chartokban egy szín = egy jelentés.

## ReportBuilder használat

```python
from reports.builder import ReportBuilder
from reports import theme

rb = ReportBuilder(scope="general", slug="target_regime", title="Target rezsim", order=50,
                   summary="1-2 mondat", tags=["target"], source={"kind": "analysis", "generator": "analyst_agent"})
rb.add_markdown("## Cél\n...")
rb.add_chart("monthly", fig, title="Havi minták", height=420)        # {{chart:monthly}} a markdownba
rb.add_table("summary", df, title="Összegzés")                       # pandas/polars df, max 2000 sor
rb.add_markdown("## Értelmezés\n...")
path = rb.write()                                                     # atomikus, idempotens
```

- `scope="model"` esetén `model_id` kötelező; `owner` az API-ban a `model_id` vagy `general`.
- Az olvasás: `GET /api/reports`, `GET /api/reports/{owner}/{slug}` és az MCP `list_reports` / `read_report`.

## Riport szerkezeti szabályok

1. `## Cél` fejezet az elején (mit és miért vizsgálunk; hivatkozás a `_doc_/methodology_doc/` "miért"-jére és a `_doc_/database_and_code_doc/` kód-referenciára).
2. Elemzési szekciók: markdown leírás -> chart/tábla placeholder.
3. Záró értelmező szekció (`## Értelmezés`, `## Döntési szempontok`): állást foglal stabilitásról, használhatóságról, kizárandó időszakokról. Nincs külön `Findings`/`Summary` cím.
4. Minden szám valódi futtatásból származik; a builder script futtatása után olvasd vissza a manifestet, a táblákat és ellenőrizd a chart JSON-t (nincs üres trace, UTC időbélyegek).
5. Táblaformázás: `*_pct`, `*_rate` oszlop `pct`, egyébként `float` + `digits`; időoszlop `datetime`.

## Md-alapú elemzések (`_doc_/research`, `_doc_/models_doc`)

- Rövid, magyar, döntés-orientált: cél, módszer, eredmény (számokkal), következtetés, nyitott pontok.
- Lezárt kísérlet kódja törölhető; az md-ben "Kód a git historyban" szakasz a commit hivatkozással (`73f2156`).
- Frissítsd a mappa `README.md` index táblázatát.

## Done checklist

- [ ] Riport: `manifest.json` érvényes (`reports.schema.Manifest`), a placeholderek egyeznek a chart/table fájlokkal
- [ ] `uv run pytest src/reports` zöld (builder-módosításnál)
- [ ] A riport megjelenik a UI Elemzések fülén (`/reports`) vagy az `GET /api/reports` listában
- [ ] Értelmező szekció van, a számok a futtatásból jönnek
- [ ] md-elemzésnél a README index frissítve
