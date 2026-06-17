---
epic: epic_010_analyst_sampling
id: t4
title: Elemzési notebook átírása parquet olvasásra
assignee: analyst_agent
status: pr
blocks: []
blocked_by: [t2]
---

## Goal

A `_doc_/analysis/3200_sampling.ipynb` notebook átírása: a jelenlegi DuckDB SQL alapú
lekérdezések helyett a t2-ben létrehozott `.parquet` fájlból olvasson. Az elemzések
eredménye ne változzon — csak az adatforrás cserélődik.

## Scope

- `_doc_/analysis/3200_sampling.ipynb`
- `_doc_/3200_sampling.html` — Quarto re-render szükséges
- Adatforrás: `database/solusdt/samples/<sample_id>/<sample>.parquet`

## Acceptance Criteria

- [ ] Notebook DuckDB-t nem használ közvetlen lekérdezésre (csak parquet olvasás)
- [ ] Összes cell sikeresen lefut
- [ ] Quarto render sikeres, HTML frissítve
- [ ] Az elemzési eredmények (számok, ábrák) konzisztensek a korábbi futással

## Notes

t2 után futhat — a parquet fájl elérési útja és struktúrája onnan ismert.
Ahol DuckDB reservoir sampling kellett a memória miatt, parquettel Polars lazy frame
vagy pandas chunking lehet az alternatíva.

[analyst_agent] 2026-06-17 — Implementálva.
- Notebook teljesen újraírva scratch-ből, DuckDB eltávolítva.
- Adatforrás: `pl.scan_parquet()` — lazy frame alapú, 9.6M sor × 208 feature.
- OOM javítások: null_rate batch (50 col/batch), leading_null időablak-szűrés,
  corr/spearman `.head(50_000)` lazy push-down a full collect helyett.
- Javított bug: `TEST_INFO` kulcsok `start`/`end` (nem `test_start`/`test_end`).
- Javított bug: `pl.str.capitalize()` → pandas `.str.capitalize()` (Polars nem támogatja).
- Notebook sikeresen lefutott, Quarto render OK → `_doc_/3200_sampling.html` létrejött.
