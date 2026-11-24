---
epic: epic_071
id: t922
title: Írd meg az S/R detektor és a sr_detection builder kód-referenciáját
assignee: code_doc_agent
status: done
blocks: []
blocked_by: []
---

## Goal
Kód-referencia bejegyzés a `_doc_/database_and_code_doc/` zónában a t910/t911 új kódjához.

## Scope
- `_doc_/database_and_code_doc/1200_sync_tables.md` — `src/data_handling/sync_tables/_levels_polars.py`: publikus API
  (`detect_all_scales`, `detect_scale`, `forward_label_events`, `levels_as_of`, `trendlines_as_of`, építőkövek),
  `ScaleResult` mezők, kimeneti sémák, `config/features.json` `levels_detector` blokk, `utils.load_levels_detector_config`,
  look-ahead szabály (`confirmed_at` = i+n. scale-bar záróideje; feature-höz csak az `*_as_of` kimenet).
- `_doc_/database_and_code_doc/8330_reports.md` — új builder `reports.builders.sr_detection` (újragenerálás:
  `uv run python -m reports.builders.sr_detection`, kimenet `_doc_/reports/sr_trendline_detection/`).

## Acceptance Criteria
- [ ] Mindkét doc fájl frissítve, a kóddal egyező szignatúrákkal

## Notes

### t922 — code_doc_agent (2026-11-19)

**Módosított fájlok**
- `_doc_/database_and_code_doc/1200_sync_tables.md` — új szakasz `_levels_polars.py`-ról: flowchart + look-ahead sequenceDiagram, publikus API táblázat, `ScaleResult` és kimeneti sémák, `utils.load_levels_detector_config` + `levels_detector` kulcsok (config v1). Felfelé link a `1700_features_sr_trendline.md`-re.
- `_doc_/database_and_code_doc/8330_reports.md` — `sr_detection` builder a builder-táblában, új `sr_detection` szakasz (belépési pont, példa-kiválasztás, kimenet, újragenerálás), a teszt-sor kiegészítve `test_sr_detection.py`-jal.

**Megjegyzés**
- Szignatúrák a kódból (`_levels_polars.py`, `utils.py`, `sr_detection.py`) vettem, nem a ticketből.
- A `feat_levels` sync (t912) nem lett dokumentálva, a `sync_tables/` kódhoz nem nyúltam.
- Nem commitoltam; a ticket neve változatlan.

[validator] Validated — 2026-11-19 → done
- 1200 `_levels_polars.py`: mind a 11 publikus függvény szignatúrája, a `ScaleResult` 8 mezője, a bars/pivots/levels/touches/trendlines/events/forward sémák és a config v1 értékei egyeznek a kóddal (inspect + séma-konstansok alapján). A t910 leakage teszt léptékei (15m, 1h) is stimmelnek.
- 8330 `sr_detection`: `build`, `load_ohlcv_1m`, `select_examples` szignatúrái, a `open_readonly_connection`, `scope=general`, `order=20`, a slug / kimeneti mappa és a `test_sr_detection.py` egyeznek.
