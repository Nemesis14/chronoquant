---
epic: epic_010_analyst_sampling
id: t1
title: Sample EDA — solusdt_fw60_2010_2605
assignee: analyst_agent
status: done
blocks: []
blocked_by: []
---

## Goal

A `solusdt_fw60_2010_2605` sample teljes feltáró elemzésének elkészítése Jupyter notebookban,
Quarto rendereléssel HTML kimenetre. Célja igazolni, hogy a sample alkalmas LightGBM binary
classifier fejlesztésre.

## Scope

- Notebook: `_doc_/analysis/3200_sampling.ipynb`
- HTML output: `_doc_/3200_sampling.html`
- Spec: `_doc_/analysis/3200_sampling_spec.md`
- DB: `database/solusdt/solusdt.duckdb`
- Feature tábla: `feat_ohlcv_quant` (208 feature)
- Target tábla: `target` (`trg_l_fw60_q90`, `trg_s_fw60_q10`)

## Acceptance Criteria

- [x] Minden szekció (1–8) lefutott, nincs placeholder szöveg
- [x] Mindkét target elemezve van
- [x] Quarto render sikeres, HTML exportálva `_doc_/3200_sampling.html`-be
- [x] Summary szekció kvantitatív számokra hivatkozik
- [x] 41/41 notebook cell sikeresen lefutott

## Notes

### 2026-06-16 — Elkészítés

Notebook teljes egészében DuckDB SQL alapú — a teljes tábla (~3M sor × 208 col) nem
tölthető be memóriába (0.8 GB szabad RAM vs ~5 GB szükséges). Minden aggregáció és
vizualizáció reservoir sampling-gal (5k–50k sor) fut.

Kulcs döntések:
- `USING SAMPLE reservoir(N ROWS) REPEATABLE (42)` szintaxis — bernoulli NEM használható
  row count-tal, csak százalékkal
- KS mátrix: 3000 sor/fold minta
- Pearson korrelációs mátrix: 50k sor a fold 5 train-ből
- Spearman: 30k sor minta

41/41 cell sikeresen lefutott. HTML: `_doc_/3200_sampling.html` (8.9 MB, embedded resources).

Javított szintaxis hibák:
- `sec23-load`, `sec32-code`, `sec41-code`, `sec43-code`, `sec61-load`, `sec62-code`:
  mindegyik `(bernoulli, 42)` cserélve `reservoir(N ROWS) REPEATABLE (42)`-re
