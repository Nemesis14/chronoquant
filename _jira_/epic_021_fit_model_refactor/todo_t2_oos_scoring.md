---
epic: epic_021
id: t2
title: OOS scoring — sample_oos.parquet
assignee: modeling_agent
status: todo
blocks: [t3]
blocked_by: [t1]
---

## Goal

A t1-ben fitelt modellt a következő naptári évre alkalmazni (OOS scoring), és a kimenetét
`artifacts/<model_id>/sample_oos.parquet`-ként menteni. A trading modul ebből kalibrálja
a stratégiát.

## Scope

- `src/modeling/training/fit_lgbm.py` — OOS scoring funkció hozzáadása (vagy külön modul)
- `config/models.json` — `oos_year` mező hozzáadása minden model entryhoz (design decision lent)

**OOS adatforrás:**
- Features: `quant_train` (DuckDB), az OOS év teljes tartománya (jan 1 – dec 31)
- Targets: `target` tábla (`long_mfe_fw60`, `short_mfe_fw60`) ugyanazon range-re
- `row_stride = 1` az OOS-nél (minden perc, nem csak every 60th)

## Acceptance Criteria

- [ ] OOS év forrása: `config/models.json` `oos_year` mező (nem CLI arg, nem derivált)
  - Példa: `"oos_year": 2022` a `lgbm_solusdt_l_fw60_q90_2021` model entryban
- [ ] OOS adatbetöltés: DuckDB `quant_train` + `target` az OOS év teljes jan–dec range-ére
- [ ] Csak a `feature_set.json["selected"]` feature oszlopok lekérdezése
- [ ] `predict_proba` alkalmazása → egyirányú prediction oszlop
- [ ] Output: `artifacts/<model_id>/sample_oos.parquet`
- [ ] Schema: `open_time | pred_long | long_mfe_fw60 | short_mfe_fw60`
  - `l` irányú model: `pred_long` töltve, `pred_short` null (vagy kihagyva)
  - `s` irányú model: `pred_short` töltve, `pred_long` null (vagy kihagyva)
- [ ] `ruff check` + `pyright` tiszta

## Notes

**Design decision — OOS év forrása:**
Opciók: a) `config/models.json`-ban `oos_year` mező, b) model_id-ből deriválja (`2021` → 2022),
c) CLI arg. **Javasolt: a) — explicit a config-ban**, mert az utolsó évnél (2025) nem triviális
a következő év (live trading), és a CLI-t egyszerűbben tartja.

**sample_oos.parquet helye:**
A projekt overview `database/<asset>/samples/<sample_id>/sample_oos.parquet`-ot ír,
de ez multi-model konfliktusos (long + short ugyanoda írna). **Javasolt: artifact dir**
(`artifacts/<model_id>/sample_oos.parquet`) — minden model saját OOS fájlt kap.
A trading modul path-ját frissíteni kell ha szükséges.

**Schema döntés:**
A trading modul mind a két irányt igényli (`pred_long` + `pred_short`). Lehetőségek:
a) Minden model csak a saját irányát írja (null a másik) — egyszerű, trading modul joinol
b) Mindkét irányú fit egyszerre fut (pipeline szinten) — komplexebb
**Javasolt: a)** — a trading modul maga joinol az l+s OOS fájlok között.

**Null target sorok:**
A target tábla utolsó 60 sora null (fw60 horizon). Ezeket is prediktálni kell (probs megvan),
csak a target oszlopok lesznek null. Include-olni kell az OOS-ben.
