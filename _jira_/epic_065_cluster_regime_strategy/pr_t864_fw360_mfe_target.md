---
epic: epic_065
id: t864
title: fw360 long/short MFE célváltozó számítása
assignee: modeling_agent
status: todo
blocks: [t866]
blocked_by: []
---

## Goal

A jelenlegi `fw60` MFE-target logika (`long_mfe_fw60`, `short_mfe_fw60`) mintájára
kiszámítani egy **360 bar (6 óra) előretekintő** változatot kutatási célra:
`long_mfe_fw360`, `short_mfe_fw360`.

Módszertani háttér: [`_doc_/_plans_/plan.md`](../../_doc_/_plans_/plan.md) 3.6 szakasz.

## Scope

- Ez egy **research célváltozó** — nem kerül a live `target` táblába, nem érinti
  `src/data_handling/sync_tables/`.
- Elegendő egy snapshot-alapú (`snap."<snapshot_id>"`) egyszeri számítás
  `research/cluster_regime/` alatt, ugyanazzal a log-MFE definícióval mint a
  meglévő `fw60` logika (csak 360 bar horizonttal).
- Kimenet: parquet vagy DuckDB tábla a `long_mfe_fw360` / `short_mfe_fw360`
  oszlopokkal, `open_time` kulccsal, illeszthető a t865/t866 taskokhoz.

## Acceptance Criteria

- [ ] `long_mfe_fw360` és `short_mfe_fw360` számítva egy referencia snapshot-on
- [ ] A definíció konzisztens a meglévő `fw60` logikával (csak a horizont változik)
- [ ] Nincs data leakage (a target csak jövőbeli barokat használ, a megfigyelés
      időpontjához képest)
- [ ] Eredmény tábla/parquet dokumentálva és elérhető a t866 (klaszterezés) számára

## Notes

**Implementáció teljesítve (2026-08-31):**

1. Megvalósítás: `research/cluster_regime/01_target_computation.py`
   - `compute_fw360_targets(asset_id, output_format)` — fw360 MFE szamítás (long/short)
   - `validate_fw360_distribution(asset_id)` — fw60 vs fw360 validáció

2. Logika:
   - long_mfe_fw360 = log(max(high[t+1:t+360]) / close[t]) — ideális long excursion 6-órás ablakon
   - short_mfe_fw360 = log(min(low[t+1:t+360]) / close[t]) — ideális short excursion 6-órás ablakon
   - Data leakage: nincsen — csak jövőbeli (t+1..t+360) barokat használunk
   - NULL kezelés: utolsó 360 sor NULL (nincs elég jövő adat)

3. Output: DuckDB tábla `target_fw360` az `solusdt.duckdb`-ben
   - Oszlopok: open_time, close, fw360_max_high, fw360_min_low, fw360_max_ratio_ret, long_mfe_fw360, short_mfe_fw360
   - 3,134,763 sor, 3,134,403 non-NULL (99.99%)

4. Validáció (fw60 vs fw360 összehasonlítás):
   - Long MFE: fw60 mean 0.72% vs fw360 mean 2.06% (ratio ~2.9x)
   - Short MFE: fw60 mean -0.73% vs fw360 mean -2.17% (ratio ~3.0x)
   - Non-NULL ratio: fw60 100% vs fw360 99.99% ✓
   - Logikus: 6 óra alatt ~3x nagyobb mozgás, mint 1 órában

5. Status: Ready for t866 (klaszterezés)
