---
epic: epic_049
id: t1
title: Signal-gyakoriság elemzés — kalibráció vs élő bevezetés
assignee: analyst_agent
status: done
blocks: []
blocked_by: []
---

## Goal

A user azt jelezte, hogy a bevezetett champion stratégia (`lgbm_solusdt_l/s_fw60_2101_2605`,
deployment `strat_solusdt_fw60_combo_2101_2605`, aktiválva 2026-06-23) élesen
szinte kizárólag short jelzést ad, longot alig/soha nem lát. Cél: kiderít­eni,
hogy ez piaci rezsimváltás vagy implementációs hiba (pl. élesen nem elérhető
feature) következménye — heti long/short signal-gyakoriság összevetésével a
kalibrációs/backtest periódus és az élő, bevezetés utáni periódus között.

## Scope

Read-only elemzés, nincs `src/` kódmódosítás. Érintett adatforrások:
`database/_registry/registry.duckdb` (deployments, strategies),
`database/solusdt/solusdt.duckdb` (predictions, feat_ohlcv_quant),
`database/solusdt/solusdt_lab.duckdb` (strat.*__trades),
`artifacts/lgbm_solusdt_l_fw60_2101_2605/strategy/`,
`artifacts/lgbm_solusdt_s_fw60_2101_2605/strategy/`.

Output: `_doc_/models_doc/7200_signal_frequency_train_vs_live.ipynb` + `.html`.

## Acceptance Criteria

- [x] Heti long/short signal-gyakoriság a kalibrációs periódusban (tényleges `strat.*__trades`)
- [x] Heti long/short signal-gyakoriság élesen, bevezetés után (rank-lookup percentile + entry_cutoff rekonstrukció, live runtime logikát követve)
- [x] Teljes idővonal chart a bevezetés időpontjával jelölve — a csökkenés időzítésének tisztázása
- [x] Implementációs hiba kizárása: model-stamp konzisztencia, NULL predikció, feature NaN ráta élőben vs kalibráción
- [x] Piaci rezsim (volatilitás) összehasonlítás objektív metrikákkal
- [x] Notebook renderelve, HTML létrejött, placeholder nélkül
- [x] Végső, döntést támogató értelmezés a futott számokra alapozva

## Notes

**Fő eredmény:** a jelenség elsősorban piaci rezsimváltásra / kalibrációs
driftre vezethető vissza, nem kódhibára.

- Kalibrációs periódusban (2025-05-01 – 2026-05-31) a short irány már eleve
  ~3.3x gyakoribb, mint a long (4.61 vs 1.38 esemény/hét) — ez az
  aszimmetrikus entry_cutoff-ból következik (long p98 = felső 2%, short p94 =
  alsó 6%), nem bevezetés utáni anomália.
- Bevezetés óta mindkét irány gyakorisága arányosan esett vissza: long -90%,
  short -88% a kalibrációs rátához képest (0.14 és 0.56 esemény/hét).
- A heti trend chart alapján a csökkenés már 2026 áprilisában elkezdődött —
  jóval a 2026-06-23-i bevezetés előtt, még a kalibrációs ablakon belül.
  Nem a deploy eseményéhez köthető lépcsős változás.
- Implementációs hiba hipotézise nem támasztható alá: a `predictions` tábla
  model-stampje konzisztensen a champion modellekre mutat, `long_pred`/
  `short_pred` egyetlen élő sorban sem NULL, és a hosszú modell mind az 57
  feature-e gyakorlatilag NaN-mentes élőben (max 0 NaN) és kalibráción (max
  48 NaN ~600k sorból).
- Volatilitás-összehasonlítás objektív magyarázatot ad: élő medián ATR14
  -61.5%, NATR14 -31.5%, hist_vol_20 -29.1% a kalibrációs ablakhoz képest —
  tartósan alacsonyabb volatilitású rezsim, ami mindkét irányú score-t a
  kalibrációs eloszlás alacsonyabb percentiliseibe tolja; mivel a long
  küszöbe eleve szigorúbb, ez a long irányt nyomja közelebb a nullához,
  miközben a short még időnként átlépi a lazább küszöbét.
- Javasolt következő lépés (nem ennek a ticketnek a hatóköre): kalibrációs
  ablak frissítése / rolling recalibration bevezetése, vagy a long
  entry_cutoff felülvizsgálata.
- Megjegyzés: finomabb, csak érték-szintű (nem NULL) feature-számítási
  eltérés élő és offline szinkron között nincs teljesen kizárva — ha a
  rezsim-magyarázat gyakorlati döntés után is kétséges marad, egy dedikált
  bar-szintű feature-érték paritás-teszt lenne a következő diagnosztikai lépés.
- Élő tényleges kereskedési napló (`trading.duckdb`) üres volt (a live
  service csak egy rövid dry-run-t futtatott) — a live signal-gyakoriságot
  ezért a `predictions` tábla nyers score-jaiból rekonstruáltuk, pontosan a
  live runtime döntési logikáját követve.

[validator] Nincs `src/` kódmódosítás — ruff/pyright/pytest nem releváns.
Self-QA: notebook tisztán lefutott (0 hiba), minden tábla `#| label`/`#| tbl-cap`
párral, HTML létrejött, nincs placeholder szöveg, végső Értelmezés szekció a
futott számokra épül. `pr_` → `done_`.
