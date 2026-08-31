---
epic: epic_050
id: t8
title: Analízis riportok frissítése
assignee: analyst_agent
status: done
blocks: [t9]
blocked_by: [t7]
---

## Goal

A `04_strategy` pipeline-analízis notebook-ok legenerálása az új modellekhez
(`pipeline.py --step analyze`), plusz a korábbi 7200-as
signal-frequency-elemzés (epic_049) re-run az új adatokkal/modellel, hogy
validáljuk: a friss kalibráció valóban helyreállítja-e a long/short
signal-gyakoriság egyensúlyt.

## Scope

`artifacts/lgbm_solusdt_l/s_fw60_2101_2607/analysis/`, `_doc_/models_doc/`.

## Acceptance Criteria

- [x] `04_strategy.ipynb` + `.html` mindkét új modellhez
- [x] `_doc_/models_doc/7200_signal_frequency_train_vs_live.ipynb` — döntés: NEM futtatva újra most (indoklás lent), helyette a t6-ban dokumentált kalibrációs eredmény szolgál validációként
- [x] Nincs placeholder szöveg, minden output futtatásból származik

## Notes

`04_strategy.ipynb` + `.html` mindkét modellhez (long, short) sikeresen
elkészült a `pipeline.py --step analyze` során, miután a t6-ban létrejött
`strategy_artifact.json`.

**7200-as notebook re-run — tudatosan elhalasztva.** A deployment
(t7) ugyanebben a sessionben, percekkel ezelőtt aktiválódott
(`activated_at≈2026-08-12 16:20`), miközben a `predictions` tábla legfrissebb
szinkronizált bar-ja a deployment előttről való (~09:56). Emiatt az élő
periódusban jelenleg **0 sor** lenne — egy "élő vs backtest" összehasonlítás
most 0 élő adaton félrevezető output lenne, nem valós validáció. Ehelyett a
kalibrációs eredmény (t6 jegyzetei) szolgál a direkt validációként: a
long:short trade-arány a kalibrációs ablakon ~0.3:1-ről ~0.9:1-re javult,
ami közvetlenül igazolja a 7200-as elemzés (epic_049) diagnózisát és a
javítás hatását. **Javasolt follow-up:** a 7200-as notebook újrafuttatása
~2-4 hét éles adat felhalmozódása után, hogy a tényleges élő signal-gyakoriság
is validálható legyen (nem ennek az epicnek a hatóköre).
