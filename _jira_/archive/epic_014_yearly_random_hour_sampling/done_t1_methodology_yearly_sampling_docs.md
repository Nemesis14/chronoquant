---
epic: epic_014
id: t1
title: Módszertani dokumentáció — yearly random-hour + monthly validation week stratégia
assignee: methodology_agent
status: pr
story_points: 3
blocks: []
blocked_by: []
---

## Goal

Módszertani dokumentációt írni a yearly random-hour sampling stratégiáról és a monthly
validation week split logikáról. Cél: a döntések mögötti rationale rögzítése, hogy a
modell eredmények értelmezhetők és reprodukálhatók legyenek.

## Scope

`_doc_/` X000 vagy X100 szinten 1–2 új dokumentum:

1. **Sampling rationale** — miért éves split, miért random hour, miért nem expanding window
2. **Validation design** — monthly validation week logika, purge/embargo indoklás, segment
   definíciók (train / valid / purge)

Nem kell kódot írni. Csak dokumentáció.

## Acceptance Criteria

- [x] Legalább 1 _doc_/ fájl létezik, ami lefedi a yearly random-hour sampling döntést
- [x] A dokumentáció tartalmazza: miért nem expanding window, miért random hour, miért éves granularitás
- [x] A purge logika (±240 perc) indoklása dokumentálva (max feature lookback = 140 perc → 240 perc purge)
- [x] A monthly validation week kiválasztás logikája és célja dokumentálva
- [x] A segment értékek (train / valid / purge) és azok definíciója dokumentálva
- [x] A várt row count-ok évente dokumentálva (~8760 total, ~2016 valid, ~96 purge, ~6648 train; szökőév és év-határon átnyúló esetekkel)
- [x] Az éves függetlenség (nincs expanding window) és a következménye (éves model stabilitás mérése) dokumentálva

## Notes

**Háttér (story-ból):**
- Target: continuous fw60 forward outcome
- 1-perces OHLCV adatból 1 random perc/óra → ~8760 sor/év
- 12 validation week (hónaponként 1), fully in valid set
- Purge: 240 perc a validation week előtt és után → nem kerül training-be
- Éves split célja: minden piaci szezon értékelhető, éves stabilitás mérhető

Párhuzamosan futhat t2-vel (kódtól független).

**Done 2026-06-17:** `_doc_/3101_sampling_yearly.md` létrehozva — mind a 6 módszertani szekció, 4 Mermaid diagram, tényleges max lookback 140 perc (nem 220 mint a task notes becsülte).
