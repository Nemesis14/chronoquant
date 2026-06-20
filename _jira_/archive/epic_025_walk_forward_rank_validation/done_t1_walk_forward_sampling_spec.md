---
epic: epic_025
id: t1
title: Walk-forward sampling specifikáció és artifact redesign
assignee: modeling_agent
status: todo
blocks: [t2, t3]
blocked_by: []
---

## Goal

Az új walk-forward validation séma pontos specifikációját és artifact contractját
lefektetni, hogy a sampling, search, train és elemzési réteg ugyanazt a fold
szemantikát használja.

## Scope

- fold definíció: `9 hónap train + 3 hónap validation`
- havi shiftelésű foldok
- óránkénti supervised sampling baseline
- gap / purge szabály definiálása
- sample metadata és parquet contract frissítése

## Design

- A train ablak időrendben zárt, a validation ablak mindig utána következik.
- A foldok validation ablaka átnyúlhat `2022`-be, ez elfogadott.
- A calibration reference a train utolsó hónapja metadata szinten jelölhető,
  de nem külön holdout split.
- A sample artifact tárolja foldonként:
  - `fold_id`
  - `train_start`
  - `train_end`
  - `valid_start`
  - `valid_end`
  - opcionálisan `calibration_month`

## Acceptance Criteria

- [ ] A fold séma egyértelműen dokumentált 2021-es példával
- [ ] A sample artifact minimális mezői rögzítve vannak
- [ ] A gap / purge szabály le van írva: `max(target horizon, longest feature lookback)`
- [ ] Egyértelmű, hogy ez validation rendszer, nem final OOS test

## Notes

### Fold séma — 2021-es példa

Paraméterek: `train_months=9`, `valid_months=3`, `shift_months=3` (non-overlapping).

| Fold | Train ablak             | Valid ablak             | Megjegyzés           |
|------|-------------------------|-------------------------|----------------------|
| 1    | Jan 2021 – Sep 2021     | Oct 2021 – Dec 2021     | válid csak 2021-ben  |
| 2    | Apr 2021 – Dec 2021     | Jan 2022 – Mar 2022     | valid átnyúlik 2022  |
| 3    | Jul 2021 – Mar 2022     | Apr 2022 – Jun 2022     | valid átnyúlik 2022  |
| 4    | Okt 2021 – Jun 2022     | Jul 2022 – Sep 2022     | valid átnyúlik 2022  |

- **Anchor év:** 2021 → az első fold valid start: `2021-10-01`
- **Shift:** 3 hónap (= valid_months) → non-overlapping valid ablakok
- **Train ablak kezdete:** A parquet-ban az anchor év 1. napjától tároljuk a sorokat, de a search az összes `fold_id != k` sort felhasználja trainnek (beleértve az előző éveket is ha a DB-ben van adat). A `train_start` csak informatív az artifactban.
- **OOS év (2022):** Az `oos_year` adatait az OOS értékeléshez tartjuk fenn külön parquet-ban — de a fold valid ablakok átnyúlhatnak 2022-be.
- **FONTOS:** A validation ablakok **nem** az OOS final tesztet helyettesítik.

### Artifact contract

**`sample_train_valid.parquet`** — azonos struktúra mint a yearly sampling esetén:
- Oszlopok: `open_time, <target_col(s)>, fold_id, feat_*`
- `fold_id` értéke: a sor valid ablakának fold száma (1, 2, 3, 4); ha egyik valid ablakba sem esik → `fold_id = 0` (train-only sor)
- Óránkénti alapú mintavétel (`select_hourly_observations`) megmarad

**`metadata.json`** — új kulcs a meglévő struktúrán felül:
```json
{
  "fold_time_windows": [
    {"fold_id": 1, "train_start": "2021-01-01", "train_end": "2021-09-30",
     "valid_start": "2021-10-01", "valid_end": "2021-12-31"},
    {"fold_id": 2, "train_start": "2021-04-01", "train_end": "2021-12-31",
     "valid_start": "2022-01-01", "valid_end": "2022-03-31"},
    {"fold_id": 3, "train_start": "2021-07-01", "train_end": "2022-03-31",
     "valid_start": "2022-04-01", "valid_end": "2022-06-30"},
    {"fold_id": 4, "train_start": "2021-10-01", "train_end": "2022-06-30",
     "valid_start": "2022-07-01", "valid_end": "2022-09-30"}
  ]
}
```
A `fold_week_assignments` kulcs törlődik (walk-forward esetén nincs értelme).

### Gap / purge szabály

Formális minimum: `purge_minutes = max(target_horizon_bars, longest_feature_lookback_bars) = max(60, 140) = 140 perc`

**Implementációban: 240 perc** (konzervatív safety margin, megtartva a jelenlegi defaultot).

A purge zóna értelmezése:
- `[train_end, train_end + 240 min]` → kizárva a trainből
- `[valid_start - 240 min, valid_start]` → kizárva a trainből
- Mindkét oldali purge a fold határán (nem csak egyik irányban)

### Implementációs döntések (t2 számára)

1. `WalkForwardSamplingConfig` a meglévő `YearlySamplingConfig` mellé kerül — nem helyettesíti
2. `select_hourly_observations()` reuse: az anchor évre ÉS a szükséges jövőbeli hónapokra (az összes fold valid ablakát lefedő tartomány)
3. DB lekérdezés: `WHERE open_time BETWEEN anchor_year_start AND last_valid_end` (purge puffer nélkül a DB oldalon)
4. `fold_id = 0` a train-only sorokhoz (egyik valid ablakba sem esik)
5. `write_yearly_artifacts()` reuse az artifact íráshoz

