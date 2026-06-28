---
id: s1
epic: epic_038_sampling_analysis
title: "Finding: walk-forward CV train split nem időrendi — jövőbeli adatot lát a korai foldok train-je"
type: story
status: open
created: 2026-06-23
---

## Finding összefoglalója

A sampling audit notebook (`5500_lgbm_l_2101_2605_sampling.ipynb`) elkészítése során
derült ki, hogy a walk-forward CV split logikája nem a várt expanding-window viselkedést
valósítja meg.

---

## Hol néztük meg

**1. Notebook fold összesítő**
Az első fold tábla `open_time BETWEEN train_start AND train_end` lekérdezéssel dolgozott
(9 hónapos ablakok: fold 1 → 2023-01-01–2023-09-30, ~6 500 sor). A user jelezte,
hogy a 2021–2022 sorok nem látszanak — azok „hol vannak?"

**2. `src/modeling/search/lgbm_search.py` — `_fold_split_walk_forward` függvény**
A tényleges split logika:
```python
valid_mask = (open_time >= valid_start) & (open_time <= valid_end)
purge_mask = (open_time > train_end) & (open_time < valid_start)
           | (open_time > valid_end)  & (open_time <= valid_end + 240min)
train_mask = ~valid_mask & ~purge_mask
```
**Nincs `open_time >= train_start` feltétel a train_mask-ban.**

**3. Tényleges split mérése a sample táblán**
A split logikát Python-ban reprodukálva a sample táblán
(`model."lgbm_solusdt_l_fw60_2101_2605__sample"`, 47 448 sor):

| fold | part  | min_date   | max_date   | count  | avg_target |
|------|-------|------------|------------|--------|------------|
| 1    | train | 2021-01-01 | 2026-05-31 | 41 588 | 0.0071     |
| 1    | valid | 2023-10-01 | 2024-05-31 |  5 856 | 0.0074     |
| 2    | train | 2021-01-01 | 2026-05-31 | 41 564 | 0.0073     |
| 2    | valid | 2024-06-01 | 2025-01-31 |  5 880 | 0.0058     |
| 3    | train | 2021-01-01 | 2026-05-31 | 41 636 | 0.0074     |
| 3    | valid | 2025-02-01 | 2025-09-30 |  5 808 | 0.0055     |
| 4    | train | 2021-01-01 | 2025-09-30 | 41 616 | 0.0075     |
| 4    | valid | 2025-10-01 | 2026-05-31 |  5 832 | 0.0045     |

---

## Finding

`train_months = 9` a `fold_time_windows` metaadatában definiált referencia ablak,
de a search kód nem használja felső határként. A tényleges train set minden fold esetén
= az összes sample sor, kivéve a fold valid ablakát és a purge zónát.

Következmény:
- **Fold 1 train tartalmazza fold 2/3/4 valid periódusait** (2024-06-01 – 2026-05-31) —
  jövőbeli adatszivárgás az első foldban.
- Csak fold 4 train-je szimmetrikusan "tiszta" (nincs jövő valid adat, mert fold 4 az utolsó).
- A CV score-ok optimisták lehetnek, különösen a korai foldokon.

A szándékolt viselkedés: expanding-window walk-forward, ahol fold k train-je
csak az `open_time <= train_end_k` sorokat tartalmazza.

---

## Érintett fájl

`src/modeling/search/lgbm_search.py` — `_fold_split_walk_forward` függvény (~450. sor)
