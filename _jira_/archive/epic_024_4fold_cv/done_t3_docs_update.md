---
epic: epic_024
id: t3
title: Dokumentáció frissítése 4-fold CV struktúrára
assignee: methodology_agent
status: pr
---

## Goal

A sampling dokumentációt frissíteni az új 4-fold stratifikált CV leírással. A régi
12-fold train/valid/purge segment struktúra leírása helyett az új fold_id alapú
megközelítést dokumentálni.

## Scope

- `_doc_/5010_sampling_yearly.md` — teljes átírás szükséges
- `_doc_/5400_sampling.md` — ellenőrizni, szükség esetén frissíteni
- `_doc_/5420_sampling_audit.md` — ellenőrizni, szükség esetén frissíteni

## Tartalom amit le kell dokumentálni

### Miért 4-fold (és nem 12)?

**Régi probléma (12-fold):**
- Egy search trial-ban 12 modell fut, mindegyik ugyanazon a ~6,648 train soron tanul
- Valid/fold: csak 168 sor → instabil metrika
- A 12 "fold" nem valódi CV, csak 12 különböző eval ablak azonos train adaton

**Új megközelítés (4-fold stratifikált CV):**
- Minden hét az évben kap fold_id-t 1-4 között (hónapon belül stratifikálva)
- Hónapon belül a hetek random shufflelve, ciklikusan kapnak 1-2-3-4 labelt
- Fold k train-je = az összes többi fold hete (~36 hét, ~6,048 sor)
- Fold k valid-ja = fold k összes hete (12 hét az évből, ~2,016 sor)
- Valódi rotáció: minden foldban más train + más valid
- Stabil metrika: 2,016 sor / fold (12× több mint régen)

### Fold assignment logika

```
Minden hónapban:
  mondays = [hét1, hét2, hét3, hét4]  (5 ha sok hétfő van)
  rng.shuffle(mondays)
  hét1 → fold 1
  hét2 → fold 2
  hét3 → fold 3
  hét4 → fold 4
  hét5 → fold 1  (ha van 5. hét)
```

Eredmény: minden fold kap 1 hetet / hónap → 12 hét összesen (~2,016 sor).

### Purge

Marad ±240 perc, de most **dinamikusan számolva** a search-ben (nem pre-komputálva
a sample-ban). A sample parquetban NEM szerepel purge szegmens.

### Metadata változások

```json
// RÉGI
{
  "selected_valid_weeks": [...12 elem...],
  "row_counts": {"train": 6648, "valid": 2016, "purge": 96}
}

// ÚJ
{
  "n_folds": 4,
  "fold_week_assignments": {
    "1": [{"start": "2021-01-XX", "end": "2021-01-XX"}, ...12 elem összesen...],
    "2": [...],
    "3": [...],
    "4": [...]
  },
  "fold_row_counts": {"1": 2016, "2": 2016, "3": 2016, "4": 2016}
}
```

### Parquet struktúra

```
// RÉGI: open_time | target | segment (train/valid/purge) | fold_id (0-11, null) | feat_*
// ÚJ:   open_time | target | fold_id (Int8, 1-4) | feat_*
```

### Per-fold méret (search-ben)

| | Régi (12-fold) | Új (4-fold) |
|---|---|---|
| Train sor / fold | 6,648 (fix, nem rotál) | ~6,048 (rotál!) |
| Valid sor / fold | 168 (1 hét) | ~2,016 (12 hét) |
| Fit per trial | 12 | 4 |

### Final fit

A final modell (train lépés) az összes hourly sort (~8,760) használja tanításhoz —
nincs segment szűrés. A fold_id csak CV metaadatként marad a parquetban.

## Acceptance Criteria
- [ ] 5010_sampling_yearly.md-ben az új 4-fold logika dokumentálva
- [ ] Régi train/valid/purge segment fogalmak eltávolítva / átírva
- [ ] Fold assignment logika (havi stratifikáció) elmagyarázva
- [ ] Purge: dinamikus számítás a search-ben, nem pre-komputált
- [ ] Metadata és parquet struktúra aktuális verziója dokumentálva

## Notes
