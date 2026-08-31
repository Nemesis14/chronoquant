---
epic: epic_058
id: t704
title: Egyszerűsített strategy réteg — fix TP/SL
assignee: modeling_agent
status: todo
blocks: [t705]
blocked_by: [t703]
---

## Goal

A strategy réteg lecsupaszítása: a TP és SL **konstans**, nem a predikcióból
származik.

## Ami eltűnik

- `rank_lookup_long/short.parquet`
- `isotonic_long/short.pkl`
- `bucket_mean/median/p75_mfe` kalibráció
- a `tp_spec` × `sl_spec` rácskeresés

**A rács 200 setupról 8 cutoff értékre zsugorodik** (a `P(+1)` küszöb).

Ez nem csak egyszerűsítés: ma azt mértük, hogy a predikcióból származtatott
TP volt a fő instabilitás-forrás. Kevesebb szabadsági fok = kevesebb
túlillesztés.

## Az új döntési szabály

```
ha P(+1) >= kuszob  ->  LONG,  TP = +100 bps, SL = -100 bps, max 240 perc
ha P(-1) >= kuszob  ->  SHORT, TP = -100 bps, SL = +100 bps, max 240 perc
kulonben            ->  nincs pozicio
```

## Acceptance Criteria

- [ ] A strategy session fix TP/SL-lel fut, kalibráció nélkül
- [ ] A költség-modell (epic_055) változatlanul érvényes
- [ ] A rács 8 küszöbérték, nem 200 setup
- [ ] Out-of-sample eredmény a 12 hónapos ablakon
- [ ] `ruff check` + `pyright` tiszta

## Notes
