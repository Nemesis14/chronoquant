---
epic: epic_058
id: t701
title: Triple-barrier címke a target táblába
assignee: database_agent
status: pr
blocks: [t702]
blocked_by: []
---

## Goal

Új target oszlop: melyik korlátot éri el előbb az ár a belépés után.

## A címke

```
tb_label_240_100  TINYINT
  +1  az ar eleri a +100 bps-t ELOBB, mint a -100 bps-t
  -1  az ar eleri a -100 bps-t ELOBB
   0  egyiket sem 240 percen belul
```

Referencia-ár: az adott perc `close` értéke.
Felső korlát: `close * 1.01`, alsó: `close * 0.99`, ablak: `t+1 .. t+240`.

## ⚠️ Kötelező szabályok

1. **Intrabar érintés számít**, nem a záróár: a felső korlátot a `high`, az
   alsót a `low` váltja ki. Ez egyezik a strategy szimulátor logikájával.
2. **Ha ugyanabban a percben mindkettőt érinti**, az alsó nyer (konzervatív) —
   ugyanaz a `same_bar_conflict_rule: sl_first` konvenció, mint a strategy-ben.
3. **Az utolsó 240 sor NULL**, nem 0. A 0 valódi osztály (timeout), nem
   hiányzó adat — ezt összekeverni csendes adathiba lenne.
4. Idempotens: újrafuttatás ugyanazt adja.

## Ellenőrző szám

A 2025-08-01 → 2026-07-31 ablakon, óránkénti mintán a mért eloszlás:

| +1 | −1 | 0 |
|---|---|---|
| 32,9% | 36,0% | 31,0% |

Ha az implementáció ettől érdemben eltér, valami elromlott — ez a horgony.

## Acceptance Criteria

- [ ] `tb_label_240_100` oszlop a `target` táblában
- [ ] Az eloszlás ±1pp-n belül reprodukálja a fenti horgonyt
- [ ] Az utolsó 240 sor NULL
- [ ] Egységteszt a same-bar ütközésre (alsó nyer)
- [ ] `quant_train` átviszi az oszlopot (a target-felfedezés már dinamikus)
- [ ] `ruff check` + `pyright` tiszta

## Notes
