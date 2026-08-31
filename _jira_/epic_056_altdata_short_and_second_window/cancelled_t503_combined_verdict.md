---
epic: epic_056
id: t503
title: Összesített verdikt és az epic_054 riport frissítése
assignee: analyst_agent
status: todo
blocks: []
blocked_by: [t501, t502]
---

## Goal

Egyetlen táblában látni mind a négy összevetést, és kimondani a végső ítéletet.

## Az összesítő tábla

| Kísérlet | Irány | Valid ablak | Kontroll bps/kötés | Kezelés bps/kötés | Δ |
|---|---|---|---|---|---|
| epic_054 | long | 2026-02 → 2026-07 | 8,60 | 11,53 | +34% |
| t501 | short | 2026-02 → 2026-07 | ? | ? | ? |
| t502 | long | 2025-08 → 2026-01 | ? | ? | ? |

## A döntési szabály — előre rögzítve

| Kimenet | Verdikt |
|---|---|
| Mindhárom pozitív | **Megerősítve.** Tier 4 (aggTrades) indulhat, és az alt-data mehet az éles modell-refresh ciklusba. |
| Kettő pozitív, egy semleges | **Valószínűleg valós**, de Tier 4 még ne. |
| Vegyes vagy negatív | **Az epic_054 eredménye egy ablak zaja volt.** Az alt-data réteg megmarad, de nem bővítjük. |

Ezt a szabályt **most** rögzítjük, nem az eredmények ismeretében — különben
utólag magyarázzuk meg, ami kijött.

## Acceptance Criteria

- [ ] `_doc_/research/altdata_gate.md` frissítve az összesítő táblával
- [ ] A fenti döntési szabály alkalmazva, az eredmény kimondva
- [ ] Tier 4 go/no-go
- [ ] Ha a verdikt megfordul, azt ugyanolyan egyértelműen kell leírni, mint a GO-t volt

## Notes
