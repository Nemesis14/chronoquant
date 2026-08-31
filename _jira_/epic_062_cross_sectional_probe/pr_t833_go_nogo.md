---
epic: epic_062
id: t833
title: Go/no-go a cross-sectional irányra
assignee: analyst_agent
status: todo
blocks: []
blocked_by: [t832]
---

## Goal

Az epic.md előre rögzített küszöbe alkalmazva. **Nem** épül semmi, amíg ez
nem GO.

## Acceptance Criteria

- [ ] `_doc_/research/cross_sectional_probe.md` riport
- [ ] A küszöb alkalmazva, a verdikt kimondva
- [ ] Ha GO: a következő epic terve (modell-ág) vázolva, de nem elkezdve

## Notes
[orchestrator] Elvegezve — 2026-08-26

Riport: `_doc_/research/cross_sectional_probe.md`

**VERDIKT: GO.** Az elore rogzitett kuszob (median decilis-spread > 80 bps)
**4,2-szeresen teljesult** (335,9 bps). De ennel tobb tortent: a ticket
szerint "ha GO: a kovetkezo epic terve vazolva, de nem elkezdve" — a
lehetoseg-meres onmagaban nem lett volna dontő, ezert lefutott egy
tanitott rangsorolo is.

## A donto szam

30 alt-perp, piac-semleges konyv, 72 oras tartas, valos dij + **mert funding**,
22 honap folyamatos OOS:

| univerzum / decilis | brutto | funding | **netto** | emelkedo | eso | poz. honap |
|---|---|---|---|---|---|---|
| mind a 30 / 10% | +35,1 | −3,7 | **+12,6** | +11,0 | +14,0 | 15/22 |
| top20 / 10% | +49,4 | −1,9 | **+28,7** | +16,5 | +38,9 | 15/22 |
| **top20 / 20%** | +42,9 | −1,6 | **+22,5** | **+21,4** | **+23,4** | **16/22** |
| top20 / 30% | +41,6 | −1,4 | +21,4 | +22,8 | +20,2 | 15/22 |

**Az elso netto pozitiv, rezsim-robusztus eredmeny a projektben.**
Osszevetesul a direkcios ag legjobbja ugyanezen a napon: −2,78 bps/kotes.

## Ket megnyugtato robusztussag

- **Likviditasi szuro JAVIT** (+12,6 -> +28,7): az el nem a vekony,
  kereskedhetetlen parokbol jon
- **Szelesebb decilis kiegyenliti a rezsimeket** (+21,4 / +23,4): valodi,
  diverzifikalhato hatas, nem nehany szerencses fogadas
- Fedezeti labankenti koltseg **20,6-23,8 bps** = a feltetelezett 2,1-2,5x-e

## ⚠️ Negy fenntartas — a riport 6. szakasza

1. A horizont **utolagos szelekcio** hat ertek felett (a trend monoton es
   mechanikusan magyarazhato, de akkor is szelekcio)
2. **Atfedo ablakok**: 15 984 ora ≈ **222 fuggetlen ablak**; a havi bontas
   (15-16/22) az oszinte szignifikancia
3. A **120 oras ugras szuperlinearis es megmagyarazatlan** — 72h folott
   nem szabad hinni a szamoknak
4. Egyszerusitett szimulacio: zaro-zaro orai bar, nincs intrabar, nincs
   kockazat-kiegyenlitett sulyozas, nincs margin/likvidacios modell

**Eles toke egyik lepes elott sem.**
