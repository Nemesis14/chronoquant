---
epic: epic_051
id: t7
title: Kutatási riport és megy/nem-megy döntés
assignee: —          # főszálon fut, agent nélkül
status: done
blocks: []
blocked_by: []
---

## Goal

A `t1`–`t6` eredményeinek összefoglalása egyértelmű **megy / nem megy**
döntéssel, ami eldönti az `epic_052` sorsát.

## Scope

```
_doc_/research/tradingview_author_skill.md   ← a riport
```

**Eltérés az eredeti tervtől:** az `s102` „riport notebook"-ot ír. Notebook
helyett markdown dokumentum készült a `_doc_/research/` alá, a meglévő
`joint_feature_param_search.md` mintájára. Indok: nincs benne futtatandó
kód vagy ábra, amit egy notebook indokolna — a számok reprodukálását a
`validate.py` és a `score_authors.py` biztosítja, nem egy cella-sorozat.

## Acceptance Criteria

- [x] A riport tartalmazza a célt, módszert, eredményeket, limitációkat
- [x] **Egyértelmű döntés kimondva**
- [x] Az erő-elemzés szerepel (mit tudtunk volna kimutatni)
- [x] A menet közben elvetett leletek dokumentálva
- [x] Reprodukálási lépések megadva

---

## DÖNTÉS — 2026-08-20

> ### NEM MEGY
>
> Nincs azonosítható szerző, akit érdemes lenne követni. FDR-korrekció után
> **nulla szerző** marad, mind az öt horizonton. A legjobb jelölt sem jobb,
> mint egy véletlen particionálás legjobbika (family-wise `p = 0,476`).

### Következmények

**`epic_052` (Reddit) → törölve.** A gate logikája az `epic.md`-ből: a
TradingView-n a címke tökéletes, mi nem gyártjuk. Ha itt nincs jel, a
Redditen — extraktor-zajjal, nagyságrenddel több munkával — végképp nem lesz.

**`epic_050` (leaderboard) → fut tovább.** Ott nem publikált véleményeket
mérünk, hanem valódi pozíciókat valódi tétekkel. Más populáció, más
hipotézis; ez az eredmény nem érinti.

### A döntés helyes olvasata

A kutatás **nem** azt mondja ki, hogy senkinek nincs képessége. Azt, hogy
**senki nem azonosítható**. Az erő-elemzés szerint csak ~0,9%/call méretű
valódi él lett volna kimutatható; egy reális, szerény él (0,2%/call)
láthatatlan maradt volna.

A gyakorlati cél szempontjából a kettő ugyanaz: ha nem tudod megmondani,
kire fogadj, a képesség léte akadémikus kérdés.

## Notes

**Ami megmarad a munkából:**

- **17 061 BTC call** teljes szöveggel és ground-truth iránnyal — kész
  kalibrációs set egy későbbi LLM-extraktorhoz. Az `s102` második indoka
  teljesült, függetlenül a hipotézis bukásától.
- **`btcusdt` OHLCV store** (4,7M perces gyertya, 2017-től) — BTC
  quantitative munkára is használható, a user kifejezett kérésére spot.
- **Forrásfüggetlen `calls` / `call_returns` séma** — ha a téma valaha újra
  előkerül, az infrastruktúra áll.

Teljes riport:
[_doc_/research/tradingview_author_skill.md](../../_doc_/research/tradingview_author_skill.md)
