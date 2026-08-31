# Epic 056: Alt-data verdikt megerősítése — short irány és második valid ablak

> **Kiváltó ok:** az `epic_054` GO verdiktje **egyetlen 6 hónapos ablakon** és
> **csak long irányban** áll. Ez erős jelzés, de nem bizonyíték.

## Goal

Eldönteni, hogy az alt-data +34%-os éle valós-e, vagy egy ablak szerencséje.

## Amit az epic_054 talált

Kontroll vs. kezelés, 2026-02 → 2026-07, long:

| | Kontroll | Kezelés |
|---|---|---|
| él / kötés (8 bps optimum) | 8,60 bps | **11,53 bps** |
| valid lift | 2,2757 | 2,2995 |
| top/alsó spread | 4,712 | **4,536** (romlott) |
| train–valid gap | 0,0019 | **0,0736** (38×) |

A modell-metrikák **nem** támasztják alá a javulást — csak a kereskedési
eredmény. Ez lehet valós (az alt-data a legfelső szűk sávban segít, ahol a
stratégia dolgozik), de lehet egyetlen ablak zaja is.

## Scope

- `config/models.json` — 4 új modell bejegyzés
- új snapshotok a második ablakhoz (a `--exclude-altdata` flaggel)
- `_doc_/research/altdata_gate.md` — verdikt frissítése

Nem érinti: `src/`, ez tisztán futtatás és kiértékelés.

## Tasks

- t501: Short irány — kontroll és kezelés modell → modeling_agent
- t502: Második valid ablak — long, kontroll és kezelés → modeling_agent
- t503: Összesített verdikt és az epic_054 riport frissítése → analyst_agent

Sorrend: (t501 ∥ t502) → t503

## Key Decisions

- **A második ablak visszafelé megy**, nem előre: train 2023-01 → 2025-07,
  valid 2025-08 → 2026-01. Előrefelé nincs adat (a `metrics` T-1-ig ér, és a
  2026-08 még nem teljes hónap).
- **A két ablak nem független.** Átfedő tanítóadaton osztoznak, tehát a
  megerősítés gyengébb, mint két teljesen külön minta. Ezt a riportnak ki kell
  mondania, nem elhallgatnia.
- **A short irány önálló teszt.** A `short_mfe_fw60` negatív értékű és
  invertált rangsorú; ha az alt-data ott is hoz, az érdemi megerősítés, mert
  más a mechanizmus.
- **A költség-modell (epic_055) előbb fusson le**, ha időben belefér — akkor
  ez az epic már nettó objectívvel mérhet, és nem kell utólag korrigálni.

## Notes

Előzmény: `epic_054`, riport `_doc_/research/altdata_gate.md`.
A Tier 4 (`aggTrades`, 10–15 GB) döntése erre az epicre vár.

---

## ⚠️ 2026-08-26 — AZ EPIC ELAVULT, FÜGGESZTVE

Ez az epic az alt-data élét a **`long_mfe_fw60` regressziós targeten** méri.
Azt a targetet az `epic_058` / [horizon_and_target_diagnosis.md](../../_doc_/research/horizon_and_target_diagnosis.md)
**lezárta**: a modell rajta volatilitást tanul, nem irányt.

Egy lezárt ág megerősítése nem hoz döntést. Két lehetőség:

1. **Átírni** triple-barrier / ATR-skálázott targetre — akkor viszont ez már
   az `epic_061` gate-jének a része (kontroll vs. kezelés azonos protokollal)
2. **Lezárni**

**Döntés: lezárva.** A benne megfogalmazott kérdés — „valós-e az alt-data éle
vagy egy ablak szerencséje" — az `epic_061` t821 ablációs mérésében él tovább,
a helyes targeten.
