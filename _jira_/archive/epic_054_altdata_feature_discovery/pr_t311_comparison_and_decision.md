---
epic: epic_054
id: t311
title: Strategy session, összevetés és döntés a Tier 3/4-ről
assignee: analyst_agent
status: pr
blocks: []
blocked_by: [t309, t310]
---

## Goal

Eldönteni, hogy az alt-data réteg tovább él-e — és ha igen, meddig.

## Amit mérni kell

**1. Modell-szint (t309 vs t310)**

| metrika | C kontroll | T kezelés | különbség |
|---|---|---|---|
| valid `top10_lift` | | | |
| train-valid gap | | | |
| bent maradt alt-változók száma | 0 | ? | |

**2. Kereskedési szint**

Strategy session mindkét modellre, azonos beállítással:
`total_fact_log_return`, `win_rate`, `n_trades`, `compounded_return_pct`.

Ez a gyakorlati válasz — a `top10_lift` javulhat úgy is, hogy kereskedésben
nem látszik.

**3. Változó-szintű verdikt**

A 22 változó mindegyikére: bent maradt / kiesett gyenge / kiesett redundáns
(és mivel korrelált).

## ⚠️ Amire figyelni kell

- **A valid ablak csak 6 hónap** (2026-02 → 2026-07). Egyetlen ablakon mért
  javulás lehet zaj. Ha a különbség kicsi, mondd ki, hogy nem eldönthető —
  ne kerekítsd fel következtetéssé.
- **Az `epic_053` tanulsága:** a gyenge, instabil jel is "van jel"-nek
  látszik, ha csak az átlagot nézed. Nézd a stabilitást is.
- A `feat_altdata` `taker_vol_ratio`-ból származó változói **gyanúsan
  redundánsak** a meglévő `feat_taker_buy_base_ratio_*`-gal — ha nem estek ki
  automatikusan, nézd meg kézzel a korrelációt.

## A döntés

| Kimenet | Következmény |
|---|---|
| T érdemben jobb | Tier 3 (`bookDepth`) és Tier 4 (`aggTrades`) indul, külön epicben |
| T nem jobb, de vannak túlélő változók | szűkített készlet, nincs Tier 3/4 |
| T nem jobb, minden alt-változó kiesett | a derivatíva-ág lezárul; a t302 likvidációs gyűjtés folytatásáról külön döntés |

## Acceptance Criteria

- [ ] `_doc_/research/altdata_gate.md` riport a fenti szekciókkal
- [ ] Minden verdikt mögött szám áll
- [ ] Explicit go/no-go a Tier 3 és Tier 4 kérdésre
- [ ] Döntés a t302 likvidációs collector sorsáról

## Notes

[orchestrator] Elvegezve — 2026-08-22

Riport: `_doc_/research/altdata_gate.md`

**Verdikt: GO, fenntartasokkal.**

- FE: 20 a 22 alt-valtozobol tulelt; nem szoritottak ki meglevo feature-t
  (kontroll 137, kezeles 156 kivalasztott feature).
- Feature importance: az alt-csoport egyuttes gain-reszesedese 8,97%, mikozben
  a feature-szam 12,8%-a. A sulya ALATT teljesit. Legjobb: 12. hely.
- Modell-metrika: a javulas elhanyagolhato (lift +1%), a top/also spread ROMLOTT
  (4,712 -> 4,536), a train-valid gap 38-szorosara ugrott.
- Kereskedes: 200 setupbol 122-ben jobb a kezeles; atlagos kulonbseg +0,0465.
- **Doonto: koltseg-tudatos optimum.** 8 bps kor-koltsegnel azonos gyakorisagon
  (415 vs 401 kotes) a kezeles 11,53 bps/kotest hoz a kontroll 8,60 bps-evel
  szemben -> +34% el.

**Legfontosabb mellektermek, ami nem alt-data kerdes:**
a `src/strategy/` SEHOL nem szamol dijjal/csuszassal. Emiatt a racskereses
szisztematikusan a tulkereskedo setupokat valasztja: a kezeles brutto optimuma
1984 kotes 2,52 bps/kotessel, ami reális taker dij mellett mélyen vesztesegés.
Ezt kulon epicben javitani kell, fuggetlenul ettol a vizsgalattol.

Tier 3 (bookDepth): GO. Tier 4 (aggTrades): varj a short iranyra es egy masodik
valid ablakra. t302 likvidacios collector: fusson tovabb.
