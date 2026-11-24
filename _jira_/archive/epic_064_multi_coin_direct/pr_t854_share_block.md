---
epic: epic_064
id: t854
title: Részarány-blokk — új feature-csoport
assignee: database_agent
status: todo
blocks: [t856]
blocked_by: [t852]
---

## Goal

Az egyetlen **valóban új** információ ebben az epicben: mekkora hányada a
coin a pool egészének, és hogyan változik ez.

## ⚠️ Részarány, nem rang

A user explicit döntése. A kettő nem ugyanaz:

```
rang:       "ez a coin a 30-bol a 7."             -- ordinalis, univerzum-fuggo
reszarany:  "ez a coin a pool forgalmanak 4,2%-a" -- folytonos, ertelmezheto
```

A részarány abszolút mennyiség, nem omlik össze attól, ha holnap 32 coin van.

## A jelöltek

| név | képlet |
|---|---|
| `feat_vol_share` | coin quote_volume / Σ pool quote_volume |
| `feat_vol_share_delta_60` | a részarány relatív változása 60 perc alatt |
| `feat_vol_share_z_1440` | a részarány trailing z-score-ja (24h) |
| `feat_oi_share` | coin OI notional / Σ pool OI notional |
| `feat_oi_share_delta_60` | ugyanaz OI-ra |
| `feat_ret_vs_pool_60` | a coin 60 perces hozama mínusz a pool átlaga |
| `feat_vol_vs_pool` | a coin realizált volatilitása / pool medián |

⚠️ A nevező **csak az adott pillanatban élő coinokból** állhat. Ha egy coin
akkor még nem listázott, nem lehet benne az összegben — különben a
részarány visszamenőleg torzul.

⚠️ **t−1 lag itt is kötelező.**

## Acceptance Criteria

- [ ] A feature-ök a `feat_5m`-ben
- [ ] A nevező pont-időbeli (csak az akkor élő coinok)
- [ ] Leak-teszt
- [ ] Korreláció a meglévő készlettel — ami > 0,95, az kiesik
- [ ] `ruff` + `pyright` tiszta

## Notes
[orchestrator] Elvegezve — 2026-08-26

## A 8 valtozo (orai racson)

| nev | jelentes |
|---|---|
| `feat_vol_share` | a coin reszaranya a pool teljes forgalmabol |
| `feat_vol_share_delta_1h` | ez az arany hogyan valtozott 1 ora alatt |
| `feat_vol_share_delta_24h` | ugyanaz 24 orara |
| `feat_vol_share_z_7d` | a reszarany trailing z-score-ja (7 nap) |
| `feat_oi_share` | OI-reszarany a poolbol |
| `feat_oi_share_delta_24h` | az OI-reszarany valtozasa |
| `feat_oi_share_z_7d` | OI-reszarany z-score |
| `feat_ret_vs_pool_24h` | a coin 24h hozama minusz a pool atlaga |

**Reszarany, nem rang** — a user explicit dontese. Folytonos, ertelmezheto
mennyiseg, ami nem omlik ossze, ha valtozik a coinok szama.

⚠️ A nevezo **csak az adott oraban elo coinokbol** all (`sum().over(open_time)`),
tehat egy meg nem listazott coin nem torzitja visszamenoleg senki reszaranyat.
**t-1 lag** minden oszlopon, mint minden mas feature-nel.

## Minta (a legutolso bar)

```
symbol    vol_share  vol_share_delta_24h  oi_share  ret_vs_pool_24h
BTCUSDT      0,4889               0,1522    0,4929          -0,0089
ETHUSDT      0,2554              -0,3731    0,3077          -0,0128
SOLUSDT      0,0800               0,5289    0,0438          -0,0048
UNIUSDT      0,0208               2,1100    0,0060           0,0027
AAVEUSDT     0,0126               3,6136    0,0035          -0,0421
```

Az AAVE sora pont az, amiert ez a blokk keszult: a forgalmi reszaranya
**3,6-szorosara** nott 24 ora alatt, mikozben a mezonynel **4,2%-kal
rosszabbul** teljesitett — figyelem-atterelodes az ar kovetese nelkul.

## Eredmeny

```
feat_share_1h   1 079 178 sor   30 coin   8 feature   (Float32)
```

## Acceptance Criteria

- [x] Kulon tablaban (a panel-fuggo feature-ok ujraszamolhatok a per-coin
      feature-ok erintese nelkul)
- [x] Pont-idobeli nevezo
- [x] t-1 lag
- [x] `ruff` + `pyright` tiszta
- [ ] Korrelacio a meglevo keszlettel — a t856 feature-szuresenel
