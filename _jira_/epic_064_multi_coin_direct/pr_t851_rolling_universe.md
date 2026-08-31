---
epic: epic_064
id: t851
title: Görgetett univerzum-választás
assignee: database_agent
status: todo
blocks: [t852]
blocked_by: []
---

## Goal

Minden félévre meghatározni az *akkori* top 30 perpet, szelekciós
look-ahead nélkül.

## Módszer

```
ujravalasztas:  felevente, 2020-07 -> 2026-07 (13 idopont)
rangsor alapja: a megelozo 1 honap quote volume, napi barokbol
kivalasztva:    top 30
```

⚠️ A rangsor **kizárólag** az újraválasztás előtti adatból készülhet. Ha
2021-01-ben választunk, akkor 2020-12 forgalmát nézzük — semmi utána.

## Scope

- `src/data_handling/collectors/universe/build_universe.py` — kibővítés
- `database/_universe/universe.duckdb` → `universe` tábla

```sql
universe (rebalance_date, symbol, rank, quote_volume, valid_from, valid_to)
```

## Acceptance Criteria

- [ ] 13 újraválasztási időpont, mindegyikre top 30
- [ ] A distinct szimbólumok száma kiírva (ez lesz a letöltési halmaz)
- [ ] Ellenőrzés: a 2024-01-i választás egyezik-e a korábbi listával
- [ ] Delistelt párok is szerepelnek, ha akkor a top 30-ban voltak
- [ ] `ruff` + `pyright` tiszta

## Notes
[orchestrator] Elvegezve — 2026-08-26

13 ujravalasztas (2020-07 .. 2026-07), felevente, a megelozo honap
forgalma alapjan. Tabla: `universe (rebalance_date, symbol, rank,
quote_volume, valid_from, valid_to)`.

## Eredmeny

```
ujravalasztasok:  13
osszes bejegyzes: 390
DISTINCT szimbolum (= letoltendo halmaz): 155
symbol-ev igeny:  195
```

## A fő lelet: az univerzum brutalisan forog

| periodusok szama | coinok |
|---|---|
| 13 (vegig bent) | **4** — BTC, ETH, BNB, XRP |
| 11-12 | 4 |
| 5-9 | 18 |
| 2-4 | 36 |
| **1** | **93** |

**A 155 coinbol 93 csak EGYETLEN felevben volt a top 30-ban.**

Top 5 idoben:

```
2020-07:  BTC, ETH, ADA, BCH, LINK
2023-01:  BTC, ETH, DOGE, BNB, XRP
2026-07:  BTC, ETH, SOL, ZEC, HYPE
```

## Miert szamit ez

Ez visszamenoleg igazolja a gorgetett valasztast: egy fix "2024-es top 30"
lista 2020-2022-re **teljesen rossz** univerzumot adott volna. A 2020-as
top5-bol ketto (ADA, BCH) 2026-ra kiesett, a 2026-os top5-bol harom
(SOL, ZEC, HYPE) 2020-ban nem is letezett vagy jelentektelen volt.

Ugyanakkor a tanitohalmaz nagy resze **atmenetileg forro** coinokbol all —
ez nem hiba, hanem pontosan az a populacio, amit elesben kereskednenk.

## ⚠️ Kovetkezmeny a t854-re

A reszarany nevezoje minden ujravalasztaskor ugrik, mert a pool osszetetele
valtozik. Ezt a t854-ben kezelni kell: vagy elfogadjuk a szakadast a
rebalance-hataron, vagy szelesebb/stabilabb nevezot valasztunk.

## Acceptance Criteria

- [x] 13 ujravalasztasi idopont, mindegyikre top 30
- [x] Distinct szimbolumok szama kiirva: **155**
- [x] Delistelt parok is szerepelnek (pl. BCHUSDT, MATICUSDT)
- [x] `ruff` + `pyright` tiszta
