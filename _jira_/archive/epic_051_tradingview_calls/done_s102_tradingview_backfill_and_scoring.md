---
epic: epic_051
id: s102
title: TradingView Ideas backfill és szerző-skill mérés
---

## Goal

SOL/BTC/ETH TradingView ideák historikus begyűjtése, normalizálás `call`
táblává, majd out-of-sample szerző-skill mérés — a végén egy egyértelmű
**megy / nem megy** döntéssel.

## Az ötlet

Minden publikált idea natívan tartalmazza:

```
author · symbol · direction (Long/Short/Education) · timeframe
published_at · title · szövegtörzs · chart snapshot
likes / boosts / views / comments · szerző követőszáma
```

Ez pontosan a `call` absztrakció (lásd [plan.md](../../plan.md) 3. szakasz),
**nulla átalakítással**. Ezért itt a mérés tisztán arról szól, hogy van-e jel —
nem keveredik bele, hogy jól olvastuk-e ki az irányt a szövegből.

Két dolgot nyerünk egyszerre:

1. **A hipotézis validációját** — van-e perzisztens szerző-skill.
2. **Egy ingyen kalibrációs setet a Reddit trackhez.** A TV ideáknál van
   szöveg *és* ismert ground-truth irány. Vagyis az `epic_052` LLM-extraktorának
   precision/recall-ja **ezen mérhető**, mielőtt éles adatra engednénk.
   Ez a második pont önmagában is indokolja az epicet.

## Terv

- [x] T1: Scraper PoC — egy szimbólum, egy oldal; mezőséma feltérképezése,
      paginálás és rate-limit viselkedés felmérése → főszálon, agent nélkül
      ([done_t1](done_t1_tradingview_scraper_poc.md))
- [x] T2: Backfill runner — **`/api/v1/ideas/` endpointon** (nem HTML!),
      BTC-only: felderítés `symbol=` lekérdezéssel → szerzőnkénti teljes
      history `by={username}`-nel; raw JSON válaszok megőrzése,
      `first_seen` mentése → főszálon ([done_t2](done_t2_tradingview_backfill_runner.md))
      ↳ az API-szerződést és a ~1000-1700 kéréses budget-becslést lásd:
        [research/tradingview_poc/README.md](../../research/tradingview_poc/README.md) „T1c"
- [x] T3: Normalizálás → `calls` tábla, `extraction=native`,
      idempotens `call_id` → főszálon
      ([done_t3](done_t3_normalize_calls_table.md))
- [x] T4: Forward return join a BTCUSDT spot OHLCV store-ból,
      h ∈ {1h, 4h, 1d, 3d, 7d} + **drift-korrekció** (BTC saját driftje ellen,
      nem béta — BTC-only scope) → főszálon
      ([done_t4](done_t4_forward_returns_drift.md))
- [x] T5: Szerző-skill scoring — empirical Bayes shrinkage,
      rolling out-of-sample rangsor → főszálon
      ([done_t5](done_t5_author_skill_scoring.md)) — **nem talált jelet**
- [x] T6: Validációs battery — placebo teszt (szerző-permutáció) + FDR
      korrekció → főszálon ([done_t6](done_t6_validation_battery.md))
- [x] T7: Riport és **megy/nem-megy döntés** → főszálon
      ([done_t7](done_t7_report_and_decision.md)) — **NEM MEGY**

## Acceptance Criteria

- [x] BTC ideák backfillelve, raw réteg megőrizve (SOL/ETH elhalasztva — BTC-first)
- [x] `calls` tábla feltöltve, minden sor visszavezethető a raw rétegre
- [x] Forward returnök drift-korrigáltan számolva (nem béta — BTC-only scope)
- [x] Szerző-rangsor **kizárólag `t` előtti adatból**, minden `t`-re
- [x] Placebo teszt lefutott, eredménye a riportban szerepel
- [x] A riport egyértelmű döntést mond ki: **`epic_052` törlődik**

## Notes

**Miért ez az első és nem a Reddit:** a Reddit ingyenes és mélyebb, de ott mi
gyártjuk a labelt. Ha ott nincs jel, nem tudjuk megmondani, hogy a szerzők
voltak-e rosszak vagy az extraktorunk. Itt ez a kérdés fel sem merül.

**Mennyiség:** reálisan pár tízezer idea a három szimbólumra. Szerzőnként ez
gyakran csak 10-30 call — ezért **kötelező a shrinkage**, különben a
„legjobb elemző" definíció szerint a legszerencsésebb lesz.

**T6 a legfontosabb task.** A placebo teszt a legolcsóbb és leggyilkosabb
ellenőrzés: ha véletlenszerűen permutált szerző-címkékkel is „kiemelkednek"
top szerzők, akkor nincs jel, csak overfit. Ezt a lépést nem szabad
kihagyni vagy a végére tolni.

**ToS:** a TradingView tiltja az automatizált gyűjtést, kereskedelmi cél
nélkül is. Saját kutatásnál a reális kockázat IP/account ban → lassú rate,
agresszív cache, egyszeri backfill után csak inkrementális.
Ha a projekt valaha termékké válna, a raw réteget újra be kell szerezni —
ezért van a `raw/` és a `calls/` külön rétegben.
