---
epic: epic_054
id: t307
title: Binance Vision letöltő — 5 perces altdata_5m tábla
assignee: database_agent
status: pr
blocks: [t308]
blocked_by: []
---

## Goal

Letölteni és DuckDB-be tölteni a Binance Vision derivatíva-adatokat, hogy a
t308 feature-réteg dolgozhasson rajtuk.

Ez **bulk letöltés, nem élő gyűjtés** — a t301 kimutatta, hogy ez az adat
2021-12-01-ig visszamenőleg elérhető, ~20 MB-ban.

## Scope

- `src/data_handling/collectors/derivatives/vision.py` — Vision letöltő
- `src/data_handling/collectors/derivatives/load.py` — parse + DuckDB betöltés
- `database/solusdt/solusdt.duckdb` → új `altdata_5m` tábla (main séma)

## Letöltendő

| Forrás | Útvonal | Felbontás | Ablak |
|---|---|---|---|
| metrics | `futures/um/daily/metrics/SOLUSDT/` | 5m | 2021-12-01 → T-1 |
| premium index | `futures/um/monthly/premiumIndexKlines/SOLUSDT/5m/` | 5m | 2021-12 → |
| funding | `futures/um/monthly/fundingRate/SOLUSDT/` | 8h | 2021-12 → |
| spot klines | `spot/monthly/klines/SOLUSDT/1m/` | 1m → 5m aggregálva | 2021-12 → |

## Tábla: `altdata_5m`

```
open_time TIMESTAMP PRIMARY KEY   -- 5 perces racs, UTC
oi_coin              DOUBLE       -- sum_open_interest (SOL)
oi_value             DOUBLE       -- sum_open_interest_value (USD)
ls_count             DOUBLE       -- count_long_short_ratio
ls_top_count         DOUBLE       -- count_toptrader_long_short_ratio
ls_top_sum           DOUBLE       -- sum_toptrader_long_short_ratio
taker_vol_ratio      DOUBLE       -- sum_taker_long_short_vol_ratio
prem_open/high/low/close DOUBLE   -- premium index OHLC
funding_last         DOUBLE       -- utolso elszamolt funding (ffill)
funding_next_ts      TIMESTAMP    -- kovetkezo elszamolas idopontja
spot_close           DOUBLE       -- spot zaroar az 5m bar vegen
spot_volume          DOUBLE       -- spot volumen az 5m barban
perp_close           DOUBLE       -- futures zaroar (a meglevo ohlcv-bol)
perp_volume          DOUBLE
```

## ⚠️ Szabályok

1. **Idempotens upsert** `open_time`-ra — a projekt konvenciója szerint.
   Nincs delete/truncate.
2. **Hiányzó napok NULL-lal**, nem 0-val, és a lefedettségi riport listázza őket.
3. A `metrics` napi fájl 288 sort tartalmaz — ha kevesebb, azt naplózni kell
   (tőzsdei karbantartási ablak).
4. Retry + backoff a letöltésnél; a CHECKSUM fájlok ellenőrizhetők.
5. Letöltött zip-ek cache-elve, hogy újrafuttatás ne töltsön újra.

## Acceptance Criteria

- [ ] `altdata_5m` feltöltve 2021-12-01 → T-1, 5 perces rács
- [ ] Lefedettségi riport: első/utolsó timestamp, hiányzó 5m barok száma
- [ ] Kétszeri futtatás nem duplikál
- [ ] `ruff check` + `pyright` tiszta

## Notes

[orchestrator] Elvegezve — 2026-08-21

Modulok: `src/data_handling/collectors/derivatives/vision.py` (letolto + parser)
es `load.py` (join + upsert). `ruff` es `pyright` tiszta.

`altdata_5m` feltoltve: **496 349 sor, 2021-12-01 -> 2026-08-20**, 5 perces racs.
Cache: `data/vision_cache/` (a zip-ek maguk a raw reteg, publikus URL-bol
byte-reprodukalhatok).

Ket archivum-csapda, amit kezelni kellett — mindketto csendben rontana el az
adatot, ha feltetelezessel mennenk el mellette:

1. **Fejlec csak az ujabb fajlokban van.** A 2022-es premiumIndexKlines-ban
   nincs, a 2025-osben van. Fajlonkent detektalva az elso token alapjan.
2. **A spot kline timestamp 2025 folyaman MIKROSZEKUNDUMRA valtott**, mikozben
   a futures fajlok ms-ban maradtak. Ertekenkent detektalva nagysagrend szerint,
   es minden forras us pontossagra normalizalva (kulonben a join elszall).

Tovabbi adatminosegi eszrevetel: a 2022-es metrics fajlokban **ures mezok**
vannak, ezert minden numerikus cast `strict=False`.

**LEFEDETTSEG — ez befolyasolja a modell-ablakot:**

| oszlop | 100%-os lefedettseg kezdete |
|---|---|
| `oi_coin` / `oi_value` | 2021-12 |
| `ls_count` | 2022-02 |
| `taker_vol_ratio` | 2022-06 |
| `ls_top_count` / `ls_top_sum` | **2023-01** |

2022-ben a top trader oszlopok gyakorlatilag uresek (eves szinten 13%).
Ez 3 valtozot erint a 22-bol, koztuk a `feat_ls_retail_vs_top_60`-at.
Dontes a t309/t310 train ablakarol meg nyitott.

**Ismert hianyossag:** a `prem_close` es `spot_close` 2026-07-31-ig van meg,
mert a havi archivum az aktualis honapra (2026-08) meg nem jelent meg. A valid
ablak 2026-07-31-en zarul, tehat a modellezest nem erinti — de eles hasznalat
elott a napi archivumbol vagy REST-bol potolni kell.
