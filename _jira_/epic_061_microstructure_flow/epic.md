# Epic 061: Mikrostruktúra — order flow imbalance

> **A helyesbítés, amivel indul:** a `t705` riport azt írja, a `bookDepth`
> „megvan és soha nem használtuk". Ez **téves** — az 5 mélység-feature
> (`feat_depth_imb_1/5`, `feat_depth_slope`, `feat_depth_total_z_240`,
> `feat_depth_drain_60`) benne van a `quant_train`-ben, tehát a +9,3pp-es
> eredmény **már tartalmazza őket**. Amit nem tudunk: mennyit adtak hozzá.

## Goal

Két kérdés, ebben a sorrendben:

1. Mennyi a `bookDepth` **marginális** hozzájárulása? (olcsó, az adat megvan)
2. Ad-e többet az `aggTrades`-ből számolt **order flow imbalance**?

## Miért épp az OFI

A jelenlegi 235 feature gyakorlatilag mind **ár-származtatott** — ugyanannak
az OHLCV idősornak a transzformációi. A feature importance ezt vissza is
igazolja: a top 4 mind volatilitás, együtt 43,9% gain.

Az `aggTrades` más információ: **ki kezdeményezett**. A taker buy/sell
imbalance és a kötésméret-eloszlás az egyetlen elérhető adat, ami rövid
horizonton *irányt* hordoz, nem csak magnitúdót.

Költség: 10–15 GB, **ingyenes** (Binance Vision, 2020-09-től). Nincs
szolgáltatói függés.

## Scope

- `src/data_handling/collectors/derivatives/vision.py` — aggTrades letöltés
- `src/data_handling/sync_tables/_altdata_features_polars.py` — OFI blokk
- `_doc_/research/microstructure_gate.md`

## Tasks

- t821: bookDepth marginális hozzájárulás (A/B, az adat megvan)
- t822: aggTrades archívum letöltés és 1 perces aggregálás
- t823: OFI feature-réteg
- t824: Gate mérés és verdikt

Sorrend: t821 ∥ t822 → t823 → t824

## A döntési szabály — előre rögzítve

| Kimenet | Verdikt |
|---|---|
| Az OFI-val a lift **mindkét rezsimben** > +2pp-vel nő | **GO** |
| A lift nő, de csak az egyik rezsimben | gyenge — nem elég önmagában |
| Δlift ≤ +1pp | **NO-GO** — a mikrostruktúra sem hoz irányt ezen a horizonton |

⚠️ A `bookDepth` 2023-01-től van, az `aggTrades` 2020-09-től. A két
lefedettségi határ **nem keverhető**: a gate mérés a szűkebbik ablakon fut,
a kontroll ugyanazon.
