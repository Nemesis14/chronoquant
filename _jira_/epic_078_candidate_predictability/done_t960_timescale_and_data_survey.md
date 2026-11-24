---
epic: epic_078
id: t960
title: Időtáv-táblázat és adat-felmérés
assignee: analyst_agent
status: todo
blocks: [t961]
blocked_by: []
---

## Goal
Hogy lássuk, milyen időtávon élnek a modellek: skálánként (60/240/960 perc, k=4) az első lökés hossza, nagysága, átlagos range, a teljes mozgás hossza (perc és óra). Plusz felmérés: milyen adat van a lab DB-ben a változócsoportokhoz (volumen, taker flow, ügyletszám, funding, OI).

## Acceptance Criteria
- [ ] Táblázat skálánként: első lökés / teljes mozgás hossza (medián, p25, p75), lökés mérete %-ban, átlagos range.
- [ ] Adat-felmérés: melyik változócsoporthoz van adat 2021-02-től, hiányok jelezve.
- [ ] OOS érintetlen; ruff + pyright tiszta.

## Notes

[analyst] 2026-11-24 — kész.
- Kód: `src/reports/builders/cand_pred_timescale.py`; tábla `research."cand_pred_v1_timescale"` (btcusdt lab DB).
- Időtáv (k=4, medián [p25–p75], perc): jelölt-megerősítés a mélypont után 57 / 209 / 886 (T=60/240/960) ≈ 0,9 T; nagy mozgás hossza 305 [125–643] / 1277 [539–2472] / 5618 [2624–9146] ≈ 5 T; nettó méret 3,2% / 6,6% / 13,5% (≈ 6σ_T); σ_T = 0,55% / 1,09% / 2,13%; átlagos T-perces ablak-range 0,84% / 1,74% / 3,60%.
- A c=0,33 szintű "első lökés" a mozgások ~86%-ánál maga a teljes mozgás (share = 1,0 a mediánnál) — az "első szakasz" ezért a gyakorlatban a megerősítésig tartó rész (~0,9 T), és a jóslás ennek ismeretében történik.
- Jelöltek száma k=4: 4227 / 1221 / 346; P(nagy) = 0,42 / 0,41 / 0,40.
- Adat-felmérés (btcusdt live DB): `ohlcv` 2021-01-01-től teljes 1m: volume, quote_volume, trades (180 nulla perc), taker_buy_base/quote (nincs hiány) → volumen, VWAP (quote_volume/volume), taker-flow, ügyletszám csoportok rendben. `funding_rate` 8 órás, 2021-01-től (6461 sor) → funding csoport használható, lassú. **OI nincs** → kimarad. `feat_ohlcv_quant` 1m, 2021-01-től, `available_ts` oszloppal → a quant/klasszikus csoport kauzális forrása. `feat_levels` üres.
- Statisztikai erő: a T=960 cellában csak 346 jelölt (~140 nagy) van — az idő szerinti validációs év (2025) ~1/5-e ennek, ezért t962-ben a "tanítás 2021–24, validáció 2025" helyett **expanding-window éves fold-ok (teszt: 2023, 2024, 2025)** kerülnek, hogy a kiértékelő minta ~háromszoros legyen. Eltérés a jóváhagyott tervtől, a riportban jelölni.
