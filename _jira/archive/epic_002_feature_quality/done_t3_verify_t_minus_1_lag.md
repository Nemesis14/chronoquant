---
epic: epic_002_feature_quality
id: t3
title: t-1 lag garancia ellenőrző script és tesztek
assignee: database_agent
status: pr
blocks: []
blocked_by: [t2]
---

## Goal

Igazolni, hogy a `feat_ohlcv_quant` táblában minden feature kizárólag
`open_time <= t-1` adatot tartalmaz — nincs jelenbeli (t) vagy jövőbeli (t+k)
adat. A `T_MINUS_1_SKIP` halmaz kivételek listáját tartalmazza (time-index
features), ezek tudatosan nem shiftelt értékek.

Két ellenőrzési szintet kell lefedni:
1. **Statikus elemzés** — kód-szintű audit: mikor hívódik `shift()` és mikor nem
2. **Adatszintű ellenőrzés** — `available_ts <= open_time` invariáns, + korrelációs szúrópróba

## Scope

- `src/data_pipeline/_features_polars.py` — t-1 lag alkalmazás logikája
- `src/data_pipeline/sync_features.py` — `available_ts` beállítása
- `_tests/store/` — új ellenőrző script: `test_feature_lag_invariants.py`

### Tesztek tartalma

1. `available_ts <= open_time` — minden sorban (már meglévő teszt kiterjesztése)
2. Korrelációs szúrópróba: `feat_rsi_14[t]` == `rsi(close[:t-1], 14)` — mintavételes igazolás
3. `feat_close_position[t]` nem egyenlő `close[t]`-vel (csak `close[t-1]`-gyel)
4. `T_MINUS_1_SKIP` lista minden tagja megtalálható a táblában (nincs elveszett feature)
5. A shift után az első sor minden OHLCV-alapú feature esetén NULL

## Acceptance Criteria

- [ ] `test_feature_lag_invariants.py` fut és zöld
- [ ] Legalább 5 különböző feature-típus korrelációs szúrópróba-tesztelve
- [ ] `T_MINUS_1_SKIP` tagok igazolva: jelenlegi `open_time` adathoz kötöttek
- [ ] `uv run pytest _tests/store/test_feature_lag_invariants.py -v -s` kiadja az eredményeket

## Notes

Vizsgálat (2026-06-14): `_features_polars.py` dokumentációja szerint
"t-1 lag guarantee: all OHLCV-based features are shifted by 1 bar before write".
A kód `T_MINUS_1_SKIP` frozenset-et definiál az exempt feature-ökhöz.
`available_ts` és `lookback_end_ts` a `sync_features.py`-ban kerül beállításra.
Ez a task a t2 (min_samples fix) után futtatandó, hogy a nullok is helyesek legyenek.
