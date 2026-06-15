---
epic: epic_002_feature_quality
id: t2
title: min_samples hiányok javítása rolling window feature számításokban
assignee: database_agent
status: pr
blocks: []
blocked_by: []
---

## Goal

Számos rolling számítás `min_samples` paraméter nélkül fut, így Polars
alapértelmezése (`min_samples=1`) érvényesül — az első ablakméret-1 sorban
részleges ablakból számolódik az érték ahelyett, hogy NULL lenne.

A szabály: ha egy feature `w`-bár ablakot igényel és nincs `w` megfigyelés,
az eredmény NULL kell legyen, nem részleges átlag/max/min.

## Scope

Érintett helyek `src/data_pipeline/_features_polars.py`-ban:

| Feature csoport | Függvényhívás | Javítás |
|---|---|---|
| Stochastic K | `rolling_max(w)`, `rolling_min(w)` | `min_samples=w` |
| Stochastic D | `stoch_k.rolling_mean(sw)` | `min_samples=sw` |
| Williams R | `rolling_max(w)`, `rolling_min(w)` | `min_samples=w` |
| Body ratio SMA | `rolling_mean(w)` × 2 | `min_samples=w` |
| Signed body SMA | `rolling_mean(w)` | `min_samples=w` |
| Wick imbalance SMA | `rolling_mean(w)` | `min_samples=w` |
| Parkinson vol | `rolling_mean(w)` | `min_samples=w` |
| GK vol | `rolling_mean(w)` | `min_samples=w` |
| Range expansion | `rolling_mean(short/medium)` | `min_samples=short/medium` |
| Volume/trade accel | `rolling_mean(short/medium)` | `min_samples=short/medium` |

A numpy-alapú (`_rolling_*_arr`) függvények jók — ezek `sliding_window_view`-t
használnak, ami automatikusan csak teljes ablakra számol (`result[w-1:]`).

## Acceptance Criteria

- [ ] Minden `rolling_max` / `rolling_min` / `rolling_mean` / `rolling_std` híváshoz `min_samples=w` van beállítva (kivéve ahol szándékosan nem kell, dokumentálva)
- [ ] `feat_rsi_14` esetén az első 13 sor NULL — igazolva
- [ ] `feat_stoch_k_14` esetén az első 13 sor NULL — igazolva (jelenleg nem az)
- [ ] Rebuild után a NULL sorok száma feature-enként `== ablakméret - 1` (+ t-1 lag)
- [ ] Numerikus egyenértékűség: a nem-érintett sorokon az értékek nem változnak
- [ ] `uv run pyright src/data_pipeline/` — 0 hiba
- [ ] `uv run pytest _tests/data_pipeline/ -v` — zöld

## Notes

Vizsgálat (2026-06-14): a grep eredmény alapján azonosított helyek a fenti táblában.
A fix után szükséges lehet egy teljes `sync_features` rebuild, mert az első
`ablakméret-1` sor értékei megváltoznak (NULL-lá válnak).
Az EWM-alapú (`ewm_mean(min_samples=w)`) számítások rendben vannak.
