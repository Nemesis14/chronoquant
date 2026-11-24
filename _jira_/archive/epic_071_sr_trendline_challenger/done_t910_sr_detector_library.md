---
epic: epic_071
id: t910
title: Implementáld az S/R + trendvonal detektor könyvtárat look-ahead-mentesen
assignee: database_agent
status: done
blocks: [t911, t912]
blocked_by: []
---

## Goal
Tiszta (mellékhatás-mentes) Polars-alapú detektor könyvtár, amely 1m OHLCV-ből több léptéken
pivotokat, vízszintes S/R szinteket, trendvonalakat és eseményeket (touch/break/retest) állít elő,
minden objektumhoz `confirmed_at` időbélyeggel. **Ebben a taskban nincs DB írás** (a feature tábla a t912).

## Scope
- Új: `src/data_handling/sync_tables/_levels_polars.py` (vagy hasonló név a `_triple_barrier.py` mintájára)
- Paraméterek: `config/features.json` új `levels_detector` blokk, olvasás **csak** `src/utils.py`-n át
- Tesztek: `src/data_handling/tests/` alatt

## Spec
Paraméterek (a t909 módszertani doksival párhuzamosan készül, ez a spec az irányadó):
- Léptékek: `s` = 1h bar, lookback 7 nap; `m` = 4h bar, 30 nap; `l` = 1d bar, 180 nap. ATR14 a scale-baron.
- **Pivot:** fraktál, n=3 scale-bar mindkét oldalon, prominencia ≥ 1.0·ATR. `confirmed_at = pivot_time + n bar`.
- **Vízszintes szint:** a lookback-en belüli megerősített pivotok klaszterezése 0.25·ATR tűréssel, ≥2 érintés.
  Mezők: `level_id, scale, price, n_touches, first_touch, last_touch, confirmed_at, strength (Σ exp(-age/τ), τ=lookback/3),
  is_round (10 többszöröse; 5 másodlagos flag), role (support/resistance az aktuális árhoz képest), n_role_flips, broken_at`.
- **Trendvonal:** azonos típusú pivot-párokból jelöltek (emelkedő mélypontok = támasz, csökkenő csúcsok = ellenállás),
  ≥3 érintés ε=0.25·ATR-en belül (2 érintéses jelölt `n_touches=2` flaggel megtartható), a szélső érintések között
  nincs ε-nál nagyobb vonalon túli záróár. Duplikátumszűrés (átfedő vonalak közül a több érintéses marad).
  Mezők: `line_id, scale, kind, anchor_time/price ×2, slope_per_bar_atr, n_touches, confirmed_at, broken_at`.
- **Események:** `touch` (ár ε-on belül a megközelítés oldaláról), `break` (záróár 0.5·ATR-rel túl),
  `retest` (break után 12 scale-baron belül visszatér ε-on belülre). Mezők: `event_time, scale, obj_type (level/line),
  obj_id, event_type, side, price, atr`.
- **Kimenet-címkék (kutatáshoz, külön függvény, explicit `forward_` prefix):** touch → bounce (≥1·ATR elmozdulás 24
  scale-baron belül break előtt) / break; break → follow_through (≥1·ATR tovább 24 baron belül) / false_break;
  retest → hold / fail.
- **Look-ahead tilalom:** bármely T időpontban a detektor kimenete csak `confirmed_at ≤ T` objektumokat tartalmazhat.

## Acceptance Criteria
- [x] Publikus függvények: pivotok, szintek, trendvonalak, események, forward címkék (típusannotált, docstring)
- [x] Paraméterek a `config/features.json`-ből `utils.py`-n át, nincs nyers JSON olvasás
- [x] Teszt: szintetikus sor ismert szinttel/trendvonallal → detektor megtalálja
- [x] **Leakage teszt:** T-ig csonkolt adaton futtatva a `confirmed_at ≤ T` objektumok azonosak a teljes futás objektumaival
- [x] Teljesítmény: SOL teljes 1m történetre (2021–) mindhárom léptéken lefut ésszerű időn belül (mérd és írd a Notes-ba)
- [x] `ruff check` + `pyright` + pytest zöld
- [ ] `_doc_/database_and_code_doc/` releváns referencia-jegyzet (vagy Notes-ban jelezve a code_doc_agent felé)

## Notes

### t910 — database_agent (2026-11-19)

**Fájlok**
- új: `src/data_handling/sync_tables/_levels_polars.py` (tiszta könyvtár, nincs DB/fájl írás)
- új: `src/data_handling/tests/sync_tables/sanity/test_levels_detector.py` (9 teszt)
- módosítva: `config/features.json` — új top-level `levels_detector` blokk (a meglévő user-változások érintetlenek)
- módosítva: `src/utils.py` — új getter `load_levels_detector_config() -> dict`

**Publikus API** (`data_handling.sync_tables._levels_polars`)
- `detect_all_scales(df_1m, scales=None, params=None) -> dict[str, ScaleResult]` — t911 belépési pont
- `detect_scale(df_1m, scale, params=None) -> ScaleResult` (`bars, pivots, levels, level_touches, trendlines, line_touches, events`)
- építőkövek: `resample_ohlcv`, `detect_pivots`, `detect_levels`, `detect_trendlines`, `detect_events`
- kutatási címke: `forward_label_events(bars, events, params=None)` — JÖVŐT HASZNÁL, csak t913
- as-of pillanatkép (t912-höz is): `levels_as_of(res, T, active_only=True)`, `trendlines_as_of(res, T, active_only=True)`
- `params=None` → `utils.load_levels_detector_config()`; teszt/kutatás explicit dict-et adhat át
- A kimeneti sémák a függvények docstringjeiben vannak.

**Look-ahead**: `confirmed_at` = az i+n. scale-bar ZÁRÓIDEJE (orchestrátor/1700 pontosítás szerint); a szint/vonal a teljessé tevő pivot `confirmed_at`-jén születik, az esemény az eseménybar záróidején. Online feldolgozás `confirmed_at` sorrendben. Leakage teszt: 6 véletlen percre (bar-közepe is) csonkolt futás == teljes futás `confirmed_at ≤ T` objektumai (pivots, events, touches, `levels_as_of`, `trendlines_as_of`), 15m és 1h léptéken.

**SOL teljes 1m történet** (2020-09-14 → 2026-11-19, 3.25M sor; betöltés 3 s)

| lépték | bars | pivots | levels | trendlines | events (szint t/b/r ; vonal t/b/r) | max. egyidejű aktív szint / vonal | idő |
|---|---|---|---|---|---|---|---|
| s (1h, 7d) | 54 174 | 10 579 | 2 622 | 3 481 | 32 613 (11 264/8 187/3 708 ; 4 839/3 230/1 385) | 18 / 10 | 20.3 s |
| m (4h, 30d) | 13 544 | 2 625 | 642 | 903 | 8 042 (2 735/1 997/940 ; 1 166/843/361) | 20 / 9 | 3.0 s |
| l (1d, 180d) | 2 257 | 443 | 104 | 137 | 1 164 (374/300/140 ; 181/131/38) | 16 / 7 | 0.5 s |

Trendvonal érintés-eloszlás (s): 3→2628, 4→643, 5→165, 6+→45. Szint érintés medián 3, max 13. `forward_label_events` s-en 1.5 s. Darabszámok józanok (max 10 párhuzamos aktív vonal), dedup-hangolás nem kellett.

**Spec-értelmezések / eltérések**
1. Szint ára = az első `level_min_touches` (2) érintés átlaga, utána fagyasztva (a további érintések csak számlálódnak) — csonkolás-invariáns, nincs drift.
2. Szint/vonal elévül: utolsó érintés `confirmed_at` + lookback (`expired_at`). A szint a break-et túléli (szerepcsere, `n_role_flips`), a trendvonal a break után a 12 bar retest-ablak végén meghal (`expiry_reason='break'`).
3. Pivot prominencia: `high[i] - min(low[i-n..i+n]) ≥ 1·ATR[i]` (lows tükrözve); fraktál bal oldalt szigorú, jobb oldalt nem szigorú egyenlőtlenség (lapos csúcs = 1 pivot).
4. Trendvonal érintés csak addig számít, amíg az 1. horgony óta nem volt ε-nál nagyobb vonalon túli záróár; 2-érintéses jelöltek belsők, nem kerülnek kimenetre (a spec opcionális flagjét nem implementáltam a kimenetméret miatt). Dedup (kauzális, a régebbi megerősített vonal nyer): ≥2 közös érintés-pivot, VAGY azonos irány + ár a megerősítő baron ε-on belül + meredekség-különbség ≤ `line_dedup_slope_atr`·ATR (0.1, új paraméter).
5. Touch re-arm: egy touch csak akkor, ha az előző bar nem volt ε-zónában (lebegés = 1 touch). `side` = az objektum szerepe az eseménykor (`support`/`resistance`; break-nél a break előtti szerep).
6. Események extra oszlopai a spec felett: `bar_idx, bar_num, obj_slope_per_bar, bar_close` (a forward címkéhez és a vonal-árhoz).
7. Forward címkék az 1700 doksi szerint (touch→bounce/break, break→follow_through/false_break [visszazárás ε-on belülre], retest→hold/fail [záróár a régi oldalon]); timeout esetén a negatív címke + `forward_timeout=True`; ha < 24 bar jövő van és nincs feloldás → null. Extra: `forward_bars, forward_mfe_atr, forward_mae_atr`.
8. `strength` a `levels` táblában az objektum élete végén (min(expired_at, data_end)) értékelve; T-beli érték: `levels_as_of`. `is_round`: ár 0.25·ATR-en belül 10 többszöröséhez (0 kizárva); `is_round_secondary`: 5 többszöröse, de nem 10-é.

**Ellenőrzés**
- `uv run ruff check src/data_handling/ --fix` → All checks passed; `src/utils.py` is tiszta
- `uv run pyright src/data_handling/ src/utils.py` → 0 errors
- `uv run pytest src/data_handling/tests/` → 191 passed (9 új mind zöld); 3 failed + 20 error **környezeti**: a `database/solusdt/solusdt.duckdb`-t egy másik processz (PID 14056, worker/sync) lockolja, a valós DB-s store perf/sanity tesztek nem tudnak csatlakozni — nem kapcsolódik ehhez a taskhoz, a validátor futtassa újra lock nélkül.

**code_doc_agent felé**: `_doc_/database_and_code_doc/1200_sync_tables.md`-be kell egy referencia-bekezdés a `_levels_polars.py`-ról (publikus API + `levels_detector` config blokk + `utils.load_levels_detector_config`); a doc-zónába nem írtam.

### [validator] Validated — 2026-11-19
- `uv run ruff check` (érintett fájlok + `src/utils.py`) → All checks passed; `uv run pyright` → 0 errors.
- `uv run pytest src/data_handling/tests/ src/reports -v` → 245 passed, 3 failed, 0 error (most nem volt DB-lock). A 9 új detektor-teszt zöld. A 3 hiba valós DB adatállapota, nem ehhez a taskhoz tartozik: `test_feat_ohlcv_quant_no_time_gap` (25 rés), `test_target_row_count_matches_features` (3 250 070 vs 3 120 401), `test_predictions_score_range` (3 093 530 sor a [0,1]-en kívül).
- Look-ahead átnézve: a pivot `confirmed_at` = `close_time[i+n]`; a szint/vonal a teljessé tevő pivot `confirmed_at`-jén jön létre; az esemény-szkennelés `confirm_idx+1`-től indul. A jövőt is látó segédértékeket (`viol`, `break_pos`) a kód csak ismert időponttal (`m`, `confirm_idx`, `c`) veti össze, így csonkolás-invariánsak. A leakage teszt (6 véletlen bar-közepi vágás, 15m+1h) a pivots / events / touches / `levels_as_of` / `trendlines_as_of` egyezést nézi, és ez lefedi a szabályt.
- Nincs nyers JSON config olvasás: a config csak a `utils.load_levels_detector_config()`-on át jön (deepcopy).
- Nem-blocker (a t912-höz): a `levels` / `trendlines` tábla `n_touches`, `last_touch`, `strength`, `role`, `n_role_flips`, `broken_at`, `expired_at` mezői az objektum élete végén érvényes állapotot mutatják, tehát jövőbeli információt tartalmaznak. Feature-hez csak a `levels_as_of` / `trendlines_as_of` használható. Az 1d lépték nincs külön a leakage tesztben (ugyanaz a kód fut rajta). A code_doc_agent referencia-jegyzete (`1200_sync_tables.md`) még hátravan.
