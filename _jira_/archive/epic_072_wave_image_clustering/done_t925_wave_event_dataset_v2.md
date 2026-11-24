---
epic: epic_072
id: t925
title: wave_events_v2 adatkészlet + riport frissítés
assignee: analyst_agent
status: done
blocks: [t919]
blocked_by: [t924]
---

## Goal
A t924-ben rögzített v2 módszertan szerint építsd újra az eseményadatkészletet:
`research."wave_events_v2"` (+ `__funnel`, `__meta`) a lab DB-ben (`database/solusdt/solusdt_lab.duckdb`),
csak a (4h,4h) és (4h,8h) cella, háromosztályos first-touch címkével + bináris fő címkével,
CV-blokk azonosítóval (6 időbeli blokk a train-en, purge jelöléssel). A v1 tábla maradjon (nem törlendő).

Frissítsd a `src/reports/builders/wave_events.py` / `wave_event_dataset.py` kódot (v1 visszafelé kompatibilis,
vagy verzió paraméter), a teszteket, és a riportot `_doc_/reports/wave_event_dataset/` alatt (v2 tartalom).

## Acceptance Criteria
- [ ] Look-ahead tesztek a first-touch címkére (azonos baros ütközés szabály)
- [ ] Riport csak train példákat mutat; OOS-ból csak darabszám
- [ ] Riportban: osztályarányok cellánként és CV-blokkonként, A/σ_W vs impulzus-arány
- [ ] OOS eseményszám a két cellában együtt jelentve (≥100 feltétel teljesül-e)
- [ ] ⛔ Utána vizuális ellenőrzőpont (user) a t919 előtt

## Notes

**2026-11-20 — analyst_agent (kész, ⛔ user vizuális ellenőrzőpont a t919 előtt; ticket státusz változatlan)**

Fájlok:
- `src/reports/builders/wave_events.py` — `WaveParams.version` ("v1" alap, fagyasztva / "v2" = `params_v2()`), `tie_break`, `cv_blocks`; `first_touch_class()` (tiszta függvény); v2 oszlopok `same_bar`, `cv_block`, `cv_straddle`; `cv_block_bounds`, `assign_cv_blocks`, `cv_fold` (purge a teljes `[t0−B, t_c+H]`-ra + embargo H fold-időben), `cv_summary`; `persist` / `load_events(version=)` verzió-táblába ír.
- `src/reports/builders/wave_event_dataset.py` — `build_v2` (a `build` a `params.version != "v1"` esetén ide ágazik; CLI alap v2, `--version v1`).
- `src/reports/tests/test_wave_events.py` — +11 teszt (first-touch + same-bar, ablakon kívüli bar-ok, csonkolt futás, v1 változatlan + v1 tábla érintetlen, cv blokkok, cv_fold purge/embargo, cv_summary, v2 riport train-only).
- Riport: `_doc_/reports/wave_event_dataset/` (v2 tartalom, 15 chart, 9 tábla). Megjegyzés: a mappa-csere (`rename`) Windows hozzáférési hibát adott (valamelyik folyamat tartja a mappát), ezért a riport egy ideiglenes helyre épült és a tartalom a meglévő mappába lett másolva (tartalma azonos a `ReportBuilder` kimenetével).
- Lab DB: `research."wave_events_v2"` (1815 sor: train 1499 / oos 143 / holdout2 85 / purged 88), `__funnel`, `__meta` (hash `2c9934c5a11abb60`). A v1 tábla érintetlen (2693 sor, hash `f258c528a0e36273`).

Eredmények (train; cella: train | impulzus / hamis indulás / oldalazás):
- W4h_H4h: 779 | 66 (8,5%) / 58 (7,4%) / 655 (84,1%)
- W4h_H8h: 720 | 117 (16,3%) / 115 (16,0%) / 488 (67,8%)
- Same-bar kettős érintés (train): 0 / 779 és 0 / 720 = 0,00% → nem lényeges (< 2%), az ellenkező tie-break érzékenység azonos osztályarányt ad.
- CV blokkok (6, egész napos határok: 2021-01-01, 2021-11-07, 2022-09-12, 2023-07-18, 2024-05-23, 2025-03-29, train-határ): átlógó esemény összesen 1 (W4h_H4h) / 2 (W4h_H8h).
  - W4h_H4h teszt-esemény blokkonként 143 / 118 / 129 / 121 / 124 / 143; impulzus 13 / 7 / 13 / 12 / 9 / 12; hamis indulás 9 / 12 / 9 / 9 / 7 / 12.
  - W4h_H8h teszt-esemény blokkonként 130 / 108 / 120 / 113 / 115 / 132; impulzus 26 / 18 / 18 / 22 / 15 / 18; hamis indulás 18 / 24 / 21 / 18 / 15 / 19.
- 5200 minimumok: blokkonként ≥ 40 → mindkét cellában igen (min. 118 / 108). Összevont ≥ 100 impulzus és ≥ 100 nem-impulzus: **W4h_H4h FAIL** (66 impulzus, 712 nem-impulzus), **W4h_H8h PASS** (117 / 601). → W4h_H4h a 5200 szerint NO-GO (nem küszöb-csökkentés); user döntés kell.
- A/σ_W vs impulzus-arány (kvintilis Q1 → Q5): W4h_H4h 15,4% → 1,9%; W4h_H8h 28,5% → 2,8% (erősen csökkenő: erős skalár alap várható).
- OOS (csak db): W4h_H4h 73, W4h_H8h 70, cella-összeg 143, **egyedi t0: 75** (a két cella t0-i szinte teljesen közösek). ≥ 100: az összegen igen (143), az egyedi számon **NEM** (75). A konzervatív (egyedi) olvasat szerint az OOS előjel-ellenőrzés nem futhat → "CV-GO, OOS-megerősítetlen" státusz legfeljebb (5200 szerint); user/t921 döntés. Holdout-2: 44 + 41 = 85 cella-összeg, 44 egyedi t0 (csak db).
- OOS-hygiene: a riportban OOS/holdout-2 csak darabszám (nincs példa, nincs címke-arány); chart/tábla JSON-ban nincs ≥ 2026-02-01 időpont (a manifest generálási dátuma kivételével). A lab táblában az OOS sorok címkézve vannak (mint v1-ben), a riport nem használja.

Ellenőrzés: `uv run ruff check src/reports` OK, `uv run pyright src/reports` 0 hiba, `uv run pytest src/reports/tests` 63 passed (ebből `test_wave_events.py` 21).

**2026-11-20 — validator_agent: PASS -> done**
- ruff OK, pyright 0 hiba, pytest src/reports/tests 63 passed.
- First-touch: kód helyes (ablak csak (t_c, t_c+H], azonos bar -> false_start). A lab DB mind az 1499 train eseményének címkéje független újraszámolással (1m OHLCV) egyezik (0 eltérés); 10 véletlen minta is egyezik. Mindhárom osztály szerepel a mintában.
- Same-bar = 0: nem hiba. Független számítás mind az 1815 eseményre: 0 azonos baros kettős ütközés, de 6 esemény érinti mindkét szintet (különböző barban) -> a detektálás működik, a 0 valódi.
- Feature look-ahead: close_tc = a t_c előtti utolsó lezárt 1m bar zárása; a feature-ablak [t0-B, t_c].
- CV: 6 egész napos blokk, straddle 1 / 2 (H4h / H8h) egyezik; cv_fold purge a teljes [event_start, label_end) + embargo H; blokkonkénti minimumok egyeznek az 5200-zal.
- SQL: 1815 sor (1499/143/85/88), nincs null címke, nincs átfedés a cellán belül, nincs split-határt átlépő esemény, cv_block csak train-en. v1: 2693 sor, hash f258c528a0e36273 érintetlen.
- Riport: manifest érvényes, 15 chart + 9 tábla mind hivatkozott, nincs árva/elavult fájl, nincs >= 2026-02-01 dátum (csak a generálási dátum), OOS-ból csak darabszám, pontszám <= 2000.
- Dok: 0002_data_architecture.md (wave_events_v2 létezik), 8330_reports.md (wave builder sor + teszt) frissítve.
- Figyelem a vizuális ellenőrzőponthoz: W4h_H4h CV-minimum FAIL (66 impulzus); OOS >= 100 csak a cella-összegen (143), egyedi t0-n 75.
