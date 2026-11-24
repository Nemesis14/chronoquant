---
epic: epic_072
id: t918
title: Építsd fel a lokális mélypont esemény-adatkészletet címkékkel
assignee: analyst_agent
status: done
blocks: [t919]
blocked_by: [t917]
---

## Goal
SOLUSDT 1m-ből a (W,H) rács minden pontjára: események (t₀, t_c), prefix útvonal [t₀, t_c] 15m-en,
előzmény-kontextus (esés mélysége/hossza t₀ előtt, 3 napos trend, volumen-arány), skalár baseline jellemzők,
bináris címke + al-jelölés. Nem átfedő események, train (2021–2024) / OOS (2025–2026) jelöléssel.
Leíró riport: eseményszám, címke-arányok, néhány példa-kép.

⛔ **Ellenőrzőpont:** a leíró riport (UI Elemzések) mutasson képeket: áttekintő gyertyás chart a detektált
mélypontokkal (t₀ és t_c jelölve), és (W,H)-ként 8–12 determinisztikusan kiválasztott példát, címke szerint
(impulzus / korrekció, al-jelöléssel), **kizárólag a train időszakból**. A t919 csak user jóváhagyás után indul.
Összefoglaló: eseményszám (W,H)-ként és évenként, címke-arányok, prefix-hossz és amplitúdó (A/σ) eloszlás.

## Acceptance Criteria
- [ ] Look-ahead teszt: a prefix és a jellemzők csak ≤ t_c adatot használnak
- [ ] Adatkészlet a lab.duckdb-ben (vagy research táblában), reprodukálható
- [ ] Leíró riport `_doc_/reports/wave_event_dataset/`

## Notes

**2026-11-20 — analyst_agent (kész, ⛔ user vizuális ellenőrzésre vár)**

Fájlok:
- `src/reports/builders/wave_events.py` — adatkészlet-könyvtár (detektálás, S0/C jellemzők, címke, split+purge, 15m prefix, `prefix_image`, `persist`, `load_events`); t919/t920 ezt használja.
- `src/reports/builders/wave_event_dataset.py` — riport-builder (slug `wave_event_dataset`, scope general, order 30).
- `src/reports/tests/test_wave_events.py` — 10 teszt szintetikus adaton (look-ahead csonkolt vs teljes, címke csak (t_c, t_c+H], σ csak zár ≤ t0, nem-átfedés, purge, prefix-ablak, lab roundtrip, riport csak train, determinisztikus példák).
- Riport: `_doc_/reports/wave_event_dataset/` (43 chart, 8 tábla).

Tábla (LAB DB, a live-ba nem ír): `database/solusdt/solusdt_lab.duckdb` → `research."wave_events_v1"` (2693 sor: train 2234 / oos 203 / holdout2 126 / purged 130),
`research."wave_events_v1__funnel"`, `research."wave_events_v1__meta"` (params_json, ohlcv range, content_hash `f258c528a0e36273`). Lab lock nem volt (20× újrapróba beépítve).
Indoklás: a lab DB a kutatási/derivált táblák helye (snap/model/strat mellé), az új `research` séma elkülöníti a pipeline-sémáktól. Doc-követés: a `research` séma felvétele a `0002_data_architecture.md`-be (code_doc_agent).

Újragenerálás (repo gyökér): `uv run python -m reports.builders.wave_event_dataset` (~25 s; `--no-store` = csak riport, `--no-sensitivity`).
Új adatnál a train/OOS események nem változnak (greedy időrendben, determinisztikus), csak a holdout-2 / purged vége.

Train eredmények (cella: train db | impulzus / t0 alá / oldalazott | OOS db | holdout-2 db):
- W4h_H4h: 779 | 8.6% / 7.3% / 84.1% | 73 | 44
- W4h_H8h: 720 | 16.5% / 15.7% / 67.8% | 70 | 41
- W8h_H8h: 429 | 6.8% / 6.3% / 86.9% | 39 | 22
- W12h_H12h: 306 | 8.8% / 6.5% / 84.6% | 21 | 19
- Min. eseményszám: train ≥ 500 csak a két 4h cellában teljesül; **OOS ≥ 150 egyik cellában sem** (és a érzékenységi változatok egyikében sem, legjobb 148 az A ≥ 1,0σ_W + címke-ablakos nem-átfedéssel).
- Medián A/σ_W ≈ 2,0 minden cellában; teljes évek eseményszáma egyenletes (pl. W4h_H4h 134–169/év).
- Az impulzus-arány az A/σ_W-vel erősen CSÖKKEN (W4h_H4h Q1 15,4% → Q5 1,9%), mert P* az A-val nő → erős skalár alap várható.

Példák (determinisztikus: cellánként osztályonként 3, időrendben a (k+0,5)/3 pozíciókban, csak train) — 36 chart, lista a riport `examples` táblájában
(pl. W4h_H4h: impulzus 2021-10-06 / 2023-09-13 / 2025-04-07, t0 alá 2021-12-01 / 2023-03-07 / 2025-05-30, oldalazott 2021-10-12 / 2023-07-14 / 2025-04-22 t0-val).
A két W=4h cella t0-i azonosak (azonos W), a példák részben egyeznek.

Eltérések / értelmezések a 5200-hoz képest:
- Nem-átfedés: a teljes `[t0−B, t_c+H]` intervallumra (epic Key Decision), szigorúbb a 5200 szövegénél (t0' ≥ t_c+H); ~7–10% train eseményt vesz el.
- A ticket Goal "train 2021–2024 / OOS 2025–2026" elavult; a 5200 / epic döntés szerinti split ment (train 2021-01-01→2026-01-31, OOS →2026-07-31, holdout-2 utána, teljes intervallumos purge).
- ATR14(15m) = a true range SMA-ja (nem Wilder), óra-igazított 15m bar; a prefix a csonka bal szélső és a t_c-t átfedő 15m bart kihagyja → 2W/15 vagy 2W/15−1 bar (91%-ban az utóbbi).
- σ_1m/volumen-átlag min. 90% kitöltés (az adatban nincs rés; rés esetén `purge_reason='gap'`).
- Extra oszlopok (nem a S0/C része): `drop_depth_sigma`, `fwd_mfe_log`, `fwd_mae_log`, `mins_to_target`, `mins_to_t0_break`, `first_hit`.
- Az érzékenységi tábla csak darabszám + train impulzus-arány, nem döntési alap; a v1 fagyasztva.

Nyitott (user döntés a t919 előtt): v2 kell-e (OOS ≥ 150 elérhetetlen; 8h/12h train < 500), 3-osztályos címke (az oldalazott 68–87%-ban dominál), impulzus-szorzó.

[validator] Done — 2026-11-20 (a ⛔ user vizuális ellenőrzőpont a t919 előtt továbbra is érvényes)
- `uv run ruff check src/reports/` → All checks passed; `uv run pyright src/reports/` → 0 errors; `uv run pytest src/reports -v` → 52 passed (ebből 10 `test_wave_events.py`).
- Look-ahead: a teszt ténylegesen ellenőrzi — t_c-nél csonkolt adaton (`open_time < t_c`) az esemény, minden jellemző és a prefix bitre azonos; a `label_end` utáni bar-ok módosítása semmit nem változtat; a `(t_c, t_c+H]` módosítása csak a címkét; σ_1m explicit képlettel a `i−10080..i−1` bar-okra (close ≤ t0). Kódban: σ/vol `shift(1)`, jellemzők `≤ tc` index, címke `tc+1..tc+H`, ATR az utolsó `close ≤ t_c` 15m bucketig.
- Lab DB (read-only nyitva): `research.wave_events_v1` 2693 sor (train 2234 / oos 203 / holdout2 126 / purged 130), `__funnel`, `__meta` (hash `f258c528a0e36273`). SQL-invariánsok: nem-átfedés (`event_start ≥ előző label_end`) 0 sértés; split-határ (teljes `[t0−B, t_c+H]`) 0 sértés; nem-purged null címke 0; `a_sigma < 1.5` 0; `prefix_end > t_c` 0.
- Live DB: csak `open_readonly_connection` olvasás; írás csak `utils.lab_db_path`-ra.
- Riport: manifest a `reports.schema.Manifest`-nek megfelel (43 chart, 8 tábla). Az összes chart/tábla JSON időbélyeg-szkennelése: egyik sem tartalmaz ≥ 2026-02-01 időpontot (overview vége 2026-01-31 23:00, példák 2021-05 → 2025-08); minden eloszlás/arány `split == "train"`-re szűrt, OOS/holdout-2 csak darabszám.
- Észrevételek (nem blocker): (1) a példa-chart x-tartománya `label_end + 30 perc` — egy határ-közeli train példánál max. 30 perc OOS ár látszódhatna; a determinisztikus (k+0,5)/3 kiválasztás miatt most nem fordul elő. (2) A 5200 szövege (85., 192. sor) még `t0' ≥ t_c + H` nem-átfedést ír, a kód a szigorúbb epic-döntést követi → methodology_agent frissítse. (3) A `research` séma még nincs a `0002_data_architecture.md`-ben (code_doc_agent follow-up, ahogy a ticket is jelzi).
