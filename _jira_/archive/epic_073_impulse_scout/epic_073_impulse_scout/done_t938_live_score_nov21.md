---
epic: epic_073
id: t938
title: Impulse Scout post-hoc élő pontozás — SOLUSDT 2026-11-21-i mélypont
assignee: analyst_agent
status: done
blocks: []
blocked_by: []
---

## Goal
A user kérése (2026-11-23): az elmúlt 3 nap nagy eladási hulláma után (mélypont 2026-11-21) mit mond a nyertes Scout
modell (S0 + C baseline logit) az eseményről: 3 osztályos valószínűség, percentilis, top-q tagság, kereskedési terv
(alap szabály és R2_03), és a realizált kimenet. Leíró, post-hoc elemzés, döntési szerepe nincs.

## Scope
- `src/reports/builders/scout_live_score.py` (új, CLI): `uv run python -m reports.builders.scout_live_score --asset solusdt --since 2026-11-20`
- `src/reports/tests/test_scout_live_score.py` (új, szintetikus adatos teszt; DB nélkül)
- Nincs perzisztencia (sem lab DB, sem riport), nincs modell-artifact.

## Acceptance Criteria
- [x] 1 perces SOLUSDT bar-ok a live DB-ből read-only, csak a minimális ablak (2026-11-12 00:00 → 2026-11-23 04:43)
- [x] Esemény-detektálás a 5300 / 5200 v2 pontos definíciójával (`scout_params("a15")`, W4h_H8h, `we.build_events`)
- [x] Baseline: `baseline_spec()` (ScalarLogit, C = 0,1, S0 + C) a teljes előkészített train frame-en (t930 a15, pooled SOL+BTC+ETH, 2170 esemény / 308 impulzus); train OOF AUC 0,636 = a leaderboard értéke
- [x] Top-20% / 10% küszöb = a train OOF score 80. / 90. percentilise (5300)
- [x] Kereskedési tervek árakkal + post-hoc kimenet; a `scout_bench.rules.simulate_rule_event` ugyanazt adja
- [x] ruff + pyright tiszta, 4 teszt zöld

## Notes

### 2026-11-23 — eredmény (analyst_agent)

**Adat.** SOLUSDT live DB, read-only, utolsó bar 2026-11-23 04:43 UTC. BTCUSDT / ETHUSDT live adata csak
2026-11-21 10:28-ig tart (nem éri el a mai napot, és a 11-21-i esemény címke-ablakát sem fedi le), ezért ezek
nincsenek pontozva.

**Esemény (egyetlen minősülő esemény 2026-11-20 óta).** A detektált esemény-halmaz 8 / 10 / 12 napos
visszatekintéssel azonos (a greedy nem-átfedő lánc stabil).

| Mező | Érték |
|---|---|
| event_id | `solusdt\|W4h_H8h\|20261121T0617` |
| t0 (mélypont) | 2026-11-21 06:17 UTC, low[t0] = 64,83 (a 11-18 óta mért minimum is ez) |
| t_c (megerősítés) | 2026-11-21 10:17 UTC, close[t_c] = 68,00 |
| A / σ_W / A/σ_W | 0,0477 (log, +4,9%) / 0,0192 / 2,49 |
| P* = close[t_c]·exp(A) | 71,325 |
| stop = low[t0] | 64,83 |
| Címke-ablak | (11-21 10:17, 11-21 18:17] — **lezárult** |

**Modell (S0 + C logit, teljes train-en illesztve).**

| | Érték |
|---|---|
| P(impulzus) / P(hamis indulás) / P(oldalazás) | **0,096 / 0,164 / 0,740** (train alapráta: 14,2% impulzus) |
| percentilis a train OOF score-eloszlásban | **22.** (medián 0,143; küszöb top20 = 0,1879, top10 = 0,2106) |
| top 20% / top 10% jelzés | **nem / nem** → a protokoll szerint **nincs belépő** |

Miért alacsony: a logit-hozzájárulások (standardizált): pretrend_3d = −7,0 σ_W (z −1,06) → −0,16 (az erős 3 napos
esés a train-ben csökkenti az impulzus esélyét); A/σ_W = 2,49 (z +0,14, negatív együttható) → −0,11 (nagyobb
visszapattanás → távolabbi P*); velocity_1h −0,05; log_sigma_1m −0,04; drop_depth −0,02; volume_ratio = 2,02
(z +0,56) → +0,08 (az egyetlen pozitív tényező).

**Kereskedési tervek (belépő: a t_c-kor nyíló bar nyitója, 68,01; csak illusztráció, a jelzés nem aktív).**

| Szabály | TP | SL | Időkorlát | Kimenet (POST-HOC) | Nettó |
|---|---|---|---|---|---|
| Alap (P* / low[t0] / 8h) | 71,325 (+4,87%) | 64,83 (−4,68%) | 11-21 18:17 | időkorlát, zárás 70,63 | +372 bp |
| R2_03 (TP +3σ_W / SL −1,5σ_W / 8h) | 72,043 (+5,93%) | 66,079 (−2,84%) | 11-21 18:17 | időkorlát, zárás 70,63 | +372 bp |

**Realizált címke (POST-HOC): oldalazás** — az ablakban a max. high 71,08 (P* 71,325 alatt), a min. low 67,90
(stop felett). A modell legvalószínűbb osztálya (oldalazás, 0,74) egyezett. A P*-ot az ár csak 2026-11-23 03:37-kor
lépte át (high 71,76), kb. 41 órával t_c után, az ablakon kívül. A pozitív időkorlát-hozam egyetlen eset, nem
bizonyíték semmire: a modell szerint ez nem volt jelzés, és a protokoll szerint nem lett volna kereskedés.

### Módszertani megjegyzés — holdout-2 higiénia (KÖTELEZŐ figyelembe venni)
- Az esemény a holdout-2 időszakba esik (≥ 2026-08-01). Betöltve csak a 2026-11-12 00:00 → 2026-11-23 04:43
  közötti SOLUSDT 1 perces ablak. A 2026-11-20 előtti t0-jú események címkéje memóriában kiszámolódott, de
  megjelenítés nélkül el lett dobva (a `detect` szűr először); BTC/ETH holdout-2 esemény nem lett detektálva.
- **Már látott (megnézett címkéjű) holdout-2 esemény-kulcs:** `solusdt|W4h_H8h|20261121T0617`
  (flow_key: `solusdt|20261121T0617`). A jövőbeli holdout-2 próbában ezt "already seen" jelöléssel kell kezelni:
  a fő eredmény ezzel és nélküle is közlendő, vagy kizárandó.
- A train-en illesztett modellben nincs OOS / holdout-2 adat (`guard_no_holdout`).
- Egy esemény N = 1: a fenti szám leíró; a Scout döntési forrásként nem minősített (epic_073: NO-GO).

### Ellenőrzés
- `uv run pytest -q src/reports/tests/test_scout_live_score.py` → 4 passed
- `ruff check` és `pyright` a két új fájlon: tiszta
- A két szabály kimenete a `scout_bench.rules.simulate_rule_event`-tel újraszámolva azonos (net 0,03721).
- Megjegyzés: az `_jira_/epic_073_impulse_scout/` mappa a session közben archiválódott; ez a ticket új mappában jött
  létre (archívumhoz nem nyúltam). A `jira.json` nem változott (epic_counter 73).

### [validator] Validálva — 2026-11-23 → done
- **Esemény-detektálás:** `scout_params("a15")` = `params_v2` (W4h_H8h, σ_W = 10080 bar múltbeli 1m szórás, A ≥ 1,5 σ_W,
  `full_interval` nem-átfedés), `we.build_events` + `se.event_id` — azonos a t930 kóddal. Lookback 8 és 10 nap: ugyanaz
  az eseményhalmaz.
- **Baseline:** `baseline_spec()` ScalarLogit, a `load_scout_events` (SQL `split = 'train'`) → `prepare_frame` →
  `guard_no_holdout` frame-en illesztve; OOS / holdout-2 nem kerül a fitbe. Top-20/10 küszöb = train OOF score 80./90.
  percentilise.
- **Kereskedési tervek:** a szintek a `rules._level` szerint (sigma: entry·exp(±kσ_W)), a költségképlet = `simulate_event`;
  független újraszámolás `rules.simulate_rule_event`-tel (DEFAULT_RULE és R2_03): mindkettő timeout, +372,1 bp.
- **CLI spot-check** (`--asset solusdt --since 2026-11-20`): `solusdt|W4h_H8h|20261121T0617`, P(imp) 0,096,
  percentilis 22,2, P* 71,325, címke sideways (a `build_events` saját v2 címkéje is sideways) — egyezik a riporttal.
- **`--since` guard** (< 2026-08-01 → ValueError) tesztelve; csak a minimális SOL ablak töltődik read-only.
- ruff + pyright tiszta; `uv run --group research pytest src/reports/tests -q` → 183 passed, 1 skipped.
- Apró, nem blokkoló megjegyzés: a `first_touch` a NaN (hiányzó) bart csendben átugorja, míg a `simulate_rule_event`
  az ilyen ablakot érvénytelennek veszi; ennél az eseménynél nincs rés, az eredmény azonos.
