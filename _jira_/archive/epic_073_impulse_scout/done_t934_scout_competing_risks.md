---
epic: epic_073
id: t934
title: Modell 3 — versengő kockázatok / diszkrét idejű hazard modell
assignee: analyst_agent
status: done
blocks: [t936]
blocked_by: [t930, t931]
---

## Goal
Modell 3 — versengő kockázatok / diszkrét idejű hazard modell. A t931 közös keretén, a t930 adatkészletén, az 5300 protokoll szerint. Eredmény a leaderboardba, UI riport.

## Acceptance Criteria
- [x] Leaderboard sor + riport; csak train CV (holdout-2 érintetlen)

## Notes

**Állapot:** kész, `todo_` marad (a rename az orchestrátoré / validátoré). Nincs commit.

**Kód (új fájlok, a `scout_bench` meglévő fájljai nem változtak):**
- `src/reports/builders/scout_models_hazard.py`: `HazardModel` (`sb.ScoutModel`), `time_bin`, `expand_person_period`, `cif_from_hazards`, `three_class_from_cif`, `spec()` / `registry()` (baseline + `scout_hazard_r1`, friss registry), `load_frame()`, `load_bars()`, `persist_model_rows()`.
- `src/reports/builders/scout_hazard_report.py`: futtatás + riport (`python -m reports.builders.scout_hazard_report [--n-perm 1000]`).
- `src/reports/tests/test_scout_hazard.py` (10 teszt). Dokumentáció: `_doc_/database_and_code_doc/8330_reports.md` (builder-sor + tesztleírás).
- Riport: `_doc_/reports/scout_hazard/` (manifest, report.md, 5 chart, 9 tábla; CIF-görbék score-kvintilisenként, időbeli kalibráció, hazard-alak, együtthatók).

**Előre rögzített 1. köri konfiguráció (`ModelSpec("scout_hazard_r1", family="P3", hypothesis="H3", round=1)`):** (0, 480] perc 16 × 30 perces sávra bontva; személy-periódus kiterjesztés (impulzus / hamis indulás a `j` sávban: `0..j` sor, esemény a `j`-ben; oldalazás: 16 sor, cenzorált H-nál); multinomiális logit (`nincs / impulzus / hamis indulás`, sklearn lbfgs, L2, C = 0,1), sáv-dummy-k, arányos kovariáns-hatások. Kovariánsok: S0 + C (6) + 4 flow a `scout_flow_v1`-ből (`vol_decline_rel` log, `trades_rel_decline` log, `qv_logz_prefix`, `tb_ratio_rebound`); fold-train medián-imputálás, standardizálás, ±5 vágás. 3 osztály = (CIF_imp(H), CIF_fs(H), 1 − összeg); score = CIF_imp(H). A kiterjesztés a `fit`-ben, a fold tanító eseményein készül → egy esemény sorai nem lépnek át foldot. Determinisztikus (nincs véletlen).

**Adathigiénia:** `sb.load_scout_events()` (train split), a flow csak a train `flow_key`-ekre olvasva (join a lab DB-ben), 1m bar SQL-szűrővel `open_time < 2026-02-01`; holdout-2 sor nem került memóriába. `t_c` utáni kovariáns nincs (teszt: a pontozott sorok címkéje / `tau`-ja nem hat a becslésre); másik asset címkéje nincs.

**Eredmény (futás `78a7589a381e`, 2170 CV-esemény / 308 impulzus, SOL+BTC+ETH W4h_H8h a15, fold-hash `0d90521c16be85de`, N_perm = 1000 teljes refit, bootstrap 2000):**

| | Baseline S0C_logit | scout_hazard_r1 |
|---|---|---|
| AUC (pooled OOF) | 0,6361 | **0,6474** |
| ΔAUC [95% nap-klaszter CI] | — | **+0,0114 [−0,0040; +0,0269]** |
| 3-oszt. log-loss | 0,8009 | **0,7677** (ΔLL −0,0332) |
| Felső 20% prec. (fold-rang, elsődleges) | 0,241 (lift 1,70) | 0,228 (lift 1,60) |
| Felső 10% prec. (fold-rang) | 0,248 | 0,243 |
| Felső 20% prec. (train-küszöb, másodlagos) | 0,240 (n 421) | 0,230 (n 434) |
| ΔAUC > 0 blokk | — | 4 / 6 (legjobb blokk nélkül átlag +0,0061) |
| Top20 prec. jobb blokk | — | 0 / 6 |
| ΔAUC ≥ 0 asset | — | 3 / 3 (SOL +0,005, BTC +0,000, ETH +0,028) |
| Kalibráció meredekség | 0,96 | 0,85 |
| Perm. p | 0,001 (AUC) | **0,269** (ΔAUC; Holm csak H3-ra = 0,269) |
| SPA p (csak ez az egy modell) | — | 0,091 |
| Nettó bp / esemény, felső 20% [CI] | +2,7 [−14,7; +21,4] | −8,9 [−28,1; +10,3] |
| Nettó bp, felső 10% | −3,1 | −13,3 |
| Nettó bp, szelekció nélkül | −5,6 [−18,7; +8,6] | (azonos) |
| Go / no-go (harness) | baseline | **NO-GO** (bukott: dauc, top20, stability, spa, holm, econ) |

**Értelmezés:** a hazard modell **jobb valószínűség-hármast** ad (ΔLL −0,033: a hamis indulás / oldalazás szétválasztását a kovariánsokból becsüli, a baseline konstans aránnyal osztja), és az **időbeli kalibrációja kiváló** (pooled CIF megfigyelt vs becsült max. eltérés impulzusnál ~0,006, minden kvintilisben a görbék egyeznek; a legfelső kvintilis impulzusai korábban érnek célba, medián τ 171 perc vs 228–292). A **rangsorolásban nincs megkülönböztethető többlet**: ΔAUC +0,011, a CI tartalmazza a nullát, perm. p 0,27; a felső 20% precizitás a baseline alatt van, egyetlen blokkban sem jobb. A gazdasági előszűrő negatív. → **H3 1. kör: NO-GO**, a skalár baseline marad. Az együtthatók szerint a jel zöme továbbra is az `a_sigma` (impulzus és hamis indulás hazardját egyaránt csökkenti: nagy visszapattanás → lassabb döntés), a 4 flow-ból csak a `vol_decline_rel` (log) érdemi.

**Megjegyzések / feltételezések:**
- A Holm-család ebben a futásban csak a H3-at tartalmazza (friss registry, hogy csak a saját sort írjam); a H1–H3 együttes Holm-korrekció és a teljes univerzumra futó SPA a t936 feladata. Mivel a nyers p = 0,27, a Holm nem változtat a döntésen.
- A flow-részhalmazt a t930 leíró train-táblája alapján választottam (enyhe előzetes adat-ismeret, a 2. körhöz nem számít hangolásnak).
- A riport saját OOF-illesztése bitre egyezik a harness OOF-jával (max. eltérés 0).
- A pooled 3-asset baseline AUC 0,636 (a SOL-only t920/t931 0,6647-hez képest; SOL részminta itt 0,658).
- Lab DB: csak a `scout_hazard_r1` sor + a futás sora íródott (delete+insert `(run_id, model)` szerint, hiányzó oszlopokat `ALTER TABLE`-lel pótolja); ellenőrizve: a t931 baseline sor érintetlen.

**Harness-javaslatok (nem módosítottam a `scout_bench`-et):**
1. `sb.load_scout_events(with_flow=..., flow_cols=...)`: a flow join (csak train `flow_key`-ekre) a keretben legyen, ne minden modell külön írja.
2. `persist(res, models=[...])`: szűrhető persist, hogy a párhuzamos modellfuttatások ne írják újra a baseline sort; illetve `run_benchmark` opcionálisan adja vissza a modellek extra kimeneteit (pl. CIF-görbék), hogy ne kelljen a riportban újra illeszteni.
3. A harness `load_bars`-szerű segédfüggvénye SQL-szűrővel (`open_time < oos_start`) töltse az 1m bart (a `report.run_default` jelenleg mindent betölt, utána szűr).

**Ellenőrzés:** `ruff check` (3 új fájl) tiszta; `pyright` 0 hiba; `pytest src/reports/tests -q` **137 passed** (10 új).

**[validator] Validálva — 2026-11-21 → `done_`**
- `ruff check` (3 fájl) tiszta; `pyright` 0 hiba; `pytest src/reports/tests -q` **150 passed** (a párhuzamos t932/t933 tesztekkel együtt; a 10 t934 teszt zöld).
- Személy-periódus kiterjesztés: a `fit`-en belül, a fold tanító eseményein készül → egy esemény sorai nem lépnek át foldot; `time_bin` `(j·30, (j+1)·30]`, oldalazás 16 `none` sor (cenzorálás H=480-nál). Same-bar döntetlen: a címke `first_touch_class(tie_break="false_start")`, a hamis indulás `tau = mins_to_t0_break` (ugyanaz a bar) → a hazard a címkével konzisztens; a train adatban 0 same-bar eset, impulzusnál nincs `mins_to_t0_break ≤ mins_to_target`.
- CIF: `S(j)=∏h_none`, `CIF_c=Σ S(j−1)·h_c(j)`, 3 osztály = (CIF_imp(H), CIF_fs(H), 1−összeg) — helyes, kézi teszt fedi.
- Nincs `t_c` utáni kovariáns (S0+C + `scout_flow_v1`, a `tau`/`label_class` csak célváltozó). Holdout-2: `load_scout_events` train split + `guard_no_holdout`, flow csak train `flow_key`-ekre, 1m bar SQL `< oos_start` + `assert_no_holdout` → holdout-2 sor nem töltődik.
- Számok: a lab DB `scout_hazard_r1` sora (run `78a7589a381e`, 2170/308) és a `report.md` egyezik a ticket táblájával (AUC 0,6474, ΔAUC +0,0114, LL 0,7677, top20 0,228 / lift 1,60, perm p 0,269, SPA 0,0914, nettó −8,9 / −13,3 bp, NO-GO ugyanazokkal a bukott kapukkal). A t931 baseline sor érintetlen. A flow-szelekciós enyhe előzetes adat-ismeret a ticketben és a riport „Korlátok” pontjában dokumentált. A 1000-perm benchmarkot nem futtattam újra (a perzisztált kimenetekkel ellenőrizve).
- Javítva: `persist_model_rows` DELETE+INSERT egy tranzakcióba téve (`BEGIN`/`COMMIT`, mint a harness `persist`-ben) — hiba esetén nem marad törölt sor. Párhuzamos írók: a DuckDB fájl-zár szerializál, a retry-hurok kezeli; a `run_id` a registry spec-hash-eit tartalmazza → a `__runs` törlés csak a saját futást érinti.
- **Nyitott (nem t934 hiba, harness/t936):** a `research."scout_leaderboard_v1__runs".spa_p` oszlop típusa `INTEGER` (a t931 első futása NULL értékkel hozta létre) → a run-sorban a 0,0914 0-ra csonkolódott. A leaderboard sor `spa_p` (DOUBLE) helyes. Javaslat: `ALTER TABLE research."scout_leaderboard_v1__runs" ALTER spa_p TYPE DOUBLE` + a harness persist explicit típus-castja (a t932/t933 futásokat is érinti).
