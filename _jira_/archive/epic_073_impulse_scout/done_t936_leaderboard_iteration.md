---
epic: epic_073
id: t936
title: Leaderboard, 2. iterációs kör, összefoglaló
assignee: analyst_agent
status: done
blocks: []
blocked_by: [t932, t933, t934, t935]
---

## Goal
Leaderboard értékelése (többszörös tesztelés kontrollal), 2. iterációs kör a legjobb 1–2 irányon, összefoglaló
`_doc_/research/impulse_scout.md`, javaslat a holdout-2 végső próbára (mikor, mivel).

## Acceptance Criteria
- [x] md összefoglaló + README index

## Notes
- (orchestrátor, t934 validátor) Harness-javítás: `research."scout_leaderboard_v1__runs".spa_p` INTEGER típusú → SPA p 0-ra csonkol; `ALTER ... TYPE DOUBLE` + explicit cast a `persist`-ben, majd a run sorok újraszámolása. t934 javaslatai: flow-join opció a `load_scout_events`-ben, modell-szűrő a `persist`-ben, extra kimenetek (CIF), SQL-szűrt train bar loader.
- (orchestrátor) A spa_p INTEGER hibát a t932 javította (DOUBLE migráció + cast a persist-ben, 3 érték visszaállítva); `persist(res, models=[...])` modell-szűrő kész.
- (orchestrátor, t935 validátor) Az SPA `>=` javítás után a riportok (scout_foundation, scout_lgbm, scout_hazard) SPA értékei elavultak → t936-ban rebuild/frissítés. A t935 a t933 `scout_models_rocket.load_bars/build_prefix` függvényeit használja.
- (orchestrátor, t932 validátor) SPA fix kész (`>=`, regressziós teszt, 3 sor spa_p→1.0). t936 teendők: végső SPA a teljes leaderboard-univerzumon (most futásonként néhány modell); `gate_spa` a kiválasztott legjobb modellre vonatkozzon, ne a futás minden modelljére; Gate B top-10%-ot is nézze; scout_foundation riport SPA értékének frissítése.

### 2. kör — ELŐZETES REGISZTRÁCIÓ (rögzítve 2026-11-22, a 2. kör bármely futtatása előtt)
User döntés (2026-11-22): nem új modelltípus, hanem kereskedési szabály-iteráció a baseline S0+C score-on.
- **Score:** a baseline `S0C_logit` pooled OOF score (6 blokkos purged CV, fold-hash `0d90521c16be85de`), változatlan; a szabályok nem illesztenek paramétert (nincs tanulás) → a CV-OOF score-on értékelhetők.
- **Közös:** hosszú belépő a `t_c`-nél nyíló első 1m bar nyitóján (taker + csúszás), költségek `config/strategies.json` `costs` (tp maker, sl / timeout taker + csúszás), same-bar (cél és stop egy bar-ban) = stop, ablak a `t_c`-től legfeljebb H = 8 h, funding nincs. σ_W és ATR14(15m) az esemény saját, `t_c`-ig ismert értéke; σ-szintek log-egységben a belépő árhoz, ATR-szintek ár-egységben a belépőhöz. Jelzés: teszt-foldon belüli rangsor (fold-rang), mint a harness elsődleges nézete.
- **Változatok (12):**
  - `R2_01_sym1s`: TP +1,0 σ_W / SL −1,0 σ_W / 8 h / felső 20%
  - `R2_02_tp2s_sl1s`: TP +2,0 σ_W / SL −1,0 σ_W / 8 h / felső 20%
  - `R2_03_tp3s_sl15s`: TP +3,0 σ_W / SL −1,5 σ_W / 8 h / felső 20%
  - `R2_04_atr2_1`: TP +2 ATR / SL −1 ATR / 8 h / felső 20%
  - `R2_05_atr4_2`: TP +4 ATR / SL −2 ATR / 8 h / felső 20%
  - `R2_06_partial_be`: alap szintek (TP P*, SL low[t0]); 50% kilépés a félúton (`close_tc·exp(A/2)`, maker), utána a maradék stopja a belépő ár (break-even), TP P*, 8 h / felső 20%
  - `R2_07_be1s`: alap szintek; ha a high eléri a belépő +1,0 σ_W-t, a stop a belépő árra kerül (ugyanabban a bar-ban is érvényes, konzervatív) / 8 h / felső 20%
  - `R2_08_time4h`: alap szintek, időkorlát 4 h / felső 20%
  - `R2_09_time2h`: alap szintek, időkorlát 2 h / felső 20%
  - `R2_10_top20_alo`: alap szabály, felső 20% ÉS `a_sigma < 2,5` (belépési szűrő; a t930 leíró táblája szerint az alacsony A/σ_W sávban magasabb az impulzus-arány — előzetes adat-ismeret, jelölve)
  - `R2_11_top30`: alap szabály, felső 30%
  - `R2_12_top10_tp2s_sl1s`: TP +2,0 σ_W / SL −1,0 σ_W / 8 h / felső 10%
- **Értékelés:** eseményenkénti nettó bp, nap-klaszter bootstrap 95% CI, hit / stop / timeout, break-even hit (ahol értelmezhető), blokkonkénti (6) és assetenkénti (3) átlag, equity görbe.
- **Univerzum és SPA (Hansen, nap-összegzett nettó bp, stacionárius bootstrap 5 nap, 5000):** 1. kör 7 modell × {felső 20%, felső 10%} alap szabállyal (14) + a 12 2. körös változat = **26 stratégia**. Elsődleges benchmark: **nincs kereskedés (0)**; másodlagos: a baseline alap szabály felső 20%.
- **Kiválasztás (rögzített):** a 2. kör "legjobbja" = a legnagyobb nap-szintű studentizált átlagú változat (az SPA argmax-a a 2. körös változatok között).
- **Gazdasági jelölt feltétel (mind):** átlag nettó > 0 és CI alsó > 0; hit > break-even (ha van); blokkonként az átlag > 0 legalább 4/6; assetenként > 0 legalább 2/3; SPA p (univerzum, benchmark 0) < 0,05 a kiválasztottra.
- **Holdout-2 jelölt szabálya (rögzített):** ha a kiválasztott 2. körös változat minden feltételt teljesít → az a jelölt (S0+C + az a szabály). Különben: **S0+C baseline + az alap triple-barrier szabály (felső 20%)** a referencia-próba, és a holdout-2 megnyitását nem javasoljuk GO-jelöltként (5300: jelölt nélkül nincs megnyitás; legfeljebb leíró megerősítő próba user döntéssel).
- OOS (2026-02→07): nem része; ha szerepel, csak leíró, post-hoc jelöléssel. Holdout-2 nem töltődik.

### Végrehajtás (analyst_agent, 2026-11-22) — kész, `todo_` marad (validátorra vár), nincs commit

**Harness (`src/reports/builders/scout_bench/`):**
- `runner.go_no_go`: `gate_spa` csak a kiválasztott SPA-legjobb modellre (`spa_best`); a többinél None + `go_failed`-ben `not_spa_best` (nem lehet jelölt). `gate_econ` = felső 20% ÉS felső 10% (nettó > 0, CI alsó > 0, hit > break-even).
- Új `universe.py`: `logloss_spa` (univerzum SPA), `joint_holm` (hiányzó p = 1), `apply_universe` (SPA / Holm / kapuk újra), `event_pnl`, `pnl_spa` (stratégia-SPA, benchmark 0 vagy egy stratégia), `studentized_best`.
- Új `rules.py`: `TradeRule` (P* / low[t0] / σ_W / ATR / félút szint, időkorlát, részleges kilépés + break-even, break-even trigger; bar-on belül a stop az első, konzervatív), `simulate_rule[_event]`; az alap szabály == `trades.simulate_event` (teszt, 300 véletlen pálya).
- Új builder `scout_leaderboard.py` (`--stage oof|eval|report|refresh|all`): OOF újraszámolás → visszajátszás egy `run_benchmark` futásban (`StoredOOF`, eredeti config-hash) → univerzum SPA, Holm, kapuk; 2. kör; persist; riport; a 4 round-1 riport frissítése.

**OOF:** mind a 7 modell újraszámolva (fold-hash `0d90521c16be85de`, 2170 / 308), AUC eltérés a tárolttól **0** mindegyiknél. Tárolva: `data/scout_leaderboard/oof_round1.parquet` (gitignored), lab DB `research."scout_oof_v1"` (15190 sor). Futásidő ~5 perc (Inception 179 s, MiniRocket 107 s).

**1. kör univerzum:** SPA (6 modell vs baseline, bináris log-loss, napi összeg, 5000 stac. bootstrap) **p = 0,230** (alsó 0,188 / felső 0,329), legjobb `scout_hazard_r1` (az egyetlen, amelynek a bináris log-lossa jobb: napi Δ +0,0044). **H1–H3 együttes Holm:** 0,806 mindháromra (nyers p 0,720 / 0,306 / 0,269). Mind NO-GO. A lab DB `scout_leaderboard_v1` round-1 sorai frissítve (UPDATE: spa_p, spa_best, holm_p, gate_*, go_status, go_failed; a metrikák és a t931 SOL baseline sor érintetlen). Az InceptionTime permutációt nem futtattam újra (nem kötelező; kísérleti, a döntést nem változtatja).

**2. kör (az előzetes regisztráció szerint, 12 változat, baseline OOF score):**
- Kiválasztott (legnagyobb studentizált napi átlag): **R2_03_tp3s_sl15s** (TP +3 σ_W / SL −1,5 σ_W / 8 h, top 20%): **+15,5 bp / jelzés, CI [−5,2; +37,7]**, hit 9,2% (break-even 5,8%), blokk > 0: 4/6, asset > 0: 2/3 (SOL +69, BTC +6, ETH −9 bp).
- Stratégia-SPA (26 stratégia, benchmark nincs kereskedés): **p = 0,442**, legjobb R2_03. A baseline alap szabálya (top 20%, +2,7 bp) mint benchmark ellen: p = 0,023 (relatívan jobb geometria, abszolút él nem igazolt).
- Gazdasági feltételek: R2_03 bukott: `net` (CI alsó < 0), `spa`. A többi változat negatív vagy ~0 (R2_04 ATR 2/1 és R2_09 2h időkorlát CI-je szignifikánsan negatív). **Eredmény: NO-GO.**
- Holdout-2 jelölt (rögzített szabály szerint): mivel R2_03 nem teljesít, a referencia-próba az **S0+C baseline + alap triple barrier, top 20%**. Holdout-2 GO-próbaként nem javasolt, legfeljebb leíró próba user döntéssel. Legkorábbi időpont a t930 rátáival: 40 impulzus 2027-04-07 (opt.) / **2027-04-26** (pessz.) → 2027 április vége.
- OOS-t nem értékeltem (post-hoc leíró sem), holdout-2 nem töltődött.

**Riportok:** `_doc_/reports/scout_leaderboard/` (új tartalom, a t931 riportot felváltja; 10 tábla/chart: leaderboard, dauc, gates, perm_sources, round2, equity_round2, blocks_round2, stability, round1_trades, equity_round1; a régi, manifestben nem szereplő fájlok törölve; manifest séma-validált). Frissítve (`--stage refresh`): scout_lgbm, scout_rocket, scout_hazard, scout_foundation — JSON táblákban spa_p 0,23 / holm_p 0,806 / kapuk / go_failed, report.md végén „t936 frissítés” szakasz, a foundation elavult SPA-figyelmeztetése kicserélve.

**Dokumentáció:** `_doc_/research/impulse_scout.md` (új) + index sor a `_doc_/research/README.md`-ben; `_doc_/database_and_code_doc/8330_reports.md` (új builder-sor, `test_scout_universe.py` leírás, go/no-go változás).

**Ellenőrzés:** `ruff check` + `pyright` a módosított fájlokon: tiszta (0 hiba; a builder fejlécében a többi scout-builderhez hasonló pyright-lazítás). `uv run --group research pytest src/reports/tests -q`: **179 passed, 1 skipped**. Új tesztek: `test_scout_universe.py` (13), `test_scout_bench.py` +2 (SPA-kapu csak a legjobbra; B kapu top 10%), a meglévő go/no-go teszt az új szemantikára igazítva.

**Megjegyzés a validátornak:** a `python -m reports.builders.scout_bench.report` (t931 futtató) ugyanarra a `scout_leaderboard` slugra ír — ne futtasd, mert felülírja a t936 riportot (dokumentálva a 8330-ban). A `scout_leaderboard.py` modul a pickle miatt a CLI-ben önmagát importálva fut (`__main__` helyett csomagnév).

### [validator] Validálva — 2026-11-22 → done
- **Kód:** `rules.py` alap szabálya logikailag azonos a `trades.simulate_event`-tel (stop elsőbbség = same-bar stop, kitöltés `max/min(level, open)`, azonos érvényesség a teljes H ablakon); teszt 300 véletlen pályán + kereten. Bar-on belül áthelyezett stop ugyanabban a bar-ban ellenőrizve (konzervatív). σ_W (gördülő σ a t0-ig), ATR14(15m) (utolsó, `t_c`-ig zárt 15m vödör), P* (close_tc-ből) mind `t_c`-ig ismert → nincs TP/SL look-ahead. `runner.go_no_go`: `gate_spa` csak `spa_best`-re, a többinél None + `not_spa_best`; `gate_econ` = top 20% ÉS top 10%. `universe.py`: `event_pnl` a pozicionális `row_id`-t használja (a frame-ben `row_id` = sorpozíció, ellenőrizve).
- **Előzetes regisztráció:** a 12 változat + kiválasztási szabály a ticketben a végrehajtási szakasz előtt, dátumozva; a `ROUND2_RULES` 1:1 egyezik vele (R2_10 előzetes adat-ismerete jelölve). Időbélyeggel nem igazolható (azonos nap), a szöveg-sorrend alapján plauzibilis.
- **Független újraszámolás** (a `scout_oof_v1` OOF-ból, `rules.simulate_rule`-lal): 26 stratégia; R2_03 +15,46 bp (SOL +69,4 / BTC +5,6 / ETH −9,3), ref +2,71 bp; SPA vs 0: **p = 0,4416** (legjobb R2_03), vs alap szabály: **p = 0,0226**; studentizált argmax a 12 között: R2_03 — egyezik a tárolttal. Költség a `config/strategies.json`-ból (maker 2 / taker 5 / slip 1,5 bp; tp maker, sl/timeout taker).
- **OOS / holdout-2:** események `split='train'` + `guard_no_holdout` (t0 max 2026-01-30), bar-ok `open_time < oos_start` szűrővel; OOS / holdout-2 sehol nem töltődik.
- **Lab DB:** `scout_leaderboard_v1` round-1 sorai: spa_p 0,23 mind, `spa_best` csak hazard, holm 0,806 (H1–H3; nyers 0,720 / 0,306 / 0,269), gate_spa csak a hazardnál, go_failed `not_spa_best` a többinél — egyezik a tickettel. `scout_oof_v1` 15190 sor (7 × 2170), egy fold-hash `0d90521c16be85de`; az újraszámolt OOF AUC-k eltérése a tárolttól **0** mind a 7 modellnél. `scout_universe_v1` 26 sor. `data/` gitignored.
- **Riportok:** 5 manifest (`scout_leaderboard`, `scout_lgbm`, `scout_rocket`, `scout_hazard`, `scout_foundation`) séma-validált, nincs hiányzó / árva fájl; a „t936 frissítés” szakaszok és a JSON táblák konzisztensek (spa 0,23 / holm 0,806). `_doc_/research/impulse_scout.md` számai egyeznek a DB-vel; README index sor és 8330 sor rendben.
- **Ellenőrzés:** `ruff check` + `pyright` (universe, rules, runner, scout_leaderboard, test_scout_universe, test_scout_bench): 0 hiba. `uv run --group research pytest src/reports/tests -q`: **179 passed, 1 skipped**.
- Apró megjegyzés (nem blokkoló): `go_no_go`-ban hiányzó top 10% szimuláció esetén (`e10 is None`) a B kapu csak a top 20%-ot nézi.
