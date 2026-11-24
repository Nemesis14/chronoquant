---
epic: epic_073
id: t933
title: Modell 2 — MiniRocket + InceptionTime a nyers prefixen
assignee: analyst_agent
status: done
blocks: [t936]
blocked_by: [t930, t931]
---

## Goal
Modell 2 — MiniRocket + InceptionTime a nyers prefixen. A t931 közös keretén, a t930 adatkészletén, az 5300 protokoll szerint. Eredmény a leaderboardba, UI riport.

## Acceptance Criteria
- [x] Leaderboard sor + riport; csak train CV (holdout-2 érintetlen)

## Notes

**Állapot:** kész, `todo_` marad (a rename az orchestrátoré). Nincs commit.

**Kód (új fájlok, a `scout_bench` meglévő fájljai érintetlenek):**
- `src/reports/builders/scout_models_rocket.py` — prefix (`event_prefix`, `build_prefix`, `load_bars`), saját numpy MiniRocket (`minirocket_fit` / `minirocket_transform`), `MiniRocketModel` (P2/H2), `InceptionModel` (X1), `register(registry)` / `registry()`, `permutation_null_parallel`. Nincs új függőség (sktime/aeon nem kellett; torch a research csoportból).
- `src/reports/builders/scout_rocket_report.py` — 3 lépcsős futtató (`--stage bench|perm|final`, köztes eredmény `--cache-dir`-ben) + riport; `refresh_spa(res)` a javított `stats.spa_test`-tel számolja újra az SPA-t a tárolt OOF score-okon.
- `src/reports/tests/test_scout_rocket.py` — 10 teszt (prefix alak/kézi érték, **a `t_c`-nél/után nyíló bar-ok és a `t0−B` előtti bar nem hatnak a prefixre**, rés-kitöltés, MiniRocket alak + determinizmus, zárt alakú LOO-ridge = brute-force, mindkét modell 3 osztályos determinisztikus kimenete, a cache címkefüggetlen, spec-ek).
- Doksi: `_doc_/database_and_code_doc/8330_reports.md` (builder-sor + teszt-leírás). Riport: `_doc_/reports/scout_rocket/` (manifest + report.md + 7 chart + 6 tábla, a manifest séma-validált).

**Előre rögzített konfiguráció (H2, 1. kör, a futás előtt rögzítve, `config_hash 9854a1bf00dd`):** prefix = a `[t0−B, t_c−1m]` nyitású 1m bar-ok (480, mind `t_c`-ig zárul) → 5 perces bar (L=96), csatornák `close_sigma = log(close/low[t0])/σ_W` (amplitúdó megmarad), `log_vol_rel = log1p(vol/(5·vol7d_mean_1m))`, `taker_imb = taker_buy_base/volume − 0,5`. MiniRocket: 84 mag × dilatációk, többváltozós csatorna-kombinációk, ~10k PPV, bias a fold-train példákból (seed). Fej: one-hot ridge (Gram-sajátfelbontás), `alpha` pontos LOO-MSE-vel a 10⁰…10⁷ rácson (15 pont), multinomiális kalibrátor (C=1) a LOO-kimeneteken. A címkefüggetlen transzformáció foldonként cache-elve → **N=1000 teljes refit** (a fej minden permutációban újratanul) 13 perc alatt.
**InceptionTime (X1, kísérleti, `95ace9117cdb`):** 3 modul, 8 szűrő, magok 15/7/3, bottleneck 8, batch 128, max 25 epoch, early stopping (türelem 4) a fold-train utolsó 20%-án (időrend), 1 szál, determinisztikus, nincs ensemble. A méretet a CPU-futásidő döntötte el (16 szűrő/40 epoch ~95 s/fit volt párhuzamos agent-terhelés mellett), eredmény megnézése előtt.

**Eredmény (run `b98ecd97217b`, 2170 train esemény / 308 impulzus, SOL+BTC+ETH, fold-hash `0d90521c16be85de`, fold-rang top-q nézet):**

| | baseline S0C | MiniRocket (H2) | InceptionTime (X1) |
|---|---|---|---|
| AUC | 0,6361 | 0,6324 | 0,6271 |
| ΔAUC [95% nap-klaszter CI] | — | −0,0037 [−0,0349; +0,0295] | −0,0090 [−0,0424; +0,0247] |
| 3-oszt. log-loss (Δ) | 0,8009 | 0,7776 (−0,0233) | 0,7878 (−0,0131) |
| felső 20% prec. (lift) | 0,241 (1,70) | 0,216 (1,52) | 0,234 (1,65) |
| ΔAUC>0 blokk / top20 jobb blokk | — | 3/6 / 1/6 | 3/6 / 3/6 |
| asset ΔAUC (SOL/BTC/ETH) | — | +0,009 / −0,040 / −0,004 | −0,026 / −0,016 / +0,004 |
| perm. p | 0,0010 (AUC) | **0,3057** (N=1000, teljes refit; Holm egy primaryre = 0,3057) | **nem futott** (lásd lent) |
| SPA p (javított `>=`) | — | 1,0 | 1,0 |
| nettó bp/esemény felső 20% [CI] | +2,7 [−14,7; +21,4] | −17,8 [−47,1; +9,7] | +0,1 [−23,1; +24,3] |
| hit / break-even (felső 20%) | 24,1% / 23,3% | 21,6% / 26,1% | 23,4% / 23,4% |
| go / no-go | — | **NO-GO** (dauc, top20, stability, spa, holm, econ) | **NO-GO** (dauc, top20, stability, spa, econ) |
| CV futásidő (6 fold) | 3 s | 130 s (első transzformáció) | 158 s |

Teljes bench-futás (CV + 1000 permutáció baseline + H2, bootstrap, SPA, szimuláció): 1095 s. Szelekció nélkül (2170 esemény): −5,6 bp [−18,7; +8,6].
**Értelmezés:** a **H2 nem igazolt**: a MiniRocket a nyers prefix alakjából nem ad a skalár alapon túli, zajtól megkülönböztethető impulzus-rangsorolást (ΔAUC ≈ 0, p = 0,31). A 3-osztályos log-loss javulása a hamis indulás / oldalazás szétválasztásából és a kalibrációból jön, nem az impulzus-rangsorból (a H2 döntési metrikája). Az InceptionTime sem jobb. A baseline marad.

**Eltérések / megjegyzések:**
- **X1 permutáció:** a csökkentett (N=100, 4 worker) InceptionTime nullt a user kérésére az orchestrátor leállította (CPU); **nem indítottam újra**. `perm_p` = NULL a leaderboardon, a run `note`-ja jelzi („X1 perm_p: not run (stopped by user, CPU)”); a t936 szükség esetén újrafuttathatja: `python -m reports.builders.scout_rocket_report --stage perm --cache-dir DIR --n-perm-x1 100` majd `--stage final`. Kísérleti modellnél a perm-p nem go-feltétel; a go_status NO-GO enélkül is (a ΔAUC negatív).
- **SPA:** a bench a régi `stats.py`-val futott (15:23); a t932 validátor `>=` javítása után a `final` lépés `refresh_spa` újraszámolta (0,6706 → 1,0; egyik modell sem jobb bináris log-lossban, így a statisztika 0), a gate-ek újraszámolva. A közös H1–H3 Holm és a teljes univerzum SPA a t936-é.
- **Perzisztálás:** csak a 2 saját sor (`sb.persist(res, models=[...])`, delete+insert modell-név szerint) + run-sor; a többi modell sora érintetlen (ellenőrizve).
- **Adat:** `sb.load_scout_events()` (train); 1m bar SQL-szűrővel `open_time < 2026-02-01` — OOS/holdout-2 nem töltődött. A live DB-t az app API/worker időnként hosszan zárolta → `load_bars` 60 perces retry-t kapott, és a szűrt train-bar-okat `bars_<asset>.parquet`-ként cache-eli a `--cache-dir`-ben (a worker-t nem bántottam).
- **LOO-kalibrátor:** a LOO ridge-kimenet egy-esemény-kihagyásos, az egyidejű más-asset esemény a tanítóban marad → a kalibrátor enyhén optimista lehet (csak kalibráció; a rangsort nem érinti).
- A train-küszöbös (másodlagos) top-q nézetnél a ridge in-sample train-score-ja miatt a küszöb magasabb (n_top20_trthr 406 vs 435); az elsődleges fold-rang nézetet ez nem érinti.
- t935 a `load_bars` / `build_prefix` / `event_prefix` API-t és a csatorna-sorrendet használja: változatlan (csak új opcionális `cache_dir` kwarg); `test_scout_foundation.py` zöld.

**Ellenőrzés:** `ruff check` és `pyright` a 3 új fájlra tiszta (0 hiba); `pytest src/reports/tests` 150 passed (a riport-modul utolsó módosításai előtt), utána `test_scout_rocket.py` + `test_scout_bench.py` + `test_scout_foundation.py` 45 passed, 1 skipped.

[validator] Elfogadva — 2026-11-21
- `ruff check` + `pyright` a 3 új fájlon: 0 hiba. `uv run --group research pytest src/reports/tests -q`: 164 passed, 1 skipped (benne `test_scout_rocket.py` 10 teszt és a t935 `test_scout_foundation.py` — kompatibilis).
- Prefix: `event_start = t0 − B`, `t_c = t0 + W`, `i1` = a `t_c`-nél nyíló bar kizárva → `[t0−B, t_c−1m]`, 5m aggregálás (close = utolsó, vol/taker = összeg). `vol7d_mean_1m` és `sigma_1m` a `wave_events.rolling_inputs`-ból `_shift1`-gyel (i−window..i−1, csak múlt). Csatornák a specifikáció szerint.
- Adat: SQL `open_time < oos_start` + `assert_no_holdout`, frame `prepare_frame` (train split, `guard_no_holdout`). A bar-cache csak a megadott `--cache-dir`-be ír; a repóban nincs `bars_*.parquet` / pkl / npy.
- MiniRocket: a referencia szerinti kernelek ({−1,2}, −Σ+3·Σ3), dilatációk, kvantilisek, váltakozó padding; seed-determinisztikus; fit/standardizálás csak fold-train sorokból, címkefüggetlen → a `(sorok, seed)` kulcsú cache és a permutációs teljes refit (csak a fej tanul újra, a permutáció csak címke-oszlopot mozgat) érvényes. LOO-ridge: a `1/n` intercept-tag helyes, mert Z fold-train-centrált (brute-force teszt egyezik).
- InceptionTime: early stopping a `row_id`-rendezett (= időrend) fold-train utolsó 20%-án, a teszt fold sosem érintett.
- Lab DB (`solusdt_lab.duckdb`) ellenőrizve: `b98ecd97217b` alatt csak a 2 saját sor; MiniRocket AUC 0,6324 / ΔAUC −0,0037 / perm p 0,3057, InceptionTime AUC 0,6271 / perm p NULL, SPA p 1,0 (javított `>=`, `refresh_spa`), mindkettő NO-GO; a run `note` és a riport dokumentálja az X1 perm hiányát (kísérleti, elfogadható). A többi modell sora érintetlen.
- Permutációt nem futtattam újra. Javítás nem kellett.
