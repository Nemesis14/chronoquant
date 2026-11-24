---
epic: epic_073
id: t935
title: Modell 4 — Chronos/Kronos embedding + lineáris fej (kísérleti)
assignee: analyst_agent
status: done
blocks: [t936]
blocked_by: [t930, t931]
---

## Goal
Modell 4 — Chronos/Kronos embedding + lineáris fej (kísérleti). A t931 közös keretén, a t930 adatkészletén, az 5300 protokoll szerint. Eredmény a leaderboardba, UI riport.

## Acceptance Criteria
- [x] Leaderboard sor + riport; csak train CV (holdout-2 érintetlen)

## Notes

**Állapot:** kész, `todo_` marad (a rename az orchestrátoré / validátoré). Nincs commit.

**Kód (új fájlok; a `scout_bench` meglévő fájljai nem változtak):**
- `src/reports/builders/scout_models_foundation.py`: `FoundationEmbedding`, `FoundationHead` (`sb.ScoutModel`), `load_pipeline`, `embed_series` / `embed_prefix`, `pool_states`, `load_or_embed` (cache), `fit_pca` / `fold_pca`, `emb_spec` / `emb_s0c_spec` / `registry`, `permutation_null` (fej-refit).
- `src/reports/builders/scout_foundation_report.py`: futtatás + riport (`uv run --group research python -m reports.builders.scout_foundation_report [--n-perm 200] [--rebuild]`).
- `src/reports/tests/test_scout_foundation.py` (12 teszt + 1 nehéz, `CQ_HEAVY_TESTS=1`). Dokumentáció: `_doc_/database_and_code_doc/8330_reports.md` (builder-sor + tesztleírás).
- Riport: `_doc_/reports/scout_foundation/` (order 37; 6 chart, 5 tábla).
- `pyproject.toml` `research` csoport: `chronos-forecasting>=2.3,<3` (+ `uv.lock`). Telepítve: chronos-forecasting 2.3.2, transformers 5.17.0, accelerate, einops; torch 2.15.0+cpu.

**Alapmodell-választás:** **Chronos-Bolt-small** — `amazon/chronos-bolt-small`, revízió `772f3d25d38aec6d914c8949dab4462e2d46f5d8` (rögzítve), letöltés ~191 MB (`model.safetensors` 190,9 MB, a HF cache-be). T5 encoder, d_model 512, 16-os patch, `[REG]` token. A Kronos (`NeoQuasar/Kronos-*`) nem pip-csomag, csak a GitHub-repó kódjával fut → kimaradt; a Chronos-2 (többváltozós) egy esetleges 2. kör jelöltje.

**Rögzített 1. köri konfiguráció (X2, kísérleti, hipotézis nélkül, a Holm-családon kívül):**
- Prefix: `[t0 − B, t_c − 1m]` 1m bar (L = 480, a t933 `build_prefix` lépésköz 1-gyel), csatornák `close_sigma` (σ_W, amplitúdóval) és `log_vol_rel`; a Chronos egyváltozós → csatornánként külön.
- Embedding: a 30 patch-token encoder-állapotának átlaga (`[REG]` nélkül) → 2 × 512 = 1024 dim. A Chronos-Bolt példányonként normalizál (mean/std), ami eldobná az amplitúdót → a `loc` és a `log scale` csatornánként explicit skalárként megmarad (4 db). Befagyasztott modell, címke nélkül → eseményenként egyszer számolódik; cache: `data/scout_foundation/emb_49d19a1c27051d3e.npz` (gitignored; a train-időszaki 1m bar parquet-cache ugyanitt).
- Fej: fold-train standardizálás → fold-train PCA 64 (címkefüggetlen, foldonként cache) + 4 loc/scale (+ 6 S0C a `scout_chronos_s0c_r1`-ben) → standardizálás, ±5 vágás → multinomiális L2 logit (lbfgs, C = 0,1). Determinisztikus.
- Permutáció: a keret `runner.permutation_null`-ja (ugyanazok a nap-csomagos permutációk), **N = 200, csak fej-refit** (az embedding és a fold-PCA címkefüggetlen, így ez a 5300 szerinti teljes refit). Kísérleti modell → csökkentett N, a minimális p = 0,005.

**Adathigiénia:** `sb.load_scout_events()` (train split) + `prepare_frame`; 1m bar a t933 `load_bars`-szal, SQL-szűrővel `open_time < 2026-02-01` (+ `assert_no_holdout`); a keretben a max. `t_c` 2026-01-30 22:23. Holdout-2 sor nem került memóriába, `t_c` utáni bar nem hat az embeddingre (teszttel ellenőrizve), másik asset címkéje nincs.

**Eredmény (futás `ab75270600ae`, 2170 CV-esemény / 308 impulzus, SOL+BTC+ETH W4h_H8h, fold-hash `0d90521c16be85de`, bootstrap 2000):**

| | Baseline S0C_logit | scout_chronos_r1 (embedding) | scout_chronos_s0c_r1 (+ S0C) |
|---|---|---|---|
| AUC (pooled OOF) | 0,6361 | 0,6254 | **0,6401** |
| ΔAUC [95% nap-klaszter CI] | — | −0,0106 [−0,0470; +0,0258] | +0,0040 [−0,0300; +0,0371] |
| 3-oszt. log-loss | 0,8009 | 0,8076 (Δ +0,0067) | 0,7996 (Δ −0,0013) |
| Felső 20% prec. (fold-rang, elsődleges) | 0,241 (lift 1,70) | 0,202 (lift 1,43) | 0,216 (lift 1,52) |
| Felső 10% prec. (fold-rang) | 0,248 | 0,211 | 0,248 |
| Felső 20% prec. (train-küszöb, másodlagos) | 0,240 (n 421) | 0,201 (n 443) | 0,212 (n 438) |
| ΔAUC > 0 blokk | — | 3 / 6 (legjobb nélkül −0,017) | 4 / 6 (legjobb nélkül −0,003) |
| Top20 prec. jobb blokk | — | 1 / 6 | 1 / 6 |
| ΔAUC ≥ 0 asset | — | 1 / 3 (SOL +0,011, BTC −0,033, ETH −0,037) | 1 / 3 (SOL +0,018, BTC −0,008, ETH −0,017) |
| Kalibráció meredekség | 0,96 | 0,46 | 0,48 |
| Perm. p (N = 200, fej-refit) | 0,005 (AUC) | 0,692 (ΔAUC) | 0,498 (ΔAUC) |
| SPA p (a 2 X2 sor) | — | 0,0134* | 0,0134* |
| Nettó bp / esemény, felső 20% [CI] | +2,7 [−14,7; +21,4] | −2,1 [−25,7; +21,7] | −0,0 [−24,7; +25,0] |
| Nettó bp, felső 10% | −3,1 | −14,9 | −4,4 |
| Nettó bp, szelekció nélkül | −5,6 | (azonos) | (azonos) |
| Go / no-go (harness) | baseline | **NO-GO** (dauc, logloss, top20, stability, econ) | **NO-GO** (dauc, top20, stability, econ) |

**Futásidő (CPU, párhuzamos agent-futások mellett):** modell-betöltés 17 s, embedding 102 s (2170 esemény × 2 csatorna, egyszer); CV fit 25 s (embedding, a fold-PCA-kkal együtt) / 1,3 s (+ S0C); benchmark 68 s; permutáció (N = 200) 441 s; input-betöltés cache-ből 4 s. Az első bar-betöltés a live DB-zár miatt ~30 s volt.

**Értelmezés:** a befagyasztott Chronos-Bolt reprezentáció **önmagában a hat kézi skalár alatt marad** (ΔAUC −0,011; a log-loss és a top-20% precizitás is rosszabb). **Az S0C-hoz adva sem ad megkülönböztethető többletet** (ΔAUC +0,004, a CI tartalmazza a nullát, perm. p 0,50, a legjobb blokk nélkül negatív, csak a SOL-on pozitív). A fej túlmagabiztos: a kalibrációs meredekség ~0,47 (68–74 jellemző ~1800 tanító eseményen). Leíró vizsgálat: a legerősebb egyedi embedding-jellemző a `close_sigma_loc` (az amplitúdó-skalár; max(AUC, 1−AUC) 0,618, |corr| 0,69 a `volume_ratio`-val). Ami jel van, az tehát átfed a baseline-nal; a PCA-komponensek gyengék. Ez összhangban van a 5300 X2 előzetes várakozásával (pénzügyi, zajos adaton gyenge / vegyes eredmény). → **X2 1. kör: NO-GO**, a skalár baseline marad. A 2. körbe nem javaslom (legfeljebb egy alacsony prioritású Chronos-2 többváltozós próbát).

**\*SPA harness-hiba (nem t935-hiba, a t932/t936 hatásköre; a `scout_bench`-et nem módosítottam):** az SPA p = 0,0134, pedig egyik X2 sor sem jobb a baseline-nál bináris log-loss-ban (átlag 0,410 / 0,407 vs 0,394; napi `dbar` −0,032 / −0,027; `t_stat = 0`). Az ok a `stats.spa_test`-ben van: `out[name] = mean(tb > tstat)`. `tstat = 0` mellett a `mu_c = dbar`-ral visszatolt bootstrap-statisztika szinte mindig pontosan 0, így a szigorú `>` hamisan kicsi p-t ad. Javaslat: `>=` (vagy `tstat == 0` → p = 1). Emiatt a sorokban a `gate_spa = True` hamis; a NO-GO döntést nem befolyásolja. Ugyanez érinthet minden futást, amelyben egyik modell sem veri a baseline-t, ezért a t936 SPA-ja előtt javítandó. A riport „Értelmezés” szekciója is jelzi.

**Megjegyzések / feltételezések:**
- A fej konfigurációját (PCA 64, C = 0,1, loc/scale skalárok) a futás előtt rögzítettem, hangolás nélkül. A smoke-futás (n_perm = 0) ugyanezzel a konfigurációval ment, nem iteráltam.
- Lab DB: csak a `scout_chronos_r1` és a `scout_chronos_s0c_r1` sor + a futás sora íródott (`scout_models_hazard.persist_model_rows`, delete+insert `(run_id, model)` szerint); a baseline sort nem írtam. A run-sor `n_perm` mezője 200 (a `run_id` hash a harness n_perm = 0 hívásából származik).
- Környezet: a `uv sync --group research` eltávolította a lockban nem szereplő, ad hoc telepített `matplotlib`-et (+ függőségeit), és egy párhuzamos folyamat által fogott numpy-DLL miatt a numpy `dist-info` sérült. Helyreállítottam (numpy 2.4.6 dist-info a PyPI wheelből; matplotlib 3.10.9 + függőségek újratelepítve), és minden projekt-importot ellenőriztem. A `uv run` nem pontos szinkronnal fut, így ez nem ismétlődik.

**Ellenőrzés:** `ruff check` (3 új fájl) tiszta; `pyright` 0 hiba; `uv run --group research pytest src/reports/tests -q` → **162 passed, 1 skipped** (a skip a nehéz Chronos teszt; külön, `CQ_HEAVY_TESTS=1`-gyel futtatva zöld).

[validator] Validálva — 2026-11-21 → done
- **Statikus ellenőrzés:** `ruff check` (3 fájl) tiszta; `uv run --group research pyright` 0 hiba (megj.: mindkét builder fájlszintű `# pyright: report…=false` elnyomással fut — a torch/HF típusok miatt elfogadható, de széles).
- **Tesztek:** `uv run --group research pytest src/reports/tests -q` → **164 passed, 1 skipped** (a skip a `CQ_HEAVY_TESTS=1` Chronos-letöltés teszt).
- **Prefix ≤ t_c:** `event_prefix` a `[event_start, t_c)` nyitású bar-okat veszi (`i1` = a t_c-kor nyíló bar, kizárva), forward-fill csak a múltból; teszt: `test_embedding_uses_no_bar_at_or_after_t_c`. OK.
- **Holdout-2:** `load_bars` SQL `open_time < oos_start`, `assert_no_holdout` + `sb.guard_no_holdout`; a cache-elt `bars_*.parquet` max `open_time` = 2026-01-31 23:59 (mindhárom asset). OK.
- **Címkefüggetlen embedding, egyszer számolva:** a befagyasztott modell csak a prefixet látja, `load_or_embed` hash-kulcsos cache → a permutáció fej-refitje valid teljes refit. **PCA** foldonként csak a fold-train sorokon (`fold_pca`, kulcs a train row-set). OK.
- **Cache gitignored:** `data/scout_foundation/` a `.gitignore` `data/` szabálya alatt (`git check-ignore` megerősítve).
- **Saját sor perzisztálás:** lab DB-ben csak `scout_chronos_r1` / `scout_chronos_s0c_r1` + run `ab75270600ae` (n_perm 200, 2170/308, fold-hash `0d90521c16be85de`). A leaderboard-számok (AUC, ΔAUC, CI, log-loss, top20, perm. p 0,692 / 0,498, nettó bp, NO-GO) egyeznek a fenti táblával.
- **Eltérés (nem blokkoló):** a lab DB-ben az SPA már javított: `spa_p = 1.0`, és a `go_failed` tartalmazza az `spa`-t (a t932 `stats.spa_test` `>=` javítás után frissítve). A riport (`_doc_/reports/scout_foundation/`) és a fenti tábla még a régi 0,0134-et + a hiba-figyelmeztetést mutatja; a NO-GO nem változik. A riport SPA-szövegét a t936 újrafuttatása frissíti (permutációt nem futtattam újra).
- **Függőségek:** `pyproject.toml` / `uv.lock` változás a `research` csoportra korlátozódik (a root `requires-dist` változatlan; az új lock-csomagok a chronos/torch/tslearn tranzitív függőségei, pl. a `statsmodels` a tslearn-ből); `uv lock --check` OK. `matplotlib`-et projektkód nem importál (csak egy elavult `.claude/worktrees` másolat) → nem kell a lockba.
- **Figyelem:** a t935 a t933 (még `todo_`, nem verziókezelt) `scout_models_rocket.load_bars` / `build_prefix` függvényeire épül; ha a t933 ezek szignatúráját/csatorna-sorrendjét változtatja, a `_ROCKET_CH` leképezés és a cache-hash érintett.
