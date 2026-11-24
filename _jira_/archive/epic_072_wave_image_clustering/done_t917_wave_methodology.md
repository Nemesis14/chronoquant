---
epic: epic_072
id: t917
title: Írd meg a hullám-kezdet esemény és címke módszertanát
assignee: methodology_agent
status: done
blocks: [t918]
blocked_by: []
---

## Goal
Kód-mentes módszertani dokumentum, amely **az adatok megnézése előtt** rögzíti: a lokális mélypont
eseménydefinícióját (look-ahead-mentes megerősítés), a (W,H) rácsot, az impulzus/korrekció címkét
(Elliott-rationale: 3. hullám ≥ 1. hullám), a képformátumot (OHLC+volumen, ATR-normálás, 15m),
az előzmény-kontextust, a skalár alapmodellt és a go/no-go küszöböket
(ΔAUC ≥ 0.02, permutációs p < 0.01, OOS azonos előjel), valamint a data-snooping kontrollt.

## Acceptance Criteria
- [x] md a `_doc_/methodology_doc/`-ban, forrásokkal (Lo–Mamaysky–Wang 2000, Jiang–Kelly–Xiu 2023, Sullivan–Timmermann–White 1999)
- [x] Paraméterek és küszöbök táblázatban

## Notes

**Fájl:** `_doc_/methodology_doc/5200_wave_start_image_model.md` (kód-mentes, nincs lefelé hivatkozás kódra). Az acceptance criteria első két pontja teljesül (forrásokkal, paraméter-tábla). A ticket neve és státusza változatlan (`todo_`, nincs `pr_` átnevezés, a kérés szerint).

**Számozási döntés:** `5200` (X100, Sampling/Modelling tartomány, `5000`/`5100` után a következő szabad slot). Indoklás: a koncepció modell-koncepció, nem feature-csoport (`1xxx` kizárva), nem cél-definíció (`2000_targets` egy fejezet-szintű meglévő doksi, a koncepció ennél szélesebb), nem stratégia (`6500` a cluster-regime stratégia-szintű kutatás). A `docs_skill.md` és a tényleges zóna eltér (a features-tartomány a skillben `2000–2999`, a zónában `1000–1700` + `2000_targets`); ezt nem oldottam meg, csak jeleztem.

**Paraméter-pontosítások (az epic/ticket szövegéhez képest):**
1. `B = W` cellánként (szimmetrikus bal/jobb ablak); a `[t0-B, t0+W]` zárt intervallum bar-konvencióval: a jobb oldal záróideje `t_c = t0 + W`, az utolsó bar záróideje ennyi.
2. `sigma_W = sigma_1m * sqrt(W / 1 perc)`; `sigma_1m` 7 napos múltbeli 1m log-hozam-szórás (`close <= t0`).
3. Tie-break: legkorábbi azonos mélypont. Nem átfedés: greedy, `t0' >= t_c + H`.
4. Kép: 15m bar-ok záróideje `<= t_c`, kép-hossz `2W`; ATR14(15m) elsődleges skála, A-normálás csak diagnosztika (célszivárgás-kockázat).
5. Címke: `P* = close[t_c] * exp(A)`; impulzus = `max high (t_c, t_c+H] > P*`; korrekció al-jelöléssel (`min low < low[t0]` vs oldalazott).
6. Skalár alap `S0` = {A/sigma_W, sebesség (1 óra, sigma_1m√60), előzmény-trend 3 nap, log sigma}; kontextus `C` = {esés-mélység, volumen-arány} mindkét modellben. ΔAUC = kép-modell − (S0+C).
7. Holm-család: 4 (W,H) × 2 reprezentáció = 8 cella; White Reality Check másodlagos. Permutáció: nap-blokkos label-permutáció teljes refittel, 1000 ismétlés.
8. Min. eseményszám (500 train / 150 OOS, ≥50 pos és ≥50 neg OOS-en) feltételezés — validálásra vár.
9. Klaszter: k ∈ {4..10}, választás csak train-en belüli hónap-blokkos CV-vel; embedding és klaszter csak train-en.

**Split (szembesítés a champion-nal):** train `2021-01-01 → 2026-01-31` (= champion train), OOS `2026-02-01 → 2026-07-31` (= champion valid, csak t920), holdout-2 `2026-08-01 → adat vége` (érintetlen, csak integrációs döntés után). Purge: teljes esemény-intervallum nem lóghat át a határon.

**Eltérések a meglévő doksikban (nem javítottam, jelezve):**
- `_doc_/methodology_doc/1700_features_sr_trendline.md` a champion valid-ablakot `2025-05-01 – 2026-05-31`-nek írja, a `config/models.json` szerint a valid `2026-02-01 – 2026-07-31`. A 1700 doksi elavult lehet.
- `_doc_/research/sr_placebo_study.md` OOS-a `2025-05-01 → 2026-11-19`, ami átfed a champion train-jével és a visszamenőleg látott időszakkal (az epic_071 szerint az OOS „vizuálisan látott”). Ez az epic_071 OOS-tisztaság-tanulságát érinti.

**Nyitott user-döntések:**
1. OOS-határ: champion-valid ablak (`2026-02..07`) vs csak holdout-2 mint OOS (javaslat: az előbbi t920-ra, az utóbbi érintetlen marad).
2. Többszörös teszt: Holm (elsődleges) + White RC vs csak az egyik.
3. Min. eseményszám-küszöbök (500/150/50) — t918 után felülvizsgálandó, csak új verzióban.
4. Permutáció: 1000 teljes refit (drága) vs OOS-only permutáció (gyengébb).
5. Számozás: `5200` megerősítése.

**Nem commitolva.**

[validator] Validated — 2026-11-19. Eredmény: PASS → done.
- AC1 (md `_doc_/methodology_doc/`-ban, forrásokkal): OK (Lo–Mamaysky–Wang 2000, Jiang–Kelly–Xiu 2023, Sullivan–Timmermann–White 1999; plusz Paparrizos–Gravano 2015).
- AC2 (paraméter- és küszöbtábla): OK.
- Epic Key Decisions + epic_071 tanulságok: look-ahead-mentes esemény (t_c = utolsó lezárt bar záróideje, σ_W csak `close <= t0`), (W,H) rács, bináris címke + al-jelölés, skalár baseline (S0 + C), train 2021-01-01→2026-01-31 / OOS 2026-02-01→2026-07-31 / holdout-2 2026-08-01→ (érintetlen), train-only leíró riport, go/no-go táblázat (ΔAUC ≥ 0,02, p < 0,01 Holm után, előjel), Holm + White RC, források: mind jelen van.
- Champion-egyezés: `config/models.json` `lgbm_solusdt_l/s_fw60_2101_2607` train 2021-01-01→2026-01-31, valid 2026-02-01→2026-07-31; a doksi ezzel egyezik.
- Kód-mentesség: OK. Nincs kódblokk, nincs `src/` / `config/` / fájlútvonal a törzsben; a hivatkozások csak methodology- és research-dokumentumra mutatnak.
- Mermaid: 6 diagram, minden fence zárt (`grep` alapján 6 nyitó + 6 záró).
- Index: a zónában nincs TOC-fájl (`docs_skill.md`: „no separate TOC-index file”); a `0000_overview.md` a fő pipeline-láncot listázza, és az 1700/6500 doksik sincsenek benne, ezért index-sor nem került felvételre.
- Javítás: a paramétertáblában a W és H külön sorai helyett egy `(W, H) rács` sor került, a négy rögzített párral (`(4h,4h)`, `(4h,8h)`, `(8h,8h)`, `(12h,12h)`).
- Nem blokkoló megjegyzések (methodology_agent, t918 előtt):
  1. Az „Előzmény-kontextus” és a „Klaszterezés” szekcióból hiányzik a Mermaid-ábra (`methodology_doc_skill.md`: minden kulcsfogalom-szekcióhoz legalább 1 diagram).
  2. A nem átfedő szabály csak a címke-ablakokra vonatkozik. H > W cellákban (pl. `(4h, 8h)`) a következő esemény kép-ablaka `[t0' - B, t_c']` átnyúlhat az előző `(t_c, t_c + H]` címke-ablakba. Döntés kell: a purge/nem-átfedés a teljes `[t0 - B, t_c + H]` intervallumra vonatkozzon-e (javaslat: igen, a t918 dataset-építésben).
- Az eredeti Notes-sor („a ticket neve és státusza változatlan, `todo_`”) a validáláskor már elavult volt: a fájl `pr_` állapotban volt. Ezt a bejegyzést a validátor frissítette.
