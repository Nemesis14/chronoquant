---
epic: epic_071
id: t913
title: Futtass placebo-eseménytanulmányt és memória-görbét az S/R szintekre
assignee: analyst_agent
status: done
blocks: [t914]
blocked_by: [t909, t912]
---

## Goal
Osler-féle teszt: valódi szinteken/vonalakon gyakoribb-e a visszapattanás (ill. a kitörés follow-through),
mint azonos távolságú/meredekségű véletlen placebo szinteken? Memória-görbe: a hatás a szint korának
függvényében (<1d, 1–7d, 7–30d, 30–180d). Eredmény: melyik lépték/feature-csoport menjen a challengerbe.

## Scope
- Általános riport `_doc_/reports/sr_placebo_study/` + összefoglaló `_doc_/research/sr_placebo_study.md`

## Acceptance Criteria
- [ ] Placebo: ≥1000 véletlen szint-készlet; bootstrap p-értékek
- [ ] Train (2021–2024) és OOS (2025–2026) külön
- [ ] Go/no-go a t909 küszöbei szerint, léptékenként és eseménytípusonként
- [ ] Javaslat a t914 feature-válogatásra

## Notes

### t913 — analyst_agent (2026-11-19)

**Fájlok**
- új: `src/reports/builders/sr_placebo.py` (builder: placebo-generálás, vektorizált esemény- és címke-logika, napi blokk-bootstrap, Holm, memória-görbe, feat_levels diagnosztika, riport)
- új: `src/reports/tests/test_sr_placebo.py` (8 teszt, szintetikus adat; köztük: a batch scan és a label minden léptéken pontosan egyezik a detektorral több placebo készleten, valamint valódi szinteken K=1-gyel)
- generált: `_doc_/reports/sr_placebo_study/` (manifest, report.md, 9 chart, 6 tábla, ~0,7 MB; `order=21`, megjelenik a `general` listában)
- új: `_doc_/research/sr_placebo_study.md`; módosítva: `_doc_/research/README.md` (index sor)
- A detektor (`_levels_polars.py`) és a config nem változott; más fájlhoz nem nyúltam.

**Újragenerálás** (repo gyökérből, ~6 perc): `uv run python -m reports.builders.sr_placebo`
- Adat: `live.ohlcv` és `live.quant_train__solusdt_fw60_sr` óránkénti sorai, read-only (`open_readonly_connection`, újrapróbával). Lock nem volt, a workerhez nem nyúltam.
- Determinisztikus (fix seed). Futásidő: detektor 25 s, placebo s/m/l 155/38/6 s, feature-diagnosztika 90 s.

**Módszer röviden**
- 1000 placebo készlet.
  - Placebo szint: azonos `confirm_idx` és `expired_at`; az ár `close ± |d|·ATR`, a `|d|` a valódi eloszlásból, az irány véletlen.
  - Placebo vonal: azonos típus, `confirm_idx`, `expired_at` és `expiry_reason`; a távolság és a meredekség az azonos típusú valódi vonalakból, függetlenül húzva.
- Az esemény- és címke-logika a detektor `_scan_object` + `forward_label_events` függvényeinek vektorizált másolata (`scan_batch`, `label_batch`). Minden build a 0. placebo készletet a detektor függvényein is lefuttatja, és pontos egyezést követel: s 30 784, m 7 514, l 1 057 esemény egyezik.
- Cellák: lépték × szint/vonal × támasz/ellenállás × touch/break/retest = 36 cella.
- p-érték: napi blokk-bootstrap (5000 ismétlés, a napok közösen húzva). Kétoldalas, normál közelítés a bootstrap SE-vel, mert az empirikus p felbontása (1/5000) a Holm-küszöbhöz kevés. Az empirikus p és a placebo-rang p is szerepel.
- Holm periódusonként a 36 cellán.
- Periódusok: train 2021-01-01 → 2025-05-01, OOS 2025-05-01 → adat vége ("vizuálisan látott" jelöléssel).
- Feltételezés: n ≥ 100 a train-ben ÉS az OOS-ben is kell.
- GO: Holm p train < 0,01, Δ ≥ +5 pp, azonos OOS előjel. Kiegészítő jelölés: "NO-GO (fordított)", ha Δ ≤ −5 pp és a többi feltétel teljesül.

**Fő eredmény: 0/36 GO, 0 fordított.** Értékelhető (n ≥ 100 mindkét periódusban): 21 cella. A Holm p (train) mindenhol 1,0, a legkisebb korrigálatlan p 0,040 (s vonal, támasz, touch; az OOS előjel ellentétes).

| lépték | objektum/irány | touch→bounce Δ tr / OOS | break→FT Δ tr / OOS | retest→hold Δ tr / OOS | döntés |
|---|---|---|---|---|---|
| s | szint támasz (n 3997/2792/1254) | −0,0 / +0,1 pp (66,5 vs 66,5%) | −0,0 / +1,5 | −0,2 / −0,7 | NO-GO (nem szign.) |
| s | szint ellenállás | +0,1 / +0,5 | −0,4 / +0,2 | −0,7 / −1,0 | NO-GO |
| s | vonal támasz | +2,1 / −3,2 | +1,1 / +1,0 | −1,6 / +2,4 | NO-GO |
| s | vonal ellenállás | +0,5 / +6,5 | +0,7 / −1,3 | −0,7 / −2,9 | NO-GO |
| m | szint támasz | +0,3 / +0,1 | +0,6 / +2,8 | −1,8 / +1,6 | NO-GO |
| m | szint ellenállás | −1,6 / +0,6 | +0,0 / +0,3 | −2,6 / −0,4 | NO-GO |
| m | vonal támasz | +0,6 / −1,2 | +1,8 / −2,9 | −2,2 / +4,3 (n<100) | NO-GO |
| m | vonal ellenállás | +1,7 / −1,5 | +4,4 / +3,2 (OOS n=96) | +1,5 / +4,8 (n<100) | NO-GO |
| l | mind a 12 cella | \|Δ\| ≤ 10,8 pp | n < 100 (train 10–135, OOS 4–78) | | NO-GO (nem értékelhető) |

A valódi és a placebo arányok 0–2 pp-n belül egyeznek. A t911 nyers bounce-arányai (58–66%) a volatilitásból és a címkeküszöbből jönnek, nem a szintből.

**Memória-görbe** (touch → bounce Δ, kor = első érintés óta): lapos.
- Szintek, train: s <1d +0,9 / 1–7d +0,1 / 7–30d −0,2 pp; m 1–7d +1,1 / 7–30d −0,5 / 30–180d −1,2; l 30–180d −4,3.
- A friss szintek erősebb hatása (memória) nem látszik.
- Korrigálatlanul p < 0,01 két vonal-sáv volt, a Holm után egyik sem szignifikáns:
  - s vonal 7–30d: train +4,5, OOS +5,5 pp (Holm 0,08);
  - m vonal 30–180d: train +9,8, OOS −4,6 pp (Holm 0,07).

**Feature-diagnosztika** (train, óránkénti minta)
- Champion medián |ρ| 0,025 / 0,018 (long/short), max 0,35 (`feat_natr_14`).
- Legerősebb `feat_levels` oszlopok: `feat_sr_dist_sup_atr_l` 0,13, `feat_sr_touches_{res,sup}_l` 0,10. Ezek OOS-ben előjelet váltanak vagy ~0-ra esnek.
- Stabil, nem redundáns oszlopok (|ρ| ≥ champion medián, azonos OOS előjel, max |ρ| a championnal < 0,8), `l` nélkül: 25 db, főleg m/s távolság-, érintés- és aktív-szám-oszlopok. A teljes lista a riportban és a research md-ben van.
- A ρ a long és a short targettel ellentétes előjelű (irány/pozíció). A legközelebbi champion társ többnyire a `feat_weekly_open_return` (0,2–0,46) → nem S/R-memória.

**t914 javaslat — ⛔ DÖNTÉSI PONT (user)**
- A 1700 szerint a detektor NO-GO. Alapértelmezés: (A) megállás, a t914/t915 nem fut, az epic negatív eredménnyel zárul.
- Ha a user folytatná: (C) szűkített, exploratív t914 a 25 szűrt oszloppal (s+m, `l` nélkül), "nem S/R-hipotézis" címkével. Kicsi várható haszon (univariáns ρ ~0,04–0,09).
- (B), a teljes 50 feature, nem javasolt: az `l` csoport instabil.
- Az elemző választása folytatás esetén: (C).

**Megjegyzések**
- A `config/models.json` szerint a champion jelenleg train 2021-01 → 2026-01, valid 2026-02 → 2026-07. Az epic "2025-05-01 = champion valid kezdete" indoklása elavult; a határt a döntés szerint tartottam, az eltérést a riport jelzi.
- Korlátok: a placebo nem rezsim-stratifikált; nincs esemény-dedup (a blokk-bootstrap kezeli); a léptékek közti duplikáció külön cellákban szerepel.

**Ellenőrzés**
- `uv run ruff check src/reports/ --fix` → All checks passed
- `uv run pyright src/reports/` → 0 errors
- `uv run pytest src/reports -v` → 42 passed (8 új `test_sr_placebo.py`)

[validator] Validated — 2026-11-19 → done
- `uv run ruff check src/reports/`: All checks passed; `uv run pyright src/reports/`: 0 errors, 0 warnings; `uv run pytest src/reports -v`: 42 passed (8 db `test_sr_placebo.py`).
- A detektor nem módosult: a `_levels_polars.py` mtime-ja 14:16 (a t910 előtt), a `config/features.json`-é 15:20 (t912); a `levels_detector` blokk megvan. A t913 egyiket sem érintette.
- Read-only: OHLCV `load_ohlcv_1m`, a feature-minta `open_readonly_connection` (újrapróbával) útján; a builder nem ír a DB-be.
- Egyezés a detektorral: a `test_batch_scan_equals_detector` 4 placebo készleten × lépték a detektor `detect_events` + `forward_label_events` kimenetével hasonlít (objektum, bar, eseménytípus, oldal, címke, ár); a `..._on_real_objects` K=1-gyel a valódi szintek eseményeit adja vissza. Javítás: a tesztbe `assert n_checked > 0` került, hogy az összevetés ne lehessen üres (8/8 zöld).
- Manifest: `reports.schema.Manifest` validál, `order=21`; a 9 chart és 6 tábla megvan. A `verification.json` (s/m/l 30 784 / 7 514 / 1 057) és a `go_nogo.json` (36 sor, mind NO-GO) egyezik a research md-vel és a tickettel. README index sor megvan.
- A riport nem lett újragenerálva.
