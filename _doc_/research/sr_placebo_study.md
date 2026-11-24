# S/R szintek és trendvonalak — placebo eseménytanulmány (epic_071 / t913)

**Időszak:** 2026-11-19 · **Asset:** SOLUSDT · **Detektor:** `levels_detector` config v1 (befagyasztva) · **Státusz:** lezárt, nemleges (eseményszinten NO-GO). A t914 indításáról a user dönt.

Riport (UI Elemzések): `_doc_/reports/sr_placebo_study/`. Builder: `src/reports/builders/sr_placebo.py`.
Módszertan és küszöbök: `_doc_/methodology_doc/1700_features_sr_trendline.md`.

## Cél

A kérdés, hogy a t910 detektor szintjein és trendvonalain gyakoribb-e a visszapattanás (touch → bounce), a kitörés utáni továbbhaladás (break → follow-through) és a visszateszt megtartása (retest → hold), mint azonos szerkezetű **véletlen** (placebo) szinteken és vonalakon (Osler 2000). A válasz dönti el, mely léptékek és feature-csoportok mehetnek a t914 challenger modellbe.

## Módszer

- **Placebo szint:** ugyanaz a megerősítő bar és élettartam, mint a valódi szinté. Az ár `close ± |d|·ATR`, ahol `|d|` a valódi megerősítéskori ATR-távolság-eloszlásból jön, az irány véletlen.
- **Placebo vonal:** ugyanaz a típus, megerősítő bar és elévülés. A távolság és a meredekség (ATR/bar) az azonos típusú valódi vonalak eloszlásából van húzva.
- A kvantilisek valódi és placebo között egyeznek (riport: `placebo_check`).
- **Azonos logika:** a detektor állapotgépe és `forward_label_events` címkéi futnak a placebo objektumokon is, a builderben vektorizálva. Minden futás a 0. placebo készletet a detektor saját függvényein is lefuttatja, és pontos egyezést követel: s 30 784, m 7 514, l 1 057 esemény egyezik. A detektor könyvtár nem változott.
- **1000 placebo készlet**, 36 cella: lépték × szint/vonal × támasz/ellenállás × eseménytípus.
- **Bootstrap:** napi blokk-bootstrap (5000 ismétlés, a valódi és a placebo esemény napi párban), kétoldalas p. A Holm-korrekció periódusonként mind a 36 cellára vonatkozik. A placebo-rang p (Osler-féle) kiegészítő mutató.
- **Periódusok:** train 2021-01-01 → 2025-05-01, OOS 2025-05-01 → 2026-11-19. Az OOS „vizuálisan látott”, mert a t911 riport megmutatta. **Korlát (2026-11-19):** a 2025-05-01 határt tévesen indokoltuk a champion valid kezdetével; a champion train 2021-01 → 2026-01, így ez az OOS átfed vele, és nem teljesen tiszta. A NO-GO konklúziót nem érinti (a train időszakon is 0/36 GO).
- **GO feltételei:**
  - n ≥ 100 a train és az OOS periódusban is;
  - Holm p (train) < 0,01;
  - Δ ≥ +5 pp;
  - azonos OOS előjel.

## Eredmény

**0 / 36 cella GO** (fordított hatás sincs). Értékelhető, n ≥ 100 mindkét periódusban: 21 cella. A legkisebb Holm p 1,0, a legkisebb korrigálatlan bootstrap p 0,040. Egyik cellában sem esik a valódi arány a placebo-eloszlás szélső 5%-ába; 36 cellánál véletlenül is kb. 2 lenne várható.

| Lépték | Objektum | touch → bounce Δ (train / OOS) | break → follow-through Δ | retest → hold Δ | Döntés |
|---|---|---|---|---|---|
| s | szint, támasz | −0,0 / +0,1 pp (66,5% vs 66,5%) | −0,0 / +1,5 | −0,2 / −0,7 | NO-GO |
| s | szint, ellenállás | +0,1 / +0,5 | −0,4 / +0,2 | −0,7 / −1,0 | NO-GO |
| s | vonal, támasz | +2,1 / −3,2 (p 0,04, Holm 1) | +1,1 / +1,0 | −1,6 / +2,4 | NO-GO |
| s | vonal, ellenállás | +0,5 / +6,5 | +0,7 / −1,3 | −0,7 / −2,9 | NO-GO |
| m | szint, támasz | +0,3 / +0,1 | +0,6 / +2,8 | −1,8 / +1,6 | NO-GO |
| m | szint, ellenállás | −1,6 / +0,6 | +0,0 / +0,3 | −2,6 / −0,4 | NO-GO |
| m | vonal, támasz | +0,6 / −1,2 | +1,8 / −2,9 | kevés esemény | NO-GO |
| m | vonal, ellenállás | +1,7 / −1,5 | +4,4 / +3,2 (OOS n = 96) | kevés esemény | NO-GO |
| l | mind | \|Δ\| ≤ 10,8 pp, de n < 100 (train 10–135, OOS 4–78) | | | nem értékelhető → NO-GO |

**Értelmezés:** a valódi szintek arányai léptékenként 0–2 pp-n belül megegyeznek a placebóéval (pl. s támasz bounce 66,5% vs 66,5%). A t911 riport magas nyers bounce-arányai (58–66%) tehát a volatilitásból és az 1 ATR-es címkeküszöbből jönnek, nem a szintből: egy véletlen, ugyanilyen távolságra húzott vonal ugyanannyiszor „pattan vissza”. SOLUSDT-n a v1 detektor eseményszinten nem talál a véletlenen túli S/R-hatást.

## Memória-görbe

A touch → bounce Δ a kor szerint (az első érintés óta) **lapos**:
- s szint: <1d +0,9 pp, 1–7d +0,1, 7–30d −0,2;
- m szint: 1–7d +1,1, 7–30d −0,5, 30–180d −1,2;
- l szint: 30–180d −4,3 (n = 139).

A friss szintek erősebb hatását („memória”) egyik léptéken sem látjuk.

Korrigálatlanul p < 0,01 két vonal-sáv volt:
- s vonal 7–30d: train +4,5 pp, OOS +5,5 pp;
- m vonal 30–180d: train +9,8 pp, OOS −4,6 pp.

A Holm-korrekció után egyik sem szignifikáns (Holm p 0,08 és 0,07). Az s-vonal sáv előjel-stabil, ez legfeljebb hipotézis, döntést nem alapozunk rá.

## Kiegészítő feature-diagnosztika (`feat_levels`, train, óránkénti minta)

- **Benchmark:** a champion 158 feature-ének medián |Spearman ρ|-ja a targettel 0,025 (long) / 0,018 (short); a legerősebb a `feat_natr_14` (0,35).
- **Legerősebb `feat_levels` oszlopok:** `feat_sr_dist_sup_atr_l` 0,13, `feat_sr_touches_{res,sup}_l` 0,10. Az `l` lépték legerősebb oszlopai OOS-ben ~0-ra esnek vagy előjelet váltanak (pl. `dist_sup_l` long ρ +0,116 → −0,013), ezért nem stabilak.
- **Stabil és nem redundáns oszlopok** (|ρ| ≥ champion medián, azonos OOS előjel, max |ρ| a champion feature-ökkel < 0,8), `l` nélkül 25 oszlop:
  - `m` lépték: távolságok, érintés- és aktív-szint-szám, vonal-break kora;
  - `s` lépték: távolság, `is_round_res`, érintés, flips.
  - A teljes lista |ρ| szerint: `feat_tl_dist_sup_atr_m`, `feat_sr_dist_sup_atr_m`, `feat_tl_dist_res_atr_s`, `feat_sr_touches_sup_m`, `feat_tl_dist_res_atr_m`, `feat_sr_n_active_m`, `feat_tl_break_age_m`, `feat_sr_dist_sup_atr_s`, `feat_sr_touches_res_m`, `feat_sr_is_round_res_s`, `feat_sr_touches_sup_s`, `feat_sr_n_active_s`, `feat_sr_touches_res_s`, `feat_tl_dist_sup_atr_s`, `feat_tl_retest_flag_m`, `feat_sr_dist_res_atr_s`, `feat_sr_dist_res_atr_m`, `feat_sr_retest_flag_s`, `feat_tl_break_age_s`, `feat_sr_flips_sup_s`, `feat_sr_strength_sup_s`, `feat_sr_flips_res_s`, `feat_sr_retest_flag_m`, `feat_tl_break_dir_s`, `feat_sr_is_round_res_m`.
- A ρ a long és a short targettel ellentétes előjelű, tehát irány/pozíció-információ. A legközelebbi champion társ többnyire a `feat_weekly_open_return` (0,2–0,46). Ezek inkább azt írják le, hol van az ár a közelmúlt tartományában; S/R-memóriát nem jeleznek.

## Javaslat a t914-hez — ⛔ döntési pont (user)

A 1700 szabálya szerint a detektor **NO-GO**, az alapértelmezés: **(A) megállás**, a t914 és a t915 nem fut.

Ha a user mégis folytatná:

- **(C) szűkített, exploratív t914:** csak a szűrőn átment 25 oszlop (`s` és `m` lépték, `l` nélkül), „exploratív, nem S/R-hipotézis” címkével. A hipotézis gyengébb: pozíció a lokális tartományban. Az elvárt haszon kicsi, az univariáns ρ ~0,04–0,09.
- **(B) a teljes 50 feature:** nem javasolt. Az `l` csoport instabil, a többi oszlop zajt ad.

Az elemző választása, ha a user folytatni akarja: **(C)**. Ha nem: **(A)**, az epic negatív eredménnyel zárul.

## Korlátok

- A placebo nem rezsim-stratifikált.
- Az események nincsenek dedupolva; az autokorrelációt a napi blokk-bootstrap kezeli.
- A léptékek közti duplikált szintek külön cellákban szerepelnek.
- A retest → hold definíció szigorú.
- A szintek vizsgálata egyetlen asseten (SOL) és egyetlen paraméterkészleten (v1) történt.
- Az OOS vizuálisan látott. A champion jelenlegi train ablaka (2021-01 → 2026-01) átfedi az itteni OOS-t.

## Újragenerálás

`uv run python -m reports.builders.sr_placebo` (repo gyökérből, kb. 6 perc: detektor 25 s, placebo 200 s, feature-diagnosztika 90 s). A kód a repóban van (`src/reports/builders/sr_placebo.py`, teszt: `src/reports/tests/test_sr_placebo.py`).
