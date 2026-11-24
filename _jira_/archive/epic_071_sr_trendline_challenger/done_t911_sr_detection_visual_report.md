---
epic: epic_071
id: t911
title: Készíts képes detektálási riportot és összefoglaló statisztikát az UI Elemzések oldalára
assignee: analyst_agent
status: done
blocks: [t912]
blocked_by: [t910]
---

## Goal
A user **vizuálisan ellenőrizni** akarja, mielőtt a detektált alakzatok feature-ként a modellbe kerülnek:
jó alakzatokat talál-e az eljárás. Általános riport (`scope: general`) a report-formátumban, ami megjelenik
az UI Elemzések oldalán. ⛔ **Ez az ellenőrzőpont** — utána megállunk.

## Scope
- Új builder: `src/reports/builders/sr_detection.py` (a meglévő builderek mintájára, pl. `target_regime.py`)
- Kimenet: `_doc_/reports/sr_trendline_detection/` (`manifest.json`, `report.md`, `charts/*.json`, `tables/*.json`)
- Adat: SOLUSDT live `ohlcv` **read-only**; detektor: t910 könyvtára

## Tartalom
**Összefoglaló táblák** (teljes SOL 1m történet, léptékenként s/m/l):
- pivotok száma; szintek száma (támasz/ellenállás), érintésszám-eloszlás, szint-élettartam (median/p90), kerek szám arány
- trendvonalak száma (emelkedő támasz / csökkenő ellenállás), érintésszám, hossz, meredekség-eloszlás
- események száma típusonként (touch / break / retest), havi/éves bontás
- nyers kimenet-arányok (bounce vs break, follow-through vs false break, retest hold vs fail) — **placebo nélkül, jelölve hogy nyers**

**Képek (Plotly candlestick + overlay, ≤ 2000 gyertya/chart a `MAX_CANDLE_POINTS` szerint):**
- Áttekintő: utolsó ~90 nap 4h gyertyákon az aktív `m`/`l` szintekkel (szín = támasz/ellenállás, vastagság = erő)
- Utolsó ~30 nap 1h gyertyákon az `s` szintekkel és trendvonalakkal
- **8–12 kinagyított példa** az elmúlt időszakból, kategóriánként 2–3: erős többszörösen érintett szint visszapattanással;
  szerepcsere (áttört ellenállás → támasz); trendvonal-kitörés + sikeres visszateszt; hamis kitörés.
  Minden példán jelölve: pivotok, a vonal/szint `confirmed_at` pontja, az esemény, és a hover-ben a paraméterek.
- A példák kiválasztása legyen determinisztikus (pl. legnagyobb erő / legtöbb érintés az adott kategóriában), ne cherry-pick.

**report.md:** rövid magyar leírás: módszer, paraméterek, mit mutatnak a képek, mire figyeljen a user az ellenőrzésnél
(pl. „túl sok vonal?”, „hiányzik nyilvánvaló szint?”), és nyitott kérdések a paraméterhangoláshoz.

## Acceptance Criteria
- [x] Riport generálható CLI-vel / builder hívással, újrafuttatható
- [x] A riport a manifest sémának megfelel, az API `/api/reports` listázza (general), az UI megjeleníti
- [x] Összefoglaló táblák léptékenként + képek a fenti kategóriákban
- [x] Builder teszt `src/reports/tests/` alatt (kis szintetikus adaton)
- [x] `ruff check` + `pyright` + pytest zöld

## Notes

### t911 — analyst_agent (2026-11-19)

**Fájlok**
- új: `src/reports/builders/sr_detection.py` (builder; `build(root=None, *, df_1m=None, params=None, asset_id=None)`, `load_ohlcv_1m()`, `select_examples()`)
- új: `src/reports/tests/test_sr_detection.py` (5 teszt, szintetikus 1m adat, rövidített lookback-paraméterek)
- generált: `_doc_/reports/sr_trendline_detection/` (`manifest.json`, `report.md`, 15 chart, 10 tábla; charts ~0.8 MB)
- más fájlhoz nem nyúltam (a `src/reports/builders/` alatti egyéb `M` fájlok korábbi user-változások)

**Újragenerálás** (repo gyökérből, ~35 s: betöltés 3 s + detektor 25 s): `uv run python -m reports.builders.sr_detection`
- Adat: `live.ohlcv` a `readonly_query.open_readonly_connection`-nel (READ_ONLY attach, lock esetén 10× 3 s újrapróba). Futáskor nem volt lock.
- Determinisztikus: azonos adat + config → azonos példák (csak `created_at`/`updated_at` változik). Új adattal a példák és az áttekintő ablakok eltolódnak.
- Ellenőrizve: `store.list_reports('general')` → `target_yearly_regime` (order 10), `sr_trendline_detection` (order 20); a futó BFF (`:8600`) `GET /api/reports` listázza, a chart endpoint 200. A chartokat a frontend sötét témájával offline PNG-re renderelve (ideiglenes `uv run --with kaleido==0.2.1`, nem projektfüggőség) vizuálisan átnéztem; böngészőben az UI-t nem néztem.

**Tartalom**
- Táblák: `params`, `scale_summary`, `level_touch_dist`, `line_summary`, `line_touch_dist`, `event_counts`, `event_yearly`, `forward_raw` (NYERS, placebo nélkül — címben, szövegben jelölve), `active_levels_now`, `examples`; chart: `event_monthly` (log).
- Áttekintő: `overview_4h_m_l` (utolsó 90 nap, 4h; l folytonos, m szaggatott; 16 l + 41 m szint látható), `overview_1h_s` (utolsó 30 nap, 1h; 42 s szint + 51 trendvonal).
- Jelölés: zöld = támasz, piros = ellenállás, a *pillanatnyi* szerep szerint (a szint színe kitörésnél vált). Vastagság + átlátszóság = erő 3 fokozatban; szintnél az adott pillanatban ismert `strength` (Σ exp(−kor/τ)), határok 0.5 / 1.2 (a kirajzolt idő ~50/35/15%-a; az 1/2 határ mellett 80% „gyenge” lett volna). Trendvonalnál az addig ismert érintésszám: 3 / 4 / ≥5. Minden objektum csak `confirmed_at` → elévülés (vonal: törés) között látszik, szakaszonként (a szakasz ott bomlik, ahol a szerep vagy az erő-fokozat vált).
- Példák (12 db, `ex01..ex12`): utolsó 365 nap; kategóriánként léptékenkénti rangsor (érintésszám az eseménykor ↓, MFE ↓ [hamis törésnél: visszatérés bar ↑], idő ↓), round-robin `l → m → s`, egy objektum egyszer, a kulcsesemények közt ≥ 7 nap. Szerepcserénél és vonal-retestnél a break `follow_through` és a retest `hold` (nyers címke). Ablak: legfeljebb 90 scale-bar a kulcsesemény előtt + 24 bar forward + 6.

| # | kategória | lépték | objektum | kulcsesemény (bar zárás, UTC) | érintés az eseménykor |
|---|---|---|---|---|---|
| 1 | szint + bounce | l | l-L000064 | 2025-12-12 00:00 | 7 |
| 2 | szint + bounce | m | m-L000574 | 2026-05-24 00:00 | 8 |
| 3 | szint + bounce | s | s-L002582 | 2026-10-28 21:00 | 9 |
| 4 | szerepcsere | l | l-L000091 | 2026-07-09 00:00 | 4 |
| 5 | szerepcsere | m | m-L000566 | 2026-05-05 20:00 | 8 |
| 6 | szerepcsere | s | s-L002502 | 2026-08-18 07:00 | 7 |
| 7 | vonal-törés + retest | l | l-T000119 | 2026-03-28 00:00 | 3 |
| 8 | vonal-törés + retest | m | m-T000818 | 2026-04-27 16:00 | 4 |
| 9 | vonal-törés + retest | s | s-T003012 | 2026-01-27 16:00 | 5 |
| 10 | hamis kitörés | l | l-L000058 | 2025-12-04 00:00 | 12 |
| 11 | hamis kitörés | m | m-L000572 | 2026-05-16 12:00 | 7 |
| 12 | hamis kitörés | s | s-L002297 | 2026-03-12 15:00 | 9 |

**Kulcs darabszámok** (teljes történet 2020-09-14 → 2026-11-19, 3 250 462 1m sor; egyeznek a t910 Notes-szal)

| lépték | pivot | szint (támasz/ellenállás megerősítéskor) | érintés med/p90/max | élettartam med/p90 (nap) | kerek 10 / 5 | szerepcserés | trendvonal (támasz/ellenállás) | vonal törve ért véget | esemény szint t/b/r ; vonal t/b/r |
|---|---|---|---|---|---|---|---|---|---|
| s | 10 579 | 2 622 (1 323 / 1 299) | 3 / 5 / 13 | 7.5 / 14.6 | 6.8% / 7.7% | 78% | 3 481 (1 774 / 1 707) | 93% | 11 264/8 187/3 708 ; 4 839/3 230/1 385 |
| m | 2 625 | 642 (325 / 317) | 3 / 6 / 13 | 31.8 / 62.5 | 14.0% / 15.6% | 78% | 903 (439 / 464) | 93% | 2 735/1 997/940 ; 1 166/843/361 |
| l | 443 | 104 (57 / 47) | 2.5 / 5 / 12 | 180 / 331 | 31.7% / 26.9% | 77% | 137 (68 / 69) | 96% | 374/300/140 ; 181/131/38 |

Trendvonal medián élet: s 0.54 nap, m ~1.9 nap, l ~11 nap; meredekség (ATR/bar) medián támasz +0.04..+0.05, ellenállás −0.06..−0.11.
NYERS pozitív arány (placebo nélkül): szint touch→bounce s 66% / m 66% / l 58%; szint break→follow_through 51/53/55%; szint retest→hold 21/20/22%; vonal touch→bounce 56/53/51%; vonal break→follow_through 58/63/66%; vonal retest→hold 30/34/38%.

**Megjelenítési korlátok / megjegyzések**
- A frontend a chart JSON `config`-ját nem adja át (csak `data` + `layout`), ezért chart-szintű config nincs. Csak a sötét UI-témára hangoltam (`#111418` panel); a `theme.LONG` / `SHORT` szín a gyenge fokozatban halványabb, de olvasható. Frontend-módosítás nem kellett.
- Hover a szakaszok elején, közepén és végén van (Plotly scatter csak pontokon hoverel), illetve a `confirmed_at` csillagon, a pivotokon és az eseményeken. A `layout.shapes` nem hoverel, ezért a szinteket trace-ként rajzoltam.
- Az y-tengely a gyertyákhoz igazodik (±4%); a képen kívüli objektumok nem rajzolódnak ki (darabszámuk a szövegben, most 0).
- Ha a `confirmed_at` a példa-ablak előtt van (hosszú életű szint), üres csillag kerül a bal szélre, és egy felirat jelzi a dátumot és az ablak előtti érintés-pivotok számát.
- A rangsor „legtöbb érintés” miatt a szint-példák jellemzően hosszú életű, sokat tesztelt szintek, sok eseménnyel (zsúfolt markerek oldalazásban — pl. ex12).
- A gyertyák szürkék, így a zöld/piros csak a szerepet jelöli.

**A usernek a vizuális ellenőrzéshez** (a report.md 4. fejezetében is)
- OOS-szennyeződés: a riport a 2025–2026-os időszakot is mutatja (1700 nyitott döntés: kontaminált jelölés vagy train-re korlátozás).
- Szelekciós torzítás: paraméterváltoztatás e riport után = új verzió, a t913-at azzal kell futtatni.
- Feltűnő: (1) a szintek ~77–78%-a szerepet cserél, oldalazásban oda-vissza törések sorozata (ex12); (2) a trendvonalak 93–96%-a töréssel ér véget, rövid élettel, a 3. érintés megerősítése gyakran közvetlenül a törés előtt jön (ex07: kb. 1 barral); (3) az `l` kerek-10 flag 32% — 1d ATR mellett közel triviális; (4) a 90 napos 4h képen sűrű, közel párhuzamos `l`/`m` támasz-köteg a 72–80 sávban (túl sűrű?); (5) a retest `hold` szigorú definíció, ezért ritka (20–38%).

### [validator] Validated — 2026-11-19
- `uv run ruff check` → All checks passed. `uv run pyright` 6 hibát talált a `src/reports/tests/test_sr_detection.py` 125–127. sorában (a plotly stub hibásan típusozza a `Figure.data`-t). Javítva: a trace-eket `traces: list[Any] = list(fig.data)` listán iterálja a teszt → 0 errors.
- pytest: az 5 builder-teszt zöld (a javítás után újrafuttatva: 14/14 az epic tesztjeiből).
- A generált `_doc_/reports/sr_trendline_detection/manifest.json` megfelel a `Manifest` sémának (general, 15 chart, 10 tábla). A report.md jelöli, hogy a forward arányok NYERSEK (placebo nélkül). A builder nem olvas nyers JSON configot.
- Nem-blocker: az UI-t böngészőben senki nem nézte meg (az analyst offline PNG-n ellenőrzött). A vizuális ellenőrzőpont (⛔) a userre vár, mielőtt a t912 indulhat.
