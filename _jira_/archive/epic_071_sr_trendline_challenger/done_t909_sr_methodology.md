---
epic: epic_071
id: t909
title: Írd meg az S/R + trendvonal detektálás módszertani rationale-jét
assignee: methodology_agent
status: done
blocks: [t913]
blocked_by: []
---

## Goal
Kód-mentes módszertani dokumentum a `_doc_/methodology_doc/` zónában, amely rögzíti — **az adatok
megnézése előtt** — a támasz/ellenállás és trendvonal detektálás definícióit, a memória-léptékeket,
az eseménytípusokat, a kimenet-címkéket és a későbbi placebo-teszt (t913) go/no-go küszöbeit.

## Scope
- Új fájl: `_doc_/methodology_doc/` alatt, a zóna számozási konvenciója szerint (nézd meg a meglévőket).
- Hivatkozások: Osler (2000) NY Fed EPR "Support for Resistance" (placebo-szint módszer),
  Osler Staff Report 125 (stop-loss/take-profit klaszterek, kerek számok), Lo–Mamaysky–Wang (2000),
  Sullivan–Timmermann–White (1999) data-snooping.

## Tartalmi spec (ezt kell indokolni és rögzíteni)
1. **Léptékek és memória:** s = 1h bar / 7 nap, m = 4h bar / 30 nap, l = 1d bar / 180 nap visszatekintés.
   Szint-erő = Σ exp(-kor/τ) az érintéseken, τ = lookback/3. Miért több lépték, miért nem egy ablak.
2. **Pivot:** fraktál pivot n=3 scale-bar mindkét oldalon + minimum prominencia 1.0·ATR14(scale).
   `confirmed_at = pivot_time + n bar` — a pivot ettől létezik.
3. **Vízszintes szint:** pivotok (csúcs+mély együtt) klaszterezése 0.25·ATR tűréssel; ≥2 érintés.
   Kerek szám flag (SOL: 10 többszörösei, másodlagosan 5). Áttörés után szerepcsere (role flip).
4. **Trendvonal:** azonos típusú pivotokon át (emelkedő mélypontok = támasz, csökkenő csúcsok = ellenállás),
   ≥3 érintés ε = 0.25·ATR tűréssel (2 érintéses jelölt flaggel), két szélső érintés között nincs ε-nál
   nagyobb vonalon túli záróár. A vonal a 3. (ill. 2.) érintés pivotjának `confirmed_at`-jától létezik.
5. **Események:** touch (ár ε-on belül, a megközelítés oldaláról), break (záróár 0.5·ATR-rel túl),
   retest (break után N=12 scale-baron belül visszatér ε-on belülre).
6. **Kimenet-címkék (csak kutatáshoz, soha nem feature):** touch → bounce, ha ≥1·ATR elmozdulás a szinttől
   24 scale-baron belül, mielőtt break történne; különben break. Break → follow-through vs hamis kitörés.
   Retest → hold vs fail.
7. **Placebo-teszt (t913):** véletlen szintek azonos távolság-eloszlással; véletlen vonalak azonos
   meredekség-eloszlással. Go küszöb: bounce-rate különbség ≥ 5 százalékpont, bootstrap p < 0.01,
   azonos előjel a train (2021–2024) és OOS (2025–2026) időszakon.
8. **Look-ahead szabályok** és a t-1 lag kapcsolata a feature-ökhöz (t912).

## Acceptance Criteria
- [ ] Egy md fájl a `_doc_/methodology_doc/`-ban, kód-mentes, magyarul
- [ ] Minden fenti paraméter indoklással és forrással
- [ ] Go/no-go küszöbök explicit táblázatban
- [ ] Ha a zónának van indexe/README-je, frissítve

## Notes
- Létrehozva: `_doc_/methodology_doc/1700_features_sr_trendline.md` (kód-mentes, magyar). Számozás: a feature-csoportok sorában a 1600 után (1100–1600 foglalt); a `1000_features.md` "Részletes alfejezetek" táblázatába felvéve.
- Meglévő SR Levels (1500, rolling high/low + előző session H/L) elkülönítve: nem pivot-alapú, az új detektor kiegészíti, nem helyettesíti.
- Spec-paraméterek értékei NEM változtak. Pontosítások: (1) `confirmed_at` definíció egyértelművé téve: a `pivot_time + n bar` nyitóidő-alapú értelmezésben egy bar-nyi look-ahead marad; pontos definíció: pivot-bar nyitóideje + (n+1) bar = i+n bar záróideje. (2) Bounce iránya explicit. (3) Break/follow-through, retest/hold címkék definíciója és a placebo/bootstrap részletek (blokk 1 nap, kétoldalas p) feltételezésként rögzítve. (4) Min. 100 esemény/cella: feltételezés, nincs a spec-ben.
- Nyitott döntések (user): (a) t911 utáni paraméter-fagyasztás és OOS-szennyeződés (a riport 2025–2026-ot is mutat); (b) többszörös teszt korrekció (~18 cella, p<0,01 cellánként); (c) OOS-felosztás igazítása a champion valid kezdetéhez (2025-05-01).
- Forrás-megjegyzés: az Osler (2000) placebo-szint eljárás részletei nem lettek ellenőrizve a forrásban (nincs közvetlen web-hozzáférés ebben a futásban); a dokumentum ezt validálásra várónak jelöli.

### [validator] Validated — 2026-11-19
- Forma: egy md (`1700_features_sr_trendline.md`), magyar, kód-mentes (csak mermaid diagramok, a zónában szokásos), `1000_features.md` index frissítve.
- Tartalom: a spec 1–8. pontja paraméter-táblával és indoklással; go/no-go küszöbök explicit táblázatban; look-ahead szabályok táblázatban. A feltételezések (min. 100 esemény/cella, 1 napos bootstrap blokk) jelölve.
- Nem-blocker: a nyitott user-döntések (OOS-szennyeződés, többszörös teszt korrekció, OOS-határ) a t913 előtt döntendők. A doksi "2 érintés = jelölt flag"-et említ; a t910 ezeket nem adja ki (a spec opcionálisnak hagyta).
