# Epic 079: Kitörés-előrejelezhetőség v2 — a maximum kihozása modellezéssel, túltanulás nélkül (BTC long)

## Goal
Az epic_078 gyenge modell-térrel (lineáris + hangolatlan GBM), kevés mintán (3 530 teszt-jelölt) és pillanatkép-változókkal futott. Itt a modellezést a lehető legjobbra állítjuk, hogy vagy találjunk valódi, placebón túli előrejelzést, vagy **magabiztosan és erővel együtt** lezárjuk: a BTC-n quant (1m ár, volumen, flow, funding) változókkal a kitörés nem becsülhető.

## Scope
- Asset: BTCUSDT 1m, csak long, **csak BTC-adat (nincs cross-asset)**. Ablak 2021-02-04 → 2026-01-31; OOS (2026-02-től) érintetlen; ha valami túlél, az OOS egyszeri felnyitásáról külön kérdezünk.
- Mintakészlet: T = 15, 30, 60, 120, 240, 480, 960 perc, k = 4 (k = 3 robusztusság); skálák közti klaszterezés (azonos mélypont), 1/klaszterméret súly, effektív mintaszám közölve. Kauzális kizárások: V-alakú azonnali visszapattanás (flash), adathiányos ablak.
- Célok (előre rögzítve): (1) nagy mozgás az analitikus véletlen-bolyongási offset felett: `p_rw = exp(−(k − risk_z)/(ρ·k))`; (2) +1R a stop előtt; (3) előre vett hozam 0,5T / 1T / 2T-re σ-egységben (rang-IC).
- Változók: az epic_078 48 változója + dinamika (RSI/VWAP/flow/volumen/vol változása 0,25T / 0,5T / 1T alatt), zigzag-sorozat (utolsó 6 láb), nagyobb (4T) skála állapota, Wyckoff-szerű (spring, teszt, erőfeszítés/eredmény), rezsim (trend, vol-percentilis, napszak/hét napja, idő a legutóbbi nagy mozgás óta, funding-változás).
- Modellek (előre): M0 offset+rekalibráció, M1 ridge-logit/ridge offsettel, M2 sekély LightGBM (init_score = offset), M3 ExtraTrees a reziduumra, M4 rang-átlag.
- Védelem: beágyazott purge-olt idő-CV (hiperparaméter + változószám csak belül), külső féléves tesztblokkok 2023H1–2025H2, tanulási görbe, train–teszt rés.
- Nullok: blokk-permutált címkék (200) + véletlen előjelű placebo (kb. 24), a **legjobb-mindenből** statisztika null-eloszlása.
- Szegmensek (max. 4, előre): összes; friss mélypont vs magasabb mélypont (chain); a 30 napos trend iránya; „tiszta" rezsim (nem oldalazó).

## Döntési szabály (előre)
Találat, ha egyszerre: (1) a külső teszt javulása az M0 felett a nap-klaszteres CI alsó határán > 0; (2) a null-maximum 95. percentilise fölött; (3) azonos előjel ≥ 2 évben és ≥ 2 skálán; (4) a legjobb tizedben gazdaságilag számít (hit rate / díj utáni R). Különben lezárás a teljesítmény-számítással (MDE): mekkora hatást tudtunk volna kimutatni.

## Tasks
- t970: Mintakészlet, offset, célok, erő-számítás (analyst_agent)
- t971: Új változók + look-ahead teszt (analyst_agent)
- t972: Pipeline: beágyazott CV, modellek, kiválasztás, null-gépezet (analyst_agent)
- t973: Futtatás: modellek × célok × szegmensek (analyst_agent)
- t974: Nullok: permutáció + placebo, maximum-statisztika (analyst_agent)
- t975: Riport + cikk + ítélet; a korábbi cikk verdiktjének javítása (analyst_agent)

Sorrend: t970 → t971 → t972 → t973 → t974 → t975
