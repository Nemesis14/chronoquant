# Epic 077: Multi-skálás nagy-mozgás katalógus (long, BTC) — előelemzés

## Goal
A Scout (epic_072/073) fix skálájú (W=4h, A ≥ 1,5σ_W, H=8h) eseménydefiníciója helyett **felülről lefelé, skálafüggetlen** megközelítés:
az 1m adatból megkeressük a nagy emelkedéseket, leírjuk az első lökésük és a korai szakaszuk szerkezetét, és megmérjük,
mennyi ebből a struktúra (placebóhoz képest), és érvényes-e a fraktál-feltevés (az arányok skálánként hasonlók-e).
**Modell nem készül** — csak leíró elemzés, darabszámok, arányok, null-összehasonlítás. Eredménye a későbbi
setup-detektor és kimenet-modell (Scout 2) bemenete.

## Scope
- Asset: **BTCUSDT**, 1m, csak **long** (a short a 2. kör; tükrözés a −log ár soron).
- Idő: train-ablak 2021-01-01 → 2026-01-31. OOS (2026-02 →) és holdout-2 (2026-08 →) **érintetlen**.
- Kód: `src/reports/builders/` (új builder-ek; újrahasznosítható: `wave_events.py`, `_levels_polars.py`), kimenet `research."move_census_v1_*"` táblák (lab DB) + riportok (`_doc_/reports/`).
- Futtatás: `research` uv csoport, ha kell.
- A quant fw60 modellt nem érinti (user döntés, 2026-11-24).

## Key Decisions
- **Skála-rács:** 15 perctől ~3 napig, geometriai lépésekkel (~8 skála). Ahol a darabszám üres/túl kicsi, a riport jelzi, nem erőltetjük.
- **„Nagy mozgás":** nettó elmozdulás ≥ k × az adott skálán mért trailing 30 napos empirikus σ (nem √T skálázás), + trendesség (efficiency ratio) és max. belső visszaesés feltétel. **k-rács (3, 4, 6) előre rögzítve**, érzékenység közlendő, nincs utólagos küszöbválasztás.
- **Ciklusszám (k), nem Elliott-számlálás:** 0 = induló lökés + első korrekció; 1 = egy nagy lökés + korrekció után; ≥2 = több ciklus után. A 3-as/5-ös hullám valós időben nem megkülönböztethető.
- **Katalógus utólagos (hindsight)**, ezért a valószínűség csak a nevezővel együtt értelmezhető (t953). A leíró keret (hossz, méret, arányok) nem előrejelzés.
- **Placebo kötelező** (epic_071 tanulság): blokk-bootstrap / napon belüli keverés, megőrzött vol-klaszterezéssel.
- **Fraktál-feltevés tesztelendő:** arányok (első lökés/szülő-mozgás, korrekció mélysége, időarányok) skála-invarianciája.
- Csak long az 1. körben, hogy az eredmény értelmezhető legyen.

## Tasks
- t950: Multi-skálás szakasz-detektor + nagy-mozgás katalógus (BTC long) (analyst_agent)
- t951: Placebo-null a katalógus rácsára (analyst_agent)
- t952: Anatómia: leg-felbontás, ciklusszám, arányok, skála-invariancia (analyst_agent)
- t953: Nevező: korai-impulzus jelöltek és a nagy mozgássá válás aránya (analyst_agent)
- t954: SOL/ETH replikáció + Scout-események elhelyezése a katalógusban (analyst_agent)

Sorrend: t950 → (t951, t952) → t953 → t954
