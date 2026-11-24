# Epic 080: Elliott-hullámok tesztje BTC 1m-en — becsülhető-e a 3. és az 5. hullám?

## Goal
Az Elliott-szabályok szerint (kauzálisan, megerősített pivotokból) keresünk korai impulzusokat: (A) beágyazott 1-2-1-2 után a 3. hullámot, (B) 1-2-3-4 után az 5. hullámot (long). Megmérjük, milyen gyakran követi a setupot jelentős hullám a szerkezeti stop előtt, és hogy ez többet ad-e (1) az analitikus véletlen-bolyongás találati aránynál, (2) a szabályokat nem teljesítő, azonos alakú pivot-ablakoknál (kontroll), (3) a véletlen előjelű placebón.

## Scope
- BTCUSDT 1m, csak long, nincs cross-asset (SOL/ETH csak akkor, ha BTC-n kiemelkedik valami). Ablak 2021-02-04 → 2026-01-31; OOS (2026-02-től) érintetlen.
- Pivotok: az epic_077 σ-normált zigzagja, T = 15…960 perc (7 skála) × k ∈ {3, 4} (ρ = 0,5).
- Setup A (p3 < p1): kemény szabályok p2 > p0, p4 > p2; Fibonacci-változat: retr2, retr4 ∈ [0,382; 0,786]. Setup B (p3 > p1): p2 > p0, p4 > p2, p4 > p1, W3 ≥ W1; Fib: retr2 ∈ [0,382; 0,786], retr4 ∈ [0,236; 0,5].
- Belépés a P4 megerősítő bar zárásán. Stop: p4 (szoros), p2 (tág). Célok: A elsődleges p2 + 1,618·W1, másodlagos p1; B elsődleges p4 + W1, másodlagos p3. Időkorlát 60·T bar; a nem lezárt események kimaradnak (arányuk közölve).
- Mérce: p_rw = D/(U + D) (driftmentes bolyongás, először a cél vagy a stop), kontroll = a szabályokat nem teljesítő, azonos képlettel számolt pivot-ablakok, placebo = véletlen előjelű ár, a teljes lánc újrafuttatva.
- Díj: 13 bps. 16 előre rögzített változat (család × cél × stop × szabályszint); null: a változatok maximuma.

## Döntési szabály (előre)
Találat, ha egyszerre: (1) az Elliott-setupok többlete a p_rw felett és a kontroll felett is a nap-klaszteres CI alsó határán > 0; (2) a placebo-null maximumának 95. percentilise fölött; (3) azonos előjel ≥ 2 évben és ≥ 2 skálán; (4) díj (13 bps) után a nettó átlag pozitív. Különben lezárás MDE-vel.

## Tasks
- t980: Detektor A / B, kauzális, look-ahead teszt (analyst_agent)
- t981: Események, címkék, gyakoriság, RW-mérce, kontroll (analyst_agent)
- t982: Placebo-null, erő (MDE) (analyst_agent)
- t983: Feltételes vizsgálat: Fib-arányok, skála, év (analyst_agent)
- t984: Riport / doc + a régi Elliott-lapok pontosítása (analyst_agent)
- t985: Validálás (validator_agent)

Sorrend: t980 → t981 → t982 / t983 → t984 → t985
Kapcsolódik: epic_077–079 (zigzag, offset, placebo-gépezet), `_doc_/research/elliott_waves/`.
