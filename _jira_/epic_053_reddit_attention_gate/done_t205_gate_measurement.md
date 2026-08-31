---
epic: epic_053
id: t205
title: Gate mérés — IC/MI, permutációs placebo, FDR
assignee: modeling_agent
status: done
blocks: [t206]
blocked_by: [t201, t204]
---

## Goal

Megmérni, hogy a `t-1` lagolt figyelem-feature-ök hordoznak-e információt a
BTC 60 perces kilengésről. **Ez a gate — ez dönti el, folytatódik-e az epic.**

## A két kérdés, külön

**(a) Magnitúdó — ez a fő hipotézis.**
Cél: `|fw60_close_logret|`, `long_mfe_fw60`, `|short_mfe_fw60|`.
Elvárás: pozitív, monoton kapcsolat — nagyobb figyelem, nagyobb kilengés.

**(b) Irány — kontroll, nem elvárás.**
Cél: előjeles `fw60_close_logret`.
Az előzetes várakozás **nulla**. Ha itt jön ki "jel", az gyanús, és a
placebónak ki kell szűrnie.

## Mérés

- Órás feature → 1 perces bar forward-fill (a feature konstans az órán belül;
  ezt a riportban ki kell mondani, nem elrejteni)
- IC (Spearman) és mutual information, feature-enként és horizontonként
- Overlapping fw60 ablakok miatt **blokk-bootstrap** vagy Newey-West
  korrekció — a naiv standard hiba itt nagyságrenddel alábecsülne
- Rezsim-bontás: fő mérés **2021-01 →**, külön a 2017-2020 szakasz
  robusztusság-ellenőrzésként

## Validáció — az `epic_051` fegyelme kötelező

- **Permutációs placebo**: a feature-idősort blokkosan permutálva
  (autokorreláció megőrizve, a naiv shuffle túl könnyű baseline), min. 5000
  ismétlés
- **Benjamini-Hochberg FDR** a feature × cél × horizont családra
- Nyers `p < 0,05` önmagában **nem** eredmény — ez a tanulság az `epic_051`-ből,
  ahol a legjobb jelölt nyers `p = 0,0062`-ről FDR után `0,849`-re esett

## Scope

- `src/modeling/analyst_signal/` — gate mérés modul
- újrahasznosítható: [validate.py](../../src/modeling/analyst_signal/validate.py)
  permutációs + FDR logikája
- output: `oos_validation`-höz hasonló eredménytábla az analyst_signal.duckdb-ben

## Acceptance Criteria

- [x] IC és MI megvan minden feature × cél párra, rezsim-bontásban
- [x] Overlapping-korrigált standard hiba (blokk-bootstrap vagy Newey-West) —
      lásd Notes: óránkénti `:00` mintavétel miatt a fw60 ablakok nem
      átfednek, a target oldalon nincs szükség korrekcióra; a feature oldal
      autokorrelációját a blokk-permutáció kezeli
- [x] Permutációs placebo lefutott, min. 5000 ismétlés
- [x] FDR-korrigált p-értékek táblázatban
- [x] Explicit **go / no-go** állítás: a magnitúdó-ág túléli-e a korrekciót
- [x] Erő-elemzés: mekkora valódi hatás lett volna kimutatható ezen a mintán
      (az `epic_051` riport ezt a részt csinálta jól — kövesd)
- [x] `ruff check` + `pyright` tiszta

## Notes

[orchestrator] LEFUTOTT — 2026-08-21

Panel: **73 269 megfigyeles** (78 822 oras `:00` bar x 78 888 feature-ora,
inner join). Az oranként EGY megfigyeles miatt a fw60 ablakok NEM atfedok
(`h:00` -> `h:01..h+1:00`), tehat nincs szukseg atfedes-korrekciora.
100 teszt rezsimenkent (4 cel x 5 feature x 5 sorozat), 5000 blokk-permutacio,
168 oras blokk, Benjamini-Hochberg FDR.

### Eredmeny: **GO** — a magnitudo-ag atmegy, az irany-ag nem

| rezsim | cel | FDR<0,05 tulelo | max abs IC |
|---|---|---:|---:|
| main 2021-2025 | `abs_fw60_close_logret` | **13 / 25** | 0,1770 |
| main 2021-2025 | `abs_short_mfe_fw60` | **13 / 25** | 0,1753 |
| main 2021-2025 | `long_mfe_fw60` | **15 / 25** | 0,1605 |
| main 2021-2025 | `fw60_close_logret` (IRANY) | **0 / 25** | 0,0089 |
| robust 2017-2020 | `abs_fw60_close_logret` | 13 / 25 | 0,1242 |
| robust 2017-2020 | `abs_short_mfe_fw60` | 14 / 25 | 0,1285 |
| robust 2017-2020 | `long_mfe_fw60` | 11 / 25 | 0,1151 |
| robust 2017-2020 | `fw60_close_logret` (IRANY) | **0 / 25** | 0,0106 |

**A kontroll-ag pontosan ugy viselkedik, ahogy elore rogzitettuk.** Az iranyra
nulla tulelo mindket rezsimben — ez a legerosebb ervenyessegi jel az egeszben.
Ha a pipeline barmibol jelet gyartana, az iranyon is felvillanna.

**A legerosebb feature: `items_per_author`** (`n_items / n_authors`, azaz a
beszelgetes intenzitasa resztvevonkent), nem a nyers figyelem-volumen:

| sorozat | feature | cel | IC | FDR |
|---|---|---|---:|---:|
| BitcoinMarkets | items_per_author | abs_fw60_close_logret | 0,1770 | 0,0005 |
| BitcoinMarkets | items_per_author | abs_short_mfe_fw60 | 0,1753 | 0,0005 |
| Bitcoin | items_per_author | abs_short_mfe_fw60 | 0,1645 | 0,0005 |
| BitcoinMarkets | items_per_author | long_mfe_fw60 | 0,1605 | 0,0005 |

**Ero-elemzes**: main rezsim n=43 680, 80% erohoz min |rho| = 0,0134
(alpha=0,05) / 0,0207 (Bonferroni). A megfigyelt max |IC| = 0,1770,
azaz **~13x a kimutathatosagi kuszob**. Ez nem hatarhelyzet.

### KET FENNTARTAS — ezeket a t206 riportnak ki KELL mondania

**1. Ez NEM bizonyitja az inkrementalis erteket.** A gate azt mutatja, hogy
letezik statisztikai kapcsolat a Reddit-aktivitas es a BTC 60 perces
kilenges-nagysaga kozott. Azt NEM mutatja, hogy ez barmit hozzatesz ahhoz,
amit a modell mar tud. A `items_per_author` -> `|hozam|` kapcsolat nagy
valoszinuseggel jelentos reszben **volatilitas-clustering**: a Reddit-beszelgetes
felporog volatilis idoszakokban, a volatilitas pedig autokorrelalt. A meglevo
modellben mar vannak volatilitas-feature-ok.

**A dontо kerdes — ami ebben a taskban NEM szerepelt — az, hogy a feature ad-e
barmit a trailing realized volatility felett** (parcialis korrelacio /
inkrementalis IC). Enelkul a riport tulertekelne az eredmenyt.

**2. A blokk-permutacio blokkhossza vitathato.** ACF(att_z): lag 24h -> 0,457,
lag **168h -> 0,285**. A valasztott blokkhossz 168h, de a fuggoseg egy heten
tul is jelentos. Hosszabb blokk (pl. 720h) konzervativabb null-t adna.
A hatasmeret 13x a kuszob felett van, tehat ez valoszinutlenul forditja meg az
eredmenyt, de a riportban szerepelnie kell.

---

### [modeling_agent] KORREKCIÓ — 2026-08-21 — a 168h blokkhossz valódi torzítást okozott, nem csak "vitatható" volt

A fenti "2. fenntartás" alábecsülte a saját problémáját. A blokkhossz nem
csak konzervativitási kérdés volt — **torzított null-eloszlást adott**, amit
egy konkrét ellenőrzés lepleztet le. Az eredeti (168h) futásban volt egy
gyanús sor: `BitcoinMarkets/authors_z/abs_short_mfe_fw60` IC=-0,0007
(gyakorlatilag nulla) mégis `p=0,0002` (a legszélsőségesebb lehetséges
érték). Ez csak úgy lehetséges, ha maga a null-eloszlás nincs nulla körül.
Direkt kiszámoltam a null-t: **`null_mean=-0,062`** — erősen eltolva.

**Az ok**: a blokk-permutáció csak a blokkok SORRENDJÉT keveri, a blokkon
belüli pozíciót (óra-a-héten fázist) nem érinti. Ha a blokkhossz a 24
többszöröse (168 = 24×7), minden permutáció megőrzi az óra-a-héten fázist
minden pozícióban. Ez csak akkor torzításmentes, ha a feature-nek pontosan
nulla az átlaga minden egyes óra-a-héten fázisban. Az `att_z`/`authors_z`
deszezonalizálása (`social_features.py`) egy **kauzális, expanding** trailing
átlaggal történik — ez valós, nullától eltérő reziduális szezonalitást hagy
maga után, ami a target saját szezonalitásával (kereskedési idő szerinti
volatilitás-mintázat) kombinálva torzítja a null-t. Ez FÜGGETLEN attól, hogy
a feature "trailing-normalizált"-e — még a valóban deszezonalizált
`att_z`/`authors_z` is érintett volt, csak nem mindig olyan látványosan mint
a fenti példa.

**Javítás**: `block_len=169` (nem többszöröse 24-nek, megtöri a
fázis-lezárást). Ellenőrizve több sorozat×cél párra: `null_mean` minden
esetben ~0-ra esik vissza (pl. `Bitcoin/att_z/long_mfe_fw60`:
`null_mean=-0,0262` @168h → `null_mean=-0,0003` @169h), miközben a **valós**
korrelációk gyakorlatilag változatlanok (0,03009 @168h vs 0,03003 @169h —
a blokkhossz csak a null-t érinti, az adatot nem). A javítás a
`gate_measurement.py`-ban a `BLOCK_LEN_HOURS=169` konstansban és a hozzá
tartozó docstringben van dokumentálva.

**Újrafuttatva `block_len=169`-cel, a DB-be írva (`gate_validation`/
`gate_power`, `analyst_signal.duckdb`) — ez a hiteles, végleges eredmény,
nem a fenti (168h-s) táblázat.**

Panel: n=43 771 (main), n=29 406 (robustness) — kis eltérés a fenti 43 680-tól
a blokk-hosszhoz igazított maradék-levágás miatt, érdemi hatás nélkül.

| rezsim | teljes csalad (100 teszt) FDR<0,05 | irany-kontroll FDR<0,05 |
|---|---:|---:|
| main 2021-2025 | 26/100 | 0/25 |
| robust 2017-2020 | 37/100 | 0/25 |

Az irány-kontroll a javítás UTÁN is tiszta nulla mindkét rezsimben (min nyers
p 0,060 / 0,070) — ez nem változott, továbbra is a legerősebb érvényességi
jel.

**A magnitúdó-oldal FDR-túlélőinek száma csökkent** (main: 41→26, mert a
168h-s futásban több álpozitív is FDR alá esett a torzított null miatt), de
**a fő következtetés nem változik: GO.**

**Fontosabb pontosítás a "legerősebb feature" állításra.** A fenti táblázat
`items_per_author`-t emeli ki mint legerősebb — ez a szám igaz, de
**megbízhatatlan bizonyíték**, egy konkrétan kimutatott ok miatt (nem csak
"valószínűleg volatilitás-clustering", ahogy az 1. fenntartás sejti):

- `items_per_author` és `chars_per_item` **nem trailing-normalizált** a
  `social_features.py` kódja szerint (`n_items/n_authors` ill.
  `n_chars/n_items`, csak t-1 lag-gel) — ellentétben a t205 ticket eredeti
  leírásával, ami "MINDEGYIK" feature-t trailing-normalizáltnak mondta.
- Évenkénti átlaga monoton csökken minden subredditre (Bitcoin
  `items_per_author`: 1,73 (2021) → 1,38 (2025)), és a `long_mfe_fw60` célé
  is csökken ugyanebben az ablakban (0,00565 → 0,00276) — **közös többéves
  trend**, nem attention→magnitúdó kapcsolat.
- Éven belüli IC (Bitcoin, `items_per_author` vs `long_mfe_fw60`): 2021=0,09,
  **2022=-0,027**, 2023=0,025, 2024=0,024, 2025=0,047 — inkonzisztens,
  előjelet vált, jóval gyengébb mint a pooled 0,136. Ez a between-year
  confounding klasszikus jele.
- Kontrollként: `att_z` évenkénti átlaga ~0 minden évben (ahogy egy valóban
  detrendelt z-score-tól elvárható), éven belüli IC-je (0,094, 0,064, -0,053,
  0,044, 0,065) jóval stabilabb, nincs mögötte monoton trend.

**A hiteles bizonyíték tehát `att_z` és `authors_z`** (a ténylegesen
trailing-normalizált attention/szerző-volumen feature-ök), NEM
`items_per_author`/`chars_per_item`:

| rezsim | FDR<0,05 túlélő (att_z+authors_z, 27 teszt) | IC tartomány | előjel |
|---|---:|---|---|
| main 2021-2025 | 14/27 | 0,022 .. 0,059 | Bitcoin, CryptoCurrency, CryptoMarkets, ALL_AVG: **+**; BitcoinMarkets: negatív, n.s. |
| robust 2017-2020 | 19/27 | 0,028 .. 0,101 | mind az 5 sorozat **+**, BitcoinMarkets itt a legerősebb |

`Bitcoin`, `CryptoCurrency` és `ALL_AVG`×`att_z` **mindkét, egymástól
független rezsimben** pozitív és FDR-szignifikáns — ez a legmegbízhatóbb,
replikált eredmény. `BitcoinMarkets` instabil előjelű a két rezsim között
(összhangban a t204 jegyzeteivel: ez a legzajosabb/legritkább subreddit, 6-12
comment/óra, ±29% Poisson zaj). `CryptoMarkets` gyenge/n.s. mindkét
rezsimben. `new_author_share`, `att_accel`, `chars_per_item` gyakorlatilag
mindenhol n.s.

**Erő-elemzés (javított futásból)**: main n=43 771, min |rho| 80% erőhöz:
0,0134 (nominál α=0,05) / 0,0207 (Bonferroni, család=100). Robust n=29 406:
0,0163 / 0,0252. Az `att_z`/`authors_z` megfigyelt IC-i (0,02-0,10) jóval a
család-szintű Bonferroni küszöb fölött — nem határeset, ezen a mintaméreten
tisztán kimutatható, ha szerény hatás.

**Az 1. fenntartás (inkrementális érték a volatilitás-feature-ök felett)
továbbra is érvényes és t206 feladata marad** — ezt a korrekció nem érinti,
csak azt pontosítja, hogy MELYIK feature-re vonatkozik hitelt érdemlően
(`att_z`/`authors_z`, nem `items_per_author`).

**Végső GO/NO-GO — megerősítve, pontosított bizonyítékkal**: **GO.** A
magnitúdó-ág túléli a placebót és az FDR-t a valóban trailing-normalizált
`att_z`/`authors_z` feature-ökre, két független rezsimben replikálva, tiszta
nulla irány-kontroll mellett. Az epic záró feltétele ("ha a magnitúdó-ág SEM
éli túl... a track lezárul") nem teljesül — t206 folytatódhat, de a riportnak
mindkét fenntartást (inkrementális érték + `items_per_author` confound) ki
kell mondania, és a `items_per_author`-t NEM szabad a fő bizonyítékként
idéznie.

**Kimenet**: `src/modeling/analyst_signal/gate_measurement.py` (mérés,
blokk-permutáció, FDR — `bh_fdr` újrahasznosítva `validate.py`-ból),
`analyst_signal.duckdb` két új tábla (`gate_validation` 200 sor,
`gate_power` 2 sor, migráció v9/v10 `analyst_signal_store.py`-ban),
`src/modeling/tests/analyst_signal/smoke/test_gate_measurement.py` (13 smoke
teszt). `ruff check` + `pyright` tiszta, `pytest` 20/20 zöld a teljes
`src/modeling/tests/analyst_signal/` suite-on.

[validator] Lezarva — 2026-08-21
ruff check: tiszta. pyright: 0 error, 0 warning.
pytest src/modeling/tests/analyst_signal/: 20/20 zold.
Az epic lezarult (lasd `done_t206`), tovabbi munka nem indul rola.
