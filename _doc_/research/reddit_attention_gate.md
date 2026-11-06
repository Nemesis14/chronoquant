# Kutatás: Reddit aggregált figyelem — prediktív-e a BTC 60 perces kilengésére?

**Cél:** Eldönteni, hogy az aggregált Reddit-aktivitás (szerző nélkül, LLM nélkül)
hordoz-e használható információt a BTC 60 perces kilengés-nagyságáról.

**Időszak:** 2017-01 – 2025-12 (Reddit), 2017-08 – 2026-08 (árfolyam)
**Epic:** `epic_053_reddit_attention_gate` · **Lefutva:** 2026-08-21 (egy nap)

> **Döntés: NEM ERŐLTETJÜK.** A jel statisztikailag valós, de gyenge (IC 0,02–0,10),
> nem stabil subredditek között, és nincs bizonyítva, hogy bármit hozzáad a
> modell meglévő volatilitás-feature-jeihez. A várható hozadék nem indokolja a
> további munkát.

---

## Miért indult el ez egyáltalán

Az `epic_051` (TradingView) elvetette a **szerző-skill** hipotézist: FDR-korrekció
után nulla azonosítható szerző, mind az öt horizonton. A gate szerint az
`epic_052` (Reddit call-extrakció) törölve lett — helyesen, mert ott mi gyártanánk
a címkét, extraktor-zajjal.

Ez az epic **nem** azt a hipotézist nyitotta újra. Más kérdést tett fel:

> A Reddit valódi értéke nem az egyedi vélemény, hanem az **aggregált figyelem
> idősora** — ahol nincs szerző, nincs extraktor, nincs LLM, tehát nincs mit
> elrontani egy modell-döntéssel.

És egy szándékosan szerényebb célt:

> A Reddit-forgalom többnyire **reagál** az árra, nem előzi. Ezért nem az irányt
> jósoljuk, hanem a **magnitúdót**. Ez illeszkedik a targethez (`long_mfe_fw60`
> folytonos kilengés-nagyság, nem előjel).

---

## Módszer

### 1. Adatbeszerzés — az API-út nem volt járható

Mérés, nem becslés: az Arctic Shift search endpoint 100 sor/kérés plafonos, a
szűk keresztmetszet a kérésszám. A négy subreddit teljes története **66,7M
rekord** → ~370 óra folyamatos lekérdezés egy ingyenes, közösségi szolgáltatáson.
Sem technikailag, sem etikailag nem vállalható. Az `/api/*/search/aggregate`
endpoint, ami olcsón adna darabszámot, minden próbán `"Timeout. Maybe slow down
a bit"`-tel válaszolt.

**A megoldás a Watchful1 per-subreddit dump volt**, egy akadállyal: az
academictorrents.com degradált, cache-elt módban fut (`"This is a cached version
of the website"`) — minden `/details/` oldal 404, a CSV export üres.

> **A weboldal nem kell.** Az infohash birtokában a magnet link DHT-n keresztül
> működik. A torrent metaadat 8 másodperc alatt megjött 44 peertől.

A teljes torrent 3,97 TB / 79 955 fájl; ebből `--select-file`-lal **8 fájl,
8,0 GB**, ~9 perc. Betöltés parquetbe: 46 perc.

| subreddit | komment | poszt | kezdet |
|---|---:|---:|---|
| CryptoCurrency | 40 511 383 | 2 046 865 | 2013-03 |
| Bitcoin | 18 311 349 | 1 365 194 | 2010-09 |
| BitcoinMarkets | 2 777 172 | 58 848 | 2013-04 |
| CryptoMarkets | 1 345 417 | 338 545 | 2013-11 |

### 2. A leak-szabály — nem elméleti

A dumpok tartalmazzák a `score`, `ups`, `downs`, `num_comments` mezőket. Ezek a
**crawl-időpontbeli** értéket tükrözik, nem a keletkezéskorit. Egy mintavett
r/BitcoinMarkets komment:

```
created_utc  = 2013-04-11   <- a komment keletkezése
retrieved_on = 2015-05-14   <- amikor a score-t lementették
```

**Két év.** Ennek a score-nak a használata egy 2013-as bar feature-eként közvetlen
lookahead. Ugyanaz a csapda, mint a TradingView-nál az `updated_at` (90,2 óra).

A `dump_ingest.py` a mezőszűrést a beolvasás pillanatában végzi: a tiltott mezők
a raw rétegbe se jutnak be. A `social_hourly` tábla **kizárólag `created_utc`-hez
horgonyzott darabszámokat** tárol.

### 3. Órás aggregátum

Mind a 8 sorozat pontosan **78 888 óra**, 2017-01-01 → 2025-12-31, folytonos rács
(hiányzó óra = explicit 0, nem hiányzó sor).

| sorozat | elem | nulla óra |
|---|---:|---:|
| CryptoCurrency komment | 40,4M | 0,5% |
| Bitcoin komment | 13,3M | 0,0% |
| BitcoinMarkets komment | 2,2M | 1,9% |
| CryptoMarkets komment | 1,3M | 6,2% |
| BitcoinMarkets **poszt** | 45,9k | **61,8%** |

**A posztok órás bontásban használhatatlanok** (0,3/óra). A mérés kommentekre futott.

### 4. Feature réteg és a rezsim-probléma

A nyers darabszám önmagában használhatatlan: a subreddit-populáció 2013→2021
nagyságrendekkel nőtt, a 2023-06 API-lezárás letörte, majd részben visszaépült.
Ez **nem monoton hanyatlás**, hanem törés (BitcoinMarkets kommentek: 525 047
(2018) → 56 515 (2023) → 150 271 (2024)).

Óra-a-héten deszezonalizálás + 30 napos trailing z-score, 14 nap warmup, egyetlen
`shift(1)` fojtóponton át a számítás legvégén. Eredmény:

| év | nyers átlag/óra | `att_z` átlag | `att_z` szórás |
|---|---:|---:|---:|
| 2019 | 112 | −0,042 | 1,054 |
| **2021** | **1229** | +0,086 | 1,168 |
| 2023 | 353 | −0,037 | 1,107 |
| 2025 | 129 | −0,049 | 1,044 |

A nyers jel 11x-et ingadozik, a normalizált stabil. A rezsimtörés eltűnt.

### 5. A mérés felépítése

**Óránként egyetlen megfigyelés — a `:00` perces bar.** Ennek nem nyilvánvaló, de
fontos következménye: a `h:00` bar fw60 ablaka `h:01..h+1:00`, a következőé
`h+1:01..h+2:00` — **az ablakok nem fedik át egymást.** Ezzel az átfedés-inflálta
standard hiba problémája megszűnik, HAC/Newey-West nélkül. Panel: **73 269
megfigyelés**.

25 sorozat × 4 cél × 2 rezsim = 100 teszt rezsimenként. Spearman IC + kvantilis-
binned MI, blokk-permutációs placebo (5000 ismétlés), Benjamini-Hochberg FDR.

---

## Két hiba, amit menet közben találtunk

Ezek önmagukban is átvihető tanulságok.

### A blokkhossz fázis-lezárása

Az első futás `block_len = 168h`-cel ment — ami pontosan 7 nap. Ezért **minden
permutált blokk ugyanabba a hét-órájába záródott**, és a null-eloszlás elhúzódott
a nullától:

```
Bitcoin/att_z/long_mfe_fw60:
  null_mean = -0,0262  @168h     <- torzított
  null_mean = -0,0003  @169h     <- javítva
  valós IC  =  0,03009 @168h  vs  0,03003 @169h   <- az adatot nem érinti
```

A blokkhossz csak a null-t érinti. De a torzított null miatt a főrezsimben
**41 FDR-túlélő volt 26 helyett** — 15 álpozitív. Tanulság: a blokkhossz soha ne
legyen a szezonális periódus egész többszöröse.

### Feature-ök, amik nem azok voltak, aminek látszottak

Az `items_per_author` és a `chars_per_item` **nem trailing-normalizált** — nyers
arányszámok, csak t-1 laggel. A dokumentáció szerint minden feature normalizált volt.

Ez azért kritikus, mert ezek adták a legnagyobb IC-t (0,18), és ez **többéves
szekuláris trend-konfound**:

- `items_per_author` évente monoton csökken (Bitcoin: 1,73 (2021) → 1,38 (2025))
- a `long_mfe_fw60` cél is csökken ugyanebben az ablakban (0,00565 → 0,00276)
- **éven belüli** IC: 2021=0,09, **2022=−0,027**, 2023=0,025, 2024=0,024, 2025=0,047
  — előjelet vált, jóval gyengébb a pooled 0,136-nál

Kontrollként az `att_z` évenkénti átlaga ~0 minden évben, éven belüli IC-je
stabilabb. **A hiteles bizonyíték tehát `att_z` és `authors_z`**, nem a másik kettő.

---

## Eredmények

### Az irány-kontroll tiszta nulla — ez a legfontosabb szám

| rezsim | irány (`fw60_close_logret`) FDR<0,05 | max abs IC | min nyers p |
|---|---:|---:|---:|
| main 2021–2025 | **0 / 25** | 0,0089 | 0,060 |
| robust 2017–2020 | **0 / 25** | 0,0106 | 0,070 |

Előre rögzítettük, hogy az iránytól nullát várunk. Nulla lett, mindkét korszakban,
a blokkhossz-javítás után is. **Ha a pipeline bármiből jelet gyártana, ez itt
felvillanna.** Ez teszi hitelessé a magnitúdó-oldalt.

### A magnitúdó-oldal — valós, de gyenge

Csak a hiteles feature-ökkel (`att_z`, `authors_z`), 27 teszt rezsimenként:

| rezsim | FDR<0,05 túlélő | IC tartomány |
|---|---:|---|
| main 2021–2025 | 14 / 27 | 0,022 – 0,059 |
| robust 2017–2020 | 19 / 27 | 0,028 – 0,101 |

Erő-elemzés: min kimutatható abszolút rho 80% erőnél = 0,0134 (nominális) / 0,0207
(Bonferroni). A megfigyelt IC ennek **3–4x-e** — kimutatható, de nem nagy.

### A döntő gyengeség: sorozatok közti instabilitás

| sorozat | main 2021–2025 | robust 2017–2020 |
|---|---:|---:|
| Bitcoin | 5 túlélő | 4 túlélő |
| CryptoCurrency | 3 | 6 |
| BitcoinMarkets | **0** | **6** |
| CryptoMarkets | **6** | **0** |
| ALL_AVG | 0 | 3 |

**Amelyik subreddit az egyik korszakban működik, a másikban nem.** A BitcoinMarkets
2017–2020-ban a legerősebb, 2021–2025-ben nulla; a CryptoMarkets pont fordítva.
Csak a `Bitcoin` és a `CryptoCurrency` él túl mindkettőben, és ott is gyengén.

Ez a mintázat nem az, amit egy stabil, deployolható feature-től várnánk. És a jel
**a korábbi korszakban erősebb** — deployment szempontból pont a rossz irány.

---

## Döntés

**`epic_053` → NEM ERŐLTETJÜK.**

Nem azért, mert nincs jel — van. Hanem mert:

1. **Gyenge.** IC 0,02–0,10, a kimutathatósági küszöb 3–4x-e.
2. **Nem stabil.** A működő subreddit korszakonként változik.
3. **Az inkrementális érték nincs bizonyítva.** A Reddit-beszélgetés felpörög
   volatilis időszakokban, a volatilitás autokorrelált, és a modellben **már
   vannak volatilitás-feature-ök**. Egy 0,06-os IC-t ezek könnyen elnyelnek.
4. A 3. pont megméréséhez (`t207`: parciális korreláció / inkrementális IC a
   trailing realized volatility felett) további munka kellene — és a várható
   hozadék ezt nem indokolja.

**Ha valaki később mégis folytatná, a 3. pont az egyetlen érdemi nyitott kérdés,
és azzal kell kezdeni.** Ha ott nulla marad, a téma véglegesen lezárható; ha nem,
akkor érdemes tovább vinni.

---

## Limitációk — mit NEM mond ki ez a kutatás

**Nem azt, hogy a Reddit-figyelem haszontalan.** Azt, hogy az általunk mért
formájában gyenge és instabil, és nem bizonyítottan több, mint a volatilitás.

Amit nem próbáltunk meg, és elvileg erősebb lehetne:

- **Szentiment.** A gate szándékosan NLP-mentes volt (a költség ott van). Lehet,
  hogy a hangulat iránya többet mond, mint a puszta volumen.
- **A `CryptoCurrency:btc` szűrt variáns.** A kód támogatja (`keyword_re`), de nem
  futott — először a szűretlen figyelmet akartuk megmérni.
- **Hosszabb horizont.** Az fw60 (60 perc) egy órás feature-höz nagyon rövid. A
  szakirodalom, ami hatást talál, jellemzően **napi** horizonton méri.
- **Napi aggregátum a posztokból.** Órás bontásban használhatatlanok, napiban nem
  feltétlenül.

**Az órás feature / 60 perces target eltérés végig fennállt.** A feature konstans
a predikciós ablakon belül, tehát ez rezsim/kontextus-jelzés, nem per-bar alfa.

---

## Ami megmarad ebből a munkából

- **66,7M Reddit rekord** parquetben (2010→2025-12), 4 subreddit, leak-mentesen
  szűrve — bármilyen későbbi szöveges/szentiment munkához kész alap.
- **`social_hourly`** — 8 sorozat × 78 888 óra, folytonos rács.
- **`social_features`** — 315 552 sor, trailing-normalizált, t-1 laggel.
- **`btcusdt` teljes fw60 target** (4 729 212 sor, 2017-08→2026-08) — BTC
  kvantitatív munkához önmagában is használható.
- **Mutációval igazolt leak-teszt-minta.** A `test_leak_safety.py` shift/lag/warmup
  hármasa bármelyik jövőbeli feature-rétegre ráhúzható. Igazolás: a szezonális
  baseline-t szándékosan teljes-mintás átlagra cserélve a tesztek azonnal
  elbuktak, visszaállítás után újra zöldek.
- **A nem-átfedő órás mintavétel trükkje** (`:00` bar → független fw60 ablakok) —
  bármely jövőbeli fw60-alapú korrelációs mérésnél újrahasznosítható.

---

## Reprodukálás

```bash
# 1. torrent metaadat DHT-n (a weboldal nem kell)
tools/aria2-1.37.0-win-64bit-build1/aria2c.exe --enable-dht=true \
  --bt-metadata-only=true --bt-save-metadata=true -d tools/meta \
  "magnet:?xt=urn:btih:3e3f64dee22dc304cdd2546254ca1f8e8ae542b4"

# 2. szelektív letöltés (8 fájl a 79 955-ből)
aria2c --select-file=6140,6141,6150,6151,10935,10936,10951,10952 \
  -d data/analyst_signal/raw/source=reddit/_dumps tools/meta/<hash>.torrent

# 3. dump -> parquet
python -m data_handling.collectors.reddit.dump_ingest \
  --dump <...>/Bitcoin_comments.zst --subreddit Bitcoin --kind comment

# 4. parquet -> social_hourly
python -m data_handling.collectors.reddit.aggregate \
  --subreddit Bitcoin --kind comment --source dump --start 2017-01-01 --end 2018-01-01

# 5. social_hourly -> social_features
python -m modeling.analyst_signal.social_features --source dump --kinds comment

# 6. gate mérés
python -m modeling.analyst_signal.gate_measurement
```

Eredmények: `database/analyst_signal/analyst_signal.duckdb`
(`social_hourly`, `social_features`, `gate_validation`, `gate_power`).
A 168 órás (érvénytelen) futás pillanatképe összehasonlításra:
`data/analyst_signal/gate_snapshots/`.

Előzmény: [tradingview_author_skill.md](tradingview_author_skill.md) — a
szerző-skill hipotézis, amit ez az epic **nem** nyitott újra.

---

## Kód a git historyban

A kísérleti kód a repóból törlődik (epic_068, t894/t895). Törlés előtti állapot: commit `73f2156`, útvonalak: `src/data_handling/collectors/reddit/`, `src/modeling/analyst_signal/{social_features,gate_measurement,validate}.py`, `store/analyst_signal_store.py`.
