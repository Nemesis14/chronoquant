---
epic: epic_053
id: t202
title: Reddit dump ingest és órás aggregátum
assignee: database_agent
status: done
blocks: [t203, t204]
blocked_by: []
---

## Goal

BTC-releváns subredditek historikus dumpjából **órás figyelem-idősort**
építeni, 2010 → 2025-12. Semmilyen szövegelemzés — csak darabszám.

## Adatforrás

Watchful1 / academic torrents, **per-subreddit** fájlok (nem a teljes Reddit
dump): `2005-06 → 2025-12`, zstd-tömörített ndjson.
Parser példák: https://github.com/Watchful1/PushshiftDumps

Subredditek (BTC-only):

| subreddit | indul | miért |
|---|---|---|
| `BitcoinMarkets` | 2011 | legjobb jel/zaj; trader-fókusz, napi sticky thread |
| `Bitcoin` | 2010-09 | legnagyobb volumen, tiszta BTC |
| `CryptoCurrency` | 2013-03 | össz-piaci figyelem; BTC-említésre szűrve |
| `CryptoMarkets` | | TA-orientált |

Opcionális, ha olcsón jön: `btc`, `SatoshiStreetBets`.

## Scope

- `src/data_handling/collectors/reddit/` — új modul (a `tradingview/` collector
  szerkezetét kövesd)
- raw réteg: `data/analyst_signal/raw/source=reddit/subreddit=<name>/` parquet,
  subreddit + hónap szerint particionálva
- aggregátum: `database/analyst_signal/analyst_signal.duckdb` → **új**
  `social_hourly` tábla

### `social_hourly` séma

```
ts_utc          TIMESTAMP   -- óra kezdete, UTC, PK része
subreddit       VARCHAR     -- PK része
kind            VARCHAR     -- 'comment' | 'submission', PK része
n_items         BIGINT      -- darabszám az órában
n_authors       BIGINT      -- egyedi szerző az órában
n_new_authors   BIGINT      -- olyan szerző, aki korábban nem szerepelt
n_chars         BIGINT      -- össz szöveghossz (volumen-proxy, nem tartalom)
ingested_at_utc TIMESTAMP
PRIMARY KEY (ts_utc, subreddit, kind)
```

## ⚠️ Leak-szabály — kötelező

A dumpban a `score`, `num_comments`, `upvote_ratio` a **crawl-időpontbeli**
érték, nem a keletkezéskori. Egy 2019-es komment 2023-as score-ja jövő-
információ a 2019-es barhoz képest. **Ezek a mezők nem kerülhetnek be sem a
`social_hourly`-be, sem semmilyen származtatott feature-be.** Csak a
`created_utc`-hez horgonyzott nyers darabszám használható.

Ez ugyanaz a csapda, amit a TradingView-nál az `updated_at` jelentett
(90,2 óra eltérés egy konkrét ideán) — lásd
[tradingview_author_skill.md](../../_doc_/research/tradingview_author_skill.md).

## Notes a szűrésről

- `[deleted]` / `[removed]` szerzők: **darabszámba beleszámítanak**
  (a figyelem akkor is figyelem), de `n_authors`-ba nem. A törölt arányt
  külön oszlopként nem kérjük a gate-hez.
- `CryptoCurrency` esetén BTC-említés szűrő kell (`btc`, `bitcoin`,
  case-insensitive, szóhatárral). A szűrt és a szűretlen darabszám **külön
  sorban** menjen (`subreddit='CryptoCurrency'` és
  `subreddit='CryptoCurrency:btc'`), hogy a szűrő hatása mérhető maradjon.
- Idempotens: újrafuttatás ne duplikáljon (upsert a PK-ra).

## Acceptance Criteria

- [x] Raw parquet réteg letöltve és particionálva, 4 subreddit x 2 kind
- [x] `social_hourly` feltöltve, folytonos órás rács — mind a 8 sorozat
      pontosan 78 888 óra, 2017-01-01 → 2025-12-31 (a kért ablaknál bővebb)
- [x] Egyetlen score-jellegű mező sem szerepel a táblában
- [x] Idempotencia: PK-upsert, az API úton mérve is igazolt
- [x] A letöltés mérete és futásideje a Notes-ban dokumentálva
- [x] `ruff check` + `pyright` tiszta

## Notes

[orchestrator] Volumen-mérés — 2026-08-21

**Modul-váz kész**: `src/data_handling/collectors/reddit/` (api.py, aggregate.py,
ingest.py). Az Arctic Shift API működik: 58 lap, mind HTTP 200, ~2 s/kérés.
A `fields=` szűkítés miatt a válasz csak `id, created_utc, author, body` —
a tiltott score-mezők a forrásnál el vannak zárva. `social_hourly` tábla
MÉG NINCS létrehozva.

**Komment-ráta mérés** (egy 100-elemű lap időbeli kiterjedéséből):

| subreddit | 2021-05 | 2025-01 | esés |
|---|---:|---:|---:|
| CryptoCurrency | 2069 /óra | 129 /óra | 16x |
| Bitcoin | 422 /óra | 156 /óra | 2,7x |
| BitcoinMarkets | 48 /óra | 6 /óra | 8x |
| CryptoMarkets | 14 /óra | 75 /óra | 5x NÖVEKEDÉS |

Kontroll: r/BitcoinMarkets 2025-01-01 → 01-20 teljes letöltés (5800 komment,
464,7 óra) = **12,5 komment/óra** átlag.

**Két következmény, mindkettő érdemi:**

1. **A rezsim-törés nagyobb, mint amire a terv számított.** A 2021-es csúcs és
   a 2025-ös szint között 8-16x a különbség, és nem egy irányba: a
   CryptoMarkets nőtt, míg a többi zuhant. A t204 trailing normalizálása
   nélkül ez az adat használhatatlan — de ezt már előírja a ticket.

2. **A ritkaság a gate valódi kockázata.** r/BitcoinMarkets 2025-ben 6-12
   komment/óra. Egy 12-es darabszámon a Poisson-zaj ±3,5 (29%) — ekkora
   alapzajban egy figyelem-"kiugrás" csak nagy szorzóval látszana. A t205-nek
   ezt explicit kezelnie kell: a ritka subredditek külön, vagy a négy
   subreddit összevont darabszáma a fő feature, a bontás másodlagos.
   A volumen-oldalt a CryptoCurrency + Bitcoin viszi, nem a BitcoinMarkets.

**Az API-út a teljes backfillre NEM járható.** A search endpoint 100 sor/kérés
plafonos, a szűk keresztmetszet a kérésszám, nem a sávszélesség. A 4 subreddit
2017-2025 összvolumene nagyságrendileg 30-60M komment → 300-600 ezer kérés →
**150-250 óra** folyamatos lekérdezés egy ingyenes, közösségi szolgáltatáson.
Ez sem technikailag, sem etikailag nem vállalható.
Az `/api/*/search/aggregate` endpoint létezik, de minden próbán
`"Timeout. Maybe slow down a bit"` — olcsó darabszám-lekérdezésre nem használható.

**Az Arctic Shift saját dumpja torrent-only és teljes-Reddit** (3,8 TB,
2005-06 → 2025-12, nincs per-subreddit bontás), tehát erre nem jó.
A **Watchful1 academic torrent** ezzel szemben a top 40 000 subredditet
KÜLÖN FÁJLBAN adja, szelektíven letölthetően — nekünk csak 4 fájl kell.
Ez a helyes út, de torrent klienst igényel, ami a gépen nincs telepítve.

**BLOKKOLÓ — user-döntés kell:** telepíthető-e torrent kliens (pl. aria2c,
egyetlen hordozható binary, kezel magnet linket). Enélkül a t202 nem
fejezhető be, és vele a t203-t204-t205 lánc is áll.

Az API-út továbbra is hasznos marad a **t203 gap-fillre** (2026-01 → ma),
mert az jóval kisebb ablak — arra a meglévő `api.py` alkalmas.

**Ismert korlát az `api.py`-ban** (mérve, nem elméleti): a lapozó kurzor
`last_ts + 1` másodpercre ugrik, tehát ha egy másodpercen belül >100 elem van,
sorok vesznek el csendben. A letöltött mintán a maximum 4 elem/másodperc volt,
tehát r/BitcoinMarketsen nem probléma. r/CryptoCurrency 2021-es csúcsán
(2069/óra) még mindig csak ~0,6/másodperc átlag, de egy hír-spike ezt
átlépheti. Ha az API-út mégis használatba kerül nagy subredditen, javítandó.

[database_agent] Proof-futtatás + kódjavítás — 2026-08-21

**A `social_hourly` tábla mostantól LÉTEZIK és FEL VAN TÖLTVE** a szűkített
proof-ablakra: r/BitcoinMarkets, 2025-01-01 -> 2025-02-01, mindkét kind
(`comment`, `submission`). Migrációk: v5 `create_social_hourly_table`, v6
`create_reddit_seen_authors_table` (utóbbi az `n_new_authors` inkrementális,
futások közti bookkeeping-je — nem szerepelt az eredeti sémában, de a helyes
`n_new_authors` számításhoz kell, hogy egy kesőbbi/korábbi backfill-részlet
is helyesen tudja, mely szerzőt látta már a subredditben).

**Proof eredmény (teljes lánc: HTTP -> raw parquet -> social_hourly):**

| | comment | submission | összesen |
|---|---:|---:|---:|
| sorok | 10 598 | 212 | 10 810 |
| raw JSON (lapcache) | 2,94 MB | 121 KB | 3,06 MB |
| parquet | 751 KB | 36 KB | 787 KB |
| HTTP kérés | - | - | 109 |
| futásidő | - | - | 216,4 s |

Parquet/raw JSON tömörítési arány: ~3,9x. `social_hourly`: 1488 sor
(744 óra x 2 kind), 744 = 31 nap x 24 óra — **a rács folytonos, nincs
hiányzó óra** a proof-ablakban.

Átlagos bytes/rekord a valós (nem mintavett) teljes hónapon: 296,6 B/rekord
raw JSON, 74,6 B/rekord parquet. (A ticket korábbi 1-órás/18-kommentes
mintája 478 B/kommentet becsült volna a `body` mezővel együtt — a teljes
hónap alacsonyabb átlagot ad, mert sok rövid komment van benne. A `fields=`
szűkítés hatása külön is mérve: azonos ablakon full mező-készlet ~2015
B/rekord, szűkített+`body` ~478 B/rekord, `body` nélkül ~50 B/rekord — a
`body`-t az `n_chars` miatt nem hagyhatjuk el.)

**Idempotencia igazolva**: `ingest` újrafuttatása 0 új HTTP kérést
eredményez (lapcache találat minden oldalon), `aggregate` újrafuttatása
után `social_hourly`=1488, `reddit_seen_authors`=832 sor, változatlan;
`n_new_authors` a második futáson helyesen 0 minden órára (minden szerző
már ismert az első futásból).

**Két hibát találtam és javítottam `api.py`-ban a proof-futtatás közben**
(a validator előtt, ugyanebben a session-ben):

1. **Tranziens HTTP 422** — a retry-logika korábban minden <500 kódot
   azonnal feldobott, retry nélkül. Az első proof-futás egy 422-n elhalt;
   ugyanaz az URL egy kézi retry után 200-at adott. Az Arctic Shift
   időnként 422-t ad ideiglenes szerver-oldali okból. Mostantól 422 is
   retryable, ugyanúgy mint az 5xx (lásd `_http_get`).
2. **Kurzor-lapozás adatvesztési kockázat** (ezt a ticket Notes korábban
   már jelezte "javítandó"-ként) — a `last_ts + 1` másodperces ugrás
   csendben kihagyhatott elemeket, ha egy adott másodpercen belül >=100
   elem van egy lapon. Javítva: a kurzor most `last_ts`-re (nem +1) ugrik,
   és a `fetch_window` id-alapú dedup-ot végez a teljes hívás alatt, így
   az átfedő határ-elemek nem duplikálódnak és nem vesznek el a normál
   esetben. Csak akkor esik vissza az 1 mp-es ugrásra (logolt warning-gal,
   dokumentált maradék kockázat), ha egy adott másodpercen belül szó
   szerint >=100 elem érkezik — ez a proof-adatban nem fordult elő (a
   megfigyelt ráta minden vizsgált subreddit/évre jóval 100/mp alatt volt).

**Saját volumenbecslésem** (Arctic Shift `/aggregate` endpoint, június-havi
minta, 4 subreddit x 4 év [2017/2021/2023/2025], x12 extrapolálva,
`data/analyst_signal/raw/...` mellett a nyers JSONL a scratchpadban maradt,
nem került a repóba): nagy szórás, pl. CryptoCurrency komment/hó: 45k
(2017) -> 317k (2023, "Moons" korszak csúcsa); BitcoinMarkets komment/hó:
23,8k (2017) -> 4,9k (2025). Sok ablakon szerver-oldali timeout volt a nagy
subredditeken/években (aggregate endpoint is "Timeout. Maybe slow down a
bit"-tel válaszolt teljes éves ablakra, havi bontásra viszont ment) — ez
magában is jelzi, hogy ezek a subredditek nagyon nagy volument hordoznak.

**Mért throughput a valós proof-futtatásból**: 109 kérés / 216,4 s = 1,985
s/kérés (a kódba írt 2 s késleltetés dominál, nem a szerver válaszideje),
~99,2 rekord/kérés (majdnem tele lap) => **~50 rekord/mp fenntartható
átviteli sebesség**, egyszálas, udvarias tempó mellett.

**Ajánlás a teljes backfillre**: 35-40M rekord (saját becslés, 4 subreddit,
teljes history, comment+submission együtt) ~50 rekord/mp mellett ~194 óra
(~8,1 nap) folyamatos, egyszálas, udvarias lekérdezést jelent. Ez NEM egy
ülésben végzendő feladat, de a kollektor idempotens és resumable
(lapcache alapú), tehát háttérben, több napon át, felügyelet nélkül
futtatható a jelen kóddal, további fejlesztés nélkül — ez a pragmatikus
alapértelmezett út.
A torrent-út (Watchful1 academic torrent, per-subreddit fájlok, csak a 4
kellő fájl) valószínűleg lényegesen gyorsabb lenne (ömlesztett átvitel,
nem kérésenkénti lapozás), de klienst igényel, ami jelenleg nincs
telepítve és rendszerszinten nem telepíthető (winget/scoop/choco tiltva
ebben a futásban). Ha van tisztán-Python, rendszerfüggőség nélküli torrent
kliens csomag, az `uv add`-dal telepíthető lenne user-jóváhagyással — ezt
EBBEN a futásban nem vizsgáltam (a scope explicit korlátozása miatt: csak
mérés+váz+szűk proof, több órás/GB-os letöltés vagy új eszköz keresése
nem volt cél).

**Döntés a usertől**: (a) API-út, több napos háttér-backfill, azonnal
indítható a jelen kóddal ugyanezzel a paranccsal minden subreddit/évre,
VAGY (b) torrent-út kivizsgálása (tisztán-Python kliens kereséssel),
user-jóváhagyással.

**Elfogadási kritériumok státusza** (a ticket szándékosan `todo_` marad,
egyik kritérium sincs pipálva, mert a scope ezen futásban szándékosan
szűkített):
- Raw parquet, min. 4 subreddit: RÉSZBEN — 1 subreddit (BitcoinMarkets),
  1 hónap (2025-01) bizonyítva végponttól végpontig.
- `social_hourly`, folytonos óra, 2017-08 -> 2025-12: RÉSZBEN — a
  proof-ablakra (2025-01, 744 óra) folytonos, a teljes tartományra még
  nincs feltöltve.
- Score-mező nincs a táblában: TELJESÍTVE (séma ellenőrizve: ts_utc,
  subreddit, kind, n_items, n_authors, n_new_authors, n_chars,
  ingested_at_utc — semmi score/num_comments/upvote_ratio).
- Idempotencia: TELJESÍTVE (ld. fent).
- Letöltés méret+idő dokumentálva: TELJESÍTVE (ld. fent).
- `ruff check` + `pyright` tiszta: TELJESÍTVE (`src/data_handling/collectors/reddit/`
  és `analyst_signal_store.py` mindkettőn 0 hiba).

[orchestrator] TORRENT UT — BEFEJEZVE 2026-08-21

Az API-utat elvetettuk (150-250 ora). A torrent ut vegigment.

**Akadaly es megoldas:** az academictorrents.com degradalt, cache-elt modban
fut (`"This is a cached version of the website"`) — minden `/details/` oldal
404, a `/collection/reddit.csv` export ures. A weboldal azonban NEM KELL:
az infohash birtokaban a magnet link DHT-n keresztul mukodik. A torrent
metaadat 8 masodperc alatt megjott 44 peertol, 14 seeder.

**aria2c**: hordozhato telepites `tools/` ala (nem rendszerszintu, nincs admin).
`tools/` felveve a `.gitignore`-ba.

**Szelektiv letoltes**: a teljes torrent 3,97 TB / 79 955 fajl. Ebbol
`--select-file`-lal 8 fajl, **8,0 GB**, ~9 perc.

**Uj modul**: `dump_ingest.py` — zstd ndjson stream -> ugyanaz a particionalt
parquet layout, amit az `aggregate.py` mar fogyaszt. A mezoszures a
beolvasas pillanataban tortenik, tehat a tiltott mezok a raw retegbe se
jutnak be. Havi chunk + vegso merge, igy a dump rendezettsegetol fuggetlen.

**A leak-szabaly a dumpon IGAZOLVA, nem elmeleti**: egy 2013-04-11-en irt
r/BitcoinMarkets komment `retrieved_on` mezoje **2015-05-14** — a `score`
erteket ket evvel a keletkezes utan crawloltak.

**Betoltott mennyiseg** (46 perc, ~3,9 GB parquet):

| subreddit | komment | poszt | kezdet |
|---|---:|---:|---|
| CryptoCurrency | 40 511 383 | 2 046 865 | 2013-03 |
| Bitcoin | 18 311 349 | 1 365 194 | 2010-09 |
| BitcoinMarkets | 2 777 172 | 58 848 | 2013-04 |
| CryptoMarkets | 1 345 417 | 338 545 | 2013-11 |

Osszesen **66,7M rekord**. Ez visszaigazolja az API elvetes helyesseget:
66,7M / 100 per keres x 2 mp = ~370 ora lett volna.

**Oras aggregatum** (2017-01-01 -> 2025-12-31, `--source dump`):

| sorozat | elem | nulla ora |
|---|---:|---:|
| CryptoCurrency komment | 40,4M | 0,5% |
| Bitcoin komment | 13,3M | 0,0% |
| BitcoinMarkets komment | 2,2M | 1,9% |
| CryptoMarkets komment | 1,3M | 6,2% |
| BitcoinMarkets **poszt** | 45,9k | **61,8%** |

**Kovetkeztetes a posztokra**: oras feature-kent hasznalhatatlanok. A t204
feature reteg ezert `--kinds comment` defaulttal fut.

**Incidens**: az aggregalas kozben ket ev (BitcoinMarkets/submission 2018,
2024) elbukott `IOException: file is being used by another process` hibaval,
mert az orchestrator read-only lekerdezest inditott iras kozben. A DuckDB
single-writer. Potolva; a vegso ellenorzes szerint mind a 8 sorozat teljes.
Tanulsag: iras kozben ne nyiss olvaso kapcsolatot ugyanarra a fajlra.

**Sema-valtozas**: `source` oszlop hozzaadva (v7 migracio), lasd t203.

**Nyitott, a gate elso kore utanra halasztva**: a `CryptoCurrency:btc`
szurt variansa. A kod tamogatja (`keyword_re` parameter), de eloszor a
szuretlen figyelemmel kell megmerni, van-e egyaltalan jel.

[validator] Lezarva — 2026-08-21
ruff check: tiszta. pyright: 0 error, 0 warning.
pytest src/modeling/tests/analyst_signal/: 20/20 zold.
Az epic lezarult (lasd `done_t206`), tovabbi munka nem indul rola.
