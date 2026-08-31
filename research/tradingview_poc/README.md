# TradingView Ideas — scraper PoC findings

> Ticket: `_jira_/epic_051_tradingview_calls/todo_t1_tradingview_scraper_poc.md`
> Futtatva: 2026-08-17 · **6 HTTP kérés összesen** · anonim, böngésző-UA
> Reprodukálás: `python probe.py fetch` majd `python probe.py parse`

---

## Verdikt

**T2 indulhat — három megkötéssel.**

Minden mező megvan, amire az `epic_051` mérése épül, és lényegesen jobb
minőségben, mint amire számítottunk: a detail oldalba ágyazott JSON gyakorlatilag
egy REST-válasz, nem HTML-kaparás. De **a listaoldal rossz timestampet mutat**,
és ez leak-kritikus. A három megkötés lent, a "T2 következményei" szakaszban.

---

## G1 — timestamp ✅ / ⚠️ (a legfontosabb)

| Alkérdés | Válasz | Bizonyíték |
|---|---|---|
| a) abszolút? | ✅ igen | `"created_at":"2026-08-08T11:13:48.277777+00:00"` |
| b) felbontás? | ✅ **mikroszekundum** | ugyanaz — a kártyán másodperc |
| c) UTC? | ✅ igen | `+00:00` a JSON-ben, `Z` a kártyán |
| d) publikálás vagy szerkesztés? | ⚠️ **mindkettő van, de a listaoldal a rosszat mutatja** | lásd lent |

**A `d` a lényeg.** A `created_at` és az `updated_at` **külön mező** a detail
JSON-ben — vagyis az eredeti publikálási idő megszerezhető. Csakhogy:

```
listaoldal kártya:   <time dateTime="2026-08-12T05:23:34.000Z">Updated </time>
detail JSON:         "created_at": "2026-08-08T11:13:48.277777+00:00"   ← ez az igazi
                     "updated_at": "2026-08-12T05:23:34.322193+00:00"   ← ez van a kártyán
```

**90.2 óra (3.8 nap) eltérés** ezen az egy ideán. A `h=1h` horizonton ez nem
pontatlanság, hanem **lookahead leak**: a szerző 3.8 nappal később nyúlt hozzá,
a mérés meg úgy venné, mintha akkor jósolt volna.

Két további megfigyelés, ami szigorítja a képet:

1. **Nem csak a `Updated` címkéjűeknél tér el.** A 6 detail-rekord
   *mindegyikénél* nemnulla a `created_at` ↔ `updated_at` rés (3.9h – 90.2h),
   köztük olyanoknál is, ahol `updates == 0`. Vagyis az `updated_at` nem csak
   valódi szerkesztéskor mozdul.
2. **Az `updates[]` tömb hozzáférhető, saját timestamppel.** Az idea utólagos
   kiegészítései külön rekordként, nem az eredeti felülírásaként jelennek meg:

   ```
   created  2026-08-08T11:13:48
     +      2026-08-09T03:28:17   "Here We GO"
     +      2026-08-12T05:23:34   "Every Thing Done Perfectly "
   ```

   Ez jó hír: az eredeti `description` + `direction` láthatóan érintetlen marad.
   **Reziduális kockázat:** az eredeti szöveg csendes átírása nem
   megkülönböztethető — ezért kell a `raw/` réteg és a `first_seen`, ahogy az
   `epic.md` előírja. Most már mérési bizonyítékkal.

## G2 — direction ✅ (de 57%-os hasznos kitöltöttség)

A detail JSON-ben **numerikus**: `"direction": 2`. A listaoldalon szöveges
címke. Kitöltöttség a 23 kártyás mintán:

| Címke | n | % |
|---|---:|---:|
| Short | 8 | 35% |
| Long | 5 | 22% |
| Education | 2 | 9% |
| *(nincs)* | 8 | 35% |

**Használható call csak a Long+Short = 13/23 ≈ 57%.** Az `Education` külön
`flags.is_education` boolean-ként is jelölve van, tehát tisztán szűrhető.

**Kódolás:** `2 = Short` **megerősítve** (a kártya címkéjével keresztellenőrizve).
`1 = Long` és `0 = nincs irány` **következtetés**, egyelőre nem igazolt —
T2-nek több mintán validálnia kell, mielőtt ráépít.

## G3 — author_id ⚠️ nincs tiszta stabil kulcs

A `user` objektum **csak `username` + `uri`** párost ad:

```json
{"username": "XAUApex", "uri": "/u/XAUApex/", "badges": [{"name": "pro:pro_premium"}]}
```

Numerikus user id **nincs** a rekordban. Opportunista kerülőút: az avatar URL
tartalmaz egyet — `userpics/128728795-r5o6.png` → `128728795` —, de ez törékeny
(avatar cserélhető, és avatar nélküli usernél nincs).

**Következmény:** a szerző-azonosító a `username`. Ha egy szerző nevet vált,
a története kettétörik, és ez a szerző-skill mérés alapegységét érinti.
Kezelés T2-ben: mentsük az avatar-id-t is másodlagos kulcsként, ahol van.

## G4 — paginálás ✅ működik, ⚠️ de nem kronologikus

```
/symbols/BTCUSD/ideas/          -> 23 <article>
/symbols/BTCUSD/ideas/page-2/   -> 23 <article>,  átfedés: 0
```

Tiszta, átfedésmentes lapozás. **Viszont a sorrend nem időrendi** — a dátumok
összevissza ugrálnak a lapon belül *és* a lapok között is:

```
p1 doc-order:  08-12, 08-15, 08-10, 08-14, 08-16, 08-11, ...
p1 tartomány:  2026-08-08 .. 2026-08-16
p2 tartomány:  2026-08-07 .. 2026-08-16   ← átfedő időablak, más ideák
```

Nem találtam kitett `sort=recent` linket a HTML-ben (csak egy
"Editors' picks" szűrő). Ennek két következménye van T2-re:

- **A backfill nem tud dátum alapján leállni** — végig kell paginálni,
  és nem lehet előre megmondani, hány oldal.
- **Az inkrementális frissítés nem működik naivan.** Az "1. oldalt húzzuk le,
  amíg ismert ideákat nem látunk" stratégia elbukik, mert az új ideák nem
  garantáltan az 1. oldalon jelennek meg.

## G5 — entry / stop / target ❌ nincs strukturáltan

A detail rekord `content` mezője a chart rajz-JSON-t tartalmazza
(**172 KB egyetlen ideánál**), de nem találtam benne `entryPrice` /
`stopLevel` / `profitLevel` kulcsokat — a rajzelemek generikus `line` / `text`
objektumok. Az árszintek a szövegben és a chart képen élnek.

Nem blokkoló: az epic hipotéziséhez `direction + ts` elég.

---

## Bónusz: amit nem kérdeztünk, de megvan

**`interval` = a szerző által megadott timeframe** — a `plan.md` `call`
absztrakciójának `horizon_hint` mezője. `"interval":"240"` (4h), `"60"` (1h).
**Csak a detail JSON-ben van, a listakártyán nincs.**

**Teljes `description`** — a `paragraph` kártyán is ott a szöveg (433–1859
karakter a mintában), a detail JSON-ben a teljes. Ez az, amiért az `s102`
második indoka (kalibrációs set az `epic_052` LLM-extraktorához) **teljesül**.

**Confidence proxyk:** `likes_count` (182), `comments_count` (60),
`views` (5293) — 100%-os kitöltöttség.

**Exchange-kvalifikált szimbólum:** `symbol.pro_symbol` = `BITSTAMP:BTCUSD`.
A `/symbols/BTCUSD/ideas/` oldal **több tőzsdét aggregál**: a 23 kártyán
`CRYPTO:BTCUSD` (11), `BITSTAMP` (7), `COINBASE` (3), `BINANCE` (1),
`OANDA` (1). BTC-nél az árkülönbség elhanyagolható, de a `calls` táblában
a `pro_symbol`-t érdemes megőrizni.

**A detail oldal többet ad, mint egy ideát:** a `related[]` tömbben további
teljes idea-rekordok jönnek (a mintában 6 rekord 1 kérésből).

---

## Amit a sitemap NEM ad

`sitemap-ideas.xml` = **5401 idea összesen, minden szimbólumra, minden időre**
— ez egy kurált „best of", nem a teljes korpusz. Nincs benne `<lastmod>`, és a
`<video:>` blokkok sem tartalmaznak `publication_date`-et.

**Backfill-forrásnak alkalmatlan.** Szimbólum-eloszlása viszont figyelmeztető:

```
BTCUSD 661 · BTCUSDT 258 · BTCUSDT.P 42   →  BTC ~961
ETHUSD  70 · ETHUSDT  38                  →  ETH ~108
SOLUSD   9 · SOLUSDT  15                  →  SOL  ~24
```

A SOL a BTC ~2.5%-a ebben a kurált halmazban. Torzított minta (régi, népszerű
ideák felé húz, a SOL meg újabb asset), de **nyitott kérdés, hogy a valós
korpuszban van-e egyáltalán elég SOL-idea szerző-szintű méréshez.**

---

## T2 következményei — a három megkötés

**1. A listaoldal timestampje nem használható. Detail fetch kötelező.**
Ez a legdrágább következmény. Nyitott kérdés, ami **közvetlenül a T2
költségét határozza meg**: a `Updated` címke nélküli kártyáknál a kártya
dátuma egyenlő-e a `created_at`-tel? Ha igen, csak a ~26%-ra kell detail
fetch. Ha nem, mindre. **~5 további detail kéréssel eldönthető** — érdemes
T2 tervezése előtt megcsinálni.

Nagyságrend, ha mindre kell: a detail oldal 418 KB (ebből 172 KB a `content`
rajz-JSON). A `content`-et **nem szabad eltárolni**, különben a raw réteg
gigabájtokban mérhető.

**2. A backfill nem tud dátum alapján leállni,** és az inkrementális
frissítés sem működik naiv „1. oldal" stratégiával — lásd G4. Kell egy
másik mechanizmus (pl. author-oldalak `/u/{username}/` bejárása, ami az
`s102` T2 leírásában amúgy is szerepel, és `robots.txt` szerint engedett).

**3. A `direction` kódolást validálni kell** több mintán, mielőtt T2 ráépít.
Csak a `2 = Short` megerősített.

---

# T1b — backfill-tervezési próba (2026-08-17, +11 kérés)

> `python probe2.py a` · a mérés megdöntötte a „paginálj vissza 2025-01-01-ig" tervet.

## ❌ A szimbólum-feed nem archívum — kb. 10 napot ad

Dátumtartomány laponként:

| lap | tartomány | átfedés az előzővel |
|---|---|---:|
| p1 | 2026-08-08 .. 2026-08-16 | — |
| p2 | 2026-08-07 .. 2026-08-16 | 0 |
| p10 | 2026-08-06 .. 2026-08-17 | 0 |
| p50 | 2026-08-10 .. 2026-08-17 | 0 |

**A 10. és az 50. oldal nem régebbi, mint az első.** Minden lap ugyanazt a
~10 napos ablakot fedi, csak más ideákkal. A feed tehát „friss aktivitás",
nem idősoros archívum.

Ráadásul **a paginálás elfogy**: `page-50`, `page-200` és `page-500`
bájtra azonos (md5 `24b6f668`), `page-10` még különbözik → a valódi utolsó
lap 11 és 50 között van. Felső korlát nagyságrendileg **~1150 idea**, mind
az elmúlt ~10 napból.

**Következmény: szimbólum-feedből historikus backfill nem lehetséges.**

## ❌ A szerzői oldal JS-renderelt — nincs benne adat

`/u/XAUApex/` (165 KB HTML):

```
<article>      0
dateTime=      0
idea link      0
"created_at"   0
"uuid"         0
```

Üres váz. A `window.initData` csak generikus kulcsokat tartalmaz
(`theme`, `settings`, `snowplowSettings`), idea-adatot nem.
A T2 „author-oldalak a mélyebb historyért" terve **így nem működik**.

## Részleges nyom: van `/api/v1/` névtér

A `user_profile.e9a08c5c828d5d5ce8e1.js` bundle-ben:

```
/api/v1/script_packages/author/${e}/
/api/v1/script_packages/uid/${e}/scripts/
/api/v1/creator_program/eligibility/
```

Tehát **létezik REST API**, de ez a chunk csak a *scripts* (Pine) részt fedi.
Az ideas-feed endpointja egy lazy-loaded chunkban lesz — a szerzői oldal
29 bundle-t tölt, ezek átfésülése még hátravan.

## ✅ Amit T1b megerősített

**A `related[]` a szerző saját ideáit adja**, teljes `created_at` +
`interval` + `direction` mezőkkel — 1 kérés = 6 rekord. De az ablak szűk
(a mintában 2 nap), tehát ez sem history-forrás.

5 további detail fetch lefutott a nem-`Updated` kártyákra
(`detail_{uuid}.html` az `out/`-ban) — a kártya-dátum ↔ `created_at`
összevetés még kiértékelendő, de a fentiek fényében **másodlagos**.

---

# T1c — ✅ MEGVAN AZ API (2026-08-17)

> `python probe3.py fetch && python probe3.py scan`
> A T1b összes negatív leletét tárgytalanná teszi. **A HTML-t elfelejthetjük.**

## Az endpoint

```
GET https://www.tradingview.com/api/v1/ideas/
      ?symbol=BTCUSD        exchange-kvalifikált is mehet (BITSTAMP:BTCUSD)
      &by={username}        szerző-szűrő  ← ez a kulcs
      &page=1 &per_page=100 max 100/lap
      &q= &locale= &sort= &is_video=

→ { count, page_size, page_count, next, previous, results[] }
```

Megtalálva a `67974.1c3cd73bb39e60f90c17.js` bundle-ben
(`publicationsApi.getIdeasList`). A `robots.txt` `*` blokkja az `/api/`-t
nem tiltja. Anonim hívás, `HTTP 200`, `application/json`.

## Amit egyetlen hívás ad — **detail fetch nem kell**

| Mező | Érték | Megoldja |
|---|---|---|
| `created_at` | `"2026-08-17T05:35:33+00:00"` | **G1** — publikálás, UTC |
| `updated_at` | nullable, külön mező | **G1(d)** — a leak-probléma megszűnt |
| `date_timestamp` | unix int | ugyanaz, numerikusan |
| `symbol.direction` | `1`=long, `2`=short, `0`=nincs | **G2** |
| `symbol.interval` | timeframe | `horizon_hint` |
| `symbol.full_name` | `BITSTAMP:BTCUSD` | exchange-kvalifikált |
| `user.id` | `115947115` | **G3** — stabil numerikus szerző-id |
| `user.username` | `fx_martin9` | |
| `description` | teljes szöveg | `epic_052` kalibrációs set |
| `is_education` / `is_video` / `is_script` | bool | szűrés |
| `likes_count` / `comments_count` / `views_count` | int | confidence proxy |

**A `direction` kódolás forrásból igazolva** (nem következtetés):

```js
function d(e){ if(!e.is_script)
  return e.is_education ? "education"
       : 1 === e.symbol.direction ? "long"
       : 2 === e.symbol.direction ? "short" : void 0 }
```

**Használható call arány (long+short) a valós mintán: 78–86%** — jóval jobb,
mint a HTML-kártyákon mért 57% (a kártya egyszerűen nem rendereli mindnek).

## Limitek

**1000 találat / lekérdezés — kemény plafon.** `symbol=BTCUSD` esetén
`count: 1000`, ami **~2 hónapot** fed (2026-06-17 .. 2026-08-17).
`per_page=100` → `page_count=10`.

**De a `by={username}` külön 1000-es kerete van**, és a legtöbb szerzőnél
ez a *teljes* history: `by=XAUApex` → `count: 38`, egyetlen lapon, minden
szimbólumra. **Ez kerüli meg a szimbólum-plafont.**

## A backfill terve — két fázis

```
1. FELDERÍTÉS   symbol=BTCUSD & BTCUSDT, per_page=100, page=1..10
                → ~2000 idea az elmúlt ~2 hónapból
                → ebből a distinct user.id halmaz            ~20 kérés

2. KIEGÉSZÍTÉS  minden felderített szerzőre by={username}
                → teljes history, minden szimbólum, szűrés BTC-re
                → a legtöbb szerző 1 kéréssel elintézve   ~800-1600 kérés
```

**Nagyságrend: ~1000–1700 kérés, 6s-mal ≈ 2–3 óra. Egyetlen éjszaka.**
Raw méret ~500-700 MB (`per_page=100` válasz ≈ 400 KB).

## ✅ A szerzői history mélysége — igazolva

`by={username}` valóban a **teljes** historyt adja, nem csak a friss ablakot:

| szerző | `count` | lekérdezett tartomány | BTC arány |
|---|---:|---|---|
| **PentarhUdi** | 42 | **2015-03-11 .. 2026-07-26** (11 év, 1 kérés) | 38/42 |
| Akil_Stokes | 1000 *(plafon)* | 2025-01-12 .. 2026-08-17 (~1,5 év) | 2/100 |
| XAUApex | 38 | 2026-08-02 .. 2026-08-16 (új account) | 10/38 |

Az 1000-es plafon csak a **nagyon** sokat posztolóknál harap, és ott is
~1,5 év historyt hagy. A legtöbb szerző egyetlen kéréssel teljesen lefedhető.

## Populáció-definíció: csak aktív szerzők (user, 2026-08-17)

A felderítés az elmúlt ~2 hónap feedjéből indul, tehát csak **aktív**
szerzőket talál. Ez **szándékos, nem hiba**: production-ben csak az aktív
szerző ad jövőbeli jelet, aki abbahagyta, az irreleváns. A populáció
helyes definíciója tehát „2026-08-ban aktív BTC-szerzők".

**Ami ebből mégis marad — a visszamérésre, nem a deploymentre:**
egy 2025-06-i időpontban élesben futó rendszer az *akkor* aktív szerzőkből
rangsorolt volna, köztük olyanokból, akik később abbahagyták. A mi
backtestünk populációja viszont „2026-08-ban még aktív" — vagyis 2025-06-ban
tudunk valamit, amit élesben nem tudhattunk volna.

Hatás: a mért skill **szintje** felfelé torzul. A `s102` tényleges kérdése
viszont a *perzisztencia* (a múltbeli skill előrejelzi-e a jövőbelit), ami
rangkorrelációs kérdés, és arra ez kevésbé érzékeny.

**Kezelés:** nem blokkoló, de a T7 riportban ki kell mondani, hogy az
eredmény „feltéve, hogy a szerző 2026-08-ban még aktív" feltétel mellett
értendő. A T6 placebo teszt ezt nem szűri ki.

---

## Nyitott kérdések T2-höz

- [ ] Kártya-dátum == `created_at` a nem-`Updated` ideáknál? (~5 kérés)
- [ ] `direction` 0/1 jelentése megerősítve?
- [ ] Van-e kronologikus rendezés (query param, vagy author-oldal)?
- [ ] Meddig megy vissza a paginálás BTCUSD-n? Hány oldal?
- [ ] Elég sűrű-e a SOL a valós korpuszban a mérésehez?
- [ ] Van-e mögötte tiszta JSON endpoint, vagy marad a HTML-be ágyazott parse?

---

## Fájlok

| Fájl | Tartalom |
|---|---|
| `probe.py` | eldobható próbaszkript — `fetch` / `parse` |
| `samples/idea_detail_e2LXn8NQ.json` | a `created_at` ↔ `updated_at` rést mutató rekord |
| `samples/idea_detail_direction_codes.json` | `direction` 0 és 1 példány |
| `samples/listing_cards_btcusd.json` | 8 listakártya kinyert mezőkkel |
| `out/` | nyers letöltések — **nincs verziókövetve** |

A `content` és `description_ast` mezők a mintákból ki vannak véve (méret),
a `description` 300 karakterre vágva.
