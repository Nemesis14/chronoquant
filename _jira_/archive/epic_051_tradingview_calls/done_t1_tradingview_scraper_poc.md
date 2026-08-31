---
epic: epic_051
id: t1
title: TradingView Ideas scraper PoC — hozzáférés és mezőséma felmérés
assignee: —          # főszálon fut, agent nélkül (lásd Notes)
status: done
blocks: [t2]
blocked_by: []
---

## Goal

Eldönteni **öt kérdésben**, hogy az `epic_051` mérése egyáltalán megvalósítható-e,
mielőtt a T2 backfill runner megíródna.

Ez egy **eldobható próbadarab**. Nem épül store, nem épül séma, nem kerül semmi
`src/` alá. A deliverable nem kód, hanem egy **írott séma-riport**, ami T2
bemeneti specje lesz.

Terjedelem: **BTCUSD + BTCUSDT, összesen ~5-10 HTTP kérés**, kézzel indítva,
loop nélkül. (A kettő ugyanaz az asset, de a TV külön oldalon tartja őket —
mindkettő kell, mert az idea-mennyiség eltérhet.)

## Scope

```
research/tradingview_poc/
  README.md          ← findings riport, G1–G5 megválaszolva + verdikt
  probe.py           ← eldobható szkript
  samples/           ← 2-3 lekicsinyített minta JSON (séma-bizonyíték)
```

A nagy nyers HTML/JSON dumpok a scratchpadbe mennek, **nem** a repóba.
A `research/` könyvtár még nem létezik — ez a task hozza létre.

## Gate-kérdések

| # | Kérdés | Ha nem |
|---|---|---|
| **G1** | Van-e abszolút, gépi timestamp? | 🛑 epic megáll |
| G2 | Strukturált-e a direction (Long/Short/Education)? | 🛑 epic megáll |
| G3 | Stabil-e az `author_id`, vagy csak username? | ⚠️ mérés torzul névváltásnál |
| G4 | Meddig megy vissza a paginálás? | ⚠️ < 1 év esetén kevés a historia |
| G5 | Van-e entry / stop / target strukturáltan? | ℹ️ nice-to-have, nem blokkol |

### G1 négy alkérdése — ez a legfontosabb

A `plan.md` 3. szakaszában a `ts_utc` mellett ott áll: **„leak-kritikus"**.
Négy külön dolgot kell ellenőrizni:

**a) Abszolút vagy relatív?** A látható szöveg valószínűleg `„3 months ago"` —
ez használhatatlan. A HTML `<time datetime="...">` attribútumában vagy egy JSON
mezőben lehet a pontos érték. **Azt kell megkeresni, nem a látható szöveget.**

**b) Milyen a felbontása?** Ez dönti el, mely horizontok élik túl a T4-et:

| Felbontás | h=1h | h=4h | h=1d/3d/7d |
|---|---|---|---|
| perc | ✅ | ✅ | ✅ |
| nap | ❌ | ❌ | ✅ (zajosan) |
| hónap | ❌ | ❌ | ❌ |

Nap-pontosságnál az epic nem hal meg, de **T4 tervét át kell írni**.

**c) UTC vagy lokális?** ±12 óra hiba egy 1h-s horizonton értelmetlenné teszi
a mérést. A `Z` suffix / offset megléte nem részletkérdés.

**d) Publikálás vagy utolsó szerkesztés?** ← a legalattomosabb.
Ha a TV a *last updated* időt adja, és a szerzők utólag szerkesztik az ideákat,
akkor **közvetlen lookahead leak** van a mérésben. Ellenőrizni kell, hogy
van-e külön `created` és `updated` mező — és ha csak egy van, melyik.
Ez az, amiért az `epic.md` a `first_seen` mentését előírja.

## Terv

**Lépés 0a — ingyen előszűrő (nulla kockázat)**
`robots.txt` letöltése + Google `site:` keresés. Ha az idea oldalak indexeltek,
az erős jel, hogy anonim `GET` kiszolgálást kapnak.

**Lépés 0b — hozzáférés (első gate)**
`https://www.tradingview.com/symbols/BTCUSDT/ideas/` — benne vannak-e az ideák
a HTML-ben, vagy JS injektálja őket? Átenged-e a Cloudflare egy sima kérést?

Három lehetséges kimenet:
- ideák a HTML-ben → könnyű dolgunk van
- beágyazott JSON blob a HTML-ben → még jobb, strukturált
- üres váz + XHR → belső endpointot kell keresni DevTools Network fülön

**Fallback, ha mindhárom elhasal:** böngészőből „mentés másként", a lementett
HTML offline parse-olása. Nulla ban-kockázat, és G1–G5-re ugyanúgy válaszol.

**Lépés 1 — mezőséma**
Kiolvasható mezők listája, mindegyikre **kitöltöttségi aránnyal** a mintában.
Nem elég hogy „van ilyen mező" — az is kell, hány százalékban kitöltött.

**Lépés 2 — detail oldal vs listaoldal**
2-3 ideát külön is lehúzni és összevetni a kártyával: megéri-e a plusz kérés?
T2-ben ez szorzódik az idea-számmal.

**Lépés 3 — paginálás**
A 2. oldal URL-formája, meddig lehet visszamenni, van-e hard limit.

## Acceptance Criteria

- [x] `research/tradingview_poc/README.md` létezik, G1–G5 megválaszolva
- [x] G1 mind a négy alkérdésére (a-d) konkrét válasz, idézett mezőnévvel
- [x] Mezőséma táblázat kitöltöttségi aránnyal
- [x] 3 minta JSON a `samples/` alatt, séma-bizonyítékként
- [x] Paginálás viselkedése dokumentálva
- [x] A riport **explicit verdiktet** mond ki:
      T2 indulhat / nem indulhat / indulhat, de ezekkel a megkötésekkel

## Notes

**Miért nincs agent:** a PoC-nak nincs olyan domain-tudása, amit egy agent
manifest hozzáadna — nem `src/data_handling/`-be írunk, tehát a `database_agent`
kontextusa itt felesleges teher. Ilyen jellegű munka még nem volt a projektben.
A főszál végzi. Az agentek T2-től jönnek, amikor már `src/` alá kerül kód.

**Miért nem lépünk be a TradingView accounttal:**

- *Aszimmetrikus kockázat.* Anonimnál a rosszabbik kimenet IP-blokk — kivárható,
  megkerülhető. Bejelentkezve **account ban**, ami végleges.
- *A ToS-t is rontja.* Anonimnál a tiltás elvont; bejelentkezve névvel,
  elfogadott szerződéssel sérül, ami más kategória.
- *T2-t törékennyé teszi.* Session-függő backfill = lejáró cookie, 2FA,
  újraléptetés — plusz hibaforrás.
- *Valószínűleg felesleges.* A TV ideák Google-ben indexeltek, tehát a TV
  kiszolgálja őket anonim crawlernek.
- *Elrontaná a választ.* T1 kérdése épp az, hogy „tudunk-e automatikusan
  hozzáférni". Bejelentkezve nem tudnánk megmondani, hogy azért ment-e,
  mert megy, vagy mert be voltunk lépve.

Sorrend: anonim → ha fal, **eldobható account** (nem a saját) → a saját user
csak T2-nél, tudatos döntésként, ha másképp nem megy.

**Rate:** lassú, kézzel indított kérések. Nincs loop, nincs párhuzamosság.
Támpont a `robots.txt`-ből: a `*` blokkban nincs `Crawl-delay`, de bingbot 5s,
Yandex 10s → **≥5s két kérés között**.

---

### 0a eredmény (`robots.txt`) — 2026-08-17

**Zöld a hozzáférés.** A `User-agent: *` blokk az ideák közül **csak** a
`/ideas/search/`-öt tiltja. A `/symbols/*/ideas/` és a `/u/*` szerzői oldalak —
amire T2 épülne — nincsenek tiltva. Megerősíti, hogy a TV indexeltetni akarja
az ideákat.

**Van `sitemap-ideas.xml`** a `https://www.tradingview.com/sitemap.xml` indexben.
T2-nek jelentős: ha az idea-URL-ek sitemapből felsorolhatók, nem kell vakon
paginálni — nagyságrenddel kevesebb kérés, és G4 is olcsón megválaszolható.

**AI-crawler opt-out.** Külön blokk sorolja fel a `Google-Extended`,
`PerplexityBot`, `Bytespider`, `DeepSeekBot`, `ClaudeBot` stb. UA-kat, és nekik
tiltja a `/ideas/*`, `/u/*`, `/chart/*` útvonalakat. A TV tehát tudatosan
megkülönbözteti a keresőindexelést az AI-célú tömeges begyűjtéstől, és épp azt
a két útvonalat zárja el, amire ez az epic épül.

**Döntés (user, 2026-08-17):** folytatjuk. Indoklás: **kizárólag saját kutatás,
termékké válás kizárva** — így a licencelt újrabeszerzés kérdése fel sem merül.
A `*` szabály megenged minket, a tartalom publikus, a PoC mennyisége pár kérés.
Kezelés: lassú rate, normál böngésző-UA, egyszeri backfill + inkrementális.

---

### T1 LEZÁRVA — 2026-08-17

Teljes riport: [research/tradingview_poc/README.md](../../research/tradingview_poc/README.md)
Összesen **6 HTTP kérés**, anonim, böngésző-UA. Nem volt Cloudflare challenge.

**Verdikt: T2 indulhat, három megkötéssel.**

| Gate | Eredmény |
|---|---|
| G1 timestamp | ✅ abszolút, mikroszekundum, UTC — **de a listaoldal az `updated_at`-et mutatja** |
| G2 direction | ✅ strukturált; hasznos (Long+Short) kitöltöttség **57%** |
| G3 author_id | ⚠️ csak `username`, nincs stabil numerikus id |
| G4 paginálás | ✅ `/page-N/`, átfedésmentes — ⚠️ **nem kronologikus** |
| G5 entry/stop/target | ❌ nincs strukturáltan (nem blokkoló) |

**A kulcs-leletet G1(d) adta**, pontosan az előre gyanított hibamód:

```
kártya:      dateTime="2026-08-12T05:23:34.000Z"   ← updated_at
detail JSON: "created_at": "2026-08-08T11:13:48"   ← az igazi publikálás
```

**90.2 óra eltérés.** A listaoldal timestampjével a mérés lookahead leaket
kapna. A `created_at` viszont elérhető a detail JSON-ben, tehát javítható.

**Bónusz:** a detail rekord tartalmazza az `interval` mezőt (= `horizon_hint`
a `plan.md` `call` absztrakciójából), a teljes `description`-t, és az
`updates[]` szerkesztéstörténetet saját timestampekkel.

**Figyelmeztetés az epic scope-jára:** a kurált `sitemap-ideas.xml`-ben
SOL ≈ a BTC 2.5%-a (24 vs 961). Torzított minta, de nyitott kérdés, hogy a
valós korpuszban elég sűrű-e a SOL a szerző-szintű méréshez.

**A három megkötés + 6 nyitott kérdés T2-höz** a riport végén.

---

### T1b + T1c — 2026-08-17, ugyanaznap

**T1b (HTML út) — zsákutca.** A szimbólum-feed nem archívum: a `page-50`
ugyanazt a ~10 napos ablakot adja, mint a `page-1`, és a lapozás elfogy
(`page-50` == `page-200` == `page-500`). A szerzői oldal `/u/{username}/`
JS-renderelt, 0 `<article>`, 0 `created_at`. A T2 eredeti terve — „symbol
page paginálás visszafelé, majd author-oldalak" — **ezen az úton nem
kivitelezhető**.

**T1c (API) — megoldva.** A JS bundle-ökben megtalálva:

```
GET /api/v1/ideas/?symbol=&by=&page=&per_page=100
→ { count, page_count, results[] }
```

Egyetlen hívás adja a `created_at`-et **és** a `updated_at`-et külön,
a `symbol.direction`-t (1=long, 2=short — **forrásból igazolva**),
a `symbol.interval`-t, a `user.id` stabil numerikus szerző-azonosítót,
és a teljes `description`-t. **Detail fetch nem kell.**

Ezzel a G1(d) leak-probléma, a G3 author-id probléma és a T1b két
zsákutcája **egyszerre megszűnt**. Használható call arány 78–86%.

**Új T2 terv:** felderítés `symbol=` lekérdezésekkel (~20 kérés) →
szerzőnkénti teljes history `by={username}`-nel (~800–1600 kérés).
**Összesen ~1000–1700 kérés, 6s-mal 2–3 óra, egyetlen éjszaka.**

**Megmaradó limitáció:** a szerző-felderítés csak az elmúlt ~2 hónapot
látja (1000-es query-plafon) → **survivorship bias a szerző-halmazon**.
A felderített szerzők historyja teljes, de aki abbahagyta, láthatatlan.
A T6 placebo teszt ezt nem szűri ki — külön dokumentálandó.
