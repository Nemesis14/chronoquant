# Terv: Rendszeresen frissülő TradingView piaci hangulat report (BTC, később bármely szimbólum)

Státusz: **draft — ötlet és scope rögzítve, fejlesztés előtt** (root szintű sandbox doksi, nem `_jira_` ticket még)
Eredet: interaktív Claude Code sessionben manuálisan végzett, ismételt TradingView-elemzés (2026-09-06/07/08), ami bevált munkafolyamatnak bizonyult — ezt a dokumentum egy ismételhető, később automatizálható folyamattá formalizálja.
Kapcsolódó modul (jövőbeli): `src/ui/` (megjelenítés) + új adatgyűjtő komponens (még nincs modul-otthona)

---

## 0. Motiváció

A manuális session során kialakult egy munkafolyamat, ami érdemi jelet adott:

1. TradingView "Ideas" lista lekérdezése (BTCUSD, legfrissebb elöl)
2. **Minden cikk szövegének ÉS chart-képének** egyenkénti elolvasása/megnézése
3. Minőség szerinti szűrés (nem minden poszt ér egyformán — sok az odavetett, ismétlődő, vagy elavult tartalom)
4. Konszenzus-szintek (support/resistance) kiolvasása több szerzőtől
5. Makró-hírekkel (CPI, FOMC, szabályozási események, tőzsdei incidensek) való keresztellenőrzés

Ez a dokumentum azt írja le, **mit** kellene ebből egy ismételhető, igény szerint (majd később ütemezve) frissülő reportba built-elni, és **hogyan** valósítható meg technikailag.

---

## 1. MIT — a report tartalmi specifikációja

Amikor a user belép és kér egy friss elemzést, a rendszer az alábbi struktúrájú összefoglalót adja:

### 1.1 Piaci állapot fejléc
- Aktuális ár, rövid táv elmozdulás
- Hány cikk került feldolgozásra, milyen időablakból (pl. "utolsó 24 óra, 23 cikk")

### 1.2 Kiemelt (kurált) elemzések
- Nem lista-szerűen minden cikk egyformán, hanem **kiemelve a legjobb minőségűek**
- Mindegyiknél: mit gondol a szerző, **miért** (a mögöttes indoklás emberi nyelven összefoglalva — makró, on-chain, tisztán technikai mintázat stb.), milyen szintekkel

### 1.3 Irány-összegzés (long vs short)
- **Nyers megoszlás** (az összes cikk között)
- **Minőség szerint szűrt megoszlás** — ez a fontosabb szám, mert a session tapasztalata szerint ez a kettő gyakran eltérő, sőt ellentétes képet mutat (pl. mennyiségileg long-túlsúly, minőség szerint short-túlsúly)
- Explicit válasz a "mit gondol most a piac" kérdésre, ha van egyértelmű konszenzus; ha nincs (kiegyensúlyozott a kép), az is legyen kimondva — ez önmagában jel (döntés előtti bizonytalanság)

### 1.4 Célárak, támasz/ellenállás konszenzus
- Klaszterezve: mely szinteket említi több szerző egybehangzóan (ez sokkal erősebb jel, mint egy elszigetelt szám)
- Entry/Stop/Target hármas, ahol egy cikk explicit trade-tervet ad
- Külön jelölve: hosszabb távú (heti/havi chart) vs rövid távú (napon belüli) szintek — ezek ne keveredjenek egy kalapba

### 1.5 Releváns hírek — mire érdemes rákeresni
- A cikkekben említett katalizátorok (adat-megjelenések, szabályozási események, tőzsdei/hálózati incidensek) kigyűjtve, **külön meg is erősítve valós hírforrásból**, nem csak a TradingView-poszt állítása alapján elfogadva
- Dátumozva, sorrendbe rakva ("ez a hét eseménysorozata: X → Y → Z")

### 1.6 Minőség-jelzők, amikre figyelni kell (a session tanulságai)
- **Ismétlődő/spam-szerű szerzők**: van, aki naponta többször posztol szinte tartalom nélkül (pl. csak "Long idea" cím, üres chart) — ezeket alacsonyan kell súlyozni, és érdemes szerzőnkénti "megbízhatósági előzményt" építeni idővel
- **Elavult/újrahasznosított chartok**: néhány cikk olyan ár-szintekre hivatkozik, ami nem egyeztethető a jelenlegi piaci árral — ez arra utal, hogy régi chart friss dátummal újra van posztolva. Ellenőrizendő: a cikkben említett "jelenlegi ár" közel van-e a ténylegesen aktuális árhoz.
- **Érdekütközés-jelzés**: van szerző, aki maga vallja be, hogy fizetett influencer / jeleket árul (Telegram-csatorna reklám a poszt végén) — ez nem feltétlenül teszi hamissá az elemzést, de jelezni kell
- **Bukott előrejelzések**: ha egy korábban látott setup időközben stop-losst ütött, azt a rendszernek fel kellene tudnia ismerni (legalább a "ugyanaz a szerző, korábbi poszt" szinten) és megjegyezni

---

## 2. HOGYAN — technikai megvalósítás

### 2.1 Adatforrás és lekérdezés mechanikája

TradingView-nak **nincs hivatalos, nyilvános API-ja** az Ideas feed-hez → ez scraping, ami:
- **Törékeny** (markup változhat), és a TradingView ToS-a felőle érdemes tájékozódni, mielőtt rendszeresített, gépi, gyakori lekérdezés épül rá
- Két különböző oldaltípust kell külön kezelni:

| Oldal | Mit ad | Korlát |
|---|---|---|
| Lista (`/symbols/BTCUSD/ideas/?sort=recent`) | Címek, szerzők, URL-ek | **JS-renderelt**, nyers HTML (curl) gyakorlatilag üres váz — csak JS-t végrehajtó fetch (a session ezt a WebFetch tool-lal oldotta meg) ad használható listát. Az engagement-számok (komment/like) a lista-nézetben **megbízhatatlanok** voltak a session során (nyilvánvalóan hibás értékek jöttek vissza) |
| Egyedi cikk (`/chart/BTCUSD/<slug>/`) | Teljes szöveg, időbélyeg, chart-kép URL (`og:image` meta tag, `s3.tradingview.com/.../..._big.png`) | Ez **nyers HTML-ből (curl) is kinyerhető** — az `og:image` már a szerver-oldali HTML-ben benne van, nem kell JS-render hozzá. Relatív időbélyeg ("13 hours ago") a JS-renderelt nézetben megbízható volt |

**Következtetés a megvalósításhoz:** a lista-oldalt JS-képes fetch-csel kell járni (headless browser vagy hasonló), az egyedi cikkoldalakat viszont elég nyers HTTP-vel (curl/requests) lekérni — ez utóbbi gyorsabb és olcsóbb, és **ez adja a chart-kép URL-t is**, ami kritikus (lásd 2.3).

### 2.2 Creation date / időbélyeg problémák

- A lista-nézet **nem ad megbízható per-item időbélyeget** — ezt a session tévesen "nincs is dátum" -ként értékelte először, majd kiderült, hogy csak a lista-agregátumban vész el, az egyedi oldalon megvan
- Az egyedi oldal relatív időbélyeget ad ("Xh ago", "Xd ago", "Updated Xh ago (posted Xd ago)") — ezt **abszolút időponttá kell parse-olni** a lekérdezés időpontjához képest
- **Duplikáció-kezelés szükséges**: ugyanaz a szerző gyakran posztol új URL-lel majdnem azonos tartalmat (pl. napi ismétlődő "45-Minute Technical Analysis" sablon-cím) — ezeket vagy szerző+cím-minta alapján kell csoportosítani, vagy explicit engedni, hogy "ugyanattól a szerzőtől érkezett új verzió"
- **Elavult tartalom detektálása**: a cikkben szereplő "jelenlegi ár" referenciát érdemes összevetni a ténylegesen lekérdezett spot árral — ha nagy az eltérés, valószínűleg újraposztolt régi chart

### 2.3 Mit kell fixen lerakni (a rendszer "magja")

Amit a session iterálva alakított ki, és amit érdemes kódba/promptba fixálni, hogy ne kelljen minden futtatáskor újra kitalálni:

1. **Lista-scrape prompt/logika** — mit kell kinyerni a lista-oldalról (cím, szerző, URL), és hogy ismerje fel, mely elemek újak egy korábbi futtatáshoz képest
2. **Egyedi cikk extrakciós prompt/séma** — fix mezőkészlet: időbélyeg, szerző, engagement, teljes szöveg, entry/stop/target, resistance/support lista, irány (long/short/neutral)
3. **Kép-letöltési és -elemzési lépés** — az `og:image` URL kinyerése nyers HTML-ből (egyszerű regex, nem kell LLM hozzá), majd a letöltött kép **vizuális elemzése egy vízió-képes modellel** — ez adja a chart-ba rajzolt szinteket, amik gyakran NEM szerepelnek a szöveges leírásban (ez volt a session egyik legfontosabb felismerése — pl. a "nincs konkrét szint" első benyomás tévesnek bizonyult, amikor a képet is megnéztük)
4. **Minőség-pontozási rubrika** — kódolt, nem csak "érzésre" eldöntött szempontlista:
   - van-e explicit invalidációs/stop szint
   - makró/on-chain alátámasztás vs. tisztán mintázat-illesztés
   - ár-konzisztencia check (lásd 2.2)
   - szerző-előzmény (spam-gyanús vagy megbízható korábbi posztok alapján)
   - érdekütközés-jelzés (promóciós tartalom)
5. **Makró-naptár keresztellenőrzés** — fix esemény-lista (CPI megjelenés, FOMC-ülés, ismert szabályozási határidők) + friss hírkeresés, amikor egy cikk katalizátorra hivatkozik
6. **Udvarias lekérdezési szabályok** — User-Agent, kérések közötti késleltetés, ésszerű gyakoriság (ne minden oldalbetöltésnél fusson újra a teljes pipeline)

Python-függvény váz (célszerű bontás, nem véglegesített modulstruktúra):

```
fetch_ideas_list(symbol, since=None) -> list[IdeaRef]
fetch_idea_detail(idea_url) -> IdeaDetail          # nyers HTTP elég
download_chart_image(image_url) -> Path
analyze_chart_image(image_path) -> dict            # LLM vízió hívás
score_quality(idea_detail, chart_analysis, history) -> QualityTier
dedupe_and_merge(new_ideas, previous_state) -> list[IdeaDetail]
cross_check_news(mentioned_catalysts) -> list[NewsItem]
build_report(ideas, news) -> ReportModel
```

### 2.4 UI — Streamlit integráció

Megvalósítható új oldalként a meglévő dashboardban (`src/ui/pages/`, ami jelenleg üres — ide illeszkedne egy pl. `05_market_sentiment.py`), a meglévő komponens-mintát követve (`src/ui/components/`).

Két üzemmód közül lehet választani:
- **On-demand ("Frissítés" gomb)**: a user kéri, a pipeline lefut, eredmény megjelenik — egyszerűbb, de a user vár a válaszra (a session tapasztalata szerint egy teljes kör — lista + ~15-20 cikk szöveg+kép + összegzés — nem pillanatok alatt fut le)
- **Ütemezett (háttérben, pl. óránként/naponta Nx)**: a pipeline egy scheduler-ből fut (pl. a meglévő `sync_runner.py` mintájára), eredményt DuckDB-be/cache-be ír, a Streamlit oldal csak a legutóbbi cache-elt eredményt jeleníti meg — gyorsabb UX, de plusz infrastruktúra (lásd 2.6)

**Nyitott döntés:** melyik legyen az első verzió — javaslat: **on-demand előbb**, mert egyszerűbb és validálja az értéket, mielőtt ütemezésbe/historizálásba fektetünk.

### 2.5 Claude/API-költség kérdés — kritikus tisztázandó pont

Ez a session most **interaktív Claude Code session-ön belül** futott — a WebFetch és kép-olvasó (Read) tool-okat ez a session használta, ami **nem ugyanaz**, mint amit egy önállóan futó Streamlit-háttérfolyamat elérne.

**Ami emiatt szükséges lesz, ha ezt automatizálni/beépíteni akarjuk:**
- Egy **önálló Anthropic API-kulcs** (console.anthropic.com, pay-per-token), mert a Streamlit szerver-oldali kódja nem tudja "meghívni ezt a Claude Code sessiont" — a Claude Code egy interaktív CLI-eszköz, nem beágyazható backend szolgáltatás
- Ez **külön költség**, a jelenlegi Claude Code előfizetéstől függetlenül elszámolva (token alapú, modellenként eltérő ár)
- **Nagyságrendi becslés** (durva, pontosítandó mielőtt commitolunk mellé): egy teljes kör kb. 15-20 chart-kép vízió-elemzése + 15-20 szöveg-extrakció + 1 összegző hívás — ez centekbe/néhány dollárba kerülhet futtatásonként Sonnet-tel, a pontos szám a képméretektől és futtatási gyakoriságtól függ. **Ezt érdemes egy kis pilot-futtatással ténylegesen lemérni**, mielőtt ütemezett/gyakori üzemmódot építünk rá.
- **Olcsóbb alternatíva**: kép-elemzés kihagyása, csak szöveges extrakció — ez viszont **elveszíti a chart-ba rajzolt szinteket** (session tapasztalat: ez volt az egyik legértékesebb infó-forrás, pl. a Fibonacci-célárak vagy support/resistance dobozok gyakran csak a képen látszottak). Trade-off, amit a userrel érdemes explicit eldönteni.
- Költség-kontroll: gyakoriság-limitálás (pl. napi 2-4 futtatás, nem minden oldalbetöltés), és/vagy csak az **új** cikkek elemzése (a már látottak cache-ből jönnek, lásd 2.2 dedup)

### 2.6 Historizálás

**Igen, jól illeszkedik a meglévő infrastruktúrához** — a projekt már használ DuckDB-t (`database_agent` domain, `src/data_handling/store`). Javasolt minta:
- `tv_sentiment_runs` tábla: run_id, futtatás időpontja, feldolgozott cikkek száma, aktuális ár
- `tv_sentiment_ideas` tábla: run_id (FK), szerző, cím, URL, időbélyeg, irány, minőség-kategória, extraháltszintek (JSON), rövid indoklás-összegzés
- Ebből idővel levezethető egy **hangulat-idősor** (pl. "minőség-szűrt long/short arány naponta") — ez akár önálló feature-jelöltté is válhat a `src/modeling/feature_engineering`-ben, bár ez már túlmutat a jelen terv scope-ján, csak jelezve, hogy az adatszerkezet erre is nyitva hagyható

---

## 3. Döntések (2026-09-08, lezárva — fejlesztés indulhat)

1. **On-demand** első verzió. Nincs ütemezés/scheduler ebben a körben.
2. **Szöveg-only** — nincs kép/vízió-elemzés az első verzióban. (Következmény: a csak chart-ba rajzolt szintek — lásd session-tapasztalat CoinChartist-nál — ebben a verzióban kimaradnak. Tudatosan vállalt trade-off.)
3. **Csak BTC** most, később szimbólum-paraméteressé tehető.
4. **Ingyenes, saját használatra** — nincs külön Anthropic API-kulcs. Az AI-elemző maga a Claude Code session (lásd 4. fejezet architektúra). A mechanikus lekérdezés (lista + cikkszöveg) sima Python/HTTP, AI nélkül.
5. **ToS-review nem kell** most — személyes, nem-kereskedelmi, alacsony gyakoriságú (on-demand) használatra épül.
6. **Nincs `_jira_` epic** — közvetlenül fejlesztés, dokumentáció ez a fájl marad.

## 4. Architektúra — hogyan fut le ténylegesen

Mivel az AI-elemző maga a Claude Code session, **nincs "magától lefutó" pipeline** — mindig egy aktív sessionben, user-kérésre történik. A folyamat két élesen elválasztott részből áll:

| Lépés | Végrehajtó | Kell-e AI |
|---|---|---|
| 1. Lista lekérdezés + új cikkek azonosítása (dedup korábbi futtatáshoz képest) | Python szkript (`requests` + HTML-parse) | Nem |
| 2. Egyedi cikkek szövegének letöltése + megtisztítása (readable text) | Python szkript | Nem |
| 3. Szövegek elolvasása, irány/szint/minőség-értékelés, összegzés | **Claude Code (a session, ami futtatja)** | Igen — ez az egyetlen AI-lépés |
| 4. Report elmentése historizáláshoz | Python szkript | Nem |

**UI:** nincs az első verzióban. A report markdown fájlba kerül (`research/tv_reports/`), historizálva; a chatbe is kiírásra kerül. Statikus (AI-hívás nélküli) Streamlit-nézet a mentett reportokra később, igény esetén, nem blokkolja a jelen kört.

**Korlát, amit érdemes tudatosítani:** ez nem "indítom és elmegy" automatizmus — mindig kell hozzá egy futó, kérést kapó Claude Code session.

---

## 5. Megvalósítás — v1 státusz (2026-09-08, kész)

**Nagy fordulat a 2. fejezethez képest:** a `research/tradingview_poc/` alatt már korábban (más okból, `epic_051`-hez) fel volt térképezve egy **hivatalos, dokumentálatlan, de működő TradingView REST API** (`GET /api/v1/ideas/?symbol=BTCUSD&per_page=100`), és hozzá egy kész, rate-limitelt, resumable Python-kliens: `src/data_handling/collectors/tradingview/api.py` (`IdeasClient`). Ez **strukturált JSON-t ad vissza egyetlen HTTP-hívásból**, benne:

- pontos ISO `created_at` (a listaoldal-HTML/JS-render bizonytalanságát ez teljesen kiváltja — a 2.2 fejezet leírta problémák innentől tárgytalanok)
- **teljes szöveg** (`description`, nem csonkolt)
- `symbol.direction` (1=long, 2=short, hiányzik=nincs cimkézve) — forrásból igazolt kódolás
- `is_education` / `is_video` / `is_script` flag-ek (automatikus szűrés)
- `likes_count` / `comments_count` / `views_count` (megbízható, nem a korábbi lista-nézetes "2121 komment" típusú hibás szám)

**Következmény:** a 2.1 fejezetben vázolt "JS-renderelt lista + nyers HTML detail-oldal" kétlépéses terv **elavult** — nincs szükség sem JS-renderelésre, sem HTML-parse-ra, sem `og:image` regexre. Egyetlen `requests`-szintű HTTP hívás mindent ad, amit a szöveg-only v1-hez a terv előírt.

### Mit írtunk

`research/tv_report/fetch_report_data.py` — újrahasznosítja a meglévő `IdeasClient`-et és a `normalize.py` szimbólum-whitelistjét/interval-parszerét, de **nem ír az `analyst_signal.duckdb`-be** (az az epic_051 backfill-célja, más scope). Ehelyett:

1. Lekéri a legfrissebb ideákat (alapértelmezett: 1 lap = ~100 idea)
2. Kiszűri a nem-BTC-árfolyam szimbólumokat (`SYMBOL_WHITELIST` újrahasznosítva)
3. Megtartja a **teljes** szöveget (a normalize.py ezt eldobja, mi pont ezért írtunk külön szkriptet)
4. Minden ideát megjelöl: van-e explicit irány-címke, hány órás, új-e az előző futtatás óta (`state.json` dedup)
5. Kiír egy `latest.json`-t (amit Claude közvetlenül `Read`-del beolvas) + historizál egy `runs/<run_ts>/` mappába (nyers API-válasz, változatlanul)

**Élesben tesztelve (2026-09-08):** 1 hívás → 99 idea, 93 nem-oktatási, 67 explicit irányzott (38 long / 29 short) — ez **egyetlen script-futtatás alatt** annyi anyagot ad, amennyihez korábban a manuális session ~10-15 `WebFetch` hívást használt, és emellett pontos időbélyeggel, teljes szöveggel, hibátlan engagement-számokkal.

### Hogyan kell használni (mostantól)

```
.venv/Scripts/python research/tv_report/fetch_report_data.py
```

majd a chatben: *"olvasd el a research/tv_report/latest.json-t és foglald össze"* — ez indítja a 4. fejezetben leírt 3. lépést (a Claude Code session olvassa, értékeli, összegzi).

Opcionális kapcsolók: `--hours 24` (csak az utolsó N óra), `--pages 2` (több mint 100 idea), `--out <path>`.

### Nyitva maradt, később eldöntendő
- ~~Szerzőnkénti megbízhatósági előzmény~~ — lásd 6. fejezet, elindult
- Kép-elemzés hozzáadása (tudatosan kihagyva v1-ből, lásd 2.5 fejezet költség-trade-off)
- Makró-hír keresztellenőrzés még mindig manuális (WebSearch), nincs automatizálva

---

## 6. Szerző-megbízhatósági visszamérés (author track record backtest)

**Eredet:** a session-ök során ismétlődően felmerült, hogy a "jó minőségű" (tiszta szintek, jó indoklás) cikkek szerzői nem feltétlenül azok, akiknek a hívásai ténylegesen bejönnek. Ez a fejezet írja le az erre kifejlesztett, önálló visszamérő pipeline-t.

### 6.1 Cél

Szerzőnként megválaszolni: **hány posztot írt, milyen TP/SL-lel dolgozott, és ezek ténylegesen teljesültek-e** a valós árfolyam ellen visszamérve — függetlenül attól, hogy a szöveg "jó minőségűnek" tűnik-e emberi/AI-olvasásra.

### 6.2 Módszertan

1. **Felfedezés** (`author_backtest.py discover`): széles, több crypto szimbólumon (BTC, ETH, SOL, XRP, DOGE, ADA, BNB, LTC, LINK, AVAX) átívelő TradingView Ideas-lekérdezés, szerzőnkénti aggregálás. Szűrők:
   - **Aktivitás**: csak az utolsó 90 napban posztoló szerzők (aki 3 hónapja nem aktív, nem "követhető")
   - **Crypto-fókusz**: elsősorban crypto-t kereskedő szerzők; tisztán forex/arany-elemzőket kizárjuk (lásd FXCM-eset lent)
2. **Előzmény-lekérdezés** (`fetch_author_history`): a `by=<username>` API-paraméterrel a szerző teljes elérhető története (max 1000, API-sapka), utána crypto-szimbólumra szűrve.
3. **TP/SL-kinyerés** (`extract_tp_sl`): regex-alapú, több formátum-mintát fedve (`Entry/SL/TP1`, `Target 1 (BSL 1): X`, `"target of X and Y with stoploss of Z"`, "érték — cimke" sorrendű dash-formátum). **Plauzibilitás-szűrő**: a cél és a stop nem lehet 5×-nél nagyobb arányban egymástól — enélkül a regex időnként a címke saját sorszámát (pl. "Target **1**") kapta el árként, hamis $1-es célárakat generálva (valós hiba volt, javítva).
4. **Visszamérés** (`evaluate`): Binance óránkénti gyertyák (`high`/`low`, nem záróár) ellen, 3 kimenettel — TP / SL / inconclusive (nem dőlt el a horizonton belül). Azonos gyertyás TP+SL ütközésnél konzervatívan SL nyer.
5. **OHLCV cache**: `research/tv_report/runs/ohlcv_cache/` — **nem** az éles `database/`/`config/assets.json` alá kerül, kifejezetten azért, hogy ne keveredjen a `solusdt` éles kereskedési adatbázissal.

### 6.3 Eredmény (2026-09-21, 121 szerző screenelve)

Tölcsér: 121 aktív, crypto-fókuszú szerző (min. 3 aktivitás) → 34-nél volt egyáltalán mérhető (numerikus TP+SL) poszt → 14-nél volt N≥5 érdemi minta.

| Szerző | N | Találati arány | Megjegyzés |
|---|---|---|---|
| **Apex_Legends** | 22 | **59%** | Validált: pozitív expectancy (+0.96 R/trade), BTC 73%/ETH 45%, javuló időbeli trend |
| PremiumTrader57 | 26 | 62% | Részlegesen validált: a saját belépési zónáit használva (N=10 alminta) +1.56 R/trade |
| FXCM | 25 | 60% | **Kizárva a "kövessük" listáról** — csak 3%-ban crypto-fókuszú, valójában forex/CFD-bróker |
| ChartWithMG | 51 | 49% | Legnagyobb minta, ~fej-vagy-írás |
| FrankAgbo | 64 | 28% | Nagyon konzisztens, professzionális formátum, de gyenge pontosság — **forma ≠ pontosság** |

**Legfontosabb tanulságok:**
- A kvalitatív "jó minőség" (tiszta szintek, jó indoklás) **nem jósolja be** a találati arányt (FrankAgbo a legtisztább formátum, mégis a leggyengébbek között).
- A puszta találati arány félrevezető lehet R:R (kockázat/hozam) nélkül — ezért mindig együtt kell nézni.
- A legmagasabb volumenű szerzők egy része (TradingShot 248, Sporia 259, FX_Professor 257 crypto poszt) **kézzel ellenőrizve is genuinely nem mérhető** — hosszú, narratív makro-ciklus tézist írnak numerikus TP/SL nélkül, ez nem regex-hiba, hanem stílus.
- Csak crypto-arány (nem abszolút crypto-poszt-szám) alapján kizárni félrevezető lenne — FXCM-nél az abszolút szám (25 mérhető) megtévesztő volt, a 3%-os arány mutatta meg, hogy nem crypto-szerző.

### 6.4 Eszközök
- `research/tv_report/author_backtest.py` — a fő pipeline (`discover` / `run <min_count>`)
- `research/tv_report/runs/author_backtest/report.json` — legutóbbi teljes futás eredménye (szerzőnként: total_posts, crypto_posts, crypto_share, measurable, TP, SL, hitrate, rows)
- `research/tv_report/runs/ohlcv_cache/` — újrafelhasználható Binance OHLCV cache szimbólumonként

### 6.5 Nyitva maradt
- A regex-kinyerés még mindig sok szerzőnél 0 találatot ad (nem feltétlenül hiba — sokuk genuinely narratív, szám nélküli stílusban ír)
- Az R:R/expectancy-számítás csak akkor megbízható, ha a szerző explicit belépési zónát ad — enélkül a "posztolás-pillanati ár" proxy torzít (lásd PremiumTrader57-eset)
- Nincs még automatikus, rendszeres újrafuttatás — ad-hoc, session-ön belüli indítás
