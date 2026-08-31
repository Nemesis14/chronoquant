# Epic 054: Alt-data feature discovery — derivatives, on-chain, flow

## Goal

Kiépíteni egy **feature discovery pipeline**-t, ami a jelenlegi tisztán
OHLCV-alapú feature-készletet piaci mikrostruktúra- és on-chain adatokkal
egészíti ki, majd **megméri**, hogy ezek hordoznak-e prediktív információt a
`fw60` MFE targetre.

A jelenlegi modell mindent az árból és a volumenből származtat. Az ár azonban
következmény: a pozicionáltság (open interest, funding, tőkeáttétel), a
tőzsdei flow (be/ki áramlás) és a stablecoin-likviditás **okok** — ezek
elvben előbb mozdulnak, mint az ár. Ez az epic azt méri meg, hogy a
gyakorlatban is így van-e, a mi 1 perces rácsunkon és a mi 60 perces
horizontunkon.

## A hipotézis

> A derivatíva-pozicionáltság és a tőzsdei flow változása **vezeti** a 60 perces
> kilengést: OI ↑ + funding ↑ → tőkeáttétel épül → nagyobb likvidációs
> potenciál; exchange inflow ugrás → eladási nyomás; outflow ugrás → akkumuláció.

Egyik sem önálló buy/sell jel — feature-ként mennek a modellbe, és ugyanazon a
gate-en kell átmenniük, mint az `epic_053` figyelem-idősorának.

## Gate

Ha egyetlen csoport (derivatives / flow / on-chain) sem éli túl a permutációs
placebót és az FDR-korrekciót, az adott csoport lezárul. A **fizetős**
adatforrásra (CryptoQuant, Glassnode, CoinGlass fizetős tier) csak akkor
költünk, ha az ingyenes/olcsó rétegen már látszik jel. A drága adat nem
belépő, hanem jutalom.

## Scope

- `src/data_handling/collectors/derivatives/` — új modul (Binance/Bybit public)
- `src/data_handling/collectors/onchain/` — új modul (ingyenes chart/flow API-k)
- `data/analyst_signal/raw/source=derivatives|onchain/` — raw parquet réteg
- `database/analyst_signal/analyst_signal.duckdb` — `deriv_*` / `chain_*` táblák
- `src/modeling/analyst_signal/` — feature normalizálás + gate mérés (közös az
  epic_053 rétegével)
- `_doc_/research/altdata_source_matrix.md` — forrásmátrix
- `_doc_/research/altdata_gate.md` — gate riport

Nem érinti: `src/strategy/`, `src/trading/`, `src/ui/`, és **nem nyúl hozzá** a
solusdt champion pipeline feature-készletéhez. A modellbe integrálás pozitív
gate után, külön epicben történik.

## Tasks

- t301: Adatforrás-mátrix — mélység, granularitás, ár, Python-hozzáférés → analyst_agent
- t302: Time-critical derivatives collector (OI, L/S ratio, taker flow, likvidációk) → database_agent
- t303: Backfillelhető historikus ingest (funding, stablecoin supply, chain-aggregátumok) → database_agent
- t304: Alt-data feature réteg — lag, delta, trailing z-score, rezsim-normalizálás → modeling_agent
- t305: Gate mérés — lagged IC/MI, Granger, permutációs placebo, FDR → modeling_agent
- t306: Riport és csoportonkénti go/no-go döntés → analyst_agent

Sorrend: (t301 ∥ t302) → t303 → t304 → t305 → t306

## Key Decisions

- **A t302 nem várhat a t301-re.** A derivatíva-idősorok jelentős része
  **nem backfillelhető**: a Binance futures `openInterestHist`,
  `topLongShortAccountRatio` és `takerlongshortRatio` endpointjai ismerten
  korlátozott (nagyságrendileg ~30 napos) visszamenőleges ablakot adnak, a
  likvidációk historikusan pedig egyáltalán nem. Ugyanaz a logika, mint az
  `epic_050`-nél: amit ma nem mentünk, az véglegesen elveszett. A gyűjtés
  ezért a felméréssel **párhuzamosan** indul, nem utána.
- **Saját node és raw blockchain letöltés kizárva.** A cím→entitás címkézés
  (exchange / whale / miner / market maker) önálló data engineering projekt,
  és pont az a rész, amiért a CryptoQuant/Glassnode-féle szolgáltatókat fizetik.
  Ha ide jutunk, adatot veszünk, nem chaint szinkronizálunk.
- **Asset-hasadás szándékos.** A derivatíva-réteg **közvetlenül SOLUSDT-re**
  gyűjt (ez az aktív asset, és a SOL perp likvid). Az on-chain/stablecoin réteg
  viszont **BTC/ETH + aggregált stablecoin** — ott van adatminőség és ott van
  ingyenes historikus mélység; ezek piaci rezsim-feature-ök, nem SOL-specifikusak.
  SOL-natív on-chain csak akkor, ha ingyen adódik.
- **Point-in-time korrektség kötelező.** Két ismert csapda: (1) az on-chain
  metrikák utólag revideálódnak és visszamenőleg felülíródnak — a ma letöltött
  történelmi sor nem az, amit `t`-ben láttunk volna; (2) az exchange balance /
  reserve típusú mezőket a szolgáltatók óránként *felülírják*, nem
  eseményhez horgonyozzák. Minden felvett sor kap `observed_at` mezőt a
  `value_ts` mellett — enélkül ugyanaz a lookahead, ami az `epic_051`
  TradingView `updated_at`-jét megbuktatta.
- **Likvidációs adat gyanús alapból.** A Binance 2021 óta throttle-olja a
  likvidációs streamet (nagyságrendileg 1 esemény/másodperc), tehát a publikus
  likvidációs volumen szisztematikusan alulmért, és nagy kilengéskor mér a
  legrosszabbul — pont amikor számítana. Feature-ként használható, de a t306
  riportnak ezt explicit korlátként kell kezelnie.
- **Nem 300 feature-rel indulunk.** A t301 kimenete egy **10-20 elemű
  shortlist**. A lag/delta/z-score expanzió multiplikátor: 15 nyers sorozat ×
  7 lag × 4 transzformáció már 400+ oszlop, ami FDR mellett önmagában megöli a
  szignifikanciát. A széles expanzió csak a gate-en átment csoportokra fut.
- **Az `epic_051` validációs fegyelme kötelező.** Permutációs placebo + FDR,
  nem nyers p-érték. A mérés a `t-1` lag konvencióval fut, mint minden más
  feature a projektben.

## Notes

Előzmények: `epic_050` (leaderboard collector — ugyanaz a "gyűjts most, elemezz
később" logika), `epic_053` (reddit attention gate — ugyanaz a gate-metodika és
feature-normalizálási réteg újrahasznosítható).

A felvetés forrása: user research-jegyzet a CoinGlass V4 / CryptoQuant /
Glassnode / Coin Metrics adatkínálatról (2026-08 állapot). A konkrét
elérhetőségeket és árakat a t301 ellenőrzi — a jegyzetben szereplő állítások
felmérési inputok, nem tények.
