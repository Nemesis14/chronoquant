# Epic 054: Alt-data feature discovery — derivatívák

> **Frissítve 2026-08-21** — a t301 felmérés után a terv jelentősen egyszerűsödött.
> Az eredeti "nem backfillelhető, gyűjtsünk hónapokig" premissza **téves volt**:
> a Binance Vision archívum 2021-12-01-ig visszamenőleg ad 5 perces
> pozicionáltsági adatot, ~20 MB-ban. A mérés tehát **most futtatható**.

## Goal

Megmérni, hogy a Binance derivatíva-adatok (open interest, long/short
pozicionáltság, prémium index, bázis) hozzáadnak-e bármit a meglévő,
tisztán OHLCV-alapú feature-készlethez a `fw60` MFE target előrejelzésében.

A jelenlegi 208 feature mindent az árból és a volumenből származtat. Az ár
azonban következmény: a pozicionáltság és a tőkeáttétel ára **okok** — elvben
előbb mozdulnak. Ez az epic azt méri meg, hogy a gyakorlatban is így van-e.

## A kísérlet felépítése

Nem "alt-only" modellt építünk — az IV és MI univariáns mérőszámok, ugyanazt
adnák. Helyette **kontroll és kezelés**, azonos ablakon:

| Modell | Feature-ök | Train | Valid |
|---|---|---|---|
| **C — kontroll** | 208 meglévő | 2022-01-01 → 2026-01-31 | 2026-02-01 → 2026-07-31 |
| **T — kezelés** | 208 + 22 alt | ugyanaz | ugyanaz |
| *champion (élő)* | 208 | 2021-01-01 → | ugyanaz | 

C és T között **egyetlen dolog különbözik: a 22 alt-változó.** A különbségük
tehát tényleg az alt-adat hatása, semmi másé. A valid ablak szándékosan azonos
a championével, így a `top10_lift` közvetlenül összevethető.

A döntést a **kereskedési eredmény** is alátámasztja: strategy session mindkét
modellre, `total_fact_log_return` és win rate összevetéssel.

## Scope

- `src/data_handling/collectors/derivatives/` — Vision letöltő + likvidációs stream
- `database/solusdt/solusdt.duckdb` → `altdata_5m`, `feat_altdata` táblák
- `src/data_handling/sync_tables/_altdata_features_polars.py` — 22 feature
- `config/models.json` — két új modell bejegyzés
- `_doc_/research/altdata_source_matrix.md` — forrásmátrix (kész)
- `_doc_/research/altdata_gate.md` — végső riport

Nem érinti: `src/trading/`, `src/ui/`, és **nem módosítja az élő champion
modellt** — a C és T modellek külön `model_id`-n futnak.

## Tasks

- t301: Adatforrás-mátrix → analyst_agent — **KÉSZ** (`pr_`)
- t302: Likvidációs stream collector → database_agent *(párhuzamos, nem kritikus úton)*
- t307: Vision letöltő → `altdata_5m` tábla → database_agent
- t308: 22 feature számítása + `quant_train` join → database_agent
- t309: Modell C (kontroll) → modeling_agent
- t310: Modell T (kezelés) → modeling_agent
- t311: Strategy session, összevetés, Tier 3/4 döntés → analyst_agent

Sorrend: t307 → t308 → (t309 ∥ t310) → t311
Párhuzamosan, függetlenül: t302

## Key Decisions

- **Fizetős szolgáltató nem kell.** A CoinGlass mélysége a felbontással
  fordítottan arányos — a 4,7 éves 5 perces ablakot egyik tier sem adja meg,
  a Vision ingyen igen. A CryptoQuant kizárva: a **Solana nincs a támogatott
  asset-listáján**, és $99-ért csak napi felbontást ad.
- **1 perces pozíció-adat sehol nem létezik.** Binance, Bybit, OKX mind 5m-nél
  áll meg. Ez nem gond: a target 60 perces, és a sampler óránként vesz egy sort.
- **Az 5m → 1m átvezetés forward-fill**, az utolsó lezárt 5 perces barból.
  Sampler-változtatás nem kell — óránkénti mintavétel mellett a ffill nem hoz
  létre ismétlődő mintasorokat.
- **Minden ablak ≤ 240 perc, a normalizálás is.** A projekt embargója 240 perc;
  hosszú baseline (30 napos z-score) nem fér bele. Ezért dimenziótlan arányok
  (`oi_to_volume`, `retail_vs_top`, `basis`) trailing z-score helyett, ahol lehet.
- **A train 2022-01-01-től indul**, mert a `metrics` 2021-12-01-től létezik.
  A 11 hónap veszteséget a kontroll modell semlegesíti.
- **A `bookDepth` (Tier 3) és `aggTrades` (Tier 4) kimarad az első körből** —
  a bookDepth csak 2023-01-tól van, második lefedettségi határt vezetne be.
  A t311 dönt róluk.
- **A likvidáció az egyetlen nem backfillelhető adat**, ezért a t302 fut, de
  nincs a kritikus úton. A Binance ~1 esemény/mp throttle-ja miatt eleve alulmér.

## Notes

Előzmények: `epic_050` (leaderboard collector), `epic_053` (reddit attention
gate — LEZÁRVA: volt jel, de nem derült ki, hogy hozzáad-e a meglévő
volatilitás-feature-ökhöz). **Ez az epic pont azt a hibát kerüli el**: a
kontroll/kezelés felépítés eleve az inkrementális értéket méri, nem az önálló
korrelációt.

---

## Kimenet és folytatás (2026-08-22)

Riport: [_doc_/research/altdata_gate.md](../../_doc_/research/altdata_gate.md)

**Verdikt: GO, fenntartásokkal.** 20 a 22 alt-változóból túlélte a FE szűrést.
A modell-metrikák alig javultak (lift +1%, a top/alsó spread romlott, a
train–valid gap 38-szorosára ugrott), a **kereskedési eredmény viszont
markánsan**: 8 bps kör-költségnél azonos gyakoriságon (415 vs 401 kötés)
11,53 vs 8,60 bps/kötés — **+34% él**.

Folytatás két külön epicben:

- **`epic_055`** — kereskedési költség-modell a strategy rácskeresésbe.
  Ez a vizsgálat legfontosabb mellékterméke: a kereső jelenleg bruttóra
  optimalizál, és ezért olyan beállítást választott, ami a 100 USD tőkét
  kilencszer felszámolta volna. Nem alt-data kérdés, alaprendszer-hiányosság.
- **`epic_056`** — short irány és második valid ablak. A +34% egyetlen
  6 hónapos ablakon és csak longban áll; ez erős jelzés, nem bizonyíték.

Nyitva marad: `t302` likvidációs collector (az egyetlen nem backfillelhető adat).
