# Epic 051: TradingView Ideas — szerző-skill validáció

> **LEZÁRVA 2026-08-20 — a verdikt: NEM MEGY.**
> Nincs azonosítható szerző, akit érdemes lenne követni. FDR-korrekció után
> **nulla** szerző marad, mind az öt horizonton; a legjobb jelölt sem jobb egy
> véletlen particionálás legjobbikánál (family-wise `p = 0,476`).
>
> Teljes riport: [_doc_/research/tradingview_author_skill.md](../../_doc_/research/tradingview_author_skill.md)
>
> **Következmény:** az `epic_052` (Reddit) a gate szerint törlődik.
> Az `epic_050` (leaderboard) ettől függetlenül fut tovább — ott valódi
> pozíciókat mérünk, nem publikált véleményeket.

## Goal

Kideríteni **olcsón és NLP nélkül**, hogy létezik-e egyáltalán perzisztens,
out-of-sample kimutatható szerző-szintű előrejelzési képesség publikus
platformokon.

A TradingView Ideas az egyetlen forrás, ahol az `(author, asset, direction,
timestamp)` négyes **natívan** megvan — a szerző maga jelöli meg a posztot
Long/Short-ként, szimbólummal és timeframe-mel. Nincs szükség
szövegértelmezésre, tehát nincs extraktor-hiba, ami elmossa a mérést.

## Ez az epic egy GATE

Ha itt a validáció nem mutat jelet, **az `epic_052` (Reddit) törlődik**, és a
teljes szöveges irány lezárul. A sorrend nem opcionális: a Reddit track
nagyságrenddel több munka, és ha itt nincs jel, az mind kidobott idő.

Az `epic_050` (leaderboard) ettől **független** és párhuzamosan fut — az
időérzékenysége miatt nem várhat erre a döntésre.

## Scope

- `src/data_handling/collectors/tradingview/` — új modul
- `data/analyst_signal/raw/source=tradingview/`
- `data/analyst_signal/calls/`, `scores/` — normalizálás és scoring
- riport notebook a megy/nem-megy döntéshez

Felhasznált meglévő infra: OHLCV store
([src/data_handling/sync_tables/sync_ohlcv.py](../../src/data_handling/sync_tables/sync_ohlcv.py))
adja a forward returnöket — nem kell új árfolyamforrás.

## Stories

- s102: TradingView Ideas backfill és szerző-skill mérés

## Key Decisions

- **Hivatalos API nincs.** A fizetős TradingView API csak chart/market data;
  az Ideas felület nem érhető el hivatalosan. Marad a scraping.
- **Scope: SOL + BTC + ETH**, nem csak SOL. A BTC kell a béta-korrekcióhoz —
  enélkül a 2021-es bull minden longot zseninek mutat.
- **BTC-first (user, 2026-08-17):** a validáció **kizárólag BTC-n** fut
  (BTCUSD + BTCUSDT). A BTC nem csak béta-referencia, hanem maga a gate:
  ez a legnépszerűbb, legtöbb szerzővel lefedett asset — ha itt nincs jel,
  a SOL-on végképp nem lesz. A `t1` alátámasztja: a kurált sitemapben
  SOL ≈ a BTC 2.5%-a. A SOL és az ETH **elhalasztva**, amíg BTC-n nincs
  pozitív eredmény.
- **Egyszeri backfill + inkrementális frissítés**, lassú rate-tel. A kockázat
  nem jogi (saját kutatás), hanem operatív: IP/account ban.
- A raw HTML/JSON válaszok **megőrzendők**. A törölt vagy szerkesztett ideák
  különben visszamenőleg torzítanak, és a `first_seen` nélkül ez nem detektálható.

## Notes

Kiindulásnak létezik egy publikus dataset — [DiljitSingh14/tradingIdeas2.0](https://huggingface.co/datasets/DiljitSingh14/tradingIdeas2.0)
(szöveg + chart képek) —, de nem SOL-fókuszú és nem friss. Legfeljebb
séma-referenciának jó, a mérés nem épülhet rá.


---

## Lezárás — 2026-08-20

### Amit elvégeztünk

| task | eredmény |
|---|---|
| t1 | Scraper PoC — a HTML út zsákutca, megvan a `/api/v1/ideas/` endpoint |
| t2 | Backfill: 741 szerző, 1913 kérés, 144 966 idea, 397 MB |
| t3 | Normalizálás: **17 061 BTC call**, szimbólum-whitelisttel |
| t4 | Forward return + drift-korrekció, 85 305 sor, 5 horizont |
| t5 | Empirical Bayes shrinkage + rolling OOS — `tau2` ≈ 0 |
| t6 | Placebo battery + FDR — **nulla túlélő** |
| t7 | Riport és döntés |

### A döntés helyes olvasata

A kutatás **nem** azt mondja, hogy senkinek nincs képessége — azt, hogy
**senki nem azonosítható**. Erő-elemzés: csak ~0,9%/call méretű valódi él
lett volna kimutatható; egy reális, szerény él (0,2%/call) láthatatlan
maradt volna. Ehhez szerzőnként több ezer call kellene; a legtermékenyebbnek
716 van, a mediánnak 7.

A gyakorlati cél szempontjából a kettő ugyanaz: ha nem tudod megmondani,
kire fogadj, a képesség léte akadémikus kérdés.

### Ami megmarad

- **17 061 BTC call** szöveggel és ground-truth iránnyal — kalibrációs set
  egy későbbi LLM-extraktorhoz (az `s102` második indoka teljesült)
- **`btcusdt` OHLCV store** — 4,7M perces gyertya 2017-től, spot
- **Forrásfüggetlen `calls` / `call_returns` séma**
