# TradingView piaci hangulat riport (tv_report) — chat funkció

> **Státusz: ÉL** (t896: `src/sentiment/tv_report/`, MCP `sentiment_latest`/`sentiment_refresh`, worker job `sentiment_refresh`; kimenet `data/tv_report/`). Ez nem lezárt kutatás, hanem a chat „piaci összkép" képességének motorja.

## Mit csinál
A TradingView Ideas BTC-ideáiból hangulat-összképet ad a Claude chat számára.

## Módszer
- `fetch.py` (régi `fetch_report_data.py`): ideák lekérése a `collectors.tradingview.IdeasClient`-tel (`--hours`, `--pages`), BTC-szűrés, dedup (`state.json`), megtisztított `latest.json` + nyers `runs/<ts>/BTCUSD_p*.json`. Nincs AI-lépés.
- `author_backtest.py` (`python -m sentiment.tv_report.author_backtest`; `discover` / `run`): szerzői call-ok regex-kinyerése és Binance 1h OHLCV-vel visszamérés (percekig-félórákig fut).
- Az értékelést (irány, szint, minőség, összegzés, hírek WebSearch-csel) maga a chat végzi a `.agent/skills/tv_sentiment_skill.md` és a persona prompt szerint. Nincs LLM-pontozó pipeline, nincs chart-elemzés, nincs ütemezés.

## Döntés
A két működő script és a collector marad. A kimenetek (`latest.json`, `state.json`, `runs/` ~264 MB nyers lapok és OHLCV-cache, ~50 `scratch_*.txt`, `*_latest*.json`) törölve (t886), a scriptek újragenerálják őket. A szerző-szintű követés kutatási eredménye nemleges: [tradingview_author_skill.md](tradingview_author_skill.md).

## Kód
A scriptek korábban git-ignoráltak voltak, sosem kerültek commitba; a t896 óta `src/sentiment/tv_report/` alatt verziózottak.

## Eredeti terv (kivonat a törölt `PLAN_tv_market_sentiment.md`-ből, t890)
A tervet manuális, ismételt TradingView-elemzések (2026-09) alapozták meg; a gyökér-draft megvalósult (`src/sentiment/tv_report/`), a teljes szöveg a git historyban (`73f2156`).
- **Nyers vs minőség-szűrt irány-megoszlás**: a két szám gyakran eltér, sőt ellentétes (mennyiségileg long-túlsúly, minőség szerint short-túlsúly); a szűrt a fontosabb. Ha nincs konszenzus, azt is ki kell mondani (a kiegyensúlyozottság önmagában jel).
- **Konszenzus-szintek**: a több szerző által egybehangzóan említett támasz/ellenállás az erős jel; rövid- és hosszútávú szintek külön kezelendők.
- **Minőség-jelzők**: ismétlődő/spam szerzők (üres chart, csak cím), újraposztolt régi chartok (a cikk ára nem egyezik a spot árral), érdekütközés (fizetett influencer, jel-értékesítés), bukott setupok (azonos szerző korábbi posztja stoplosst ütött).
- **Technikai tanulság**: a TradingView Ideas listához nincs hivatalos API (JS-renderelt lista), az egyedi cikkoldal nyers HTTP-ből is kinyerhető (`og:image`); relatív időbélyegek ("Xh ago") abszolút időre parse-olandók, a duplikáció (azonos szerző, sablon-cím) kezelendő.
- **Költség-kérdés**: a háttérben futó folyamat nem éri el a Claude Code interaktív eszközeit (WebFetch, képolvasás); ezért az értékelést a chat végzi, a script csak adatot gyűjt.
