# 8350 — `frontend/` (React UI)

Vite 5 + React 18 + TypeScript + Tailwind 3; Plotly (`plotly.js-dist-min` + `react-plotly.js`), `react-markdown` + `remark-gfm`, `react-router-dom` 6, `lucide-react`.
Dev: `npm run dev` (Vite :5173, `/api` és `/mcp` proxy -> `127.0.0.1:8600`, rewrite nélkül; a cél a `CQ_API` env-vel felülírható, pl. egy második, teszt API példányra: `CQ_API=http://127.0.0.1:8601 npx vite --port 5174`). Prod: `npm run build` (`tsc && vite build`) -> `frontend/dist`, amit az API szolgál ki — API restart nélkül, a fájlokat kérésenként lemezről olvassa; az `index.html` `Cache-Control: no-cache`-sel megy (t948), így navigáláskor is az új build töltődik (e header nélkül a böngésző heurisztikusan cache-elte a régi shellt, csak F5 hozta az újat).

Kapu: `cd frontend && npx tsc --noEmit && npm run build`.

## Oldalak (`src/pages`, `App.tsx` routing, `components/Layout.tsx`)

| Route | Oldal | Tartalom |
|-------|-------|----------|
| `/` -> `/a/:asset` | `DashboardPage` (asset-fülsor a Layoutban (`AssetTabs`), sorrend = `/api/assets` = `config/assets.json` `display_order`: BTC, BCH, SOL, ETH; az asset a route-ból jön, `lib/asset.tsx` `useAsset()`, minden API hívás `?asset=`; asset váltáskor remount; utolsó asset: localStorage `cq_last_asset`) | Fejléc (symbol, DB állapot, utolsó gyertya kora, szinkron jelző, 24h/8h/4h ablak), `PredictionPriceChart` (+ Scout réteg, kapcsoló a chart fejlécében), Scout esemény-grafikon (ha esemény kiválasztva), `TradeTabs` (equity (`EquityChart`), lezárt tradek, journal, log (`LogPanel`)), `StrategyPanel`, `ModelsPanel`, `ActiveTradeCard`, `BinanceFillsPanel`, `SyncPanel` (job indítás, auto-sync, job log SSE), `ScoutPanel` (a SyncPanel alatt). **Quant modell nélküli asset** (pl. ETH; `status.models.long/short.model_id` mind `null`, és `status.live` nem `null` — DB busy esetén a quant nézet marad): „Nincs quant modell” sáv, a `PredictionPriceChart` `quant={false}` → csak gyertya (nincs long/short pct sor), a `StrategyPanel` és `ActiveTradeCard` rejtve; Scout, Sync, Modellek/adat, TradeTabs marad (t946) |
| `/reports` | `ReportsPage` | "Elemzések": modell/általános szűrő, asset szűrő (`?asset=`, a riport `asset_id`-ja a modell manifestből) + riport lista (order szerint), olvasó (`ReportMarkdown` + lazy `ReportChart`/`ReportTable`); állapot a `?owner=&slug=` query-ben |
| `/chat` | `ChatPage` | Teljes oldalas chat; `?popout=1` = önálló ablak |

`ChatDock` (a Layoutban, a `/chat` útvonalon rejtve): lebegő gomb + jobb oldali csúszó panel; mountolva marad zárva is, hogy a futó válasz ne szakadjon meg; nyitott állapot `localStorage` (`cq_chat_dock_open`); popout: `lib/popout.ts` (`/chat?popout=1`, azonos origin: közös localStorage + BroadcastChannel). A lebegő gomb helyét a Layout `<main>` `.cq-dock-clear` osztálya (`index.css`, t948) tartja szabadon (csak ahol a dock látszik — a `/chat` oldalon nem, mert ott a `ChatPage` 100vh-hoz méretez): jobb gutter = 76 px mínusz a centrált `max-w-[1800px]` margója (min. 1rem; ~1936 px szélességtől 0 többlet) + 80 px alsó padding, így a gomb semmilyen panelt nem takar (pl. a Scout panel R2_03 oszlopát / szabály-lábjegyzetét keskenyebb kijelzőn); a gomb méretének/pozíciójának változásakor a kettőt együtt kell módosítani.

## Impulse Scout réteg és panel (epic_074 / t943)

Kísérleti, NO-GO modell (5300, [5800](5800_scout_runtime.md)) — a UI mindenhol „Kísérleti – nem kereskedési jel” jelölést mutat (`meta.experimental`). Adat: egyetlen `getScoutLatest(asset, 20)` poll (60 s) a `DashboardPage`-ben, ezt kapja a chart-réteg, a panel és az esemény-grafikon.

| Elem | Fájl | Tartalom |
|------|------|----------|
| Chart-réteg | `lib/scout.ts` `scoutOverlay()`, `PredictionPriceChart` (`scout`, `selectedId` prop) | a fókuszablakot átfedő események: ▲ lila jelölő a mélyponton (`t0`, `low_t0`), ● a megerősítésnél (`t_c`, `close_tc`, állapotszín), pontozott P* (zöld) és stop (piros) vonal + halvány sáv `t_c → label_end` (H = 8 h), kis címke `Scout P(imp) · percentilis · top · állapot`; a P*/stop szint az ár-tengely tartományát is bővíti. Kapcsoló: „Scout réteg” gomb (localStorage `cq_scout_layer`, alap be); csak `available` asseten látszik |
| Állapotszín | `lib/scout.ts` `eventColor()` | nyitott = kék, lezárt = a realizált osztály (impulzus zöld, hamis indulás piros, oldalazás szürke), érvénytelen = szürke; mindig szöveg/ikon is mellette (✓ TP, ✗ SL, ◷ lejárt, ∅ érvénytelen) |
| Panel | `components/dashboard/ScoutPanel.tsx` | kísérleti badge + modell meta (model_id, OOF AUC, live_start), számlálók, **nyitott esemény(ek) kiemelve** (kék keret): P(impulzus), percentilis, top-20/10 % badge + küszöbök, 3-osztályos valószínűség-sáv, t0/t_c/entry/ablak vége, kereskedési terv mindkét szabályra (`default`: P*/mélypont; `r2_03`: +3σ/−1,5σ — TP/SL ár és %-távolság az entrytől, kimenet, nettó bp); legutóbbi 12 esemény táblája (kattintás = részletek + esemény-grafikon); BCH (`available=false`): „nem elérhető”; régi API (404): „API újraindítás után aktív” |
| Esemény-grafikon | `components/charts/ScoutEventChart.tsx` | a kiválasztott esemény ablaka `[t0 − 2h, label_end + 1h]`, 5m gyertyák (`getCandles(asset, {hours, tf, end})` → `/api/market/candles?end=`), Scout réteggel; nyitott eseménynél „most” vonal és 60 s frissítés — így a 24 h ablakon kívüli (régebbi) események is megnézhetők |

Asset-váltáskor a kiválasztás törlődik; a poll asset-függő (SOL/BTC/ETH Scout asset, BCH nem).

## Könyvtárak

| Mappa | Tartalom |
|-------|----------|
| `src/api/` | `client.ts` (fetch wrapper, egységes hibatest `{detail, code}`), `dashboard.ts` (`getCandles` is), `scout.ts` (Scout végpontok), `reports.ts`, `chat.ts` (POST + SSE olvasás fetch ReadableStream-mel) |
| `src/lib/` | `usePolling.ts` (nem blokkoló polling: hiba/`db_busy` esetén az utolsó jó adat marad), `jobStream.ts` (EventSource `/api/jobs/{id}/events?from=`), `chatStore.ts` / `chatStream.ts` / `useChat.ts` (chat állapot és stream-feldolgozás), `format.ts`, `popout.ts`, `scout.ts` (Scout címkék, színek, `scoutOverlay`) |
| `src/components/` | `charts/` (Plotly burkolók), `dashboard/`, `chat/` (`ChatPanel`, `ChatDock`, `ChatMarkdown`), `reports/` (`useInView.ts` lazy betöltés), `Layout.tsx` |
| `src/types/` | `api.ts`, `chat.ts`, `reports.ts` — az API kontraktus típusai |

Megjegyzés: a Plotly bundle az első chart mountig nem töltődik (lazy); a `/api/market/chart` lassú lehet (~6 s Binance fallback), ezért az adat frissítés közben is látható marad.

Szöveg: a UI magyar nyelvű; kódazonosítók és API mezők eredeti formában.
