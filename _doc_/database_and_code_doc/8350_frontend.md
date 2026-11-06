# 8350 — `frontend/` (React UI)

Vite 5 + React 18 + TypeScript + Tailwind 3; Plotly (`plotly.js-dist-min` + `react-plotly.js`), `react-markdown` + `remark-gfm`, `react-router-dom` 6, `lucide-react`.
Dev: `npm run dev` (Vite :5173, `/api` és `/mcp` proxy -> `127.0.0.1:8600`, rewrite nélkül). Prod: `npm run build` (`tsc && vite build`) -> `frontend/dist`, amit az API szolgál ki.

Kapu: `cd frontend && npx tsc --noEmit && npm run build`.

## Oldalak (`src/pages`, `App.tsx` routing, `components/Layout.tsx`)

| Route | Oldal | Tartalom |
|-------|-------|----------|
| `/` | `DashboardPage` | Fejléc (symbol, DB állapot, utolsó gyertya kora, szinkron jelző, 24h/8h/4h ablak), `PredictionPriceChart`, `TradeTabs` (equity (`EquityChart`), lezárt tradek, journal, log (`LogPanel`)), `StrategyPanel`, `ModelsPanel`, `ActiveTradeCard`, `BinanceFillsPanel`, `SyncPanel` (job indítás, auto-sync, job log SSE) |
| `/reports` | `ReportsPage` | "Elemzések": modell/általános szűrő, riport lista (order szerint), olvasó (`ReportMarkdown` + lazy `ReportChart`/`ReportTable`); állapot a `?owner=&slug=` query-ben |
| `/chat` | `ChatPage` | Teljes oldalas chat; `?popout=1` = önálló ablak |

`ChatDock` (a Layoutban, a `/chat` útvonalon rejtve): lebegő gomb + jobb oldali csúszó panel; mountolva marad zárva is, hogy a futó válasz ne szakadjon meg; nyitott állapot `localStorage` (`cq_chat_dock_open`); popout: `lib/popout.ts` (`/chat?popout=1`, azonos origin: közös localStorage + BroadcastChannel).

## Könyvtárak

| Mappa | Tartalom |
|-------|----------|
| `src/api/` | `client.ts` (fetch wrapper, egységes hibatest `{detail, code}`), `dashboard.ts`, `reports.ts`, `chat.ts` (POST + SSE olvasás fetch ReadableStream-mel) |
| `src/lib/` | `usePolling.ts` (nem blokkoló polling: hiba/`db_busy` esetén az utolsó jó adat marad), `jobStream.ts` (EventSource `/api/jobs/{id}/events?from=`), `chatStore.ts` / `chatStream.ts` / `useChat.ts` (chat állapot és stream-feldolgozás), `format.ts`, `popout.ts` |
| `src/components/` | `charts/` (Plotly burkolók), `dashboard/`, `chat/` (`ChatPanel`, `ChatDock`, `ChatMarkdown`), `reports/` (`useInView.ts` lazy betöltés), `Layout.tsx` |
| `src/types/` | `api.ts`, `chat.ts`, `reports.ts` — az API kontraktus típusai |

Megjegyzés: a Plotly bundle az első chart mountig nem töltődik (lazy); a `/api/market/chart` lassú lehet (~6 s Binance fallback), ezért az adat frissítés közben is látható marad.

Szöveg: a UI magyar nyelvű; kódazonosítók és API mezők eredeti formában.
