# 8320 — `src/cq_mcp` (chronoquant MCP szerver)

FastMCP (streamable HTTP), az API processzben fut: `/mcp/chronoquant` (`api.main` route, nincs 307 slash-redirect). Stdio-n is indítható:
`uv run python -m cq_mcp.server`. A chat (`claude` CLI) ezen át éri el az adatokat; szerver-név: `chronoquant`.

A tool-függvények szinkronok, `_threaded` wrapper futtatja őket worker szálon. Kimenet: JSON szöveg (`common.dump`: NaN -> null, max 50 000 karakter, `[TRUNCATED]` jelzéssel); sorok max 1000.

## Tool-lista (18)

| Tool | Modul | Típus | Leírás |
|------|-------|-------|--------|
| `get_status` | tools_models | olvasó | Adatfrissesség, aktív modellek, worker és sync állapot |
| `list_tables` | tools_data | olvasó | Táblák és oszlopok (`catalog`: live\|lab\|reg, `filter`) |
| `query_sql` | tools_data | olvasó | Egy csak-olvasó SELECT; aliasok `live.`, `lab.` (`lab.strat.*`), `reg.`; max 1000 sor |
| `get_candles` | tools_data | olvasó | OHLCV, `hours` 1..168, `tf` 1m\|5m\|15m |
| `get_predictions` | tools_data | olvasó | Predikciók (rank percentil + nyers score), `hours` 1..72 |
| `get_latest_signal` | tools_data | olvasó | Legfrissebb predikció és állapota az aktív entry cutoffokhoz képest |
| `get_active_strategy` | tools_models | olvasó | Aktív long/short stratégia kártyák |
| `get_trades` | tools_models | olvasó | `source`: strat (backtest ledger) \| journal \| binance; `limit` |
| `get_equity` | tools_models | olvasó | Equity pontok + összegzés (`session`, `direction`) |
| `list_models` | tools_models | olvasó | Registry modellek (státusz szűrő) |
| `get_model` | tools_models | olvasó | Registry sor, manifest, params, best search, artifact lista |
| `get_registry` | tools_models | olvasó | Whitelist registry tábla (`table`, `filter`, `limit`) |
| `list_reports` | tools_reports | olvasó | Riport manifestek (`owner`: model_id vagy `general`) |
| `read_report` | tools_reports | olvasó | Riport markdown + chart címek; `include_charts=true` a Plotly JSON-t is |
| `list_jobs` | tools_jobs | olvasó | Worker jobok (státusz szűrő, `limit`) |
| `enqueue_sync` | tools_jobs | **író** | `sync_full` job enqueue (dedup) |
| `sentiment_latest` | tools_sentiment | olvasó | A legutóbbi TradingView BTC ötlet-riport kivonata (`max_ideas`, `desc_chars`) |
| `sentiment_refresh` | tools_sentiment | **író** | `sentiment_refresh` worker job (`hours`, `pages`; ~30-60 s; utána `list_jobs`, majd `sentiment_latest`) |

## Jogosultság

- Csak két tool "ír", és mindkettő kizárólag worker jobot ütemez (DuckDB-t nem ír): `enqueue_sync`, `sentiment_refresh`.
- A chat többi eszköze: Read, WebSearch, WebFetch; Bash/Edit/Write tiltott (lásd [8300_api.md](8300_api.md)).
- Új író tool bővítése jogosultsági döntés: frissítsd a `server.INSTRUCTIONS` szöveget, a persona promptot és ezt az oldalt.

## Tesztek

`uv run pytest src/cq_mcp` (`test_tools.py`).
