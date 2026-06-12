
| # | Task | Scope | Megjegyzes |
|---|------|-------|-----------|
| 4 | Tablasema JSON registry | `db/table_ops.py`, uj config fajl | Explicit DDL definicio, generate-if-not-exists pattern |
| 5 | Concurrent write vedelem | `sync_features.py`, `table_ops.py` | `drop_existing + to_sql` race condition; atallas upsert-re vagy BEGIN EXCLUSIVE tranzakcio |
| 6 | DuckDB atallas elemzese | egesz `src/` | Elonyok: pandas integracios, analitikus query speed; Hatrany: concurrent write ugyanugy nem megoldott |
| 7 | Historikus adatok ujraletoltese es Parquet rebuild | `data/`, `scripts/` | SQLite archivalas utan: OHLCV Binance-bol teljesen ujra, features + predictions ujraszamolas; fuggo a DuckDB atallaastol |


-- claude api - agent ?!
Manage usage on claude.ai
What’s contributing to your limits usage?
Day
Week
Approximate, based on local sessions on this machine — does not include other devices or claude.ai
Last 24h · these are independent characteristics of your usage, not a breakdown
17% of your usage came from subagent-heavy sessions
Each subagent runs its own requests. Be deliberate about spawning them — and consider configuring a cheaper model for simpler subagents.
Skills
% of usage
/claude-api
4%