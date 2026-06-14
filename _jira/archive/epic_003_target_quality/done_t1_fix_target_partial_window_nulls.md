---
epic: epic_003_target_quality
id: t1
title: Target partial-window NULL javítása — utolsó horizon-1 sor NULL kell legyen
assignee: database_agent
status: pr
blocks: [t2, t3]
blocked_by: []
---

## Goal

A `sync_targets.py` DuckDB window funkciója (`ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING`)
partial window esetén IS visszaad értéket — pl. az utolsó előtti sorban a
forward_max_close az egyetlen következő sor close értéke lesz, holott
60 bár kellene hozzá. Ez hamis pozitív/negatív labeleket generál a
dataset végén.

**Fix:** Azon soroknál, ahol a forward window nem teljes (kevesebb mint
`horizon` következő bar érhető el), a target érték NULL kell legyen.

A DuckDB `COUNT(*) OVER (ROWS BETWEEN 1 FOLLOWING AND {horizon} FOLLOWING)`
visszaadja az elérhető következő sorok számát — ezzel szűrhető a részleges ablak.

## Scope

- `src/data_pipeline/sync_targets.py` — `_TARGET_SQL` és `_compute_target_df` módosítása

### Javítás iránya a SQL-ben

```sql
forward_extrema AS (
    SELECT
        open_time,
        close,
        MAX(close) OVER (ORDER BY open_time ROWS BETWEEN 1 FOLLOWING AND {horizon} FOLLOWING) AS future_max_close,
        MIN(close) OVER (ORDER BY open_time ROWS BETWEEN 1 FOLLOWING AND {horizon} FOLLOWING) AS future_min_close,
        COUNT(*) OVER (ORDER BY open_time ROWS BETWEEN 1 FOLLOWING AND {horizon} FOLLOWING) AS future_bar_count
    FROM ohlcv_ordered
),
-- future_max_close / future_min_close → NULL ahol future_bar_count < horizon
```

Majd a `returns` CTE-ben: `NULLIF`-szerű feltétel a `future_bar_count < {horizon}` esetére.

## Acceptance Criteria

- [ ] `target` táblában az utolsó `horizon - 1` sorban mindkét target NULL
- [ ] Pontosan `horizon - 1` db NULL sor van a `target` táblában (jelenlegi: 1)
- [ ] A többi sor értéke nem változik (csak a window vége érintett)
- [ ] Quantile threshold számítás CSAK a teljes window-ú sorokból történik (már így van, `drop_nulls()`)
- [ ] `sync_targets` rebuild lefut hibátlanul
- [ ] `uv run pytest _tests/store/ -v` — zöld

## Notes

Vizsgálat (2026-06-14): `SELECT trg_l_fw60_q90, COUNT(*) FROM target GROUP BY 1`
eredmény: `{False: 2720574, True: 302286, None: 1}` — csak 1 NULL sor van,
holott fw60 horizonnál 59 sornak kellene NULL-nak lennie.
A threshold számítás már `drop_nulls()`-szal dolgozik, tehát az csak az értékes sorokat nézi.
