# OHLCV Table Analysis Spec

## Table

`ohlcv`

Primary key: `open_time`

Expected granularity:

- 1 minute bars
- UTC timestamps
- monotonic ascending timeline
- no duplicate `open_time`
- no missing bars unless explicitly explained

Core columns:

- `open_time`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `quote_volume`
- `trades`
- `taker_buy_base`
- `taker_buy_quote`

## Purpose

This table is the raw market-data source of truth for all downstream target, feature, model, and prediction analysis.

The analyst must treat `ohlcv` as immutable market event data. Any issue here can invalidate all downstream tables.

## Required Checks

### 1. Time coverage

Report:

- first `open_time`
- last `open_time`
- total row count
- expected row count for continuous 1-minute data
- missing minute count
- duplicate `open_time` count
- maximum gap length
- top 20 largest gaps

Suggested SQL:

```sql
WITH ordered AS (
    SELECT
        open_time,
        LEAD(open_time) OVER (ORDER BY open_time) AS next_open_time
    FROM ohlcv
),
gaps AS (
    SELECT
        open_time,
        next_open_time,
        date_diff('minute', open_time, next_open_time) AS gap_minutes
    FROM ordered
    WHERE next_open_time IS NOT NULL
)
SELECT *
FROM gaps
WHERE gap_minutes > 1
ORDER BY gap_minutes DESC
LIMIT 20;
```

### 2. Candle validity

Check every row:

- `high >= open`
- `high >= close`
- `high >= low`
- `low <= open`
- `low <= close`
- `open > 0`
- `high > 0`
- `low > 0`
- `close > 0`
- `volume >= 0`
- `quote_volume >= 0`
- `trades >= 0`
- `taker_buy_base >= 0`
- `taker_buy_quote >= 0`
- `taker_buy_base <= volume`, if comparable
- `taker_buy_quote <= quote_volume`, if comparable

Flag impossible candles, zero-price candles, negative activity, and suspicious stale candles.

### 3. Return and volatility sanity

Compute:

- 1-bar close-to-close log return
- absolute return distribution
- rolling realized volatility over 60, 240, 1440 bars
- top return outliers
- top volume spikes
- price jumps around missing gaps

Report distribution statistics and inspect whether outliers are real market moves or bad data.

### 4. Market microstructure/activity sanity

Inspect:

- `volume`
- `quote_volume`
- `trades`
- `taker_buy_base`
- `taker_buy_quote`

Report null counts, zero counts, monthly/yearly distributions, volume/trade correlations, average trade size proxy, and taker buy ratios.

Flag structural breaks, impossible ratios, long zero-volume runs, and missing activity fields.

### 5. Downstream alignment readiness

Compare `open_time` coverage with:

- `target`
- `feat_ohlcv_quant`
- `predictions`

Report rows missing in downstream tables, downstream rows not in `ohlcv`, and common timestamp ranges.

### 6. Regime segmentation

Summarize by:

- year
- month
- volatility regime
- volume regime
- session bucket if available

Purpose: identify distribution shift and regime changes.

## Required Notebook Outputs

1. Summary table with row count, min/max time, gap count, duplicate count.
2. Candle validity table.
3. Missing-gap table.
4. Return distribution table.
5. Top 20 extreme returns with surrounding timestamps.
6. Volume/activity distribution.
7. Regime summaries by year/month.
8. Cross-table coverage comparison.

## Critical Findings

Mark as critical if any of these occur:

- duplicate `open_time`
- impossible candle geometry
- negative price or volume
- downstream rows without matching OHLCV row
- large unexplained time gaps
- severe return outliers caused by bad data
- activity columns with structural breaks
