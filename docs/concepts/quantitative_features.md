# Quantitative Feature Concepts

Feature columns describe market state available at prediction time. The concrete
generated feature list is controlled by `config/features.json`; the data
dictionary lives in `docs/data/dictionary/features.md`.

## Feature Families

| Family | Purpose |
|---|---|
| Momentum | Captures speed and direction of price movement |
| Trend | Captures relation to moving averages and trend filters |
| Volatility | Captures range expansion/compression and dispersion |
| Volume/activity | Captures participation and taker-flow context |
| Price action | Captures candle and return shape |
| Market structure | Captures local highs/lows and swing behavior |
| Regime/rank | Captures relative context inside rolling windows |
| Pattern/Elliott | Captures detected technical pattern context |

## Safety Requirement

Every feature used by a live model must be computable with information available
at or before the prediction timestamp. Delayed-confirmation features must use
confirmation time, not the historical pivot time.

