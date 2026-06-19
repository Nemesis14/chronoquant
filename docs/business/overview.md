# Business Overview

ChronoQuant egy SOLUSDT futures-focused quantitative trading rendszer. A cel
nem altalanos arirany-joslas, hanem olyan nagyobb valoszinusegu mozgasok
azonositasa, amelyekbol szabalyalapu long/short strategia epitheto.

## Current Use Case

| Item | Current Scope |
|---|---|
| Asset | `SOLUSDT` futures |
| Bar interval | 1 minute |
| Prediction horizon | 60 minutes |
| Model family | LightGBM binary classifiers |
| Runtime UI | Streamlit dashboard |
| Evaluation | Threshold sweep and bar-by-bar backtest |
| Trading state | Local runtime state and journal storage |

## Business Questions

1. Mikor varhato eleg eros long iranyu mozgas a kovetkezo 60 percben?
2. Mikor varhato eleg eros short iranyu mozgas a kovetkezo 60 percben?
3. Milyen probability threshold alakit ezekbol hasznalhato trade signalt?
4. Stabil marad-e a trigger kulonbozo idoszakokban es untouched holdouton?
5. A live trader ugyanazt a dontesi logikat hajtja-e vegre, mint a backtest?

## Documentation Links

- Strategy summary: `trading_strategy.md`
- Risk assumptions: `risk_and_assumptions.md`
- Terms: `glossary.md`
- Model evidence: `../modeling/model_cards/`
- Backtest evidence: `../evaluation/reports/`
