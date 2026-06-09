# ChronoQuant Backlog Board

Ez a board a `docs/plans/backlog/` alatti terveket rendezi tematikus
munkacsomagokba. A reszletes leiras minden tasknal a backlog fajlban van
linkelve.

## Modellezesi feladatok

| Task | Prioritas | Reszletes leiras | Megjegyzes |
|---|---:|---|---|
| Target definicio dokumentalasa: `NULL`, `0`, `1` jelentese | P2 | [live_target_edge_nulling_plan.md](docs/plans/backlog/live_target_edge_nulling_plan.md) | A jovobeli modellek adatertelmezesi hibajat elozi meg |
| SOL modellek ujraepitese futures OHLCV adaton (Google Colab) | P1 | — | A spot->futures OHLCV atallas utan a jelenlegi modell spot feature-okon tanult; futures feature-okon retrain kell. Colab search_lgbm.py + sweep_strategy.py workflow. |

## UI feladatok

| Task | Prioritas | Reszletes leiras | Megjegyzes |
|---|---:|---|---|
| Dashboard plot/trades arany modositasa: 80-20 -> 75-25 | P2 | — | A chart es a trades panel aranya legyen 75/25 (jelenleg 80/20). |

## Data pipeline / market data feladatok

| Task | Prioritas | Reszletes leiras | Megjegyzes |
|---|---:|---|---|
| Spot vs futures OHLCV adatforras szetvalasztasa es SOL runtime forras dontes | KESZ | [market_data_source_alignment_plan.md](docs/plans/backlog/market_data_source_alignment_plan.md) | In-place csere: solusdt_fw60 futures klines-ra atallitva. Modell retrain meg hatra. |
