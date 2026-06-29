# pr_t1 — feature_engineering_scatter.ipynb újraírása

**Epic:** epic_042_feature_iv_scatter  
**Assignee:** analyst_agent  
**Status:** pr  
**Created:** 2026-06-24

## Leírás

Teljesen újraírva: `artifacts/lgbm_solusdt_l_fw60_2101_2605/feature_engineering/feature_engineering_scatter.ipynb`

## Elvégzett munka

- Új 7-celles notebook létrehozva a specifikáció szerint
- Adatforrás: DuckDB (`snap.solusdt_fw60_2101_2605__21668185` JOIN `model.lgbm_solusdt_l_fw60_2101_2605__sample`)
- `ContinuousOptimalBinning` (max 10 bin) 208 feature-re előszámolva (208/208 OK)
- Tabset szekciók: 16 csoport (Accel, Autocorrelation, Candle Pattern, Donchian, Gap, Ichimoku,
  Interaction, Market Structure, Momentum, Price Action, Regime Rank, Return Distance,
  Tail Risk, Time/Session, Trend, Trend Slope, Volatility, Volume)
- Minden feature-re: bin scatter plot (matplotlib, base64 PNG embed) + regressziós egyenes
- Összefoglaló IV tábla csoportonként rendezve
- Quarto render sikeres: HTML 18.7 MB, 208 base64 PNG kép, minden panel-tabset renderelve

## Fájlok

- `artifacts/lgbm_solusdt_l_fw60_2101_2605/feature_engineering/feature_engineering_scatter.ipynb`
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/feature_engineering/feature_engineering_scatter.html`

## Notes

- `sample_train_valid.parquet` nem létezik az artifact mappában — DuckDB-t használ (mint az előző notebook)
- `ax.set_title()` megtartva a tabset scatter plotokban (a spec expliciten előírta), de ez Quarto-ban
  nem jelent problémát, mert a plotok base64 PNG-ként vannak beágyazva, nem Quarto fig-cap-ként
