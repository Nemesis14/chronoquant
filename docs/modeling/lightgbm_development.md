# LightGBM Development Workflow

This legacy guide was retired after the yearly-modeling refactor.

Use the current `_doc_` pages instead:

- `_doc_/5000_modelling.md` — active modeling pipeline overview
- `_doc_/5010_sampling_yearly.md` — yearly random-hour sample methodology
- `_doc_/2010_feature_engineering.md` — feature engineering and feature set output
- `_doc_/5500_hyper_param_search.md` — LightGBM hyperparameter search

Current model artifacts live under `artifacts/<model_id>/`, not `models/<model_id>/`.
The active model ID format is `lgbm_{asset}_{direction}_fw{horizon}_q{quantile}_{year}`.
