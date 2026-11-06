# models_doc — elemzések indexe

A Quarto/ipynb/HTML kimenet megszűnt (epic_068). Két forma létezik:

- **Újragenerálható riportok:** `src/reports` builderek írják.
  - Modell-riportok: `artifacts/<model_id>/reports/<slug>/` (pipeline `--step analyze`: 01_sampling, 02_feature_engineering, 03_hyperparameter_search, 04_strategy).
  - Általános riportok: `_doc_/reports/<slug>/` (jelenleg: `target_yearly_regime`, builder `src/reports/builders/target_regime.py`).
  - Megtekintés: UI Elemzések fül / `GET /api/reports`.
- **Rövid .md konklúziók** (régi, kutatási jellegű elemzések; mit vizsgáltunk, módszer, eredmény, döntés):

| Fájl | Téma |
|---|---|
| `target_yearly_regime.md` | target éves rezsim (riportként is újragenerálva) |
| `sampling_2101_2605.md` | régi champion sampling (kiváltva: 01_sampling riport) |
| `champion_search_2101_2605.md` | régi champion search (kiváltva: 03 riport) |
| `model_2021_train_valid.md` | archivált 2021-es éves modell |
| `cluster_regime_oos_validation.md` | klaszter-rezsim OOS validáció (t867) |
| `6500_cluster_regime_analysis_v2.md`, `6501_*.md`, `6502_*.md` | k=10 klaszter kutatás (Epic 065) |
| `signal_frequency_train_vs_live.md` | élő vs kalibrációs signal-gyakoriság diagnosztika |
