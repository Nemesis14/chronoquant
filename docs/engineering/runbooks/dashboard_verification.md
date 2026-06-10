# Runbook: Dashboard Verification

Use after data, model, or strategy config changes.

```python
import sys
sys.path.insert(0, "src")

from streamlit_app.data import load_dashboard_config

cfg = load_dashboard_config(asset_id="solusdt_fw60")
print(cfg["runtime_model_id"])
print(cfg["strategy_id"])
print(cfg["strategy"]["entry_threshold"])
```

Then run:

```bash
streamlit run src/streamlit_app/main.py
```

Check:

- active model ID;
- active strategy ID;
- latest prediction timestamp;
- backtest/report summary;
- logs and sync status.

