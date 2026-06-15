"""Root test configuration — adds src/ to sys.path for all test modules."""

import sys
from pathlib import Path

# Ensure src/ is on the path so tests can import database.store, database.data_pipeline, etc.
_src = Path(__file__).resolve().parents[1]
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))
