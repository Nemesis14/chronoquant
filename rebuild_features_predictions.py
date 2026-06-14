"""KAN-57: Full features + predictions rebuild on clean Hive OHLCV.

Drops existing features/predictions partitions then rebuilds from OHLCV.
Uses ohlcv_hive_latest_open_time (reads last file only) to avoid scanning
all 50k+ Hive OHLCV files for MAX(open_time).
"""

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("rebuild_features_predictions.log", encoding="utf-8"),
    ]
)

sys.path.insert(0, "src")
import utils
from store.parquet_store import ohlcv_hive_latest_open_time
from store.duckdb_query import row_count
from store.maintenance import rebuild_derived_tables

db_cfg = utils.load_asset_config()
data_dir = db_cfg["database"]["data_dir"]

logging.info("=== KAN-57 FEATURES + PREDICTIONS REBUILD START ===")
logging.info("data_dir=%s", data_dir)

# Use known OHLCV start (SOL listing) and scan only the last Hive file for end
OHLCV_START = "2020-09-14 07:00:00"

latest_ts = ohlcv_hive_latest_open_time(data_dir)
if latest_ts is None:
    logging.error("Nincsenek OHLCV Hive fajlok! Rebuild megszakitva.")
    sys.exit(1)

ohlcv_end = latest_ts.strftime("%Y-%m-%d %H:%M:%S")
logging.info("OHLCV range: %s -> %s", OHLCV_START, ohlcv_end)

from store.duckdb_query import query_range as _qr
_feat_latest = _qr(data_dir, "features", columns=["open_time"])
if not _feat_latest.empty:
    resume_start = _feat_latest["open_time"].max()
    logging.info("Resume: legutolso feature sor=%s, onnan folytatjuk", resume_start)
    drop = False
else:
    resume_start = OHLCV_START
    drop = True

logging.info("Rebuild indul: start=%s end=%s drop=%s chunk_months=6", resume_start, ohlcv_end, drop)
rebuild_derived_tables(
    start=str(resume_start),
    end=ohlcv_end,
    drop=drop,
    chunk_months=6,
)

feat_rows = row_count(data_dir, "features")
pred_rows = row_count(data_dir, "predictions")
logging.info("=== KAN-57 REBUILD DONE === features=%d  predictions=%d", feat_rows, pred_rows)
