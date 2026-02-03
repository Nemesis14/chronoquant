import os
import sys
import time
import sqlite3
import threading
import queue
import traceback

import pandas as pd

import utils
from app import settings
from database_codes.sync_ohlcv import sync_ohlcv
from database_codes.features import sync_features
from database_codes.predictions import sync_predictions
from database_codes.pred_view import fetch_predictions_df

# =============================================================================
# WORKER LOOP: runs the sync/predict cycle and streams output via a queue
# =============================================================================


def get_last_timestamp(db_path: str, table_name: str) -> str:
    try:
        with sqlite3.connect(db_path) as conn:
            result = pd.read_sql_query(
                f"SELECT MAX(open_time) as max_time FROM {table_name}",
                conn
            )
        return result["max_time"].iloc[0]
    except Exception:
        return None


def truncate_log_if_configured(config: dict) -> None:
    try:
        logging_cfg = config.get("logging", {})
        log_file = logging_cfg.get("log_file")
        if not log_file:
            return
        if not os.path.isabs(log_file):
            log_file = os.path.join(utils._app_root(), log_file)
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir, exist_ok=True)
            except Exception:
                pass
        with open(log_file, "w", encoding="utf-8") as f:
            f.write("")
    except Exception:
        pass


class QueueWriter:
    def __init__(self, q: queue.Queue) -> None:
        self.q = q

    def write(self, text: str) -> None:
        if text:
            self.q.put(("log", text))

    def flush(self) -> None:
        return


class Worker:
    def __init__(self, q: queue.Queue, stop_event: threading.Event) -> None:
        self.queue = q
        self.stop_event = stop_event
        self.thread = None

    def start(self) -> None:
        if self.thread is None:
            self.thread = threading.Thread(target=self._run, daemon=True)
            self.thread.start()

    def _run(self) -> None:
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = QueueWriter(self.queue)
        sys.stderr = QueueWriter(self.queue)

        try:
            db_cfg = utils.load_db_config()
            db_path = db_cfg["database"]["db_path"]

            table_ohlcv = db_cfg["database"]["tables"]["ohlcv"]
            table_feat = db_cfg["database"]["tables"]["features"]
            table_pred = db_cfg["database"]["tables"]["predictions"]

            cycle = 1

            truncate_log_if_configured(db_cfg)

            while not self.stop_event.is_set():
                self.queue.put(("clear", None))

                try:
                    truncate_log_if_configured(db_cfg)

                    print(f"\n{settings.SEPARATOR}")
                    print(f"Cycle #{cycle} at {utils.now_utc_str()}")
                    print(settings.SEPARATOR)

                    print("OHLCV SECTION")
                    max_ohlcv = get_last_timestamp(db_path, table_ohlcv)

                    if max_ohlcv:
                        print(f"   Last: {max_ohlcv}")
                        start_ms = int(pd.to_datetime(max_ohlcv, utc=True).timestamp() * 1000) + 60000
                    else:
                        print(f"   Table empty. Initializing from {settings.INIT_START_DATE}...")
                        start_ms = int(pd.to_datetime(settings.INIT_START_DATE, utc=True).timestamp() * 1000)

                    sync_ohlcv(start_ms)

                    print("\nFEATURES SECTION")
                    max_feat = get_last_timestamp(db_path, table_feat)
                    max_ohlcv_now = get_last_timestamp(db_path, table_ohlcv)

                    if max_ohlcv_now:
                        if max_feat is None:
                            start_feat = settings.INIT_START_DATE
                            print(f"   Table empty. Initializing from {start_feat}...")
                            sync_features(start_feat)
                        elif max_feat < max_ohlcv_now:
                            start_feat = (pd.to_datetime(max_feat, utc=True) + pd.Timedelta(minutes=1)).strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                            print(f"   Last: {max_feat}")
                            sync_features(start_feat)
                        else:
                            print(f"   Last: {max_feat} (up to date)")
                    else:
                        print("   No OHLCV data yet. Skipping.")

                    print("\nPREDICTIONS SECTION")
                    max_pred = get_last_timestamp(db_path, table_pred)
                    max_feat_now = get_last_timestamp(db_path, table_feat)

                    if max_feat_now:
                        if max_pred is None:
                            start_pred = settings.INIT_START_DATE
                            print(f"   Table empty. Initializing from {start_pred}...")
                            sync_predictions(start_pred)
                        elif max_pred < max_feat_now:
                            start_pred = (pd.to_datetime(max_pred, utc=True) + pd.Timedelta(minutes=1)).strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                            print(f"   Last: {max_pred}")
                            sync_predictions(start_pred)
                        else:
                            print(f"   Last: {max_pred} (up to date)")
                    else:
                        print("   No Features data yet. Skipping.")

                    print("\nVIEW SECTION")
                    df = fetch_predictions_df(print_status=True)
                    if df is not None and not df.empty:
                        self.queue.put(("plot", df))
                    else:
                        self.queue.put(("plot", None))

                    print(f"\n{settings.SEPARATOR}")
                    print(f"Cycle #{cycle} complete. Sleeping {settings.POLL_SECONDS}s...")
                    print(settings.SEPARATOR)
                except Exception:
                    print("ERROR: Execution failed")
                    print(traceback.format_exc())

                cycle += 1
                if self.stop_event.wait(settings.POLL_SECONDS):
                    break
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
