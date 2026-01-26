import os
import sys
import time
import sqlite3
import threading
import queue

import pandas as pd
import matplotlib
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import tkinter as tk
from tkinter import ttk

matplotlib.use("TkAgg")

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import utils
from database_codes.sync_ohlcv import sync_ohlcv
from database_codes.features import sync_features
from database_codes.predictions import sync_predictions
from database_codes.pred_view import fetch_predictions_df, plot_predictions_df

# =============================================================================
# CONFIGURATION
# =============================================================================
POLL_SECONDS       = 60
SEPARATOR          = "=" * 80
INIT_START_DATE    = "2017-01-01 00:00:00"
LOOKBACK_MINUTES   = 240


def get_last_timestamp(db_path: str, table_name: str) -> str:
    try:
        with sqlite3.connect(db_path) as conn:
            result = pd.read_sql_query(
                f"SELECT MAX(open_time) as max_time FROM {table_name}",
                conn
            )
        max_time = result["max_time"].iloc[0]
        return max_time
    except Exception:
        return None


def truncate_log_if_configured(config: dict) -> None:
    try:
        logging_cfg = config.get("logging", {})
        log_file = logging_cfg.get("log_file")
        if not log_file:
            return
        if not os.path.isabs(log_file):
            try:
                repo_root = utils._repo_root()
                log_file = os.path.join(repo_root, log_file)
            except Exception:
                pass
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


class App:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.queue = queue.Queue()
        self.stop_event = threading.Event()

        self._build_ui()

        self.worker = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker.start()

        self.root.after(100, self._process_queue)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self) -> None:
        self.root.title("ChronoQuant")
        self.root.geometry("1100x700")

        main = ttk.Frame(self.root, padding=8)
        main.pack(fill="both", expand=True)

        log_frame = ttk.Frame(main)
        log_frame.pack(fill="both", expand=False)

        self.log_text = tk.Text(log_frame, height=18, wrap="word")
        self.log_text.pack(side="left", fill="both", expand=True)

        scroll = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scroll.pack(side="right", fill="y")
        self.log_text.configure(yscrollcommand=scroll.set)

        plot_frame = ttk.Frame(main)
        plot_frame.pack(fill="both", expand=True, pady=(8, 0))

        self.figure = Figure(figsize=(10, 4))
        self.ax = self.figure.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.figure, master=plot_frame)
        self.canvas.get_tk_widget().pack(fill="both", expand=True)

    def _process_queue(self) -> None:
        try:
            while True:
                kind, payload = self.queue.get_nowait()
                if kind == "clear":
                    self.log_text.delete("1.0", "end")
                    self.ax.clear()
                    self.canvas.draw()
                elif kind == "log":
                    self.log_text.insert("end", payload)
                    self.log_text.see("end")
                elif kind == "plot":
                    if payload is not None and not payload.empty:
                        plot_predictions_df(payload, self.ax)
                        self.canvas.draw()
        except queue.Empty:
            pass

        self.root.after(100, self._process_queue)

    def _worker_loop(self) -> None:
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = QueueWriter(self.queue)
        sys.stderr = QueueWriter(self.queue)

        try:
            config  = utils._load_config()
            db_path = config["database"]["db_path"]

            table_ohlcv = config["database"]["tables"]["ohlcv"]
            table_feat  = config["database"]["tables"]["features"]
            table_pred  = config["database"]["tables"]["predictions"]

            cycle = 1

            truncate_log_if_configured(config)

            while not self.stop_event.is_set():
                self.queue.put(("clear", None))

                try:
                    truncate_log_if_configured(config)

                    print(f"\n{SEPARATOR}")
                    print(f"Cycle #{cycle} at {utils.now_utc_str()}")
                    print(SEPARATOR)

                    print("OHLCV SECTION")
                    max_ohlcv = get_last_timestamp(db_path, table_ohlcv)

                    if max_ohlcv:
                        print(f"   Last: {max_ohlcv}")
                        start_ms = int(pd.to_datetime(max_ohlcv).timestamp() * 1000) + 60000
                    else:
                        print(f"   Table empty. Initializing from {INIT_START_DATE}...")
                        start_ms = int(pd.to_datetime(INIT_START_DATE).timestamp() * 1000)

                    sync_ohlcv(start_ms)

                    print("\nFEATURES SECTION")
                    max_feat      = get_last_timestamp(db_path, table_feat)
                    max_ohlcv_now = get_last_timestamp(db_path, table_ohlcv)

                    if max_ohlcv_now:
                        if max_feat is None:
                            start_feat = INIT_START_DATE
                            print(f"   Table empty. Initializing from {start_feat}...")
                            sync_features(start_feat)
                        elif max_feat < max_ohlcv_now:
                            start_feat = (pd.to_datetime(max_feat) + pd.Timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
                            print(f"   Last: {max_feat}")
                            sync_features(start_feat)
                        else:
                            print(f"   Last: {max_feat} (up to date)")
                    else:
                        print("   No OHLCV data yet. Skipping.")

                    print("\nPREDICTIONS SECTION")
                    max_pred     = get_last_timestamp(db_path, table_pred)
                    max_feat_now = get_last_timestamp(db_path, table_feat)

                    if max_feat_now:
                        if max_pred is None:
                            start_pred = INIT_START_DATE
                            print(f"   Table empty. Initializing from {start_pred}...")
                            sync_predictions(start_pred)
                        elif max_pred < max_feat_now:
                            start_pred = (pd.to_datetime(max_pred) + pd.Timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
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

                    print(f"\n{SEPARATOR}")
                    print(f"Cycle #{cycle} complete. Sleeping {POLL_SECONDS}s...")
                    print(SEPARATOR)
                except Exception as exc:
                    print(f"ERROR: {exc}")

                cycle += 1
                if self.stop_event.wait(POLL_SECONDS):
                    break
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr

    def _on_close(self) -> None:
        self.stop_event.set()
        self.root.destroy()


def main() -> None:
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
