import queue
import threading

import matplotlib
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import tkinter as tk
from tkinter import ttk

from app import settings
from app.worker import Worker
from plotting.prediction_view import plot_predictions_df

matplotlib.use("TkAgg")

# =============================================================================
# UI: Tkinter window that shows log output and the latest prediction plot
# =============================================================================


class App:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.queue = queue.Queue()
        self.stop_event = threading.Event()

        self._build_ui()

        self.worker = Worker(self.queue, self.stop_event)
        self.worker.start()

        self.root.after(100, self._process_queue)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self) -> None:
        self.root.title(settings.UI_TITLE)
        self.root.geometry(settings.UI_GEOMETRY)

        main = ttk.Frame(self.root, padding=8)
        main.pack(fill="both", expand=True)

        # Log panel
        log_frame = ttk.Frame(main)
        log_frame.pack(fill="both", expand=False)

        self.log_text = tk.Text(log_frame, height=settings.LOG_HEIGHT, wrap="word")
        self.log_text.pack(side="left", fill="both", expand=True)

        scroll = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scroll.pack(side="right", fill="y")
        self.log_text.configure(yscrollcommand=scroll.set)

        # Plot panel
        plot_frame = ttk.Frame(main)
        plot_frame.pack(fill="both", expand=True, pady=(8, 0))

        self.figure = Figure(figsize=settings.PLOT_FIGSIZE)
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

    def _on_close(self) -> None:
        self.stop_event.set()
        self.root.destroy()

