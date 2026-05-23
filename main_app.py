import os
import sys
import tkinter as tk

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from app.ui import App

# =============================================================================
# ENTRYPOINT: keep this file minimal and stable for PyInstaller
# =============================================================================


def main() -> None:
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()

