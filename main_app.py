import tkinter as tk

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
