#!/usr/bin/env python3
# =============================================================================
# Legfrissebb 1-2-1-2 setup vizualizáció — 5m japángyertya chart
# =============================================================================

import sys
from pathlib import Path

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import seaborn as sns

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from elliott_waves.elliott_1212 import detect_1212, load_ohlcv, resample_ohlcv

DB_PATH   = "D:/repos/chronoquant/database/solusdt_data_dev.db"
TABLE     = "solusdt_1m"
CONTEXT_LEFT  = 20   # extra gyertya a setup előtt
CONTEXT_RIGHT = 50   # extra gyertya a P4 conf. bar után

# -------------------------------------------------------------------------
# Adatok betöltése és detektor futtatása
# -------------------------------------------------------------------------
print("Betöltés...")
df_1m = load_ohlcv(DB_PATH, TABLE)
df5   = resample_ohlcv(df_1m, "5min")
df5["open_time"] = pd.to_datetime(df5["open_time"])
df5   = df5.reset_index(drop=True)

print("Detektálás...")
setups = detect_1212(df5, threshold=0.010)
setups = setups.sort_values("conf_time", ascending=False).reset_index(drop=True)
latest = setups.iloc[0]

print(f"Legfrissebb setup: {latest['conf_time']}")
print(f"  P0={latest['p0']:.2f}  P1={latest['p1']:.2f}  P2={latest['p2']:.2f}"
      f"  P3={latest['p3']:.2f}  P4={latest['p4']:.2f}")
print(f"  R_big={latest['r_big']:.3f}  R_sub={latest['r_sub']:.3f}  score={latest['score']:.3f}")

# -------------------------------------------------------------------------
# Pivot időpontok visszakeresése a df5-ben
# -------------------------------------------------------------------------
p_times = {
    "P0": latest["p0_time"],
    "P1": None,
    "P2": None,
    "P3": None,
    "P4": latest["p4_time"],
}

# P1–P3 időpontjait a source_pivots_json-ből nem tároljuk,
# de visszakereshetjük a df5-ből az áruk alapján a P0–P4 ablakban
p0_idx = df5[df5["open_time"] == pd.Timestamp(latest["p0_time"])].index[0]
p4_idx = df5[df5["open_time"] == pd.Timestamp(latest["p4_time"])].index[0]
window_df = df5.iloc[p0_idx:p4_idx + 1]

# P1 = legmagasabb high P0 és P2 között → keressük meg a közbülső pontokat
# újrafuttatjuk a pivot detektort a teljes adatra és kiemeljük a megfelelőt
from elliott_waves.elliott_1212 import detect_pivots
all_pivots = detect_pivots(df5, threshold=0.010)

# megkeressük azt az 5-ös ablakot ahol p0 és p4 egyezik
p_points: dict[str, tuple[int, float]] = {}
p4_conf_bar = None
for i in range(4, len(all_pivots)):
    pv = all_pivots[i - 4: i + 1]
    if (abs(pv[0].price - latest["p0"]) < 0.01 and
            abs(pv[4].price - latest["p4"]) < 0.01 and
            pv[0].bar == p0_idx):
        p_points = {
            "P0": (pv[0].bar, pv[0].price),
            "P1": (pv[1].bar, pv[1].price),
            "P2": (pv[2].bar, pv[2].price),
            "P3": (pv[3].bar, pv[3].price),
            "P4": (pv[4].bar, pv[4].price),
        }
        p4_conf_bar = pv[4].conf_bar
        break

if not p_points:
    print("WARN: nem sikerult visszakeresni az osszes pivot indexet, direkt keresés...")
    # fallback: a window_df-ben ár alapján
    p1_idx = window_df["high"].idxmax()
    p2_idx = window_df.loc[p1_idx:, "low"].idxmin()
    p3_idx = window_df.loc[p2_idx:, "high"].idxmax()
    p_points = {
        "P0": (p0_idx,  float(df5.loc[p0_idx, "low"])),
        "P1": (p1_idx,  float(df5.loc[p1_idx, "high"])),
        "P2": (p2_idx,  float(df5.loc[p2_idx, "low"])),
        "P3": (p3_idx,  float(df5.loc[p3_idx, "high"])),
        "P4": (p4_idx,  float(df5.loc[p4_idx, "low"])),
    }

# -------------------------------------------------------------------------
# Chart ablak: CONTEXT gyertyával bővítve mindkét irányban
# -------------------------------------------------------------------------
right_edge  = p4_conf_bar if p4_conf_bar is not None else p4_idx
chart_start = max(0, p0_idx - CONTEXT_LEFT)
chart_end   = min(len(df5) - 1, right_edge + CONTEXT_RIGHT)
chart_df    = df5.iloc[chart_start: chart_end + 1].copy().reset_index(drop=True)

# pivot indexek átszámolása a chart_df-hez
offset = chart_start
pvt_chart = {k: (idx - offset, price) for k, (idx, price) in p_points.items()}

# -------------------------------------------------------------------------
# Japángyertya chart
# -------------------------------------------------------------------------
sns.set_theme(style="darkgrid")

fig, ax = plt.subplots(figsize=(16, 7))
fig.patch.set_facecolor("#1a1a2e")
ax.set_facecolor("#16213e")
ax.tick_params(colors="white")
ax.xaxis.label.set_color("white")
ax.yaxis.label.set_color("white")
ax.title.set_color("white")
for spine in ax.spines.values():
    spine.set_edgecolor("#444466")
ax.grid(color="#2a2a4a", linewidth=0.5)

times  = chart_df["open_time"].to_numpy()
x_pos  = range(len(chart_df))

candle_w = 0.4
for xi, row in enumerate(chart_df.itertuples()):
    color  = "#26a69a" if row.close >= row.open else "#ef5350"
    body_lo = min(row.open, row.close)
    body_hi = max(row.open, row.close)
    # wick
    ax.plot([xi, xi], [row.low, row.high], color=color, linewidth=0.8, zorder=2)
    # body
    rect = mpatches.FancyBboxPatch(
        (xi - candle_w / 2, body_lo),
        candle_w,
        max(body_hi - body_lo, row.close * 0.0001),
        boxstyle="square,pad=0",
        facecolor=color,
        edgecolor=color,
        linewidth=0.5,
        zorder=3,
    )
    ax.add_patch(rect)

# -------------------------------------------------------------------------
# Pivot pontok és összekötő vonalak
# -------------------------------------------------------------------------
COLORS = {
    "P0": "#ffeb3b",
    "P1": "#4fc3f7",
    "P2": "#ffeb3b",
    "P3": "#4fc3f7",
    "P4": "#ffeb3b",
}
OFFSETS = {
    "P0": -0.6, "P2": -0.6, "P4": -0.6,   # low pontok: lefelé
    "P1": +0.6, "P3": +0.6,                # high pontok: felfelé
}
VA = {"P0": "top", "P2": "top", "P4": "top", "P1": "bottom", "P3": "bottom"}

line_xs = [pvt_chart[k][0] for k in ["P0", "P1", "P2", "P3", "P4"]]
line_ys = [pvt_chart[k][1] for k in ["P0", "P1", "P2", "P3", "P4"]]
ax.plot(line_xs, line_ys, color="#b0bec5", linewidth=1.4,
        linestyle="--", zorder=4, alpha=0.8)

for label, (xi, price) in pvt_chart.items():
    col    = COLORS[label]
    y_off  = OFFSETS[label]
    va     = VA[label]
    ax.scatter(xi, price, s=90, color=col, zorder=6, marker="o")
    ax.annotate(
        f"{label}\n{price:.2f}",
        xy     = (xi, price),
        xytext = (xi, price + y_off),
        fontsize = 8.5,
        color    = col,
        ha       = "center",
        va       = va,
        fontweight = "bold",
        zorder     = 7,
    )

# -------------------------------------------------------------------------
# P4 low → confirmation zóna: árnyékolt sáv + függőleges vonalak
# -------------------------------------------------------------------------
if p4_conf_bar is not None:
    p4_low_xi  = pvt_chart["P4"][0]
    p4_conf_xi = p4_conf_bar - offset

    # halvány piros sáv: "még nem tudjuk, hogy P4"
    ax.axvspan(p4_low_xi, p4_conf_xi, color="#ef5350", alpha=0.10, zorder=1,
               label="P4 bizonytalan zóna")

    # P4 tényleges mélypont — szaggatott függőleges
    ax.axvline(p4_low_xi, color="#ef5350", linewidth=1.2, linestyle=":",
               zorder=5, alpha=0.8)
    ax.text(p4_low_xi + 0.3, ax.get_ylim()[0],
            "P4 low", color="#ef5350", fontsize=7.5, va="bottom", alpha=0.9)

    # P4 megerősítés — zöld teli vonal
    ax.axvline(p4_conf_xi, color="#66bb6a", linewidth=1.6, linestyle="--",
               zorder=5, alpha=0.9)
    ax.text(p4_conf_xi + 0.3, ax.get_ylim()[0],
            "P4 conf.\n(signal)", color="#66bb6a", fontsize=7.5, va="bottom")

    # P3 trigger szint: vízszintes vonal P4 conf.-tól jobbra
    p3_price  = pvt_chart["P3"][1]
    x_trigger = p4_conf_xi
    ax.hlines(p3_price, x_trigger, len(chart_df) - 1,
              colors="#ffa726", linewidths=1.2, linestyles="--", zorder=4, alpha=0.85)
    ax.text(len(chart_df) - 1, p3_price + 0.02,
            f"P3 trigger  {p3_price:.2f}", color="#ffa726",
            fontsize=7.5, ha="right", va="bottom")

# -------------------------------------------------------------------------
# Retrace és score infó a charton
# -------------------------------------------------------------------------
info = (
    f"R_big = {latest['r_big']:.3f}  |  R_sub = {latest['r_sub']:.3f}  |  "
    f"Score = {latest['score']:.3f}  |  Threshold = 1.0%  |  TF = 5m"
)
ax.text(
    0.5, 0.97, info,
    transform    = ax.transAxes,
    fontsize     = 9,
    color        = "#cfd8dc",
    ha           = "center",
    va           = "top",
    bbox         = dict(facecolor="#0d0d1a", alpha=0.7, edgecolor="#445566", boxstyle="round,pad=0.3"),
)

# -------------------------------------------------------------------------
# X tengely: időcímkék
# -------------------------------------------------------------------------
tick_step = max(1, len(chart_df) // 12)
tick_pos  = list(range(0, len(chart_df), tick_step))
tick_labs = [pd.Timestamp(times[i]).strftime("%m-%d %H:%M") for i in tick_pos]
ax.set_xticks(tick_pos)
ax.set_xticklabels(tick_labs, rotation=35, ha="right", fontsize=8, color="#b0bec5")
ax.set_xlim(-1, len(chart_df))

ax.set_ylabel("Ár (USDT)", color="#b0bec5", fontsize=10)
ax.set_title(
    f"SOLUSDT 5m — Elliott 1-2-1-2 bullish setup  ({latest['conf_time']})",
    fontsize    = 13,
    color       = "white",
    pad         = 12,
)

plt.tight_layout()
out_path = "docs/analysis/eliot_waves/latest_1212_setup.png"
plt.savefig(out_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"OK: {out_path}")
plt.show()
