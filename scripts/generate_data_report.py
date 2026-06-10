# =============================================================================
# Generate data understanding report
# =============================================================================
# Purpose:
#  - Compute and print a data understanding summary for solusdt_data_dev.db
#  - Run this after backfilling or when tables grow to refresh the report
#  - Output mirrors docs/data/understanding.md statistics
#
# Usage:
#   python scripts/generate_data_report.py
#   python scripts/generate_data_report.py --asset-id solusdt_fw60
# =============================================================================

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import sqlite3
import pandas as pd
import utils


def _run_report(db_path: str) -> None:
    conn = sqlite3.connect(db_path)

    # ------------------------------------------------------------------
    # 1. Table-level overview
    # ------------------------------------------------------------------
    tables = {
        "solusdt_1m":               "Raw OHLCV",
        "solusdt_1m_features":      "Features",
        "solusdt_1m_predictions":   "Predictions",
        "solusdt_elliott_events":   "Elliott events",
    }

    print("=" * 72)
    print("DATA UNDERSTANDING REPORT")
    print(f"DB: {db_path}")
    print("=" * 72)

    print("\n### Table overview\n")
    header = f"{'Table':<35} {'Rows':>10}  {'From':>19}  {'To':>19}"
    print(header)
    print("-" * len(header))
    for tbl, label in tables.items():
        try:
            rows = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            min_t, max_t = conn.execute(
                f"SELECT MIN(open_time), MAX(open_time) FROM {tbl}"
            ).fetchone()
            print(f"{tbl:<35} {rows:>10,}  {str(min_t):>19}  {str(max_t):>19}")
        except sqlite3.OperationalError:
            print(f"{tbl:<35} {'(table missing)':>10}")

    # ------------------------------------------------------------------
    # 2. OHLCV: annual stats + gap check
    # ------------------------------------------------------------------
    print("\n\n### OHLCV annual statistics (solusdt_1m)\n")
    df_annual = pd.read_sql_query(
        """
        SELECT
            substr(open_time, 1, 4)   AS year,
            COUNT(*)                  AS bars,
            ROUND(MIN(low),  4)       AS year_low,
            ROUND(MAX(high), 4)       AS year_high
        FROM solusdt_1m
        GROUP BY year
        ORDER BY year
        """,
        conn,
    )
    print(df_annual.to_string(index=False))

    df_gaps = pd.read_sql_query(
        """
        WITH ordered AS (
            SELECT open_time,
                   LAG(open_time) OVER (ORDER BY open_time) AS prev_time
            FROM solusdt_1m
        )
        SELECT
            prev_time  AS gap_start,
            open_time  AS gap_end,
            CAST(ROUND((julianday(open_time) - julianday(prev_time)) * 1440) AS INTEGER)
                       AS gap_minutes
        FROM ordered
        WHERE prev_time IS NOT NULL
          AND (julianday(open_time) - julianday(prev_time)) * 1440 > 2
        ORDER BY gap_minutes DESC
        LIMIT 20
        """,
        conn,
    )
    print(f"\nGaps > 2 minutes: {len(df_gaps)}")
    if not df_gaps.empty:
        print(df_gaps.to_string(index=False))

    # ------------------------------------------------------------------
    # 3. Features: null summary
    # ------------------------------------------------------------------
    print("\n\n### Features null summary (solusdt_1m_features)\n")
    col_count = conn.execute(
        "SELECT COUNT(*) FROM pragma_table_info('solusdt_1m_features')"
    ).fetchone()[0]
    row_count = conn.execute(
        "SELECT COUNT(*) FROM solusdt_1m_features"
    ).fetchone()[0]
    null_trg_l = conn.execute(
        "SELECT COUNT(*) FROM solusdt_1m_features WHERE trg_l_fw60_q90 IS NULL"
    ).fetchone()[0]
    null_trg_s = conn.execute(
        "SELECT COUNT(*) FROM solusdt_1m_features WHERE trg_s_fw60_q10 IS NULL"
    ).fetchone()[0]
    null_close = conn.execute(
        "SELECT COUNT(*) FROM solusdt_1m_features WHERE close IS NULL"
    ).fetchone()[0]
    last_nn_l  = conn.execute(
        "SELECT MAX(open_time) FROM solusdt_1m_features WHERE trg_l_fw60_q90 IS NOT NULL"
    ).fetchone()[0]
    last_nn_s  = conn.execute(
        "SELECT MAX(open_time) FROM solusdt_1m_features WHERE trg_s_fw60_q10 IS NOT NULL"
    ).fetchone()[0]
    max_feat   = conn.execute(
        "SELECT MAX(open_time) FROM solusdt_1m_features"
    ).fetchone()[0]

    print(f"  Total rows:              {row_count:,}")
    print(f"  Total columns:           {col_count}")
    print(f"  NULL trg_l_fw60_q90:     {null_trg_l:,}  (last non-null: {last_nn_l})")
    print(f"  NULL trg_s_fw60_q10:     {null_trg_s:,}  (last non-null: {last_nn_s})")
    print(f"  NULL close:              {null_close:,}")
    print(f"  Max open_time:           {max_feat}")
    if max_feat and last_nn_l:
        trailing_mins = int(
            (pd.Timestamp(max_feat) - pd.Timestamp(last_nn_l)).total_seconds() / 60
        )
        print(f"  Trailing NULL window:    {trailing_mins} min (fw60 edge, expected)")

    # ------------------------------------------------------------------
    # 4. Predictions: column list + annual stats
    # ------------------------------------------------------------------
    print("\n\n### Predictions (solusdt_1m_predictions)\n")
    try:
        pred_cols = [
            row[1] for row in conn.execute(
                "PRAGMA table_info('solusdt_1m_predictions')"
            ).fetchall()
        ]
        print(f"  Columns: {pred_cols}\n")

        model_p_cols = [c for c in pred_cols if c.endswith("_p")]
        if model_p_cols:
            agg_parts = []
            for col in model_p_cols:
                safe = col.replace('"', '""')
                agg_parts.append(
                    f'ROUND(MIN("{safe}"),4) AS "{safe}_min", '
                    f'ROUND(MAX("{safe}"),4) AS "{safe}_max", '
                    f'ROUND(AVG("{safe}"),4) AS "{safe}_avg"'
                )
            df_pred = pd.read_sql_query(
                f"""
                SELECT substr(open_time,1,4) AS year,
                       COUNT(*) AS rows,
                       {", ".join(agg_parts)}
                FROM solusdt_1m_predictions
                GROUP BY year ORDER BY year
                """,
                conn,
            )
            print(df_pred.to_string(index=False))
    except sqlite3.OperationalError as exc:
        print(f"  (skipped: {exc})")

    # ------------------------------------------------------------------
    # 5. Elliott events: breakdown
    # ------------------------------------------------------------------
    print("\n\n### Elliott events (solusdt_elliott_events)\n")
    try:
        df_ell = pd.read_sql_query(
            """
            SELECT
                event_type,
                direction,
                COUNT(*)                               AS cnt,
                ROUND(AVG(setup_score), 3)             AS avg_score,
                ROUND(AVG(forward_close_return_60m), 4) AS avg_fwd_ret_60m,
                SUM(price_up_60m)                      AS n_up
            FROM solusdt_elliott_events
            GROUP BY event_type, direction
            ORDER BY cnt DESC
            """,
            conn,
        )
        if df_ell.empty:
            print("  No events.")
        else:
            print(df_ell.to_string(index=False))
    except sqlite3.OperationalError as exc:
        print(f"  (skipped: {exc})")

    print("\n" + "=" * 72)
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate data understanding report")
    parser.add_argument(
        "--asset-id",
        default=None,
        help="Asset ID from config/assets.json (e.g. solusdt_fw60)",
    )
    args = parser.parse_args()

    db_cfg = utils.load_asset_config(args.asset_id)["database"]
    _run_report(db_cfg["db_path"])


if __name__ == "__main__":
    main()
