#!/usr/bin/env python
"""
Update predictions table with the live signal column.
"""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import utils
from db.maintenance import update_prediction_signals


def main() -> None:
    db_cfg  = utils.load_db_config()["database"]
    db_path = db_cfg["db_path"]
    table   = db_cfg["tables"]["predictions"]

    update_prediction_signals(db_path, table)

    # Verify
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT
                signal,
                COUNT(*) as cnt,
                ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM {table}), 2) as pct
            FROM {table}
            GROUP BY signal
            ORDER BY cnt DESC
            """,
        ).fetchall()

    print()
    print("Signal Distribution:")
    print("-" * 50)
    for signal, cnt, pct in rows:
        print(f"  {signal:8s}: {cnt:10d} ({pct:6.2f}%)")

    print()
    print("Predictions table updated successfully")


if __name__ == "__main__":
    main()

