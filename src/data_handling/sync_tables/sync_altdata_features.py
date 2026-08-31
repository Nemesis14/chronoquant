"""Materialize feat_altdata from altdata_5m (epic_054 / t308).

Reads the 5-minute alt-data table, computes the 22 derivatives features, and
writes them onto the 1-minute grid of `ohlcv` as `feat_altdata`. The output
uses the `feat_` prefix so `rebuild_quant_train` picks it up like any other
feature table.

Usage:
    python -m data_handling.sync_tables.sync_altdata_features
    python -m data_handling.sync_tables.sync_altdata_features --start 2023-01-01
"""

import argparse
import logging
import sys

import polars as pl

import utils
from data_handling.store.duckdb_store import get_connection
from data_handling.sync_tables._altdata_features_polars import (
    ALTDATA_FEATURES,
    compute_altdata_features,
    expand_to_minute_grid,
)

logger = logging.getLogger(__name__)

TABLE = "feat_altdata"


def sync_altdata_features(
    asset_id: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
) -> int:
    """Rebuild feat_altdata from altdata_5m for the given open_time window.

    Args:
        asset_id: Asset key from config/assets.json; uses default if None.
        start_time: Optional lower bound (YYYY-MM-DD), inclusive.
        end_time: Optional upper bound (YYYY-MM-DD), inclusive.

    Returns:
        Row count written.
    """
    db_path = utils.load_asset_config(asset_id)["database"]["db_path"]
    conn = get_connection(db_path)
    try:
        where = []
        params: list[str] = []
        if start_time:
            where.append("open_time >= ?")
            params.append(start_time)
        if end_time:
            where.append("open_time <= ?")
            params.append(end_time)
        clause = f"WHERE {' AND '.join(where)}" if where else ""

        # The 5m source needs a 240-minute head start so the longest rolling
        # window is warm at the first minute we actually emit.
        alt_clause = clause
        alt_params = list(params)
        if start_time:
            alt_clause = alt_clause.replace(
                "open_time >= ?", "open_time >= (CAST(? AS TIMESTAMP) - INTERVAL 300 MINUTE)"
            )

        alt = conn.execute(f"SELECT * FROM altdata_5m {alt_clause} ORDER BY open_time",
                           alt_params).pl()
        if alt.is_empty():
            logger.warning("altdata_5m ures a kert tartomanyra -- skip")
            return 0

        minutes = conn.execute(
            f"SELECT open_time FROM ohlcv {clause} ORDER BY open_time", params
        ).pl()

        feats_5m = compute_altdata_features(alt)
        out = expand_to_minute_grid(feats_5m, minutes)

        conn.register("incoming", out.to_arrow())
        if start_time or end_time:
            cols = ", ".join(f'"{c}"' for c in ["open_time", *ALTDATA_FEATURES])
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {TABLE} AS SELECT {cols} FROM incoming WHERE 1=0"
            )
            conn.execute(
                f"DELETE FROM {TABLE} WHERE open_time IN (SELECT open_time FROM incoming)"
            )
            conn.execute(f"INSERT INTO {TABLE} SELECT {cols} FROM incoming")
        else:
            conn.execute(f"CREATE OR REPLACE TABLE {TABLE} AS SELECT * FROM incoming")
        conn.unregister("incoming")

        row = conn.execute(f"SELECT count(*) FROM {TABLE}").fetchone()
        total = int(row[0]) if row else 0
        logger.info("%s: %d sor irva, tabla merete %d", TABLE, out.height, total)
        return total
    finally:
        conn.close()


def fill_report(asset_id: str | None = None) -> pl.DataFrame:
    """Per-year non-null share of every feat_altdata column."""
    db_path = utils.load_asset_config(asset_id)["database"]["db_path"]
    conn = get_connection(db_path)
    try:
        parts = ", ".join(
            f'round(100.0 * count("{c}") / count(*)) AS "{c[5:]}"' for c in ALTDATA_FEATURES
        )
        return conn.execute(
            f"SELECT strftime(open_time, '%Y') AS ev, count(*) AS sorok, {parts} "
            f"FROM {TABLE} GROUP BY 1 ORDER BY 1"
        ).pl()
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build feat_altdata from altdata_5m")
    parser.add_argument("--asset-id", default="solusdt")
    parser.add_argument("--start", default=None, help="YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="YYYY-MM-DD")
    parser.add_argument("--report", action="store_true", help="Csak kitoltottsegi riport")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.report:
        with pl.Config(tbl_cols=25, tbl_width_chars=200):
            print(fill_report(args.asset_id))
        return

    sync_altdata_features(args.asset_id, args.start, args.end)


if __name__ == "__main__":
    main()
