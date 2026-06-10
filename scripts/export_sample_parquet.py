#!/usr/bin/env python3
# =============================================================================
# Export modeling sample to Parquet for remote training (Google Colab)
# =============================================================================
# Purpose:
#  - Query the features table for a sample's full data range
#  - Save feat_*, trg_*, close, and open_time columns to Parquet
#  - Optionally copy the Parquet to Google Drive for Colab access
# Usage:
#   python scripts/export_sample_parquet.py --sample-id base_solusdt_fw60_futures_v1
#   python scripts/export_sample_parquet.py --sample-id base_solusdt_fw60_futures_v1 --copy-to-drive
# =============================================================================

import argparse
import json
import shutil
import sqlite3
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

DRIVE_SAMPLES_PATH = Path("F:/My Drive/chronoquant/samples")


# =============================================================================
# parse_args() -> argparse.Namespace
# =============================================================================
# Purpose:
#  - Parse CLI args for parquet export
# =============================================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export modeling sample features to Parquet for Colab training.",
    )
    parser.add_argument(
        "--sample-id",
        required = True,
        help     = "Sample ID (directory under samples/)",
    )
    parser.add_argument(
        "--copy-to-drive",
        action = "store_true",
        help   = "Copy the Parquet to F:/My Drive/chronoquant/samples/<sample-id>/",
    )
    return parser.parse_args()


# =============================================================================
# main() -> None
# =============================================================================
# Purpose:
#  - Load metadata, query features table, write Parquet, optionally copy to Drive
# =============================================================================
def main() -> None:
    args       = parse_args()
    sample_id  = args.sample_id
    sample_dir = Path("samples") / sample_id

    meta_path = sample_dir / "metadata.json"
    if not meta_path.exists():
        print(f"ERROR: {meta_path} not found — run create_sample_splits.py first")
        sys.exit(1)

    meta       = json.loads(meta_path.read_text())
    asset_id   = meta["source"]["asset_id"]
    table_name = meta["source"]["table_name"]
    db_path    = meta["source"]["db_path"]

    print(f"INFO: asset_id={asset_id}  table={table_name}")
    print(f"INFO: db_path={db_path}")

    # --------- Resolve column list ---------
    with sqlite3.connect(db_path) as conn:
        col_rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()

    all_cols = [r[1] for r in col_rows]
    keep = (
        ["open_time"]
        + (["close"] if "close" in all_cols else [])
        + [c for c in all_cols if c.startswith("feat_")]
        + [c for c in all_cols if c.startswith("trg_")]
    )
    feat_count = len([c for c in keep if c.startswith("feat_")])
    trg_count  = len([c for c in keep if c.startswith("trg_")])
    print(f"INFO: Exporting {len(keep)} columns ({feat_count} feat_, {trg_count} trg_)")

    # --------- Determine fixed schema upfront (avoids dtype drift between chunks) ---------
    CHUNK_SIZE = 100_000
    col_list   = ", ".join(f'"{c}"' for c in keep)
    out_path   = sample_dir / "dataset.parquet"

    def _cast_chunk(df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            if col == "open_time":
                continue
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].astype("float32")
            elif df[col].dtype == object:
                # All-null chunks from SQLite arrive as object dtype; coerce to float32
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
        return df

    with sqlite3.connect(db_path) as conn:
        schema_df = pd.read_sql_query(
            f"SELECT {col_list} FROM {table_name} LIMIT 1000",
            conn,
        )
    schema_df    = _cast_chunk(schema_df)
    arrow_schema = pa.Schema.from_pandas(schema_df, preserve_index=False)
    print(f"INFO: Schema fixed ({len(arrow_schema)} fields, numeric -> float32)")

    # --------- Chunked write ---------
    total_rows = 0
    first_time = None
    last_time  = None

    with sqlite3.connect(db_path) as conn:
        with pq.ParquetWriter(out_path, arrow_schema) as writer:
            for chunk in pd.read_sql_query(
                f"SELECT {col_list} FROM {table_name} ORDER BY open_time ASC",
                conn,
                chunksize = CHUNK_SIZE,
            ):
                chunk = _cast_chunk(chunk)
                table = pa.Table.from_pandas(chunk, schema=arrow_schema, preserve_index=False)
                writer.write_table(table)
                if first_time is None:
                    first_time = chunk["open_time"].iloc[0]
                total_rows += len(chunk)
                last_time   = chunk["open_time"].iloc[-1]
                print(f"  INFO: {total_rows:,} rows written...", end="\r")

    print(f"\nINFO: {total_rows} rows  [{first_time} -> {last_time}]")

    size_mb = out_path.stat().st_size / 1_000_000
    print(f"OK: Saved {out_path}  ({size_mb:.1f} MB)")

    # --------- Copy to Drive ---------
    if args.copy_to_drive:
        drive_dir = DRIVE_SAMPLES_PATH / sample_id
        try:
            drive_dir.mkdir(parents=True, exist_ok=True)
            drive_out = drive_dir / "dataset.parquet"
            shutil.copy2(out_path, drive_out)
            print(f"OK: Copied to {drive_out}")
        except Exception as e:
            print(f"ERROR: Drive copy failed: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
