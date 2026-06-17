"""Smoke tests for sampling._write_sample_parquet."""

import tempfile
from pathlib import Path

import duckdb
import polars as pl
import pytest

from modeling.quantitative.sampling.create_sample import _write_sample_parquet

pytestmark = pytest.mark.smoke

_FEAT_COLS = ["feat_a", "feat_b"]
_TARGET_COLS = ["long_mfe_fw60", "short_mfe_fw60"]

SPLITS_ONE_FOLD = {
    "folds": [
        {
            "fold"       : 1,
            "train_start": "2021-01-01 00:00:00",
            "train_end"  : "2021-06-30 23:59:00",
            "valid_start": "2021-07-01 00:01:00",
            "valid_end"  : "2021-09-30 23:59:00",
        }
    ],
    "test": {
        "start": "2021-10-01 00:01:00",
        "end"  : "2021-12-31 23:59:00",
    },
}


def _build_temp_db(db_path: str, rows_per_segment: int = 5) -> None:
    """Create a minimal DuckDB with feat_ohlcv_quant and target tables."""
    boundaries = [
        ("2021-01-01 00:00:00", "2021-06-30 23:59:00"),
        ("2021-07-01 00:01:00", "2021-09-30 23:59:00"),
        ("2021-10-01 00:01:00", "2021-12-31 23:59:00"),
    ]
    ts_values = []
    for start, _ in boundaries:
        ts_values += [f"'{start}'::TIMESTAMP + INTERVAL '{i} minutes'" for i in range(rows_per_segment)]

    ts_union = " UNION ALL ".join(f"SELECT {t} AS open_time" for t in ts_values)

    con = duckdb.connect(db_path)
    try:
        con.execute(f"""
            CREATE TABLE feat_ohlcv_quant AS
            SELECT open_time, 1.0 AS feat_a, 2.0 AS feat_b
            FROM ({ts_union})
        """)
        con.execute(f"""
            CREATE TABLE target AS
            SELECT open_time, 0.01 AS long_mfe_fw60, -0.01 AS short_mfe_fw60
            FROM ({ts_union})
        """)
    finally:
        con.close()


def test_parquet_file_created() -> None:
    """_write_sample_parquet creates sample.parquet in sample_dir."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path    = str(Path(tmp) / "test.duckdb")
        sample_dir = Path(tmp) / "sample"
        sample_dir.mkdir()
        _build_temp_db(db_path)

        _write_sample_parquet(db_path, sample_dir, SPLITS_ONE_FOLD)

        assert (sample_dir / "sample.parquet").exists()


def test_parquet_has_segment_column() -> None:
    """Output parquet contains a 'segment' string column."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path    = str(Path(tmp) / "test.duckdb")
        sample_dir = Path(tmp) / "sample"
        sample_dir.mkdir()
        _build_temp_db(db_path)

        _write_sample_parquet(db_path, sample_dir, SPLITS_ONE_FOLD)

        df = pl.read_parquet(sample_dir / "sample.parquet")
        assert "segment" in df.columns
        assert df["segment"].dtype == pl.String


def test_parquet_segment_values() -> None:
    """Output parquet contains exactly the expected segment labels."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path    = str(Path(tmp) / "test.duckdb")
        sample_dir = Path(tmp) / "sample"
        sample_dir.mkdir()
        _build_temp_db(db_path)

        _write_sample_parquet(db_path, sample_dir, SPLITS_ONE_FOLD)

        df     = pl.read_parquet(sample_dir / "sample.parquet")
        actual = set(df["segment"].unique().to_list())
        assert actual == {"fold_1_train", "fold_1_valid", "test"}


def test_parquet_has_feature_and_target_columns() -> None:
    """Output parquet includes feature and target columns from the DB tables."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path    = str(Path(tmp) / "test.duckdb")
        sample_dir = Path(tmp) / "sample"
        sample_dir.mkdir()
        _build_temp_db(db_path)

        _write_sample_parquet(db_path, sample_dir, SPLITS_ONE_FOLD)

        df = pl.read_parquet(sample_dir / "sample.parquet")
        for col in _FEAT_COLS + _TARGET_COLS + ["open_time"]:
            assert col in df.columns, f"Missing column: {col}"
