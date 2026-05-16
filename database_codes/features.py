# =============================================================================
# Compute technical indicators and target variable for feature engineering
# =============================================================================
# Purpose:
#  - Load raw OHLCV data from database
#  - Compute rolling max, ratio, target variable
#  - Generate technical indicators (RSI, ROC, MACD, SMA, Bollinger)
#  - Insert feature rows into database with 'feat_' prefix
# =============================================================================

import os
import sys
import sqlite3
import pandas as pd
import ta
from datetime import timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
import utils

# =============================================================================
# sync_features(start_time: str, lookback_bars: int = 240) -> None
# =============================================================================
# Purpose:
#  - Fetch raw OHLCV data from [start_time - lookback, end]
#  - Compute target variable (ratio >= percentile)
#  - Generate all configured technical indicators with 'feat_' prefix
#  - Insert rows into features table
# Parameters:
#  - start_time: "YYYY-MM-DD HH:MM:SS" (UTC)
#  - lookback_bars: minutes to look back for feature computation
# =============================================================================
def sync_features(start_time: str, lookback_bars: int = 240) -> None:
	# -------------------------------------------------------------------------
	# Load configuration
	# -------------------------------------------------------------------------
	db_cfg      = utils.load_db_config()
	feat_cfg    = utils.load_features_config()
	db_path     = db_cfg["database"]["db_path"]
	table_ohlcv = db_cfg["database"]["tables"]["ohlcv"]
	table_feat  = db_cfg["database"]["tables"]["features"]
	cfg_feat    = feat_cfg["database"]["features"]
	targets_cfg = cfg_feat.get("targets", [])

	# -------------------------------------------------------------------------
	# Fetch raw OHLCV data
	# -------------------------------------------------------------------------
	fetch_start = (
		pd.to_datetime(start_time) - timedelta(minutes=lookback_bars)
	).strftime("%Y-%m-%d %H:%M:%S")

	with sqlite3.connect(db_path) as conn:
		df = pd.read_sql_query(
			f"""
			SELECT open_time, close FROM {table_ohlcv}
			WHERE open_time >= ? ORDER BY open_time ASC
			""",
			conn,
			params=(fetch_start,)
		)

	df["open_time"] = pd.to_datetime(df["open_time"])
	df.set_index("open_time", inplace=True)

	# -------------------------------------------------------------------------
	# Compute target variables
	# -------------------------------------------------------------------------
	for target_cfg in targets_cfg:
		direction = target_cfg["direction"]
		rolling_win = target_cfg["rolling_window"]
		percentile = target_cfg["percentile"]
		target_col = utils.target_name_from_config(target_cfg)

		if direction == "long":
			rolling_max 	= df["close"][::-1].rolling(rolling_win, min_periods=1).max()[::-1]
			ratio_long 		= rolling_max / df["close"]
			threshold 		= ratio_long.quantile(percentile)
			df[target_col]  = (ratio_long >= threshold).astype(int)

		elif direction == "short":
			rolling_min 	= df["close"][::-1].rolling(rolling_win, min_periods=1).min()[::-1]
			ratio_short 	= rolling_min / df["close"]
			threshold 		= ratio_short.quantile(percentile)  # percentile=0.1 => alsó 10%
			df[target_col] 	= (ratio_short <= threshold).astype(int)

		else:
			raise ValueError(f"Unknown target direction: {direction}")

	# -------------------------------------------------------------------------
	# Generate technical indicators with 'feat_' prefix
	# -------------------------------------------------------------------------
	indicators = cfg_feat["indicators"]
	feat_prefix = "feat_"

	# Momentum: RSI
	for rsi_cfg in indicators.get("momentum", {}).get("rsi", []):
		w             = rsi_cfg["window"]
		feat_name     = f"{feat_prefix}rsi_{w}"
		df[feat_name] = ta.momentum.RSIIndicator(close=df["close"], window=w).rsi()

	# Momentum: ROC
	for roc_cfg in indicators.get("momentum", {}).get("roc", []):
		w             = roc_cfg["window"]
		feat_name     = f"{feat_prefix}roc_{w}"
		df[feat_name] = ta.momentum.ROCIndicator(close=df["close"], window=w).roc()

	# Trend: MACD
	for macd_cfg in indicators.get("trend", {}).get("macd", []):
		fast		  = macd_cfg.get("fast", 12)
		slow		  = macd_cfg.get("slow", 26)
		feat_name	  = f"{feat_prefix}macd_diff"
		macd		  = ta.trend.MACD(close=df["close"], window_fast=fast, window_slow=slow)
		df[feat_name] = macd.macd_diff()

	# Trend: SMA ratio
	for sma_cfg in indicators.get("trend", {}).get("sma", []):
		w 			  = sma_cfg["window"]
		feat_name 	  = f"{feat_prefix}sma_ratio_{w}"
		sma 		  = ta.trend.SMAIndicator(close=df["close"], window=w).sma_indicator()
		df[feat_name] = df["close"] / sma

	# Volatility: Bollinger Bands
	for bb_cfg in indicators.get("volatility", {}).get("bollinger", []):
		w 	      = bb_cfg["window"]
		feat_name = f"{feat_prefix}bb_width_{w}"
		bb 	      = ta.volatility.BollingerBands(close=df["close"], window=w)
		upper     = bb.bollinger_hband()
		lower     = bb.bollinger_lband()
		df[feat_name] = (upper - lower) / df["close"]

	# -------------------------------------------------------------------------
	# Prepare and insert into database
	# -------------------------------------------------------------------------
	df_reset = df.reset_index()

	# Select open_time, targets, and feature columns (all with 'feat_' prefix)
	feat_cols    = [c for c in df_reset.columns if c.startswith(feat_prefix)]
	target_cols  = utils.target_columns_from_config(feat_cfg)
	cols_to_keep = ["open_time", "close"] + target_cols + feat_cols
	df_final     = df_reset[cols_to_keep]

	with sqlite3.connect(db_path) as conn:
		df_final.to_sql(table_feat, conn, index=False, if_exists="append")

	print(f"✅ Computed {len(df_final)} feature rows into '{table_feat}'")
