#!/usr/bin/env python3
# =============================================================================
# Fetch and sync OHLCV data from Binance
# =============================================================================
# Purpose:
#  - Query Binance klines API
#  - Insert raw OHLCV data into database table
#  - Config-driven, minimal approach
# =============================================================================

import os
import json
import sqlite3
import pandas as pd
from binance.client import Client

import utils

# =============================================================================
# sync_ohlcv(open_time_ms_from: int) -> None
# =============================================================================
# Purpose:
#  - Fetch klines from Binance starting at open_time_ms_from (epoch ms)
#  - Build DataFrame with normalized columns
#  - Insert rows into database OHLCV table
# Parameters:
#  - open_time_ms_from: epoch milliseconds (UTC)
# =============================================================================
def sync_ohlcv(open_time_ms_from: int) -> None:
	# -------------------------------------------------------------------------
	# Load configuration
	# -------------------------------------------------------------------------
	db_cfg       = utils.load_db_config()
	env_cfg      = utils.load_env_config()
	db_path      = db_cfg["database"]["db_path"]
	symbol       = db_cfg["database"]["symbol"]
	table_name   = db_cfg["database"]["tables"]["ohlcv"]
	binance_keys = env_cfg["api"]["binance_keys_path"]

	# -------------------------------------------------------------------------
	# Load Binance API keys
	# -------------------------------------------------------------------------
	with open(binance_keys, "r", encoding="utf-8") as f:
		keys = json.load(f)
	api_key    = keys.get("api_key") or keys.get("key")
	api_secret = keys.get("api_secret") or keys.get("secret")

	# -------------------------------------------------------------------------
	# Fetch klines from Binance
	# -------------------------------------------------------------------------
	client = Client(api_key, api_secret)
	rows   = client.get_klines(
		symbol=symbol,
		interval=Client.KLINE_INTERVAL_1MINUTE,
		startTime=int(open_time_ms_from)
	)

	if not rows:
		print("⚠️ No klines returned from Binance")
		return

	# -------------------------------------------------------------------------
	# Build DataFrame and normalize columns
	# -------------------------------------------------------------------------
	df = pd.DataFrame(
		rows,
		columns=[
			"open_time", "open", "high", "low", "close", "volume",
			"close_time", "quote_volume", "trades",
			"taker_buy_base", "taker_buy_quote", "ignore"
		]
	)

	# Convert open_time to ms and human-readable string
	df["open_time_ms"] = pd.to_numeric(df["open_time"], errors="coerce").astype("int64")
	df["open_time"]    = df["open_time_ms"].apply(lambda ms: utils.ms_to_utc_str(int(ms)))

	# Convert numeric columns to proper types
	for col in ["open", "high", "low", "close", "volume"]:
		df[col] = pd.to_numeric(df[col], errors="coerce")

	# Keep only needed columns
	df = df[["open_time", "open", "high", "low", "close", "volume"]]

	# -------------------------------------------------------------------------
	# Insert into database
	# -------------------------------------------------------------------------
	with sqlite3.connect(db_path) as conn:
		df.to_sql(table_name, conn, index=False, if_exists="append")

	print(f"✅ Synced {len(df)} klines into '{table_name}'")
