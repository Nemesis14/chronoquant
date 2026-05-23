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
	# Fetch klines from Binance (paginated)
	# -------------------------------------------------------------------------
	client   = Client(api_key, api_secret)
	limit    = 1000
	start_ms = int(open_time_ms_from)
	end_ms   = utils.now_utc_ms()
	total    = 0

	while True:
		rows = client.get_klines(
			symbol     = symbol,
			interval   = Client.KLINE_INTERVAL_1MINUTE,
			startTime  = start_ms,
			limit      = limit
		)

		if not rows:
			break

		# ---------------------------------------------------------------------
		# Build DataFrame and normalize columns
		# ---------------------------------------------------------------------
		df = pd.DataFrame(
			rows,
			columns=[
				"open_time", "open", "high", "low", "close", "volume",
				"close_time", "quote_volume", "trades",
				"taker_buy_base", "taker_buy_quote", "ignore"
			]
		)

		df["open_time_ms"] = pd.to_numeric(df["open_time"], errors="coerce").astype("int64")
		df["open_time"]    = df["open_time_ms"].apply(lambda ms: utils.ms_to_utc_str(int(ms)))

		for col in ["open", "high", "low", "close", "volume"]:
			df[col] = pd.to_numeric(df[col], errors="coerce")

		df = df[["open_time", "open", "high", "low", "close", "volume"]]

		with sqlite3.connect(db_path) as conn:
			df.to_sql(table_name, conn, index=False, if_exists="append")

		total += len(df)

		last_open_ms = int(rows[-1][0])
		next_start   = last_open_ms + 60000
		if next_start <= start_ms:
			break

		start_ms = next_start
		if len(rows) < limit:
			break

	if total == 0:
		print("No klines returned from Binance")
		return

	print(f"Synced {total} klines into '{table_name}'")



