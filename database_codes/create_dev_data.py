import pandas as pd
import subprocess
import os
import sqlite3
import json
import ta


def create_dev_data_table(config_path, open_time_from, open_time_to):

    # =============================================================================
    # DB_PATH retrieval
    # =============================================================================
    repo_root = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
    DB_PATH   = json.load(open(os.path.join(repo_root, "config.json"), "r"))["db_path"]

    # =============================================================================
    # Load configuration from JSON file
    # =============================================================================
    with open(config_path, 'r') as f:
        config = json.load(f)

    DB_PATH          = DB_PATH
    TABLE_NAME       = config["table_name"]
    TABLE_NAME_DEV   = config.get("table_name_dev", f"{TABLE_NAME}_dev")
    ROLLING_WINDOW   = config["rolling_window"]
    TARGET           = config["target"]
    PERCENTILE       = config["percentile"]
    indicator_config = config["indicator_config"]

    # =============================================================================
    # Generate features list from indicator configuration
    # =============================================================================
    features = []
    for category, indicators in indicator_config.items():
        for indicator, params_list in indicators.items():
            for params in params_list:
                if "window" in params:
                    features.append(f"{params['name']}_{params['window']}")
                else:
                    features.append(params["name"])

    # =============================================================================
    # Fetch data from database for given time interval
    # =============================================================================
    conn = sqlite3.connect(DB_PATH)
    query = f"""
        SELECT t.open_time, t.close
        FROM {TABLE_NAME} t
        WHERE open_time BETWEEN ? AND ?
        ORDER BY open_time ASC
    """
    df = pd.read_sql_query(query, conn, params=(open_time_from, open_time_to))
    conn.close()

    df["open_time"] = pd.to_datetime(df["open_time"])
    df.set_index("open_time", inplace=True)

    # =============================================================================
    # Calculate rolling max, ratio, and percentile-based target
    # =============================================================================
    df["rolling_max"] = (
        df.iloc[::-1]["close"].rolling(window=ROLLING_WINDOW, min_periods=1).max().iloc[::-1]
    )
    df["ratio"]      = df["rolling_max"] / df["close"]
    percentile_value = df["ratio"].quantile(PERCENTILE)
    df[TARGET]       = (df["ratio"] >= percentile_value).astype(int)

    # =============================================================================
    # Calculate technical indicators
    # =============================================================================
    # Momentum indicators
    for params in indicator_config["momentum"]["rsi"]:
        df[f"{params['name']}_{params['window']}"] = ta.momentum.RSIIndicator(
            close=df["close"], window=params["window"]
        ).rsi()
    for params in indicator_config["momentum"]["roc"]:
        df[f"{params['name']}_{params['window']}"] = ta.momentum.ROCIndicator(
            close=df["close"], window=params["window"]
        ).roc()
    # Trend indicators
    for params in indicator_config["trend"]["macd"]:
        macd_calc = ta.trend.MACD(close=df["close"])
        df[params["name"]] = macd_calc.macd_diff()
    for params in indicator_config["trend"]["sma"]:
        df[f"{params['name']}_{params['window']}"] = df["close"] / ta.trend.SMAIndicator(
            close=df["close"], window=params["window"]
        ).sma_indicator()
    # Volatility indicators
    for params in indicator_config["volatility"]["bollinger_band"]:
        bb_calc = ta.volatility.BollingerBands(close=df["close"], window=params["window"])
        df[f"{params['name']}_{params['window']}"] = bb_calc.bollinger_wband()

    # =============================================================================
    # Write DataFrame to dev table in database, including index as column
    # =============================================================================
    conn = sqlite3.connect(DB_PATH)
    df.to_sql(TABLE_NAME_DEV, conn, if_exists='replace', index=True)
    conn.close()
    print(f"Table '{TABLE_NAME_DEV}' written to DB '{DB_PATH}' for interval {open_time_from} to {open_time_to}.")
