import pandas as pd
import subprocess
import os
import sqlite3
import json
import ta


def create_dev_data_table(open_time_from, open_time_to):

    # =============================================================================
    # DB_PATH retrieval
    # =============================================================================
    import sys, os; sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
    import utils as utils
    
    config  = utils._load_config()

    db_cfg  = config.get("database", {})        # a "database" alatti dict
    dev_cfg = db_cfg.get("dev_data", {})        # a "dev_data" alatti dict

    DB_PATH          = db_cfg.get("db_path")            # string: a DB file útvonala
    TABLE_NAME       = dev_cfg.get("table_name")
    TABLE_NAME_DEV   = dev_cfg.get("table_name_dev")
    ROLLING_WINDOW   = dev_cfg.get("rolling_window")
    TARGET           = dev_cfg.get("target")
    PERCENTILE       = dev_cfg.get("percentile")
    indicator_config = dev_cfg.get("indicator_config")

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
    # build features with prefix
    # =============================================================================
    prefix           = "feat_"  
    indicator_config = config.get("database", {}).get("dev_data", {}).get("indicator_config", {})

    # features lista összeállítása (duplikátum eltávolítás, sorrend megtartása)
    features = []
    _seen = set()
    for category, indicators in indicator_config.items():
        if not isinstance(indicators, dict):
            continue
        for params_list in indicators.values():
            if not isinstance(params_list, list):
                continue
            for params in params_list:
                if not isinstance(params, dict):
                    continue
                name = params.get("name")
                if not name:
                    continue
                window = params.get("window")
                feat = f"{name}_{window}" if window is not None else name
                if feat not in _seen:
                    features.append(feat)
                    _seen.add(feat)
    
    # =============================================================================
    # create indicators and add to DataFrame
    # =============================================================================
    # Momentum indicators
    for params in indicator_config["momentum"]["rsi"]:
        name = f"{params['name']}_{params['window']}"
        df[f"{prefix}{name}"] = ta.momentum.RSIIndicator(
            close=df["close"], window=params["window"]
        ).rsi()

    for params in indicator_config["momentum"]["roc"]:
        name = f"{params['name']}_{params['window']}"
        df[f"{prefix}{name}"] = ta.momentum.ROCIndicator(
            close=df["close"], window=params["window"]
        ).roc()

    # Trend indicators
    for params in indicator_config["trend"]["macd"]:
        name = params["name"]
        df[f"{prefix}{name}"] = ta.trend.MACD(close=df["close"]).macd_diff()

    for params in indicator_config["trend"]["sma"]:
        name = f"{params['name']}_{params['window']}"
        df[f"{prefix}{name}"] = df["close"] / ta.trend.SMAIndicator(
            close=df["close"], window=params["window"]
        ).sma_indicator()

    # Volatility indicators
    for params in indicator_config["volatility"]["bollinger_band"]:
        name = f"{params['name']}_{params['window']}"
        bb_calc = ta.volatility.BollingerBands(close=df["close"], window=params["window"])
        df[f"{prefix}{name}"] = bb_calc.bollinger_wband()

    # =============================================================================
    # Write DataFrame to dev table in database, including index as column
    # =============================================================================
    conn = sqlite3.connect(DB_PATH)
    df.to_sql(TABLE_NAME_DEV, conn, if_exists='replace', index=True)
    conn.close()
    print(f"Table '{TABLE_NAME_DEV}' written to DB '{DB_PATH}' for interval {open_time_from} to {open_time_to}.")
