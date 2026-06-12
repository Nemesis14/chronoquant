"""One-shot script: sync predictions for lgbm_solusdt_s_fw60_q10_local_v3 in chunks."""
import os

os.environ['OMP_NUM_THREADS'] = '1'
os.environ['OPENBLAS_NUM_THREADS'] = '1'

import json
import pickle
import sys

sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parents[1] / 'src'))

import pandas as pd
from db.table_ops import ensure_table_columns, sqlite_connect

import utils
from data_pipeline.sync_predictions import _feature_list_for_prediction, _write_predictions

MODEL_ID   = 'lgbm_solusdt_s_fw60_q10_local_v3'
ASSET_ID   = 'solusdt_fw60'
START      = '2024-01-01 00:00:00'
CHUNK_SIZE = 300_000

model_cfg  = utils.load_models_config()
db_cfg     = utils.load_asset_config(ASSET_ID)
db_path    = db_cfg['database']['db_path']
table_feat = db_cfg['database']['tables']['features']
table_pred = db_cfg['database']['tables']['predictions']
model_meta = model_cfg['models'][MODEL_ID]
target_col = model_meta['target_name']

model_dir = utils._resolve_path(model_meta['paths']['model_dir'])
with open(f'{model_dir}/features.json') as f:
    features_data = json.load(f)
with open(f'{model_dir}/model.pkl', 'rb') as f:
    model = pickle.load(f)

feature_list = _feature_list_for_prediction(features_data, model, model_meta.get('trainer', ''))
pred_col     = utils.prediction_col_name(MODEL_ID)
live_cols    = utils.live_prediction_columns()

select_cols = list(dict.fromkeys(['open_time', 'close', target_col] + feature_list))
cols_str    = ', '.join([f'"{c}"' for c in select_cols])

print(f'Syncing {MODEL_ID}: {len(feature_list)} features, chunk={CHUNK_SIZE}')

first_chunk  = True
total_new    = 0
total_update = 0

with sqlite_connect(db_path) as conn:
    for chunk in pd.read_sql_query(
        f'SELECT {cols_str} FROM {table_feat} WHERE open_time >= ? ORDER BY open_time ASC',
        conn,
        params=(START,),
        chunksize=CHUNK_SIZE,
    ):
        chunk = chunk.drop_duplicates(subset=['open_time'], keep='last').copy()
        X     = chunk[feature_list].apply(pd.to_numeric, errors='coerce').fillna(0)
        proba = model.predict_proba(X)[:, 1]

        df_out = chunk[['open_time', 'close']].copy()
        df_out[live_cols['target']] = chunk[target_col]
        df_out[pred_col]            = proba.astype(float)

        if first_chunk:
            ensure_table_columns(db_path, table_pred, df_out)
            first_chunk = False

        update_cols = [pred_col]
        n, u = _write_predictions(db_path, table_pred, df_out, update_cols)
        total_new    += n
        total_update += u
        print(f'  chunk done: +{n} new, ~{u} updated (cumulative: {total_new}+{total_update})')

print(f'Done. Inserted {total_new}, updated {total_update} rows.')
