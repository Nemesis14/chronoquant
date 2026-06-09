# Backlog: Live sync global lock

## Problema

A dashboard auto-sync es a trading service is ugyanazt a DB sync folyamatot
hivja:

- `src/streamlit_app/sync.py -> run_database_sync`
- `src/trading/service.py -> _sync_data -> run_database_sync`

Asset-szintu thread lock mar van a Streamlit sync runnerben, de ez csak az adott
runneren beluli parhuzamos inditasokat fogja meg. A trading service kozvetlenul
hivja a syncet, igy a ket utvonal egymassal versenyezhet.

### Tuntet

SOLUSDT live futas kozben duplikalt `open_time` sorok jelentek meg:

- `solusdt_1m`: ugyanaz az `open_time` tobbszor szerepel
- `solusdt_1m_features`: a duplikalt OHLCV miatt tobbszorozott feature sorok
- `solusdt_1m_predictions`: duplikalt prediction sorok

Ez kulon javitasi tervben is szerepel:

- `docs/plans/backlog/ohlcv_duplicate_rows_fix.md`

### Miert gond

1. A sync lepesek nem tranzakciosan vedettek teljes pipeline szinten.
2. A `drop_existing_open_times` csak read-then-write vedelmet ad; parhuzamos
   folyamatoknal race condition marad.
3. A duplikalt OHLCV sorok torzithatjak a rolling feature szamitast.
4. A prediction es UI lekerdezesek `rowid` vagy `open_time` sorrendben
   felrevezeto eredmenyt adhatnak, ha tobb sor van ugyanarra a percre.

## Javasolt megoldas

Adjunk asset-szintu global sync lockot a `run_database_sync` kore, amely minden
sync hivot ved, nem csak a Streamlit sync runner utvonalat.

### Implementacios irany

1. Hozzunk letre kozos lock registryt a sync layerben, peldaul
   `src/streamlit_app/sync.py` vagy egy uj `src/data_pipeline/sync_lock.py`
   modul alatt.
2. A lock kulcsa legyen `asset_id` vagy a resolved `db_path`.
3. `run_database_sync(asset_id=...)` elejen szerezze meg a lockot.
4. Ha a lock mar foglalt, a hivo:
   - vagy varjon rovid timeouttal;
   - vagy adjon vissza explicit "sync already running" eredmenyt.
5. A trading service es dashboard ugyanazt a vedett `run_database_sync`
   fuggvenyt hasznalja.

### Javasolt API

```python
with acquire_sync_lock(asset_id=asset_id, timeout_seconds=5):
    ...
```

Vagy:

```python
if not try_acquire_sync_lock(asset_id):
    logger.info("Sync already running for asset_id=%s", asset_id)
    return SyncResult(...)
```

## Elfogadasi feltetelek

- Egy assetre egyszerre csak egy OHLCV/features/predictions sync futhat.
- Dashboard auto-sync es trading service parhuzamos futtatasa mellett nem jonnek
  letre uj duplikalt `open_time` sorok.
- A lock elengedese `finally` blokkal garantalt akkor is, ha a sync kozben
  exception tortenik.
- A logokban latszik, ha egy sync futas lock miatt kimarad vagy varakozik.
- A megoldas nem blokkolja mas assetek fuggetlen syncjet.

## Erintett fajlok

- `src/streamlit_app/sync.py` - `run_database_sync` global lockolasa
- `src/streamlit_app/sync_runner.py` - meglavo session lock osszehangolasa
- `src/trading/service.py` - kozvetett hasznalat ellenorzese
- `tests/` - focused teszt a parhuzamos sync lock viselkedesre

## Megjegyzes

Ez nem helyettesiti az adatbazis-szintu vedelmet. A duplikalt sorok vegleges
megelozesehez a `open_time` UNIQUE index es/vagy upsert/insert-ignore logika is
kell, lasd `ohlcv_duplicate_rows_fix.md`.
