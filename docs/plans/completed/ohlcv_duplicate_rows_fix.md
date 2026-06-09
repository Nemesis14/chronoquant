# Backlog: OHLCV duplikált sorok javítása

## Probléma

A `solusdt_1m` OHLCV táblában duplikált `open_time` sorok keletkeznek. Példa: Jun 8 22:53–22:57 között minden percnek 2 sora van (10 duplikált sor összesen).

Diagnosztika:
```sql
SELECT open_time, COUNT(*) as cnt
FROM solusdt_1m
GROUP BY open_time
HAVING cnt > 1
ORDER BY open_time DESC;
```

### Hatása

1. **Feature torzítás:** `sync_features` az OHLCV-t `pd.read_sql_query`-val tölti be, majd `set_index("open_time")` után rolling window számításokat végez. Duplikált soroknál ezek a sorok kétszer szerepelnek a rolling ablakban → az érintett időszak körüli `feat_*` értékek enyhén torzítottak.

2. **Row count eltérés:** A features/predictions tábla row száma nem egyezik az OHLCV-vel, ami megnehezíti az adatminőség-ellenőrzést.

3. **JOIN problémák:** Ha az OHLCV táblát más lekérdezésekben JOIN-olják `open_time` alapján, a duplikált sorok szorzódást okozhatnak (pl. diagnosztikai scriptek).

### Gyökérok (valószínű)

A `sync_ohlcv` inkrementális sync során `start_ms`-től tölti le az adatot. Ha egy sync megszakad és újraindul, vagy az overlap-logika nem megfelelő, ugyanaz az intervallum kétszer kerülhet az OHLCV táblába. A predictions/features tábláknál van `drop_existing_open_times` védelem, de az OHLCV táblánál nincs ilyen.

## Javasolt megoldás

### 1. Azonnali védekezés — `sync_ohlcv` upsert vagy pre-check

A `sync_ohlcv.py`-ban az insert előtt ellenőrizni kell, hogy az adott `open_time` már létezik-e:

```python
# Opció A: INSERT OR IGNORE (ha az open_time UNIQUE constraint-tel van indexelve)
conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlcv_time ON solusdt_1m (open_time)")

# Opció B: drop_duplicates a beírás előtt (ha UNIQUE index nem megoldható)
df_new = df_new.drop_duplicates(subset=["open_time"], keep="last")
```

### 2. Védekezés a feature számításban — `sync_features.py`

Az OHLCV fetch után duplikátumok eltávolítása:

```python
df = pd.read_sql_query(...)
df = df.drop_duplicates(subset=["open_time"], keep="last")  # ← hozzáadni
df["open_time"] = pd.to_datetime(df["open_time"])
```

Ez megakadályozza, hogy az esetleg bekerülő OHLCV duplikátumok a rolling window számításokat torzítsák.

### 3. Egyszeri cleanup — meglévő duplikátumok eltávolítása

```sql
-- SQLite: régebbi sort megtartjuk (kisebb rowid)
DELETE FROM solusdt_1m
WHERE rowid NOT IN (
    SELECT MIN(rowid)
    FROM solusdt_1m
    GROUP BY open_time
);
```

Ugyanezt kell futtatni a features és predictions táblákra is, ha ott is vannak duplikátumok.

### 4. UNIQUE index hozzáadása

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_solusdt_1m_open_time ON solusdt_1m (open_time);
CREATE UNIQUE INDEX IF NOT EXISTS idx_solusdt_1m_features_open_time ON solusdt_1m_features (open_time);
CREATE UNIQUE INDEX IF NOT EXISTS idx_solusdt_1m_predictions_open_time ON solusdt_1m_predictions (open_time);
```

Ez után az `INSERT OR IGNORE` vagy `INSERT OR REPLACE` stratégia automatikusan véd.

## Érintett fájlok

- `src/data_pipeline/sync_ohlcv.py` — UNIQUE index vagy pre-check az insert előtt
- `src/data_pipeline/sync_features.py` — `drop_duplicates` az OHLCV fetch után
- `src/db/maintenance.py` — cleanup script kiterjesztése (opcionális)
