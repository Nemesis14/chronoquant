# Database — DuckDB Schema

A ChronoQuant egyetlen DuckDB fájlban tárolja az összes piaci adatot, feature-t és predikciót egy assetenként.

---

## Áttekintés

```mermaid
erDiagram
    ohlcv {
        TIMESTAMP open_time PK
        DOUBLE open
        DOUBLE high
        DOUBLE low
        DOUBLE close
        DOUBLE volume
        DOUBLE quote_volume
        BIGINT trades
        DOUBLE taker_buy_base
        DOUBLE taker_buy_quote
    }

    target {
        TIMESTAMP open_time PK
        DOUBLE close
        DOUBLE long_mfe_fw60 "log(max_fw60/close) — LONG TARGET"
        DOUBLE short_mfe_fw60 "log(min_fw60/close) — SHORT TARGET"
        DOUBLE fw60_close "close[t+60]"
        DOUBLE fw60_max "max(close[t+1..t+60])"
        DOUBLE fw60_min "min(close[t+1..t+60])"
        DOUBLE fw60_close_ret "simple return"
        DOUBLE fw60_close_logret "log return"
        DOUBLE fw60_max_ratio "max/close"
        DOUBLE fw60_min_ratio "min/close"
    }

    feat_ohlcv_quant {
        TIMESTAMP open_time PK
        DOUBLE close
        TIMESTAMP available_ts
        TIMESTAMP lookback_end_ts
        DOUBLE feat_cols "feat_* oszlopok (config-driven, ~100+)"
    }

    predictions {
        TIMESTAMP open_time PK
        DOUBLE close
        TIMESTAMP label_end_ts
        DOUBLE long_mfe_fw60 "fw60 long outcome (NULL in live sync)"
        DOUBLE short_mfe_fw60 "fw60 short outcome (NULL in live sync)"
        DOUBLE long_pred "long model prediction score"
        DOUBLE short_pred "short model prediction score"
        VARCHAR long_model_id "model ID stamp"
        VARCHAR short_model_id "model ID stamp"
    }

    quant_train {
        TIMESTAMP open_time PK
        DOUBLE feat_cols "összes feat_* oszlop (feat_ohlcv_quant-ból)"
        DOUBLE long_mfe_fw60 "fw60 long outcome"
        DOUBLE short_mfe_fw60 "fw60 short outcome"
    }

    feat_levels {
        TIMESTAMP open_time PK
        TIMESTAMP available_ts "= open_time"
        DOUBLE feat_cols "50 db feat_* (S/R szint + trendvonal, 1m, t-1 lag)"
    }

    scout_signals {
        VARCHAR event_id PK "asset|W4h_H8h|t0"
        TIMESTAMP t0
        TIMESTAMP t_c "predikció ideje"
        DOUBLE p_impulse "immutable predikció"
        VARCHAR status "open / resolved / invalid"
        TIMESTAMP scored_at "írás ideje"
        BOOLEAN backfilled
    }

    ohlcv ||--o{ target : "open_time"
    ohlcv ||--o{ scout_signals : "scout.run (detect_anchor-tól)"
    ohlcv ||--o{ feat_levels : "open_time (detektor, teljes történet)"
    feat_levels ||--o{ quant_train : "LEFT JOIN open_time (challenger quant_train__&lt;profile&gt; only)"
    ohlcv ||--o{ feat_ohlcv_quant : "open_time"
    feat_ohlcv_quant ||--o{ predictions : "available_ts ASOF join"
    feat_ohlcv_quant ||--o{ quant_train : "INNER JOIN open_time"
    target ||--o{ quant_train : "INNER JOIN open_time"
```

Minden tábla `open_time` TIMESTAMP primary key-en alapul (kivétel: `scout_signals`, PK `event_id`; `funding_rate`, kulcs `funding_ts`). Az összes timestamp **UTC**, `YYYY-MM-DD HH:MM:SS` formátumban tárolva.

**DuckDB fájl helye:** `database/<asset_id>/<asset_id>.duckdb`

Aktív asset: `solusdt` → `database/solusdt/solusdt.duckdb`

Az elérési út mindig a `config/assets.json` → `utils.load_asset_config(asset_id)` → `db_path` mezőjéből jön.

---

## Táblák

### ohlcv

**Cél:** Nyers, változatlan Binance 1-perces kline adatok. A pipeline alapja — minden downstream tábla ebből épül fel.

**Beírási mód:** append-only. Csak a tárolt `MAX(open_time)`-nál újabb sorok kerülnek be (`_insert_append_only`). Nincs upsert, nincs törlés.

| Oszlop | Típus | Leírás |
|--------|-------|--------|
| `open_time` | `TIMESTAMP` (PK) | Gyertya nyitásának időpontja, UTC. Minden sor egyedi. |
| `open` | `DOUBLE` | Nyitóár USDT-ben |
| `high` | `DOUBLE` | Legmagasabb ár az 1 perces ablakban |
| `low` | `DOUBLE` | Legalacsonyabb ár az 1 perces ablakban |
| `close` | `DOUBLE` | Záróár USDT-ben |
| `volume` | `DOUBLE` | Forgalom base asset-ben (SOL) |
| `quote_volume` | `DOUBLE` | Forgalom quote asset-ben (USDT) |
| `trades` | `BIGINT` | Kötések száma az 1 perces ablakban |
| `taker_buy_base` | `DOUBLE` | Taker vevő forgalom base asset-ben (SOL) |
| `taker_buy_quote` | `DOUBLE` | Taker vevő forgalom quote asset-ben (USDT) |

**Nem tárolt Binance mezők:** `close_time` (redundáns), `ignore` (deprecated).

```sql
CREATE TABLE IF NOT EXISTS ohlcv (
    open_time       TIMESTAMP PRIMARY KEY,
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,
    volume          DOUBLE,
    quote_volume    DOUBLE,
    trades          BIGINT,
    taker_buy_base  DOUBLE,
    taker_buy_quote DOUBLE
)
```

---

### target

**Cél:** Folytonos forward logreturn outcome-ok (fw60). Az `ohlcv.close` alapján, DuckDB SQL ablakfüggvényekkel számítva. Teljes rebuild minden `sync_targets` híváskor.

**Beírási mód:** DELETE + INSERT a teljes tartományra (`insert_target`). Az előre definiált időablakban (`ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING`) az aktuális bar (`t`) NEM szerepel a forward window-ban.

**NULL sorok:** Az utolsó 60 sor minden fw60 outcome oszlopban `NULL` — nincs elegendő jövőbeli adat.

**Kód referencia:** [`_doc_/database_and_code_doc/3100_sync_targets.md`](3100_sync_targets.md) | **Metodológia:** [`_doc_/methodology_doc/2000_targets.md`](../methodology_doc/2000_targets.md)

| Oszlop | Típus | Leírás |
|--------|-------|--------|
| `open_time` | `TIMESTAMP` (PK) | Bar nyitási ideje, UTC |
| `close` | `DOUBLE` | Bar záróára (referencia close[t]) |
| `fw60_close` | `DOUBLE` | close[t+60] — nyers forward close |
| `fw60_max` | `DOUBLE` | max(close[t+1:t+60]) |
| `fw60_min` | `DOUBLE` | min(close[t+1:t+60]) |
| `fw60_close_ret` | `DOUBLE` | close[t+60] / close[t] − 1 |
| `fw60_close_logret` | `DOUBLE` | log(close[t+60] / close[t]) |
| `fw60_max_ratio` | `DOUBLE` | max(close[t+1:t+60]) / close[t] |
| `fw60_min_ratio` | `DOUBLE` | min(close[t+1:t+60]) / close[t] |
| **`long_mfe_fw60`** | **`DOUBLE`** | **log(max(close[t+1:t+60]) / close[t]) — LONG TARGET** |
| **`short_mfe_fw60`** | **`DOUBLE`** | **log(min(close[t+1:t+60]) / close[t]) — SHORT TARGET** |

```sql
CREATE TABLE IF NOT EXISTS target (
    open_time        TIMESTAMP PRIMARY KEY,
    close            DOUBLE,
    fw60_close       DOUBLE,
    fw60_max         DOUBLE,
    fw60_min         DOUBLE,
    fw60_close_ret   DOUBLE,
    fw60_close_logret DOUBLE,
    fw60_max_ratio   DOUBLE,
    fw60_min_ratio   DOUBLE,
    long_mfe_fw60    DOUBLE,
    short_mfe_fw60   DOUBLE
)
```

---

### feat_ohlcv_quant

**Cél:** Technikai indikátorok és feature-ök (Polars LazyFrame pipeline). Séma config-driven — az oszlopok száma és neve a `config/features.json` indikátor konfigurációjától függ (~100+ `feat_*` oszlop).

**Beírási mód:** append-only (`_insert_append_only`). A séma automatikusan bővül az első `insert_feat_ohlcv_quant` hívásnál (tábla létrehozása) és ha új feature oszlopok jelennek meg.

**t-1 lag:** Minden OHLCV-alapú feature 1 barral el van tolva (`shift(1)`) a `compute_features_polars` hívásban. A P2 (időindexes) feature-ök kivételek — ezek nem kerülnek eltolásra (`T_MINUS_1_SKIP`).

| Oszlop | Típus | Leírás |
|--------|-------|--------|
| `open_time` | `TIMESTAMP` (PK) | Bar nyitási ideje, UTC |
| `close` | `DOUBLE` | Bar záróára (referencia) |
| `available_ts` | `TIMESTAMP` | Mikor válik elérhetővé a feature (== `open_time`, t-1 lag garantált) |
| `lookback_end_ts` | `TIMESTAMP` | A lookback ablak vége (== `open_time`) |
| `feat_*` | `DOUBLE` / `BOOLEAN` | Config-driven feature oszlopok, `feat_` prefix |

Az ASOF join (`predictions` ↔ `feat_ohlcv_quant`) az `available_ts` oszlopon alapul: `p.open_time >= f.available_ts`.

---

### predictions

**Cél:** Champion modellek által generált predikciós pontszámok. Egy sor per `open_time`, unified long+short output.

**Beírási mód:** append-only (`_insert_append_only`). A séma rögzített — `ensure_tables` hozza létre.

| Oszlop | Típus | Leírás |
|--------|-------|--------|
| `open_time` | `TIMESTAMP` (PK) | Bar nyitási ideje, UTC |
| `close` | `DOUBLE` | Bar záróára (az `ohlcv.close`-val egyezik) |
| `label_end_ts` | `TIMESTAMP` | A forward window vége: `open_time + fw_minutes` |
| `long_mfe_fw60` | `DOUBLE` | Fw60 long outcome (target join-ból; live sync esetén `NULL`) |
| `short_mfe_fw60` | `DOUBLE` | Fw60 short outcome (target join-ból; live sync esetén `NULL`) |
| `long_pred` | `DOUBLE` | Long modell predikciós értéke (`predict_proba` vagy `predict`, config szerint) |
| `short_pred` | `DOUBLE` | Short modell predikciós értéke (`predict_proba` vagy `predict`, config szerint) |
| `long_model_id` | `VARCHAR` | A long predikciót generáló model ID stamp |
| `short_model_id` | `VARCHAR` | A short predikciót generáló model ID stamp |

```sql
CREATE TABLE IF NOT EXISTS predictions (
    open_time       TIMESTAMP PRIMARY KEY,
    close           DOUBLE,
    label_end_ts    TIMESTAMP,
    long_mfe_fw60   DOUBLE,
    short_mfe_fw60  DOUBLE,
    long_pred       DOUBLE,
    short_pred      DOUBLE,
    long_model_id   VARCHAR,
    short_model_id  VARCHAR
)
```

**Legacy oszlopok:** `dataset_split`, `fold_id`, valamint a régi `trg_*` bináris target oszlopok — verziózott migrációk (v3, v4) távolítják el `ALTER TABLE DROP COLUMN`-nal.

---

### quant_train

**Cél:** Model-ready join tábla — az összes `feat_*` feature és a két aktív fw60 target oszlop (`long_mfe_fw60`, `short_mfe_fw60`) egyetlen lekérdezhető táblaként. A feature engineering, sampling és LightGBM tanítás kiindulópontja.

**Forrás:** `feat_ohlcv_quant` INNER JOIN `target` ON `open_time`. NULL target sorok kizárva.

**Beírási mód:** Ad-hoc rebuild, NEM a live sync pipeline része. Tanítás előtt futtatandó:
- Full rebuild: `CREATE OR REPLACE TABLE` (determinisztikus)
- Range rebuild: `DELETE + INSERT` a megadott `open_time` ablakra

**CLI:** `uv run python src/data_handling/03_build_quant_train.py [--start YYYY-MM-DD] [--end YYYY-MM-DD]`

**Kód referencia:** [`_doc_/database_and_code_doc/4100_quant_train.md`](4100_quant_train.md)

| Oszlop | Típus | Leírás |
|--------|-------|--------|
| `open_time` | `TIMESTAMP` (PK) | Bar nyitási ideje, UTC. Egyedi — INNER JOIN garantálja. |
| `feat_*` | `DOUBLE` | Az összes `feat_ohlcv_quant`-ban szereplő feature oszlop (t-1 lag már alkalmazva) |
| `long_mfe_fw60` | `DOUBLE` | Fw60 long outcome: `log(max_fw60 / close[t])`. NULL sorok kizárva. |
| `short_mfe_fw60` | `DOUBLE` | Fw60 short outcome: `log(min_fw60 / close[t])`. NULL sorok kizárva. |

**Challenger tábla:** a `quant_train__<profile_id>` (pl. `quant_train__solusdt_fw60_sr`) ugyanezt a sorhalmazt adja, a `feat_levels` `feat_*` oszlopaival kiegészítve (LEFT JOIN). Részletek: [4100_quant_train.md](4100_quant_train.md) (Challenger profil szakasz).

---

### feat_levels

**Cél:** S/R szint- és trendvonal-feature-ök 1m baronként (epic_071 challenger). A champion `feat_ohlcv_quant` nem olvassa.

**Forrás:** a `levels_detector` (config v1) detektor a teljes `ohlcv` történeten; a sorok `t-1` lag-gel, `open_time` szerint.

**Beírási mód:** idempotens upsert (`INSERT OR REPLACE`, PK `open_time`). Sync: `sync_levels.py` / `02_sync_pipeline.py --tables levels`.

| Oszlop | Típus | Leírás |
|--------|-------|--------|
| `open_time` | `TIMESTAMP` (PK) | Bar nyitási ideje, UTC |
| `available_ts` | `TIMESTAMP` | = `open_time` (konvenció, mint `feat_ohlcv_quant`) |
| `feat_*` | `DOUBLE` | 50 db feature; NULL = nincs az adott objektum `T`-ben. Lista és definíciók: [1200_sync_tables.md](1200_sync_tables.md) |

**Kód referencia:** [`1200_sync_tables.md`](1200_sync_tables.md) (`_levels_features.py`, `sync_levels.py`), migráció: [`1110_duckdb_store.md`](1110_duckdb_store.md) (v6).

### scout_signals

**Cél:** az Impulse Scout (`scout_s0c_r1`, epic_074) **forward-logja**: minden megerősített mélypont-esemény egy
sor, a predikció a kimenet előtt íródik és utána nem módosul. Kísérleti, NO-GO — nem kereskedési jel, a quant modell
nem olvassa. Minden live DB-ben létrejön (migráció v8); írva SOL / BTC / ETH-re.

**Forrás:** `scout.run(asset_id, ohlcv_1m, artifact)` ([5800](5800_scout_runtime.md)) az `ohlcv` táblán,
`detect_anchor`-tól (2026-07-24); események `t0 >= live_start` (2026-11-01).

**Beírási mód:** `sync_scout.py` (egyetlen író) — új esemény INSERT; meglévő sornál csak a megengedett mezők
frissülnek (lásd lent). Sor soha nem törlődik. Sync: `02_sync_pipeline.py --tables scout` (opt-in) / worker (t942).

| Oszlopcsoport | Oszlopok | Szabály |
|---|---|---|
| kulcs, geometria | `event_id` (PK, `asset\|cell\|t0`), `asset_id, model_id, cell_id, t0, t_c, label_end, low_t0, close_tc, a_log, sigma_w, a_sigma, p_star, stop` | **immutable** |
| feature-ök | `velocity_1h, pretrend_3d, log_sigma_1m, drop_depth, volume_ratio, scorable` | **immutable** |
| predikció | `p_impulse, p_false_start, p_sideways, oof_percentile, top20, top10` | **immutable** (NULL, ha `scorable = false`) |
| tervek | `entry`, `default_tp, default_sl, r2_03_tp, r2_03_sl` | set-once (csak NULL → érték) |
| kimenet | `{default,r2_03}_{outcome, outcome_at, exit_price, net_bps}`, `status`, `label_class` | egyszer, `status`: `open` → `resolved` / `invalid` |
| meta | `as_of` | frissül, amíg `status = open`; utána fagyott |
| író | `scored_at` (TIMESTAMP, beszúrás ideje, UTC), `backfilled` (BOOLEAN) | insertkor, utána soha |

Az oszlopok forrása a `scout.OUTPUT_SCHEMA` (+ `scored_at`, `backfilled`); a típusokat a
`duckdb_store.scout_signals_columns()` képzi. Eltérő runtime séma (új artifact-verzió) → új migráció kell; a sync
addig hibát dob.

**`backfilled`:** `t_c < first_live_sync_at`, ahol `first_live_sync_at` = `MIN(scored_at)` az asset táblájában (az első
sikeres írás ideje; az első futás a saját `now`-ját használja). Az első éles futás előtt lezárt (`t_c`) események
backfill-ek, a későbbiek valódi forward predikciók. Az írási késés soronként: `scored_at − t_c`.
Első éles futás: 2026-11-23 07:33 UTC (SOL 8, BTC 9, ETH 8 esemény, mind `backfilled = true`).

**Kód referencia:** [`1200_sync_tables.md`](1200_sync_tables.md) (`sync_scout.py`), migráció:
[`1110_duckdb_store.md`](1110_duckdb_store.md) (v8), runtime: [`5800_scout_runtime.md`](5800_scout_runtime.md).

---

> **Megjegyzés:** A `quant_train` nem tartalmaz `close`, `available_ts`, `label_end_ts` vagy predikció oszlopokat.
> A legacy `trg_*` boolean target elnevezés NEM kerül felhasználásra ebben a rétegben.

---

## Általános konvenciók

| Szabály | Részlet |
|---------|---------|
| **Timestamp formátum** | UTC, `YYYY-MM-DD HH:MM:SS` (naiv string, UTC-ként értelmezve) |
| **Epoch ms** | Csak Binance API-val való kommunikációban, nem tárolva |
| **Config gateway** | Mindig `utils.load_asset_config(asset_id)` → `db_path` |
| **Idempotens upsert** | Minden sync operáció biztonságosan újrafuttatható |
| **Zonemap** | Sorok `open_time` szerint rendezve kerülnek be (DuckDB range query optimalizálás) |
