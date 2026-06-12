# Prediction Long Short Schema

A prediction reteg stabil long/short target es probability semajanak kialakitasa.

---

## 1. Cel

A `predictions` dataset/tábla minden `open_time` sorban tartalmazza:

| Oszlop | Jelentes | Tipus |
|---|---|---|
| `open_time` | UTC perces timestamp | string/datetime |
| `close` | zaro ar displayhez es joinhoz | float |
| `trg_l_fw60_q90` | long fact target, feature retegbol masolva | int/float nullable |
| `trg_s_fw60_q10` | short fact target, feature retegbol masolva | int/float nullable |
| `long_pred` | aktualis champion long modell probability kimenete | float nullable |
| `short_pred` | aktualis champion short modell probability kimenete | float nullable |

Fontos szabaly:

- A `trg_*` oszlopok fact/label mezok, nem probabilityk.
- A `long_pred` es `short_pred` probability mezok.
- A fogyasztoi retegek nem model ID alapu oszlopneveket olvasnak.
- A backend donti el, melyik champion long es short modell aktiv, es ezek probjait irja a stabil `long_pred` es `short_pred` oszlopokba.

---

## 2. Hatter

A jelenlegi kodban ket minta keveredik:

| Regi minta | Uj cel |
|---|---|
| Model ID-bol kepzett oszlop: `<model_id>_p` | Stabil oszlop: `long_pred`, `short_pred` |
| Generic `target` egy runtime modellhez | Mindket fact target megmarad: `trg_l_*`, `trg_s_*` |
| Modelenkenti kulon upsert `open_time` kulccsal | Egyesitett long/short sor irasa, oszlopvesztes nelkul |
| UI/trading model-specific oszlopokat keres | UI/trading stabil oszlopokat olvas |

A hiba oka: ha `sync_predictions.py` kulon ir long es short modelt ugyanarra az
`open_time` kulcsra, az `upsert_partition()` deduplikacioja felulirhatja az elozo
modell oszlopait. Igy nem garantalt, hogy ugyanabban a sorban megmaradjon mindket
probability.

---

## 3. Erintett Modulok

| Modul | Feladat |
|---|---|
| `src/data_pipeline/sync_predictions.py` | Champion long/short modellek feloldasa, probabilityk egyesitese, stabil oszlopok irasa |
| `src/store/parquet_store.py` | Ellenorizni, hogy az upsert nem nullazza le a hianyzo oszlopokat; ha kell, merge logika javitasa |
| `src/utils.py` | Champion model feloldas helper: long es short irany szerint |
| `config/models.json` | Champion/active model konvencio tisztazasa |
| `config/predictions.json` | Stabil prediction schema dokumentalasa |
| `config/strategies.json` | Strategiak model_id mezoi es champion source osszhangja |
| `src/streamlit_app/data.py` | UI olvasas atallitasa `long_pred` es `short_pred` oszlopokra |
| `src/trading/service.py` | Trading olvasas atallitasa stabil oszlopokra |
| `src/evaluation/backtest.py` | Backtest input tisztazasa: model-specific futas vagy stabil champion pred |
| `src/data_pipeline/map_data_store_pipeline.md` | Diagram es tabla frissitese |

---

## 4. Implementacios Taskok Claude-nak

### 4.1 Champion model feloldas

Feladat:

- Legyen egyertelmu helper, ami assethez visszaadja az aktualis champion long es short modelt.
- A helper ne `asset_id=None` szerint szurjon, hanem elobb oldja fel a default assetet.
- Long modell az, amelynek `target_name` prefixe `trg_l_`.
- Short modell az, amelynek `target_name` prefixe `trg_s_`.

Elfogadas:

- `asset_id=None` es `asset_id="solusdt_fw60"` ugyanazt a default assetet adja.
- Pontosan egy champion long es egy champion short modell legyen feloldhato.
- Ha hianyzik valamelyik irany, ertheto hibauzenet legyen.

### 4.2 `sync_predictions.py` uj irasi logika

Feladat:

- A sync egy tartomanyra olvassa be a `features` datasetet ugy, hogy mindket target oszlop elerheto legyen.
- Szamolja ki a champion long modell probabilityjet.
- Szamolja ki a champion short modell probabilityjet.
- Egy kozos `df_out` keszuljon, ne modelenkent kulon vegleges sor:
  - `open_time`
  - `close`
  - `trg_l_fw60_q90`
  - `trg_s_fw60_q10`
  - `long_pred`
  - `short_pred`
- Csak ez az egyesitett DataFrame menjen `upsert_partition(data_dir, "predictions", df_out)` hivasba.

Elfogadas:

- Egy prediction sorban egyszerre jelen van a long target, short target, long prob es short prob.
- Nincs model ID-bol kepzett `*_p` oszlop a stabil live prediction datasetben.
- A probability oszlopok neve pontosan `long_pred` es `short_pred`.

### 4.3 Store merge vedelme oszlopvesztes ellen

Feladat:

- Ellenorizni kell, hogy `upsert_partition()` hogyan viselkedik, ha a meglevo particio es az uj DataFrame oszloplistaja elter.
- Ne fordulhasson elo, hogy egy kesobbi reszleges update lenullazza vagy eldobja a masik irany prediction oszlopat.
- Ha reszleges update tovabbra is megengedett, akkor oszloponkent merge szabaly kell: uj nem-null ertek felulirhat, hianyzo oszlop ne toroljon regi erteket.

Elfogadas:

- Long-only vagy short-only reszleges input nem torli a masik oldal meglevo ertekeit.
- Teljes long+short `df_out` stabilan idempotens.

### 4.4 UI es chart olvasas

Feladat:

- `src/streamlit_app/data.py` ne model-specific `*_p` oszlopokat keressen a live prediction megjeleniteshez.
- A chart input oszlopai tovabbra is lehetnek `long_prediction` es `short_prediction`, de ezek forrasa:
  - `long_pred` -> `long_prediction`
  - `short_pred` -> `short_prediction`
- A model ID oszlopfeloldas csak model stat/card megjelenitesre maradjon, ne prediction adatforrasra.

Elfogadas:

- Dashboard akkor is mukodik, ha a prediction datasetben nincs `<model_id>_p` oszlop.
- Dashboard long es short panelje a stabil `long_pred` es `short_pred` adatokbol rajzol.

### 4.5 Trading service olvasas

Feladat:

- `src/trading/service.py` ne `self.long_pred_col` es `self.short_pred_col` model ID alapu oszlopokat olvasson a prediction tablabol.
- Stabil oszlopokat olvasson:
  - `p.long_pred AS pred_long`
  - `p.short_pred AS pred_short`
- A strategy config tovabbra is megmondhatja, hogy melyik champion modellhez tartozik a threshold, de az adatforras stabil oszlop legyen.

Elfogadas:

- Trading csak olyan sort vesz figyelembe, ahol `long_pred` es `short_pred` is nem NULL.
- A signal journal tovabbra is tarolja a felhasznalt long es short probabilityt.

### 4.6 Backtest viselkedes tisztazasa

Feladat:

- El kell donteni, hogy a backtest:
  - champion prediction datasetbol dolgozik (`long_pred` vagy `short_pred`), vagy
  - explicit `model_id` alapjan ujraszamolja/olvas model-specific predet.
- Live prediction datasetben ne legyen model-specific oszlop.
- Ha model comparison kell, az kulon evaluation output legyen, ne live `predictions`.

Elfogadas:

- Egy long strategy backtest egyertelmuen `long_pred`-et vagy explicit long model outputot hasznal.
- Egy short strategy backtest egyertelmuen `short_pred`-et vagy explicit short model outputot hasznal.
- A kod kommentjei es hibauzenetei nem hivatkoznak regi `solusdt_1m_predictions` SQLite feltetelezesre, ha mar Parquet az adatforras.

### 4.7 Config es dokumentacio frissites

Feladat:

- `config/predictions.json` `column_docs` resze irja le:
  - `trg_l_fw60_q90`
  - `trg_s_fw60_q10`
  - `long_pred`
  - `short_pred`
- `src/data_pipeline/map_data_store_pipeline.md` ER diagramja a stabil prediction oszlopokat mutassa.
- Ha van adat-doksi, ott is szerepeljen, hogy `trg_*` fact target, `*_pred` probability.

Elfogadas:

- A dokumentaciobol egyertelmuen latszik, hogy a prediction dataset nem model registry.
- A champion modell kivalasztas backend concern, nem schema concern.

### 4.8 Tesztek es validacio

Minimum validacios parancsok:

```powershell
uv run python -m pyright src/data_pipeline/sync_predictions.py
uv run python -m pyright src/store/parquet_store.py
uv run pytest
```

Javasolt adatvalidacio:

```python
from store.duckdb_query import query_range
import utils

data_dir = utils.load_asset_config("solusdt_fw60")["database"]["data_dir"]
df = query_range(data_dir, "predictions", columns=[
    "open_time", "trg_l_fw60_q90", "trg_s_fw60_q10", "long_pred", "short_pred"
])
print(df[["long_pred", "short_pred"]].notna().mean())
print(df.tail())
```

Elfogadas:

- `predictions` schema tartalmazza mind az ot kotelezo uzleti oszlopot: `open_time`, `trg_l_fw60_q90`, `trg_s_fw60_q10`, `long_pred`, `short_pred`.
- `long_pred` es `short_pred` ugyanazon sorokban tud nem NULL lenni.
- Re-run utan a sorszam nem duplikalodik `open_time` szerint.
- Re-run utan a masik oldal predictionje nem tunik el.

---

## 5. Kockazatok

| Kockazat | Kezeles |
|---|---|
| SQLite es Parquet fogyasztok keverednek | Eloszor donteni kell, melyik a live source of truth |
| Model-specific oszlopokhoz kotott regi UI kod | Stabil `long_pred` es `short_pred` adapterrel frissiteni |
| Reszleges upsert oszlopvesztest okoz | Store merge teszt es javitas |
| Champion definicio nem explicit | Config/helper szinten egyertelmu long es short champion feloldas |

---

## 6. Nyitott Dontesek

- A live `predictions` dataset Parquet legyen-e az egyetlen source of truth, vagy kell ideiglenes SQLite kompatibilitasi adapter?
- A `target` generic oszlop teljesen kivezetheto-e a live predictionbol?
- Kell-e `champion_model_long` es `champion_model_short` metadata oszlop vagy ez maradjon csak config/journal szinten?
