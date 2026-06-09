# Backlog: Market data source alignment for spot vs futures

## Problema

A live dashboardon es a kereskedesi osszehasonlitasban a SOLUSDT chartot
Binance Futures Perpetual charttal hasonlitjuk, mikozben a jelenlegi OHLCV
sync `python-binance` spot kline API-t hasznal:

```python
client.get_klines(...)
```

Ez spot `SOLUSDT` gyertyakat ad, nem USD-M Futures `SOLUSDT` perpetual
gyertyakat. A dashboard, feature tabla, prediction tabla es model input jelenleg
ennek az adatforrasnak az eredmenyet mutatja.

## Miert fontos

- A Binance Futures UI-val valo vizualis osszevetes csak akkor korrekt, ha a
  dashboard ugyanazt a futures kline adatforrast hasznalja.
- A trading service Futures mark price/order logikaval dolgozik, ezert a live
  trade kontextus es a model input piaca elterhet.
- A spot es futures gyertyak hasonloak lehetnek, de nem azonosak: open/high/low/
  close, volume es mikrostrukturak elterhetnek.
- Ha a modellek spot OHLCV-bol kepzett feature-okon tanultak, akkor egyszeru
  atkapcsolas futures OHLCV-re adatdisztribucio-valtozast okozhat.

## Jelenlegi megfigyeles

A korabbi nagy candle gap-ek egy resze incomplete kline mentesi hiba volt:
az OHLCV sync elmentette a meg nyitott percet, majd `open_time` utkozes miatt
kesobb nem frissitette. Ez kulon javitasra kerult upsert + csak lezart kline
mentessel.

Ez azonban nem oldja meg a spot vs futures forraselterest. A dashboard tovabbra
is spot klines-t jelenit meg, ha `sync_ohlcv` spot API-n marad.

## Javasolt megoldas

### 1. Konfiguralhato market data source

Az asset configban legyen explicit market tipus:

```json
{
  "symbol": "SOLUSDT",
  "interval": "1m",
  "market": "futures"
}
```

Tamogatott ertekek:

- `spot`: `client.get_klines`
- `futures`: `client.futures_klines`

Alapertelmezett viselkedes maradhat `spot`, hogy a BCH es legacy assetek ne
valtozzanak implicit modon.

### 2. `sync_ohlcv` API valasztas

`src/data_pipeline/sync_ohlcv.py` valassza ki az API-t az asset config alapjan:

```python
if market == "futures":
    rows = client.futures_klines(...)
else:
    rows = client.get_klines(...)
```

### 3. Adatkonzisztencia dontes

El kell donteni, hogy a SOL runtime stack teljesen futures adatforrasra valt-e.
Ha igen:

1. OHLCV tabla ujratoltese vagy kulon futures OHLCV tabla letrehozasa.
2. Feature tabla ujraepitese ugyanarra a forrasra.
3. Prediction tabla ujraszinkronizalasa.
4. Modellek validalasa futures feature-okon.
5. Dashboard ellenorzes Binance Futures chart ellen.

### 4. Tabla es artifact szeparacio

Ket biztonsagos opcio:

- Uj asset id: `solusdt_perp_fw60`
- Vagy uj tabla prefix: `solusdt_perp_1m`, `solusdt_perp_1m_features`,
  `solusdt_perp_1m_predictions`

Ez tisztabb, mint a jelenlegi `solusdt_1m` tabla csendes atirasa, mert a regi
spot-alapu modellek es backtestek provenance-e megmarad.

## Erintett fajlok

- `config/assets.json` - market/source mezo assetenkent
- `src/data_pipeline/sync_ohlcv.py` - spot/futures kline API valasztas
- `src/utils.py` - asset config default/validacio, ha szukseges
- `src/streamlit_app/data.py` - dashboard provenance megjelenites opcionlisan
- `config/models.json` - model provenance ellenorzes
- `config/strategies.json` - futures-alapu runtime strategy frissites csak validacio utan

## Elfogadasi kriteriumok

- A SOL dashboard OHLCV sorai egyertelmuen dokumentalt forrasbol jonnek:
  `spot` vagy `futures`.
- Futures modban a dashboard gyertyai egyeznek a Binance Futures SOLUSDT perp
  chart gyertyaertekeivel ugyanazon UTC timestampen.
- A feature es prediction tablaban nem keveredik spot es futures OHLCV.
- A runtime modelrol egyertelmu, hogy milyen market data source-on keszult.
- A valtast nem lehet csendben megtenni model/backtest validacio nelkul.

## Megjegyzes

Ez nem pusztan UI fix. A market data source a teljes modeling es trading
adatkontrakt resze. Futures tradinghez a futures OHLCV logikusabb, de a valtast
model- es strategy-validacioval kell vegigvinni.
