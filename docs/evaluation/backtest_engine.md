# Backtest Engine

Forrás: `src/evaluation/backtest.py`

A backtest motor modell- és eszközfüggetlen szimulációs keretrendszer.
Bármely bináris valószínűségi modell és bármely OHLCV-alapú eszköz esetén
futtatható, ha a szükséges adatok és modell artifacts elérhetők.

---

## Fő belépési pontok

### `run_configured_strategy(strategy_id: str) -> dict`

Egy stratégiát tölt be `config/strategies.json`-ból és lefuttatja a
visszamérést. Minden artifact-ot automatikusan ment.

```python
from evaluation.backtest import run_configured_strategy
summary = run_configured_strategy("solusdt_long_fw60_q90_local_v3")
```

### `run_strategy_backtest(strategy_id, strategy_cfg) -> dict`

Alacsonyabb szintű belépési pont — ha a strategy config dict már megvan
(pl. sweep-ből). Ugyanúgy ment minden artifact-ot.

### `build_backtest_frame(model_id, start, end, asset_id) -> pd.DataFrame`

Előkészíti a szimulációs keretet: betölti a modell predictions-t és
join-olja az OHLCV adatokkal. Visszaad egy DataFrame-t
`[open_time, open, high, low, close, target, prediction]` oszlopokkal.

### `simulate_long_probability_strategy(frame, strategy_cfg) -> (trades_df, equity_df, summary)`

A tényleges bar-by-bar szimuláció LONG/FLAT stratégiákhoz.
SHORT stratégiák esetén automatikusan `_simulate_short_probability_strategy`-re
irányít.

---

## Adatfolyam

```
config/models.json + config/assets.json
    ↓
model.pkl + features.json betöltése
    ↓
SQLite features tábla → chunked feature olvasás (50 000 sor/chunk)
    ↓
model.predict_proba(X) → prediction oszlop
    ↓
join OHLCV táblával (open_time kulcson)
    ↓
bar-by-bar szimuláció
    ↓
trades.csv + equity_curve.csv + summary.json + strategy_config.json + report.html
```

---

## Szimulációs logika

A szimuláció minden sorban (1-perces gyertya) a következő sorrendben fut:

### Belépés (ha nincs nyitott pozíció)

```
1. Ha not armed ÉS cooldown lejárt ÉS prev_prediction <= rearm_threshold:
       armed = True

2. Ha armed ÉS cooldown lejárt ÉS prev_prediction >= entry_threshold:
       Belépés az aktuális bar OPEN áron
       LONG:  entry_price = open × (1 + slippage)
       SHORT: entry_price = open × (1 - slippage)
       armed = False
```

**Fontos:** a döntés mindig az előző (`i-1`) bar predikciójára épül,
a végrehajtás az aktuális (`i`) bar nyitóárán történik. Soha nem
kereskedünk a még nem lezárt gyertya adatán.

### Kilépés (ha van nyitott pozíció) — prioritás sorrendben

| # | Feltétel | Kiszállási ár | Reason |
|---|---|---|---|
| 1 | `low <= entry × (1 - stop_loss_pct)` | hard_stop ár | `stop_loss` |
| 2 | `high >= entry × (1 + take_profit_pct)` | take_profit ár | `take_profit` |
| 3 | trailing stop aktív ÉS `low <= trailing_stop_price` | trailing_stop_price | `trailing_stop` |
| 4 | `hold_minutes >= max_hold_minutes` | aktuális Close | `max_hold` |
| 5 | `hold_minutes >= min_hold_minutes` ÉS `prev_prediction <= exit_threshold` | következő bar OPEN | `probability_exit` |
| 6 | Backtest vége, pozíció még nyitva | utolsó Close | `end_of_backtest` |

SHORT esetén az 1–3 feltételek tükrözöttek (stop = high >= ..., TP = low <= ...).

### Trailing stop logika

```
Aktiváció:  highest_price >= entry × (1 + trailing_activation_pct)
Stop szint: trailing_stop_price = highest_price × (1 - trailing_stop_pct)
            (mindig felfelé mozog, soha nem csökken)
```

SHORT esetén `lowest_price` és `trailing_cover_price` a megfelelők.

### Cooldown és rearm

```
Zárás után: cooldown_until = exit_time + cooldown_minutes
             armed = False

Újra armed: now >= cooldown_until ÉS prev_prediction <= rearm_threshold
```

A rearm threshold megakadályozza az azonnali újrabelépést akkor, ha
a modell valószínűsége még mindig magas a kilépés után.

---

## Költségmodell

```
fee      = fee_bps_per_side / 10_000      # pl. 10 bps → 0.0010
slippage = slippage_bps_per_side / 10_000 # pl. 2 bps  → 0.0002

LONG net_return  = (exit_price × (1-slip) × (1-fee)) / (entry_raw × (1+slip) × (1+fee)) - 1
SHORT net_return = (entry_raw  × (1-slip) × (1-fee)) / (exit_price × (1+slip) × (1+fee)) - 1

equity *= (1 + net_return)   # tőke kompound módon nő/csökken
```

A `gross_return` a díjak és slippage nélküli mozgást mutatja (csak
diagnosztikai célra).

---

## Kimeneti artifacts

Minden futás `strategy_cfg["output_dir"]` alá ment:

| Fájl | Tartalom |
|---|---|
| `trades.csv` | Minden trade részletei (entry/exit idő, ár, return, reason) |
| `equity_curve.csv` | Equity idősor minden trade után |
| `summary.json` | Aggregált metrikák (win rate, PF, max DD stb.) |
| `strategy_config.json` | A futáshoz használt teljes strategy config snapshot |
| `report.html` | HTML összefoglaló (config + summary + utolsó 50 trade) |

Sweep-ek esetén az összesített eredmény:
`backtests/sweep_<model_id>.csv`

---

## Summary metrikák

A `summarize_trades()` függvény számolja:

| Metrika | Leírás |
|---|---|
| `trade_count` | Trade-ek száma |
| `win_rate` | Nyerő trade-ek aránya |
| `profit_factor` | Nyerők összege / vesztők abszolút összege |
| `max_drawdown` | Maximális csúcstól való visszaesés (equity alapú) |
| `total_return` | `final_equity / initial_equity - 1` |
| `avg_hold_minutes` | Átlagos tartási idő |
| `exposure_pct` | Pozícióban töltött idő aránya a teljes periódushoz |
| `exit_reasons` | Kiszállási okok eloszlása (dict) |

---

## Memóriahatékonyság

A feature olvasás chunked módon történik (alapértelmezett 50 000 sor/chunk),
hogy nagy eszköz-idősoron (pl. 3 millió 1-perces sor × 200 feature) ne
fogyjon el a RAM. A kimeneti frame mindig slim (csak 4 oszlop:
`open_time, close, target, prediction`).

---

## Kiterjesztési pontok

Új coin / modell hozzáadásához szükséges:

1. `config/assets.json` — új asset_id bejegyzés (db_path, táblanevek)
2. `config/models.json` — új model bejegyzés (active=true, trainer, paths)
3. `config/strategies.json` — új strategy bejegyzés az asset_id és model_id-val
4. A backtest motor kód változtatás nélkül futtatható az új konfigon

Új szimulátor típus hozzáadásához (pl. funding rate, position sizing):
- Adj hozzá új `_simulate_*` függvényt a `backtest.py`-ban
- Irányítsd át a `simulate_long_probability_strategy()` dispatch logikájából

---

## Kapcsolodo dokumentumok

- `docs/evaluation/strategy_evaluation.md` - mikor es hogyan futtass backtestet
- `docs/modeling/sampling.md` - adat szetvalasztas, holdout policy
- `docs/business/trading_strategy.md` - SOL-specifikus strategia reszletek
- `docs/architecture/components/live_trading.md` - live trader komponens es runtime state
