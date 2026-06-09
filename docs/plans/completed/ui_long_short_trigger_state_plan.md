# Backlog: UI long/short trigger state separation

## Problema

A SOLUSDT dashboardon a generic `prediction` es `signal` oszlop csak a runtime
modellhez kotodik. Jelenleg a runtime model:

```text
solusdt_fw60 -> lgbm_solusdt_l_fw60_q90_local_v3
```

Ez azt jelenti, hogy:

- `prediction` = long v3 probability
- `signal` = long oldali generic jel, `config/predictions.json` threshold alapjan
- a short v3 probability kulon modell-oszlopban van:
  `lgbm_solusdt_s_fw60_q10_local_v3_p`

A trading service ezzel szemben mindket oldalt explicit olvassa es ertekeli:

- long strategy: `solusdt_long_fw60_q90_local_v3`
- short strategy: `solusdt_short_fw60_q10_local_v3`

Ez UI-ban felrevezeto lehet: ha a user a `signal` mezot nezi, azt hiheti, hogy
short trigger sem volt, mikozben a `signal` eleve nem short-oldali jel.

## Miert gond

1. A generic `signal` nem azonos a trading service dontesevel.
2. A generic signal threshold `config/predictions.json` szerint 0.5, mig a v3
   long/short strategy entry threshold 0.45.
3. A charton vagy status panelen nem egyertelmu, hogy:
   - long score hol all a long entry/rearm/exit kuszobokhoz kepest;
   - short score hol all a short entry/rearm/exit kuszobokhoz kepest;
   - a trading service tenyleges dontese `HOLD`, `ENTER_LONG`,
     `ENTER_SHORT`, stb.

## Javasolt megoldas

A SOLUSDT UI jelenitsen meg kulon long es short trigger allapotot, ne csak a
generic `prediction/signal` mezot.

### Adat layer

`src/streamlit_app/data.py` mar feloldja az aktiv long/short prediction
oszlopokat:

```python
long_pred_col, short_pred_col = _resolve_long_short_pred_cols(...)
```

Ezt boviteni kell ugy, hogy a visszaadott prediction frame egyertelmuen
tartalmazza:

- `long_prediction`
- `short_prediction`
- `long_entry_threshold`
- `short_entry_threshold`
- opcionálisan `long_signal_active`, `short_signal_active`

Fontos: a threshold forrasa a `config/strategies.json` legyen, nem a generic
`config/predictions.json`.

### UI layer

A dashboardon kulon jelenjen meg:

- Long score + entry threshold
- Short score + entry threshold
- Long trigger aktiv/inaktiv
- Short trigger aktiv/inaktiv
- Legutobbi trading service dontes, ha van `trading_signals` adat

A charton a long es short score sajat kuszobvonalat kapjon. A generic `signal`
maradhat, de a label jelezze, hogy az runtime/generic signal, nem teljes
long+short trading decision.

## Elfogadasi feltetelek

- A SOLUSDT dashboardon nem lehet osszekeverni a generic runtime signal-t a
  long/short trading triggerrel.
- A short score es short entry threshold explicit latszik.
- A long score es long entry threshold explicit latszik.
- A status panel vagy chart jelzi, ha `long_prediction >= long_entry_threshold`.
- A status panel vagy chart jelzi, ha `short_prediction >= short_entry_threshold`.
- A trading service legutobbi dontese kulon jelenik meg, ha a `trading_signals`
  tabla elerheto.

## Erintett fajlok

- `src/streamlit_app/data.py` - long/short prediction es threshold mezok
- `src/streamlit_app/main.py` - status panel es trigger megjelenites
- `src/streamlit_app/components/charts.py` - kulon long/short trigger vonalak
- `config/strategies.json` - threshold forras ellenorzese

## Megjegyzes

Ez UI/monitoring feladat. Nem modositsa a trading service dontesi logikajat.
A cel az, hogy a dashboard ugyanazt a fogalmi modellt mutassa, amit a trading
service hasznal: kulon long es short model, kulon strategy threshold, kulon
trigger allapot.
