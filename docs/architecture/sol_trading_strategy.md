# SOL/USDT Trading Strategy — Összefoglaló

Dátum: 2026-06-08

Ez a dokumentum egy helyen foglalja össze a SOLUSDT automata kereskedési
rendszer teljes logikáját: mit mér a modell, hogyan készült a visszamérés,
mik a stratégia paraméterei, és hogyan kapcsolódik ez a live tradinghez.

---

## 1. Mit jósol a modell?

Két független LightGBM bináris osztályozó fut párhuzamosan.

### 1a. Long modell — `lgbm_solusdt_l_fw60_q90_local_v3`

**Target:** `trg_l_fw60_q90`

Az adott 1-perces gyertya bezárása utáni **60 percen belül** az ár eléri-e
a következő 60 perc **90. percentilis** hozamát? Tehát: várható-e erős
emelkedés az elkövetkező 1 órában?

- 1 = igen (áremelkedés a 90. percentilis fölé)
- 0 = nem

**Kimenet:** 0–1 közötti valószínűség (`prediction`). Minél nagyobb, annál
valószínűbb az emelkedés.

### 1b. Short modell — `lgbm_solusdt_s_fw60_q10_local_v3`

**Target:** `trg_s_fw60_q10`

Az adott gyertya után 60 percen belül az ár eléri-e a következő 60 perc
**10. percentilis** hozamát? Tehát: várható-e erős esés az elkövetkező
1 órában?

- 1 = igen (áresés a 10. percentilis alá)
- 0 = nem

**Kimenet:** 0–1 közötti valószínűség. Minél nagyobb, annál valószínűbb
az esés.

### Miért 90/10 percentilis?

A célváltozó nem azt kérdezi, hogy „emelkedik-e az ár". Azt kérdezi, hogy
„a következő 60 perc top 10%-ába kerülő emelkedés fog-e bekövetkezni".
Ez szűri a kis, zajos mozgásokat és csak az erős trendeket jelzi.

---

## 2. Modell architektúra

| | Long v3 | Short v3 |
|---|---|---|
| Modell ID | `lgbm_solusdt_l_fw60_q90_local_v3` | `lgbm_solusdt_s_fw60_q10_local_v3` |
| Algoritmus | LightGBM bináris osztályozó | LightGBM bináris osztályozó |
| Feature-ök száma | 200 | 200 |
| Training CV | 5-fold expanding-window | 5-fold expanding-window |
| row_stride | 60 (nem minden percet tanít, hanem minden 60.) | 60 |
| Train/valid PR AUC | 0.478 / 0.363 | 0.410 / 0.350 |
| Train/valid ROC AUC | 0.867 / 0.814 | 0.836 / 0.809 |
| Train cut-off | 2025-06-04 | 2025-06-04 |
| Holdout időszak | 2025-06-05 – 2026-06-05 | 2025-06-05 – 2026-06-05 |

**Feature-ök:** 200 technikai indikátor (momentum, volatilitás, trend,
volume-alapú, session, Elliott-wave stb.) — részletes lista:
`models/lgbm_solusdt_l_fw60_q90_local_v3/search/features_search.json`

**Hyperparameter keresés:** Optuna-alapú search (explore 60 trial, 5-fold
CV), majd refine fázis. Legfontosabb anti-overfit beállítások:
- shallow tree (`num_leaves=9`, `max_depth=4`)
- `extra_trees=True` (randomizált splitek)
- `min_child_samples=392` (konzervatív levélméret)
- erős regularizáció (`reg_lambda=8+`)

---

## 3. Visszamérés (backtest) logikája

A visszamérés forrása: `src/evaluation/backtest.py`

### Adatfolyam

```
Adatbázis (solusdt_data_dev.db)
  solusdt_1m_features tábla
      ↓  (feature_list a model artifacts-ból)
  LightGBM model → prediction valószínűség minden 1-perces gyertyára
      ↓
  join OHLCV-vel (open, high, low, close)
      ↓
  simulate_long/short_probability_strategy()
      ↓
  trades.csv + equity_curve.csv + summary.json + report.html
```

### Szimulációs logika (bar-by-bar, 1-perces felbontásban)

A szimuláció **minden lezárt 1-perces gyertyán** fut végig, nem csak
1 óránként. Az 1h-s jelleg abból jön, hogy a target 60 perces előretekintést
mér, de a belépési/kilépési döntések percenként értékelődnek.

**Belépési logika (Long):**

```
Előző gyertya predikciója >= entry_threshold (0.45)
ÉS strategy armed == True
ÉS cooldown lejárt
→ Belépés a következő gyertya OPEN áron + slippage
→ armed = False
```

**Belépési logika (Short — tükrözött):**

```
Ugyanaz, de entry_raw × (1 - slippage) → short pozíció
P&L pozitív ha az ár esik belépés után
```

**Kilépési sorrend (prioritás):**

| # | Feltétel | Kiszállás |
|---|---|---|
| 1 | Hard stop loss (ha be van állítva) | LONG: low <= entry×(1-sl%) |
| 2 | Take profit (ha be van állítva) | LONG: high >= entry×(1+tp%) |
| 3 | Trailing stop (ha aktiválva) | legmagasabb ár × (1-trail%) |
| 4 | Max tartási idő lejárt | Close áron |
| 5 | Prob exit: pred <= exit_threshold ÉS min_hold eltelt | Következő bar OPEN |
| 6 | Backtest vége (nyitott pozi maradt) | Utolsó Close |

**Rearm logika:**

```
Zárás után cooldown_minutes ideig nem léphet be.
Cooldown után: prediction <= rearm_threshold kell az armed=True-hoz.
Ez megakadályozza az újrabelépést azonnal magas valószínűség esetén.
```

**Költségmodell:**

```
fee = fee_bps_per_side / 10000   (mindkét oldalon)
slippage = slippage_bps_per_side / 10000

Long net_return = (exit × (1 - slip) × (1 - fee)) / (entry × (1 + slip) × (1 + fee)) - 1
Short net_return = (entry × (1 - slip) × (1 - fee)) / (exit × (1 + slip) × (1 + fee)) - 1
```

---

## 4. Stratégia paraméterek

Az aktív v3 stratégiák (`config/strategies.json`):

| Paraméter | Long v3 | Short v3 |
|---|---|---|
| Stratégia ID | `solusdt_long_fw60_q90_local_v3` | `solusdt_short_fw60_q10_local_v3` |
| Modell | `lgbm_solusdt_l_fw60_q90_local_v3` | `lgbm_solusdt_s_fw60_q10_local_v3` |
| entry_threshold | **0.45** | **0.45** |
| rearm_threshold | 0.18 | 0.18 |
| exit_threshold | 0.10 | 0.10 |
| min_hold_minutes | 5 | 5 |
| max_hold_minutes | **120** | **120** |
| take_profit_pct | 0.0 (nincs) | 0.0 (nincs) |
| stop_loss_pct | 0.0 (nincs) | 0.0 (nincs) |
| cooldown_minutes | **60** | **60** |
| fee_bps_per_side | 10 bps | 10 bps |
| slippage_bps_per_side | 2 bps | 2 bps |

**Miért nincs take profit?** A sweep megmutatta, hogy take profit nélkül
jobb a profit factor. A modell valószínűsége jobban jelzi a kilépési
időpontot, mint egy fix árcél.

---

## 5. Holdout eredmények (érintetlen teszt, 2025-06-05 – 2026-06-05)

### Long v3

| Metrika | Érték |
|---|---|
| Időszak | 2025-06-05 – 2026-06-05 (1 év, érintetlen holdout) |
| Trade-ek száma | 180 |
| Win rate | **82.2%** (148 nyerő / 32 vesztes) |
| Total return | **+499.1%** (10 000 → 59 905 USDT) |
| Max drawdown | -6.6% |
| Entry küszöb | 0.45 |
| Max tartás | 120 perc |

### Short v3

| Metrika | Érték |
|---|---|
| Időszak | 2025-06-05 – 2026-06-05 (1 év, érintetlen holdout) |
| Trade-ek száma | 94 |
| Win rate | **72.3%** (68 nyerő / 26 vesztes) |
| Total return | **+118.6%** (10 000 → 21 862 USDT) |
| Max drawdown | -4.8% |
| Entry küszöb | 0.45 |
| Max tartás | 120 perc |

**Fontos megjegyzés:** A holdout eredmények a stratégia kiválasztása
ELŐTT rögzített, érintetlen adaton mértek. A sweep és a threshold
kiválasztás a 2024-01-01 – 2025-06-04 pre-holdout perióduson történt.

---

## 6. Adatszétválasztás (sampling)

```
Teljes adat:    2021-08 – 2026-06 (közel 3 millió 1-perces gyertya)
                │
                ├── Research (pre-holdout): 2021-08 – 2025-06-04
                │     └── 5-fold expanding-window CV
                │           Fold 1: train → 2022-08, valid → 3 hónap
                │           Fold 2–5: egyre bővülő train ablak
                │
                └── Holdout (ÉRINTETLEN): 2025-06-05 – 2026-06-05
                      → Csak a végső ellenőrzéshez, threshold sweep UTÁN
```

**Embargo:** A forward-looking target (60 perc) miatt az összes fold
között 60-perces embargo van, hogy ne "szivárogjon" a jövőbeli adat
a validációba.

---

## 7. Live trading kapcsolat

A live automata trader pontosan ezt a szimulációs logikát hajtja végre
valós piacon:

| Backtest elem | Live megfelelője |
|---|---|
| Szimuláció bar-by-bar | 60-másodperces polling loop |
| `prev_prediction >= 0.45` | sync_predictions() → DB → döntés |
| Entry = következő bar OPEN | MARKET order Binance Futures-on |
| Exit = OPEN/CLOSE | MARKET order |
| fee 10 bps + slippage 2 bps | Valós Binance díj (~4 bps maker/taker) |

**Pozíció méret live-ban:**
- Saját tőke: $10 / trade
- Tőkeáttétel: 10×
- Notionális méret: $100 SOLUSDT Perpetual Futures
- Margin mód: Cross margin

**Prioritás ha egyszerre triggerel Long és Short:** Long élvez elsőbbséget.
Csak egy pozíció lehet nyitva egyidejűleg.

**Exit feltételek live-ban (sorrendben):**
1. Ellentétes jel triggerelődik → azonnali zárás
2. Max tartás (120 perc) elérésekor → zárás
3. prediction < 0.10 ÉS min 5 perce nyitva → zárás

---

## 8. Artifact helyek

| Artifact | Hol van |
|---|---|
| Stratégia paraméterek | `config/strategies.json` |
| Long modell | `models/lgbm_solusdt_l_fw60_q90_local_v3/` |
| Short modell | `models/lgbm_solusdt_s_fw60_q10_local_v3/` |
| Modell holdout számok | `models/*/model_card.json` |
| Backtest motor kód | `src/evaluation/backtest.py` |
| Sweep eredmények | `backtests/sweep_lgbm_solusdt_*_local_v3.csv` |
| Feature lista | `models/*/search/features_search.json` |
| Fejlesztési log (long v3) | `docs/plans/completed/` (lgbm_stability_*) |
| Fejlesztési log (short v3) | `docs/plans/completed/solusdt_short_lgbm_local_v2_plan.md` |
| Live trading terv | `docs/plans/backlog/live_trading_plan.md` |
| Ez a dokumentum | `docs/architecture/sol_trading_strategy.md` |
