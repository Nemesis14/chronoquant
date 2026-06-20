---
epic: epic_026
id: t2
title: Strategy calibration methodology dokumentáció
assignee: methodology_agent
status: todo
blocks: [t3]
blocked_by: []
---

## Goal

Írni egy `_doc_/` oldalt, amely lefekteti a strategy calibration modul methodológiai alapjait.
Ez a doc lesz t3 (modul implementáció) alapja — kód előtt kell elkészülnie.

## Scope

Új fájl: `_doc_/6000_strategy_calibration_methodology.md` (vagy appropriate sorszám)

## Tartalom

A következő kérdésekre kell választ adni:

### 1. Score calibration — miért és hogyan

- A long és short modell raw regression score-jai különböző skálán lehetnek
- Isotonic regression: `raw_score → E[mfe | raw_score]`
  - Fit: calibration perióduson (történeti adat)
  - Apply: evaluation/live perióduson
- Miért isotonic: monoton feltétel teljesül, non-parametric, continuous target-re természetes
- Hogyan kezeljük a két modellt: külön isotonic per irány (long és short független)

### 2. Strategy table séma

```
open_time           TIMESTAMP
pred_long_raw       FLOAT   -- raw model output
pred_short_raw      FLOAT   -- raw model output
pred_long_cal       FLOAT   -- isotonic calibrated E[long_mfe | score]
pred_short_cal      FLOAT   -- isotonic calibrated E[short_mfe | score]
long_mfe_fw60       FLOAT   -- actual target
short_mfe_fw60      FLOAT   -- actual target
```

### 3. Entry/exit/cooldown logika elvei

- Mikor lépünk be: `pred_long_cal > entry_threshold_long` VAGY `pred_short_cal > entry_threshold_short`
- Prioritás: ha mindkettő tüzel, melyik irány preferált (vagy mindkettő tiltott?)
- Tartási idő: max hold time, vagy exit signal alapján
- Cooldown: minimum idő két trade között (overfitting megakadályozása)
- Stop-loss / take-profit: opcionális, de a sweep-nek ki kell tudnia kapcsolni

### 4. Optimalizáció objektívuma

- Target: continuous MFE (nem bináris win/loss)
- Objective jelöltek:
  - `mean(actual_mfe | entry fired)` — átlagos realized MFE az entryknél
  - `sharpe(per-trade pnl)` — kockázat-korrigált
  - `total_pnl – fee*n_trades` — nettó eredmény
- Ajánlás + indoklás melyiket válasszuk primary objektívumnak
- Stabilizáló penalty: n_trades minimum (hogy ne 1-2 trade optimalizáljon)

### 5. Periódusok szétválasztása

- Calibration periódus: score calibration + entry threshold keresés
- Evaluation periódus: stratégia teljesítmény mérése (out-of-calibration)
- Nincs overlap a kettő között

### 6. Output: strategy_artifact.json séma

```json
{
  "long_model": "lgbm_solusdt_l_fw60_2101_2605",
  "short_model": "lgbm_solusdt_s_fw60_2101_2605",
  "calib_period": {"start": "...", "end": "..."},
  "eval_period": {"start": "...", "end": "..."},
  "entry_threshold_long": 0.003,
  "entry_threshold_short": 0.003,
  "max_hold_minutes": 120,
  "cooldown_minutes": 60,
  "isotonic_long": { ... serialized ... },
  "isotonic_short": { ... serialized ... },
  "metrics": { "total_return": ..., "n_trades": ..., "sharpe": ... }
}
```

## Acceptance Criteria

- [ ] Minden fenti szekció ki van töltve indoklással
- [ ] Az isotonic regression fit/apply szétválasztása egyértelműen le van írva
- [ ] Az optimalizáció objektívum ki van választva és indokolva
- [ ] A strategy_artifact.json séma végleges és t3 implementálhatja belőle

## Notes

**2026-06-20 — methodology_agent**

Elkészítve: `_doc_/6010_strategy_calibration.md` (X100 metodológiai szint, 6000-es trading blokkon belül).

Döntések:
- Sorszám: `6010` — metodológia mindig alacsonyabb számot kap a kódszintű doc-nál (6100 = aktuális single-model code reference); az `src/strategy/` redesign metodológiája ide kerül.
- Isotonic regression: részletes összehasonlítás más módszerekkel (quantile norm, Platt scaling, nincs calibration) — mindhárom elvetett, indoklással.
- Objektívum: `mean(pred_long_cal | entry_fired) + mean(pred_short_cal | entry_fired)` a primary; total_pnl validáció, nem objetívum.
- Dual-model konfliktus: `conflict_priority = "long"` default, konfigurálható `"highest_cal"` alternatívával.
- `strategy_artifact.json` séma végleges — tartalmazza az isotonic modellek állapotát, Optuna best trial-t, metrics-t.
- `6000_trading.md` Fejezetek szekció frissítve a `6010` bejegyzéssel.

