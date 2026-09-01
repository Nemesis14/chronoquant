# Epic 065 — Klaszter-alapú Setup-felismerés

**Status:** IN PROGRESS (harmadik nekifutás — t872 pr_ állapotban, validátorra vár)

**Tracker:** d:\repos\chronoquant\_jira_\epic_065_cluster_regime_strategy\

**Created:** 2026-08-31
**Last updated:** 2026-09-01

---

## Summary

Kutatási epik az unsupervised klaszterezésen alapuló setup-felismeréshez.
360 perces ablak, alacsony-redundanciájú feature-készlet, K-means klaszterezés, out-of-sample validáció.

**⚠️ A korábbi "Final Result" (lásd alább, t870-ből) ELAVULT — t871 megcáfolta.**

### Iteráció történet

1. **t863–t870 (fix k=10, post-hoc irány-hozzárendelés):** k=10 K-means, 4 "tradeable" setup
   (2 SHORT + 2 LONG), 1-éves idealizált OOS backtest: 2,235 trades, $63.3k nettó profit, 98.3%
   win rate (MFE-alapú, zero-cost, nem realisztikus).
2. **t871 (realisztikus validáció + hibajavítás):** Az idealizált 98.3% win rate **mérési
   artifact** volt, nem valódi edge. Két kritikus hibát találtunk és javítottunk (LONG-only bug,
   majd egy matematikailag hibás t-test az irány-klasszifikációban), és egy kalibrált TP/SL
   tesztet is futtattunk. **Hat konfiguráció mindegyike ugyanarra jutott: nincs kihasználható
   trading edge realisztikus költségek mellett** (0.3% costs + 0.4% slippage → szükséges win
   rate ~67%, elért ~35-38%). Részletek: `_doc_/models_doc/6501_k10_validation_realistic_backtest.md`.
3. **t872 (CV-optimalizált klaszter-szeparáció, jelenlegi):** A fix-k megközelítés helyett
   walk-forward CV-vel (4 fold) optimalizáltuk magát a klaszterezést (ANOVA F-score, k=2..30
   keresés). Optimális k=13. **Ez a task NEM tartalmazott trading backtestet** — csak a
   klaszterezés minőségét validálta. A SHORT-only irányaszimmetria (0 szignifikáns LONG klaszter)
   megismétlődött a k=10-ből ismert mintázattal. Részletek: `_doc_/models_doc/6502_cv_cluster_separation_optimization.md`.
   Ticket: `pr_t872_cv_optimized_cluster_separation.md` — validátorra vár.

---

## Validation Summary (korábbi, 2026-09-01, t870-re vonatkozott — ELAVULT)

**t863–t869: done_ status.** t870 validálva volt saját (idealizált) scope-jában, de az eredményt
t871 felülírta realisztikus feltételek mellett.

---

## Notes

- **Ne a t870 "98.3% win rate" eredményt idézd döntéshez** — az idealizált, zero-cost scenario,
  amit t871 megcáfolt.
- **t871 lezárt következtetése:** fix-k klaszterezés (k=3 vagy k=10) nem életképes trading
  stratégia realisztikus végrehajtás mellett, semmilyen irány-klasszifikációs módszertannal vagy
  TP/SL kalibrációval sem.
- **t872 nyitott kérdése:** ha a klaszterezés minősége (CV-optimalizált k=13) objektíven jobb,
  mint a fix k=10-é, ez javítja-e a realisztikus P&L-t egy jövőbeli, feltételes backtest taskban?
  Az eddigi eredmények (hasonló SHORT-only aszimmetria) alapján a várakozás mérsékelt kell legyen.
