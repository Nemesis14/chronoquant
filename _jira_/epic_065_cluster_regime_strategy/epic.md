# Epic 065 — Klaszter-alapú Setup-felismerés

**Status:** DONE

**Tracker:** d:\repos\chronoquant\_jira_\epic_065_cluster_regime_strategy\

**Created:** 2026-08-31  
**Completed:** 2026-09-01

---

## Summary

Kutatási epik az unsupervised klaszterezésen alapuló setup-felismeréshez.
360 perces ablak, alacsony-redundanciájú feature-készlet, K-means klaszterezés, out-of-sample validáció, k=10 optimalizálás, 1-éves backtesting.

**Final Result:** k=10 K-means azonosít 4 direkt setupot (2 SHORT + 2 LONG). 1-éves OOS backtesting: 2,235 trades, $63.3k nettó profit, 98.3% win rate (MFE-alapú, ideális scenario).

---

## Validation Summary (2026-09-01)

**All tickets validated:**
- t863–t869: done_ status
- t870 (k=10 optimization): 
  - ✅ Documentation complete (_doc_/models_doc/6500_cluster_regime_analysis_v2.md)
  - ✅ Backtest CSV valid (2,235 trades, all calculations verified)
  - ✅ Code quality: ruff PASS
  - ✅ Setup definitions correct (Bonferroni-corrected significance)
  - ✅ Critical caveats documented (98.3% win rate unrealistic for live trading)

---

## Notes

Cluster 0 and 9 show significant long bias; Cluster 2 and 3 show significant short bias in OOS period.
**Phase 3 ready:** Paper trading with micro-sizing recommended before live deployment.
Watch for realistic win rate 60-80% vs. idealistic 98.3%.
