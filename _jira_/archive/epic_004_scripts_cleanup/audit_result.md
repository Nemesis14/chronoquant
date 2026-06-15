# Epic 004 Scripts Audit Result

Dátum: 2026-06-14

---

## Összefoglalás

| Script | Kategória | Megjegyzés |
|--------|-----------|------------|
| `data_pipeline/sync_ohlcv.py` | active | Fő OHLCV sync belépő pont |
| `evaluation/backtest_strategy.py` | active | Strategy backtesting CLI |
| `evaluation/generate_model_card.py` | active | Model card generálás |
| `evaluation/sweep_strategy.py` | active | Strategy threshold sweep |
| `modeling/search_lgbm.py` | active | Hiperparaméter keresés |
| `modeling/train_model.py` | active | Modell tanítás CLI |
| `store/benchmark_duckdb.py` | active | Teljesítmény benchmark |
| `store/validate_duckdb_stats.py` | active | DB validáció / smoke report |
| `trading/run_trading_service.py` | active | Live trading CLI |
| `final_fit_lgbm_v4.py` | **dead** | Egyszeri v4 modell final fit — modell artifact elkészült, script értelmét vesztette |
| `promote_lgbm_v2.py` | **dead** | v2 modell promóciója — v4 az aktív modell, v2 support megszűnt |
| `modeling/create_sample_splits.py` | **dead** | `dataset_split` oszlop eltávolítva (epic_002 t1) — script értelmetlen |
| `research/elliott_event_study.py` | **dead** | Elliott waves izolált research, nem része a live pipeline-nak |
| `research/elliott_scan.py` | **dead** | Elliott waves izolált research; pyright hibák is vannak |

---

## Dead scripts részletei

### `modeling/create_sample_splits.py`
- **Miért halott:** A script `dataset_split` és `fold_id` oszlopokat töltene be
  a `feat_ohlcv_quant` táblába. Ezeket az oszlopokat az epic_002 t1 keretében
  eltávolítottuk a sémából — a split-logika a modeling/sampling réteg feladata,
  nem a base tábláé.
- **Hivatkozza-e valami:** Nem. Nincs import más `src/` modulból.
- **Törlési kockázat:** Nulla — az oszlopok és a logika eltűnt.

### `research/elliott_event_study.py`
- **Miért halott:** Az Elliott-hullám modul izolált research (`src/elliott_waves/`),
  nem táplál a live prediction/trading pipeline-ba. A project_overview.md is
  explicite jelzi: "it does not feed the live trading pipeline."
- **Hivatkozza-e valami:** Csak `src/elliott_waves/` modulokat importál.
- **Törlési kockázat:** Alacsony — research-only, nincs termelési hatás.

### `research/elliott_scan.py`
- **Miért halott:** Ugyanaz mint `elliott_event_study.py`. Ráadásul pyright
  12 hibát talált benne (pandas `sort_values` overload problémák).
- **Hivatkozza-e valami:** Csak `src/elliott_waves/` modulokat importál.
- **Törlési kockázat:** Alacsony.

---

## Refactor megjegyzések (aktív scripteken)

Az aktív scriptek némelyike `# ==============` stílusú fejléc-blokkot használ
modul-docstring helyett (coding_skill.md szerint a fájl tetején docstring kell).
Érintett fájlok:
- `data_pipeline/sync_ohlcv.py`
- `evaluation/backtest_strategy.py`
- `evaluation/sweep_strategy.py`
- `modeling/create_sample_splits.py` (dead → törlendő)
- `modeling/search_lgbm.py`
- `modeling/train_model.py`
- `research/elliott_event_study.py` (dead → törlendő)
- `research/elliott_scan.py` (dead → törlendő)

Ez egy alacsony prioritású cleanup — funcionalitást nem érint.

---

## Következő lépés (t2)

**User jóváhagyás szükséges** a következő fájlok törléséhez:

```
scripts/modeling/create_sample_splits.py
scripts/research/elliott_event_study.py
scripts/research/elliott_scan.py
```
