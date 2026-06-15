# _docs — Module Documentation

Flat file structure with compound numbering. Main sections: `NN_module.md`, subsections: `NNSS_topic.md`.

See `.agent/skills/docs_skill.md` for conventions and the doc file template.

---

## Structure

```
_doc_/
  0000_project_overview.md

  01_database.md              ← src/database/
    0101_ohlcv_schema.md
    0201_sync_ohlcv.md

  02_modeling.md              ← src/modeling/quantitative/
    0201_*.md

  03_evaluation.md            ← src/modeling/quantitative/evaluation/
    0301_*.md

  04_ui.md                    ← src/ui/
    0401_*.md

  05_trading.md               ← src/trading/
    0501_*.md

  06_elliott.md               ← src/modeling/elliott/
    0601_*.md
```
