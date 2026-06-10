# ChronoQuant Documentation

ChronoQuant dokumentacioja retegek szerint van rendezve. A cel az, hogy az
uzleti logika, adat, modell, evaluation, architecture es engineering runbook ne
keveredjen egyetlen nagy dokumentumba.

## Map

| Folder | Purpose |
|---|---|
| `business/` | Mit akar megoldani a rendszer, trading logika, kockazatok, glossary |
| `concepts/` | Elmeleti es kutatasi anyagok: targetek, quantitative feature-ok, Elliott Wave |
| `data/` | Adatbazisok, semak, data dictionary, lineage, quality checks |
| `architecture/` | Stabil rendszerarchitektura es komponenshatarok |
| `modeling/` | Modellezesi guide, sampling, LightGBM workflow, model cards |
| `evaluation/` | Strategy evaluation, backtest engine, eredmenyriportok |
| `engineering/` | Code style, parancsok, tooling, testing, fejlesztoi workflow |
| `reference/` | Config, artifact es script referencia |
| `plans/` | Active/backlog/completed implementacios tervek |

## Recommended Reading Order

1. `business/overview.md`
2. `architecture/overview.md`
3. `data/databases.md`
4. `data/lineage.md`
5. `modeling/guide.md`
6. `evaluation/strategy_evaluation.md`
7. `engineering/commands.md`

## Documentation Rules

- Stable system design belongs in `architecture/`.
- Business intent and strategy meaning belong in `business/`.
- Data and schema facts belong in `data/`.
- Model-specific evidence belongs in `modeling/model_cards/`.
- Backtest/strategy evidence belongs in `evaluation/reports/`.
- How-to-run instructions belong in `engineering/`.

