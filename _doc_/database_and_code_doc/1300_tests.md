# tests/ — Teszt Áttekintés

`src/database/tests/`

A database modul tesztjei négy szinten ellenőrzik az adatintegritást, store működést és pipeline helyességét.

---

## Teszt struktúra

```
src/database/tests/
├── store/
│   ├── conftest.py          db_path és conn fixture-ök
│   ├── smoke/               Gyors szintaktikai ellenőrzések
│   ├── sanity/              Adatintegritás invariánsok az éles DB-n
│   └── perf/                Teljesítmény benchmarkok
├── sync_tables/
│   ├── smoke/               Sync funkciók mock adattal
│   ├── sanity/              Lookahead bias és adatminőség
│   └── integration/         Cross-layer pipeline flow
└── sync_pipeline/
    └── smoke/               02_sync_pipeline.py CLI helper tesztek
```

Részletes tesztek:
- Store tesztek → [1310_store_tests.md](1310_store_tests.md)
- Pipeline tesztek → [1320_pipeline_tests.md](1320_pipeline_tests.md)

---

## pytest Markok

| Mark | Leírás | Futtatás |
|------|--------|----------|
| `smoke` | Gyors szintaktikai + funkcionális ellenőrzések | Minden commit |
| `sanity` | Adat-invariáns ellenőrzések az éles DB-n | Napi / sync után |
| `perf` | Wall-clock teljesítmény benchmarkok | Ad hoc / regresszió gyanú |
| `integration` | Cross-layer pipeline flow | Release / major refactor |

---

## Fixtures (`store/conftest.py`)

### `db_path` fixture

Betölti az éles DB elérési útját a configból (`utils.load_asset_config("solusdt")["database"]["db_path"]`).

Ha a DB fájl nem létezik, a tesztek `pytest.skip()`-pel átlépnek — nem hibáznak.

### `conn` fixture

Read-only DuckDB kapcsolat az éles DB-re. Az összes `sanity` és `perf` teszt ezt kapja dependency injectionként.

---

## Futtatás

```bash
# Összes database teszt
uv run pytest src/database/tests/ -v

# Csak smoke tesztek (gyors, CI-ban)
uv run pytest src/database/tests/ -m smoke -v

# Csak sanity (éles DB kell)
uv run pytest src/database/tests/ -m sanity -v

# Perf benchmarkok
uv run pytest src/database/tests/ -m perf -v -s

# Integration tesztek (synthetic data, mock models)
uv run pytest src/database/tests/ -m integration -v
```

A `-s` flag a `print()` kimeneteket is megjeleníti — perf teszteknél fontos a timing értékek láthatóságához.
