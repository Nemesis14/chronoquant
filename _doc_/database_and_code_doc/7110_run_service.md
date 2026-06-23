# 7110 - 01_run_service.py

`src/trading/01_run_service.py`

Headless CLI entrypoint a live trading service-hez. Betölti a trading configot,
opcionálisan felülírja a módot, live módban interaktív jóváhagyást kér, majd a
`TradingService` foreground loopját futtatja.

> Módszertani háttér (live trading rationale, mode design, signal lifecycle):
> → [`../methodology_doc/7000_trading.md`](../methodology_doc/7000_trading.md)
> → [`../methodology_doc/7100_live_trading.md`](../methodology_doc/7100_live_trading.md)

---

## Overview

```mermaid
sequenceDiagram
  participant User
  participant CLI as 01_run_service.py
  participant Utils as utils
  participant Svc as TradingService

  User->>CLI: uv run python src/trading/01_run_service.py --mode ...
  CLI->>Utils: load_trading_config()
  CLI->>Svc: TradingService(config)
  CLI->>Svc: _run()
```

---

## `main()`

Beolvassa a `--mode` CLI paramétert, majd a configból létrehozza a service-t.

| Paraméter | Típus | Leírás |
|-----------|------|--------|
| `--mode` | `dry_run | live` | opcionális config felülírás |

Returns: `None` - a folyamat a service leállásáig blokkol.

```mermaid
flowchart TD
  A["parse args"] --> B["load_trading_config()"]
  B --> C{"--mode megadva?"}
  C -->|igen| D["config['mode'] felülírás"]
  C -->|nem| E["config mód marad"]
  D --> F{"mode == live?"}
  E --> F
  F -->|igen| G["yes/no megerősítés"]
  F -->|nem| H["TradingService(config)"]
  G -->|yes| H
  G -->|no| I["exit 0"]
  H --> J["signal handler regisztráció"]
  J --> K["service._run()"]
```

## CLI Paraméterek

```powershell
# Dry-run mód (biztonságos, szimulált orderek)
uv run python src/trading/01_run_service.py --mode dry_run

# Live mód (valós Binance Futures orderek — interaktív megerősítés kell)
uv run python src/trading/01_run_service.py --mode live

# Config szerinti mód (--mode elhagyható)
uv run python src/trading/01_run_service.py
```

## Signal kezelés

- `SIGINT` és `SIGTERM` esetén a handler `service.stop()`-ot hív.
- A tényleges ciklus leállítása nem itt, hanem a service loopban történik.

---

## Kapcsolódó dokumentumok

- [`7120_trading_service.md`](7120_trading_service.md) — `TradingService` loop kód-referencia
- [`../methodology_doc/7100_live_trading.md`](../methodology_doc/7100_live_trading.md) — live trading módszertan
