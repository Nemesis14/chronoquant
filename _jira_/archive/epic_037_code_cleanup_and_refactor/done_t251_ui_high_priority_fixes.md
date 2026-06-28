---
epic: epic_037
id: t251
title: UI high-priority fixes — race condition, _dash_log, silent exceptions
assignee: ui_agent
status: done
blocks: [t252]
blocked_by: []
---

## Goal

Három HIGH-severity probléma javítása a trading és UI rétegben:
threading race condition, no-op production stub, és 18 debugolhatatlan
silent exception.

## Scope

- `src/ui/trading_runner.py`
- `src/trading/live/service.py`
- `src/ui/data.py`

## Acceptance Criteria

### 1. `trading_runner.py` race condition (HIGH)

- [ ] `_service_instance` és `_last_error` globális változókat `threading.Lock` védi
- [ ] `start_trading()`, `stop_trading()`, `is_trading_running()`, `get_last_error()` mind a lock-on belül olvasnak/írnak
- [ ] Lock inicializálás module-level: `_lock = threading.Lock()`

### 2. `_dash_log` no-op eltávolítása (HIGH)

- [ ] `service.py:45` — `_dash_log` törlése VAGY bekötése a meglévő `logger`-be
  - Opció A (preferált, ha nincs dashboard logging terv): törlés + 3 call site (`198`, `304`, `360`) vagy loggerre cserélés
  - Opció B: `_dash_log` delegál `logger.info/warning/error`-ra
- [ ] No-op `_ = (msg, level)` nem maradhat production kódban
- [ ] Döntés a Notes szekcióba kerül

### 3. Silent `except Exception:` blokkok (HIGH)

- [ ] `data.py` mind a 18 silent exception blokkja tartalmaz legalább `logger.exception(...)` hívást visszatérés előtt
- [ ] Mintaformátum: `except Exception: logger.exception("context description"); return []`
- [ ] A visszatérési értékek ([], None, {}) maradhatnak — csak a logging hiányzik

## Notes

Az audit szerint a `data.py` 18 silent exception-je miatt blank UI panel-eknél
semmiféle log trail nincs. Ez production debugging szempontból critical.

A threading.Lock hiánya Streamlit threaded env-ben valós race condition —
két fragment egyidejű rerun-ja olvashat/írhat _service_instance-t lock nélkül.

[ui_agent] 2026-06-23 — Implementálva

**1. trading_runner.py race condition:** `threading.Lock` hozzáadva module-level
(`_lock = threading.Lock()`). Minden `_service_instance` és `_last_error` olvasás/írás
a lock-on belül történik: `start_trading()`, `stop_trading()`, `is_trading_running()`,
`get_trading_mode()`, `get_last_error()`. A start_trading()-ban a service indítása
lock-on kívül fut (blokkoló művelet), de az instance assignment lock-on belül.

**2. _dash_log döntés — Opció A (törlés):** A `_dash_log` no-op függvény és mind
a 3 call site (`_cycle`, `_open_position`, `_close_position`) eltávolításra került.
Az eltávolított `_dash_log` üzenetek mindegyike közvetlenül megelőzte az azonos
tartalmat loggó `logger.info(...)` hívást — tehát nincs információveszteség.
Ha a jövőben dashboard streaming szükséges, külön mechanizmust (queue, callback)
érdemes bevezetni.

**3. data.py silent exceptions:** 15 `except Exception:` blokk kapta meg a
`logger.exception(...)` hívást (az audit 18-at említett, a tényleges szám 15 volt).
`logging` import és `logger = logging.getLogger(__name__)` hozzáadva a fájl tetejére.

**Pyright:** 13 pre-existing hiba (query_range return type annotations, nem érintett kód).
Ruff: clean.
