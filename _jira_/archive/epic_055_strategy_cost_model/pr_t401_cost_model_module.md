---
epic: epic_055
id: t401
title: Költség-modell modul és konfiguráció
assignee: modeling_agent
status: pr
blocks: [t403]
blocked_by: []
---

## Goal

Egy helyen kiszámolható legyen egy kötés teljes költsége: díj + csúszás.

## Scope

- `src/strategy/strategy/costs.py` — új modul
- `config/strategies.json` — `costs` blokk

## Konfiguráció

```json
"costs": {
  "maker_bps": 2.0,
  "taker_bps": 5.0,
  "slippage_bps": 1.5,
  "exit_order_type": { "tp": "maker", "sl": "taker", "timeout": "taker" }
}
```

**A user tenyleges dijszintje** (ellenorizve 2026-08-22):

- Binance **VIP0** ("Regular"). 30 napos futures forgalom: 371 479 USDT
  (a VIP1-hez 5M USDT **es** 5 BNB kellene).
- USDS-M futures alapdij: **maker 0,0200% / taker 0,0500%** = 2,0 / 5,0 bps.
- A "Use BNB to pay USDS-M Futures fees" kapcsolo be van kapcsolva, **de a
  BNB egyenleg 0** -> a -10% kedvezmeny NEM ervenyesul. Ezert a teljes
  alapdijjal szamolunk.

⚠️ Ne keverd a **spot** dijjal (0,1000% / 0,0750% BNB-vel). A stratégia
USDS-M perpetual futuresen fut, ami otodannyi dij.

Kor-koltseg ebbol:
- TP-vel zarva (taker be + maker ki): **7,0 bps** + csuszas
- SL/timeout-tal zarva (taker + taker): **10,0 bps** + csuszas

Ha a user kesobb BNB-t tart, ez 1,8 / 4,5 bps-re valt (kor: 6,3 / 9,0 bps).
Ezert jon configbol — NE egesd bele a kodba.

## Számítás

```
belepes_koltseg = notional * (taker_bps + slippage_bps) / 10000
kilepes_koltseg = notional * (dij(exit_reason) + slippage_bps) / 10000
```

A belépés mindig taker (jelre lépünk be, nem várunk limit teljesülésre).
A kilépés díja az `exit_reason`-tól függ.

## Acceptance Criteria

- [ ] `costs.py` egyetlen kötés költségét adja vissza `exit_reason` alapján
- [ ] Minden paraméter configból jön, semmi hardcode
- [ ] Egységteszt: 1000 USD notional, taker-taker, 5+1,5 bps -> 1,30 USD kör
- [ ] `ruff check` + `pyright` tiszta

## Notes
