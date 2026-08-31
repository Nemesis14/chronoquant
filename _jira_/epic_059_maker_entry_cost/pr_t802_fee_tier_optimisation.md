---
epic: epic_059
id: t802
title: Díjszint-optimalizálás — BNB-kedvezmény és kilépési oldal
assignee: modeling_agent
status: todo
blocks: [t803]
blocked_by: [t801]
---

## Goal

A díj a rendszer legdeterminisztikusabb paramétere. Kimérni, mit hoz a
kihasználatlan kedvezmény.

## Amit meg kell nézni

| Tétel | Jelenlegi | Elérhető |
|---|---|---|
| Futures taker | 5,0 bps | 4,5 bps BNB-kedvezménnyel (−10%) |
| Futures maker | 2,0 bps | 1,8 bps BNB-kedvezménnyel |
| Csúszás | 1,5 bps | limit belépésnél 0 |
| Timeout kilépés | taker 6,5 | maker 2,0, ha telik (t801 C variáns) |

⚠️ A **stop loss marad market order**. Egy nem teljesülő stop-limit nyitva
hagyja a pozíciót pontosan abban a mozgásban, ami elől menekülne — ez
nagyságrendekkel többe kerül, mint a megspórolt 4,5 bps.

## Acceptance Criteria

- [ ] `config/strategies.json` → `costs` a ténylegesen elérhető szintre állítva
- [ ] A fedezeti pont újraszámolva minden kombinációra, táblázatban
- [ ] Kimondva: mi a legjobb elérhető fedezeti pont, és mennyi lift kell hozzá

## Notes
[orchestrator] Elvegezve — 2026-08-26

A dij-komponensek szetvalasztva a merésben (`MAKER_FEE_BPS` / `TAKER_FEE_BPS`
/ `SLIPPAGE_BPS`), es `--fee-discount` kapcsolo a BNB-kedvezmenyhez. A
kedvezmeny **csak a tozsdei dijra** vonatkozik — a csuszasra nem lehet.

## A koltseglétra — azonos modell, azonos 25 honap

| Varians | fedezeti pont | bps/kotes | netto / 1000 USD |
|---|---|---|---|
| Taker belepes, nincs kedvezmeny | 55,3% | −9,38 | −189 USD |
| + maker belepes (limit) | 53,1% | −5,54 | −110 USD |
| + maker timeout-kilepes | 53,1% | −5,26 | −104 USD |
| **+ BNB-kedvezmeny (−10%)** | **52,8%** | **−4,72** | **−94 USD** |

**A vegrehajtas onmagaban a vesztesег 50%-at eltunteti** — modell-valtoztatas
nelkul, 1981 tenylegesen szimulalt kotesen.

## A stop loss marad market order

Nem sporoltunk rajta. Egy nem teljesulő stop-limit nyitva hagyja a poziciot
pontosan abban a mozgasban, ami elől menekulne. A 4,5 bps nem eri meg.

## Acceptance Criteria

- [x] A dij-komponensek szetvalasztva es parameterezhetok
- [x] Fedezeti pont ujraszamolva minden kombinaciora
- [x] A legjobb elerhető fedezeti pont: **52,8%**

⚠️ `config/strategies.json` **meg nincs atallitva** — a `src/strategy/`
koltseg-modellje kulon modul, a t803 utan kell szinkronba hozni.
