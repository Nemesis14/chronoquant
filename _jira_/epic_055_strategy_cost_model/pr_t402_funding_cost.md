---
epic: epic_055
id: t402
title: Funding-költség a ténylegesen átnyúlt elszámolásokból
assignee: modeling_agent
status: pr
blocks: [t403]
blocked_by: []
---

## Goal

Minden kötésre kiszámolni a tényleges funding-hatást — nem átalánnyal, hanem
az átnyúlt elszámolások összegzésével.

## Forrás

Az `altdata_5m` tábla (`epic_054` / t307) tartalmazza a `funding_last` és
`funding_next_ts` oszlopot 2021-12-től. Az elszámolási időpontok innen jönnek.

⚠️ **Az intervallum nem mindig 8 óra.** A SOLUSDT-nek volt 2 és 4 órás
szakasza is (mért: ~98 db 2 órás, néhány 4 órás). Ne hardcode-old a
00/08/16 UTC rácsot — a tényleges `funding_next_ts` értékeket használd.

## Számítás

```sql
funding_hatas = SUM(funding_rate) minden elszamolasra,
                ahol entry_time < elszamolas_ts <= exit_time
koltseg_usd   = funding_hatas * notional     -- long pozicional
```

Long pozíciónál **pozitív funding = fizetsz**, negatív = kapsz.
Shortnál fordítva.

## Mért kiindulási érték (epic_054, 2026-02 → 2026-07, long)

| | Kötés | Átnyúló | Összes funding | 1000 USD-re |
|---|---|---|---|---|
| Kontroll | 672 | 86 (12,8%) | −109,8 bps | **+10,98 USD (bevétel)** |
| Kezelés | 1984 | 67 (3,4%) | −108,1 bps | **+10,81 USD (bevétel)** |

Ha az implementáció ettől érdemben eltérő számot ad ugyanezen a sessionön,
valami elromlott — ez a szám a regressziós horgony.

## Acceptance Criteria

- [ ] Kötésenkénti funding-hatás a trades táblában, új oszlopként
- [ ] Az előjel-konvenció tesztelve mindkét irányra
- [ ] A fenti két session újraszámolva ±1% pontossággal reprodukálja a horgonyt
- [ ] Nem feltételez fix 8 órás intervallumot — teszt a 2 órás szakaszra
- [ ] `ruff check` + `pyright` tiszta

## Notes
