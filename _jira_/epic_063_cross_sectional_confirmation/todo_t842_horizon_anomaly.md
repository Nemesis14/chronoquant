---
epic: epic_063
id: t842
title: A 120 órás anomália felderítése
assignee: modeling_agent
status: todo
blocks: [t843]
blocked_by: []
---

## Goal

Megérteni, miért ugrik a bruttó +35,1-ről (72h) +79,2-re (120h).

## Miért gyanús

A keresztmetszeti szórás nagyjából **√t** szerint nő. 72h → 120h az
1,67-szeres idő, tehát √1,67 = **1,29×** várható. A mért ugrás **2,25×**.

Vagy a jel valóban erősödik hosszabb horizonton (van rá közgazdasági érv:
lassú value/mean-reversion hatás), vagy **leak van**.

## A leak-gyanúsítottak, sorban

| Hol | Mit kell ellenőrizni |
|---|---|
| `train_hi = m_start - HORIZON_H` purge | 120h-nál 5 nap purge kell — elég-e havi újratanításnál? |
| `close.shift(-HORIZON_H)` | **sor-alapú, nem idő-alapú.** Hiányzó bar esetén a "120 óra" több lehet |
| `df.groupby("symbol").shift(1)` | ugyanaz a sor-vs-idő probléma a feature oldalon |
| delistelt párok | a hosszabb horizont több sort ejt ki a végén — torzít-e? |
| `fwd_demeaned` | az órák közti átfedés hosszabb horizonton nagyobb |

## Acceptance Criteria

- [ ] Idő-alapú (nem sor-alapú) shift, és a különbség lemérve
- [ ] A purge elégségességének ellenőrzése minden horizontra
- [ ] Explicit válasz: leak vagy valódi
- [ ] Ha leak: a 24/48/72h számok újramérve a javított kóddal

## Notes
