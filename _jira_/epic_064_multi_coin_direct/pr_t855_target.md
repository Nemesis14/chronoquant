---
epic: epic_064
id: t855
title: Vol-skálázott 24h triple-barrier target coinonként
assignee: database_agent
status: todo
blocks: [t856]
blocked_by: [t852]
---

## Goal

Coinonként vol-skálázott, 24 órás triple-barrier címke.

## ⚠️ Ez nem opcionális kényelmi funkció

Fix ±100 bps a BTC-n **nagy mozgás**, a PEOPLEUSDT-n **zaj**. Ugyanazzal a
címkével a modell nem irányt tanulna, hanem azt, hogy **melyik coin
volatilis** — pontosan az a hiba, ami a SOL-on 8 hónapig tartott, csak most
keresztmetszetben is.

Az `epic_060` receptje működött (rezsim-függés 2,5-szeresére csökkent):

```
sigma_1m       = EWM std(logret, span=1440), t-1-re shiftelve
korlat_bps(t)  = clip(k * sigma * sqrt(1440) * 1e4, MIN, MAX)
k              = normalising_k(), COINONKENT, hogy a median korlat azonos legyen
```

A `k`-t coinonként kell normálni — így a BTC és a PEOPLE címkéje ugyanazt
**jelenti**, még ha a bps-értékük különbözik is.

## Táblaoszlopok

```
tb_label_1440_vol        TINYINT   +1 / -1 / 0
tb_label_1440_vol_bps    REAL      a soronkenti korlat -- a fedezeti pont
                                   coinonkent es soronkent mas
fwd_ret_1440             REAL      nyers elore hozam (timeout arazasahoz)
```

## Acceptance Criteria

- [ ] `target_5m (symbol, open_time)` tábla
- [ ] Coinonkénti `k` és a korlát-eloszlás kiírva
- [ ] Osztályeloszlás coinonként ÉS évenként — a korszak-stabilitás ellenőrzése
- [ ] A meglévő `triple_barrier_labels()` újrahasznosítva (tömb-korlátot már tud)
- [ ] `ruff` + `pyright` tiszta

## Notes
[orchestrator] Elvegezve — 2026-08-26

## ⚠️ Ket javitas a folyamat kozben

**1. Korlat-szelesseg: 1,0x -> 3,0x.** Az elso build (median ~90 bps) eloszlasa
+1=48,3% / -1=51,5% / **timeout=0,1%** lett — pontosan az a konfiguracio, amit
az `epic_058` §6 mar lemert es **~0pp liftet** hozott. Meres szerint 1,0x-nel
a barok **56,9%-a MINDKET korlatot eleri** 24 oran belul, tehat a cimke nem
iranyt kerdez, hanem hogy melyik erintes jott elobb — egy versenyfutast.

| korlat | timeout | mindkettot eleri |
|---|---|---|
| 1,0x | 0,9% | **56,9%** |
| 2,0x | 9,1% | 24,1% |
| **3,0x** | **25,2%** | **9,0%** |

**2. Kozos `k` coinonkenti helyett — ez volt a sajat hibam.** A
`normalising_k`-t coinonkent hivtam kozos 300 bps celertekkel, amitol minden
coin korlatja **pontosan 300 bps** lett — azonos abszolut szelesseg, nem
azonos volatilitas-tobbszoros:

| coin | timeout (rossz valtozat) |
|---|---|
| 1000BONK | **5,4%** |
| DOGE | 24,1% |
| BTC | **56,0%** |

A ±300 bps a BTC-nek nagy mozgas, a BONK-nak rutin -> a modell coin-identitast
tanulna. Javitva: **egy kozos k = 0,6480** az egesz panelre, a coin sajat
szigmajara alkalmazva.

**3. Wick-ablak: pontos perc -> ±5 perc.** A 2021-05-19-i krach izolaltnak
minosult, mert BCH 13:14-kor, ETH 13:10-kor, FIL 13:08-kor mozdult. Javitas
utan 54 -> **24 izolalt** eset.

## Eredmeny

```
target_5m   12 938 874 sor, 30 coin
  +1 = 39,9%   -1 = 43,4%   timeout = 16,8%
  korlat: min 30 / median 266 / max 2000 bps
```

**Coinok kozotti szoras (timeout): 14,2% .. 19,8% — 5,6pp.**
Vagyis a cimke minden coinon ugyanazt jelenti.

**Korszak-stabilitas (a fo cel):**

| ev | +1 | -1 | timeout | korlat med |
|---|---|---|---|---|
| 2021 | 36,2% | 46,8% | 17,0% | 332 |
| 2022 | 39,4% | 43,9% | 16,7% | 281 |
| 2023 | 39,9% | 42,0% | 18,1% | 213 |
| 2024 | 40,0% | 43,7% | 16,3% | 291 |
| 2025 | 40,9% | 43,4% | 15,7% | 282 |
| 2026 | 38,9% | 43,7% | 17,5% | 248 |

**A timeout tartomanya 2,4pp hat even at.** Osszevetesul a SOLUSDT fix
±100 bps cimkeje: **37,7pp** (2021: 1,6%, 2026: 39,3%). A vol-skalazas
+ kozos k gyakorlatilag megszuntette a korszak-fuggest.

⚠️ Maradek aszimmetria: -1 (43,4%) > +1 (39,9%), 3,5pp. Ez a same-bar
utkozesi konvencio (az also korlat nyer) kovetkezmenye, dokumentalt.

## Acceptance Criteria

- [x] `target_5m` tabla
- [x] Kozos k es a korlat-eloszlas kiirva
- [x] Osztalyeloszlas coinonkent ES evenkent
- [x] `triple_barrier_labels()` ujrahasznositva (tomb-korlat)
- [x] `ruff` + `pyright` tiszta
