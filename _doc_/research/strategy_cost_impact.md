# A kereskedési költség hatása a stratégia-keresésre (epic_055 / t404)

> **Az eredmény kellemetlen: a kontroll stratégia költségek után veszteséges,
> a kezelt épphogy nyereséges.** A korábbi, díj nélküli számok mind
> érvénytelenek.
>
> Dátum: 2026-08-25 · Ablak: 2026-02-01 → 2026-07-31 · Long irány

---

## 1. Mit változtattunk

A rácskeresés célfüggvénye **bruttó** hozamról **nettó**ra váltott:

```
régi:  objective = total_fact_log_return
új:    objective = total_fact_log_return − díj − csúszás − funding
```

Díjparaméterek (`config/strategies.json` → `costs`), a user tényleges
Binance VIP0 szintje BNB-kedvezmény nélkül:

| | bps |
|---|---|
| maker | 2,0 |
| taker | 5,0 |
| csúszás | 1,5 |

Belépés mindig taker. A kilépés díja az `exit_reason`-tól függ: a take profit
limit orderként pihen (maker, csúszás nélkül), a stop loss és az időtúllépés
market (taker + csúszás).

---

## 2. Setup-váltás — a keresés mást választ

| | Régi (bruttó) optimum | Új (nettó) optimum |
|---|---|---|
| **Kontroll** | cutoff 0,94 · 672 kötés | cutoff 0,98 · **415 kötés** |
| **Kezelés** | cutoff 0,95 · 1984 kötés | cutoff 0,98 · **401 kötés** |

**A 200 setupból a kontrollnál mind a 200, a kezelésnél 197 helyezése
megváltozott.** A régi rangsor gyakorlatilag semmit nem mondott a valós
sorrendről.

A bruttó optimumok nettó eredménye:

| | Bruttó optimum nettó értéke | Nettó optimum | Nyereség a váltásból |
|---|---|---|---|
| Kontroll | −0,3186 | −0,0508 | **+0,2678** |
| Kezelés | **−1,7819** | +0,0214 | **+1,8034** |

A kezelés régi „győztes" beállítása (1984 kötés) nettó **−1,78 log return** —
1000 USD notionalon ez a tőke többszörös elvesztése.

---

## 3. Az eredmény díjakkal

| | cutoff | kötés | bruttó | díj | funding | **NETTÓ** |
|---|---|---|---|---|---|---|
| Kontroll | 0,98 | 415 | 0,3570 | 0,4126 | −0,0049 | **−0,0508** |
| Kezelés | 0,98 | 401 | 0,4624 | 0,4462 | −0,0052 | **+0,0214** |

1000 USD notional / 100 USD tőke (10x), 6 hónap:

- **Kontroll: −49 USD** (a tőke −49%-a)
- **Kezelés: +22 USD** (a tőke +22%-a)

### Break-even díjszint

| | Break-even | Tényleges költség | Tartalék |
|---|---|---|---|
| Kontroll | 8,72 bps | 9,94 bps | **−1,22 bps** |
| Kezelés | 11,66 bps | 11,13 bps | **+0,53 bps** |

**A kezelés tartaléka fél bázispont.** Ha a csúszás 1,5 helyett 2 bps, elfogy.

### Miért drágább a kezelés kötésenként?

A kilépések összetétele dönt, mert a TP maker, a többi taker:

| | tp (maker) | timeout (taker) | sl (taker) |
|---|---|---|---|
| Kontroll | 282 (68%) | 133 (32%) | 0 |
| Kezelés | 167 (42%) | 192 (48%) | 42 (10%) |

A kezelés több drága kilépést használ (11,13 vs 9,94 bps/kötés), de eleget
keres, hogy fedezze.

---

## 4. Az epic_054 verdikt felülvizsgálata

**Az alt-data előnye megmarad, sőt fontosabb lett** — de az abszolút
következtetést vissza kell vonni.

| | Korábbi állítás (8 bps átalány) | Valóság (kilépés-függő díj) |
|---|---|---|
| Kontroll | +97 USD | **−49 USD** |
| Kezelés | +267 USD | **+22 USD** |
| Alt-data előny | +34% él | **a veszteség és a nyereség közti különbség** |

A korábbi 8 bps-es átalány **optimista** volt: a tényleges átlag 9,9–11,1 bps,
mert az időtúllépéses kilépések taker díjat fizetnek, és ezek aránya magas
(32–48%).

**Az alt-data tehát nem „javít 34%-ot egy nyereséges stratégián", hanem
átbillenti a stratégiát veszteségesből épphogy nyereségesbe.** Ez erősebb
állítás, mint amit korábban tettem — de sokkal kisebb mozgástérrel.

---

## 5. Amit ez a mérés nem mond meg

- **A 200 setupból 198–200 nettó veszteséges.** A nyertes egy szűk kivétel,
  ugyanazon az ablakon kiválasztva, amin mérjük (`same_window`). Egy másik
  ablakon nagy eséllyel más setup lenne a nyerő — és nem biztos, hogy pozitív.
- **A +22 USD in-sample optimum.** Nem várható hozam, hanem felső korlát.
- **Csak long irány, egyetlen 6 hónapos ablak.**
- A csúszás 1,5 bps **feltételezés**, nem mért érték. A tényleges csúszást
  a live trade journalból lehetne kimérni.

---

## 6. Következtetés

1. **A díjmodell nem opcionális.** Nélküle a keresés olyan beállítást
   választott, ami a tőkét többszörösen felszámolta volna.
2. **A jelenlegi feature-készlet (kontroll) ezen az ablakon nem fedezi a
   díjat.** Ez független az alt-datától, és a legfontosabb üzenet.
3. **Az alt-data segít, de a mozgástér fél bázispont.** Ez nem az a tartalék,
   amire éles tőkét lehet tenni.
4. A következő lépés nem több feature, hanem **kevesebb, jobb kötés**: a
   cutoff feljebb (0,98 fölé), vagy olyan kilépés-logika, ami több TP-t
   eredményez maker díjjal.
