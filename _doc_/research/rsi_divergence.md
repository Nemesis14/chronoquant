# RSI / ár trend-divergencia — 15m, SOLUSDT

> **Verdikt: nincs használható jel.** A divergencia-esemény MFE-je a szűrés nélküli változatban látszólag szignifikáns, de az R² szűrővel a long oldal eltűnik; a hatás kicsi, a költséget nem fedezi.
>
> ~6 év SOLUSDT 15m · 2026-09

## Mit néztünk
Prediktív-e az RSI(14) és az ár ellentétes irányú trendje (bullish: ár esik, RSI nő → long; bearish: fordítva → short) a következő 24 órás kedvező elmozdulásra (MFE)?

## Módszer
RSI(14) Wilder-simítással a 15m close-on; trend = OLS meredekség trailing 12 órás (48 bar) ablakon az árra (log) és az RSI-re, R² a trend tisztaságára. „Érdemi trend" = p25/p75 küszöb a teljes mintán (enyhén jövőbe látó kalibráció). Esemény = rising edge. Kiértékelés: 24 órás (96 bar) irány-helyes MFE, Welch-t-teszt a feltétel nélküli baseline MFE-hez képest; R² ≥ 0,3 szűrővel és anélkül.

## Fő számok
| változat | irány | n | excess MFE | Welch t |
|---|---|---|---|---|
| R² szűrő nélkül | long | 629 | +0,72% | 3,29 |
| R² szűrő nélkül | short | 663 | +0,66% | 2,91 |
| R² ≥ 0,3 | long | 345 | −0,17% | −0,73 |
| R² ≥ 0,3 | short | 371 | +0,62% | 1,89 |

Az MFE-nek eleve pozitív torzítása van (a találatok ~99,5%-ában MFE > 0), ezért csak a baseline-hoz mért többlet számít; az excess <1%, és az R² szűrővel az eredmény nem robusztus.

## Döntés / következmény
Nem építünk rá. A kvartilis-kalibráció a teljes mintán készült (look-ahead), tehát még a szűrés nélküli t-statok is optimisták.

## Kód a git historyban
**Nincs.** A `research/rsi_divergence/` git-ignorált volt, sosem került commitba; a script és a `out/summary.csv` (13 KB) törlése után ez az md az egyetlen forrás.
