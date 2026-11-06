# Signal-gyakoriság: kalibráció vs élő (régi 7200 notebook, 2026-08-12)

**Státusz:** egyszeri diagnosztika, nem újragenerált. Notebook törölve.

**Mit vizsgáltunk.** A bevezetett champion (`..._2101_2605`, deployment `strat_solusdt_fw60_combo_2101_2605`, aktiválva 2026-06-23) heti long/short signal-gyakorisága a kalibrációs/backtest ablakban (2025-05..2026-05) és élesben; kód-hiba vagy piaci rezsim okozza-e a visszaesést.

**Módszer.** Kalibrációs trade-ek heti száma; élő signal rekonstrukció a `predictions` táblából a live logikával (rank-percentile lookup, long p98 / short p94 entry_cutoff, 60 perces eseménytávolság); időzítés a teljes idővonalon; implementációs hiba kizárása (model stamp, NULL predikció, feature NaN-ráta); volatilitás-medián összevetés.

**Eredmény.** A short eleve gyakoribb (aszimmetrikus cutoff: long felső 2%, short alsó 6%). Élesben mindkét irány aránytalanul nem, de arányosan esett vissza, és a csökkenés már 2026 áprilisa óta, a deploy előtt, a kalibrációs ablakon belül elindult. Implementációs hibára nincs jel (egységes model stamp, nincs NULL, feature NaN ~0). A volatilitás (ATR14) az élő ablakban mérhetően alacsonyabb.

**Döntés.** Piaci rezsim / kalibrációs drift, nem kódhiba: a fix cutoff + egy évvel korábbi kalibráció alacsony vol rezsimben ritkábban lő. Következmény: rolling recalibration (ez lett a havi refresh + 6 hónapos kalibrációs ablak a `2101_2607` championnál); szükség esetén feature-paritás teszt (élő sync vs offline).
