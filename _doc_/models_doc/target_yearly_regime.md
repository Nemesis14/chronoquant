# Target éves rezsim (régi 3200 notebook)

**Státusz:** újragenerálva riportként: `_doc_/reports/target_yearly_regime/` (builder: `src/reports/builders/target_regime.py`). A régi notebook (2026-06-20) törölve.

**Mit vizsgáltunk.** A `target` tábla `long_mfe_fw60` (és `short_mfe_fw60`) éves/havi viselkedését: használható-e a teljes history együtt tanításra és a 2026-os kalibrációra.

**Módszer.** Havi/éves átlag, szórás, kvantilisek, éves eloszlások hasonlósága (azonos időablakú összevetés a csonka 2026-tal), havi profil korreláció a korábbi évekkel.

**Eredmény.** A target szintje évről évre lecsúszik: long éves átlag 2021 ~1.14%, 2022 ~0.77%, 2023-2025 ~0.62-0.55%, 2026 ~0.41% (short tükörszerűen). A 2021-2022 blokk magasabb átlagú és szórású, a 2023-2026 alacsonyabb, egymáshoz közelebbi rezsim. A havi profil korrelációja a korábbi évekkel gyenge: nincs erős ismétlődő szezonalitás, a 2026 inkább alacsonyabb opportunity-rezsim.

**Döntés.** A teljes history nem kizárandó, de nem kezelhető egységes rezsimként. Expanding-window + recency-aware validáció; a kalibrációhoz a 2023-2025 blokk (főleg 2025) az elsődleges referencia. Ez vezetett a rolling 6 hónapos kalibrációs ablakhoz.
