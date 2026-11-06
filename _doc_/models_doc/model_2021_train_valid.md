# 2021-es long modell train/valid elemzés (régi 5600 ARCHIVE notebookok)

**Státusz:** archivált, elavult (`lgbm_solusdt_l_fw60_2021` éves modell, random-week CV és yearly walk-forward változat). Mindkét notebook törölve.

**Mit vizsgáltunk.** A 2021-es long modell train vs valid hibáit, R2/corr értékét, decilis-monotonitását.

**Módszer.** Foldonkénti CV predikciók (random-week CV, ill. walk-forward), split metrikák, decilis-összegzés a teljes mintához viszonyítva.

**Eredmény.** Nincs durva overfit (train/valid RMSE közel), de a valid R2 mérsékelt, a target zajos. A decilis-rangsor train és valid oldalon is monoton, a valid D10 target-átlag a teljes valid átlag többszöröse.

**Döntés.** A modell rangsoroló/threshold-szelekciós jelként erős, pontbecslőként gyenge; a kalibrációs/threshold döntéshez a foldok közti szórást is figyelni kell. Az éves modellek a v4 champion pipeline-ban nem aktívak.
