# sampling tárolása

jelenleg a samplingról csak metadata jön léter, de ezt meg kell változtatni.

a meta adatok létrehozása után a sample creation készítse el .parquet file-be.
legyen oszlop ami a segments mutatja (fold_1, fold_2, ... test). 

mehet a database megfelelő sample mappájába és megfelelő névvel.

ezeket a parquetet használjuk majd modeltraingenre. 

újra kell dokumentálni hogy létrejön
le kell futatni

elemzéseket erre a táblára kell futatni, nem pedig újra lekérdezni.


# eredmények — kalibráció / eloszláseltolódás

Az elemzés során feltűnt, hogy a train és valid target rate jelentősen eltér:

| target | fold | split | positive rate | rows |
|---|---|---|---|---|
| trg_l_fw60_q90 | 1 | train | 16.87% | 1 051 140 |
| trg_l_fw60_q90 | 1 | valid | 8.83% | 259 200 |

**Okozó tényező:** a target globálisan számolt 90. percentilis felett van, de az időbeli
szeletelés miatt a train és valid periódus különböző piaci rezsimekbe eshet — a lokális
pozitív ráta ezért eltér. Stratified split nem alkalmazható idősoron look-ahead bias nélkül.

**Hatások:**
- A modell a train priorhoz (~17%) kalibrálja magát, miközben a valid ~9%-os eseményt lát
  → a kibocsátott valószínűségek szisztematikusan magasabbak, mint a tényleges ráta (overcalibration)
- **Logloss érzékeny a kalibrációra** — a torzított prior rontja a logloss értéket validáción
- A **ranking képesség** (AUC, PR-AUC) megmarad — a relatív sorrendezés nem sérül

**Tennivaló (nincs blocker):** dokumentálni mint ismert korlát; fontolóra venni
probability calibration (Platt scaling / isotonic regression) bevezetését a predict pipeline-ban.
