Igen, magyarul. A lényeg: az irány jó, de a jelenlegi validációs logikát én nem hagynám így production backtestnek. A legnagyobb gond nem a 60 perces target vagy a 36 hét train / 12 hét valid arány, hanem az, hogy a train hetek random módon vannak kiszedve a teljes 12 hónapból, így előfordulhat, hogy jövőbeli hónapból tanítasz, és korábbi hónapon validálsz. Idősoros / pénzügyi modellnél ez módszertanilag veszélyes.

Rövid vélemény

A mostani ötlet adatcsökkentésnek érthető, de modellezési validációnak nem elég korrekt.

Amit én megtartanék:

60 perces target esetén a 60 perces mintavétel jó baseline.
A 36 hét train mennyiség önmagában rendben lehet.
A heti kalibráció + havi újratanítás jó production logika.
Percentile score esetén külön kell kezelni a ranking minőséget és a kalibrációt.

Amit változtatnék:

Ne legyen random train/valid split a teljes 12 hónapon belül.
Ne legyen olyan, hogy decemberi adat trainben van, novemberi adat validban.
Ne a véletlenül kiválasztott hetek validálják a modellt, hanem a production működést utánzó, időben előrehaladó backtest.
A hyperparameter search is időalapú legyen, ne random CV.
1. 60 perces minták 60 perces targethez

Ez jó kiinduló megközelítés.

Ha a target például:

adott időponttól számított következő 60 perc hozama / mozgása / outcome-ja,

akkor ha percenként veszel mintát, a címkék nagyon erősen átfednek.

Példa:

10:00 target: 10:00–11:00
10:01 target: 10:01–11:01
10:02 target: 10:02–11:02

Ezek majdnem ugyanazt a jövőbeli időablakot látják. Ettől a dataset látszólag sokkal nagyobb lesz, de az effektív információtartalom nem nő arányosan. A modellvalidáció túl optimistává válhat, mert a train és valid minták között nagyon hasonló jövőbeli szakaszok lehetnek.

Ezért 60 perces targethez a 60 perces anchor mintavétel teljesen védhető baseline.

Én így fogalmaznám:

Production baseline-nak órás target esetén órás mintavétel legyen. A teljes 1 perces adatot használd feature engineeringre, de ne feltétlenül supervised sample-ként minden percre.

Vagyis:

input feature-ök számolódhatnak 1 perces adatokból,
de label / training sample lehet óránként egy.
2. Van-e értelme a teljes 1 perces mintát használni?

Van értelme, de nem első körös baseline-nak.

Teljes 1 perces sampling akkor lehet jó, ha:

nagyon kevés az órás minta,
a modellnek tényleg hasznos a nagyobb sample density,
tudsz kezelni overlapping label problémát,
van purging / embargo / gap a validációban,
a score nem csak szép validációs számot ad, hanem production backtesten is nyer.

A teljes 1 perces minta hátránya:

erős label overlap,
autokorrelált minták,
túloptimista validáció,
hamis stabilitásérzet,
a hyperparameter search is könnyebben overfitel.

Ezért javaslatom:

Baseline: 60 perces minták.
Challenger: 5 / 10 / 15 perces vagy 1 perces sűrűbb mintavétel.
A challenger csak akkor maradjon, ha leakage-safe rolling backtesten tényleg jobb.

3. A 3 hét random train 12 hónapból jó-e?

Production-validációnak szerintem nem jó.

A gond ezzel:

12 hónapból random kiválasztasz 36 hetet trainnek, a maradék 12 hét valid.

Ez keveri az időt. Így a modell tanulhat olyan jövőbeli rezsimekből, volatilitásból, szezonális mintázatból vagy piaci állapotból, amely a validációs időszakban még nem lett volna ismert.

Pénzügyi / intraday idősornál nem igazán elfogadható az az érv, hogy:

„Mindegy, mert percent adatok vannak és 60 perces target, ezért függetlenek.”

Nem függetlenek teljesen. Lehet:

volatilitás-klaszterezés,
napon belüli seasonality,
rezsimváltás,
hónapfüggő piaci szerkezet,
eseményhatás,
likviditási / spread környezet változás.

Tehát a decemberi train + novemberi valid időbeli inkonzisztenciát okozhat. Nem azért, mert a label konkrétan átfed, hanem mert a validáció már nem azt szimulálja, amit élesben csinálnál.

Élesben novemberben nem tudnál decemberi adatból tanítani. Ezért backtestben se engedném.

4. Heti kalibráció + havi újratanítás mellett hogyan kellene tanítani?

A validation scheme-nek le kell másolnia a production működést.

Ha élesben ez lesz:

base model újratanítás havonta,
score / percentile kalibráció hetente,
prediction órás targetre,
scoring percent / percentile alapon,

akkor a backtest is ezt szimulálja.

Javasolt működés:

Outer backtest: havi production szimuláció

Példa:

Teszt hónap	Train adat	Kalibráció	Teszt
Május	előző 36 hét	április utolsó 1–4 hete	május
Június	előző 36 hét	május utolsó 1–4 hete	június
Július	előző 36 hét	június utolsó 1–4 hete	július

A lényeg:

Minden teszt hónapnál csak olyan adatot használhatsz trainre és kalibrációra, amely időben a teszt hónap előtt van.

Ez fogja jól modellezni a production helyzetet.

5. Train-fold és hyperparameter search

Én nem randomizált hetekkel csinálnám a hyperparameter searchöt, hanem nested rolling-origin módon.

Egyszerűbben:

Külső loop

Ez méri a production teljesítményt.

Train: múltbeli időablak, például előző 36 hét.
Test: következő 4 hét / következő hónap.
Ismételve több hónapon át.
Belső loop

Ez választ hyperparamétert.

A 36 hetes train időszakon belül több időalapú split:

train korábbi rész,
valid későbbi rész,
gap / embargo a kettő között.

Példa 36 hét belső bontásra:

Inner fold	Train	Valid
1	hét 1–20	hét 21–24
2	hét 1–24	hét 25–28
3	hét 1–28	hét 29–32
4	hét 1–32	hét 33–36

Vagy sliding formában:

Inner fold	Train	Valid
1	hét 1–20	hét 21–24
2	hét 5–24	hét 25–28
3	hét 9–28	hét 29–32
4	hét 13–32	hét 33–36

Én a sliding verziót preferálnám, ha erős concept drift van. Expanding window jobb lehet, ha kevés adat van és stabilabb a jel.

6. Mekkora gap / embargo kell?

Minimum:

target horizon = 60 perc

De ha feature-ök hosszabb lookbackből készülnek, akkor:

gap = max(target horizon, leghosszabb feature lookback)

Példa:

target: következő 60 perc,
feature: elmúlt 240 perc rolling volatility,
akkor validációs gap legalább 240 perc legyen, nem csak 60.

Ez azért kell, hogy a train és valid között ne legyen rejtett időbeli összefolyás.

7. Percentile score esetén mit optimalizáljunk?

Ha percentként / percentile-ként fut a score, akkor nem feltétlenül a klasszikus accuracy vagy RMSE a legfontosabb.

Két külön dolgot néznék:

Ranking minőség

Ha a score arra kell, hogy rangsorolja az eseteket, akkor jó metrikák:

AUC,
Average Precision,
top decile / top percentile hit rate,
top 5% vs bottom 5% outcome spread,
Spearman rank correlation,
lift chart percentile bucketek szerint.

Például:

A top 10% score bucket tényleg magasabb realizált targetet hoz-e, mint az alsó 10%?

Ez nagyon fontos, mert percentile score esetén a modell akkor hasznos, ha jól rangsorol.

Kalibráció

Ha a percentile score-nak értelmezhetőnek is kell lennie, akkor külön kalibráció kell.

Én ezt csinálnám:

base model havonta újratanítva,
raw score generálva,
weekly calibrator frissítve az utolsó elérhető hetek alapján,
production score = kalibrált percentile / rank mapping.

A kalibrátor lehet egyszerű:

rolling percentile mapping,
isotonic calibration,
Platt / logistic calibration, ha binary target,
bucket-based empirical calibration.
8. Konkrét javasolt végső design

Én ezt választanám fő production backtestnek:

Dataset
1 perces nyers adat marad.
Feature-ök számolódnak 1 perces adatokból.
Supervised minta baseline-ban óránként egy.
Target: következő 60 perc.
Gap: legalább 60 perc, de inkább a leghosszabb feature lookback szerint.
Train ablak

Első fő verzió:

rolling 36 hét train

Második összehasonlító verzió:

expanding train window

Harmadik verzió:

52 hét rolling train, ha van elég adat

A 36 hetes rolling szerintem jó első jelölt, mert illeszkedik a havi redevelopmenthez és valamennyire kezeli a concept driftet.

Validation / test

Ne random 36 hét vs 12 hét legyen.

Hanem:

minden hónapra: train az előtte lévő 36 hét, valid/test a következő hónap.

Példa:

Run	Train	Kalibráció	Test
1	Jan–Aug	Aug utolsó hetei	Sep
2	Feb–Sep	Sep utolsó hetei	Oct
3	Mar–Oct	Oct utolsó hetei	Nov
4	Apr–Nov	Nov utolsó hetei	Dec

Itt nincs olyan, hogy decemberből tanítasz november validra.

Hyperparameter search
Csak a train időszakon belül fusson.
Időalapú inner split legyen.
Ne legyen shuffle.
Ne legyen random week mixing.
Objective: ranking metric + stability.

Példa objective:

primary_metric = average_precision vagy AUC
secondary_metric = top_decile_lift
stability_metric = havi / heti bucket drift
calibration_metric = Brier / calibration error, ha probability kell
Production szimuláció

Minden test hónapban:

hónap elején base model train az addigi múltból;
hét elején calibrator frissítés csak múltbeli adatokból;
óránként score;
outcome mérés 60 perces targetre;
percentile bucketek értékelése.

Ez adja a legélesebb képet arról, hogy productionben működni fog-e.

9. Válasz a konkrét kérdéseidre

Jó-e így tanítani?
Nem teljesen. A 60 perces sampling jó lehet, de a randomizált train/valid időkeverés nem jó fő validációs módszernek.

60 percenként vegyünk mintát vagy teljes 1 perces mintát használjunk?
Baseline-nak 60 percenként. A teljes 1 perces mintát challenger kísérletként használnám, de csak purged / embargoed rolling validációval.

A decemberi train és novemberi valid okozhat-e időbeli inkonzisztenciát?
Igen. Én ezt nem engedném a fő backtestben. Nem elég, hogy a label nem fed át. A jövőbeli rezsim információja is torzíthat.

Mindegy-e percent adatoknál és órás targetnél?
Nem mindegy. A percent / return jelleg nem teszi függetlenné az idősoros mintákat. A volatilitás, intraday seasonality és rezsimfüggés miatt időrendben kell validálni.

Mi legyen a legjobb train-fold kialakítás?
Havi outer rolling-origin backtest + belső időalapú hyperparameter search. Train csak múltból, test csak jövőből. Gap legalább 60 perc vagy hosszabb feature lookback szerint.

Végső ajánlás

A legjobb kialakítás szerintem:

Órás anchor baseline, 60 perces target, rolling 36 hét train, következő 1 hónap test, heti kalibráció szimulálva, havi base model retrain, belső rolling hyperparameter search, legalább 60 perc gap / embargo.

A random 36 hét train + maradék 12 hét valid megközelítést legfeljebb robustness checkként használnám, nem döntő production-validációként.