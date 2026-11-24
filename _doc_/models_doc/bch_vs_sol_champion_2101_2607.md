# BCH vs SOL champion, `2101_2607` (epic_069 t908)

**Mit vizsgáltunk.** A `lgbm_bchusdt_{l,s}_fw60_2101_2607` párt a `lgbm_solusdt_{l,s}_fw60_2101_2607` champion párral összevetve: (1) valódi-e az előrejelző erő, (2) miért net negatív a BCH stratégia, (3) mit érnek a jelek (percentil >= 0,99) önmagukban, (4) használható-e.

**Módszer.** Riportok: `artifacts/<id>/reports/{03_hyperparameter_search,04_strategy}`. Saját számolás csak-olvasó DuckDB-ből (`predictions` + `target`, minden perc): train 2021-01..2026-01 (2,67 M sor), valid 2026-02-01..2026-07-31 (260 k sor). Lift = top 10% predikciójú sorok átlag-MFE-je / teljes átlag. Jel = valid-ablak 99. percentilisén belüli sor (short: alsó 1%), a forward 60 perces mozgás a `target.fw60_max_ratio / fw60_min_ratio / fw60_close_logret` alapján (bps, log). A feldolgozott "first-of-run" sor a jelsorozat első bárja (a stratégia `rank_first` módjához közelebb).

## 1. Előrejelző erő (valid vs train)

| | SOL long | SOL short | BCH long | BCH short |
|---|---|---|---|---|
| top10 lift, train / valid | 2,39 / 2,02 | 2,35 / 2,02 | 2,29 / 1,98 | 2,41 / 2,07 |
| top1% lift, train / valid | 4,70 / 3,39 | 4,40 / 3,38 | 2,47 / 2,77 | 4,62 / 2,94 |
| Pearson(pred, MFE) valid | 0,39 | 0,35 | 0,32 | 0,35 |
| search valid ratio (p925 / p075) | 2,255 | 1,523 | 2,119 | 1,340 |
| train-valid gap | 0,39 | -0,07 | 0,51 | 0,06 |
| search Spearman / decilis-monoton. | 0,303 / 0,89 | 0,112 / 0,78 | 0,317 / 1,00 | 0,100 / 0,44 |

A top10 lift BCH-n 1,98 / 2,07 valid (a t906 óránkénti mintán 1,93 / 2,18), a SOL-lal gyakorlatilag azonos és a train-hez konzisztens (nincs jelentős túlillesztés). A rangsorolás tehát **valódi, de kb. 5% gyengébb a SOL-nál** a farokban (top1% lift és Pearson alacsonyabb). A short search-metrika (p075 1,34, decilis-monotonitás 0,44) szerényebb, mint a SOL-é (1,52 / 0,78); a BCH long top1% lift a train-ben csak 2,47 (BCH long `long_pred` diszkrét/ties: a valid 99. percentilis fölött 3 958 sor van a várt ~2 600 helyett), vagyis a jelszint-élesség gyengébb. Fontos: a target az **MFE nagysága**, és ezt a modell jól rangsorolja; ez nem azonos az irány-előrejelzéssel (lásd 3.).

## 2. Miért net negatív a BCH stratégia

| | BCH long | BCH short |
|---|---|---|
| trade (valid, cutoff 0,99) | 203 | 232 |
| gross log return | +0,0116 | +0,1301 |
| gross / trade | 0,6 bps | 5,6 bps |
| költség (fee+slip) / trade | 11,1 bps | 10,5 bps |
| net log return | -0,2124 | -0,1146 |
| win rate | 55,2% | 63,4% |

Nem a trade-szám a fő gond (203-232 trade kevés, a SOL 2 700-3 000 trade), hanem hogy a **gross edge/trade (0,6-5,6 bps) a ~10,5-11 bps körút költségnek csak töredéke**. A win rate magas (TP közel van), de a nyereség kicsi, a veszteséges kilépések nagyobbak. Mind a 200+200 grid-setup net negatív. Likviditás (valid, 1m): medián quote volume BCH 32,6 k vs SOL 702 k USDT/perc (~21x vékonyabb), medián trade-szám 261 vs 842, átlagos 1m high-low range 10,0 vs 11,6 bps (medián 7,2 vs 8,9). A BCH nem kevésbé volatilis, de vékonyabb a könyv: a globális 1,5 bps slippage BCH-ra optimista, a valós költség a modellezettnél nagyobb lehet; a spread/mélység közvetlenül nem mérhető a kline-okból.

## 3. A jelek önmagukban (forward 60 perc, valid, bps)

| csoport | n | MFE | MAE | MFE-MAE | 60p close ret |
|---|---|---|---|---|---|
| SOL minden bár | 260 635 | 43,0 | 45,2 | -2,2 | -0,9 |
| SOL long top1% | 2 607 | 145,9 | 148,2 | -2,3 | +15,7 |
| SOL long top1% first-of-run | 339 | 133,0 | 117,0 | +16,0 | +30,6 |
| SOL short alsó1% | 2 607 | 152,9 | 147,8 | +5,1 | +20,3 |
| SOL short alsó1% first-of-run | 235 | 114,2 | 117,0 | -2,8 | +19,8 |
| BCH minden bár | 260 640 | 40,6 | 43,5 | -2,9 | -2,1 |
| BCH long top1% | 3 958 | 112,6 | 107,0 | +5,6 | +16,0 |
| BCH long top1% first-of-run | 438 | 95,2 | 98,6 | -3,4 | +6,2 |
| BCH short alsó1% | 2 607 | 128,0 | 129,3 | -1,3 | +20,2 |
| BCH short alsó1% first-of-run | 345 | 129,5 | 100,1 | +29,4 | -13,0 |

(MFE/MAE az adott irány szerint: long MFE = max felmozgás, MAE = min lemozgás; short fordítva. A "60p close ret" a nyers ár-log return, short esetén a pozitív érték a short vesztesége.) Mindkét asset jelei után a mozgás **3x nagyobb az átlagnál (MFE ~100-150 vs 40-45 bps), de MFE ≈ MAE**: a modell elsősorban a **kitörés/volatilitás nagyságát** jelzi előre, az irányt alig. A BCH long jelek után a 60 perces close return +16 bps (kb. SOL-szint), a short jelek után az ár átlagosan *felfelé* megy (+20 bps, a short vesztes; first-of-run mintán n=345 -13 bps, de kis minta, átfedő ablakok, erős szóródás). A jelek által mutatott irányú edge (MFE-MAE, close ret) ~0-15 bps, ez a ~11 bps költség körül mozog, a SOL-nál sem robusztusan pozitív.

## 4. Verdikt

**Használható megjelenítésre / jel-figyelésre** (a háromszögek nagy mozgású szakaszokat jeleznek előre: ~2,5-3x átlagos 60 perces mozgás), **nem megbízható kereskedési (irány-) jelnek**: az élő/éles kereskedés BCH-n nem javasolt, a költség utáni stratégia mindkét irányban veszteséges.

**Korlátok.** Rövid valid ablak (6 hónap, 2026-02..07, egyetlen rezsim); egy asset; nincs OOS (a kalibráció és a stratégia-opt ugyanazon valid ablakon fut, `same_window`); a lift szignifikanciáját nem teszteltük, a minden-perc minták erősen autokorreláltak, effektív n kisebb; a BCH slippage-becslés optimista (vékony könyv); a 2019-2020 szakasz kimaradt; a short search metrika gyenge (decilis-monotonitás 0,44).

**UI-ellenőrzés (2026-11-07).** `/api/assets` (solusdt, bchusdt, has_data), `/api/status?asset=bchusdt` (live adatok, modell-id-k, worker alive), `/api/market/chart?asset=bchusdt` (1 441 bar, long_pct/short_pct, küszöbök 0,99/0,99, 24 h-ban 36 long / 0 short jel), `/api/strat/active?asset=bchusdt` (cutoff 0,99, artifact metrikák), `/api/reports` (8 BCH riport: 4+4 modell), `GET /a/bchusdt` és `/a/solusdt` route 200 (a BFF 8600-on szolgálja a frontendet). Megfigyelés: a chart `active_trade` egy valódi Binance BCHUSDT long (entry 268,49), nem hiba.
