## Cél

**t933 (epic_073):** az Impulse Scout második modellcsaládja a `_doc_/methodology_doc/5300_impulse_scout.md` szerint: **P2 MiniRocket** (elsődleges, előre regisztrált **H2: P2 > baseline**, 1. kör, rögzített konfiguráció) és **X1 InceptionTime** (kísérleti, hipotézis nélkül), mindkettő a **nyers, normalizált prefixen**. A kérdés: a kézi skalár jellemzők (S0 + C) mellett hordoz-e a sorozat alakja többletet az impulzus előrejelzésében.

Futás `b98ecd97217b`: **2170 train esemény**, ebből **308 impulzus** (14.2%), assetek: btcusdt,ethusdt,solusdt; közös 6 blokkos purged CV (fold-hash `0d90521c16be85de`), embargó 8 h. A számok a t931 benchmark kereten (`scout_bench`, a t932 fold-rang top-q nézetével) születtek; a leaderboard elsődleges top-q nézete a **teszt-foldon belüli rangsor**.

> **Holdout-védelem:** csak a train split; az 1m bar-ok SQL-szűrővel `open_time < 2026-02-01` (OOS és holdout-2 nem került memóriába); a prefix kizárólag a `t_c` előtt záruló bar-okból épül (teszttel ellenőrizve).

## Bemenet: a nyers prefix

A `[t0 − B, t_c − 1m]` nyitású 1m bar-ok (B = W = 4 h, 480 bar, mind `t_c`-ig zárul) **5 perces** bar-okra összevonva (L = 96), három csatorna:

| csatorna | definíció | megjegyzés |
|---|---|---|
| `close_sigma` | `log(close / low[t0]) / σ_W` | az amplitúdó megmarad (5300: a jel a visszapattanás relatív mérete) |
| `log_vol_rel` | `log1p(volume / (5 · vol7d_mean_1m))` | az asset saját, `t0`-ig ismert 7 napos átlagához |
| `taker_imb` | `taker_buy_base / volume − 0,5` | order flow a bar-okból (üres bar: 0) |

Asset-azonosító nincs (5300). Az osztályonkénti átlagos prefix leíró jellegű (train címkék):

{{chart:prefix_close}}

{{chart:prefix_imb}}

## Modellek (rögzített konfiguráció)

**P2 MiniRocket (H2):** a Dempster–Schmidt–Webb (2021) MiniRocket saját numpy-implementációja (nincs sktime / aeon függőség): 84 rögzített, 9 hosszú {−1, 2} súlyú mag × a sorozathosszból adódó dilatációk, többváltozós csatorna-kombinációk, a bias-kvantilisek a fold-train példákból (seed), PPV jellemzők (~10 000). A transzformáció **címkefüggetlen**, foldonként egyszer számolódik és cache-elődik: a permutációban a jellemzők azonosak, a címkefüggő fej újratanul (a 5300 szerinti teljes refit). Fej: ridge regresszió az egy-forró osztályokra (zárt alak a fold-train Gram-mátrix sajátfelbontásával), `alpha` pontos leave-one-out hibával egy rögzített rácson (10⁰…10⁷, 15 pont, mint a `RidgeClassifierCV`), majd multinomiális logisztikus kalibrátor (C = 1) a LOO ridge-kimeneteken → 3 osztály valószínűsége.

**X1 InceptionTime (kísérleti):** kis változat (3 Inception-modul, 8 szűrő, magméretek 15 / 7 / 3, bottleneck 8, reziduális a 3. modul után, GAP + lineáris fej), Adam lr 1e-3, weight decay 1e-4, batch 128, max 25 epoch, **early stopping (türelem 4) a fold-train utolsó 20%-án (időrend)** — a teszt-fold soha. Egy háló (nincs ensemble), 1 szál, determinisztikus. A méretet a CPU-futásidő határozta meg (párhuzamos agent-futások mellett), nem eredmény.

Konfiguráció-hash: MiniRocket `9854a1bf00dd`, InceptionTime `95ace9117cdb`.

## 1. Leaderboard (ez a futás)

A baseline sor referencia (a közös tábla baseline-sorát a t931/t932 futás adja; ide csak a t933 sorai kerültek perzisztálásra).

{{table:leaderboard}}

## 2. H2: MiniRocket vs baseline

Összevont OOF AUC **0.6324** a baseline 0.6361 mellett: ΔAUC **-0.0037**, nap-klaszter bootstrap 95% CI [-0.0349; 0.0295]; **perm. p = 0.3057** (N = 1000, nap-csomagos, teljes refit; Holm ebben a futásban egyetlen elsődlegesre = 0.3057, a H1–H3 közös Holm-ja a t936-ban). 3-osztályos log-loss 0.7776 (Δ -0.0233); felső 20% (fold-rang) precizitás 0.216 vs baseline 0.241 (lift 1.52); ΔAUC > 0 blokkok: 3/6. Go / no-go: **NO-GO** (bukott: dauc,top20,stability,spa,holm,econ).

{{chart:null_rocket}}

{{chart:blocks_rocket}}

{{table:assets_rocket}}

## 3. X1: InceptionTime (kísérleti)

Összevont OOF AUC **0.6271**, ΔAUC **-0.0090** [-0.0424; 0.0247], 3-oszt. log-loss 0.7878 (Δ -0.0131); felső 20% precizitás 0.234 (lift 1.65); ΔAUC > 0 blokkok 3/6; perm. p **n/a** (**nem futott**: a csökkentett, N = 100-as X1 permutációt a user CPU-terhelés miatt leállította; a t936 szükség esetén újrafuttathatja — a perm-függő érték hiányzik, nem teljesült). Go / no-go: **NO-GO** (bukott: dauc,top20,stability,spa,econ).

{{chart:blocks_inception}}

{{table:assets_inception}}

## 4. Kalibráció

{{chart:reliability_rocket}}

{{chart:reliability_inception}}

{{table:calibration_asset}}

## 5. Kereskedési szimuláció (triple barrier, `config/strategies.json` költségek)

Elsődleges jelzés-halmaz: felső 20% / 10% a teszt-foldon belüli rangsor szerint; a train-küszöbös nézet másodlagos. A CV-szimuláció csak előszűrő (5300 B).

{{table:trades}}

## 6. Futásidő

{{table:runtime}}

## Értelmezés

- **H2 nem igazolt:** a MiniRocket ΔAUC -0.0037 (CI [-0.0349; 0.0295]), perm. p 0.3057 ≥ 0,01 → a nyers prefix alakja a skalár alapon túl nem ad zajtól megkülönböztethető többletet; státusz **NO-GO** (a baseline marad).
- A MiniRocket 3-osztályos log-lossza jobb a baseline-nál (Δ -0.0233), miközben a bináris rangsorolás (AUC, felső 20%) nem javul: a többlet a hamis indulás / oldalazás szétválasztásából és a kalibrációból jön, nem az impulzus-rangsorból, ami a H2 döntési metrikája.
- Assetenként a ΔAUC előjele nem egységes (nem negatív: 1/3, legrosszabb -0.0402); blokkonként 3/6 pozitív.
- **InceptionTime (X1):** ΔAUC -0.0090, perm. p nem futott (user leállította, CPU); kísérleti modell, státusz NO-GO.
- A H1–H3 közös Holm-korrekcióját, a teljes modell-univerzum SPA-ját és a 2. kört a **t936** végzi; ez a riport az 1. kör rögzített konfigurációinak eredménye, iteráció nélkül.
- Holdout-2 érintetlen; az OOS sem került felhasználásra.

## t936 frissítés (2026-11-22, leaderboard-univerzum)

Az SPA és a Holm értékek a **t936** együttes kiértékeléséből származnak (`_doc_/reports/scout_leaderboard/`): a teljes 1. körös univerzum (6 modell a baseline ellen, bináris log-loss, nap-összegzett) SPA p = **0.2300**, legjobb modell: `scout_hazard_r1`. Az A7 kapu csak a kiválasztott legjobb modellre vonatkozik (a többinél n/a, `not_spa_best`), a B kapu a felső 20% és 10% szimulációt is nézi. A táblákban a `spa_p` / `holm_p` / kapuk ennek megfelelően frissítve; a fenti szöveg korábbi, futásonkénti SPA-értékei elavultak.

- `scout_minirocket_r1`: SPA-legjobb: nem, Holm p (H1–H3 együtt) **0.8062**; go / no-go **NO-GO** (bukott: `dauc,top20,stability,holm,econ,not_spa_best`).
- `scout_inception_r1`: SPA-legjobb: nem; go / no-go **NO-GO** (bukott: `dauc,top20,stability,econ,not_spa_best`).
