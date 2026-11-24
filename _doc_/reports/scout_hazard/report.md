## Cél

**t934 (epic_073), H3:** a 5300 P3 családja — a megerősített mélypont (`t_c`) utáni két, egymást kizáró esemény (**impulzus**: high > P*, **hamis indulás**: low < low[t0]) **versenye az idő függvényében**, az oldalazás mint `H = 480` percnél cenzorált kimenet. A modell a végpont-címke mellett az első érintés idejét (`τ`) is felhasználja; a P(impulzus) a kumulált incidencia `H`-nál. Az összehasonlítás a közös benchmark keretben (`scout_bench`, t931/t932) fut, a skalár S0 + C baseline ellen, ugyanazokon a foldokon.

Futás `78a7589a381e`: **2170 CV-esemény**, ebből **308 impulzus** (14.2%), assetek: btcusdt,ethusdt,solusdt, cella W4h_H8h. Fold-hash `0d90521c16be85de`, adat-hash `0a723719fc86eedc`.

> **Holdout-védelem:** csak a train split (`sb.load_scout_events`), a flow-jellemzők csak a train események `flow_key`-ére olvasva, az 1m árak SQL-szűrővel `open_time < 2026-02-01`; holdout-2 sor (≥ 2026-08-01) nem került memóriába.

## Modell (előre rögzített 1. köri konfiguráció)

- **Idő-diszkretizálás:** `(0, 480]` perc, **16 × 30 perc** sáv; a `j` sáv `(30j, 30(j+1)]`.
- **Személy-periódus kiterjesztés:** impulzus / hamis indulás a `j` sávban → `0..j` sorok, a `j`-ben az esemény, előtte `nincs`; oldalazás → mind a 16 sor `nincs` (cenzorált `H`-nál). A kiterjesztés a fold tanító eseményein belül, a `fit`-ben készül, így egy esemény sorai sosem lépnek át foldot (a purge eseményekre hat).
- **Tanuló:** multinomiális logit (`nincs / impulzus / hamis indulás`), L2, C = 0.1; idő-tagok: sáv-dummy-k (arányos kovariáns-hatások).
- **Kovariánsok (`t_c`-ig ismert):** S0 + C (a_sigma, velocity_1h, pretrend_3d, log_sigma_1m, drop_depth, volume_ratio) + 4 order-flow (vol_decline_rel, trades_rel_decline, qv_logz_prefix, tb_ratio_rebound; log: vol_decline_rel, trades_rel_decline); fold-train medián-imputálás, standardizálás, ±5 vágás.
- **Háromosztályos valószínűség:** `CIF_imp(H)`, `CIF_fs(H)`, oldalazás = `S(H) = 1 − összeg` (a konstrukció miatt ≤ 1). Bináris score = `CIF_imp(H)`.
- **Ellenőrzés:** a riport saját OOF illesztése és a harness OOF valószínűsége közti max. eltérés 0.00e+00.

{{table:person_period}}

## 1. Eredmény a baseline-hoz képest

ΔAUC és CI: összevont OOF, nap-klaszter bootstrap. A felső-q% elsődleges nézete a **teszt-foldon belüli rangsor** (epic döntés, t932 harness). A `perm. p` a hazardnál a ΔAUC nap-csomagos permutációs p-értéke (N = 1000, teljes refit mindkét modellen); ebben a futásban a Holm-család csak a H3-at tartalmazza, a H1–H3 együttes Holm-korrekciója a t936 leaderboardon történik. Az SPA itt egyetlen modellre fut (a modell-univerzum a t936-ban).

{{table:comparison}}

{{chart:null_dauc}}

## 2. Kumulált incidencia a becsült score kvintilisei szerint

A pooled OOF `CIF_imp(H)` score szerint 5 egyenlő csoport; folytonos vonal: megfigyelt CIF (a cenzorálás csak `H`-nál történik, így a kumulált arány pontosan az Aalen–Johansen becslés), szaggatott: a modell átlagos becsült görbéje. A két görbe illeszkedése az **időbeli (time-to-hit) kalibráció**.

{{chart:cif_quintiles}}

{{table:quintiles}}

{{chart:time_calibration}}

{{table:time_calibration_table}}

## 3. Hazard-alak és együtthatók

Átlagos OOF diszkrét hazard sávonként (az események kockázatban lévő halmazától függetlenül, minden eseményre kiértékelve), és a teljes train-en illesztett modell standardizált együtthatói (log-odds a `nincs` kimenethez képest; leíró, nem döntési alap).

{{chart:hazard_shape}}

{{table:coefficients}}

## 4. Blokk-, asset- és kalibráció-bontás

{{chart:blocks}}

{{table:blocks_table}}

{{table:assets_table}}

{{table:calibration_asset}}

## 5. Triple-barrier szimuláció (díjak után)

Jelzés a teszt-foldon belüli rangsor szerint; költségek a `config/strategies.json`-ból (`{"entry_order_type": "taker", "exit_order_type": {"sl": "taker", "timeout": "taker", "tp": "maker"}, "maker_bps": 2.0, "slippage_bps": 1.5, "taker_bps": 5.0}`); CI nap-klaszter bootstrap.

{{table:trades}}

## Értelmezés

- **Rangsorolás:** AUC **0.6474** vs baseline 0.6361, ΔAUC **+0.0114** (CI [-0.0040; +0.0269]), perm. p **0.2687**; ΔAUC > 0 a blokkok 4/6-ában, legjobb blokk nélkül átlag +0.0061.
- **3 osztály:** log-loss 0.7677 vs 0.8009 (ΔLL -0.0332); a hazard a hamis indulás / oldalazás szétválasztását is a kovariánsokból becsüli (a baseline ezt konstans aránnyal osztja).
- **Szelekció:** felső 20% precizitás 0.228 vs 0.241 (lift 1.60); a legfelső kvintilisben a megfigyelt CIF_imp(H) 0.224 (becsült 0.243), a legalsóban 0.053.
- **Időbeli kalibráció:** a pooled megfigyelt és becsült impulzus-CIF legnagyobb eltérése 0.0062; az impulzusok medián első érintése a felső kvintilisben 171 perc.
- **Go / no-go (5300, harness):** NO-GO — bukott feltételek: dauc,top20,stability,spa,holm,econ. A H3 végleges Holm-korrigált értékelése a t936-ban (H1–H3 együtt) történik.
- **Korlátok:** az 1. kör rögzített, hangolatlan konfiguráció; a flow-részhalmazt a t930 leíró (train) táblája alapján választottuk (enyhe előzetes adat-ismeret); finanszírozási díj nincs; a holdout-2 érintetlen.

Lab DB: csak a `scout_hazard_r1` sor + a futás sora íródott (`78a7589a381e`).

## t936 frissítés (2026-11-22, leaderboard-univerzum)

Az SPA és a Holm értékek a **t936** együttes kiértékeléséből származnak (`_doc_/reports/scout_leaderboard/`): a teljes 1. körös univerzum (6 modell a baseline ellen, bináris log-loss, nap-összegzett) SPA p = **0.2300**, legjobb modell: `scout_hazard_r1`. Az A7 kapu csak a kiválasztott legjobb modellre vonatkozik (a többinél n/a, `not_spa_best`), a B kapu a felső 20% és 10% szimulációt is nézi. A táblákban a `spa_p` / `holm_p` / kapuk ennek megfelelően frissítve; a fenti szöveg korábbi, futásonkénti SPA-értékei elavultak.

- `scout_hazard_r1`: SPA-legjobb: igen, Holm p (H1–H3 együtt) **0.8062**; go / no-go **NO-GO** (bukott: `dauc,top20,stability,spa,holm,econ`).
