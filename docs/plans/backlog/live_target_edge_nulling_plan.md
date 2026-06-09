# Backlog: Live sync target edge-row nulling

## Probléma

A `sync_features` forward-looking targetet számít minden szinkronizált sorhoz:

```python
rolling_max = df["close"][::-1].rolling(rolling_win, min_periods=1).max()[::-1]
ratio_long  = _safe_div(rolling_max, df["close"])
threshold   = ratio_long.quantile(percentile)
df[target_col] = (ratio_long >= threshold).astype(int)
```

A `min_periods=1` miatt az utolsó `rolling_win` sorban (pl. fw60 esetén az utolsó 60 perc) **nincs forward adat** — a rolling max csak az aktuális záróárat látja, így `ratio = 1.0`. Mivel a threshold általában `> 1.0`, ezek a sorok mindig `target = 0` értéket kapnak.

### Miért gond ez

Az inkrementális live sync során (`sync.py` → `sync_features`) az **éppen szinkronizált új sor mindig az edge-zónába esik** (az utolsó `rolling_win` bar egyike). Tehát minden live-ból érkező sor helytelen `0` targetet kap, holott a valós forward adat még nem áll rendelkezésre.

Az ősidők (bulk load) során ez nem volt gond, mert ott a teljes historikus adaton számolódott a target és az első `rolling_win` sor szintén rossz, de ezeket sosem használjuk training-hez.

**Ha ma új modellt fejlesztenénk a jelenlegi features táblából:**
- A Jun 8–9-i (és minden incremental-sync által töltött legújabb) sorok `trg_l_fw60_q90 = 0` értékkel szerepelnének
- A training sample ezeket **helyes negatív példaként** kezelné, holott azok **ismeretlen** példák
- Ez torzítja a training adatot, különösen ha a modell fejlesztési időszak vége közel van a live időszakhoz

### Skálája a problémának

`fw60` esetén: az utolsó 60 sor mindig rossz. Inkrementális sync-ből érkező sorok száma naponta ~1440. Ezek mind helytelen `0` target-et hordoznak mindaddig, amíg 60 perc el nem telik **és** egy újabb sync felülírná őket — de a `drop_existing_open_times` megakadályozza a felülírást, tehát ezek a sorok örökre helytelenek maradnak a táblában.

## Javasolt megoldás

Az edge-zóna target-jeit `NULL`-ra kell állítani, nem `0`-ra.

### Implementáció — `sync_features.py`

Az `sync_features` függvényben, a target kiszámítása után, az utolsó `rolling_win` sort `NULL`-ra kell állítani:

```python
# Edge-row nulling: a forward ablak nem teljes az utolsó rolling_win sorban
edge_mask = df.index >= df.index[-rolling_win] if len(df) >= rolling_win else slice(None)
df.loc[edge_mask, target_col] = np.nan
```

Ez azt jelenti:
- Az utolsó `rolling_win` sorban a target `NULL` lesz a features táblában
- A predictions táblában ezekre a sorokra `target = NULL` kerül (megfigyelési célból)
- A training sample generáláskor ezeket a sorokat ki kell szűrni (`dropna` a target oszlopon)

### Egyéb teendők

1. **Training pipeline**: `dropna(subset=[target_col])` hozzáadása a sample generáláshoz, ha még nincs meg.
2. **Visszatöltés**: A már meglévő rossz `0` target értékeket a features táblában NULL-ra kell frissíteni a live sync által töltött sorokra. Ez egy egyszeri `rebuild_derived_tables` futtatással megoldható.
3. **Dokumentáció**: A features tábla target oszlopainak dokumentálásában jelezni, hogy `NULL` = "forward adat még nem áll rendelkezésre", `0` = biztosan nem teljesült, `1` = teljesült.

## Érintett fájlok

- `src/data_pipeline/sync_features.py` — edge-nulling logika a target számítása után
- `src/db/maintenance.py` — `rebuild_derived_tables` futtatás a javításhoz
- (opcionálisan) `src/modeling/datasets.py` — `dropna` ellenőrzése

## Megjegyzés

A jelenlegi live predikciókra ez **nem hat ki**: a modell csak `feat_*` oszlopokat használ inferenciára, a target oszlopot nem. A probléma kizárólag a jövőbeli modell fejlesztést érinti, ahol a features tábla a training forrás.
