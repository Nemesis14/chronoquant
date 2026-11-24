# Epic 064: Direkt irány-előrejelzés 30 coinon, szelektív belépéssel

> **A user terve.** Nem a piac-semleges könyv (`epic_062`) folytatása, hanem az
> **eredeti direkciós architektúra 30 coinra kiterjesztve**: ugyanaz a
> feature → target → modell lánc, csak `symbol` oszloppal és 21-szer több
> tanítósorral.

## Goal

Megnézni, hogy a **merítés** megoldja-e azt, amit egy coinon nem sikerült.

Egy coinon a p99-es küszöb évi ~90 kötést adott — ennél szigorúbbra nem
lehetett menni, mert elfogyott a minta. Harminc coinnal ugyanannyi kötésért
a **p99,9**-re lehet menni, olyan tartományba, amit egy coinon soha nem
tudtunk mintavételezni.

Az `epic_058` adata szerint a szigorítás emeli a találatot: p95-nél ~50%,
**p99-nél 61,8%**. A kérdés, hogy ez a merítéssel tovább vihető-e, és
rezsim-független marad-e.

## A terv, ahogy a user rögzítette

- **Nincs kötelező kereskedés.** Ha egy coin sem éri el a küszöböt, nincs kötés.
- **Long ÉS short**, amit a modell mond. Nincs long-only megkötés.
- **Direkt (abszolút) becslés**, nem relatív rangsor.
- **24 órás target.**
- **Rangok nincsenek** — helyettük **részarány**: mekkora hányada a coin a pool
  forgalmának / OI-jának, és ez hogyan változik.
- Feature-ök **5 perces rácson**, kizárólag t-ben végződő gördülő ablakokkal;
  a score minden percre elérhető (forward-fill), a **belépés 1 perces**.
- `bookDepth` **később** — 18 GB, és a t821 abláció szerint bizonytalan a haszna.

## Univerzum — görgetett választás (user döntés)

Nem fix 30 coin 2024-től, hanem **félévente újraválasztott** top 30 a
megelőző időszak forgalma alapján, 2020-ig visszamenőleg. Így minden
időpontban az *akkori* top 30 van a panelben, szelekciós look-ahead nélkül —
és a tanítóadat ~2,5 évről ~5-6 évre nő.

## Tasks

- t851: Görgetett univerzum-választás
- t852: Nyers adat letöltése (ohlcv 1m, metrics 5m, premium, spot)
- t853: Feature-réteg — a meglévő modulok per-symbol futtatása
- t854: Részarány-blokk (új feature-csoport)
- t855: Vol-skálázott 24h triple-barrier target coinonként
- t856: Tanítómintavétel + gördülő mérés, szelektív belépéssel
- t857: Verdikt

Sorrend: t851 → t852 → (t853 ∥ t854) → t855 → t856 → t857

## ⚠️ Amit a korábbi mérésekből át kell hozni

- **Coinonként vol-skálázott korlát** (`epic_060`). Fix ±100 bps a BTC-n nagy
  mozgás, a PEOPLE-n zaj — enélkül a modell azt tanulná meg, melyik coin
  volatilis. Nem opcionális.
- **Maker belépés** (`epic_059`): limit a jel utáni percben, fill az
  `ohlcv_1m`-ből ellenőrizve. Fedezeti pont 55,3% → 52,8%.
- **Óránkénti mintavétel** coinonként. 24h targetnél két szomszédos perc
  címkéje 1439 percben átfed; minden percet betenni a tanításba hamis
  biztonságot ad.
- **Nincs bar-horgonyzott feature.** Minden ablak t-ben végződjön, különben
  train/serving skew.

## A döntési szabály — előre rögzítve

A szelektált halmaz találati aránya haladja meg a **vol-normalizált fedezeti
pontot** legalább **két, egymástól független likviditási sávban** ÉS
**mindkét rezsimben**.

| Kimenet | Verdikt |
|---|---|
| Mindkét sávban és mindkét rezsimben fedezeti pont fölött | **GO** |
| Csak az egyik sávban / rezsimben | részleges — a merítés nem elég |
| Sehol | **A merítés nem oldja meg. A direkciós ág véglegesen zárul.** |
