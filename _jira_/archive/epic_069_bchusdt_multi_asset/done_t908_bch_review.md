---
epic: epic_069
id: t908
title: BCH eredmény-áttekintés és UI-ellenőrzés
assignee: analyst_agent
status: done
blocks: ["t909"]
blocked_by: ["t907", "t904"]
---

## Goal
Szakmai kapu: mit mutat a BCH modell, érdemes-e aktívnak jelölni.

## Scope
`artifacts/lgbm_bchusdt_*/reports/**`, `_doc_/models_doc/` (rövid BCH összefoglaló md).

## Acceptance Criteria
- [x] Rövid md `_doc_/models_doc/`: valid top10_lift, stratégia-eredmény költséggel, a SOL-lal összevetve, korlátok
- [x] A UI BCH fülén a chart, a jelek (zöld/piros háromszögek), a stratégia kártya és az Elemzések megjelennek (böngészős vagy API-szintű ellenőrzés)
- [x] Egyértelmű verdikt a Notes-ban: használható / csak megjelenítés / nem megbízható (lásd Notes)

## Notes

### Verdikt
**Használható megjelenítésre / jel-figyelésre; nem megbízható kereskedési (irány-) jelnek.** Az éles/költséges kereskedés BCH-n nem javasolt. Teljes elemzés: `_doc_/models_doc/bch_vs_sol_champion_2101_2607.md` (README index frissítve).

### Kulcsszámok (valid 2026-02..07, minden perc, csak-olvasó DuckDB)
- top10 lift valid (train): BCH long 1,98 (2,29), short 2,07 (2,41); SOL long 2,02 (2,39), short 2,02 (2,35). Nincs jelentős túlillesztés; a farok (top1% lift, Pearson) BCH-n gyengébb (long 2,77 vs 3,39). Short search-metrika szerényebb (BCH p075 1,34, decilis-monoton. 0,44 vs SOL 1,52 / 0,78).
- Net negatív: gross/trade long 0,6 bps, short 5,6 bps vs ~10,5-11,1 bps költség/trade; 203/232 trade; BCH likviditás ~21x vékonyabb (medián quote vol 32,6k vs 702k USDT/perc), 1m range hasonló (10,0 vs 11,6 bps) -> a 1,5 bps slippage optimista.
- Jelek (99. percentilis) után 60 percben az MFE ~100-150 bps (átlag 40-45), de MFE ≈ MAE: a modell mozgás-nagyságot/volatilitást rangsorol, irányt alig (BCH long jel: MFE 112,6 / MAE 107,0, close +16 bps; short jel: MFE 128,0 / MAE 129,3, close +20 bps = short veszt). SOL-lal azonos mintázat.
- Korlátok: 6 hónapos valid, egy asset, nincs OOS (calib=opt=valid), nincs szignifikancia-teszt, átfedő/autokorrelált minták, BCH slippage optimista.

### UI-ellenőrzés (API-szint)
- `/api/assets`, `/api/status?asset=bchusdt` (live, modell-id-k, worker alive), `/api/market/chart?asset=bchusdt` (1441 bar, long/short pct, küszöbök 0,99, 24 h-ban 36 long / 0 short jel; `active_trade` valódi Binance long, nem hiba), `/api/strat/active?asset=bchusdt`, `/api/reports` (8 BCH riport, 4+4), `GET /a/bchusdt` 200 (a BFF 8600-on szolgálja). Böngészős vizuális ellenőrzés nem történt.
- Megfigyelés: a 09:35-ös BCH `sync_full` auto-job egyszer elbukott (Windows Defender fogta a `bchusdt.duckdb` fájlt), a következő ciklus újrapróbál; ismétlődés esetén a Defender kizárás javasolt (UI/ops agent).
- DB-t nem írtam, modellhez/artifacthoz nem nyúltam.
