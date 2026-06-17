---
epic: epic_014
id: t3
title: 5 éves sample generálás (2021–2025) + row count ellenőrzés
assignee: modeling_agent
status: pr
blocks: [t4]
blocked_by: [t2]
---

## Goal

Az új yearly sampling modul segítségével legenerálni az 5 éves sample artifaktot.

## Scope

5 új directory: `database/solusdt/samples/solusdt_fw60_yearly_{year}/`

## Acceptance Criteria

- [x] 5 sample directory létezik: `solusdt_fw60_yearly_2021` … `solusdt_fw60_yearly_2025`
- [x] Minden directoryban: `metadata.json`, `sample.parquet`, `audit.json`
- [x] `sample.parquet` olvasható, tartalmaz: open_time, segment, long_mfe_fw60, short_mfe_fw60
- [x] Minden évben pontosan 12 validation week van (hónaponként 1)
- [x] Segment distribution:
  - 2021: total=8760, valid=2016, purge=96, train=6648
  - 2022: total=8760, valid=2016, purge=96, train=6648
  - 2023: total=8760, valid=2016, purge=96, train=6648
  - 2024 (szökőév): total=8784, valid=2016, purge=84, train=6684
  - 2025: total=8760, valid=1920, purge=84, train=6756
- [x] Nincs adat-overlap: valid és train set-ben ugyanaz az open_time nem szerepel
- [x] Purge sorok sem valid, sem train set-ben nem szerepelnek
- [x] Segment értékek csak: {"train", "valid", "purge"}

## Notes

**2025 valid=1920** (nem 2016): A December 2025 validation week Dec 29–Jan 4-ig tart
(crossing into 2026). Az adatbázis csak 2025-ös sorokat tartalmaz, így a Jan 1–4
(96 óra) hiányzik a valid set-ből. Ez elfogadható viselkedés a ticket szerint.

**2024 purge=84** (nem 96): A szökőév és az év-határokon átnyúló purge ablakok
enyhe csökkentést okoznak — elfogadható.
