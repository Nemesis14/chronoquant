# Epic 057: 4 órás horizont — validáció, short irány, deploy

> **Kiváltó ok:** a `fw60` (1 órás) modell éle kötésenként ~11,5 bps, a díj
> ~11 bps — a stratégia nulla körül mozgott. A 4 órás horizonton az él 27,5
> bps, a kötésszám harmada, és az eredmény **out-of-sample is pozitív**.

## Goal

Eldönteni, hogy a 4 órás modell éles tőkére alkalmas-e, és ha igen, a
felületre vinni long és short irányban.

## Ami eddig kijött

| | fw60 (1h) | fw240 (4h) |
|---|---|---|
| él / kötés | 11,53 bps | **27,54 bps** |
| kötés / 6 hó | 401 | **140** |
| díj a bruttó %-ában | 96% | **36%** |
| break-even díjszint | 11,66 bps | **34,54 bps** |
| **out-of-sample (3 hó)** | +23 USD | **+148 USD** |

Ugyanaz a setup nyert in-sample és out-of-sample: `cutoff=0,99`,
`TP=bucket_p75_mfe`, `SL=2,0×TP`.

## Sizing — eldöntve

A 140 kötés kamatos szimulációja alapján:

| Tőkeáttétel | Végső tőke | Max visszaesés |
|---|---|---|
| 2x | 1,56× | −42,9% |
| **5x** | **2,31×** | **−77,4%** |
| 10x | 1,92× | −96,4% |
| 20x | **0,02×** | −100% |

**Döntés: 5x, 1000 USD keret** (a számla ~10%-a). Az 5x nem kompromisszum:
több végső tőkét ad, mint a 10x, kisebb visszaeséssel. 20x-en négy egymás
utáni SL −95% — és 72%-os nyerő aránynál a négyes sorozat 140 kötésen
**várható**, nem szélsőség.

## Tasks

- t601: Éves out-of-sample validáció (train → 2025-07, valid 2025-08 → 2026-07) → modeling_agent
- t602: Short irány fw240 modell → modeling_agent
- t603: Deploy — predictions + strategy artifact + UI → ui_agent

Sorrend: t601 → t602 → t603

## Key Decisions

- **Az éves teszt a deploy előfeltétele.** A jelenlegi out-of-sample 25 kötés;
  ez egy éles döntéshez kevés. A train végét 2025-07-re húzva 12 hónapos
  ablakot kapunk ~100 kötéssel.
- **A havi gördülő teszt nem használható** erre a stratégiára: 0,99-es
  cutoffnál egy hónapban 0–6 kötés van. Negyedéves vagy hosszabb ablak kell.
- **A `max_hold_minutes` 240**, nem 60. A modell 4 órára jósol; 60 perc után
  kiszállni értelmetlen.
- **A TP limit orderként megy be**, a SL stop-marketként. A user kézzel
  kereskedik, tehát ez adott — a backtest költség-modellje ezzel számol
  (TP = maker 2 bps, SL/timeout = taker 5 bps + csúszás).

## Notes

Előzmény: `epic_054` (alt-data — a GO verdikt visszavonva, nem élte túl az
out-of-sample tesztet), `epic_055` (költség-modell — enélkül minden korábbi
stratégia-eredmény torzított volt).

A `fw240` modell a 22 alt-data feature-t **használja** (20 túlélte a FE-t), de
a horizont-hatás nagyságrenddel nagyobb, mint az alt-data hozadéka.
