---
name: recs
description: >-
  Operator interface to the recommendation engine in this repo. Runs the
  scripts that produce and consume buy and sell recommendations - refreshes
  warehouse data, regenerates the private decision report (buy side) and the
  weekly sell review (sell side), and records real fills through
  scripts/trade.py so the trade journal and the holdings snapshot update
  together. Use when the user asks what to buy or sell, wants the latest
  recommendations, asks for a fresh report, says they bought or sold
  something and want it recorded, wants the basis seeds installed, or asks
  what the engine currently holds on record. Never emits a new call round -
  that stays with scripts/rebalance.py on its monthly schedule.
argument-hint: "[buys|sells|refresh|trade <buy|sell> SYM QTY --account A|seed|show]"
disable-model-invocation: true
---

# /recs - recommendations and fills

All output is private (`reports/`, `trades.jsonl`, `knowledge/POSITIONS.md`
are gitignored). Never paste symbols, quantities or accounts into anything
tracked. Run every command from the repo root with `uv run`; EXECUTE the
scripts, do not read them unless one fails.

| Argument | Runs | Result |
|---|---|---|
| none | `refresh` if stale, then `buys`, then `sells` | the full picture |
| `refresh` | `scripts/backfill.py` then `scripts/build_local.py` | warehouse at the latest completed session (~3 min) |
| `buys` | freshness gate, then `scripts/decide.py` | `reports/decision_<today>.md` - buy/hold/sell calls from the latest round, held overlay, loss harvest, emerging screen |
| `sells` | freshness gate, then `scripts/weekly_review.py` | `reports/weekly_<today>.md` - act / too-soon / punt per held name |
| `trade buy SYM QTY --account "A" [--price P] [--date D] [--note N]` | `scripts/trade.py buy ...` | one journal line plus the snapshot quantity, both validated first |
| `trade sell SYM QTY --account "A" ...` | `scripts/trade.py sell ...` | same; refuses to sell more than the snapshot holds |
| `seed` | `scripts/trade.py seed` | installs the basis lots from the `_relay.md` jsonl block, once |
| `show` | `scripts/trade.py show` | open lots per account and any journal-vs-snapshot drift |

## Reporting back

After `buys` or `sells`, summarize the report in a few lines: counts per
call or verdict, which held names changed stance since the previous report
in `reports/`, and the harvest section if non-empty. Quote the report
file path. Do not restate the caveats block; say it is there.

After `trade`, echo the script's one-line confirmation and nothing else.

## Freshness gate

Before `buys` or `sells`, run `uv run python scripts/fresh.py`. Exit 0
means current - proceed. Exit 1 means the warehouse lags the last
completed session - run `refresh` first, then proceed. Exit 2 means no
warehouse - run `scripts/backfill.py` in full. Never generate a report
over stale data without saying so; if the user explicitly wants the
report anyway, state the data vintage next to the file path.

## Rails

- `decide.py` refuses when no round is recorded; a new round is
  `scripts/rebalance.py` after a month-end close or with a registry
  `authorize_off_cycle` event (`--off-cycle`). Do not run it from here.
- `weekly_review.py` writes `weekly_review_state`; a same-day re-run is
  idempotent, so refreshing the sell side twice in a day is safe.
- A fill the user describes in prose ("sold 10 GDS in brokerage") maps to
  `trade sell GDS 10 --account "BROKERAGE STOCK"`; account names are the
  `#` headers in `knowledge/POSITIONS.md`. Ask only when the account is
  ambiguous.
- If `refresh` reports a held name stale, the build has already failed
  closed; report the symbols and stop rather than running `buys` on a
  holed warehouse.
