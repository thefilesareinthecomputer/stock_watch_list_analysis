# TODO

Session-to-session handoff snapshot. Resolved threads leave; survivors compress
to state + next action + pointer. Full records: `tasks/completed/`.

## Start here - next session

**Goal: turn the POC into a real recommendation engine, local-first.**

Read in this order: `tasks/SPEC-LOCAL-WAREHOUSE.md` (what and why),
`tasks/plan.md` (the ordered tasks), `SPEC.md` (invariants that must hold).

L1 and L2 are done. The local pipeline runs end to end: bronze -> silver -> gold,
1.13M signal rows across 324 symbols, in ~2 minutes.

**NEXT ACTION: task 4 - forward returns.** Build `src/backtest/` computing
returns at 21/63/252 sessions from `warehouse/market.duckdb`, filled at the
**next open** (data lands after the close, so a same-close fill is fiction).
Verify by hand-computing one symbol over one window and matching to 6dp.
Then task 5 (costs), 6 (IC and deciles), 7 (known-answer and look-ahead).

```bash
uv run pytest tests/ -q            # 232 passing
uv run python scripts/build_local.py   # rebuild local silver + gold, ~2 min
```

If `warehouse/` is missing (gitignored, so it does not travel between devices):
`uv run python scripts/backfill.py` first - about 2 minutes for full history.

## State as of 2026-08-10

`develop` and `main` aligned, CI green. Watchlist is 324 symbols, all resolving.

Shipped today: score inversion fixed (all four components were reversed), an
append-only `gold.recommendations` snapshot, a tiered freshness gate, the
engine-parity harness, and the local backfill.

## Open

1. **Nothing runs locally yet beyond the backfill.** DuckDB holds bronze prices
   and executes fixtures in tests. There is no local silver, no indicators, no
   evaluation. That is L2 and L3.

2. **Three decisions the work will reach.** Cost model (flat bps vs
   spread-aware) before task 6; how a variant is expressed (config vs SQL
   fragment) before task 8; sector-neutral ranking before the parent spec's P4.

3. **Backtest results cannot set a go-live threshold.** Survivorship (the
   universe is today's survivors) and absent fundamentals history make the
   levels biased. The harness is a mechanics check and variant comparator; real
   evidence comes from the forward paper track.

4. **`feature/upgrade-stock-pipeline` still on origin** at `bd0694f`, fully
   superseded. Delete or keep - needs a ruling.

5. **`setup-uv` pinned to exact `v9.0.0`**, so it will not pick up patches.

## Deferred, not blocking

- Sharadar (paid) is the only clean fix for delisted price history; deferred
  until backtest levels matter, which per item 3 is not yet.
- Congressional trades stay display-only. Lowest-trust source.
- FRED, Fama-French, congressional and yfinance fundamentals stay on Databricks
  and are deliberately not ported - they never touch a rank.
