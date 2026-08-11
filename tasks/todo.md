# TODO

Session-to-session handoff snapshot. Resolved threads leave; survivors compress
to state + next action + pointer. Full records: `tasks/completed/`.

## Start here - next session

**Goal: turn the POC into a real recommendation engine, local-first.**

Read in this order: `tasks/SPEC-LOCAL-WAREHOUSE.md` (what and why),
`tasks/plan.md` (the ordered tasks), `SPEC.md` (invariants that must hold).

### The goal

**Every symbol on the list gets a buy or sell call at pipeline runtime, and the
call is trustworthy.**

A **buy** means: this will most likely outperform the benchmark over roughly the
next 6 months. That is the definition to build toward - it is deliberately a
*relative* claim, matching the settled objective in
`tasks/SPEC-RECOMMENDATION-ENGINE.md` (below the index is failure).

How that probability is actually calculated, and whether 6 months is the right
window, are **open and decided next session**. Do not assume the current
four-component composite is the answer; it is a placeholder whose components are
three correlated oscillators plus a broken value metric (parent spec P4).

### Sequence

L1 and L2 are done - the local pipeline runs bronze -> silver -> gold, 1.13M
signal rows, ~2 minutes.

**START WITH task 4 - forward returns.** Nothing can be called reliable until
there is a way to measure whether past calls were right. Build `src/backtest/`
computing returns at 21/63/252 sessions from `warehouse/market.duckdb`, filled
at the **next open** (data lands after the close, so a same-close fill is
fiction). Verify by hand-computing one symbol over one window to 6dp. Then 5
(costs), 6 (IC and deciles), 7 (known-answer and look-ahead).

Only once 4-7 exist is there any basis for calling a signal reliable. Emitting
buy/sell labels before that is just renaming the current ranking.

```bash
uv run pytest tests/ -q                # 232 passing
uv run python scripts/build_local.py   # rebuild local silver + gold, ~2 min
```

If `warehouse/` is missing (gitignored, so it does not travel between devices):
`uv run python scripts/backfill.py` first - about 2 minutes for full history.

### Established 2026-08-10, spec at `tasks/SPEC-SIGNAL-TIERS.md`

- **Validation holds out TIME, not symbols.** Stocks co-move, so a held-out set
  of tickers mostly measures the market. Score as of a past date using only what
  was knowable then, measure realized excess, roll forward.
- **Signals are tiered, never deleted.** `scored` (in the composite),
  `candidate` (computed and evaluated, weight zero), `monitored` (stored only).
  A deleted signal stops accumulating evidence, so the decision to drop it can
  never be revisited on data.
- **The four current components are incumbents, not winners.** None earned its
  tier on evidence. 12-1 momentum, gross profitability and realized volatility
  enter as candidates with better priors.
- **Trial counts are logged before results are seen.** Cannot be retrofitted.
- `POSITIONS.md` is gitignored - held positions and priority live there.

### Needed from the user next session

- **The held-position subset.** A list of tickers currently held, to be tracked
  more closely than the rest. Put it in `_relay.md`.
- **A ruling on the forecast window** - 6 months, or something else.
- **Which broad universe(s) to keep in view** - one large-cap tier, or several.
  Line of sight (task 13) and promotion into tracking (task 14) are both wanted
  and are separate mechanisms.

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
