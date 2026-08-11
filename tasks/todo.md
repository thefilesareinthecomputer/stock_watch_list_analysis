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

**Tasks 4-7 are DONE (2026-08-11): the evaluation harness exists.**
`src/backtest/` + `scripts/evaluate.py`; known-answer and look-ahead checks
pass against 16 years of history. See plan.md L3 for the first read on the
candidates (earnings yield promising, GP/A mixed, ROE weak, 30d momentum
worthless at its own horizon).

**Task 9b is DONE (2026-08-11): the trial log exists** (`backtest.trials`,
tracked `trial_log.jsonl`, wired into `evaluate.py`), and an end-to-end chain
test (`tests/test_e2e_local.py`) walks bronze -> signals -> gold composite ->
fundamentals -> candidates -> forward returns -> verdict -> trial log on
synthetic data. 287 tests.

**Tasks 8+9 are DONE (2026-08-11): variants as config entries** (decision:
not SQL fragments), compared from one command, results recorded with their
methodology hash, byte-reproducible, every run trial-logged (count: 7). See
plan.md L4 for the first comparison - the EDGAR candidates beat the
incumbent composite on every metric with 15x less turnover, survivorship
caveat standing.

**START WITH task 10b - the signal tier registry**, which promotion
decisions read from; then L5 (task 10, promote a winning variant to
Databricks behind a METHODOLOGY_VERSION bump).

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
- Held positions live in `knowledge/positions.md` (gitignored via
  `knowledge/`), sectioned by account - delivered 2026-08-10, keep the format.

### Resolved 2026-08-11

- **Forecast window: 126 sessions (~6 months), provisional.** Final choice
  comes from task 6's IC-by-horizon decay report. 126 added to the task 4
  horizons so the window is a measured one.
- **EDGAR CompanyFacts ingested end to end** - see plan.md shipped record and
  gotcha 0c (CIK fallback, XOM holdco gap, stale-shares guard).

### Needed from the user next session

- **Which broad universe(s) to keep in view** - one large-cap tier, or several.
  Line of sight (task 13) and promotion into tracking (task 14) are both wanted
  and are separate mechanisms.
- **A ruling on XOM's predecessor CIK** - merge the old operating company's
  filing history under the new holdco ticker, or accept neutral fundamentals
  until the holdco's first 10-K.

## State as of 2026-08-10

`develop` and `main` aligned, CI green. Watchlist is 324 symbols, all resolving.

Shipped today: score inversion fixed (all four components were reversed), an
append-only `gold.recommendations` snapshot, a tiered freshness gate, the
engine-parity harness, and the local backfill.

## Open

1. **No evaluation yet.** The local warehouse builds bronze through gold,
   including the EDGAR candidate tier, but nothing measures whether any
   ranking predicts returns. That is L3, and it gates everything downstream.

2. **Three decisions the work will reach.** Cost model (flat bps vs
   spread-aware) before task 6; how a variant is expressed (config vs SQL
   fragment) before task 8; sector-neutral ranking before the parent spec's P4.

3. **Backtest results cannot set a go-live threshold.** Survivorship (the
   universe is today's survivors) biases the levels; fundamentals history now
   reaches back to 2009 via EDGAR, which narrows but does not remove the
   caveat. The harness is a mechanics check and variant comparator; real
   evidence comes from the forward paper track.

4. **`feature/upgrade-stock-pipeline` still on origin** at `bd0694f`, fully
   superseded. Delete or keep - needs a ruling.

5. **`setup-uv` pinned to exact `v9.0.0`**, so it will not pick up patches.

## Deferred, not blocking

- Sharadar (paid) is the only clean fix for delisted price history; deferred
  until backtest levels matter, which per item 3 is not yet.
- Congressional trades stay display-only. Lowest-trust source.
- FRED, Fama-French, congressional and yfinance fundamentals stay on Databricks
  and are deliberately not ported - they never touch a rank. EDGAR fundamentals
  are the local, PIT-clean replacement path (candidate tier today).
