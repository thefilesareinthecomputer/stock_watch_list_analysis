# Spec: Local-First Warehouse and Evaluation Loop

In-flight feature spec. On completion its durable essence folds into the root
`SPEC.md` and this file moves to `tasks/completed/`.

Parent: `tasks/SPEC-RECOMMENDATION-ENGINE.md` (this is P1 and P3 of that plan).

---

## Objective

Make it possible to test whether a recommendation is any good **in seconds
rather than in 18-minute increments**, without weakening the guarantee that what
runs in production is what was measured locally.

Concretely: a local DuckDB warehouse owning raw prices and corporate actions, a
single SQL definition executed by both engines, and an evaluation loop that
scores the universe historically and reports whether the ranking predicted
anything.

**Why this is the bottleneck.** Every question worth asking about this system -
does the composite predict returns, does momentum beat value here, what weights,
what horizon - is answered by running a scoring variant over history and
measuring it. On Databricks that is ~18 minutes and quota per attempt, which
means in practice the questions do not get asked. Locally the same 1.1M rows
scan in under a second, so twenty variants cost less than one Databricks run.

### Success is

- A scoring change can be evaluated end to end, locally, in under a minute.
- Local results provably predict Databricks results.
- Nothing about the Databricks pipeline changes or breaks in the process.

## Non-goals

- **Not a migration.** Databricks remains the production target and keeps
  running untouched. There is no cutover and no big-bang.
- **Not porting everything.** FRED, Fama-French and congressional never touch a
  rank; they stay on Databricks. Only the scored path comes local.
- **Not a research platform.** One operator, one machine, one purpose.
- **Not live trading, sizing, or execution.** That is the parent spec's P3/P6.

## Design

### One SQL definition, two engines

Transforms stay a single SQL string. Both engines execute it; a parity test
asserts identical output. Where dialects differ, move to the **common subset**
rather than branching - a branch is two implementations, which is the drift this
exists to prevent.

Already proven: `scoring.components.percentile_sql` produces identical
percentiles and identical ordering on both engines, against a fixture with nulls
in every scored column, a negative P/E, ties, and a sentinel collision.

Already found: `CURRENT_TIMESTAMP()` is a function in Spark and a bare keyword
in DuckDB. Fixed to the bare form. Expect a handful more of these; each is a
small rewrite, not a redesign.

### Raw prices, owned

Bronze stores raw OHLCV plus dividends and split ratios. Not adjusted close.

`auto_adjust=True` rescales the entire close history on every dividend, so an
adjusted series is not stable over time - a score computed last month silently
changes this month, which makes it unusable as evidence. Storing raw prices plus
the actions that adjust them makes history immutable and lets the adjustment be
recomputed deterministically.

Verified on the smoke test: dividend payers show raw close diverging from
adjusted on most rows; non-payers match exactly.

### Layout

```
warehouse/market.duckdb     local warehouse (gitignored, regenerable)
scripts/backfill.py         yfinance -> bronze_prices
src/scoring/                shared SQL definitions (components, snapshot)
src/backtest/               NEW - evaluation harness
tests/test_engine_parity.py the anti-drift mechanism
```

### Evaluation loop

The deliverable that makes this worth building.

1. Apply a candidate scoring definition to history, per `as_of_date`.
2. Join forward returns at 21/63/252 sessions.
3. Report, per horizon: information coefficient, decile monotonicity, hit rate,
   turnover, and excess versus an equal-weight benchmark of the same universe.
4. Print a one-screen verdict so variants can be compared at a glance.

**Fills at next open with costs.** Data lands after the close; the earliest
possible action is the following open. Same-close fills are fiction and would
manufacture edge that cannot be traded.

## Honest limits

These are properties of the data, not of the implementation, and no amount of
engineering removes them. Every result the loop prints must carry them.

1. **Survivorship.** The universe is today's 324 symbols. Names that died are
   absent, so historical returns are overstated. `ticker_migrations.json` records
   nine retirements but yfinance will not serve delisted price history at all.
2. **Fundamentals have no history.** The SCD2 starts at first pipeline run, so
   `value_pct` is a constant for essentially all historical dates. Only the
   price-derived components are honestly testable.
3. **One source.** yfinance is unofficial and changes shape without notice.
   There is no cross-check.
4. **The universe is self-selected.** A rank means "best of what was already
   liked" until the rule-based universe of the parent spec lands.

Consequence: this loop **cannot** produce a go-live threshold. It is a mechanics
check and a variant comparator. Go-live evidence comes from the forward paper
track, per the parent spec.

## Phases

### L1 - Backfill — **DONE** 2026-08-10
- [x] `scripts/backfill.py` - raw OHLCV, dividends, splits, bronze contract,
      idempotent per (symbol, window).
- [x] Full history loaded: **1,190,363 rows, 324 symbols, 2010-01-04 to
      2026-08-10**, zero null closes, 14,122 dividend and 178 split events.
      Thinnest coverage is recent IPOs (CRWV 2025, RDDT 2024), which is correct.
- [x] Raw-price guarantee verified: dividend payers show `close != adj_close`,
      non-payers match exactly.
- [ ] Row-count reconciliation against Databricks silver (deferred - silver
      applies its own filters, so this needs a like-for-like query first).

**Measured:** full scan of 1.19M rows takes **0.557s**, against ~18 minutes for
a Databricks run. That ratio is the entire justification for local-first.

### L2 - Local silver
- Adjustment factors computed from dividends and splits; adjusted series derived
  rather than stored.
- Indicators ported as shared SQL, parity-tested per indicator.
- Gate: every ported transform passes parity before use.

### L3 - Evaluation harness
- Forward returns at 21/63/252 sessions, next-open fills, cost and slippage.
- IC, decile monotonicity, hit rate, turnover, excess vs equal-weight.
- **Known-answer test:** the harness run on the benchmark must return ~zero
  excess. A harness that finds edge in SPY is broken.
- **Look-ahead test:** shifting features forward one day must degrade results.

### L4 - Variant comparison
- Candidate scoring definitions as data, not code edits, so variants are
  reproducible and comparable.
- Results recorded with the methodology that produced them.

### L5 - Promote
- A winning variant bumps `METHODOLOGY_VERSION` and deploys the same SQL to
  Databricks unchanged, which the parity test already guarantees is safe.

## Testing strategy

| Level | Asserts | Why |
|---|---|---|
| Parity | Same SQL, same fixture, same result on both engines | Local results must predict production, or the loop lies |
| Direction | Intended-best input earns the highest percentile | All four components once shipped inverted |
| Point-in-time | No scored feature uses post-`as_of_date` data | The failure mode that manufactures fake edge |
| Known-answer | Harness on the benchmark yields ~zero excess | Detects a harness that finds edge in noise |
| Property | Percentiles in [0,1], grain uniqueness, append-only never shrinks | Cheap structural guards |

## Boundaries

**Always:** parity-test a transform before relying on it locally; state the
survivorship and fundamentals caveats on every historical result; fill at next
open with costs; keep the warehouse gitignored.

**Ask first:** porting anything Databricks currently owns; changing
`METHODOLOGY_VERSION`; adding a data source; spending money on data.

**Never:** branch SQL per engine; use adjusted close as the stored evidence;
delete a delisted symbol; present a backtest number without its caveats;
let a local result reach production without passing parity.

## Success criteria

| # | Criterion | Verified by |
|---|---|---|
| 1 | Identical SQL yields identical results on both engines | `tests/test_engine_parity.py` |
| 2 | Bronze stores raw, unadjusted prices | Dividend payers show close != adj_close |
| 3 | Backfill is idempotent | Re-run leaves row count unchanged |
| 4 | Local row counts reconcile with Databricks | L1 reconciliation |
| 5 | A scoring variant evaluates end to end in under a minute | Timed run |
| 6 | Harness finds ~zero excess on the benchmark | Known-answer test |
| 7 | Shifting features forward degrades results | Look-ahead test |
| 8 | Every printed result carries its caveats | Harness output |

## Open questions

1. **Universe for evaluation** - the 324 watchlist now, or wait for the
   rule-based top-N? Watchlist is available today and biased; the rule-based
   universe is the parent spec's P2.
2. **Cost model** - flat basis points, or spread-aware? Flat is defensible at a
   3-12 month horizon and much simpler.
3. **How variants are expressed** - config entries, or small SQL fragments?
   Affects how L4 stores and compares them.
