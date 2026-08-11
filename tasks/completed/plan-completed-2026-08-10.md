# Completed - 2026-08-10

## P0 - the score was inverted

`composite_score` shipped with **all four components reversed**, and every test
passed throughout. `PERCENT_RANK() OVER (ORDER BY x DESC)` assigns 0.0 to the
largest `x`, so with `composite_rank` ordering by the summed percentiles DESC,
the pipeline surfaced the worst 30-day performer, the most expensive P/E, the
most overbought RSI and the weakest money flow as its "top 10 stocks".

Proven rather than argued: running the old expressions against the new tests,
the objectively best stock - highest return, cheapest P/E, least overbought,
strongest flow - scored **0.0 on all four components**.

Two null defects surfaced only once direction was right:
- `COALESCE(pe_ratio, 999) ASC` gave a **missing** P/E the top value score, so
  any symbol without fundamentals ranked as the cheapest in the universe.
- Flipping direction would have promoted **negative** P/E, since a loss-making
  company is numerically the cheapest. Non-positive P/E now folds into the
  missing sentinel.

Directions live once in `src/scoring/components.py` with the contract written
down; both call sites render from it, because a fix landing in one and missing
the other is how this survived. `tests/test_scoring.py` pins each direction plus
a null fixture.

**Two advisors reasoned backwards about the P/E direction**, in opposite ways,
and both were wrong. Each claim was settled by executing the SQL rather than by
argument. That is the durable lesson: for ranking direction, run it.

Commits `045296b`, `5698601`, `a8ef0f3`.

## P0 - immutable recommendations and a freshness gate

`gold.recommendations` is append-only, first-write-wins per (`as_of_date`,
`methodology_version`). Nothing else recorded what the system said on a past
date, so backtesting and judging past calls were impossible in principle rather
than merely unimplemented.

A day recorded partially stays partial - topping it up would attach one run's
evidence to another's - which is exactly why the freshness gate runs **before**
the snapshot. Fail first, record second.

The gate is tiered on purpose: hard-fail any held or currently-recommended
symbol, fail above 2% stale across the rest, warn and quarantine below, and
always name the stale symbols. An absolute gate across ~300 symbols against an
unofficial API would go red on routine flakiness, and a gate that cries wolf
gets disabled.

**Live behaviour change:** the nightly job can now fail on a bad yfinance night.
`STALE_TOLERANCE` and `RECOMMENDED_DEPTH` are the dials.

## Watchlist merged to 324

Eight loose lists merged with the main list: union 330, final 324 after
de-duping and resolving retirements. Six genuinely new (ACM, APH, DSTL, GRID,
TGT, TT), all verified against yfinance.

Six retired tickers were hiding in the loose lists and would have silently
re-entered. Five were caught automatically by `ticker_migrations.json` and
collapsed into successors already held. `BRKB` was **not** - it is a formatting
error rather than a corporate action, so the merge would have left both `BRK-B`
and `BRKB`. Added under the same precedent as `TGTG`, so the migrations file now
self-corrects symbol-format errors too.

## Local-first warehouse

Databricks stays the production target; this is about iteration speed. A run
there costs ~18 minutes and quota, so scoring variants never actually got
tested. Locally the same data scans in **0.557s**.

- **Backfill** (`scripts/backfill.py`): 1,190,363 rows, 324 symbols,
  2010-01-04 to 2026-08-10. Raw OHLCV plus 14,122 dividend and 178 split events,
  zero null closes.
- **Raw, deliberately.** `auto_adjust=True` rescales the whole close history on
  every dividend, so an adjusted series cannot be evidence. Verified: dividend
  payers show `close != adj_close`, non-payers match exactly.
- **Adjustments** (`src/common/adjustments.py`): derived on demand, reconciled
  against yfinance's own `adj_close` within 0.5% for ten dividend payers across
  sixteen years including AAPL's 2020 4-for-1 split.
- **Local silver and gold** (`scripts/build_local.py`): 1,125,563 signal rows,
  324 symbols, in ~2 minutes.

**Indicators needed no porting.** `common.indicators.build_signal_series` is pure
pandas, so the local build calls the identical function the Spark job calls -
parity by construction rather than translation. Only the SQL layer needed a
parity test.

`tests/test_engine_parity.py` executes the same SQL on both engines and earned
itself immediately: `CURRENT_TIMESTAMP()` is a function in Spark and a bare
keyword in DuckDB, so the snapshot SQL would not build locally at all. Fixed to
the common subset rather than an engine branch, because a branch is two
implementations again.

## Specs written

- `SPEC.md` - architectural spec the repo lacked. Six ordered first principles;
  the recurring one is *evidence is immutable, conclusions are versioned*, which
  nearly every defect found today violates in a different place.
- `tasks/SPEC-RECOMMENDATION-ENGINE.md` - the programme. Objective settled as
  benchmark-relative (below the index is failure), 3-12 month horizon, index
  universe with the watchlist as an overlay to break the feedback loop.
- `tasks/SPEC-LOCAL-WAREHOUSE.md` - the current phase.

An adversarial validation pass reordered the programme: the paper track moved
from P6 to P3 because **calendar time is the scarcest resource** - a 3-12 month
horizon needs a year-plus of forward evidence, so every month it is delayed is a
month of the only evidence that will be trusted. Source migration and XBRL
normalization came off the critical path as attrition traps.

The go-live gate became **process-based, not performance-based**. An IR
threshold at small N is statistically meaningless in both directions: it
approves a lucky bad strategy and kills an unlucky good one.

## Standing limitation

The backtest harness cannot set a go-live threshold. Survivorship (the universe
is today's survivors) inflates returns by more than the plausible edge, and
yfinance will not serve delisted price history at any price. Relative comparison
between variants survives this; absolute levels do not. Fundamentals history is
a *sequencing* constraint rather than a wall - EDGAR CompanyFacts is as-filed
back to the 2009-2011 XBRL phase-in and is P4 of the parent spec.
