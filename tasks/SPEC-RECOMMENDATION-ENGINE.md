# Spec: Recommendation Engine

In-flight feature spec. On completion its durable essence folds into the root
`SPEC.md` and this file moves to `tasks/completed/`.

Status: **awaiting approval**. No implementation until this is signed off.

---

## Objective

Turn the current watchlist ranker into a recommendation engine whose output is
trustworthy enough to act on with real money.

**Definition of winning:** the recommended portfolio beats its benchmark on a
risk-adjusted basis, after costs. Returning less than the index is a failure
even if the account grew.

**Definition of trustworthy:** for any past date, the system can state what it
recommended, on what evidence, and whether it was right - without that record
having been rewritten since.

The engine chooses **what** to hold, never **whether** to be invested. Cash
timing and regime calls are out of scope until stock selection is proven.

### Settled parameters

| Parameter | Decision |
|---|---|
| Objective | Benchmark-relative, risk-adjusted. Below index = failure |
| Horizon | 3-12 months. Rebalance monthly or on signal change |
| Universe | Index universe ranked; watchlist is an overlay/tag, not the universe |
| Direction | Long only (revisit later) |
| Instruments | NYSE/Nasdaq-listed equities and ETFs (IBKR opens foreign later) |
| Execution | Manual. Fills modelled at next open with cost and slippage |
| Validation | Paper track before real capital, capped allocation on go-live |

**Why the universe changed.** Ranking only the watchlist means a rank says
"best of what I already liked" and can never flag that the whole watchlist is
wrong. Ranking an index universe with the watchlist tagged on top puts holdings
in context and breaks that feedback loop.

## Non-goals

- Order execution or broker integration.
- Intraday anything.
- Shorting, options, leverage, derivatives.
- Cash/regime timing (explicitly deferred - hardest part, most common failure).
- Multi-user, tenancy, or access control.
- Replacing the Databricks project. It stays, in a changed role.

## Architecture

**One implementation. Local computes, Databricks serves.**

```
LOCAL (DuckDB)                          DATABRICKS
  bronze   raw, append-only               gold tables (loaded, not computed)
  silver   validated, PIT, SCD2     -->   dashboards
  gold     facts, dims, scores            SQL exploration
  backtest + research                     the portfolio showcase
```

The Databricks job stops recomputing transforms and becomes a **load-and-serve**
job. The bundle, CI/CD, star schema and dashboards all survive; the 19-task
transform DAG collapses to an ingest of finished tables.

**Why:** two engines running the same transforms will drift - the exact bug
class fixed today with the two watchlist scripts. Only one may own the logic.
Local ownership also removes the Free Edition quota from the critical path,
kills the driver-memory `iterrows` ceiling, makes backtesting affordable, and
ends the `auto_adjust` mutability problem by storing raw prices plus explicit
adjustment factors.

## Data sources

**Every source currently ingested is retained.** None are dropped; several change
role. "Scored" means it may influence a recommendation. "Context" and "display"
mean it may inform a human but never moves a rank.

| Source | Today | Becomes | Why |
|---|---|---|---|
| yfinance prices | Scored | **Scored, promoted** - stores raw close + adjustment factors | Owning raw prices ends the `auto_adjust` history rewrite |
| yfinance fundamentals | Scored (PE only) | **Fallback** once EDGAR lands (P3); scored until then | No as-of history; EDGAR is PIT by construction |
| Splits | Corrections | **Promoted - load-bearing** | Computing our own adjustment factors from raw prices requires accurate corporate actions |
| FRED macro | Ingested, unused in scoring | **Context** | Regime/timing is out of scope, so it informs rather than ranks |
| Fama-French | Ingested, unused in scoring | **Attribution** | Explains realized returns; not a predictor |
| Congressional trades | Ingested, unused in scoring | **Display only** - see open question 6 | 45-day statutory lag, dollar ranges not amounts, Senate only |
| SEC EDGAR CompanyFacts | Not ingested | **New - primary fundamentals** | Free, as-filed, includes delisted filers |
| Sharadar SF1 | Not ingested | **Optional upgrade** | Closes the delisted-price gap |

**Known gap:** delisted price history. yfinance does not serve dead tickers, so
a fully survivorship-free backtest needs a paid source. Until then, survivorship
is a stated caveat on every historical result, not a solved problem.

**Rejected:** EODHD and FMP - priced attractively but without explicit
point-in-time semantics, so they do not fix the defect that motivates buying
data. Firecrawl/scraping - fragile and licence-hostile for a system meant to be
trustworthy.

## Phases

Ordered by dependency. Each phase gates the next.

### P0 - Stop the bleeding — **DONE** 2026-08-10
Fix the inverted score; make recommendations immutable.
- [x] All four `PERCENT_RANK` directions corrected, defined once in
  `src/scoring/components.py`, with a direction test and a null fixture each.
  Verified the old expressions fail every one of those tests.
- [x] `gold.recommendations` — append-only, first-write-wins per
  (`as_of_date`, `methodology_version`). `src/scoring/snapshot.py`.
- [x] Tiered freshness gate, running **before** the snapshot so a stale day is
  never recorded immutably. `common.quality.check_freshness`.

Delivered as `045296b`, `5698601`, `a8ef0f3`. 212 tests pass.

**Consequences to watch.** A partially-recorded day stays partial by design -
topping it up would attach one run's evidence to another's - which is why the
gate runs first. And the gate can now fail the nightly job on a bad yfinance
night; that is the intent, but it is a live behaviour change.

### P1 - Local warehouse — **DONE** 2026-08-10
Delivered as L1 and L2 of `tasks/SPEC-LOCAL-WAREHOUSE.md`: raw-price backfill
(1.19M rows, 324 symbols, 2010-2026) and a local silver/gold build running the
same indicator function and the same ranking SQL, in ~2 minutes.

- DuckDB warehouse; bronze stores **raw close plus adjustment factors**.
- Backfill prices 2010+ for the index universe.
- **Port only the scored path: prices and splits.** They keep their bronze
  contract (`_run_id`, `_ingest_ts`, `_source_system`, `_source_event_ts`,
  `_load_type`) and reconcile against Databricks on row count and date range
  before use.
- **FRED, Fama-French, congressional and yfinance fundamentals keep running on
  Databricks, untouched.** Nothing is dropped. They never touch a rank, so they
  have no local counterpart to drift from, and gating the engine on migrating
  display data is how this project dies of attrition. They move later, or never.
- No cutover. The local warehouse is built as a sibling; the existing pipeline
  keeps producing dashboards throughout. "Load-and-serve" happens incrementally
  per table, so nothing is bet on a big-bang migration.

### P2 - Rule-based universe
- `universe_membership` (symbol, added, removed, reason, successor), seeded from
  `ticker_migrations.json`. Retire symbols, never delete.
- Universe = **top-N US listings by trailing dollar volume**, recomputed monthly
  from our own price data. Not official index membership: that is licensed, and
  the free path is scraping constituent-change histories, which this spec
  rejects elsewhere. A computed rule is PIT by construction, licence-free and
  self-maintaining. Benchmark remains SPY; the universe need not equal the
  benchmark's membership.
- **ETFs are excluded from ranking** - E/P, ROE and margins are undefined for
  them. They remain as watchlist overlay and benchmark only.

### P3 - Price-only score (methodology v2) - PAPER TRACK STARTS HERE
The critical path. Everything before this exists to make this honest; everything
after it improves a system that is already accumulating evidence.

- Two components, both price-derived, both PIT-clean today:
  momentum (12-month return excluding the most recent month) and
  low risk (trailing realized volatility, ascending).
- Direction test **and null fixture** per component.
- Position sizing and exit rules as code - a paper portfolio without sizing
  rules is not a portfolio.
- Forward paper track begins, benchmark-relative attribution, daily.

### P4 - Fundamentals from EDGAR (methodology v3)
Behind the same interface, while the paper clock already runs.
- Ingest `companyfacts.zip`; normalize XBRL tags to a concept model.
- SCD2 keyed on filing date - PIT by construction.
- Adds value (earnings yield, handles negative earnings) and quality
  (ROE, margins, debt/equity). Missing fundamentals score **neutral, not worst**.
- Components checked for pairwise correlation before weighting.
- Rank within sector, or document the sector tilt as deliberate: value and
  quality ranked across a full universe become structural sector bets.
- RSI/MFI/MACD/Bollinger move to the alert layer as timing overlays only.

### P5 - Backtest as mechanics check
**Not a threshold-setter.** It runs on a survivor-only universe, so its return
levels are biased upward by more than the plausible edge. It validates the
machinery, not the strategy.
- Known-answer test: harness run on the benchmark must yield ~zero excess.
- Look-ahead test: shifting features forward one day must degrade performance.
- Decile monotonicity, turnover, cost sensitivity.
- Go-live gates come from process and the paper track, not from backtest IR.

### P6 - Real holdings and comparison
- Real positions loaded from private config, compared against what the engine
  would have recommended.

### P7 - Dashboards
Last, and cuttable. Recommendations and their evidence, replacing the six
identical bar charts.

## Commands

```bash
uv sync
uv run pytest tests/ -q
uv run python scripts/watchlist.py seed
uv run python scripts/watchlist.py check
databricks bundle validate
databricks bundle deploy
```
New commands land here as phases add them.

## Project structure

```
src/ingestion/     source -> bronze
src/transforms/    bronze -> silver -> gold
src/scoring/       NEW - components, weighting, versioning
src/backtest/      NEW - harness, metrics, cost model
src/common/        config, quality, run context
scripts/           operational entry points
tests/             pytest
warehouse/         NEW - local DuckDB, gitignored
```

## Code style

Match the existing repo. Transforms are SQL-first with Python orchestration;
every fact table declares its grain in a module docstring.

```python
def momentum_percentile(df):
    """12m-1m return, cross-sectionally ranked within as_of_date.

    Direction: highest return -> highest percentile. Asserted by
    tests/test_scoring.py::test_momentum_direction, because
    PERCENT_RANK(... DESC) assigns 0.0 to the largest value and
    silently inverts the product.
    """
```

## Testing strategy

pytest, in `tests/`. Three levels:

1. **Direction tests** - one per score component, asserting the intended-best
   input yields the highest contribution. Non-negotiable; their absence caused
   the current inversion.
2. **PIT tests** - assert no scored feature uses data unavailable at `as_of_date`.
   Fixture: a fundamentals revision filed after the scoring date must not appear.
3. **Property tests** - percentiles in [0,1]; append-only tables never shrink;
   grain uniqueness holds on every fact table.

## Boundaries

**Always:** point-in-time joins for scored features; append-only for
recommendations; direction test with every component; state the survivorship
caveat on historical results; keep portfolio figures gitignored.

**Ask first:** paying for data; changing `methodology_version`; changing the
benchmark; retiring the Databricks transform DAG; any real-money allocation.

**Never:** rewrite a past recommendation; use current-snapshot fundamentals in a
scored feature; delete a delisted symbol; let congressional data touch a
decision; commit real portfolio figures.

## Success criteria

| # | Criterion | Verified by |
|---|---|---|
| 1 | No score component is inverted | Direction test per component |
| 2 | A recommendation is immutable per (as_of_date, methodology_version) | Snapshot diff across runs |
| 3 | A stale or missing symbol fails the run | Freshness gate test with a dead ticker |
| 4 | No scored feature uses post-as-of data | PIT test with a late-filed revision |
| 5 | Delisted symbols remain in history | `universe_membership` retains removed rows |
| 6 | Backtest fills at next open with costs | Cost model test |
| 7 | Paper track reports excess vs benchmark daily | Attribution output exists |
| 8 | Backtest harness on the benchmark yields ~zero excess | Known-answer test |
| 9 | Shifting features forward one day degrades performance | Look-ahead test |
| 10 | A data outage produces an honest gap, never fabricated continuity | Incident test |
| 11 | Go-live gates met before real capital | Process gate, below |

**Go-live gate is process-based, not performance-based.** An "information ratio
above X over N months" threshold is statistically meaningless at small N in both
directions: it approves a lucky bad strategy and kills an unlucky good one. The
gate is instead: M months of immutable, PIT-clean, incident-free paper
recommendations that behave as expected, a capped initial allocation, and a
**pre-committed evaluation window and abandonment rule written down before the
first real trade**. That pre-commitment is a deliverable. It is the only guard
against both abandoning a sound strategy in a normal drawdown and letting a
broken one drift on unfalsifiably.

## Open questions

**Resolved by the validation pass:**

- **Rebalance trigger** - fixed monthly with hysteresis bands (enter on top
  decile, exit only below median). Rank-change triggers are a turnover bomb,
  because cross-sectional percentiles jiggle daily.
- **Sharadar** - defer. It matters only when backtest *levels* matter, and the
  backtest is now a mechanics check.
- **Congressional trades** - stay display-only, experiment deferred
  indefinitely. Lowest-trust source, worst return per evening spent.
- **Universe** - rule-based top-N by dollar volume, not licensed index
  membership.

**Still open:**

1. **Benchmark and N.** SPY assumed. What N for the universe - 500, 1000?
2. **Real holdings format.** Spreadsheet, broker export, or IBKR API? Shapes P6.
3. **Sector neutrality.** Rank value and quality within sector, or accept the
   structural sector tilt deliberately? Decide before P4, not during.
4. **Evaluation window and abandonment rule.** Must be written before the first
   real trade. What M, what allocation cap, what would make you stop?
