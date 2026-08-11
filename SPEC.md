# SPEC - architecture and first principles

What this system **is**, what it deliberately **is not**, and the invariants that
must hold. README says how to use it; `tasks/plan.md` holds live gotchas and
active work. This file is always true to now - completed history does not
accumulate here.

---

## 1. Purpose

Rank a personal equity watchlist daily, evidence the ranking, and record what was
recommended - so that the recommendations can later be judged against what
actually happened.

The product is **an auditable opinion, not a prediction service**. Its value
comes from being reconstructable: for any past date, it must be possible to
answer "what did this system say, on what evidence, and was it right?"

## 2. Non-goals

- **Not an execution system.** It never places orders or touches a broker.
- **Not intraday.** Daily close granularity. Nothing here is valid for timing.
- **Not a research platform for arbitrary universes.** The universe is one
  person's watchlist, which bounds what any score can mean (see 5.3).
- **Not multi-user.** One operator, one workspace, no tenancy or access model.
- **Not advice.** It ranks; the human decides.

## 3. First principles

These are ordered. When two conflict, the earlier wins.

### P1. Evidence is immutable; conclusions are versioned

Bronze records what a source said and when it said it, append-only, never
overwritten. Every conclusion drawn from it - score, rank, signal, alert -
must be **snapshotted at the time it was drawn** and never rewritten.

A conclusion that can silently change is not evidence of anything.

As of 2026-08-10 this holds partially. `gold.recommendations` is append-only and
first-write-wins, so recommendations are now evidence. Every *other* gold table
is still rebuilt with `CREATE OR REPLACE` on each run, and on Databricks
`auto_adjust=True` still rescales the close history on every dividend. Locally
that second problem is solved: bronze stores raw prices and the adjustment is
derived on demand.

### P2. Point-in-time or not at all

Any value used to compute a score for date D must have been knowable on date D.
Enforced by as-of joins against SCD2 dimensions
(`effective_from <= D < effective_to`), never by joining a current snapshot.

Corollary: current-snapshot columns may be carried for **display** but must
never reach a scored feature. Mixing the two is the single easiest way to
manufacture a backtest that cannot be traded.

### P3. Membership is point-in-time too

The universe on date D is the set of symbols active on date D - not today's
survivors. Symbols are **retired, never deleted**. A universe that only contains
what survived overstates every historical result computed on it.

### P4. Absence is a failure, not a silence

A symbol that returns no data must fail loudly. The default behaviour of this
pipeline - a dead ticker contributes no rows and the job stays green - is
unacceptable for anything a decision rests on. Freshness and completeness are
asserted, not assumed.

### P5. Separate what is measured from what is judged

Four distinct layers, each independently inspectable:

| Layer | Answers | Changes when |
|---|---|---|
| Feature | What was true? | Data arrives |
| Score | How do these rank? | Methodology changes |
| Decision | What should be done? | Policy changes |
| Outcome | What happened? | Reality happens |

Collapsing these is why a scoring tweak currently rewrites history. Feature
changes must not silently restate past decisions.

### P6. Reproducible over convenient

Any output must be regenerable from bronze plus a versioned methodology
identifier. Where an artifact is disposable, its generator is the durable thing
and lives in the repo.

---

## 4. Data sources

Each source carries a trust level. Trust governs whether a source may influence
a decision, only an alert, or only a display.

| Source | Provides | Latency | Trust | May influence |
|---|---|---|---|---|
| yfinance prices | OHLCV daily, 2010+ | ~EOD | Medium - unofficial, shape changes between releases | Decisions |
| yfinance fundamentals | PE, EPS, margins, ROE, debt | Irregular, revised | Low-medium - revised without notice; history only from first ingest | Decisions, with PIT discipline |
| SEC EDGAR CompanyFacts | As-filed annual fundamentals, 2009+, keyed by `filed` date | Filing-driven | High - official, PIT by construction | Candidate signals (E/P, GP/A, ROE), zero weight until promoted |
| FRED | Macro series | Daily/monthly | High - official | Context, regime |
| Fama-French | Factor returns | Monthly, lagged | High - academic | Attribution only |
| Congressional trades | Disclosed transactions | **45-day statutory lag**, dollar ranges not amounts, Senate only | Low | Display only - never decisions |
| Splits | Corporate actions | EOD | Medium | Corrections |
| `ticker_migrations.json` | Retirements and successors | Manual | High - human verified | Universe membership |

**Single-source risk.** Prices still come only from yfinance, an unofficial
API that breaks regularly, with no cross-check. Fundamentals now have an
official source: EDGAR CompanyFacts feeds the candidate tier locally, while
the scored composite's P/E remains yfinance until a candidate earns
promotion. A silent partial ingest remains the most likely path to a bad
decision, which is what P4 exists to catch.

**The adjusted-close problem.** `auto_adjust=True` means `close` is
dividend-and-split adjusted, so the entire history rescales whenever a dividend
is paid. Values in bronze are therefore not stable over time, which violates P1
at the source. Either raw close plus explicit adjustment factors must be stored,
or every derived conclusion must be snapshotted (P1) so the drift is bounded.

---

## 5. Data model

### 5.1 Layers

**Bronze - evidence.** Append-only. Every row carries `_run_id`, `_ingest_ts`,
`_source_system`, `_source_event_ts`, `_load_type`. Never corrected in place; a
correction is a new row with a later `_ingest_ts`.

**Silver - validated.** Deduped, typed, conformed. SCD2 where attributes change
over time (fundamentals, keyed by `attr_hash` so unchanged replays do not create
phantom versions). Silver is where point-in-time resolution happens.

**Gold - conclusions.** Kimball star schema for analysis, plus serving tables for
consumption. Facts declare a grain and state it explicitly.

### 5.2 Grain declarations

Every fact table must state its grain. Ambiguous grain is the defect that makes
a warehouse silently wrong.

| Table | Grain |
|---|---|
| `fact_market_price_daily` | one row per symbol per trading day |
| `fact_fundamental_snapshot` | one row per symbol per fundamentals version |
| `fact_signal_snapshot` | one row per symbol per as-of date |
| `dim_security` | one row per symbol per attribute version (SCD2) |
| `dim_date` | one row per calendar date |

### 5.3 What a cross-sectional rank means here

Scores are `PERCENT_RANK` within `as_of_date` across the watchlist. The universe
is self-selected, so a rank is **"best of what was already liked"** and carries
no claim about the market. Any presentation implying otherwise is wrong.

### 5.4 Required additions

Status as of 2026-08-11. Listed here because their absence blocks the stated
goal; build status is tracked in the phase specs, not asserted here long-term.

- **`gold.recommendations`** - shipped 2026-08-10: append-only, one row per
  symbol per run, first-write-wins per (`as_of_date`, `methodology_version`).
  Never rewritten. The record everything downstream depends on (P1, P5). The
  local methodology-v2 ranking does not yet have its append-only counterpart;
  that lands with the buy/sell calls (plan task 11).
- **`gold.universe_membership`** - symbol, `added_date`, `removed_date`,
  `removal_reason`, successor symbol. Sourced from `ticker_migrations.json` (P3).
- **`gold.data_quality`** - per run per source: expected vs received symbol
  counts, max date vs last trading day, null rates on scored features. Failing
  it fails the job (P4).

---

## 6. Scoring

### 6.1 Invariants

- Every scored input is point-in-time (P2).
- Ranking is cross-sectional within `as_of_date`.
- Component direction is **explicit and tested**: for each component, a fixture
  asserts that the intended-best input produces the highest contribution.
- The composite carries a `methodology_version`; changing weights or inputs
  increments it and never restates prior snapshots.

The direction invariant exists because the implementation shipped with all four
components inverted, fixed 2026-08-10. `PERCENT_RANK() OVER (ORDER BY x DESC)`
assigns 0.0 to the largest `x`, so the best 30-day performer received the lowest
momentum contribution while a higher composite ranked better. Every test passed
throughout. Untested direction is not a detail; it silently inverts the product.

### 6.2 Signals are tiered, and evidence decides the tier

Every signal is `scored` (in the composite), `candidate` (computed and evaluated
every run, weight zero) or `monitored` (stored only). Tier is **data, not code**,
so promotion and demotion are recorded, dated and reversible.

A signal removed from the code stops accumulating evidence, so the decision to
drop it can never be revisited on data. A `candidate` costs one column and buys
that option back.

Promotion requires walk-forward evidence at `t > 3.0` - the higher bar being the
multiple-testing correction - plus a prospectively logged trial count, plus
correlation with existing scored signals below a stated threshold. A signal that
duplicates one already in the composite adds false confidence, not information.

### 6.3 Validation holds out time, never symbols

Stocks co-move, so a held-out *set of symbols* largely measures the market. The
information is in held-out *periods*: score as of a past date using only what was
knowable then, measure realized excess return, roll forward, report per fold.

Full-sample cross-sectional IC - what the common tooling reports - is **not**
validation. Using the whole history to evaluate every date is the same error
class as a look-ahead backtest.

Trial counts are recorded before results are seen. Trials not recorded are trials
that cannot be counted, and without the count the best of N variants cannot be
distinguished from the luckiest of N. This is unretrofittable by construction.

Full treatment: `tasks/SPEC-SIGNAL-TIERS.md`.

### 6.4 Component design

A composite of near-duplicate inputs is one input with false confidence.
Components must be conceptually distinct and checked for correlation.

| Component | Sound basis | Not |
|---|---|---|
| Momentum | 12-month return excluding the most recent month | 30-day return, which mean-reverts |
| Value | Earnings yield (E/P), which handles negative earnings | Raw P/E, undefined and unorderable when negative |
| Quality | Fundamentals already ingested - ROE, margins, debt/equity | MFI, a volume-weighted momentum oscillator |
| Risk | Trailing realized volatility or beta, ascending | RSI, a timing oscillator |

RSI, MFI, MACD and Bollinger remain valuable as **timing overlays in the alert
layer**, where they answer "is now a bad moment to act on an existing view" -
not as composite components.

---

## 7. Decision layer boundaries

Not yet built. Constraints that must hold when it is.

- **Execution assumption.** Data lands ~22:00 ET; the earliest possible action is
  the next open. Every evaluation must fill at the next open with an explicit
  cost and slippage haircut. Same-close fills are fiction.
- **Portfolio configuration is private.** Capital, positions and cost basis live
  in gitignored config, exactly as the watchlist does. The repo stays public and
  parameterized; no real figures are committed.
- **Rules are written before they are used.** Position sizing, maximum position,
  sector caps and exit criteria exist in the repo as code, not as judgement in
  the moment.
- **Validation is forward, primarily.** Historical backtest is limited to
  price-derived components with a survivorship asterisk, since fundamentals
  history begins at first ingest. A forward paper-traded track with
  benchmark-relative attribution is the honest evidence.

---

## 8. Operational constraints

Databricks Free Edition, serverless only, daily quota. `environment_version "3"`
(Python 3.12.3) pins numpy below 2 via pyarrow 15. Job dependencies are pinned
to versions the base image tolerates. Specifics that break builds:

1. `__file__` undefined - `spark_python_task` runs via `exec()`. Use
   `try/except` + `os.getcwd()` fallback.
2. `spark.createDataFrame(pandas_df)` fails (PySpark Connect ChunkedArray
   bug). Build Row-based with string dates + SQL `CAST`.
3. DBFS disabled - no temp writes to `/tmp`. Row-based approach only.
4. `environment_version` must be `"3"` (not `"3.1"`). Valid: 1-5.
5. Serverless only, daily quota limits. No classic compute.
6. Declare deps in `environments.spec.dependencies` in `databricks.yml`.
7. yfinance >= 0.2.51 returns multi-level columns even for one ticker -
   `droplevel("Ticker")`.
8. No `Adj Close` - `auto_adjust=True` is the default; `close` IS the
   adjusted close.

**Known scaling limit.** `build_silver_signals.py` collects all silver prices to
the driver and builds rows via `iterrows` - roughly 1.3M rows at 318 symbols
over 14 years. This is a driver-memory ceiling that tightens with every added
column, and it is the first thing that will break as the universe grows.
