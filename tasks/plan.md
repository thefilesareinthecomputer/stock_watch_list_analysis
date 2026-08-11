# Plan / dev docs

Active plan and live gotchas only. Finished work lives in `tasks/completed/`.

## Active

**Recommendation engine rebuild.** Architecture `SPEC.md`; programme
`tasks/SPEC-RECOMMENDATION-ENGINE.md`; current phase
`tasks/SPEC-LOCAL-WAREHOUSE.md`.

Shipped 2026-08-10 - full record in `completed/plan-completed-2026-08-10.md`:
P0 (score inversion, immutable snapshot, freshness gate), watchlist merged to
324, engine parity harness, and L1+L2 of the local warehouse (1.19M raw price
rows and 1.13M signal rows, built locally in ~2 min against ~18 on Databricks).

Shipped 2026-08-11: EDGAR CompanyFacts end to end. `scripts/
backfill_fundamentals.py` -> `bronze_fundamentals` (268K as-filed facts, 263
symbols, 2009+); `common.fundamentals` -> `silver_fundamental_metrics` (PIT
knowledge series keyed on `filed`, restatement-aware, amendment-safe);
`scoring.candidates` -> `gold_candidate_signals` (E/P, gross profitability,
ROE at every (symbol, as_of_date), percentile-ranked over non-nulls, zero
composite weight). Wired into `build_local.py`. 260 tests.

### Ordered tasks

Dependencies: L3 -> L4 -> L5. Within L3, 6 and 7 are independent once 5 lands.

### Target state

Every symbol carries a **buy or sell call** at pipeline runtime, where buy means
"most likely to outperform the benchmark over ~6 months". Relative by
definition, matching the settled objective. The calculation and the window are
open - decide before task 11, not during it.

**Sequencing constraint:** tasks 4-7 gate task 11. A buy label emitted before
forward returns can be measured is the current ranking with a new name on it,
and the current ranking has never been shown to predict anything.

**L3 - evaluation harness** - **DONE 2026-08-11.** `src/backtest/`
(returns, costs, metrics, harness) + `scripts/evaluate.py`. All four verify
gates hold: hand-computed return matches to 6dp; zero-cost exceeds costed by
exactly the modelled bps; a synthetic perfect predictor scores IC 1.0 with
monotonic deciles; the benchmark yields exactly zero excess, a seeded random
signal finds no edge in 16 years, and a shifted leak collapses. Cost model
decision: flat 10 bps/side (spread-aware rejected at this horizon). Monthly
eval dates - daily would overlap the forecast windows and fake the t-stat.

First harness read (survivorship caveat applies, levels inflated): earnings
yield IC 0.033 at 126 sessions (t 3.9), stronger at 252 (t 5.6), turnover
0.08; gross profitability positive IC but negative top-decile excess (works
mid-rank, not as a decile-10 bet); ROE weak (t < 2.3, matches Novy-Marx);
incumbent 30d momentum scores nothing at its own horizon (t 0.5). No
promotion yet: trial logging (9b) and the correlation gate do not exist.

**L4 - variant comparison**

8. Scoring variants expressed as data, not code edits -> verify: two variants
   run from one command and produce different, reproducible results.
9. Results recorded with the methodology that produced them -> verify: re-running
   a recorded variant reproduces its output exactly.
9b. **DONE 2026-08-11.** Trial log ships as `backtest.trials` writing the
   tracked, append-only `trial_log.jsonl` at the repo root - deliberately not
   in the regenerable `warehouse/`. Logged BEFORE any result exists (the
   schema has no result field); `evaluate.py` logs every run, re-runs
   over-count on purpose (conservative), no opt-out flag. The four
   evaluations run before the log existed are retro-logged with an intent
   saying exactly that. Rationale stands: without the count the best of N is
   indistinguishable from the luckiest of N (Bailey & Lopez de Prado 2014);
   accept a factor at t > 3.0, not 2.0 (Harvey, Liu & Zhu 2016).

**L5 - promote**

10. Winning variant to Databricks -> verify: parity test green, `bundle
    validate` OK, CI deploy green, `METHODOLOGY_VERSION` bumped so past
    snapshots are not restated.

**L6 - buy/sell calls, tiering, and discovery**
Specced in `tasks/SPEC-SIGNAL-TIERS.md`.

10b. Signal tier registry - every signal is `scored`, `candidate` or
    `monitored`, held as data so promotion is a recorded event -> verify: a
    `candidate` provably contributes zero weight; promotion requires
    walk-forward IC at t > 3.0 with the trial count logged; demotion is
    automatic when a scored signal's IC turns insignificant.
    Initial: the four incumbents `scored` (they are incumbents, not winners);
    12-1 momentum, gross profitability, realized volatility, beta and earnings
    yield `candidate`; RSI/MFI/MACD/Bollinger/OBV/ATR `monitored`.

11. Emit a buy/sell call per symbol, defined as "outperforms the benchmark over
    the forecast window" -> verify: on held-out history the buy set beats the
    sell set on forward excess return, and the harness's known-answer test still
    returns ~zero on the benchmark. **Gated by tasks 4-7.**
12. Priority tier for held positions - a subset of the watchlist tracked more
    closely, sourced from private config like the watchlist itself -> verify:
    tier membership never leaks into a tracked file; held names are hard-gated
    by the freshness check (they already are, via `RECOMMENDED_DEPTH`).
13. **Line of sight - broad universe(s) ranked continuously.** One or more
    universes beyond the watchlist (rule-based top-N by dollar volume; possibly
    several tiers, e.g. large-cap and small-cap) scored every run, with the
    watchlist and held tier as *tags* on top rather than as the universe ->
    verify: a symbol can be ranked without being tracked, and the watchlist's
    position within the broader universe is queryable ("am I holding the
    best-ranked names available, or just the ones I know?").

    This is what breaks the feedback loop. Ranking only the watchlist can never
    reveal that the whole watchlist is wrong.

14. **Promotion - expand the tracking list.** Candidates from task 13 that meet
    the profitability criteria get promoted into tracking -> verify: promotion
    is an explicit, recorded event (symbol, date, reason, universe of origin) so
    the tracked set stays reconstructable at any past date; and demotion exists
    too, or the list only ever grows.

    Distinct from 13 on purpose: 13 is continuous visibility, 14 is a deliberate
    act that changes what is tracked and eventually what is held.

### Decisions needed before the work reaches them

- **Before task 8:** how a variant is expressed - config entry or SQL fragment?
- **Before task 11:** how the outperformance probability is actually computed.
  The current composite is a placeholder, not a candidate answer. The forecast
  window is ruled (2026-08-11): 126 sessions (~6 months) provisionally, final
  choice from task 6's IC-by-horizon decay report, per SPEC-SIGNAL-TIERS §2.
- **Before task 13:** which broad universe(s), and how many tiers. One
  (large-cap) is simplest; several give better context at more data cost.
- **Before task 14:** what "meets my criteria for profitability" means as a
  screen, expressed as rules over data we hold - and whether promotion is
  automatic or proposed for approval.
- **Before P4 of the parent spec:** rank value/quality within sector, or accept
  the structural sector tilt deliberately.

## Dev docs - live gotchas

0. **`PERCENT_RANK` assigns 0.0 to the FIRST row in the ordering.** So
   `ORDER BY x DESC` gives the largest `x` the *lowest* percentile. All four
   score components shipped inverted on this, and every test passed. Component
   directions now live once in `src/scoring/components.py`; never add one
   without a direction test and a null fixture. Two advisors reasoned backwards
   about this in opposite directions - settle ranking direction by executing
   the SQL, never by argument.

0a. **DuckDB `ASOF JOIN` silently drops unmatched left rows.** Use
   `ASOF LEFT JOIN`. Point-in-time fundamentals joins are exactly where this
   bites: a symbol with no filing before the as-of date vanishes from the
   result instead of appearing with nulls, so the universe silently shrinks and
   nothing fails. Same failure shape as a dead ticker contributing no rows.
   Also: Databricks time-series feature tables permit exactly one timestamp
   key, so any bitemporal collapse has to happen in silver.
   Source: `knowledge/research/2026-08-10-oss-quant-stacks.md`.

0b. **Dialect differences are found by running, not reading.** `CURRENT_TIMESTAMP()`
   is a function in Spark and a bare keyword in DuckDB. Any SQL that must run in
   both goes through `tests/test_engine_parity.py` first, and differences are
   resolved to the common subset - an engine branch is two implementations again.

0c. **EDGAR's ticker maps are incomplete and CIKs are not forever.**
   `company_tickers.json` is missing real registrants (AEP was absent;
   `common.edgar.resolve_cik_fallback` resolves via browse-edgar). XOM's
   ticker now points at a new holding-company CIK with one quarter of
   history - the operating company's history lives under the old CIK, so XOM
   has no silver fundamentals until the holdco's first 10-K (or a
   predecessor-CIK merge, which needs a ruling). And multi-class filers
   (BRK) report share counts as dimensioned facts CompanyFacts omits: the
   undimensioned count can be years stale, which is why
   `scoring.candidates` nulls earnings yield when the share count is older
   than 400 days. Stale would otherwise score as an E/P off by orders of
   magnitude.

1. **Never raise numpy above 1.x in `databricks.yml`.** Serverless
   `environment_version: "3"` ships `pyarrow==15.0.2`, which requires
   `numpy>=1.16.6,<2`, and nothing in the dependency list upgrades pyarrow.
   Arrow backs every Spark/pandas conversion, so a numpy 2.x pin breaks the job
   at the next run, not at deploy. `bundle validate` cannot catch it.
   - **If numpy must move:** pin pyarrow >=16 in the same change and prove the
     pair resolves before shipping. Why and how it was caught:
     `completed/plan-completed-2026-08-09.md`.

2. **Job deps are pinned to versions the base image tolerates, not to
   `uv.lock`.** `databricks.yml` and `pyproject.toml` are aligned by hand where
   the base image constrains a package (numpy, requests). Changing one without
   the other reintroduces the local/production split.

3. **`WATCHLIST` in the dotenv shadows `tickers.txt` locally.**
   `config._load_tickers()` reads the env var first, and `load_dotenv()` finds
   the dotenv even when the variable is unset in the parent shell. Local runs
   therefore never exercise the file path. To test the fallback, run from a
   directory with no dotenv above it.

4. **`tickers.txt` matters only for deploy.** It is gitignored, force-synced by
   `databricks.yml` (`sync.include`), and confirmed present in the deployed
   workspace bundle. A manual deploy ships whatever the file holds - run
   `uv run python scripts/watchlist.py seed` first. CI materializes it from the
   `WATCHLIST` repo secret instead.

5. **Databricks Free Edition constraints** that break the build are enumerated in
   CLAUDE.md ("Free Edition gotchas"). Still current; not duplicated here.

## Settled

- **Bundles need no local terraform.** The CLI downloads its own (TF 1.5.5 +
  provider 1.124.0); `DATABRICKS_TF_*` are air-gapped-only. Record in
  `completed/plan-completed-2026-08-09.md`.
- **`astral-sh/setup-uv` publishes floating major tags only through `v7`.** v8
  and v9 must be referenced by exact tag. Same record.
