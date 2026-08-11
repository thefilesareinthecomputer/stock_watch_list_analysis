# Plan / dev docs

Active plan and live gotchas only. Finished work lives in `tasks/completed/`.

## Active

**Recommendation engine rebuild.** Architecture `SPEC.md`; programme
`tasks/SPEC-RECOMMENDATION-ENGINE.md`; current phase
`tasks/SPEC-LOCAL-WAREHOUSE.md`.

Shipped: P0 (inverted score fixed, immutable snapshot, freshness gate) and
L1 (local backfill - 1.19M rows, 324 symbols, 2010-2026, 0.557s full scan).

### Ordered tasks

Dependencies: L2 -> L3 -> L4 -> L5. Within L2, tasks 1 and 2 are independent and
can run in parallel. Within L3, 6 and 7 are independent once 5 lands.

**L2 - local silver** (gates everything after it)

1. Adjustment factors from dividends and splits; adjusted series derived, never
   stored -> verify: derived series reconciles with yfinance `adj_close` within
   0.5% for 10 dividend payers across 3 split events.
2. Port indicator SQL to shared definitions in `src/scoring/` -> verify: a parity
   test per indicator, Spark output == DuckDB output on the standard fixture.
3. Local `silver_signals` built from the shared SQL -> verify: row count per
   symbol equals its trading-day count in bronze; no null `as_of_date`.

**L3 - evaluation harness**

4. Forward returns at 21/63/252 sessions, filled at next open -> verify: a
   hand-computed return for one symbol over one window matches to 6dp.
5. Cost and slippage model -> verify: a zero-cost run exceeds a costed run by
   exactly the modelled basis points.
6. Metrics - IC, decile monotonicity, hit rate, turnover, excess vs equal-weight
   -> verify: a synthetic perfectly-predictive feature scores IC == 1.0 and
   monotonic deciles.
7. Known-answer and look-ahead tests -> verify: harness on the benchmark returns
   ~zero excess; shifting features forward one day degrades every metric.

**L4 - variant comparison**

8. Scoring variants expressed as data, not code edits -> verify: two variants
   run from one command and produce different, reproducible results.
9. Results recorded with the methodology that produced them -> verify: re-running
   a recorded variant reproduces its output exactly.

**L5 - promote**

10. Winning variant to Databricks -> verify: parity test green, `bundle
    validate` OK, CI deploy green, `METHODOLOGY_VERSION` bumped so past
    snapshots are not restated.

### Decisions needed before the work reaches them

- **Before task 6:** cost model - flat basis points or spread-aware? Flat is
  defensible at a 3-12 month horizon and much simpler.
- **Before task 8:** how a variant is expressed - config entry or SQL fragment?
- **Before P4 of the parent spec:** rank value/quality within sector, or accept
  the structural sector tilt deliberately.

## Dev docs - live gotchas

0. **`PERCENT_RANK` assigns 0.0 to the FIRST row in the ordering.** So
   `ORDER BY x DESC` gives the largest `x` the *lowest* percentile. All four
   score components shipped inverted on this, and every test passed. Component
   directions now live once in `src/scoring/components.py`; never add one
   without a direction test and a null fixture.

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
