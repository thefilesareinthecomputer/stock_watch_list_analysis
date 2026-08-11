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

**L3 - evaluation harness** (next)

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

**L6 - buy/sell calls, tiering, and discovery** (added 2026-08-10; not yet
specced - see the note in `tasks/SPEC-RECOMMENDATION-ENGINE.md` open questions)

11. Emit a buy/sell call per symbol, defined as "outperforms the benchmark over
    the forecast window" -> verify: on held-out history the buy set beats the
    sell set on forward excess return, and the harness's known-answer test still
    returns ~zero on the benchmark. **Gated by tasks 4-7.**
12. Priority tier for held positions - a subset of the watchlist tracked more
    closely, sourced from private config like the watchlist itself -> verify:
    tier membership never leaks into a tracked file; held names are hard-gated
    by the freshness check (they already are, via `RECOMMENDED_DEPTH`).
13. Discovery sweep - screen beyond the watchlist for candidates meeting the
    profitability criteria, and promote them into tracking -> verify: the sweep
    surfaces at least one name absent from the watchlist that ranks above its
    median, and promotion is recorded so the universe stays reconstructable.

    This is the concrete form of the parent spec's rule-based universe (P2). It
    is what breaks the feedback loop: ranking only the watchlist can never tell
    you the whole watchlist is wrong.

### Decisions needed before the work reaches them

- **Before task 6:** cost model - flat basis points or spread-aware? Flat is
  defensible at a 3-12 month horizon and much simpler.
- **Before task 8:** how a variant is expressed - config entry or SQL fragment?
- **Before task 11:** the forecast window (6 months assumed) and how the
  outperformance probability is actually computed. The current composite is a
  placeholder, not a candidate answer.
- **Before task 13:** what "meets my criteria for profitability" means as a
  screen, expressed as rules over data we hold.
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

0b. **Dialect differences are found by running, not reading.** `CURRENT_TIMESTAMP()`
   is a function in Spark and a bare keyword in DuckDB. Any SQL that must run in
   both goes through `tests/test_engine_parity.py` first, and differences are
   resolved to the common subset - an engine branch is two implementations again.

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
