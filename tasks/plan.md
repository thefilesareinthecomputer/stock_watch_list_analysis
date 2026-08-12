# Plan / dev docs

Active plan and live gotchas only. Finished work lives in `tasks/completed/`.

## Active

**Recommendation engine rebuild.** Architecture `SPEC.md`; programme
`tasks/SPEC-RECOMMENDATION-ENGINE.md`; phase specs `tasks/SPEC-LOCAL-WAREHOUSE.md`
(L1-L4 done, L5 open) and `tasks/SPEC-SIGNAL-TIERS.md`.

Shipped 2026-08-10 - record in `completed/plan-completed-2026-08-10.md`:
P0 (score inversion, immutable snapshot, freshness gate), watchlist merged to
324, engine parity harness, L1+L2 of the local warehouse.

Shipped 2026-08-11 - record in `completed/plan-completed-2026-08-11.md`:
EDGAR fundamentals end to end (bronze facts -> PIT silver -> candidate
signals); L3 evaluation harness (tasks 4-7, all trust checks green); trial log
(9b, tracked `trial_log.jsonl`, count 32); L4 variants as data (tasks 8-9,
reproducible recorded results); 10b tier registry with the evidence-based
re-sort (scored = 12-1 momentum + earnings yield; incumbents demoted);
decay validated on the 21..252 monthly ladder with overlap-corrected
significance; SIC entity guard (commodity trusts out of earnings ratios).
Commits `7195ab1`, `14fe8ec`, `bf908e0`.

### Ordered tasks

**L5 - promote**

10. Winning variant to Databricks -> verify: parity test green, `bundle
    validate` OK, CI deploy green, `METHODOLOGY_VERSION` bumped so past
    snapshots are not restated. **Blocked:** methodology v2 needs candidate
    data (EDGAR) on Databricks; ship as load-and-serve tables, not a port.

**L6 - buy/sell calls, tiering, and discovery**
Specced in `tasks/SPEC-SIGNAL-TIERS.md`. 10b (registry) is done.

11. **Buy/sell calls with frozen expectations and a human-ratified
    post-mortem.** **Built 2026-08-11** (spec + rulings:
    `tasks/SPEC-BUY-SELL-CALLS.md`): state machine (`scoring.calls`),
    durable record `calls_log.jsonl` -> `gold_calls`, frozen expectations
    (`scoring.expectations`), settlement + drift + immutable post-mortems
    (`backtest.settlement`/`postmortem`), orchestrator
    `scripts/rebalance.py`. Replay validation passed: in-position beats out
    at every rung, spread positive 16/17 years @126, turnover 5.3% monthly
    vs 50% bound (`results/calls_validation.json`). 356 tests.
    **Operational start: run `scripts/rebalance.py` after the 2026-08-31
    close** - the first prospective round; earlier vintages are refused as
    backdated by `first_round_month` in the registry.

12. Priority tier for held positions - **Built 2026-08-11.**
    `common.positions` parses `knowledge/POSITIONS.md` (all caps; account-
    sectioned, fractional quantities); `build_local.py` hard-gates every
    tracked held name on freshness (stale -> build fails) and builds
    warehouse-only `gold_held_positions` (accounts, quantity, tracked flag,
    v2 rank, latest call). Held-but-untracked names surface as the
    promotion-candidate list. Verified: 72 tracked + 1 unscored (VFIAX) on
    real data; tier membership never touches a tracked file (fixture-only
    tests). Held tier does NOT gate call emission - it is an overlay.

13. **Line of sight - COMPLETE 2026-08-12.** Top 1000 by trailing median
    dollar volume as a tier TAG over the 6,264-stock inventory; size never
    filters. `emerging` = top-decile 12-1 momentum AND top-decile
    dollar-volume acceleration, EACH with absolute confirmation (rising;
    volume above its year), floors $1 / $200k - a bear market empties the
    tag by construction (test-pinned). `deteriorating` = down over 3m, 6m
    AND 12m (absolute) - the sell-side alert, joined into
    gold_held_positions and printed by build_local; covers held ETFs and
    funds that calls never touch. Verified live: 6,330 ranked, 152
    emerging, 1,000-member tier, full 2010+ history banked (bronze_prices
    3.66M rows / 1,050 symbols), 10 held names flagged deteriorating.
    Refresh monthly with the rebalance (~30-60 min; resumable across
    midnight). Full-depth scoring of the broad tier (indicators + EDGAR at
    1000-symbol scale) remains open - ties into L5/P4.

14. **Promotion - expand the tracking list.** Candidates from 13 meeting the
    profitability criteria promoted as explicit recorded events (symbol, date,
    reason, universe of origin), with a demotion path -> verify: the tracked
    set is reconstructable at any past date.

### Decisions needed before the work reaches them

- **Before task 13:** which broad universe(s), and how many tiers.
- **Before task 14:** the profitability screen as rules over data we hold;
  promotion automatic or proposed-for-approval.
- **Before P4 of the parent spec:** sector-relative ranking for value/quality
  (SIC codes now stored in `bronze_entity`) or accept the structural sector
  tilt deliberately. GP/A's broken top decile is the motivating case.
- **XOM predecessor CIK:** merge the old operating company's history under the
  holdco ticker, or accept neutral fundamentals until its first 10-K.

## Dev docs - live gotchas

0. **`PERCENT_RANK` assigns 0.0 to the FIRST row in the ordering.** So
   `ORDER BY x DESC` gives the largest `x` the *lowest* percentile. All four
   score components shipped inverted on this, and every test passed. Component
   directions live once in `src/scoring/components.py` (v1) and per-component
   direction flags in the registry/variants (v2); never add one without a
   direction test and a null fixture. Settle ranking direction by executing
   the SQL, never by argument.

0a. **DuckDB `ASOF JOIN` silently drops unmatched left rows.** Use
   `ASOF LEFT JOIN`. Point-in-time fundamentals joins are exactly where this
   bites: a symbol with no filing before the as-of date vanishes instead of
   appearing with nulls. Databricks time-series feature tables permit exactly
   one timestamp key, so any bitemporal collapse happens in silver.

0b. **Dialect differences are found by running, not reading.**
   `CURRENT_TIMESTAMP()` is a function in Spark and a bare keyword in DuckDB.
   Any SQL that must run in both goes through `tests/test_engine_parity.py`
   first; differences resolve to the common subset - an engine branch is two
   implementations again.

0c. **EDGAR's ticker maps are incomplete and CIKs are not forever.**
   `company_tickers.json` is missing real registrants (AEP;
   `common.edgar.resolve_cik_fallback` resolves via browse-edgar). XOM's
   ticker points at a new holding-company CIK with one quarter of history -
   no silver fundamentals until the holdco's first 10-K or a predecessor-CIK
   merge (needs a ruling). Multi-class filers (BRK) report share counts as
   dimensioned facts CompanyFacts omits - `scoring.candidates` nulls earnings
   yield when the share count is older than 400 days.

0d. **Metrics are security-type- and industry-conditional; build around it.**
   Commodity trusts (SLV, SIC 6221) file 10-Ks whose "net income" is metal
   appreciation - SLV ranked #2 in v2 before the guard, and it reports a
   Revenues tag, so only SIC discriminates. `bronze_entity` stores SIC per
   symbol; earnings ratios go neutral for `NON_OPERATING_SIC`. Still open:
   GP/A undefined for financials; value/quality cross-universe are structural
   sector bets (decision above). The inverted low-vol reading (t -2.6) is a
   survivor-universe artifact, not a verdict on the anomaly.

0e. **Long-horizon t-stats are inflated by window overlap.** Monthly eval
   dates with a 12-month window overlap 11/12ths; naive t at 252 read 7.0,
   corrected (yearly folds / non-overlapping windows) ~2-3.4. Judge long
   horizons on fold-level t. Documented in `backtest.metrics.ic_summary`.

1. **Never raise numpy above 1.x in `databricks.yml`.** Serverless
   `environment_version: "3"` ships `pyarrow==15.0.2` (requires numpy<2);
   a numpy 2.x pin breaks the job at the next run, not at deploy, and
   `bundle validate` cannot catch it. If numpy must move, pin pyarrow >=16 in
   the same change. Full record: `completed/plan-completed-2026-08-09.md`.

2. **Job deps are pinned to versions the base image tolerates, not `uv.lock`.**
   `databricks.yml` and `pyproject.toml` are aligned by hand where the base
   image constrains a package (numpy, requests).

3. **`WATCHLIST` in the dotenv shadows `tickers.txt` locally.**
   `config._load_tickers()` reads the env var first and `load_dotenv()` finds
   the dotenv even when unset in the parent shell. To test the file path, run
   from a directory with no dotenv above it.

4. **`tickers.txt` matters only for deploy.** Gitignored, force-synced by
   `databricks.yml`. Manual deploy ships whatever it holds - run
   `uv run python scripts/watchlist.py seed` first. CI materializes it from
   the `WATCHLIST` repo secret.

5. **Databricks Free Edition constraints** are enumerated in SPEC.md
   ("Operational constraints"). Still current.

## Settled

- **Bundles need no local terraform.** The CLI downloads its own;
  `DATABRICKS_TF_*` are air-gapped-only. Record:
  `completed/plan-completed-2026-08-09.md`.
- **`astral-sh/setup-uv` floating major tags end at `v7`;** v8+ by exact tag.
- **2026-08-11 decisions** (window 126, cadence, themes rejected, cost model,
  variant format, task 11 design): `completed/plan-completed-2026-08-11.md`.
