# Completed - 2026-08-11

## 2026-08-11

### EDGAR CompanyFacts end to end (parent spec P4 ingest, ahead of P2/P3)

`scripts/backfill_fundamentals.py` -> `bronze_fundamentals` (268K as-filed
facts, 263 symbols, 2009+); `common.fundamentals` -> `silver_fundamental_metrics`
(PIT knowledge series keyed on `filed`, restatement-aware, amendment-safe);
`scoring.candidates` -> `gold_candidate_signals` (E/P, gross profitability, ROE
at every (symbol, as_of_date), percentile-ranked over non-nulls, zero composite
weight). Wired into `build_local.py`. Per-CIK API chosen over the bulk zip at
watchlist scale. Commit `7195ab1`.

Fixes en route: CIK fallback via browse-edgar (company_tickers.json is missing
real registrants - AEP); 20-F/40-F forms included (foreign filers tagging
us-gaap); 400-day stale-shares guard (BRK's undimensioned dei share count is
15 years stale); split-adjusted share counts for market cap.

### L3 - evaluation harness (tasks 4-7)

`src/backtest/` (returns, costs, metrics, harness) + `scripts/evaluate.py`.
All four verify gates held: hand-computed return matches to 6dp; zero-cost
exceeds costed by exactly the modelled bps; a synthetic perfect predictor
scores IC 1.0 with monotonic deciles; the benchmark yields exactly zero
excess, a seeded random signal finds no edge in 16 years, and a shifted leak
collapses (at horizon 21, where windows are disjoint - at 126 consecutive
windows share ~85% of sessions, so a shift only dents IC: overlap, not
leakage). Cost model decision: flat 10 bps/side. Monthly eval dates - daily
would overlap the forecast windows and fake the t-stat. Commit `7195ab1`.

First harness read: earnings yield IC 0.033 @126 (t 3.9), stronger at 252;
gross profitability positive IC but negative top-decile excess; ROE weak
(t < 2.3, matches Novy-Marx); incumbent 30d momentum t 0.5 at its own horizon.

### Task 9b - prospective trial log

`backtest.trials` writing tracked, append-only `trial_log.jsonl` at the repo
root - deliberately not in regenerable `warehouse/`. Logged BEFORE any result
exists (schema has no result field); `evaluate.py` and `compare_variants.py`
log every run; re-runs over-count on purpose (conservative); no opt-out flag.
Pre-log evaluations retro-logged transparently. E2E chain test
(`tests/test_e2e_local.py`) walks bronze -> signals -> gold -> fundamentals ->
candidates -> returns -> verdict -> trial log; gold SQL extracted to
`build_gold()` so script and test share one definition; decile bucketing
switched to ceiling form (floor left decile 9 empty under ten symbols).
Commit `7195ab1`.

### L4 - variant comparison (tasks 8, 9)

Variants are config entries (`src/scoring/variants.json` + `scoring.variants`),
not SQL fragments: named components with constrained scalar expressions,
direction flags, weights; validation rejects statement separators.
`scripts/compare_variants.py` runs all from one command; results in
`results/variants/<name>.json` with definition, sha256, settings and data
fingerprint - re-runs reproduce files byte for byte (tested). Commit `14fe8ec`.

First comparison at 126 sessions (185 monthly dates): candidates_equal IC
0.035 t 5.3 turnover 0.06; mom12_1_ep_lowvol IC 0.026 t 2.0; incumbent-as-
variant IC 0.016 t 1.7 turnover 0.89. Survivorship inflates fundamentals
persistence; ranks credible, levels not.

### Task 10b - signal tier registry and the evidence-based re-sort

Registry at `src/scoring/signal_tiers.json` (`scoring.tiers`): every signal
tiered as data with evidence strings and dated promote/demote events; the
weight-zero test proves candidates cannot move a v2 score. Re-sorted on
measured evidence with user ruling: scored = 12-1 momentum + earnings yield
(t 4.1/3.9 @126, corr -0.07); all four incumbents demoted on measured nothing;
GP/A, 90d momentum, ROE, realized vol, beta candidates. Local v2 composite
(`gold_watchlist_ranked_v2`) reads the registry; production v1 untouched until
L5. 10-signal sweep trials 8-17. Commit `bf908e0`.

Sweep findings at 126: momentum family real (12-1 t 4.1, 365d t 4.5, 90d t
3.9); oscillators nothing (RSI -1.6, MFI 1.0, MACD -1.0, %B 0.1); ATR low-vol
BACKWARDS on this survivor universe (t -2.6, monotonicity -1.0) - survivorship
artifact, concept retains its literature prior; P/E and dividend yield
unmeasurable locally (yfinance snapshots have no history).

### Decay validation on the 21..252 monthly ladder

Forward returns extended to 12 monthly rungs (13.7M rows); `scripts/ic_decay.py`
reports IC per 1-12 month horizon into `results/decay/`. v2 mean IC rises from
0.030 (1mo) to ~0.062 plateau at 7-12 months and the rise survives independent
sampling (yearly blocks, non-overlapping windows). Naive long-horizon t-stats
were inflated ~2x by window overlap (user challenge, verified): corrected, 126
sessions scores fold-level t 3.4 (clears the bar); 12 months t 1.8-2.7 on the
only 15 independent windows 16 years contain. 126 stands as the window.
Overlap caveat documented at the t-stat source. Commits `bf908e0` + follow-up.

### Entity classification and the security-type guard

First v2 ranking put SLV/SIVR (silver trusts) at #2/#3 on fictional trust E/P -
trust "net income" is metal appreciation, and SLV even reports a Revenues tag,
so only SIC discriminates. `bronze_entity` stores SIC per symbol (EDGAR
submissions endpoint, 263 classified); earnings-based ratios go neutral for
`NON_OPERATING_SIC` (6221, 6726, 6770, 6799) plus a reported-revenues belt.
Corrected top 10 all operating companies. Commit `bf908e0`.

### Decisions settled 2026-08-11

- **Forecast window: 126 sessions**, confirmed from measured decay (plateau
  7-12mo; 7-8 marginally better; differences small).
- **Rebalance cadence: monthly with hysteresis stands; 1-3 months is
  sufficient** - the signal is slow (weakest at 1 month), so faster rebalancing
  adds turnover without information.
- **Theme/cohort aggregation rejected** (user ruling): everything stays
  ticker-level and objective-data-based. SIC guard retained as data hygiene,
  not thematics.
- **Cost model: flat 10 bps/side** (decided before task 6, as required).
- **Variants: config entries, not SQL fragments** (decided before task 8).
- **Evaluation universe: the watchlist**, caveated, until P2's rule-based
  universe.
- **Task 11 design ruled: frozen expectations + human-ratified post-mortem.**
  Every emitted call carries the decayed walk-forward expectation it will be
  graded against; before each rebalance round a post-mortem settles every
  gradeable vintage (realized vs frozen expectation, per-signal attribution)
  and emits SUGGESTED registry events with evidence attached - measurement
  automated, decisions human-ratified, single-interval misses labeled noise
  (1-month fold-t 1.9), only cumulative drift triggers suggestions. Immutable
  dated report files. Local v2 calls need an append-only snapshot first.
