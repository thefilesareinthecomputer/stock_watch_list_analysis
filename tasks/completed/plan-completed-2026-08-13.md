# Completed - 2026-08-13

## 2026-08-13

**First actionable round shipped and RUN (SPEC-FIRST-ACTIONABLE-ROUND, spec
moved here as `SPEC-FIRST-ACTIONABLE-ROUND-2026-08-13.md`).** Built overnight
2026-08-12 -> 13; all 10 success criteria carry passing tests; suite 380 -> 425.

- **Broad EDGAR backfill**: `backfill_fundamentals.py --universe`
  (`edgar.universe_backfill_targets`, resume-safe by construction). Live:
  bronze_fundamentals 948,655 facts / 980 symbols (was 268K / 263). No-CIK
  list is all ETFs; no-facts list is foreign ADRs - both expected.
- **Broad validation**: full harness over 1,038 banked symbols (2010-2026,
  185 monthly dates). Composite IC 0.0360 t 4.43 @126, folds positive 14/17
  years, negatives only 2016/2018/2020 (documented momentum-crash years).
  Passed the pre-committed t>=3.0 rule -> call universe = broad banked set.
  Trials 48-50 + decay run recorded. Registry events: `universe_validation`
  + `correction` (see gotcha 0h - selection bias, U-shaped deciles).
- **Off-cycle round emitted**: `rebalance.py --off-cycle`, gated on a
  single-use `authorize_off_cycle` registry event naming the exact vintage.
  Round 2026-08-10, off_cycle:true, 978 scored, 98 buys, expectation frozen
  from broad decay results (source-hashed). Re-run verified as no-op.
  Vintage is 08-10 not 08-11: Yahoo served no 08-11 bar for 79 symbols
  (both endpoints) - last consistent session won; `--as-of` cap added to
  build_local for exactly this.
- **Trade journal**: `common.trades` - trades.jsonl (gitignored,
  append-only), parse/validate, share counts, POSITIONS.md reconciliation
  (warns, never fails), FIFO open lots (oversell demands a seed), seed
  entries for pre-journal basis. No broker integration, no balances, by
  design: prices come from the warehouse.
- **Loss-harvest screen**: `trades.loss_harvest` - taxable accounts only
  (name-pattern excludes roth/ira/401/hsa), below-basis lots with holding
  period, deteriorating flag, cross-account 30-day wash-sale warning.
  bronze_prices is price-authoritative over the screening table.
- **Decision report**: `scripts/decide.py` -> `reports/decision_YYYY-MM-DD.md`
  (dir gitignored). Round + frozen expectations + calls + held overlay +
  harvest + emerging (labeled UNVALIDATED) + caveats. Generated live for
  2026-08-12.
- **Aug 31 agreement machinery**: `postmortem.journal_agreement` classifies
  journaled actions vs the latest round's calls (followed / contradicted /
  unprompted; seeds and pre-round trades excluded); symbol-free counts in
  tracked post-mortem reports. Machine-vs-machine comparison needs no
  section: rounds record prior_call next to call.
- **Supervisor (cover-me) fixes, same night**: `latest_calls` now folds
  state across ALL rounds oldest-first (was last-round-only - a symbol
  absent from one round lost its hysteresis state and could re-enter as a
  fresh buy; live path now matches simulate_calls). Partial-bar guard
  (`quality.completed_session_cutoff`, 16:30 ET) in both fetch paths -
  root cause of the 313 phantom pre-market rows deleted by hand that
  morning. Harvest price preference swapped to bronze-first. Rebalance now
  reports session coverage and warns when >2% of banked symbols lack the
  vintage session (60/1,038 were silently absent from the first round).
  CAVEATS text: watchlist self-selection line replaced by the
  universe-selection-bias caveat.

**Event-awareness design settled (spec stays hot: SPEC-EVENT-AWARENESS.md).**
Advisor consult + 4-angle deep-research pass (findings in session scratchpad,
key facts recorded in the spec). Rulings: monitor permanently display-tier
(with ~3 regime events in the data it can never reach the fold-t>3.0 bar -
declared, not deferred); thresholds literature-cited, never fit to own
history; headline APIs and GDELT rejected (news-sentiment horizon caps at
one quarter per Heston & Sinha 2017; attention trading measurably loses per
Barber et al. 2022; free tiers decayed); prediction markets = world events
already operationalized (Kalshi + Polymarket, read-only; political markets
compress toward 50%); weekly review verdicts act/too-soon/punt with "act"
only from sanctioned evidence; armed alerts terminate in the existing
off-cycle authorization machinery; scheduled + manual-anytime.

**Abandonment rule drafted** (`knowledge/ABANDONMENT-RULE.md`, private):
6-month evaluation window, 10% allocation cap, abandonment on -10pts vs SPY
at window close or process breach at any time - ratified. De-risk clause
open as a five-option decision memo (deadline: before Phase M or first
sleeve buy). Registry hash event pending ratification of the final clause.

**Full-dataset validation** (was queued in todo 2026-08-12): superseded by
the broad validation above - same work, done as the evidence gate.
