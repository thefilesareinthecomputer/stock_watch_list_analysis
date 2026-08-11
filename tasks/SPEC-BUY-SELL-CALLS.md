# Spec: Buy/Sell Calls with Frozen Expectations and Post-Mortems

In-flight feature spec (plan task 11). On completion its durable essence folds
into root `SPEC.md` and this file moves to `tasks/completed/`.

Parent: `tasks/SPEC-SIGNAL-TIERS.md` §4 (principles ruled 2026-08-11);
programme `tasks/SPEC-RECOMMENDATION-ENGINE.md`. Depends on: L3 harness,
tier registry, decay results - all shipped 2026-08-11.

---

## Objective

Emit a call per symbol per rebalance round from the methodology-v2 composite,
record it immutably WITH the expectation it will be graded against, and grade
every prior round before emitting a new one - so the live record accumulates
falsifiable evidence from day one, and adjustment happens only as
human-ratified registry events.

This starts the paper clock. It is the bridge from "walk-forward skill" to
"go-live evidence" (parent spec P3), and nothing about it touches production
Databricks.

## Non-goals

- Position sizing, order generation, execution. (Parent P3 sizing comes next.)
- Cash/regime timing. Calls are benchmark-relative stances, never directional.
- Automated tier changes. The post-mortem SUGGESTS registry events; a human
  records them.
- Shipping to Databricks (L5) or the broad universe (task 13).

## Design

### Calls and hysteresis

Cadence: monthly, on the last trading session of the month (the harness's
eval-date convention); action is modelled at the next open, as everywhere.

Call is a state machine per symbol, because hysteresis requires memory:

| Prior state | Condition (v2 score percentile) | Call |
|---|---|---|
| none / sell | >= 0.90 | **buy** (enter) |
| buy / hold | >= 0.50 | **hold** (stay) |
| buy / hold | < 0.50 | **sell** (exit) |
| none / sell | < 0.90 | **none** (no position path) |

Thresholds live in the registry file, not code, so changing them is a recorded
event. ETFs and benchmarks are excluded (they are yardsticks or overlays).

### Append-only snapshot: `gold_calls` (local DuckDB)

Same discipline as production `gold.recommendations`
(`src/scoring/snapshot.py`): append-only, first-write-wins per
(`as_of_date`, `methodology_version`, `symbol`). Columns: as_of_date, symbol,
methodology_version, score, rank, per-component percentiles, call, prior_call,
expectation (JSON, below), expectation_source, run_id, created_ts. A re-run of
the same round must be a no-op; history is never restated.

### Frozen expectation

Attached to every round (not per symbol) at emit time, from the latest
recorded decay results for the scored composite:

- Per horizon (21, 63, 126 sessions): the walk-forward distribution of
  top-decile net excess - mean, p10, p90 - and fold-level IC.
- All excess figures HAIRCUT by the out-of-sample decay factor (default 0.5,
  the McLean-Pontiff midpoint), recorded alongside the raw values.
- `expectation_source`: the sha256 of the decay results file used, so the
  claim is traceable to the exact recorded evidence.

### Post-mortem, before every round

`scripts/rebalance.py` (single entry point) runs strictly in this order:
settle -> report -> emit. It refuses to emit if settlement fails.

1. **Settle**: for every prior round with a closed window at any rung,
   compute realized top-decile net excess and realized IC vs the round's
   frozen expectation.
2. **Report**: write `results/post_mortem/YYYY-MM-DD.md` + `.json` (tracked;
   immutable by convention - corrections go in later reports). Contents:
   per-vintage realized vs expected, per-signal attribution (momentum leg vs
   E/P leg), cumulative drift state, and either "within expectation - nothing
   to learn yet (n=K)" or SUGGESTED registry events with evidence attached.
3. **Emit**: write the new round to `gold_calls`, log a trial.

Drift defaults (registry-held, revisable by recorded event): suggest a review
when realized 21d excess runs below the haircut mean for 5 consecutive
settlements, or below p10 for 3 consecutive; suggest demotion review for a
signal when its attributed walk-forward IC turns insignificant on fold-level
t. Single-interval misses are noise by construction (1-month fold-t 1.9) and
the report must say so.

## Boundaries

**Always:** settle before emit; freeze expectations at emit; append-only;
exclude ETFs/benchmarks; state the survivorship caveat in every report.

**Ask first:** changing thresholds or drift constants (registry event);
emitting off-cadence rounds; any interpretation of a post-mortem as a go-live
signal.

**Never:** restate a past call or expectation; auto-apply a suggested registry
event; grade a vintage against anything but its own frozen expectation.

## Success criteria

| # | Criterion | Verified by |
|---|---|---|
| 1 | On held-out history, buy set beats sell set on forward excess | Backtest of the state machine over the walk-forward period |
| 2 | Known-answer still holds: benchmark yields ~zero | Existing harness test stays green |
| 3 | `gold_calls` is append-only, first-write-wins | Re-run diff test (no-op) |
| 4 | Every round carries a frozen, source-hashed expectation | Snapshot schema test |
| 5 | Hysteresis: no exit above the 0.50 percentile | State-machine transition tests |
| 6 | Post-mortem settles all gradeable vintages before emit | Ordering test; emit refused on settlement failure |
| 7 | Synthetic drifting vintage -> demotion suggestion; healthy -> none | Post-mortem fixture tests |
| 8 | Every round logs a trial | Trial count test |
| 9 | Turnover of the buy set stays below ~50% one-sided monthly | Measured on the historical state machine (Novy-Marx & Velikov bound) |

## Open questions

1. **Drift constants** (5 / 3 / fold-t bar) - defaults above; tune only by
   recorded registry event once real settlements exist.
2. **Haircut factor** 0.5 - revisit when the paper track has its own data.
3. **Does the held-position tier (task 12) gate call visibility** or only
   report ordering? Decide when task 12 lands.
4. **Backfilled state machine start**: seed prior state from the walk-forward
   history's last state, or start all symbols at `none`? Proposed: start at
   `none` - cleaner, and the paper record should not inherit simulated state.
