# TODO

Session-to-session handoff snapshot. Resolved threads leave; survivors compress
to state + next action + pointer. Full records: `tasks/completed/`.

## Start here - next session

**Goal: every symbol gets a trustworthy buy or sell call at pipeline runtime.**
A buy means "most likely to outperform the benchmark over 126 sessions" - a
relative claim; below the index is failure.

Read in this order: `tasks/plan.md` (ordered tasks + gotchas), `SPEC.md`
(invariants), `tasks/SPEC-SIGNAL-TIERS.md` (tiers and calls).

**State: the measurement layer is complete and the re-sort is done.** Local
warehouse builds bronze -> silver -> gold incl. EDGAR candidates (~3 min);
evaluation harness with known-answer/look-ahead checks; trial log (32);
variants as recorded reproducible config; tier registry with methodology v2
(scored = 12-1 momentum + earnings yield, incumbents demoted on evidence);
decay validated 1-12 months with overlap-corrected significance. 303 tests.
All synced to develop and main at `bf908e0` + doc follow-ups.

**START WITH task 11 - spec is written and ready to build:**
`tasks/SPEC-BUY-SELL-CALLS.md` (state machine, `gold_calls` schema, frozen
expectations, settle->report->emit, drift defaults, success criteria 1-9).
This starts the paper clock, the only path to go-live evidence. Four open
questions are listed in the spec with proposed defaults; none block the build.

```bash
uv run pytest tests/ -q                     # 303 passing
uv run python scripts/build_local.py        # full local rebuild, ~3 min
uv run python scripts/evaluate.py --candidates
uv run python scripts/ic_decay.py           # IC by 1-12 month horizon
```

If `warehouse/` is missing (gitignored): `scripts/backfill.py` then
`scripts/backfill_fundamentals.py` first (~5 min total).

## Needed from the user

- **Before task 13:** which broad universe(s) - one large-cap tier or several.
- **XOM predecessor-CIK ruling** (plan.md gotcha 0c).
- **Stale branch:** `feature/upgrade-stock-pipeline` on origin, superseded -
  delete or keep.

## Open

1. **L5 blocked by design:** v2 cannot deploy to Databricks until candidate
   data ships there as load-and-serve tables (EDGAR is local-only today).
2. **Sector-relative ranking** for value/quality is the flagged candidate-
   quality fix (SIC stored; GP/A's broken top decile is the motivating case).
   Decide before parent-spec P4.
3. **Go-live remains process-gated:** months of immutable paper track, capped
   allocation, pre-committed abandonment rule - never a backtest threshold.
   In-sample skill decays 26-58% out of sample; plan accordingly.
4. **`setup-uv` pinned to exact `v9.0.0`** - will not pick up patches.

## Deferred, not blocking

- Sharadar (paid) for delisted history - only when backtest levels matter.
- Congressional trades stay display-only; FRED/Fama-French stay on Databricks
  as context/attribution - none of them touch a rank.
- Theme/cohort aggregation rejected 2026-08-11: ticker-level, objective data
  only.
