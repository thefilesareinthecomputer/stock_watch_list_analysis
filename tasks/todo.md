# TODO

Session-to-session handoff snapshot. Resolved threads leave; survivors compress
to state + next action + pointer. Full records: `tasks/completed/`.

## Start here - next session

**Goal: every symbol gets a trustworthy buy or sell call at pipeline runtime.**
A buy means "most likely to outperform the benchmark over 126 sessions" - a
relative claim; below the index is failure.

Read in this order: `tasks/plan.md` (ordered tasks + gotchas), `SPEC.md`
(invariants), `tasks/SPEC-SIGNAL-TIERS.md` (tiers and calls).

**State: tasks 11 AND 12 are BUILT - the call machinery is live and waiting
for the calendar, and the held tier rides on top.** Task 12: `common.positions`
parses `knowledge/POSITIONS.md` into warehouse-only `gold_held_positions`
(72 tracked, 1 unscored on real data); stale held names fail the build.

Task 11: State machine + hysteresis, durable `calls_log.jsonl` ->
`gold_calls`, frozen expectations with source hash and 0.5 haircut,
settle->report->emit orchestrator, drift detection with immutable
post-mortems. Replay validation green (spread positive 16/17 years @126,
turnover 5.3% vs 50% bound). Rulings recorded in
`tasks/SPEC-BUY-SELL-CALLS.md`. 356 tests.

**Task 13 is COMPLETE and verified** (plan.md task 13 for the full record):
6,330 ranked, 1,000-member tier, 152 emerging (absolute-confirmed
shortlist), 10 held names flagged deteriorating (down 3/6/12 months - the
sell-side alert). Refresh `backfill_universe.py` monthly with the
rebalance; resumable across midnight.

**The 2026-08-31 first round is AUTOMATED**: durable scheduled task
`ab4a0af4` (.claude/scheduled_tasks.json) fires Aug 31 5:41pm with the full
ritual (backfill -> fundamentals -> build_local -> rebalance). Manual
fallback: run those four in order after the close. Running rebalance earlier
is safe - it refuses backdated vintages by design.

```bash
uv run pytest tests/ -q                     # 356 passing
uv run python scripts/build_local.py        # full local rebuild, ~3 min
uv run python scripts/validate_calls.py     # state machine replay
uv run python scripts/rebalance.py          # settle -> report -> emit
```

If `warehouse/` is missing (gitignored): `scripts/backfill.py` then
`scripts/backfill_fundamentals.py` first (~5 min total).

## Resolved 2026-08-11 (was "needed from the user")

- **Universe ruling:** top 1000 by trailing median dollar volume as a tier
  TAG over the full ~6.3k common-stock inventory - size never filters, and
  the `emerging` tag flags small caps surging on momentum or dollar-volume
  acceleration (the "might get big" screen).
- **EA is NOT dead:** the earlier failure was yfinance batch flakiness;
  `watchlist.py check` now retries singles before declaring death, and all
  324 resolve. Its quote type stays UNKNOWN (treated as equity). Watch for
  the take-private actually delisting it.

## Needed from the user
- **XOM predecessor-CIK ruling** (plan.md gotcha 0c).
- **Stale branch:** `feature/upgrade-stock-pipeline` on origin, superseded
  and fully merged (verified zero unique commits) - say the word and it gets
  `git push origin --delete`.
- **XOM predecessor-CIK:** recommend merging the predecessor history
  (~15 lines in edgar.py + refetch); XOM is momentum-only in v2 until then.

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

- Sharadar: agreed worth buying (2026-08-11 ruling; bundle from ~$29/mo at
  sharadar.com) once the universe refresh is routine - fixes both delisted
  history (survivorship) and the yfinance bulk-fetch throttling. Deferred
  until then, not until "backtest levels matter" as previously stated.
- Congressional trades stay display-only; FRED/Fama-French stay on Databricks
  as context/attribution - none of them touch a rank.
- Theme/cohort aggregation rejected 2026-08-11: ticker-level, objective data
  only.
