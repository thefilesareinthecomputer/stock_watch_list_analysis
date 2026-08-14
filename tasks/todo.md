# TODO

Session-to-session handoff snapshot. Resolved threads leave; survivors compress
to state + next action + pointer. Full records: `tasks/completed/`.

## Start here - next session

**State: the engine is LIVE.** First actionable round emitted 2026-08-13
(vintage 2026-08-10, off-cycle by recorded event, 978 scored / 98 buys over
the validated broad universe, frozen expectations). Decision report:
`reports/decision_2026-08-12.md` (private). 425 tests passing. Full record:
`completed/plan-completed-2026-08-13.md`.

Read in this order: `tasks/plan.md` (tasks + gotchas, note new 0f-0h),
`tasks/SPEC-EVENT-AWARENESS.md` (next build), `SPEC.md` (invariants).

**NOTHING IS COMMITTED.** Two days of work (first-actionable-round build,
supervisor fixes, event-awareness spec, docs) sit uncommitted on `develop`.
First action: commit and push via the repo-device-sync ritual.

```bash
uv run pytest tests/ -q                    # 425 passing
uv run python scripts/decide.py            # regenerate the decision report
uv run python scripts/rebalance.py         # monthly round (Aug 31 scheduled)
uv run python scripts/build_local.py --universe --as-of DATE  # broad rebuild
```

## Needed from the user

- **Journal basis seeds**: prefilled template in `_relay.md`; fill
  price/date per brokerage lot, say "seed ready". Until then the harvest
  screen has nothing to chew (GDS and MP are deteriorating brokerage names,
  so it likely fires once seeded).
- **De-risk clause**: five-option decision memo in
  `knowledge/ABANDONMENT-RULE.md`. Deadline: before the monitor builds or
  the first system-sleeve buy. Other three clauses ratified 2026-08-12.
- **XOM predecessor-CIK ruling** (plan.md gotcha 0c): recommend merging
  predecessor history (~15 lines in edgar.py + refetch); XOM is
  momentum-only in v2 until then.
- **Stale branch** `feature/upgrade-stock-pipeline` on origin: verified
  fully merged; say the word for `git push origin --delete`.

## Queued next

- **Build SPEC-EVENT-AWARENESS Phase W** (weekly review) after Phase 0
  (abandonment-rule ratification). Supervisor advice on record: ship W,
  then reassess Phases A/E against real weekly output before building them.
  Kalshi/Polymarket ToS is an unverified personal-use assumption - check
  before the Phase E fetcher ships.
- **Task 14 re-scope**: tier-wide calls made "promotion" about the
  watchlist overlay, not call eligibility. Re-scope before building.

## Calendar

- **2026-08-31 5:41pm**: scheduled task `ab4a0af4` runs the full ritual
  (backfill -> fundamentals -> build_local -> rebalance). Post-mortem report
  will carry the journal-agreement section. Hysteresis state folds across
  the off-cycle round (fixed 2026-08-13 - state carries through gaps).
- **~2026-09-09**: first settleable rung (21 sessions) of the 08-10 round.
  Settlement is automatic at the next rebalance after it closes.

## Open

1. **L5 blocked by design**: v2 cannot deploy to Databricks until candidate
   data ships there as load-and-serve tables (EDGAR is local-only).
2. **Sector-relative ranking** for value/quality - decide before parent P4.
   GP/A repeated its broken-decile pattern on the broad universe (trial 49).
3. **Go-live remains process-gated**: the abandonment rule (drafted) is the
   pre-commitment; months of immutable paper track before the cap rises.
4. **Yahoo 08-11 session hole** (gotcha 0g): 79 symbols still lack the bar.
   If Yahoo heals it, the next monthly build absorbs it; no action needed.
5. **`setup-uv` pinned to exact `v9.0.0`** - will not pick up patches.

## Deferred, not blocking

- Sharadar (~$29/mo): worth buying once universe refresh is routine - fixes
  delisted history (survivorship) AND the yfinance throttling/holes
  (gotchas 0f/0g strengthen the case). Ruling 2026-08-11.
- Congressional trades display-only; FRED/Fama-French context-only on
  Databricks; theme aggregation rejected 2026-08-11.
