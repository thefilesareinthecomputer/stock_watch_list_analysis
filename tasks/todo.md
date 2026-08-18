# TODO

Session-to-session handoff snapshot. Resolved threads leave; survivors compress
to state + next action + pointer. Full records: `tasks/completed/`.

## Start here - next session

**State: the engine is LIVE, weekly cadence running.** First actionable
round emitted 2026-08-13 (vintage 2026-08-10, off-cycle by recorded event,
978 scored / 98 buys, frozen expectations). Weekly sell review live
2026-08-16 (`scripts/weekly_review.py`, Mondays pre-open; first run: 10
act, all deteriorating). Latest decision report: `reports/` (private,
regenerate with `decide.py`). 436 tests passing.

Read in this order: `tasks/plan.md` (tasks + gotchas, note new 0f-0h),
`tasks/SPEC-EVENT-AWARENESS.md` (Phases M/A/E remain), `SPEC.md` (invariants).

Committed and pushed through `6dde1bf` (2026-08-14). Ruling 2026-08-15:
**Databricks/L5 is secondary** - the priority is operating the engine
(buys/sells this week, tracked to the Aug 31 rebalance).

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

- **Phase W is LIVE** (built 2026-08-16, plan task 16). Reassess Phases
  M/A/E against real weekly output after two or three reviews - supervisor
  advice on record. Kalshi/Polymarket ToS is an unverified personal-use
  assumption - check before the Phase E fetcher ships.
- **Task 14 re-scope**: tier-wide calls made "promotion" about the
  watchlist overlay, not call eligibility. Re-scope before building.
- **L5 / Databricks: deferred** (ruling 2026-08-15, blockers recorded in
  `tasks/SPEC-LOCAL-WAREHOUSE.md` L5).

## Calendar

- **Mondays 7:52am CT**: weekly review (harness cron `53403fac`, durable).
  Recurring harness tasks auto-expire after ~7 days - covers 08-17 and one
  final fire; recreate it or run `scripts/weekly_review.py` by hand after.
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
4. **Yahoo 08-11 session hole** (gotcha 0g): partly healed - 1,022 banked
   symbols now carry the bar; 135 (incl. 9 held, among them FBRT, ZTEK,
   ROIV) still lack it. A single mid-history bar; the held-freshness gate
   checks the latest session and passes. If Yahoo heals it, a future
   backfill absorbs it; no action needed.
5. **`setup-uv` pinned to exact `v9.0.0`** - will not pick up patches.

## Deferred, not blocking

- Sharadar (~$29/mo): worth buying once universe refresh is routine - fixes
  delisted history (survivorship) AND the yfinance throttling/holes
  (gotchas 0f/0g strengthen the case). Ruling 2026-08-11.
- Congressional trades display-only; FRED/Fama-French context-only on
  Databricks; theme aggregation rejected 2026-08-11.
