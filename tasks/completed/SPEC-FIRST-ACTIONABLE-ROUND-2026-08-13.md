# Spec: First Actionable Round - Off-Cycle Calls, Trade Journal, Loss Harvest

In-flight feature spec. On completion its durable essence folds into root
`SPEC.md` and this file moves to `tasks/completed/`.

Parent: `tasks/SPEC-BUY-SELL-CALLS.md` (call machinery, shipped) and
`tasks/SPEC-SIGNAL-TIERS.md`. Depends on: tasks 11-13 (all shipped),
the banked 1,050-symbol universe, `knowledge/POSITIONS.md`.

---

## Objective

Put a decision in front of the user on 2026-08-12 that the system stands
behind and will be graded on: a durable off-cycle call round over the widest
universe the evidence supports, a held-position overlay with sell-side and
tax-loss-harvest context, and a record of what the user actually does - so
the 2026-08-31 official round can compare machine-vs-machine-vs-human, and
the 21/63/126-session rungs settle everything on schedule.

Rulings 2026-08-12 (user): off-cycle durable round AND trade journal; call
universe is the 1,000-member tier if broad validation passes, watchlist
fallback otherwise; Aug 31 is a comparison checkpoint, not a settlement;
existing brokerage lots get one-time basis seeding so loss-harvest is live.

**Timing facts this spec is built on.** The round vintage was planned as
the 2026-08-11 close; **as run, it is 2026-08-10** - Yahoo served no
08-11 bar for 79 symbols (27 held), so the last consistent session won
(registry event 2026-08-12; `--as-of` cap added to build_local for this).
Action models at the next open, as everywhere. The earliest settleable
rung (21 sessions) closes ~2026-09-09 - Aug 31 grades nothing, by
arithmetic.

## Non-goals

- Position sizing, order generation, broker integration, balance tracking.
  The journal records shares in and out; values come from prices we already
  hold.
- Task 14 promotion. `emerging`/`deteriorating` stay unvalidated advisory
  screens; they label report rows, never emit calls.
- Cash/regime timing, Databricks changes (L5 unchanged), Sharadar.
- Waiving the go-live process gate. The system records and grades; it does
  not certify readiness. Acting on 2026-08-12 with real money is a user
  decision made against the stated caveats, and the record is built so that
  decision is auditable later.

## Design

### 1. Evidence gate: broad-universe validation (runs first)

Extend `scripts/backfill_fundamentals.py` with `--universe`: symbol source
becomes the current tier membership from the universe tables instead of
`TICKERS` (~790 members not yet fetched). Members without a CIK or facts
(foreign listings, thin filers) are reported and score **neutral, not
worst** - the existing convention.

Then the full harness over the banked 1,050 symbols: `build_local.py`,
`evaluate.py`, `ic_decay.py`. Every run logs trials. Known-answer and
look-ahead checks must stay green on the broad data or nothing downstream
happens.

**Universe decision rule** (v2 composite, walk-forward on the broad
universe, horizon 126):

| Fold-level t | IC | Call universe |
|---|---|---|
| >= 3.0 | positive | 1,000-member tier |
| 2.0 - 3.0 | positive | Gray zone - numbers go to the user for a ruling |
| < 2.0, or IC <= 0 | - | Watchlist (324), tier screens stay advisory |

The outcome is recorded as a registry event with the measured numbers.
Standing caveat unchanged: this fixes self-selection, not survivorship.

### 2. Off-cycle round

`rebalance.py --off-cycle`: emits for the latest `as_of_date` in
`silver_signals` rather than month-end. Everything else holds - settle
before emit, frozen source-hashed expectation, append-only
first-write-wins, trial logged. The entry carries `"off_cycle": true`.

Refused unless the registry holds an authorization event for that vintage
(one recorded event, dated, single-use). The cadence guard for normal runs
is untouched: `due_round_date` still yields only month-end vintages, the
Aug 31 scheduled task runs unmodified, and hysteresis state from the
off-cycle round carries into it - a name entered Aug 12 is `hold` on
Aug 31 unless it falls below the exit percentile.

### 3. Trade journal

`trades.jsonl` - gitignored, append-only, private (names real positions).
One line per fill:

```json
{"date": "2026-08-12", "symbol": "XYZ", "account": "brokerage",
 "side": "buy", "qty": 10, "price": 42.50, "note": "", "seed": false}
```

`price` optional on live entries (inferred from that date's close when
absent); required on `seed` entries. Seeds are the one-time basis backfill:
one entry per existing brokerage lot with approximate basis and acquired
date, marked `"seed": true` so they are never mistaken for tracked
decisions. `common.trades` parses and validates.

Reconciliation: journal-rolled share counts vs the `POSITIONS.md` snapshot
per (account, symbol), reported as warnings in `build_local` output -
never a build failure, and `common.positions` machinery is untouched.
POSITIONS.md remains the holdings source of truth; the journal is the
transaction record.

### 4. Loss-harvest screen

Brokerage lots only (Roth losses are tax-dead - excluded by account).
From the journal: a lot is flagged when current price < basis, with
holding period (short/long-term boundary at 365 days), unrealized loss,
`deteriorating` status, and a wash-sale reminder - a warning if the
journal shows a buy of the same symbol within the prior 30 days, and a
standing "do not rebuy within 30 days" note on every flagged lot. Output
lands in the decision report; the screen is context for a human sell
decision, never an automatic call.

### 5. Decision report

`scripts/decide.py` - reads the latest round from `gold_calls` plus
`gold_held_positions`, universe tags, and the journal; writes
`reports/decision_YYYY-MM-DD.md` (directory gitignored - it names
symbols) and prints it. Sections, in order:

1. The round: vintage, universe, methodology version, frozen expectation
   summary (haircut figures), and the standing caveats verbatim.
2. Calls: buys (entries), sells (exits), holds - rank and per-component
   percentiles per name.
3. Held overlay: every held name with its call, `deteriorating` flag, and
   loss-harvest context where applicable. Held-but-uncalled names (ETFs,
   funds, unscored) shown with screens only.
4. Advisory: the `emerging` shortlist, labeled UNVALIDATED SCREEN.

### 6. Month-close comparison (Aug 31)

The rebalance report step gains an agreement section, produced whenever a
prior off-cycle round or journal entries exist: call changes between the
off-cycle and official rounds, and journal actions classified against the
off-cycle calls (followed / ignored / contradicted). Tracked post-mortem
reports carry aggregates only, never symbols; the symbol-level detail goes
to the private decision report. No settlement is claimed before rungs
close.

## Sequencing (tonight -> tomorrow)

1. Build + test: `--universe` backfill, `--off-cycle` gate, `common.trades`,
   harvest screen, `decide.py` (slices, in that order, each verified).
2. Run: EDGAR tier backfill (~790 CIKs, rate-limited, est. 30-60 min,
   resumable) -> broad build -> evaluate + ic_decay.
3. Rule: universe per the decision rule; record the event.
4. User: seed journal basis for existing brokerage lots.
5. Emit: registry authorization event -> `rebalance.py --off-cycle` ->
   `decide.py` -> the user reads one report and decides.

## Boundaries

**Always:** settle before any emit, off-cycle included; freeze expectations
at emit; label `emerging`/`deteriorating` as unvalidated screens everywhere
they print; keep journal, decision reports, and calls log gitignored;
neutral scores for missing fundamentals; caveats verbatim on every report.

**Ask first:** acting on a gray-zone validation result; any second
off-cycle round; changing hysteresis, drift, or haircut constants; letting
a screen influence a call.

**Never:** backdate a journal entry as a decision (seeds are marked and
dated as seeds); restate a round or expectation; claim settlement before a
rung closes; put symbols in tracked reports or commits.

## Success criteria

| # | Criterion | Verified by |
|---|---|---|
| 1 | `--off-cycle` refused without a registry authorization event | Gate test |
| 2 | Off-cycle emit is append-only; re-run is a no-op | Re-run diff test |
| 3 | Normal cadence unaffected; hysteresis carries into Aug 31 | `due_round_date` unchanged + state-machine test |
| 4 | Known-answer ~zero and look-ahead degradation hold on the 1,050-symbol data | Harness runs, trials logged |
| 5 | Universe choice follows the decision rule and is recorded | Registry event with measured numbers |
| 6 | Journal parses; counts reconcile or warn against POSITIONS.md | Fixture test |
| 7 | Below-basis brokerage lot flags; Roth lot never flags; recent buy raises wash-sale warning | Harvest fixture tests |
| 8 | Decision report contains round, calls, overlay, advisory, caveats; path is gitignored | Report test + gitignore assertion |
| 9 | Aug 31 report carries the agreement section, symbol-free in tracked output | Post-mortem fixture test |
| 10 | Every emit logs a trial | Trial count test |

## Open questions

1. **Gray-zone ruling** - resolved only by the measured numbers, tonight.
2. **Harvest minimum** - no loss-size or holding-period floor for now; the
   report shows all below-basis brokerage lots and the user filters. Add a
   floor by recorded event if the list is noisy.
3. **Journal as sole holdings source** - later, if reconciliation proves
   stable; not this spec.
