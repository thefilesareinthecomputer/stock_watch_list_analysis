"""One rebalance round: settle -> report -> emit, in that order, always.

    uv run python scripts/rebalance.py
    uv run python scripts/rebalance.py --off-cycle

The ordering is the mechanism (SPEC-BUY-SELL-CALLS "Post-mortem, before
every round"): every gradeable prior vintage is settled and reported
before a new round may be emitted, and any settlement failure aborts the
run before the emit - the refusal is deliberate, not defensive.

Cadence guards: a round is emitted only for the last trading session of a
COMPLETED month (or today when today closes the month), never before the
registry's first_round_month - the paper record starts prospectively and
inherits nothing. `--off-cycle` (SPEC-FIRST-ACTIONABLE-ROUND) emits for
the latest banked session instead, and only when the registry records an
`authorize_off_cycle` event naming exactly that vintage - an off-cadence
emit is a recorded decision, never a habit.
"""
import argparse
import os
import sys
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import duckdb  # noqa: E402

from backtest.harness import CAVEATS  # noqa: E402
from backtest.postmortem import (  # noqa: E402
    drift_state, journal_agreement, write_report,
)
from backtest.settlement import settle  # noqa: E402
from backtest.trials import TRIAL_LOG, log_trial  # noqa: E402
from common.run_context import new_run_id, now_ts  # noqa: E402
from common.trades import load_trades  # noqa: E402
from scoring.calls import (  # noqa: E402
    CALLS_LOG, build_round, emit_round, latest_calls, load_gold_calls,
    read_rounds, round_scores,
)
from scoring.expectations import freeze_expectation  # noqa: E402
from scoring.tiers import load_registry  # noqa: E402

WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")


def due_round_date(con, calls_cfg, today):
    """The as_of_date this run may emit for, or None with the reason.

    The latest session of the most recent completed month; the current
    month qualifies only on its final calendar day (month-end evening,
    after data lands). Anything before first_round_month is refused -
    emitting an earlier vintage now would be a backdated call.
    """
    row = con.execute(
        "SELECT MAX(as_of_date) FROM silver_signals "
        "WHERE DATE_TRUNC('month', as_of_date) < DATE_TRUNC('month', "
        "CAST(? AS DATE))", [str(today)]).fetchone()
    candidate = row[0]
    next_month = (date(today.year + (today.month == 12),
                       today.month % 12 + 1, 1))
    if (next_month - today).days == 1:  # today closes the month
        current = con.execute(
            "SELECT MAX(as_of_date) FROM silver_signals "
            "WHERE DATE_TRUNC('month', as_of_date) = DATE_TRUNC('month', "
            "CAST(? AS DATE))", [str(today)]).fetchone()[0]
        candidate = max(c for c in (candidate, current) if c is not None) \
            if (candidate or current) else None
    if candidate is None:
        return None, "no completed month in silver_signals"
    round_month = str(candidate)[:7]
    if round_month < calls_cfg["first_round_month"]:
        return None, (f"round {str(candidate)[:10]} predates "
                      f"first_round_month {calls_cfg['first_round_month']} - "
                      "the paper record starts prospectively")
    return str(candidate)[:10], None


def off_cycle_round_date(con, registry):
    """The vintage an --off-cycle run may emit for, or None with the reason.

    The latest banked session, whatever the calendar - but only when the
    registry records an `authorize_off_cycle` event naming exactly that
    vintage. Single-use is automatic: append-only first-write-wins makes a
    second emit of the same vintage a no-op.
    """
    row = con.execute("SELECT MAX(as_of_date) FROM silver_signals").fetchone()
    if row[0] is None:
        return None, "silver_signals is empty"
    vintage = str(row[0])[:10]
    calls_cfg = registry["calls"]
    if vintage[:7] < calls_cfg["first_round_month"]:
        return None, (f"round {vintage} predates first_round_month "
                      f"{calls_cfg['first_round_month']} - the paper record "
                      "starts prospectively")
    authorized = any(
        e.get("action") == "authorize_off_cycle"
        and e.get("vintage") == vintage
        for e in registry.get("events", []))
    if not authorized:
        return None, (f"no authorize_off_cycle event for vintage {vintage} - "
                      "an off-cycle round requires a recorded registry event")
    return vintage, None


def run_rebalance(con, registry, today, calls_path=CALLS_LOG,
                  report_dir=None, decay_path=None, trial_log=TRIAL_LOG,
                  off_cycle=False, trades=()):
    """The full round. Returns a status dict; raises when settlement or
    reporting fails, which by construction prevents the emit."""
    rounds = read_rounds(calls_path)

    # 1. Settle every gradeable vintage. A failure here raises and nothing
    #    downstream happens - emit is refused, not skipped.
    settlements = settle(con, rounds)

    # 2. Report, immutably, dated today. Symbol-free journal agreement
    #    rides along when a prior round and journaled actions exist.
    drift = drift_state(settlements, registry["calls"]["drift"])
    agreement = journal_agreement(rounds[-1], trades) \
        if rounds and trades else None
    report_kwargs = {"directory": report_dir} if report_dir else {}
    report_written = write_report(str(today), settlements, drift,
                                  agreement=agreement, **report_kwargs)

    # 3. Emit, if a round is due and not already recorded.
    if off_cycle:
        round_date, reason = off_cycle_round_date(con, registry)
    else:
        round_date, reason = due_round_date(con, registry["calls"], today)
    if round_date is None:
        return {"settled": len(settlements), "report_written": report_written,
                "emitted": False, "reason": reason}

    expectation = freeze_expectation(registry, decay_path=decay_path)
    scores = round_scores(con, registry, [round_date])
    # Symbols missing this session vanish from the round silently (the
    # exact-date join); a Yahoo hole shrinks coverage without failing
    # anything. Make the shrinkage visible so it is a decision, not a
    # surprise - only held names hard-gate, by design.
    banked = con.execute(
        "SELECT COUNT(DISTINCT symbol) FROM silver_signals").fetchone()[0]
    at_date = con.execute(
        "SELECT COUNT(DISTINCT symbol) FROM silver_signals "
        "WHERE as_of_date = CAST(? AS TIMESTAMP)", [round_date]).fetchone()[0]
    entry = build_round(scores, latest_calls(rounds), registry, expectation,
                        new_run_id(), now_ts())
    if off_cycle:
        entry["off_cycle"] = True
    log_trial("gold_calls", f"round:{round_date}", [21, 63, 126], 0.0,
              ("off-cycle " if off_cycle else "") +
              "rebalance round emit: hysteresis calls with frozen "
              "expectation", path=trial_log)
    emitted = emit_round(entry, path=calls_path)
    if emitted:
        load_gold_calls(con, path=calls_path)
    return {"settled": len(settlements), "report_written": report_written,
            "emitted": emitted, "round_date": round_date,
            "n_calls": len(entry["calls"]),
            "session_coverage": (at_date, banked),
            "reason": None if emitted else "round already recorded"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--off-cycle", action="store_true",
                        help="emit for the latest banked session; requires "
                             "a recorded authorize_off_cycle registry event")
    args = parser.parse_args()

    registry = load_registry()
    con = duckdb.connect(WAREHOUSE)
    status = run_rebalance(con, registry, date.today(),
                           off_cycle=args.off_cycle, trades=load_trades())
    con.close()

    print(f"settled vintages: {status['settled']}")
    print(f"post-mortem report written: {status['report_written']}")
    if status["emitted"]:
        buys = status["n_calls"]
        print(f"emitted round {status['round_date']} ({buys} calls) "
              "-> calls_log.jsonl, gold_calls reloaded")
        at_date, banked = status["session_coverage"]
        if at_date < 0.98 * banked:
            print(f"  WARNING: only {at_date}/{banked} banked symbols have "
                  f"this session - the rest kept their prior call state "
                  "(data hole, not a signal)")
    else:
        print(f"no round emitted: {status['reason']}")
    print(f"\n{CAVEATS}")


if __name__ == "__main__":
    main()
