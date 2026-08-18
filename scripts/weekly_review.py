"""Weekly sell review: "should I sell anything?" from sanctioned evidence.

    uv run python scripts/weekly_review.py

Phase W of tasks/SPEC-EVENT-AWARENESS.md. One verdict per held name -
act / too-soon / punt - where `act` derives ONLY from evidence the system
already stands behind:

  - `deteriorating` newly true since the last review (down 3m, 6m and 12m);
  - the advisory call-state recompute showing a held name below the
    registry's exit percentile (advisory: nothing is emitted);
  - a loss-harvest flag (tax, not alpha);
  - a data-integrity failure on a held name (stale in silver_signals).

Everything else is `too-soon` (evidence exists but below the bar, stated)
or `punt` (nothing new). Drawdown-vs-basis is DISPLAYED per held name and
can never justify `act` - an unvalidated stop-loss overlay is the
bleed-money path (spec "Non-goals / explicit refusals").

Writes reports/weekly_YYYY-MM-DD.md (gitignored - it names real
positions) and prints it. The only warehouse write is the
`weekly_review_state` table keyed by review date, which defines "newly
true since last review"; a same-day re-run replaces its own rows, so
scheduled and manual runs behave identically. Monitor state and event
feeds are absent by design until Phases M and E build.
"""
import os
import sys
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import duckdb  # noqa: E402

from backtest.harness import CAVEATS  # noqa: E402
from common.trades import load_trades, loss_harvest, open_lots  # noqa: E402
from scoring.calls import (  # noqa: E402
    _IN_POSITION, latest_calls, read_rounds, round_scores,
)
from scoring.tiers import load_registry  # noqa: E402

WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")
REPORTS_DIR = os.path.join(ROOT, "reports")

STATE_TABLE = "weekly_review_state"


def _has(con, table):
    return con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = ?", [table]).fetchone()[0]


def prior_state(con, today):
    """symbol -> {deteriorating, exit_breach} at the most recent review
    strictly before today. Empty on the first review, so anything true
    now is newly true - the first run surfaces the standing evidence."""
    if not _has(con, STATE_TABLE):
        return {}
    row = con.execute(
        f"SELECT MAX(review_date) FROM {STATE_TABLE} "
        "WHERE review_date < ?", [str(today)]).fetchone()
    if row[0] is None:
        return {}
    return {
        sym: {"deteriorating": bool(det), "exit_breach": bool(breach)}
        for sym, det, breach in con.execute(
            f"SELECT symbol, deteriorating, exit_breach FROM {STATE_TABLE} "
            "WHERE review_date = ?", [str(row[0])]).fetchall()
    }


def held_evidence(con, registry, trades, today, rounds):
    """Per held symbol, everything a verdict may look at.

    Freshness gates held names only (the full-universe gate stays with the
    monthly build), and staleness here is evidence, not an exception - the
    review must still print for the names that are fine.
    """
    held = {}
    if not _has(con, "gold_held_positions") or \
            not _has(con, "silver_signals"):
        return held, None
    for (sym, accounts, qty, tracked, rank, call, det,
         ret_3m, ret_12m) in con.execute(
            "SELECT symbol, accounts, total_quantity, tracked, "
            "composite_rank, call, deteriorating, ret_3m, ret_12m "
            "FROM gold_held_positions ORDER BY symbol").fetchall():
        held[sym] = {
            "symbol": sym, "accounts": accounts, "quantity": qty,
            "tracked": bool(tracked), "rank": rank, "call": call,
            "deteriorating": bool(det) if det is not None else False,
            "ret_3m": ret_3m, "ret_12m": ret_12m,
            "stale": False, "exit_breach": False, "below_exit_line": False,
            "score_percentile": None, "harvest": [], "drawdown": None,
        }

    tracked_syms = [s for s, e in held.items() if e["tracked"]]
    latest = con.execute(
        "SELECT MAX(as_of_date) FROM silver_signals").fetchone()[0]
    if tracked_syms:
        placeholders = ", ".join("?" for _ in tracked_syms)
        have = dict(con.execute(
            f"SELECT symbol, MAX(as_of_date) FROM silver_signals "
            f"WHERE symbol IN ({placeholders}) GROUP BY symbol",
            tracked_syms).fetchall())
        for sym in tracked_syms:
            if have.get(sym) != latest:
                held[sym]["stale"] = True

    # Advisory recompute: the same registry-driven scoring the rounds use,
    # at the warehouse's latest session. Advisory only - no call emission,
    # no state transition recorded. A breach exists ONLY for a name the
    # call record holds in position (buy/hold): flagging every held name
    # below the exit line would make the cross-sectional median a stop
    # rule, which is the "no new sell criteria" refusal in the spec. A
    # low-scoring held name the system never bought is stated evidence,
    # not an act trigger; verdict() buckets it too-soon.
    call_state = latest_calls(rounds)
    scoring_tables = ("silver_signals", "gold_candidate_signals",
                      "bronze_entity", "bronze_security")
    if latest is not None and all(_has(con, t) for t in scoring_tables):
        exit_line = registry["calls"]["exit_percentile"]
        scores = round_scores(con, registry, [str(latest)[:10]])
        for row in scores.itertuples():
            if row.symbol in held:
                held[row.symbol]["score_percentile"] = row.score_percentile
                held[row.symbol]["below_exit_line"] = \
                    row.score_percentile < exit_line
                if (row.score_percentile < exit_line
                        and call_state.get(row.symbol) in _IN_POSITION):
                    held[row.symbol]["exit_breach"] = True

    for flag in loss_harvest(con, trades, today=today):
        if flag["symbol"] in held:
            held[flag["symbol"]]["harvest"].append(flag)

    # Drawdown-vs-basis where the journal knows the basis: display only.
    lots_by_symbol = {}
    for (account, sym), lots in open_lots(trades).items():
        lots_by_symbol.setdefault(sym, []).extend(
            lot for lot in lots if lot["price"] is not None)
    for sym, lots in lots_by_symbol.items():
        if sym not in held:
            continue
        row = con.execute(
            "SELECT close FROM bronze_prices WHERE symbol = ? "
            "ORDER BY date DESC LIMIT 1", [sym]).fetchone()
        qty = sum(lot["qty"] for lot in lots)
        if row and qty > 0:
            basis = sum(lot["qty"] * lot["price"] for lot in lots) / qty
            held[sym]["drawdown"] = row[0] / basis - 1.0

    return held, latest


def verdict(evidence, prior):
    """(verdict, [reasons]) for one held name. `act` only from the four
    sanctioned sources; drawdown never appears here at all."""
    reasons = []
    was = prior.get(evidence["symbol"], {})
    if evidence["stale"]:
        reasons.append("data integrity: stale in silver_signals - a held "
                       "name with missing data is a failure, not a silence")
    if evidence["deteriorating"] and not was.get("deteriorating"):
        reasons.append("DETERIORATING newly true (down 3m, 6m and 12m)")
    if evidence["exit_breach"]:
        tag = " (also breached last review)" if was.get("exit_breach") else ""
        reasons.append(f"below exit percentile at "
                       f"{evidence['score_percentile']:.3f} - advisory "
                       f"recompute, no call emitted{tag}")
    for flag in evidence["harvest"]:
        term = "long-term" if flag["long_term"] else "short-term"
        wash = "; WASH SALE - bought within 30 days" if flag["wash_sale"] \
            else ""
        reasons.append(f"loss harvest (tax, not alpha): {flag['qty']:g} sh "
                       f"@ {flag['basis']:.2f} basis, now "
                       f"{flag['price']:.2f} ({flag['loss_pct']:+.1%}, "
                       f"{term}){wash}")
    if reasons:
        return "act", reasons

    if evidence["deteriorating"]:
        return "too-soon", ["deteriorating, already surfaced at a prior "
                            "review - standing evidence, no new trigger"]
    if evidence["below_exit_line"]:
        return "too-soon", [f"ranks below the exit line at "
                            f"{evidence['score_percentile']:.3f} but the "
                            "system never issued a buy - no call state to "
                            "exit; a watchlist-discretionary holding"]
    ret_12m = evidence["ret_12m"]
    if ret_12m is not None and ret_12m < 0:
        return "too-soon", [f"down {ret_12m:+.1%} over 12m but not over "
                            "3m, 6m and 12m together - below the "
                            "deteriorating bar"]
    return "punt", ["nothing new"]


def build_review(con, registry, trades, today, rounds):
    """The report markdown plus the state rows to record. Pure over its
    inputs (rounds included) so fixtures can pin every verdict path."""
    held, latest = held_evidence(con, registry, trades, today, rounds)
    prior = prior_state(con, today)

    verdicts = {sym: verdict(e, prior) for sym, e in held.items()}

    lines = [f"# Weekly review - {today}", ""]
    vintage = str(latest)[:10] if latest is not None else "unknown"
    lines.append(f"Warehouse vintage {vintage}; "
                 f"{len(held)} held names reviewed.")
    if rounds:
        r = rounds[-1]
        lines.append(f"Standing round {r['as_of_date']}, methodology "
                     f"{r['methodology_version']}.")
    lines.append("")

    for state, title in (("act", "Act"), ("too-soon", "Too soon"),
                         ("punt", "Punt")):
        picked = sorted(s for s, (v, _) in verdicts.items() if v == state)
        lines.append(f"## {title} ({len(picked)})")
        if state == "punt":
            lines.append(", ".join(picked) if picked else "- none")
        else:
            for sym in picked:
                for reason in verdicts[sym][1]:
                    lines.append(f"- {sym}: {reason}")
            if not picked:
                lines.append("- none")
        lines.append("")

    lines.append("## Context per held name (display only, never "
                 "verdict-driving)")
    for sym in sorted(held):
        e = held[sym]
        bits = [f"{e['accounts']}, {e['quantity']:g} sh"]
        bits.append(f"rank {e['rank']}" if e["rank"] is not None
                    else ("rank only" if e["tracked"] else "screens only"))
        if e["call"]:
            bits.append(f"call {e['call']}")
        if e["score_percentile"] is not None:
            bits.append(f"percentile {e['score_percentile']:.3f}")
        if e["drawdown"] is not None:
            bits.append(f"vs basis {e['drawdown']:+.1%}")
        lines.append(f"- {sym}: " + ", ".join(bits))
    if not held:
        lines.append("- no gold_held_positions table (run build_local.py)")
    lines.append("")

    lines.append("Monitor state: not built (Phase M). Event feeds: not "
                 "built (Phase E).")
    lines += ["", CAVEATS, ""]

    state_rows = [(str(today), sym, e["deteriorating"], e["exit_breach"])
                  for sym, e in sorted(held.items())]
    return "\n".join(lines), state_rows


def record_state(con, today, state_rows):
    """Replace today's rows only - the review's single warehouse write."""
    con.execute(f"CREATE TABLE IF NOT EXISTS {STATE_TABLE} ("
                "review_date DATE, symbol VARCHAR, "
                "deteriorating BOOLEAN, exit_breach BOOLEAN)")
    con.execute(f"DELETE FROM {STATE_TABLE} WHERE review_date = ?",
                [str(today)])
    con.executemany(f"INSERT INTO {STATE_TABLE} VALUES (?, ?, ?, ?)",
                    state_rows)


def main():
    con = duckdb.connect(WAREHOUSE)
    registry = load_registry()
    today = date.today()
    report, state_rows = build_review(con, registry, load_trades(), today,
                                      read_rounds())
    record_state(con, today, state_rows)
    con.close()
    os.makedirs(REPORTS_DIR, exist_ok=True)
    path = os.path.join(REPORTS_DIR, f"weekly_{today}.md")
    with open(path, "w") as f:
        f.write(report)
    print(report)
    print(f"written: {path}")


if __name__ == "__main__":
    main()
