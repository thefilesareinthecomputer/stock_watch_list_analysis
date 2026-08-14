"""Decision report: everything the system knows, on one private page.

    uv run python scripts/decide.py

Reads the latest recorded round (calls_log.jsonl), the held tier, the
line-of-sight screens and the trade journal; writes
reports/decision_YYYY-MM-DD.md (gitignored - it names symbols and real
positions) and prints it. Read-only against the warehouse: emitting is
rebalance.py's job, and a report without a recorded round refuses rather
than inventing one (SPEC-FIRST-ACTIONABLE-ROUND "Decision report").
"""
import os
import sys
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import duckdb  # noqa: E402

from backtest.harness import CAVEATS  # noqa: E402
from common.trades import load_trades, loss_harvest  # noqa: E402
from scoring.calls import read_rounds  # noqa: E402

WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")
REPORTS_DIR = os.path.join(ROOT, "reports")


def _has(con, table):
    return con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = ?", [table]).fetchone()[0]


def _call_lines(calls, state):
    picked = sorted((c for c in calls if c["call"] == state),
                    key=lambda c: c["rank"])
    lines = []
    for c in picked:
        comps = ", ".join(f"{k[:-4]} {v:.2f}"
                          for k, v in c["component_percentiles"].items())
        lines.append(f"- {c['symbol']}: rank {c['rank']}, "
                     f"percentile {c['score_percentile']:.3f} ({comps})")
    return lines


def build_report(con, rounds, trades, today):
    """The report markdown. Raises when no round is recorded - a decision
    page without a durable round behind it would be the advisory-only
    design the user explicitly rejected."""
    if not rounds:
        raise ValueError("no recorded round in calls_log.jsonl - run "
                         "scripts/rebalance.py first")
    r = rounds[-1]
    tag = " (OFF-CYCLE)" if r.get("off_cycle") else ""
    lines = [f"# Decision report - {today}", ""]
    lines.append(f"Round {r['as_of_date']}{tag}, methodology "
                 f"{r['methodology_version']}, {len(r['calls'])} symbols "
                 "scored. Action models at the next open.")
    lines.append("")
    lines.append("Frozen expectation, top-decile net excess after the "
                 f"{r['expectation']['haircut']:.0%} out-of-sample haircut:")
    for h, e in sorted(r["expectation"]["horizons"].items(), key=lambda kv: int(kv[0])):
        cut = e["excess_net_haircut"]
        lines.append(f"- {h} sessions: mean {cut['mean']:+.2%}, "
                     f"p10 {cut['p10']:+.2%}, p90 {cut['p90']:+.2%}")

    buys = _call_lines(r["calls"], "buy")
    sells = _call_lines(r["calls"], "sell")
    holds = sorted(c["symbol"] for c in r["calls"] if c["call"] == "hold")
    n_none = sum(1 for c in r["calls"] if c["call"] == "none")
    lines += ["", f"## Calls: {len(buys)} buy, {len(holds)} hold, "
                  f"{len(sells)} sell, {n_none} none"]
    if buys:
        lines += ["", "Buy (entered this round):"] + buys
    if sells:
        lines += ["", "Sell (exited this round):"] + sells
    if holds:
        lines += ["", "Hold: " + ", ".join(holds)]

    lines += ["", "## Held positions"]
    if _has(con, "gold_held_positions"):
        held = con.execute(
            "SELECT symbol, accounts, total_quantity, tracked, "
            "composite_rank, call, deteriorating "
            "FROM gold_held_positions "
            "ORDER BY deteriorating DESC NULLS LAST, "
            "composite_rank NULLS LAST, symbol").fetchall()
        for sym, accounts, qty, tracked, rank, call, det in held:
            stance = call or ("rank only" if tracked else "screens only")
            det_tag = " DETERIORATING (down 3m, 6m, 12m)" if det else ""
            rank_tag = f", rank {rank}" if rank is not None else ""
            lines.append(f"- {sym} ({accounts}, {qty:g} sh): {stance}"
                         f"{rank_tag}{det_tag}")
    else:
        lines.append("- no gold_held_positions table (run build_local.py)")

    harvest = loss_harvest(con, trades, today=today)
    lines += ["", "## Loss harvest (taxable accounts, below basis)"]
    if harvest:
        for f in harvest:
            term = "long-term" if f["long_term"] else "short-term"
            det_tag = ", deteriorating" if f["deteriorating"] else ""
            wash = ("; WASH SALE - bought within 30 days"
                    if f["wash_sale"] else "")
            lines.append(f"- {f['symbol']} ({f['account']}, {f['qty']:g} sh "
                         f"@ {f['basis']:.2f}, now {f['price']:.2f}, "
                         f"{f['loss_pct']:+.1%}, {term}, acquired "
                         f"{f['acquired']}{det_tag}){wash}")
        lines.append("Reminder: do not rebuy a harvested symbol for 30 "
                     "days, in any account.")
    else:
        lines.append("- nothing below basis (or no journaled taxable lots)")

    lines += ["", "## Advisory - UNVALIDATED SCREEN, not calls"]
    if _has(con, "gold_line_of_sight"):
        emerging = [row[0] for row in con.execute(
            "SELECT symbol FROM gold_line_of_sight WHERE emerging "
            "ORDER BY mom_pct DESC").fetchall()]
        lines.append(f"Emerging ({len(emerging)}, absolute-confirmed "
                     "momentum + dollar-volume acceleration): "
                     + ", ".join(emerging))
    else:
        lines.append("- no gold_line_of_sight table")

    lines += ["", CAVEATS, ""]
    return "\n".join(lines)


def main():
    con = duckdb.connect(WAREHOUSE, read_only=True)
    report = build_report(con, read_rounds(), load_trades(), date.today())
    con.close()
    os.makedirs(REPORTS_DIR, exist_ok=True)
    path = os.path.join(REPORTS_DIR, f"decision_{date.today()}.md")
    with open(path, "w") as f:
        f.write(report)
    print(report)
    print(f"written: {path}")


if __name__ == "__main__":
    main()
