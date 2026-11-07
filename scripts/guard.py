"""Daily portfolio guard: which holdings need the user's attention.

Spec: tasks/SPEC-GUARD.md (private). Reads the warehouse (split-adjusted
daily closes for every holding) and knowledge/POSITIONS.md; writes a
private report to reports/guard_<date>.md and appends alert episodes to
guard_events.jsonl (gitignored, outside the regenerable warehouse).

It informs and never instructs: every alert carries its context (speed
against the name's own swings, move against the market, confirmation,
the engine's read, sector exposure) so the decision stays with the user.
Delivery (vault note + iMessage ping) is a later slice.

    uv run python scripts/guard.py                    # today's report
    uv run python scripts/guard.py --dry-run          # report, no events
    uv run python scripts/guard.py --replay 2026-10-06 2026-11-05
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402

from common.guard import (  # noqa: E402
    alert_factors, append_events, evaluate_holdings, fold_state,
    load_events, next_events,
)
from common.positions import load_positions  # noqa: E402

WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")
EVENTS = os.path.join(ROOT, "guard_events.jsonl")
REPORTS = os.path.join(ROOT, "reports")
MARKET = "SPY"
FUNDS = "Funds (ETF / mutual fund)"


def _has(con, table):
    return con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table]).fetchone()[0]


def _quantities(positions):
    qty = {}
    for p in positions:
        sym = p["symbol"].upper()
        qty[sym] = qty.get(sym, 0.0) + p["quantity"]
    return qty


def _prices(con, symbols):
    """Split-adjusted closes, one column per symbol plus the market."""
    wanted = sorted(set(symbols) | {MARKET})
    marks = ", ".join("?" for _ in wanted)
    df = con.execute(
        f"SELECT symbol, date, adj_close FROM silver_adjusted_prices "
        f"WHERE symbol IN ({marks})", wanted).df()
    px = df.pivot(index="date", columns="symbol", values="adj_close")
    px.index = pd.to_datetime(px.index)
    return px.sort_index()


def _lookup(con, table, column):
    if not _has(con, table):
        return {}
    return dict(con.execute(
        f"SELECT symbol, {column} FROM {table}").fetchall())


def _engine(con):
    if not _has(con, "gold_held_positions"):
        return {}
    return {r[0]: (r[1], r[2]) for r in con.execute(
        "SELECT symbol, composite_rank, deteriorating "
        "FROM gold_held_positions").fetchall()}


def _context(row, px, engine, limits):
    if "stale" in row["alerts"]:
        # Any figure from a dead series would describe the past as today.
        return ("no price for 2+ sessions - check the broker for a "
                "delisting or rename")
    bits = []
    if row["drawdown"] is not None:
        bits.append(f"down {-row['drawdown']:.1%} from its 6-month high, "
                    f"${row['lost_from_high']:,.0f} below that high, "
                    f"{row['weight']:.1%} of the book")
    series = px[row["symbol"]].dropna() if row["symbol"] in px else None
    if series is not None and len(series) >= 60 and MARKET in px:
        f = alert_factors(series, px[MARKET], limits)
        speed = (f" ({f['speed_sigma']:+.1f} sigma vs its own swings)"
                 if f["speed_sigma"] is not None else "")
        bits.append(f"21-day move {f['ret_21d']:+.1%}{speed}, "
                    f"vs market {f['vs_market_21d']:+.1%}")
        bits.append("held past the line two closes running"
                    if f["confirmed"] else "a single close past the line")
    rank, deteriorating = engine.get(row["symbol"], (None, None))
    if rank is not None:
        bits.append(f"engine rank {rank}")
    if deteriorating:
        bits.append("down over 3, 6 and 12 months")
    return "; ".join(bits)


def build_guard(con, positions, events, today, limits=None, px=None):
    """(report markdown, new events, [(symbol, alert) firing today])."""
    qty = _quantities(positions)
    px = _prices(con, qty) if px is None else px
    px = px[px.index <= pd.Timestamp(today)]
    rows = evaluate_holdings(px, qty, _lookup(con, "bronze_security",
                                               "quote_type"), limits)
    new_events, firing = next_events(rows, fold_state(events), today, limits)
    engine = _engine(con)
    fired_now = {sym for sym, _ in firing}

    total = sum(r["value"] for r in rows if r["value"] is not None)
    lines = [f"# Portfolio guard - {today}", "",
             f"Prices through {px.index[-1].date()}; {len(rows)} holdings; "
             f"book ${total:,.0f}. This report informs; every decision "
             "stays with you.", ""]
    lines.append("New today: " + (", ".join(
        f"{sym} ({alert})" for sym, alert in firing)
        if firing else "no new alerts"))
    lines += ["", "## Needs attention"]
    flagged = sorted((r for r in rows if r["alerts"]),
                     key=lambda r: -(r["lost_from_high"] or 0))
    for row in flagged:
        status = "new" if row["symbol"] in fired_now else "ongoing"
        lines.append(f"- {row['symbol']} [{', '.join(row['alerts'])}; "
                     f"{status}]: {_context(row, px, engine, limits)}")
    if not flagged:
        lines.append("- none")

    sectors = _lookup(con, "bronze_entity", "sic_description")
    qtypes = _lookup(con, "bronze_security", "quote_type")
    exposure = {}
    for row in rows:
        if row["value"] is None:
            continue
        group = (FUNDS if qtypes.get(row["symbol"]) in ("ETF", "MUTUALFUND")
                 else sectors.get(row["symbol"]) or "Unclassified")
        exposure[group] = exposure.get(group, 0.0) + row["value"]
    lines += ["", "## Exposure by sector"]
    for group, value in sorted(exposure.items(), key=lambda kv: -kv[1]):
        lines.append(f"- {group}: ${value:,.0f} ({value / total:.1%})")
    return "\n".join(lines) + "\n", new_events, firing


def replay(con, positions, start, end, limits=None):
    """[(symbol, alert, date)] the guard would have fired, session by
    session, with today's quantities (a labeled approximation, R20)."""
    qty = _quantities(positions)
    px = _prices(con, qty)
    events, fires = [], []
    for day in px.index[(px.index >= pd.Timestamp(start))
                        & (px.index <= pd.Timestamp(end))]:
        today = str(day.date())
        _, new, firing = build_guard(con, positions, events, today,
                                     limits, px=px)
        events += new
        fires += [(sym, alert, today) for sym, alert in firing]
    return fires


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true",
                        help="write the report but record no events")
    parser.add_argument("--replay", nargs=2, metavar=("START", "END"),
                        help="print what would have fired; writes nothing")
    args = parser.parse_args()

    positions = load_positions()
    if not positions:
        sys.exit("no knowledge/POSITIONS.md - nothing to guard")
    con = duckdb.connect(WAREHOUSE, read_only=True)
    if args.replay:
        for sym, alert, day in replay(con, positions, *args.replay):
            print(f"{day}  {sym:6} {alert}")
        return
    today = str(con.execute(
        "SELECT MAX(date) FROM silver_adjusted_prices").fetchone()[0])[:10]
    report, events, firing = build_guard(con, positions, load_events(EVENTS),
                                         today)
    os.makedirs(REPORTS, exist_ok=True)
    path = os.path.join(REPORTS, f"guard_{today}.md")
    with open(path, "w") as f:
        f.write(report)
    if not args.dry_run:
        append_events(EVENTS, events)
    print(f"{path}: {len(firing)} new alert(s)"
          + ("" if not args.dry_run else " (dry run, no events recorded)"))


if __name__ == "__main__":
    main()
