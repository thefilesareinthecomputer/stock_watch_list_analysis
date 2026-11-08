"""Record a fill: one command, both private files.

    uv run python scripts/trade.py buy  PANW 3 --account "BROKERAGE STOCK"
    uv run python scripts/trade.py sell GDS 10 --account "BROKERAGE STOCK" --price 18.2
    uv run python scripts/trade.py seed          # install the _relay.md basis lots
    uv run python scripts/trade.py show          # open lots vs POSITIONS.md

`buy`/`sell` append one line to trades.jsonl (the transaction record) and
apply the same shares to knowledge/POSITIONS.md (the holdings snapshot)
so the two never drift by hand. Both are validated before either is
written. Price is optional on live fills - the day's close is knowable
from the date (common.trades) - and date defaults to today. A symbol the
warehouse has never seen is a warning, not a refusal: funds like VFIAX
are held but never scored.

`seed` installs the one-time basis lots from the fenced jsonl block in
_relay.md: every line must carry seed=true and a real price, the lot
quantities must match the snapshot, and it refuses to run twice.
"""
import argparse
import json
import math
import os
import re
import sys
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from common.corporate_actions import split_factor, splits_for  # noqa: E402
from common.jsonl import append_jsonl  # noqa: E402
from common.positions import (  # noqa: E402
    POSITIONS_PATH, apply_fill, load_positions,
)
from common.trades import (  # noqa: E402
    TRADES_LOG, load_trades, open_lots, parse_trades, reconcile,
)

WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")
RELAY = os.path.join(ROOT, "_relay.md")


def _current_terms_factor(symbol, when, price, warehouse):
    """Shares-per-share factor from the fill date's share terms to current
    terms, from the corporate_actions log (1.0 without a warehouse). Also
    warns when a recorded price sits far from that session's close: the
    likeliest cause is a price typed in the wrong share terms or date."""
    if not os.path.exists(warehouse):
        return 1.0
    import duckdb
    con = duckdb.connect(warehouse, read_only=True)
    try:
        splits = splits_for(con, symbol)
        factor = split_factor(splits, when)
        if price is not None:
            row = con.execute(
                "SELECT date, close FROM bronze_prices WHERE symbol = ? "
                "AND date <= ? ORDER BY date DESC LIMIT 1", [symbol, when]).fetchone()
            if row and row[1] and abs(math.log(price / row[1])) > math.log(1.25):
                print(f"warning: {symbol} price {price:g} is far from the "
                      f"{row[0]} close {row[1]:.2f} - check the share terms "
                      "and the date before trusting this lot's basis")
    finally:
        con.close()
    if factor != 1.0:
        print(f"note: {symbol} split {factor:g}-for-1 after {when}; the journal "
              f"keeps the fill as it happened and the snapshot moves by the "
              f"quantity times {factor:g}")
    return factor


def record(side, symbol, qty, account, when, price=None, note="",
           trades_path=TRADES_LOG, positions_path=POSITIONS_PATH,
           warehouse=WAREHOUSE):
    """Validate, then write journal line + snapshot update. Returns the
    journal entry. Nothing is written if either validation fails.

    The journal line is the fill as it happened, in its date's share
    terms; the snapshot is in current terms, so a fill dated before a
    logged split moves the snapshot by the split-restated quantity."""
    entry = {"date": when, "symbol": symbol.upper(), "account": account,
             "side": side, "qty": qty}
    if price is not None:
        entry["price"] = price
    if note:
        entry["note"] = note
    parse_trades([json.dumps(entry)])
    factor = _current_terms_factor(entry["symbol"], when, price, warehouse)
    text = open(positions_path).read() if os.path.exists(positions_path) else ""
    delta = (qty if side == "buy" else -qty) * factor
    new_text = apply_fill(text, account, entry["symbol"], delta)
    append_jsonl(trades_path, [entry], sort_keys=False)
    with open(positions_path, "w") as f:
        f.write(new_text)
    return entry


def seed(relay_path=RELAY, trades_path=TRADES_LOG,
         positions_path=POSITIONS_PATH):
    """Install the basis lots from _relay.md's jsonl block; returns the
    count. Refuses placeholders, non-seed lines, quantities that disagree
    with POSITIONS.md, and a second run."""
    text = open(relay_path).read()
    m = re.search(r"```jsonl\n(.*?)```", text, re.S)
    if not m:
        raise ValueError(f"no ```jsonl block in {relay_path}")
    if "FILL_" in m.group(1):
        raise ValueError("FILL_PRICE / FILL_DATE placeholders remain - fill "
                         "every lot before seeding")
    entries = parse_trades(m.group(1).splitlines())
    if not entries or not all(e["seed"] for e in entries):
        raise ValueError("every seed line needs \"seed\": true")
    if any(t["seed"] for t in load_trades(trades_path)):
        raise ValueError("trades.jsonl already holds seed lots - seeding "
                         "is one-time; append live fills with buy/sell")
    positions = load_positions(positions_path)
    bad = reconcile(entries, positions)
    if bad:
        raise ValueError("seed lots disagree with the snapshot: "
                         + "; ".join(bad))
    raw = [json.loads(l) for l in m.group(1).splitlines() if l.strip()]
    append_jsonl(trades_path, raw, sort_keys=False)
    return len(raw)


def _warn_unknown(symbol):
    if not os.path.exists(WAREHOUSE):
        return
    import duckdb
    con = duckdb.connect(WAREHOUSE, read_only=True)
    n = con.execute("SELECT COUNT(*) FROM bronze_prices WHERE symbol = ?",
                    [symbol.upper()]).fetchone()[0]
    con.close()
    if not n:
        print(f"warning: {symbol.upper()} is not in the warehouse - held "
              "but never scored; check the ticker if that is unexpected")


def show():
    con = None
    if os.path.exists(WAREHOUSE):
        import duckdb
        con = duckdb.connect(WAREHOUSE, read_only=True)
    try:
        trades = load_trades(TRADES_LOG, con=con)   # fills in current terms
    finally:
        if con is not None:
            con.close()
    positions = load_positions(POSITIONS_PATH)
    for (account, symbol), lots in sorted(open_lots(trades).items()):
        desc = ", ".join(
            f"{l['qty']:g} sh @ {l['price'] if l['price'] is not None else 'close'}"
            f" {l['date']}{' seed' if l['seed'] else ''}" for l in lots)
        print(f"{account} {symbol}: {desc}")
    for w in reconcile(trades, positions):
        print(f"warning: {w}")
    print(f"{len(trades)} journal entries, {len(positions)} snapshot lines")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    for side in ("buy", "sell"):
        p = sub.add_parser(side)
        p.add_argument("symbol")
        p.add_argument("qty", type=float)
        p.add_argument("--account", required=True)
        p.add_argument("--price", type=float)
        p.add_argument("--date", default=str(date.today()))
        p.add_argument("--note", default="")
    sub.add_parser("seed")
    sub.add_parser("show")
    args = parser.parse_args(argv)
    if args.cmd == "seed":
        print(f"seeded {seed()} lots into trades.jsonl")
    elif args.cmd == "show":
        show()
    else:
        _warn_unknown(args.symbol)
        e = record(args.cmd, args.symbol, args.qty, args.account, args.date,
                   price=args.price, note=args.note,
                   trades_path=TRADES_LOG, positions_path=POSITIONS_PATH)
        print(f"recorded {e['side']} {e['qty']:g} {e['symbol']} "
              f"({e['account']}) on {e['date']} -> trades.jsonl, POSITIONS.md")


if __name__ == "__main__":
    main()
