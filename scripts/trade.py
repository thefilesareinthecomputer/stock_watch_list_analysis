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
import os
import re
import sys
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from common.positions import (  # noqa: E402
    POSITIONS_PATH, apply_fill, load_positions,
)
from common.trades import (  # noqa: E402
    TRADES_LOG, load_trades, open_lots, parse_trades, reconcile,
)

WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")
RELAY = os.path.join(ROOT, "_relay.md")


def _append(path, entries):
    with open(path, "a") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")


def record(side, symbol, qty, account, when, price=None, note="",
           trades_path=TRADES_LOG, positions_path=POSITIONS_PATH):
    """Validate, then write journal line + snapshot update. Returns the
    journal entry. Nothing is written if either validation fails."""
    entry = {"date": when, "symbol": symbol.upper(), "account": account,
             "side": side, "qty": qty}
    if price is not None:
        entry["price"] = price
    if note:
        entry["note"] = note
    parse_trades([json.dumps(entry)])
    text = open(positions_path).read() if os.path.exists(positions_path) else ""
    delta = qty if side == "buy" else -qty
    new_text = apply_fill(text, account, entry["symbol"], delta)
    _append(trades_path, [entry])
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
    _append(trades_path, raw)
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
    trades = load_trades(TRADES_LOG)
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
