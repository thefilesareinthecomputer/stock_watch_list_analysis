"""Trade journal: shares in and out, nothing else - trades.jsonl parsed.

Append-only, gitignored (it names real positions), private. One JSON line
per fill: {date, symbol, account, side, qty[, price][, note][, seed]}.
No broker integration and no balance tracking by design
(SPEC-FIRST-ACTIONABLE-ROUND): values over time come from prices the
warehouse already holds, so share counts and dates are the whole record.

Seed entries (seed: true) are the one-time basis backfill for lots that
predate the journal - basis context for the loss-harvest screen, never
tracked decisions - which is why a seed requires an explicit price while
a live entry may omit it (the day's close is knowable from the date).

POSITIONS.md stays the holdings snapshot; the journal is the transaction
record. Reconciliation between them is a warning, never a build failure.
Malformed entries fail loudly, same argument as positions.parse_positions:
a silently skipped fill would corrupt every lot computed after it.
"""
import json
import os
from datetime import date, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TRADES_LOG = os.path.join(ROOT, "trades.jsonl")

SIDES = ("buy", "sell")
_QTY_EPS = 1e-9

# Loss harvesting only makes sense where a realized loss deducts something:
# accounts whose name matches any of these are tax-advantaged and excluded.
TAX_ADVANTAGED = ("roth", "ira", "401", "hsa")

WASH_SALE_DAYS = 30
LONG_TERM_DAYS = 365


def parse_trades(lines):
    """[{date, symbol, account, side, qty, price, note, seed}] in file
    order, validated. Raises ValueError naming the line on any defect."""
    trades = []
    for lineno, raw in enumerate(lines, 1):
        line = raw.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"trades.jsonl line {lineno}: not JSON ({exc})")
        missing = [k for k in ("date", "symbol", "account", "side", "qty")
                   if k not in entry]
        if missing:
            raise ValueError(f"trades.jsonl line {lineno}: missing "
                             f"{', '.join(missing)}")
        try:
            date.fromisoformat(entry["date"])
        except (TypeError, ValueError):
            raise ValueError(f"trades.jsonl line {lineno}: date must be "
                             f"YYYY-MM-DD, got {entry['date']!r}")
        if entry["side"] not in SIDES:
            raise ValueError(f"trades.jsonl line {lineno}: side must be "
                             f"buy or sell, got {entry['side']!r}")
        qty = float(entry["qty"])
        if qty <= 0:
            raise ValueError(f"trades.jsonl line {lineno}: qty must be "
                             f"positive, got {qty}")
        price = entry.get("price")
        if price is not None and float(price) <= 0:
            raise ValueError(f"trades.jsonl line {lineno}: price must be "
                             f"positive, got {price}")
        seed = bool(entry.get("seed", False))
        if seed and entry["side"] != "buy":
            raise ValueError(f"trades.jsonl line {lineno}: a seed entry is "
                             "an existing lot - side must be buy")
        if seed and price is None:
            raise ValueError(f"trades.jsonl line {lineno}: a seed entry "
                             "needs its approximate basis as price")
        trades.append({
            "date": entry["date"],
            "symbol": entry["symbol"].upper(),
            "account": entry["account"].strip(),
            "side": entry["side"],
            "qty": qty,
            "price": float(price) if price is not None else None,
            "note": entry.get("note", ""),
            "seed": seed,
        })
    return trades


def load_trades(path=TRADES_LOG):
    """Trades from disk; empty on machines without the private file."""
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return parse_trades(f.readlines())


def share_counts(trades):
    """(account, symbol) -> net share count from the journal."""
    counts = {}
    for t in trades:
        key = (t["account"], t["symbol"])
        delta = t["qty"] if t["side"] == "buy" else -t["qty"]
        counts[key] = counts.get(key, 0.0) + delta
    return counts


def reconcile(trades, positions):
    """Warnings where journal-rolled counts disagree with the POSITIONS.md
    snapshot. Positions absent from the journal are not warned - the
    journal may be younger than the holdings; seeds close that gap when
    the user wants it closed. Never raises: the snapshot stays the
    holdings source of truth (SPEC-FIRST-ACTIONABLE-ROUND)."""
    counts = share_counts(trades)
    snapshot = {(p["account"], p["symbol"]): p["quantity"]
                for p in positions}
    warnings = []
    for key, qty in sorted(counts.items()):
        if qty < -_QTY_EPS:
            warnings.append(f"{key[0]} {key[1]}: journal nets to {qty:g} "
                            "shares - more sold than bought")
        elif key in snapshot and abs(snapshot[key] - qty) > _QTY_EPS:
            warnings.append(f"{key[0]} {key[1]}: journal nets to {qty:g} "
                            f"shares, POSITIONS.md says {snapshot[key]:g}")
        elif key not in snapshot and qty > _QTY_EPS:
            warnings.append(f"{key[0]} {key[1]}: journal holds {qty:g} "
                            "shares but the symbol is not in POSITIONS.md")
    return warnings


def open_lots(trades):
    """(account, symbol) -> FIFO open lots [{qty, price, date, seed}].

    Sells consume the oldest lots first. Overselling raises: a sell with
    no lot behind it means the lot predates the journal and needs a seed
    entry first - a silent negative lot would fabricate a basis."""
    lots = {}
    for t in sorted(trades, key=lambda t: t["date"]):
        key = (t["account"], t["symbol"])
        if t["side"] == "buy":
            lots.setdefault(key, []).append(
                {"qty": t["qty"], "price": t["price"], "date": t["date"],
                 "seed": t["seed"]})
            continue
        remaining = t["qty"]
        queue = lots.get(key, [])
        while remaining > _QTY_EPS:
            if not queue:
                raise ValueError(
                    f"{key[0]} {key[1]}: sell of {t['qty']:g} on "
                    f"{t['date']} exceeds journaled lots - seed the "
                    "pre-journal lot first")
            lot = queue[0]
            take = min(lot["qty"], remaining)
            lot["qty"] -= take
            remaining -= take
            if lot["qty"] <= _QTY_EPS:
                queue.pop(0)
    return {k: v for k, v in lots.items() if v}


def _is_taxable(account):
    lowered = account.lower()
    return not any(marker in lowered for marker in TAX_ADVANTAGED)


def _latest_prices(con):
    """symbol -> (close, deteriorating), bronze_prices price-authoritative.

    gold_line_of_sight covers every broad symbol and carries the sell-side
    flag, but it derives from the screening table, whose rows can be up to
    RESUME_MAX_AGE_DAYS stale between refreshes. bronze_prices is the
    evidence contract and fresher for tracked names, so where both exist
    the bronze close overrides and the sight flag is kept.
    """
    prices = {}
    have_sight = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'gold_line_of_sight'").fetchone()[0]
    if have_sight:
        for sym, close, det in con.execute(
                "SELECT symbol, close, deteriorating "
                "FROM gold_line_of_sight").fetchall():
            prices[sym] = (close, bool(det))
    for sym, close in con.execute("""
            SELECT symbol, close FROM (
                SELECT symbol, close, ROW_NUMBER() OVER (
                    PARTITION BY symbol ORDER BY date DESC) AS rn
                FROM bronze_prices) WHERE rn = 1""").fetchall():
        prices[sym] = (close, prices[sym][1] if sym in prices else None)
    return prices


def _basis(con, lot, key):
    """A lot's per-share basis: its recorded price, else the close on its
    trade date (the journal's whole design - prices are knowable from
    dates). None when neither exists."""
    if lot["price"] is not None:
        return lot["price"]
    row = con.execute(
        "SELECT close FROM bronze_prices WHERE symbol = ? AND date <= ? "
        "ORDER BY date DESC LIMIT 1", [key[1], lot["date"]]).fetchone()
    return row[0] if row else None


def loss_harvest(con, trades, today=None):
    """Below-basis open lots in taxable accounts: the harvest screen.

    Returns [{account, symbol, qty, basis, price, loss_pct, acquired,
    long_term, deteriorating, wash_sale}] sorted worst loss first. Context
    for a human sell decision, never an automatic call
    (SPEC-FIRST-ACTIONABLE-ROUND). Tax-advantaged accounts are excluded -
    losses there deduct nothing. `wash_sale` flags a buy of the symbol in
    ANY account within the last 30 days (the IRS looks across accounts);
    every flagged lot also carries the standing do-not-rebuy reminder in
    the report layer. Seeded basis is per share AS HELD TODAY - the user
    seeds post-split numbers, which approximate basis is anyway.
    """
    today = today or date.today()
    cutoff = str(today - timedelta(days=WASH_SALE_DAYS))
    recent_buys = {t["symbol"] for t in trades
                   if t["side"] == "buy" and not t["seed"]
                   and t["date"] >= cutoff}
    prices = _latest_prices(con)
    flagged = []
    for key, lots in open_lots(trades).items():
        account, symbol = key
        if not _is_taxable(account) or symbol not in prices:
            continue
        price, deteriorating = prices[symbol]
        for lot in lots:
            basis = _basis(con, lot, key)
            if basis is None or price >= basis:
                continue
            held_days = (today - date.fromisoformat(lot["date"])).days
            flagged.append({
                "account": account, "symbol": symbol,
                "qty": lot["qty"], "basis": basis, "price": price,
                "loss_pct": price / basis - 1.0,
                "acquired": lot["date"],
                "long_term": held_days >= LONG_TERM_DAYS,
                "deteriorating": deteriorating,
                "wash_sale": symbol in recent_buys,
            })
    return sorted(flagged, key=lambda f: f["loss_pct"])
