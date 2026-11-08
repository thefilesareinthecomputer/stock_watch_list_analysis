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

from common.corporate_actions import split_factor, splits_for, splits_map
from common.tables import has_table

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


def load_trades(path=TRADES_LOG, con=None):
    """Trades from disk; empty on machines without the private file. With
    a warehouse connection every fill is restated to current share terms
    from the corporate_actions log (restate_to_current_terms); without one
    the fills are as journaled, in their trade dates' terms."""
    if not os.path.exists(path):
        return []
    with open(path) as f:
        trades = parse_trades(f.readlines())
    if con is not None:
        trades = restate_to_current_terms(
            trades, splits_map(con, {t["symbol"] for t in trades}))
    return trades


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
                 "seed": t["seed"], "split_factor": t.get("split_factor", 1.0),
                 "splits": t.get("splits", [])})
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
    if has_table(con, "gold_line_of_sight"):
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


def restate_to_current_terms(trades, splits_by_symbol):
    """Trades with every real fill restated to current share terms.

    A fill is journaled as it happened, in its trade date's share terms.
    Here its quantity is multiplied, and its price divided, by the factor
    of the splits logged after that date, so FIFO, share counts and bases
    all live in one frame with the holdings snapshot. Seeds are entered in
    current terms and only gain the marker. Every trade gains
    `split_factor` and `splits` (the events applied) so reports can say a
    split happened rather than absorb it. Idempotent: a trade already
    carrying `split_factor` is returned as is.
    """
    out = []
    for t in trades:
        if "split_factor" in t:
            out.append(t)
            continue
        t = dict(t)
        logged = splits_by_symbol.get(t["symbol"], []) if not t["seed"] else []
        factor = split_factor(logged, t["date"])
        t["split_factor"] = factor
        t["splits"] = [(str(d)[:10], float(r)) for d, r in logged
                       if str(d)[:10] > t["date"]]
        if factor != 1.0:
            t["qty"] = t["qty"] * factor
            if t["price"] is not None:
                t["price"] = t["price"] / factor
        out.append(t)
    return out


def lot_basis(con, lot, symbol):
    """(per-share basis in current share terms, splits after the lot date)
    or (None, []) when no price is knowable.

    A lot from load_trades(con) is already restated, so a recorded price
    is the basis as it stands (seeds are entered in current terms). Without
    a price the basis is the stored close on or before the trade date,
    which bronze holds in that session's own terms, restated by the splits
    logged after that session.
    """
    splits = lot.get("splits", [])
    if lot["price"] is not None:
        return lot["price"], splits
    row = con.execute(
        "SELECT date, close FROM bronze_prices WHERE symbol = ? AND date <= ? "
        "ORDER BY date DESC LIMIT 1", [symbol, lot["date"]]).fetchone()
    if row is None:
        return None, splits
    day, close = row
    return close / split_factor(splits_for(con, symbol), day), splits


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
    # Fills must be in current share terms before FIFO and basis; a no-op
    # for trades load_trades(con) already restated.
    trades = restate_to_current_terms(
        trades, splits_map(con, {t["symbol"] for t in trades}))
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
            basis, _ = lot_basis(con, lot, symbol)
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
