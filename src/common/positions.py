"""Held positions: knowledge/POSITIONS.md parsed, never echoed.

The file is private (gitignored twice over: `knowledge/` and the root
`POSITIONS.md` pattern) and does not travel between devices. Format:
`# ACCOUNT NAME` section headers, then one `SYMBOL QUANTITY` per line;
quantities may be fractional. The filename is ALL CAPS - macOS opens the
lowercase spelling too, but code must use the real name.

Everything derived from it stays in the gitignored warehouse or stdout.
Symbols, quantities, and account names never reach commit messages,
results/, or any tracked file (CLAUDE.md privacy table).
"""
import os

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
POSITIONS_PATH = os.path.join(ROOT, "knowledge", "POSITIONS.md")


def parse_positions(text):
    """[{account, symbol, quantity}] in file order.

    A symbol may appear under several accounts (one row each). Malformed
    lines fail loudly - a silently skipped holding would exempt it from
    the freshness gate, which is the failure the gate exists to catch.
    """
    positions, account = [], None
    for lineno, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            account = line.lstrip("#").strip()
            continue
        parts = line.split()
        if account is None or len(parts) != 2:
            raise ValueError(f"POSITIONS.md line {lineno}: expected "
                             f"'SYMBOL QUANTITY' under an account header")
        symbol, quantity = parts
        positions.append({"account": account, "symbol": symbol.upper(),
                          "quantity": float(quantity)})
    return positions


def load_positions(path=POSITIONS_PATH):
    """Positions from disk; empty on machines without the private file."""
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return parse_positions(f.read())


def held_symbols(positions):
    """The distinct held symbols, aggregated across accounts."""
    return sorted({p["symbol"] for p in positions})


def check_held_freshness(con, symbols):
    """Hard gate (plan task 12): every held symbol the warehouse tracks
    must be present at the warehouse's latest as_of_date.

    Returns (tracked, untracked). Raises when a tracked held name has gone
    stale - real money is riding on those rows, so a quiet gap is exactly
    the absence-is-a-failure case (SPEC P4). Untracked held names are not
    an error: they are the promotion-candidate list, reported not raised.
    """
    if not symbols:
        return [], []
    latest = con.execute(
        "SELECT MAX(as_of_date) FROM silver_signals").fetchone()[0]
    placeholders = ", ".join("?" for _ in symbols)
    have = dict(con.execute(
        f"SELECT symbol, MAX(as_of_date) FROM silver_signals "
        f"WHERE symbol IN ({placeholders}) GROUP BY symbol",
        list(symbols)).fetchall())
    untracked = sorted(s for s in symbols if s not in have)
    stale = sorted(s for s, d in have.items() if d != latest)
    if stale:
        raise ValueError(
            f"held symbols stale in silver_signals (latest {latest}): "
            f"{', '.join(stale)} - a held name with missing data is a "
            "failure, not a silence")
    return sorted(have), untracked


def build_held_table(con, positions):
    """gold_held_positions: the held tier, warehouse-only by design.

    One row per held symbol: accounts, total quantity, tracked flag, and
    the latest v2 rank and call where those tables exist. Lives only in
    the gitignored warehouse - the leak boundary for task 12's "tier
    membership never reaches a tracked file".
    """
    def _has(table):
        return con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = ?", [table]).fetchone()[0]

    con.register("incoming_positions", pd.DataFrame(positions))
    rank_join = call_join = sight_join = ""
    rank_cols, call_col = "NULL AS composite_rank, NULL AS composite_score", \
        "NULL AS call"
    sight_cols = "NULL AS deteriorating, NULL AS ret_3m, NULL AS ret_12m"
    if _has("gold_watchlist_ranked_v2"):
        rank_cols = "r.composite_rank, r.composite_score"
        rank_join = "LEFT JOIN gold_watchlist_ranked_v2 r USING (symbol)"
    if _has("gold_calls"):
        call_col = "c.call"
        call_join = """
            LEFT JOIN (
                SELECT symbol, call FROM gold_calls
                WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold_calls)
            ) c USING (symbol)"""
    if _has("gold_line_of_sight"):
        # The sell-side alert: absolute steady decline (down over 3, 6 and
        # 12 months), covering held ETFs and funds that calls never touch.
        sight_cols = "l.deteriorating, l.ret_3m, l.ret_12m"
        sight_join = "LEFT JOIN gold_line_of_sight l USING (symbol)"
    con.execute(f"""
        CREATE OR REPLACE TABLE gold_held_positions AS
        WITH held AS (
            SELECT UPPER(symbol) AS symbol,
                   STRING_AGG(account, ', ' ORDER BY account) AS accounts,
                   SUM(quantity) AS total_quantity
            FROM incoming_positions GROUP BY UPPER(symbol)
        )
        SELECT held.symbol, held.accounts, held.total_quantity,
               EXISTS (SELECT 1 FROM silver_signals s
                       WHERE s.symbol = held.symbol) AS tracked,
               {rank_cols}, {call_col}, {sight_cols}
        FROM held
        {rank_join}
        {call_join}
        {sight_join}
        ORDER BY tracked DESC, composite_rank NULLS LAST, symbol
    """)
    con.unregister("incoming_positions")
