"""Security type per symbol: which symbols are ETFs, as warehouse data.

Buy/sell calls are emitted only for operating equities (SPEC-BUY-SELL-CALLS):
ETFs and benchmarks are yardsticks or overlays, never call targets. The SIC
guard catches commodity trusts that file with EDGAR, but plain ETFs never
appear there, and the no-EDGAR set mixes ~50 true ETFs with foreign ADRs
that deserve calls - so the discriminator is yfinance's quoteType, fetched
once per symbol and stored.

A failed lookup stores UNKNOWN rather than retrying forever; UNKNOWN is
treated as an equity downstream, because wrongly excluding an operating
company silences a call while a stray ETF merely adds a harmless row.
"""
from datetime import datetime, timezone

SCHEMA = """
    CREATE TABLE IF NOT EXISTS bronze_security (
        symbol VARCHAR PRIMARY KEY,
        quote_type VARCHAR,
        fetched_at TIMESTAMP
    )
"""


def _yfinance_quote_type(symbol):
    # The chart endpoint's instrumentType, NOT Ticker.info's quoteType: the
    # quoteSummary endpoint behind .info 401s without browser cookies
    # (observed 2026-08-11, every symbol), while the chart endpoint is the
    # one yf.download already uses and it classifies identically.
    import yfinance as yf
    try:
        meta = yf.Ticker(symbol).get_history_metadata()
        return meta.get("instrumentType") or "UNKNOWN"
    except Exception:
        return "UNKNOWN"


def ensure_quote_types(con, symbols, fetch=_yfinance_quote_type):
    """Fetch and store quoteType for symbols not yet recorded. Idempotent:
    known symbols are never re-fetched, so re-runs cost nothing."""
    con.execute(SCHEMA)
    have = {r[0] for r in con.execute(
        "SELECT symbol FROM bronze_security").fetchall()}
    todo = [s for s in symbols if s not in have]
    for i, symbol in enumerate(todo, 1):
        con.execute("INSERT INTO bronze_security VALUES (?, ?, ?)",
                    [symbol, fetch(symbol), datetime.now(timezone.utc)])
        if i % 25 == 0:
            print(f"  quote types {i}/{len(todo)}", flush=True)
    return len(todo)
