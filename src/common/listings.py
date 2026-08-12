"""US listing inventory: every NYSE/Nasdaq common stock, as warehouse data.

Source is the free NASDAQ Trader symbol directory (nasdaqlisted.txt +
otherlisted.txt) - licence-free, refreshed daily by the exchange, no
scraping. This is the raw material for the rule-based universe
(SPEC-RECOMMENDATION-ENGINE P2): membership is COMPUTED from our own
dollar-volume data over this inventory, never taken from licensed index
constituent lists.

The filter keeps common stocks and ADRs and drops what cannot be ranked:
test issues, ETFs (flagged by the source), and the suffix zoo of
warrants, rights, units and preferreds. Small caps are deliberately KEPT -
the line-of-sight universe must see a small stock before it gets big,
so nothing here filters on size; size is a tier tag applied later.
"""
import re
from datetime import datetime, timezone

import requests

NASDAQ_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHER_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"

# Plain 1-5 letter symbols only: dots, dollars and dashes are the exchanges'
# suffix conventions for preferreds, warrants, units and when-issued lines.
_PLAIN = re.compile(r"^[A-Z]{1,5}$")
# Name-level exclusions for junk the suffix rule misses. NOT "depositary
# shares" - that phrase names every ADR (AZN, NVO), and preferred-ADS
# lines carry suffix symbols the plain-symbol rule already drops.
_JUNK = re.compile(
    r"warrant|right(s)? |unit(s)?[ ,]|preferred|% notes|due 20",
    re.IGNORECASE)

SCHEMA = """
    CREATE OR REPLACE TABLE bronze_listings (
        symbol VARCHAR PRIMARY KEY,
        name VARCHAR,
        exchange VARCHAR,
        fetched_at TIMESTAMP
    )
"""


def _fetch(url):
    response = requests.get(url, timeout=30,
                            headers={"User-Agent": "stock-watch-list-analysis"})
    response.raise_for_status()
    return response.text


def parse_directory(text, symbol_col, etf_col, test_col, exchange):
    """Rows of (symbol, name, exchange) surviving the filters."""
    lines = [l for l in text.splitlines() if l.strip()]
    header = lines[0].split("|")
    idx = {name: i for i, name in enumerate(header)}
    rows = []
    for line in lines[1:]:
        if line.startswith("File Creation Time"):
            continue
        parts = line.split("|")
        if len(parts) != len(header):
            continue
        symbol = parts[idx[symbol_col]].strip()
        name = parts[idx["Security Name"]].strip()
        if parts[idx[test_col]].strip() != "N":     # test issues out
            continue
        if parts[idx[etf_col]].strip() == "Y":      # ETFs are yardsticks
            continue
        if not _PLAIN.match(symbol) or _JUNK.search(name):
            continue
        exch = parts[idx[exchange]].strip() if exchange in idx else "Q"
        rows.append((symbol, name, exch))
    return rows


def fetch_listings():
    """The filtered NYSE+Nasdaq common-stock inventory, deduplicated."""
    nasdaq = parse_directory(_fetch(NASDAQ_URL), "Symbol", "ETF",
                             "Test Issue", "Market Category")
    other = parse_directory(_fetch(OTHER_URL), "ACT Symbol", "ETF",
                            "Test Issue", "Exchange")
    seen, rows = set(), []
    for symbol, name, exch in nasdaq + other:
        if symbol not in seen:
            seen.add(symbol)
            rows.append((symbol, name, exch))
    return rows


def refresh_listings(con, rows=None):
    """Snapshot the inventory into bronze_listings; returns the row count.

    A plain snapshot, not evidence: point-in-time universe membership is
    recorded downstream in universe_membership, so this table can be
    freely replaced on every refresh.
    """
    rows = fetch_listings() if rows is None else rows
    con.execute(SCHEMA)
    now = datetime.now(timezone.utc)
    con.executemany("INSERT INTO bronze_listings VALUES (?, ?, ?, ?)",
                    [(s, n, e, now) for s, n, e in rows])
    return len(rows)
