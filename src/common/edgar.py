"""SEC EDGAR CompanyFacts client - the point-in-time fundamentals source.

Every fact arrives as-filed and carries the date it became public (`filed`),
which is what makes EDGAR point-in-time by construction where yfinance's
current-snapshot fundamentals are not (SPEC-RECOMMENDATION-ENGINE.md P4).

Bronze stores raw facts for a fixed tag set; concept selection and
point-in-time semantics live in silver. Filers vary in which XBRL tag they
use for the same concept, so each concept lists tags in preference order and
silver picks the first present.

SEC fair-access rules: at most 10 requests/second and a declarative
User-Agent naming a contact. The agent string comes from EDGAR_USER_AGENT,
falling back to ALERT_EMAIL, both read from the environment so no address
lands in tracked code.
"""
import os
import re
import time

import pandas as pd

TICKER_CIK_URL = "https://www.sec.gov/files/company_tickers.json"
BROWSE_EDGAR_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"

# Non-operating investment vehicles by SIC: commodity trusts (6221),
# investment offices (6726), blank checks (6770), investors NEC (6799).
# They file 10-Ks whose "net income" is asset appreciation, so their
# earnings-based ratios are fiction. Banks and insurers are NOT here -
# their earnings are real.
NON_OPERATING_SIC = ("6221", "6726", "6770", "6799")
COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
REQUEST_INTERVAL = 0.13  # seconds between requests; safely under 10/s

# concept -> (taxonomy, tags in preference order)
CONCEPT_TAGS = {
    "net_income": ("us-gaap", ["NetIncomeLoss"]),
    "gross_profit": ("us-gaap", ["GrossProfit"]),
    "revenues": ("us-gaap", [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "Revenues",
        "SalesRevenueNet",
    ]),
    "cost_of_revenue": ("us-gaap", [
        "CostOfRevenue",
        "CostOfGoodsAndServicesSold",
        "CostOfGoodsSold",
    ]),
    "assets": ("us-gaap", ["Assets"]),
    "equity": ("us-gaap", [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ]),
    "shares_outstanding": ("dei", ["EntityCommonStockSharesOutstanding"]),
}

# Monetary facts in USD only; foreign-currency duplicates would double-count.
ACCEPTED_UNITS = {"USD", "shares", "USD/shares"}

WANTED_TAGS = {(taxonomy, tag)
               for taxonomy, tags in CONCEPT_TAGS.values()
               for tag in tags}

SCHEMA = """
    CREATE TABLE IF NOT EXISTS bronze_fundamentals (
        symbol VARCHAR,
        cik VARCHAR,
        taxonomy VARCHAR,
        tag VARCHAR,
        unit VARCHAR,
        start_date DATE,       -- NULL for instant (balance-sheet) facts
        end_date DATE,         -- period end, or the instant itself
        value DOUBLE,
        fiscal_year INTEGER,
        fiscal_period VARCHAR, -- FY, Q1..Q4
        form VARCHAR,          -- 10-K, 10-Q, ...
        filed DATE,            -- when the fact became public knowledge
        frame VARCHAR,
        _run_id VARCHAR,
        _ingest_ts VARCHAR,
        _source_system VARCHAR,
        _source_event_ts VARCHAR,
        _load_type VARCHAR
    )
"""


def user_agent():
    """SEC-required declarative User-Agent, sourced from the environment."""
    explicit = os.getenv("EDGAR_USER_AGENT", "").strip()
    if explicit:
        return explicit
    email = os.getenv("ALERT_EMAIL", "").strip()
    if email:
        return f"stock-watch-list-analysis ({email})"
    raise RuntimeError(
        "SEC requires a contact User-Agent: set EDGAR_USER_AGENT or "
        "ALERT_EMAIL in .env"
    )


def resolve_ciks(session):
    """Map ticker -> zero-padded 10-digit CIK for every SEC registrant."""
    resp = session.get(TICKER_CIK_URL, timeout=30)
    resp.raise_for_status()
    return {row["ticker"].upper(): f"{row['cik_str']:010d}"
            for row in resp.json().values()}


def cik_from_atom(text):
    """Pull the zero-padded CIK out of a browse-edgar atom response."""
    match = re.search(r"CIK=(\d{10})", text)
    return match.group(1) if match else None


def resolve_cik_fallback(session, symbol):
    """Server-side ticker lookup for symbols the bulk mapping is missing.

    company_tickers.json is not complete (AEP, a major registrant, is absent
    from it); browse-edgar resolves the ticker against EDGAR's own index.
    Returns None for tickers EDGAR does not know (ETFs, foreign OTC).
    """
    resp = session.get(BROWSE_EDGAR_URL, params={
        "action": "getcompany", "ticker": symbol, "type": "10-K",
        "output": "atom", "count": "1",
    }, timeout=30)
    time.sleep(REQUEST_INTERVAL)
    if resp.status_code != 200:
        return None
    return cik_from_atom(resp.text)


def fetch_companyfacts(session, cik):
    """Fetch one company's facts; None when EDGAR has no filer for the CIK."""
    resp = session.get(COMPANYFACTS_URL.format(cik=cik), timeout=30)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    time.sleep(REQUEST_INTERVAL)
    return resp.json()


def fetch_entity(session, cik):
    """Entity classification from the submissions endpoint: SIC and name."""
    resp = session.get(SUBMISSIONS_URL.format(cik=cik), timeout=30)
    time.sleep(REQUEST_INTERVAL)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    data = resp.json()
    return {"sic": str(data.get("sic") or ""),
            "sic_description": data.get("sicDescription") or "",
            "entity_name": data.get("name") or ""}


ENTITY_SCHEMA = """
    CREATE TABLE IF NOT EXISTS bronze_entity (
        symbol VARCHAR,
        cik VARCHAR,
        sic VARCHAR,
        sic_description VARCHAR,
        entity_name VARCHAR,
        _ingest_ts VARCHAR
    )
"""


def upsert_entity(con, symbol, cik, entity, ingest_ts):
    con.execute(ENTITY_SCHEMA)
    con.execute("DELETE FROM bronze_entity WHERE symbol = ?", [symbol])
    con.execute("INSERT INTO bronze_entity VALUES (?, ?, ?, ?, ?, ?)",
                [symbol, cik, entity["sic"], entity["sic_description"],
                 entity["entity_name"], ingest_ts])


def extract_facts(symbol, cik, payload):
    """Flatten a companyfacts payload to rows for the wanted tag set."""
    rows = []
    facts = payload.get("facts", {})
    for taxonomy, tag in sorted(WANTED_TAGS):
        units = facts.get(taxonomy, {}).get(tag, {}).get("units", {})
        for unit, entries in units.items():
            if unit not in ACCEPTED_UNITS:
                continue
            for entry in entries:
                if entry.get("val") is None or not entry.get("filed"):
                    continue
                rows.append({
                    "symbol": symbol,
                    "cik": cik,
                    "taxonomy": taxonomy,
                    "tag": tag,
                    "unit": unit,
                    "start_date": entry.get("start"),
                    "end_date": entry.get("end"),
                    "value": float(entry["val"]),
                    "fiscal_year": entry.get("fy"),
                    "fiscal_period": entry.get("fp"),
                    "form": entry.get("form"),
                    "filed": entry["filed"],
                    "frame": entry.get("frame"),
                })
    return pd.DataFrame(rows)


def universe_backfill_targets(con):
    """Banked symbols (bronze_prices) with no stored facts yet.

    The resume-safe target list for a universe-wide backfill: a killed run
    skips everything already landed, and watchlist symbols fetched earlier
    are never refetched. Symbols that legitimately have no facts (ETFs,
    foreign listings) reappear on resume; their fetches are cheap misses.
    """
    have_facts = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'bronze_fundamentals'").fetchone()[0]
    if not have_facts:
        return [r[0] for r in con.execute(
            "SELECT DISTINCT symbol FROM bronze_prices ORDER BY symbol"
        ).fetchall()]
    return [r[0] for r in con.execute(
        "SELECT DISTINCT symbol FROM bronze_prices "
        "WHERE symbol NOT IN (SELECT DISTINCT symbol FROM bronze_fundamentals) "
        "ORDER BY symbol").fetchall()]


def upsert_facts(con, df, run_id, ingest_ts):
    """Idempotent per symbol: delete then insert, same as the price backfill."""
    if df.empty:
        return 0
    df = df.copy()
    for col in ("start_date", "end_date", "filed"):
        df[col] = pd.to_datetime(df[col]).dt.date
    df["fiscal_year"] = df["fiscal_year"].astype("Int64")
    df["_run_id"] = run_id
    df["_ingest_ts"] = ingest_ts
    df["_source_system"] = "sec_edgar"
    df["_source_event_ts"] = df["filed"].astype(str) + "T00:00:00Z"
    df["_load_type"] = "full"

    con.execute(SCHEMA)
    cols = list(con.execute("DESCRIBE bronze_fundamentals").df()["column_name"])
    df = df[cols]
    con.register("incoming_facts", df)
    con.execute(
        "DELETE FROM bronze_fundamentals WHERE symbol IN "
        "(SELECT DISTINCT symbol FROM incoming_facts)"
    )
    con.execute("INSERT INTO bronze_fundamentals SELECT * FROM incoming_facts")
    con.unregister("incoming_facts")
    return len(df)
