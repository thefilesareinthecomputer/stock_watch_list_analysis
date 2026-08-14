"""EDGAR CompanyFacts extraction and bronze upsert."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import duckdb
import pandas as pd
import pytest

from common.edgar import (
    cik_from_atom, extract_facts, universe_backfill_targets, upsert_facts,
    user_agent,
)

CIK = "0000320193"

PAYLOAD = {
    "facts": {
        "us-gaap": {
            "NetIncomeLoss": {
                "units": {
                    "USD": [
                        {"start": "2023-10-01", "end": "2024-09-28",
                         "val": 93736000000, "fy": 2024, "fp": "FY",
                         "form": "10-K", "filed": "2024-11-01",
                         "frame": "CY2024"},
                        # Restated copy of the same period from a later filing.
                        {"start": "2023-10-01", "end": "2024-09-28",
                         "val": 93700000000, "fy": 2025, "fp": "FY",
                         "form": "10-K", "filed": "2025-10-31"},
                    ],
                    # Foreign-currency duplicate must be excluded.
                    "EUR": [
                        {"start": "2023-10-01", "end": "2024-09-28",
                         "val": 86000000000, "fy": 2024, "fp": "FY",
                         "form": "10-K", "filed": "2024-11-01"},
                    ],
                },
            },
            "Assets": {
                "units": {
                    "USD": [
                        {"end": "2024-09-28", "val": 364980000000,
                         "fy": 2024, "fp": "FY", "form": "10-K",
                         "filed": "2024-11-01"},
                        # No filed date -> not knowable, dropped.
                        {"end": "2024-09-28", "val": 1},
                    ],
                },
            },
            # A tag outside the wanted set must not be extracted.
            "OperatingLeaseLiability": {
                "units": {"USD": [
                    {"end": "2024-09-28", "val": 5,
                     "form": "10-K", "filed": "2024-11-01"},
                ]},
            },
        },
        "dei": {
            "EntityCommonStockSharesOutstanding": {
                "units": {"shares": [
                    {"end": "2024-10-18", "val": 15115823000, "fy": 2024,
                     "fp": "FY", "form": "10-K", "filed": "2024-11-01"},
                ]},
            },
        },
    },
}


def test_extracts_wanted_tags_only():
    df = extract_facts("AAPL", CIK, PAYLOAD)
    assert set(df["tag"]) == {
        "NetIncomeLoss", "Assets", "EntityCommonStockSharesOutstanding",
    }


def test_excludes_foreign_currency_units():
    df = extract_facts("AAPL", CIK, PAYLOAD)
    assert set(df["unit"]) == {"USD", "shares"}
    income = df[df["tag"] == "NetIncomeLoss"]
    assert len(income) == 2  # original and restatement, no EUR row


def test_drops_facts_without_filed_date():
    df = extract_facts("AAPL", CIK, PAYLOAD)
    assets = df[df["tag"] == "Assets"]
    assert len(assets) == 1
    assert assets.iloc[0]["value"] == 364980000000.0


def test_instant_facts_have_no_start_date():
    df = extract_facts("AAPL", CIK, PAYLOAD)
    assets = df[df["tag"] == "Assets"].iloc[0]
    assert pd.isna(assets["start_date"])
    assert assets["end_date"] == "2024-09-28"


def test_empty_payload_yields_empty_frame():
    assert extract_facts("XXXX", CIK, {"facts": {}}).empty


def test_upsert_is_idempotent():
    con = duckdb.connect(":memory:")
    df = extract_facts("AAPL", CIK, PAYLOAD)

    first = upsert_facts(con, df, "run1", "2026-08-10T00:00:00Z")
    second = upsert_facts(con, df, "run2", "2026-08-10T01:00:00Z")
    count = con.execute("SELECT COUNT(*) FROM bronze_fundamentals").fetchone()[0]

    assert first == second == count


def test_upsert_replaces_only_incoming_symbols():
    con = duckdb.connect(":memory:")
    aapl = extract_facts("AAPL", CIK, PAYLOAD)
    msft = extract_facts("MSFT", "0000789019", PAYLOAD)
    upsert_facts(con, aapl, "run1", "t")
    upsert_facts(con, msft, "run1", "t")

    upsert_facts(con, aapl, "run2", "t")  # re-run one symbol only

    counts = dict(con.execute(
        "SELECT symbol, COUNT(*) FROM bronze_fundamentals GROUP BY symbol"
    ).fetchall())
    assert counts["AAPL"] == counts["MSFT"] == len(aapl)


def test_cik_from_atom_parses_browse_edgar_response():
    atom = '<link href="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000004904&type=10-K"/>'
    assert cik_from_atom(atom) == "0000004904"


def test_cik_from_atom_returns_none_on_no_match():
    assert cik_from_atom("<feed>No matching companies.</feed>") is None


def test_user_agent_prefers_explicit_setting(monkeypatch):
    monkeypatch.setenv("EDGAR_USER_AGENT", "custom agent (me@example.com)")
    monkeypatch.setenv("ALERT_EMAIL", "fallback@example.com")
    assert user_agent() == "custom agent (me@example.com)"


def test_user_agent_falls_back_to_alert_email(monkeypatch):
    monkeypatch.delenv("EDGAR_USER_AGENT", raising=False)
    monkeypatch.setenv("ALERT_EMAIL", "fallback@example.com")
    assert "fallback@example.com" in user_agent()


def test_user_agent_requires_a_contact(monkeypatch):
    monkeypatch.delenv("EDGAR_USER_AGENT", raising=False)
    monkeypatch.delenv("ALERT_EMAIL", raising=False)
    with pytest.raises(RuntimeError):
        user_agent()


def test_universe_backfill_targets_skips_symbols_with_facts():
    con = duckdb.connect()
    con.execute("CREATE TABLE bronze_prices (symbol VARCHAR, date DATE)")
    con.executemany("INSERT INTO bronze_prices VALUES (?, DATE '2026-01-02')",
                    [("AAA",), ("BBB",), ("CCC",), ("BBB",)])
    upsert_facts(con, extract_facts("BBB", CIK, PAYLOAD), "run", "ts")
    assert universe_backfill_targets(con) == ["AAA", "CCC"]


def test_universe_backfill_targets_covers_all_when_no_facts_yet():
    con = duckdb.connect()
    con.execute("CREATE TABLE bronze_prices (symbol VARCHAR, date DATE)")
    con.execute("INSERT INTO bronze_prices VALUES ('AAA', DATE '2026-01-02')")
    assert universe_backfill_targets(con) == ["AAA"]
