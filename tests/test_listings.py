"""Listing inventory: the filter keeps rankable stocks, drops the zoo."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import duckdb

from common.listings import parse_directory, refresh_listings

NASDAQ_FIXTURE = """\
Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
AAPL|Apple Inc. - Common Stock|Q|N|N|100|N|N
ZAZZT|Tick Pilot Test Stock|G|Y|N|100|N|N
QQQ|Invesco QQQ Trust|G|N|N|100|Y|N
ABCDW|Some SPAC - Warrant|G|N|N|100|N|N
File Creation Time: 0811202522:01
"""

OTHER_FIXTURE = """\
ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol
AZN|AstraZeneca PLC American Depositary Shares|N|AZN|N|100|N|AZN
BRK.B|Berkshire Hathaway Class B|N|BRK B|N|100|N|BRK/B
PFE-A|Pfizer Preferred|N|PFE-A|N|100|N|PFE-A
GE|GE Aerospace Common Stock|N|GE|N|100|N|GE
File Creation Time: 0811202522:01
"""


def test_nasdaq_filter_drops_tests_etfs_and_warrants():
    rows = parse_directory(NASDAQ_FIXTURE, "Symbol", "ETF", "Test Issue",
                           "Market Category")
    assert [r[0] for r in rows] == ["AAPL"]


def test_other_filter_keeps_adrs_drops_suffix_classes():
    rows = parse_directory(OTHER_FIXTURE, "ACT Symbol", "ETF", "Test Issue",
                           "Exchange")
    # AZN's ADS name survives: "Depositary Shares" junk targets preferreds,
    # and the ADR itself is a rankable common-stock line... BRK.B and PFE-A
    # fall to the plain-symbol rule.
    assert [r[0] for r in rows] == ["AZN", "GE"]
    assert rows[0][2] == "N"  # NYSE exchange code carried through


def test_refresh_snapshots_into_bronze_listings():
    con = duckdb.connect(":memory:")
    rows = [("AAPL", "Apple Inc.", "Q"), ("GE", "GE Aerospace", "N")]
    assert refresh_listings(con, rows=rows) == 2
    assert refresh_listings(con, rows=rows) == 2  # replace, not append
    stored = con.execute(
        "SELECT symbol, exchange FROM bronze_listings ORDER BY 1").fetchall()
    assert stored == [("AAPL", "Q"), ("GE", "N")]
