"""One-time migration of bronze_prices from Yahoo's restated closes to the
closes that traded, with a backup first and proof after.

Runs against a throwaway warehouse file: the real one moves only on the
user's word (STORY-000009).
"""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import duckdb
import pytest

from common.corporate_actions import record_actions
from common.warehouse_meta import bronze_is_traded, get_meta, set_meta
import backfill
import migrate_traded_prices as mig

ROWS = [
    # SPLT: 4-for-1 on 2020-08-31, so the two earlier rows are restated.
    ("SPLT", "2020-08-27", 100.0, 110.0, 90.0, 100.0, 95.0, 4000, 0.25, 0.0),
    ("SPLT", "2020-08-28", 124.0, 125.0, 123.0, 124.807503, 120.9, 187630000, 0.0, 0.0),
    ("SPLT", "2020-08-31", 129.0, 130.0, 128.0, 129.04, 125.0, 225702700, 0.0, 4.0),
    ("SPLT", "2020-09-01", 134.0, 135.0, 133.0, 134.18, 130.0, 150000000, 0.0, 0.0),
    # FLAT: no split, must come out byte-identical.
    ("FLAT", "2020-08-28", 10.0, 11.0, 9.0, 10.5, 10.0, 500, 0.0, 0.0),
    ("FLAT", "2020-08-31", 10.6, 11.6, 9.6, 11.0, 10.5, 600, 0.1, 0.0),
]


def _seed(path):
    con = duckdb.connect(path)
    con.execute(backfill.SCHEMA)
    con.executemany(
        "INSERT INTO bronze_prices VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
        "'run0', '2026-11-07T00:00:00Z', 'yfinance', ?, 'full')",
        [[*r, f"{r[1]}T00:00:00Z"] for r in ROWS])
    record_actions(con, [{"symbol": "SPLT", "date": "2020-08-31", "ratio": 4.0}],
                   source="yfinance")
    con.close()


@pytest.fixture
def warehouse(tmp_path):
    path = str(tmp_path / "market.duckdb")
    _seed(path)
    return path


def _query(path, fn):
    """Run fn(con) on a read-only connection that is closed afterwards:
    DuckDB refuses a second differently-configured handle on one file."""
    con = duckdb.connect(path, read_only=True)
    try:
        return fn(con)
    finally:
        con.close()


def _closes(path, symbol):
    return _query(path, lambda con: con.execute(
        "SELECT date, open, close, volume, dividend, adj_close "
        "FROM bronze_prices WHERE symbol = ? ORDER BY date", [symbol]).fetchall())


def _traded(path):
    return _query(path, bronze_is_traded)


def test_migration_backs_up_then_converts_only_pre_split_rows(warehouse, tmp_path):
    before_flat = _closes(warehouse, "FLAT")
    before_fp = _query(warehouse, mig.fingerprint)
    backups, reports = str(tmp_path / "bk"), str(tmp_path / "reports")

    report = mig.migrate(warehouse, backups, reports, today=date(2026, 11, 8))

    # Backup: present, opens read-only, matches the pre-migration fingerprint.
    backup = report["backup"]
    assert os.path.dirname(backup) == backups
    assert _query(backup, mig.fingerprint) == before_fp
    assert report["before"] == before_fp and report["after"] != before_fp
    assert report["before"][0] == report["after"][0] == len(ROWS)  # row count kept

    # Pre-split rows are in traded terms; the split-date row and later are not.
    rows = _closes(warehouse, "SPLT")
    assert rows[0][1:] == (400.0, 400.0, 1000, 1.0, 95.0)
    assert abs(rows[1][2] - 499.23) < 0.01 and rows[1][3] == 46907500
    assert rows[2][2] == 129.04 and rows[3][2] == 134.18
    assert _closes(warehouse, "FLAT") == before_flat
    assert report["changed"] == {"SPLT": 2}

    assert _traded(warehouse)
    # Lineage columns are untouched: the migration converts terms, not provenance.
    assert _query(warehouse, lambda con: con.execute(
        "SELECT DISTINCT _run_id, _load_type FROM bronze_prices").fetchall()
    ) == [("run0", "full")]

    report_path = os.path.join(reports, "migration_traded_prices_2026-11-08.md")
    assert os.path.exists(report_path)
    text = open(report_path).read()
    assert "SPLT" in text and str(before_fp[0]) in text and backup in text


def _add(path, symbol, rows, run_id, splits):
    con = duckdb.connect(path)
    con.executemany(
        "INSERT INTO bronze_prices (symbol, date, close, adj_close, dividend, "
        "volume, _run_id) VALUES (?, ?, ?, ?, 0.0, 100, ?)",
        [[symbol, d, c, a, run_id] for d, c, a in rows])
    record_actions(con, [{"symbol": symbol, "date": d, "ratio": r} for d, r in splits],
                   source="test")
    con.close()


def test_rows_captured_before_a_split_stay_in_their_own_terms(warehouse, tmp_path):
    # Banked once before the 08-11 split (run OLD): closes 90 are traded
    # already. The post-split rows came in a later run. Nothing may move.
    _add(warehouse, "BANK", [("2026-08-06", 94.0, 94.0), ("2026-08-07", 90.0, 90.0)],
         "OLD", [("2026-08-11", 2.0)])
    _add(warehouse, "BANK", [("2026-08-11", 45.5, 45.5)], "NEW", [])
    before = _closes(warehouse, "BANK")
    report = mig.migrate(warehouse, str(tmp_path / "bk"), str(tmp_path / "r"))
    assert _closes(warehouse, "BANK") == before
    assert "BANK" not in report["changed"] and report["skipped"] == []


def test_a_split_the_source_served_unrestated_is_skipped_and_reported(
        warehouse, tmp_path, capsys):
    # One run, but the served close halves on the split day: the source
    # never restated this split, so the pre-split rows are traded already.
    _add(warehouse, "RAWS", [("2026-08-06", 94.0, 94.0), ("2026-08-07", 90.0, 90.0),
                             ("2026-08-11", 45.5, 45.5)], "run0", [("2026-08-11", 2.0)])
    before = _closes(warehouse, "RAWS")
    report = mig.migrate(warehouse, str(tmp_path / "bk"), str(tmp_path / "r"))
    assert _closes(warehouse, "RAWS") == before
    assert [s[:3] for s in report["skipped"]] == [("RAWS", "2026-08-11", 2.0)]
    assert "SPLIT NOT RESTATED BY SOURCE RAWS 2026-08-11" in capsys.readouterr().out
    assert "| RAWS | 2026-08-11 | 2 |" in open(report["report"]).read()


def test_a_migrated_warehouse_refuses_a_second_run(warehouse, tmp_path):
    mig.migrate(warehouse, str(tmp_path / "bk"), str(tmp_path / "r"))
    rows = _closes(warehouse, "SPLT")
    with pytest.raises(SystemExit, match="already"):
        mig.migrate(warehouse, str(tmp_path / "bk"), str(tmp_path / "r"))
    assert _closes(warehouse, "SPLT") == rows


def test_duplicate_symbol_dates_stop_the_migration_before_any_write(
        warehouse, tmp_path):
    con = duckdb.connect(warehouse)
    con.execute("INSERT INTO bronze_prices SELECT * FROM bronze_prices "
                "WHERE symbol = 'SPLT' AND date = '2020-08-28'")
    con.close()
    before = _closes(warehouse, "SPLT")
    with pytest.raises(SystemExit, match="duplicate"):
        mig.migrate(warehouse, str(tmp_path / "bk"), str(tmp_path / "r"))
    assert _closes(warehouse, "SPLT") == before
    assert not _traded(warehouse)


def test_known_close_check_failure_rolls_back(warehouse, tmp_path, monkeypatch):
    monkeypatch.setattr(mig, "KNOWN_CLOSES", [("SPLT", "2020-08-28", 123.45)])
    before = _closes(warehouse, "SPLT")
    with pytest.raises(SystemExit, match="known close"):
        mig.migrate(warehouse, str(tmp_path / "bk"), str(tmp_path / "r"))
    assert _closes(warehouse, "SPLT") == before
    assert not _traded(warehouse)


def test_default_backup_dir_follows_the_standard():
    # One home-level directory, one folder per repo, outside any repo.
    home = os.path.expanduser("~")
    assert mig.BACKUP_DIR == os.path.join(home, "backups", os.path.basename(mig.ROOT))
    assert not os.path.realpath(mig.BACKUP_DIR).startswith(os.path.realpath(mig.ROOT))


def test_backup_dir_inside_the_repo_is_refused(warehouse):
    repo_dir = os.path.join(os.path.dirname(__file__), "..", "warehouse")
    with pytest.raises(SystemExit, match="outside the repo"):
        mig.migrate(warehouse, repo_dir, repo_dir)


def test_meta_round_trip():
    con = duckdb.connect()
    assert get_meta(con, "bronze_terms") is None
    set_meta(con, "bronze_terms", "traded")
    set_meta(con, "bronze_terms", "traded")        # idempotent
    assert get_meta(con, "bronze_terms") == "traded"
    assert con.execute("SELECT COUNT(*) FROM warehouse_meta").fetchone()[0] == 1
