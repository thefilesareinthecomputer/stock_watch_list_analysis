"""One-time migration: bronze_prices from Yahoo's restated closes to the
closes that actually traded (STORY-000009).

    uv run python scripts/migrate_traded_prices.py --backup-dir /path/outside/repo

Order of operations, each gated on the one before:
1. Refuse if the warehouse is already marked traded, or if any (symbol,
   date) appears twice.
2. CHECKPOINT, fingerprint (row count, hash sum), copy the file to
   <backup-dir>/market_<date>.duckdb, open the copy read-only and confirm
   the same fingerprint.
3. In ONE transaction: run every split symbol's pre-split rows through
   common.traded_prices.to_traded and write open/high/low/close/dividend/
   volume back; check the known closes; confirm the row count; set
   warehouse_meta bronze_terms = traded. Any failed check rolls back.
4. Write reports/migration_traded_prices_<date>.md with the evidence.

adj_close and the lineage columns are left as they were. After this the
backfill appends new sessions in traded terms and never rewrites a row.
"""
import argparse
import os
import shutil
import sys
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import duckdb  # noqa: E402

from common.corporate_actions import splits_for  # noqa: E402
from common.traded_prices import to_traded  # noqa: E402
from common.warehouse_meta import (  # noqa: E402
    BRONZE_TERMS, TRADED, bronze_is_traded, set_meta,
)

WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")
REPORTS = os.path.join(ROOT, "reports")
TABLE = "bronze_prices"
# Closes published outside Yahoo; the migration must reproduce them.
KNOWN_CLOSES = [("AAPL", "2020-08-28", 499.23)]
TOLERANCE = 0.01


def fingerprint(con):
    """(row count, sum of per-row hashes) over symbol, date, close."""
    return con.execute(
        f"SELECT COUNT(*), SUM(HASH(symbol, date, close)) FROM {TABLE}").fetchone()


def _read_only_fingerprint(path):
    con = duckdb.connect(path, read_only=True)
    try:
        return fingerprint(con)
    finally:
        con.close()


def _precheck(con):
    if bronze_is_traded(con):
        sys.exit(f"{TABLE} is already in traded terms ({BRONZE_TERMS}={TRADED}); "
                 "nothing to migrate")
    dupes = con.execute(
        f"SELECT COUNT(*) - COUNT(DISTINCT (symbol, date)) FROM {TABLE}").fetchone()[0]
    if dupes:
        sys.exit(f"{dupes} duplicate (symbol, date) rows in {TABLE}; "
                 "fix those before migrating")


def _backup(warehouse, backup_dir, today, before):
    os.makedirs(backup_dir, exist_ok=True)
    target = os.path.join(backup_dir, f"market_{today}.duckdb")
    if os.path.exists(target):
        sys.exit(f"backup already exists, refusing to overwrite: {target}")
    shutil.copy2(warehouse, target)
    copied = _read_only_fingerprint(target)
    if copied != before:
        sys.exit(f"backup fingerprint {copied} differs from live {before}")
    return target


def _convert(con):
    """Rewrite each split symbol's pre-split rows in traded terms.
    Returns {symbol: rows changed}."""
    symbols = [s for (s,) in con.execute(
        f"SELECT DISTINCT c.symbol FROM corporate_actions c "
        f"JOIN (SELECT DISTINCT symbol FROM {TABLE}) p USING (symbol) "
        "ORDER BY 1").fetchall()]
    changed = {}
    for symbol in symbols:
        splits = splits_for(con, symbol)
        last_split = max(day for day, _ in splits)
        rows = con.execute(
            f"SELECT symbol, date, open, high, low, close, dividend, volume "
            f"FROM {TABLE} WHERE symbol = ? AND date < ? ORDER BY date",
            [symbol, last_split]).df()
        if rows.empty:
            continue
        converted = to_traded(rows, splits)
        con.register("converted", converted)
        con.execute(
            f"UPDATE {TABLE} SET open = c.open, high = c.high, low = c.low, "
            "close = c.close, dividend = c.dividend, volume = c.volume "
            f"FROM converted c WHERE {TABLE}.symbol = c.symbol "
            f"AND {TABLE}.date = c.date")
        con.unregister("converted")
        changed[symbol] = len(rows)
    return changed


def _check_known_closes(con):
    results = []
    for symbol, day, published in KNOWN_CLOSES:
        row = con.execute(
            f"SELECT close FROM {TABLE} WHERE symbol = ? AND date = ?",
            [symbol, day]).fetchone()
        if row is None:
            results.append((symbol, day, published, None, "absent"))
            continue
        ok = abs(row[0] - published) < TOLERANCE
        results.append((symbol, day, published, row[0], "ok" if ok else "FAIL"))
        if not ok:
            con.rollback()
            sys.exit(f"known close check failed: {symbol} {day} stored "
                     f"{row[0]:.4f}, published {published}; rolled back")
    return results


def _write_report(path, backup, before, after, changed, known):
    lines = [
        f"# bronze_prices migration to traded closes - {date.today()}", "",
        f"Backup: `{backup}` (opens read-only; fingerprint matches pre-migration)", "",
        "| | rows | hash sum |", "|---|---|---|",
        f"| before | {before[0]} | {before[1]} |",
        f"| after | {after[0]} | {after[1]} |", "",
        f"Rows rewritten: {sum(changed.values())} across {len(changed)} symbols "
        "(only rows dated before each symbol's last logged split).", "",
        "| known close | published | stored | result |", "|---|---|---|---|",
        *[f"| {s} {d} | {p} | {'' if v is None else f'{v:.4f}'} | {r} |"
          for s, d, p, v, r in known], "",
        "| symbol | rows changed |", "|---|---|",
        *[f"| {s} | {n} |" for s, n in sorted(changed.items())], "",
    ]
    with open(path, "w") as f:
        f.write("\n".join(lines))


def migrate(warehouse=WAREHOUSE, backup_dir=None, reports_dir=REPORTS, today=None):
    if not backup_dir:
        sys.exit("--backup-dir is required")
    real_backup = os.path.realpath(backup_dir)
    if real_backup == os.path.realpath(ROOT) or real_backup.startswith(
            os.path.realpath(ROOT) + os.sep):
        sys.exit("the backup must live outside the repo")
    today = today or date.today()

    con = duckdb.connect(warehouse)
    _precheck(con)
    con.execute("CHECKPOINT")
    before = fingerprint(con)
    con.close()
    backup = _backup(warehouse, real_backup, today, before)

    con = duckdb.connect(warehouse)
    con.begin()
    changed = _convert(con)
    known = _check_known_closes(con)
    after = fingerprint(con)
    if after[0] != before[0]:
        con.rollback()
        sys.exit(f"row count changed {before[0]} -> {after[0]}; rolled back")
    set_meta(con, BRONZE_TERMS, TRADED)
    con.commit()
    con.close()

    os.makedirs(reports_dir, exist_ok=True)
    report_path = os.path.join(reports_dir, f"migration_traded_prices_{today}.md")
    _write_report(report_path, backup, before, after, changed, known)
    print(f"migrated {sum(changed.values())} rows across {len(changed)} symbols; "
          f"rows {before[0]} -> {after[0]}\n  backup {backup}\n  report {report_path}")
    return {"before": before, "after": after, "backup": backup,
            "changed": changed, "known": known, "report": report_path}


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--backup-dir", required=True,
                        help="directory outside the repo for the pre-migration copy")
    args = parser.parse_args()
    migrate(WAREHOUSE, args.backup_dir, REPORTS)


if __name__ == "__main__":
    main()
