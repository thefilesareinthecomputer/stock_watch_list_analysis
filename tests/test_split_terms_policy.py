"""The share-terms rule, pinned (SPEC 5.1).

bronze_prices holds each session's close in that session's own share
terms and corporate_actions is the only source of split events. Yahoo's
split_ratio column stays in bronze as raw evidence of what the source said
and is read by nothing but the backfill that records it into the log. Any
new reader of that column, or any return of the retired split-detection
heuristic, fails here before it can produce a number.
"""
import os
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCANNED = ("src", "scripts")
# The backfill records the column into the log; the Databricks ingestion
# and transform jobs (bronze.stock_splits -> silver.stock_splits) are the
# other engine and are documented separately (SPEC 5.1).
SPLIT_RATIO_READERS = {
    "scripts/backfill.py",
    "src/ingestion/ingest_splits.py",
    "src/transforms/build_silver_splits.py",
}
RETIRED = ("_split_is_raw", "cum_split_final")


def _sources():
    for top in SCANNED:
        for path in sorted((ROOT / top).rglob("*.py")):
            yield path.relative_to(ROOT).as_posix(), path.read_text()


def test_only_the_backfill_reads_the_split_ratio_column():
    offenders = [rel for rel, text in _sources()
                 if "split_ratio" in text and rel not in SPLIT_RATIO_READERS]
    assert not offenders, (
        f"{offenders} read bronze_prices.split_ratio; take splits from "
        "corporate_actions (splits_for, splits_map, split_factor) instead")


def test_the_split_detection_heuristic_stays_retired():
    offenders = [(rel, name) for rel, text in _sources()
                 for name in RETIRED if name in text]
    assert not offenders, offenders


def test_allow_list_names_only_files_that_exist():
    missing = [rel for rel in SPLIT_RATIO_READERS if not (ROOT / rel).exists()]
    assert not missing, missing
