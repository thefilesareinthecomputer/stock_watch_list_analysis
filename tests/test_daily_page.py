"""The daily page: one dated section per session in the private vault,
written after every run of the daily chain, passed or failed.
Pure text in, text out; no warehouse, no vault, no clock.
"""
import os
import sys
from datetime import date

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

from common.daily_page import (
    HEADER, guard_counts, proof_status, render_section, upsert_section,
)

PROOF = """# Data proof - 2026-11-11

Warehouse vintage: bronze through 2026-11-11. Written 2026-11-12T10:05:19.

| check | status | detail |
|---|---|---|
| vintage | ok | bronze through 2026-11-11 |
| held names | ATTENTION | DEAD 2: AAA, BBB |
| known closes | ok | 10/10 present, all match within a cent |

1 check(s) need attention: held names.
"""

GUARD = """# Portfolio guard - 2026-11-11

Prices through 2026-11-11; 3 holdings; book $1,000. This report informs.

New today: AAA (drawdown), BBB (size)

## Needs attention
- AAA [drawdown; new]: context
- BBB [size; new]: context
- CCC [stale; ongoing]: check the broker

## Exposure by sector
- Funds: $1,000 (100.0%)
"""

PASSED = {"job": "daily", "session": "2026-11-11", "exit_code": 0,
          "step_failed": None, "started": "2026-11-12T10:03:00",
          "finished": "2026-11-12T10:05:25"}
FAILED = {**PASSED, "exit_code": 1, "step_failed": "backfill"}
AGES = [("prices", date(2026, 11, 11)), ("fundamentals", date(2026, 8, 16)),
        ("monthly screen", None)]
TODAY = date(2026, 11, 12)


def test_proof_status_reads_each_check_and_drops_the_detail():
    assert proof_status(PROOF) == [("vintage", "ok"),
                                   ("held names", "ATTENTION"),
                                   ("known closes", "ok")]


def test_guard_counts_new_and_flagged_holdings():
    assert guard_counts(GUARD) == (2, 3)
    quiet = GUARD.replace("AAA (drawdown), BBB (size)", "no new alerts")
    assert guard_counts(quiet)[0] == 0


def test_guard_counts_a_report_with_nothing_flagged():
    text = "New today: no new alerts\n\n## Needs attention\n- none\n\n## Exposure\n"
    assert guard_counts(text) == (0, 0)


def test_guard_counts_match_the_report_the_guard_builds():
    # Pinned against the producer, so a change to the guard report's layout
    # fails here instead of printing wrong counts on the page.
    from guard import build_guard
    from tests.test_guard_script import POSITIONS, TODAY as GUARD_DAY, _con

    report, _, firing = build_guard(_con(), POSITIONS, [], GUARD_DAY)
    assert guard_counts(report) == (len(firing), 1)


def test_a_passed_run_states_status_checks_alerts_and_ages():
    section = render_section("2026-11-11", PASSED, proof_status(PROOF),
                             guard_counts(GUARD), AGES, TODAY, dry_run=True)
    assert section.startswith("## 2026-11-11 - passed\n")
    assert "2 of 3 ok; attention: held names" in section
    assert "2 new alerts, 3 holdings flagged (dry run: no alert recorded)" in section
    assert "prices 2026-11-11 (1 day old)" in section
    assert "fundamentals 2026-08-16 (88 days old)" in section
    assert "monthly screen not built" in section
    assert "reports/data_proof_2026-11-11.md" in section


def test_a_failed_run_names_the_step_and_the_last_session_covered():
    section = render_section("2026-11-11", FAILED, None, None, AGES, TODAY,
                             last_covered="2026-11-10")
    assert section.startswith("## 2026-11-11 - FAILED at backfill\n")
    assert "failed at backfill (exit 1)" in section
    assert "Last session covered: 2026-11-10" in section
    assert "Data checks: not run" in section and "Guard: not run" in section


def test_a_section_names_no_symbol_from_the_reports():
    section = render_section("2026-11-11", PASSED, proof_status(PROOF),
                             guard_counts(GUARD), AGES, TODAY)
    for symbol in ("AAA", "BBB", "CCC"):
        assert symbol not in section
    assert "$" not in section


def test_the_newest_session_goes_first_and_related_stays_last():
    page = upsert_section(HEADER, "2026-11-10", "## 2026-11-10 - passed\n\n- a\n")
    page = upsert_section(page, "2026-11-11", "## 2026-11-11 - passed\n\n- b\n")
    assert page.index("## 2026-11-11") < page.index("## 2026-11-10")
    assert page.index("## 2026-11-10") < page.index("## Related")
    assert page.rstrip().endswith(HEADER.rstrip().splitlines()[-1])


def test_a_rerun_replaces_its_own_section():
    page = upsert_section(HEADER, "2026-11-11",
                          "## 2026-11-11 - FAILED at backfill\n\n- old\n")
    page = upsert_section(page, "2026-11-11", "## 2026-11-11 - passed\n\n- new\n")
    assert page.count("## 2026-11-11") == 1
    assert "- new" in page and "- old" not in page


def test_an_older_session_is_filed_below_newer_ones():
    page = upsert_section(HEADER, "2026-11-11", "## 2026-11-11 - passed\n\n- b\n")
    page = upsert_section(page, "2026-11-10", "## 2026-11-10 - passed\n\n- a\n")
    assert page.index("## 2026-11-11") < page.index("## 2026-11-10")


def test_the_vault_folder_is_gitignored():
    with open(os.path.join(ROOT, ".gitignore")) as f:
        assert "PROJECTS/" in f.read()
