"""The daily page: one dated section per market session in the private vault.

scripts/cadence.py writes a section after every run of the daily chain,
passed or failed, so one page says whether the run happened and what the
data showed. A section names no symbol, quantity or amount: it gives each
check's status and the alert counts, and the detail stays in reports/.

Everything here is text in, text out. The caller reads the reports, the
ledger and the warehouse, and owns the file.
"""
import re

# The page as first created. It follows the vault contract for an
# agent-owned note: frontmatter with a type, one H1, and a closing Related
# section that the dated sections are filed above.
HEADER = """---
type: reference
title: agents-daily
description: The daily page - one section per market session, written by the daily chain with the run's status, data checks, guard alert counts and data ages.
tags:
  - daily
  - engine
status: stable
---

# DAILY

Written by `scripts/cadence.py` after every run of the daily chain, newest first. One section per market session, and a rerun replaces its own section. A section names no symbol, quantity or amount; the detail is in the repo's `reports/` folder.

## Related

- [[notes-updates]] - the status write-ups made by hand; this page is the engine's own daily record
"""

_DATED = re.compile(r"^## (\d{4}-\d{2}-\d{2})\b")


def proof_status(report):
    """[(check, status)] from a data-proof report's table. The detail
    column names symbols, so it is left behind."""
    out = []
    for line in report.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if (line.startswith("|") and len(cells) >= 2
                and cells[1] in ("ok", "ATTENTION")):
            out.append((cells[0], cells[1]))
    return out


def guard_counts(report):
    """(new alerts, holdings flagged) from a guard report, counts only."""
    new, flagged, in_section = 0, 0, False
    for line in report.splitlines():
        if line.startswith("New today:"):
            body = line[len("New today:"):].strip()
            new = 0 if body == "no new alerts" else len(body.split(", "))
        elif line.startswith("## "):
            in_section = line.strip() == "## Needs attention"
        elif in_section and line.startswith("- ") and line.strip() != "- none":
            flagged += 1
    return new, flagged


def _age(label, stamp, today):
    if stamp is None:
        return f"{label} not built"
    days = (today - stamp).days
    return f"{label} {stamp} ({days} day{'' if days == 1 else 's'} old)"


def render_section(session, entry, proof, guard, ages, today,
                   last_covered=None, dry_run=False):
    """One session's section.

    entry: the run's ledger record. proof: proof_status() output, or None
    when no proof report exists for the session. guard: guard_counts()
    output, or None. ages: [(label, date or None)]. last_covered: the newest
    session a successful run covered, shown when this run failed.
    """
    passed = entry.get("exit_code") == 0
    failed_step = entry.get("step_failed")
    title = "passed" if passed else f"FAILED at {failed_step}"
    lines = [f"## {session} - {title}", ""]
    run = ("passed" if passed
           else f"failed at {failed_step} (exit {entry.get('exit_code')})")
    lines.append(f"- Run: {run}; started {entry.get('started')}, "
                 f"finished {entry.get('finished')}")
    if not passed:
        lines.append(f"- Last session covered: {last_covered or 'none'}")
    if proof:
        attention = [name for name, status in proof if status != "ok"]
        ok = len(proof) - len(attention)
        lines.append(f"- Data checks: {ok} of {len(proof)} ok"
                     + (f"; attention: {', '.join(attention)}"
                        if attention else ""))
    else:
        lines.append("- Data checks: not run")
    if guard is not None:
        new, flagged = guard
        lines.append(f"- Guard: {new} new alerts, {flagged} holdings flagged"
                     + (" (dry run: no alert recorded)" if dry_run else ""))
    else:
        lines.append("- Guard: not run")
    lines.append("- Data ages: " + "; ".join(
        _age(label, stamp, today) for label, stamp in ages))
    reports = []
    if proof:
        reports.append(f"`reports/data_proof_{session}.md`")
    if guard is not None:
        reports.append(f"`reports/guard_{session}.md`")
    lines.append("- Detail: " + (", ".join(reports) if reports
                                 else "`logs/cadence.log`"))
    return "\n".join(lines) + "\n"


def upsert_section(page, session, section):
    """`page` with `section` filed for `session`: newest first, above the
    closing Related section, replacing an earlier section for the same
    session (a rerun)."""
    lines = page.splitlines()
    starts = [i for i, line in enumerate(lines) if line.startswith("## ")]
    bounds = dict(zip(starts, starts[1:] + [len(lines)]))
    for start in starts:
        hit = _DATED.match(lines[start])
        if hit and hit.group(1) == session:
            del lines[start:bounds[start]]
            break
    at = len(lines)
    for i, line in enumerate(lines):
        if not line.startswith("## "):
            continue
        hit = _DATED.match(line)
        if hit is None or hit.group(1) < session:
            at = i
            break
    block = section.rstrip("\n").splitlines() + [""]
    return "\n".join(lines[:at] + block + lines[at:]).rstrip("\n") + "\n"
