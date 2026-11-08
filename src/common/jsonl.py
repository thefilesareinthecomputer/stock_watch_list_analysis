"""Append-only JSONL logs: one reader, one appender.

Used by the calls log, the trial log, the trade journal, guard events and
the cadence ledger. Appending never rewrites an earlier line - these logs
are evidence.
"""
import json
import os


def read_jsonl(path):
    """All records, oldest first; empty when the log does not exist.

    A torn FINAL line (a run killed mid-append) is skipped with a printed
    warning so later runs keep working; a bad line anywhere else raises,
    because that is corruption, not an interrupted write.
    """
    if not os.path.exists(path):
        return []
    with open(path) as f:
        lines = [line for line in f if line.strip()]
    records = [json.loads(line) for line in lines[:-1]]
    if lines:
        try:
            records.append(json.loads(lines[-1]))
        except json.JSONDecodeError:
            print(f"WARNING: skipped a torn final line in {path}")
    return records


def append_jsonl(path, records, sort_keys=True):
    """Append records. `sort_keys=False` keeps insertion order for logs
    that were always written that way, so a line's format never changes
    mid-file."""
    with open(path, "a") as f:
        for record in records:
            f.write(json.dumps(record, sort_keys=sort_keys) + "\n")
