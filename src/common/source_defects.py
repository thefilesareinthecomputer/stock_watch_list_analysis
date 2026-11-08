"""Source defects ruled on after investigation (source_defects.json).

A check that fails for one of these symbols is reported as known, with the
ruling date, instead of as attention. The list is tracked and names only
the source's behavior and our treatment; adding to it means a human looked
and decided, which the note records.
"""
import json
import os

PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "source_defects.json")


def load_defects(path=PATH):
    with open(path) as f:
        return json.load(f)["defects"]


def known_defects(check, path=PATH):
    """{symbol: entry} for the defects ruled on for one check name."""
    return {d["symbol"]: d for d in load_defects(path) if d["check"] == check}
