"""Prospective trial log: every evaluation counted BEFORE its result exists.

Without the count of trials attempted, the best of N variants cannot be
distinguished from the luckiest of N, and no Sharpe can be deflated
(Bailey & Lopez de Prado 2014). The log therefore records the attempt, not
the outcome: a trial abandoned halfway still counts, and the API has no
field for a result at all, which is what makes retrofitting impossible to
fake.

The log is a tracked, append-only JSONL at the repo root - deliberately NOT
in `warehouse/`, which is gitignored and regenerable. Evidence must survive
a warehouse rebuild and travel between devices; a log that dies with a
scratch database counts nothing.

Re-running an unchanged evaluation logs a new trial. That over-counts, which
is the conservative direction: deflation gets stricter, never looser. There
is intentionally no opt-out flag - an escape hatch to avoid "wasting" trials
is how the count stops being honest.

Intents are free text and the file is public in the repo: describe the
methodology question, never positions or private context.
"""
import json
import os
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TRIAL_LOG = os.path.join(ROOT, "trial_log.jsonl")


def log_trial(table, signal_col, horizons, cost_bps, intent, path=TRIAL_LOG):
    """Append one trial and return its monotonically increasing number."""
    number = trial_count(path) + 1
    entry = {
        "trial": number,
        "logged_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "table": table,
        "signal": signal_col,
        "horizons": list(horizons),
        "cost_bps": cost_bps,
        "intent": intent,
    }
    with open(path, "a") as f:
        f.write(json.dumps(entry) + "\n")
    return number


def trial_count(path=TRIAL_LOG):
    """How many trials have ever been attempted. Queryable, monotonic."""
    if not os.path.exists(path):
        return 0
    with open(path) as f:
        return sum(1 for line in f if line.strip())


def read_trials(path=TRIAL_LOG):
    """The full log as a list of dicts, oldest first."""
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]
