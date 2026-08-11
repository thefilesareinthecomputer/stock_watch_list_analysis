"""Trial log: monotonic, append-only, and blind to results by construction."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from backtest.trials import log_trial, read_trials, trial_count


def test_count_starts_at_zero_and_is_monotonic(tmp_path):
    path = str(tmp_path / "log.jsonl")
    assert trial_count(path) == 0
    numbers = [log_trial("t", "s", (126,), 10.0, f"attempt {i}", path=path)
               for i in range(5)]
    assert numbers == [1, 2, 3, 4, 5]
    assert trial_count(path) == 5


def test_log_is_append_only_and_ordered(tmp_path):
    path = str(tmp_path / "log.jsonl")
    log_trial("silver_signals", "rsi", (21, 63), 10.0, "first", path=path)
    log_trial("gold_candidate_signals", "roe_pct", (126,), 0.0, "second",
              path=path)

    trials = read_trials(path)
    assert [t["trial"] for t in trials] == [1, 2]
    assert trials[0]["intent"] == "first"
    assert trials[1]["horizons"] == [126]
    assert all("logged_at" in t for t in trials)


def test_entry_has_no_result_field(tmp_path):
    # The schema records the attempt only: a trial is counted before any
    # outcome exists, so there is nothing to retrofit.
    path = str(tmp_path / "log.jsonl")
    log_trial("t", "s", (126,), 10.0, "no outcome yet", path=path)
    entry = read_trials(path)[0]
    assert set(entry) == {"trial", "logged_at", "table", "signal",
                          "horizons", "cost_bps", "intent"}


def test_abandoned_evaluation_still_counts(tmp_path):
    # Log first, evaluate second: a crash between the two leaves the trial
    # in the count, which is the entire point.
    path = str(tmp_path / "log.jsonl")
    log_trial("t", "s", (126,), 10.0, "will be abandoned", path=path)
    try:
        raise RuntimeError("evaluation blew up")
    except RuntimeError:
        pass
    assert trial_count(path) == 1
