"""The append-only JSONL logs (calls, trials, trades, guard events, cadence
ledger) share one reader and one appender."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common.jsonl import append_jsonl, read_jsonl


def test_missing_log_reads_as_empty(tmp_path):
    assert read_jsonl(tmp_path / "none.jsonl") == []


def test_append_never_rewrites_and_blank_lines_are_skipped(tmp_path):
    path = tmp_path / "log.jsonl"
    append_jsonl(path, [{"b": 1, "a": 2}])
    with open(path, "a") as f:
        f.write("\n")
    append_jsonl(path, [{"c": 3}, {"d": 4}])
    assert read_jsonl(path) == [{"a": 2, "b": 1}, {"c": 3}, {"d": 4}]


def test_key_order_is_preserved_unless_sorting_is_asked_for(tmp_path):
    # Existing logs differ: some were written sorted, some not. A shared
    # helper must not silently change a log's line format.
    plain, ordered = tmp_path / "plain.jsonl", tmp_path / "sorted.jsonl"
    append_jsonl(plain, [{"b": 1, "a": 2}], sort_keys=False)
    append_jsonl(ordered, [{"b": 1, "a": 2}])
    assert open(plain).read() == '{"b": 1, "a": 2}\n'
    assert open(ordered).read() == '{"a": 2, "b": 1}\n'
