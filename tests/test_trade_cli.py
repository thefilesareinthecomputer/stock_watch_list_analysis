"""scripts/trade.py: one command appends the journal and updates the
holdings snapshot, so a fill is never hand-written into two files."""
import importlib.util
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common.positions import apply_fill, parse_positions  # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "trade", os.path.join(os.path.dirname(__file__), "..", "scripts", "trade.py"))
trade = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(trade)

POSITIONS = "# ROTH STOCK\nABC 4\n\n# BROKERAGE STOCK\nXYZ 10\nDEF 2.5\n"


def test_apply_fill_buy_adds_to_existing_line():
    out = apply_fill(POSITIONS, "BROKERAGE STOCK", "XYZ", 3)
    assert "XYZ 13" in out and "DEF 2.5" in out and "ABC 4" in out


def test_apply_fill_buy_new_symbol_lands_under_its_account():
    out = apply_fill(POSITIONS, "BROKERAGE STOCK", "NEW", 1.5)
    pos = parse_positions(out)
    assert {"account": "BROKERAGE STOCK", "symbol": "NEW", "quantity": 1.5} in pos
    assert out.index("NEW 1.5") > out.index("# BROKERAGE STOCK")


def test_apply_fill_new_account_is_appended():
    out = apply_fill(POSITIONS, "HSA", "VTI", 2)
    assert out.rstrip().endswith("# HSA\nVTI 2")


def test_apply_fill_sell_to_zero_removes_line():
    out = apply_fill(POSITIONS, "BROKERAGE STOCK", "XYZ", -10)
    assert "XYZ" not in out and "DEF 2.5" in out


def test_apply_fill_sell_partial_keeps_fraction():
    out = apply_fill(POSITIONS, "BROKERAGE STOCK", "DEF", -1)
    assert "DEF 1.5" in out


def test_apply_fill_oversell_raises():
    with pytest.raises(ValueError, match="holds 10"):
        apply_fill(POSITIONS, "BROKERAGE STOCK", "XYZ", -11)
    with pytest.raises(ValueError, match="not held"):
        apply_fill(POSITIONS, "BROKERAGE STOCK", "NOPE", -1)


def _paths(tmp_path):
    positions = tmp_path / "POSITIONS.md"
    positions.write_text(POSITIONS)
    return str(tmp_path / "trades.jsonl"), str(positions)


def test_record_appends_journal_and_updates_positions(tmp_path):
    trades_path, positions_path = _paths(tmp_path)
    entry = trade.record("buy", "xyz", 3, "BROKERAGE STOCK", "2026-08-21",
                         price=12.5, note="per round 08-10",
                         trades_path=trades_path, positions_path=positions_path)
    assert entry["symbol"] == "XYZ" and entry["price"] == 12.5
    (line,) = open(trades_path).read().splitlines()
    assert json.loads(line) == entry and "seed" not in entry
    assert "XYZ 13" in open(positions_path).read()


def test_record_sell_without_price_omits_it(tmp_path):
    trades_path, positions_path = _paths(tmp_path)
    entry = trade.record("sell", "XYZ", 10, "BROKERAGE STOCK", "2026-08-21",
                         trades_path=trades_path, positions_path=positions_path)
    assert "price" not in entry
    assert "XYZ" not in open(positions_path).read()


def test_record_rejects_before_writing_anything(tmp_path):
    trades_path, positions_path = _paths(tmp_path)
    with pytest.raises(ValueError):
        trade.record("sell", "XYZ", 99, "BROKERAGE STOCK", "2026-08-21",
                     trades_path=trades_path, positions_path=positions_path)
    assert not os.path.exists(trades_path)
    assert open(positions_path).read() == POSITIONS


RELAY = """# seeds
```jsonl
{"date": "2024-03-15", "symbol": "XYZ", "account": "BROKERAGE STOCK", "side": "buy", "qty": 10, "price": 40.0, "seed": true}
{"date": "2025-01-15", "symbol": "DEF", "account": "BROKERAGE STOCK", "side": "buy", "qty": 2.5, "price": 9.0, "seed": true}
```
"""


def test_seed_installs_relay_block_once(tmp_path):
    trades_path, positions_path = _paths(tmp_path)
    relay = tmp_path / "_relay.md"
    relay.write_text(RELAY)
    n = trade.seed(str(relay), trades_path=trades_path,
                   positions_path=positions_path)
    assert n == 2
    assert len(open(trades_path).read().splitlines()) == 2
    assert open(positions_path).read() == POSITIONS  # seeds describe held lots
    with pytest.raises(ValueError, match="already"):
        trade.seed(str(relay), trades_path=trades_path,
                   positions_path=positions_path)


def test_seed_refuses_placeholders_and_snapshot_mismatch(tmp_path):
    trades_path, positions_path = _paths(tmp_path)
    relay = tmp_path / "_relay.md"
    relay.write_text(RELAY.replace("40.0", "FILL_PRICE"))
    with pytest.raises(ValueError, match="FILL_"):
        trade.seed(str(relay), trades_path=trades_path,
                   positions_path=positions_path)
    relay.write_text(RELAY.replace('"qty": 10', '"qty": 7'))
    with pytest.raises(ValueError, match="POSITIONS.md says 10"):
        trade.seed(str(relay), trades_path=trades_path,
                   positions_path=positions_path)
    assert not os.path.exists(trades_path)


def test_cli_end_to_end(tmp_path, monkeypatch, capsys):
    trades_path, positions_path = _paths(tmp_path)
    monkeypatch.setattr(trade, "TRADES_LOG", trades_path)
    monkeypatch.setattr(trade, "POSITIONS_PATH", positions_path)
    monkeypatch.setattr(trade, "WAREHOUSE", str(tmp_path / "none.duckdb"))
    trade.main(["buy", "abc", "2", "--account", "ROTH STOCK",
                "--date", "2026-08-21"])
    assert "ABC 6" in open(positions_path).read()
    trade.main(["show"])
    out = capsys.readouterr().out
    assert "ROTH STOCK ABC" in out and "2 sh" in out
