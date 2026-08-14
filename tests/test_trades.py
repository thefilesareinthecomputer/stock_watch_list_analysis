"""Trade journal parsing, reconciliation, FIFO lot math, loss harvest."""
import json
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import duckdb
import pytest

from common.trades import (
    load_trades, loss_harvest, open_lots, parse_trades, reconcile,
    share_counts,
)


def _line(**overrides):
    entry = {"date": "2026-08-12", "symbol": "xyz", "account": "brokerage",
             "side": "buy", "qty": 10}
    entry.update(overrides)
    return json.dumps(entry)


def test_parse_valid_entry_uppercases_and_defaults():
    (t,) = parse_trades([_line()])
    assert t["symbol"] == "XYZ" and t["qty"] == 10.0
    assert t["price"] is None and t["seed"] is False and t["note"] == ""


def test_parse_skips_blank_lines():
    assert len(parse_trades(["", _line(), "  "])) == 1


@pytest.mark.parametrize("bad, match", [
    ({"side": "short"}, "side"),
    ({"qty": 0}, "qty"),
    ({"qty": -3}, "qty"),
    ({"price": 0}, "price"),
    ({"date": "08/12/2026"}, "date"),
    ({"seed": True, "side": "sell"}, "seed"),
    ({"seed": True}, "basis"),
])
def test_parse_rejects_defects_loudly(bad, match):
    with pytest.raises(ValueError, match=match):
        parse_trades([_line(**bad)])


def test_parse_rejects_missing_fields_and_non_json():
    with pytest.raises(ValueError, match="missing"):
        parse_trades(['{"date": "2026-08-12"}'])
    with pytest.raises(ValueError, match="not JSON"):
        parse_trades(["XYZ 10 bought"])


def test_load_trades_empty_without_file(tmp_path):
    assert load_trades(str(tmp_path / "trades.jsonl")) == []


def test_share_counts_net_per_account():
    trades = parse_trades([
        _line(qty=10), _line(qty=5),
        _line(side="sell", qty=3),
        _line(account="roth", qty=2),
    ])
    counts = share_counts(trades)
    assert counts[("brokerage", "XYZ")] == pytest.approx(12.0)
    assert counts[("roth", "XYZ")] == pytest.approx(2.0)


def test_reconcile_warns_on_mismatch_only():
    trades = parse_trades([_line(qty=10)])
    positions = [{"account": "brokerage", "symbol": "XYZ", "quantity": 10.0},
                 {"account": "roth", "symbol": "VFIAX", "quantity": 3.5}]
    assert reconcile(trades, positions) == []  # match + journal-absent: quiet
    positions[0]["quantity"] = 8.0
    (warning,) = reconcile(trades, positions)
    assert "journal nets to 10" in warning and "8" in warning


def test_reconcile_warns_on_oversold_and_unknown_symbol():
    trades = parse_trades([_line(side="sell", qty=4, symbol="abc"),
                           _line(qty=5, symbol="new")])
    warnings = reconcile(trades, [])
    assert any("more sold than bought" in w for w in warnings)
    assert any("not in POSITIONS.md" in w for w in warnings)


def test_open_lots_fifo_consumes_oldest_first():
    trades = parse_trades([
        _line(date="2026-08-01", qty=10, price=5.0),
        _line(date="2026-08-05", qty=10, price=10.0),
        _line(date="2026-08-10", side="sell", qty=12),
    ])
    lots = open_lots(trades)[("brokerage", "XYZ")]
    assert len(lots) == 1
    assert lots[0]["qty"] == pytest.approx(8.0)
    assert lots[0]["price"] == pytest.approx(10.0)


def test_open_lots_oversell_demands_a_seed():
    trades = parse_trades([_line(side="sell", qty=1)])
    with pytest.raises(ValueError, match="seed"):
        open_lots(trades)


def test_open_lots_keeps_seed_flag_and_date():
    trades = parse_trades([_line(date="2024-03-01", qty=7, price=42.5,
                                 seed=True)])
    (lot,) = open_lots(trades)[("brokerage", "XYZ")]
    assert lot["seed"] is True and lot["date"] == "2024-03-01"
    assert lot["price"] == pytest.approx(42.5)


TODAY = date(2026, 8, 12)


def _harvest_con(close=30.0, deteriorating=True):
    con = duckdb.connect()
    con.execute("CREATE TABLE gold_line_of_sight "
                "(symbol VARCHAR, close DOUBLE, deteriorating BOOLEAN)")
    con.execute("INSERT INTO gold_line_of_sight VALUES ('XYZ', ?, ?)",
                [close, deteriorating])
    con.execute("CREATE TABLE bronze_prices (symbol VARCHAR, date DATE, "
                "close DOUBLE)")
    con.execute("INSERT INTO bronze_prices VALUES "
                "('XYZ', DATE '2026-08-11', ?)", [close])
    return con


def test_harvest_flags_below_basis_brokerage_lot():
    trades = parse_trades([_line(date="2024-03-01", qty=7, price=50.0,
                                 seed=True)])
    (flag,) = loss_harvest(_harvest_con(close=30.0), trades, today=TODAY)
    assert flag["symbol"] == "XYZ" and flag["account"] == "brokerage"
    assert flag["loss_pct"] == pytest.approx(-0.4)
    assert flag["long_term"] is True and flag["deteriorating"] is True
    assert flag["wash_sale"] is False


def test_harvest_never_flags_tax_advantaged_accounts():
    trades = parse_trades([
        _line(account="Roth IRA", date="2024-03-01", qty=7, price=50.0,
              seed=True),
        _line(account="Traditional IRA", date="2024-03-01", qty=7,
              price=50.0, seed=True),
    ])
    assert loss_harvest(_harvest_con(close=30.0), trades, today=TODAY) == []


def test_harvest_skips_lots_above_basis():
    trades = parse_trades([_line(date="2024-03-01", qty=7, price=20.0,
                                 seed=True)])
    assert loss_harvest(_harvest_con(close=30.0), trades, today=TODAY) == []


def test_harvest_recent_buy_raises_wash_sale_flag():
    trades = parse_trades([
        _line(date="2024-03-01", qty=7, price=50.0, seed=True),
        _line(date="2026-08-01", qty=1, price=31.0),  # inside 30 days
    ])
    flags = loss_harvest(_harvest_con(close=30.0), trades, today=TODAY)
    assert all(f["wash_sale"] for f in flags)


def test_harvest_short_term_lot_is_labeled():
    trades = parse_trades([_line(date="2026-06-01", qty=7, price=50.0,
                                 seed=True)])
    (flag,) = loss_harvest(_harvest_con(close=30.0), trades, today=TODAY)
    assert flag["long_term"] is False


def test_harvest_price_prefers_bronze_over_screening_table():
    # The screening table can lag by RESUME_MAX_AGE_DAYS; bronze is the
    # evidence contract, so its latest close wins while the sight-only
    # deteriorating flag is kept.
    con = _harvest_con(close=30.0)
    con.execute("INSERT INTO bronze_prices VALUES "
                "('XYZ', DATE '2026-08-12', 25.0)")
    trades = parse_trades([_line(date="2024-03-01", qty=7, price=50.0,
                                 seed=True)])
    (flag,) = loss_harvest(con, trades, today=TODAY)
    assert flag["price"] == pytest.approx(25.0)
    assert flag["deteriorating"] is True


def test_harvest_infers_missing_basis_from_trade_date_close():
    con = _harvest_con(close=30.0)
    con.execute("INSERT INTO bronze_prices VALUES "
                "('XYZ', DATE '2026-05-01', 45.0)")
    trades = parse_trades([_line(date="2026-05-02", qty=3)])  # no price
    (flag,) = loss_harvest(con, trades, today=TODAY)
    assert flag["basis"] == pytest.approx(45.0)
