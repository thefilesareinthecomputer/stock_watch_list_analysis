"""Assemble evaluation frames: a signal joined to what happened next.

Evaluation dates are MONTHLY - the last trading session of each month that
has a settled forward window for the benchmark. Daily evaluation would
overlap the forecast windows almost entirely, making consecutive
observations nearly identical and the t-stat a fiction; monthly matches the
rebalance cadence settled in the parent spec.

Benchmarks are excluded from the evaluated universe: they are the yardstick,
not contestants.
"""
import re

from backtest.returns import BENCHMARK
from common.config import BENCHMARK_TICKERS

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def evaluation_frame(con, table, signal_col, horizon,
                     exclude=tuple(BENCHMARK_TICKERS)):
    """(symbol, as_of_date, signal, fwd_return, excess_return) at one horizon."""
    for ident in (table, signal_col):
        if not _IDENT.match(ident):
            raise ValueError(f"invalid identifier: {ident}")
    placeholders = ", ".join("?" for _ in exclude)
    return con.execute(f"""
        WITH eval_dates AS (
            SELECT MAX(as_of_date) AS as_of_date
            FROM backtest_forward_returns
            WHERE symbol = ? AND horizon = ?
            GROUP BY DATE_TRUNC('month', as_of_date)
        )
        SELECT s.symbol, s.as_of_date, s.{signal_col} AS signal,
               r.fwd_return, r.excess_return
        FROM {table} s
        JOIN eval_dates d ON s.as_of_date = d.as_of_date
        JOIN backtest_forward_returns r
          ON r.symbol = s.symbol AND r.as_of_date = s.as_of_date
         AND r.horizon = ?
        WHERE s.{signal_col} IS NOT NULL
          AND s.symbol NOT IN ({placeholders})
        ORDER BY s.as_of_date, s.symbol
    """, [BENCHMARK, horizon, horizon, *exclude]).df()


CAVEATS = """\
CAVEATS (properties of the data, not removable by engineering):
  - Survivorship: the universe is today's survivors; levels are biased up
    and apparent persistence is manufactured. Compare variants, not levels.
  - Universe selection is forward-looking in backtests: the broad tier was
    chosen by TODAY'S dollar volume and applied across history (one
    membership snapshot so far; PIT reconstruction begins 2026-08).
    Historical IC and excess are inflated by it.
  - No go-live threshold comes from these numbers. Evidence for real money
    is the forward paper track (SPEC-RECOMMENDATION-ENGINE)."""
