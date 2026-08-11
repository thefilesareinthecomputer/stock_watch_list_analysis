"""Forward returns: what actually happened after each signal date.

Grain of `backtest_forward_returns`: (symbol, as_of_date, horizon). The
return is measured from the NEXT session's open to the open `horizon`
sessions after that. Data lands after the close, so the earliest honest
action on a signal dated D is the open of D+1; a same-close fill
manufactures edge that cannot be traded (SPEC-LOCAL-WAREHOUSE).

Prices are our own back-adjusted series (splits and dividends), so the
ratio of adjusted opens approximates total return. Rows whose exit falls
past the end of history are absent rather than filled: a truncated horizon
is a gap, never a fabricated return.

Excess is against the benchmark's forward return over the identical
(as_of_date, horizon) window, so symbol and benchmark always face the same
calendar.

DuckDB-only by design: local computes, Databricks serves (SPEC.md).
"""

HORIZONS = (21, 63, 126, 252)  # 126 is the provisional forecast window
BENCHMARK = "SPY"


def build_forward_returns(con, horizons=HORIZONS, benchmark=BENCHMARK):
    """Build backtest_forward_returns from silver_adjusted_prices."""
    horizon_values = ", ".join(f"({h})" for h in horizons)
    con.execute(f"""
        CREATE OR REPLACE TABLE backtest_forward_returns AS
        WITH ordered AS (
            SELECT symbol, date, adj_open,
                   ROW_NUMBER() OVER (
                       PARTITION BY symbol ORDER BY date) AS rn
            FROM silver_adjusted_prices
        ),
        horizons(horizon) AS (VALUES {horizon_values}),
        -- Inner joins on session offsets: an as_of whose entry or exit
        -- falls past the end of history simply produces no row.
        returns AS (
            SELECT o.symbol, o.date AS as_of_date, h.horizon,
                   e.date AS entry_date, x.date AS exit_date,
                   x.adj_open / e.adj_open - 1 AS fwd_return
            FROM ordered o
            CROSS JOIN horizons h
            JOIN ordered e
              ON e.symbol = o.symbol AND e.rn = o.rn + 1
            JOIN ordered x
              ON x.symbol = o.symbol AND x.rn = o.rn + 1 + h.horizon
            WHERE e.adj_open > 0
        ),
        bench AS (
            SELECT as_of_date, horizon, fwd_return AS benchmark_return
            FROM returns WHERE symbol = '{benchmark}'
        )
        SELECT r.*, b.benchmark_return,
               r.fwd_return - b.benchmark_return AS excess_return
        FROM returns r
        LEFT JOIN bench b USING (as_of_date, horizon)
    """)
