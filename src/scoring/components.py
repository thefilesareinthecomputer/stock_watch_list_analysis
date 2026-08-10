"""Score component percentiles - the single definition every caller uses.

DIRECTION CONTRACT
------------------
A higher percentile always means a better input, because `composite_score` sums
the percentiles and `composite_rank` orders by that sum DESC.

`PERCENT_RANK` assigns 0.0 to the FIRST row in the ordering, so:

    ORDER BY x ASC   -> largest x gets 1.0   -> use when LARGER x is better
    ORDER BY x DESC  -> smallest x gets 1.0  -> use when SMALLER x is better

This reads backwards to most people, and getting it wrong inverts the product in
silence. All four components shipped reversed: the pipeline surfaced the worst
30-day performer, the most expensive P/E, the most overbought RSI and the
weakest money flow as its top-ranked names. Every direction below is pinned by
tests/test_scoring.py, including a null fixture per component.

Defined here rather than inline because the expressions are used twice
(watchlist_ranked and daily_analytics) and a fix that lands in one place and
misses the other is how the original defect survived.

RSI and MFI remain here only to preserve current behaviour while the direction
bug is fixed. They are timing oscillators, not risk and quality measures, and
SPEC-RECOMMENDATION-ENGINE.md P4 replaces them with realized volatility and
fundamentals-derived quality.
"""

# Bump whenever a component's input, direction or weight changes. Snapshots are
# immutable per (as_of_date, methodology_version), so a bump writes new rows
# alongside the old ones instead of restating history. v1 is the first version
# whose directions are correct and tested; the inverted scoring that preceded it
# was never snapshotted, so no history is misattributed to v1.
METHODOLOGY_VERSION = "v1"

# name -> (ORDER BY argument, why this direction)
COMPONENTS = {
    # Larger 30-day return is better momentum.
    "momentum_pct": "COALESCE(change_30d_pct, 0) ASC",
    # Smaller positive P/E is better value. Non-positive P/E means the company
    # lost money, which is not a value signal, so it is folded into the missing
    # sentinel rather than ranking as the cheapest name in the universe.
    "value_pct": "COALESCE(CASE WHEN pe_ratio > 0 THEN pe_ratio END, 999) DESC",
    # Lower RSI is less overbought, so less exposed to a pullback.
    "risk_pct": "COALESCE(rsi, 50) DESC",
    # Higher money flow is stronger accumulation.
    "quality_pct": "COALESCE(mfi, 50) ASC",
}


def percentile_sql(partition_by=None, indent=16):
    """Return the component PERCENT_RANK expressions as a SQL select fragment.

    partition_by: column to rank within (e.g. "as_of_date" for a time series).
                  None ranks across the whole input, for a single-date snapshot.
    """
    partition = f"PARTITION BY {partition_by} " if partition_by else ""
    sep = ",\n" + " " * indent
    return sep.join(
        f"PERCENT_RANK() OVER ({partition}ORDER BY {order}) AS {name}"
        for name, order in COMPONENTS.items()
    )
