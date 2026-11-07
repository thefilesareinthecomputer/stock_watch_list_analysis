"""Portfolio guard: which holdings need the user's attention today.

Spec: tasks/SPEC-GUARD.md (private). The guard covers EVERY holding,
whatever the engine's call state, and it informs - it never says sell.

The dollar gate is the point (tasks/conference/REQUIREMENTS.md R3a,
replay 2026-11-07): a percentage line alone fired ~105 times a year across
held names, almost all on small cheap positions. Requiring the loss from
the high to also clear a dollar or portfolio-share floor cut that to ~7 a
year and kept the one that mattered.

Thresholds are proposals until the user rules on knowledge/RISK-LIMITS.md.
"""
import pandas as pd

DEFAULT_LIMITS = {
    "drawdown": 0.20,          # fraction below the window high
    "dollar_floor": 1000.0,    # ...and at least this many dollars lost
    "portfolio_floor": 0.01,   # ...or at least this share of the book
    "single_stock_cap": 0.10,  # size line; funds are exempt
    "window": 126,             # sessions in the trailing high
    "stale_sessions": 2,       # sessions without a price = dead or renamed
}


def evaluate_holdings(px, quantities, quote_types, limits=None):
    """One row per holding: value, weight, drawdown, dollars lost from the
    window high, and the alerts it raises ('drawdown', 'size', 'stale').

    px: DataFrame of split-adjusted closes, index = session dates (the
    market calendar), one column per symbol. quantities: symbol -> shares.
    quote_types: symbol -> 'EQUITY' / 'ETF' / 'MUTUALFUND'; only EQUITY is
    held to the single-stock size line.
    """
    limits = {**DEFAULT_LIMITS, **(limits or {})}
    sessions = px.index
    rows = []
    for symbol, qty in quantities.items():
        series = px[symbol].dropna() if symbol in px else pd.Series(dtype=float)
        row = {"symbol": symbol, "quantity": qty, "value": None,
               "weight": None, "drawdown": None, "lost_from_high": None,
               "alerts": []}
        missed = (len(sessions) if series.empty
                  else int((sessions > series.index[-1]).sum()))
        if missed >= limits["stale_sessions"]:
            row["alerts"].append("stale")
            rows.append(row)
            continue
        close = float(series.iloc[-1])
        high = float(series.iloc[-limits["window"]:].max())
        row["value"] = qty * close
        row["drawdown"] = close / high - 1.0
        row["lost_from_high"] = qty * (high - close)
        rows.append(row)

    total = sum(r["value"] for r in rows if r["value"] is not None)
    for row in rows:
        if row["value"] is None or not total:
            continue
        row["weight"] = row["value"] / total
        big_enough = (row["lost_from_high"] >= limits["dollar_floor"]
                      or row["lost_from_high"] / total
                      >= limits["portfolio_floor"])
        if row["drawdown"] <= -limits["drawdown"] and big_enough:
            row["alerts"].append("drawdown")
        if (quote_types.get(row["symbol"]) == "EQUITY"
                and row["weight"] > limits["single_stock_cap"]):
            row["alerts"].append("size")
    return rows
