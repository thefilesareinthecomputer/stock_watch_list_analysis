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

from common.jsonl import append_jsonl, read_jsonl

DEFAULT_LIMITS = {
    "drawdown": 0.20,          # fraction below the window high
    "dollar_floor": 1000.0,    # ...and at least this many dollars lost
    "portfolio_floor": 0.01,   # ...or at least this share of the book
    "single_stock_cap": 0.10,  # size line; funds are exempt
    "window": 126,             # sessions in the trailing high
    "stale_sessions": 2,       # sessions without a price = dead or renamed
}


def alert_factors(series, market, limits=None, days=21):
    """The 'why this alert' context for one holding, shown with every
    alert so the user weighs it calmly (R6) instead of reacting to a
    percentage:

    - speed_sigma: the last `days` sessions' move in units of the name's
      own typical move over that span, measured on the year BEFORE the
      window so a crash does not inflate its own yardstick. -3 is rare;
      -6 is a regime break for that name.
    - vs_market_21d: the name's move minus the market's over the same
      span. Near zero means the market moved it; far below means it is
      the name (or its sector).
    - confirmed: the drawdown line held on each of the last two closes,
      not just one print.
    """
    limits = {**DEFAULT_LIMITS, **(limits or {})}
    s = series.dropna()
    m = market.dropna()
    ret = s.iloc[-1] / s.iloc[-days - 1] - 1.0
    base = s.pct_change().iloc[-days - 252:-days].dropna()
    scale = base.std() * (days ** 0.5)
    m_ret = m.iloc[-1] / m.iloc[-days - 1] - 1.0
    dd = s / s.rolling(limits["window"], min_periods=1).max() - 1.0
    return {
        "ret_21d": ret,
        "speed_sigma": ret / scale if scale else None,
        "vs_market_21d": ret - m_ret,
        "confirmed": bool((dd.iloc[-2:] <= -limits["drawdown"]).all()),
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


# --- Episodes: each alert fires once per episode, re-arms on recovery. ---
# The event log lives OUTSIDE the warehouse (which a rebuild erases) as an
# append-only JSONL, like calls_log.jsonl: {date, symbol, alert, event}.

load_events = read_jsonl
append_events = append_jsonl


def fold_state(events):
    """(symbol, alert) -> 'fired' | 'rearmed': the latest event wins."""
    state = {}
    for e in events:
        state[(e["symbol"], e["alert"])] = e["event"]
    return state


def _recovered(alert, row, limits):
    if alert == "drawdown":
        return (row["drawdown"] is not None
                and row["drawdown"] > -limits["drawdown"] / 2)
    if alert == "size":
        return (row["weight"] is not None
                and row["weight"] < limits["single_stock_cap"] * 0.9)
    return alert not in row["alerts"]          # stale: clears when priced


def next_events(rows, state, today, limits=None):
    """(new events, [(symbol, alert) firing today]) for today's rows.

    An alert fires when raised and not already fired in this episode. A
    fired alert re-arms only after a real recovery - half the drawdown line
    back, or weight under 90% of the cap - so a name hovering at the line
    cannot alert every day.
    """
    limits = {**DEFAULT_LIMITS, **(limits or {})}
    events, firing = [], []
    for row in rows:
        sym = row["symbol"]
        for alert in row["alerts"]:
            if state.get((sym, alert)) != "fired":
                events.append({"date": today, "symbol": sym,
                               "alert": alert, "event": "fired"})
                firing.append((sym, alert))
        for (s, alert), status in state.items():
            if (s == sym and status == "fired" and alert not in row["alerts"]
                    and _recovered(alert, row, limits)):
                events.append({"date": today, "symbol": sym,
                               "alert": alert, "event": "rearmed"})
    return events, firing
