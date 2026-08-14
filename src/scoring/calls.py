"""Buy/sell call state machine: hysteresis, because bare thresholds churn.

Cross-sectional percentiles jiggle daily, so entering and exiting on the
same line generates turnover with no information (SPEC-SIGNAL-TIERS §4).
A call therefore depends on the prior call - enter only on a high rank,
exit only on a substantially worse one - which makes it a state machine
per symbol, not a stateless label.

Thresholds live in the registry's `calls` block, never here: changing them
is a recorded event (SPEC-BUY-SELL-CALLS "Design").

THE DURABLE RECORD IS THE JSONL, NOT THE WAREHOUSE. `warehouse/` is
gitignored and regenerable, so an append-only table there would die with
the next rebuild - the same argument that puts the trial log at the repo
root. calls_log.jsonl is append-only, first-write-wins per
(as_of_date, methodology_version), and gitignored because it names
watchlist symbols, which never reach the public repo. `gold_calls` is
rebuilt from it and is only a queryable view of that evidence.
"""
import json
import os

import pandas as pd

from common.config import BENCHMARK_TICKERS
from scoring.variants import component_sql
from scoring.tiers import scored_variant

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CALLS_LOG = os.path.join(ROOT, "calls_log.jsonl")

CALL_STATES = ("none", "buy", "hold", "sell")

# Prior states from which the symbol is currently held. `buy` is the entry
# event, `hold` the continued stance; `sell` is the exit event, `none` the
# continued absence.
_IN_POSITION = ("buy", "hold")


def next_call(prior, percentile, calls_cfg):
    """One transition of the per-symbol state machine.

    | Prior state | Condition (score percentile) | Call |
    |-------------|------------------------------|------|
    | none / sell | >= enter_percentile          | buy  |
    | buy / hold  | >= exit_percentile           | hold |
    | buy / hold  | <  exit_percentile           | sell |
    | none / sell | <  enter_percentile          | none |
    """
    if prior not in CALL_STATES:
        raise ValueError(f"unknown prior call state: {prior}")
    if percentile is None or not 0.0 <= percentile <= 1.0:
        raise ValueError(f"percentile must be in [0, 1], got {percentile}")
    if prior in _IN_POSITION:
        return "hold" if percentile >= calls_cfg["exit_percentile"] else "sell"
    return "buy" if percentile >= calls_cfg["enter_percentile"] else "none"


def round_scores(con, registry, dates, benchmarks=tuple(BENCHMARK_TICKERS)):
    """Score the call-eligible universe at the given as_of_dates.

    Returns one row per (symbol, as_of_date): per-component percentiles,
    composite score and rank, and `score_percentile` - the cross-sectional
    PERCENT_RANK of the composite (ascending, so the best score reads 1.0;
    direction pinned by test, per plan gotcha 0) that the state machine
    compares against the enter/exit thresholds.

    Eligibility is decided BEFORE ranking, so yardsticks never occupy call
    percentiles: benchmarks out, EDGAR non-operating SICs out (commodity
    trusts), quoteType ETFs out (plain ETFs never file CompanyFacts). The
    exclusion tables must exist - a missing bronze_security fails loudly
    rather than silently emitting calls for ETFs.
    """
    from common.edgar import NON_OPERATING_SIC
    if not dates:
        raise ValueError("round_scores needs at least one as_of_date")
    comps = scored_variant(registry)["components"]
    val_exprs, rank_ctes, rank_joins, weighted, total_weight = \
        component_sql(comps)
    pct_cols = ", ".join(
        f"COALESCE(pct_{i}, 0.5) AS {c['name']}_pct"
        for i, c in enumerate(comps))
    date_values = ", ".join("(CAST(? AS TIMESTAMP))" for _ in dates)
    bench = ", ".join("?" for _ in benchmarks)
    non_op = ", ".join("?" for _ in NON_OPERATING_SIC)
    return con.execute(f"""
        WITH round_dates(as_of_date) AS (VALUES {date_values}),
        inputs AS (
            SELECT s.*, c.earnings_yield, c.gross_profitability, c.roe
            FROM silver_signals s
            LEFT JOIN gold_candidate_signals c USING (symbol, as_of_date)
            JOIN round_dates d ON s.as_of_date = d.as_of_date
            WHERE s.symbol NOT IN ({bench})
              AND s.symbol NOT IN (SELECT symbol FROM bronze_entity
                                   WHERE COALESCE(sic, '') IN ({non_op}))
              AND s.symbol NOT IN (SELECT symbol FROM bronze_security
                                   WHERE quote_type = 'ETF')
        ),
        vals AS (
            SELECT symbol, as_of_date,
                   {val_exprs}
            FROM inputs
        ),{rank_ctes},
        scored AS (
            SELECT v.symbol, v.as_of_date, {pct_cols},
                   ({weighted}) / {total_weight} AS composite_score
            FROM vals v
            {rank_joins}
        )
        SELECT *,
               RANK() OVER (PARTITION BY as_of_date
                            ORDER BY composite_score DESC) AS composite_rank,
               PERCENT_RANK() OVER (PARTITION BY as_of_date
                                    ORDER BY composite_score ASC)
                   AS score_percentile
        FROM scored
        ORDER BY as_of_date, composite_rank
    """, [*dates, *benchmarks, *NON_OPERATING_SIC]).df()


def simulate_calls(scores, calls_cfg):
    """Run the state machine over a multi-date round_scores frame, in date
    order, every symbol starting at `none`.

    This is the historical replay behind success criteria 1 and 9: the
    same transition function the live rounds use, over the walk-forward
    period. A symbol absent on a date (not yet listed, or gone) keeps its
    state and produces no row - a gap, never a fabricated call.
    """
    state = {}
    rows = []
    for date in sorted(scores["as_of_date"].unique()):
        frame = scores[scores["as_of_date"] == date]
        for row in frame.itertuples():
            prior = state.get(row.symbol, "none")
            call = next_call(prior, row.score_percentile, calls_cfg)
            state[row.symbol] = call
            rows.append({"symbol": row.symbol, "as_of_date": date,
                         "prior_call": prior, "call": call,
                         "score_percentile": row.score_percentile})
    return pd.DataFrame(rows)


def build_round(scores, prior_calls, registry, expectation, run_id,
                created_ts):
    """Assemble one round entry from a single-date round_scores frame.

    prior_calls maps symbol -> its call in the previous round; symbols
    absent from it start at `none` (spec open question 4, ruled: the paper
    record inherits no simulated state).
    """
    dates = scores["as_of_date"].unique()
    if len(dates) != 1:
        raise ValueError(f"a round is one as_of_date, got {len(dates)}")
    pct_cols = [c for c in scores.columns if c.endswith("_pct")]
    cfg = registry["calls"]
    calls = []
    for row in scores.itertuples():
        prior = prior_calls.get(row.symbol, "none")
        calls.append({
            "symbol": row.symbol,
            "score": float(row.composite_score),
            "rank": int(row.composite_rank),
            "score_percentile": float(row.score_percentile),
            "component_percentiles": {c: float(getattr(row, c))
                                      for c in pct_cols},
            "prior_call": prior,
            "call": next_call(prior, row.score_percentile, cfg),
        })
    return {
        "as_of_date": str(dates[0])[:10],
        "methodology_version": registry["methodology_version"],
        "run_id": run_id,
        "created_ts": created_ts,
        "expectation": expectation,
        "calls": calls,
    }


def read_rounds(path=CALLS_LOG):
    """All recorded rounds, oldest first."""
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def latest_calls(rounds):
    """symbol -> call state folded across ALL rounds oldest-first, for the
    next round's prior_calls. A symbol absent from a round (Yahoo hole,
    tier churn) KEEPS its state - a gap, never a reset - matching
    simulate_calls; reading only the last round would let a held name
    re-enter as a fresh buy with no sell ever recorded."""
    state = {}
    for r in rounds:
        for c in r["calls"]:
            state[c["symbol"]] = c["call"]
    return state


def emit_round(round_entry, path=CALLS_LOG):
    """Append one round; first write wins per (as_of_date,
    methodology_version). Returns False (a no-op) if the round is already
    recorded - re-runs must never restate history, exactly as in
    scoring.snapshot. A partially-different re-run is also a no-op: topping
    up a recorded round would attach one run's evidence to another's.
    """
    key = (round_entry["as_of_date"], round_entry["methodology_version"])
    recorded = {(r["as_of_date"], r["methodology_version"])
                for r in read_rounds(path)}
    if key in recorded:
        return False
    with open(path, "a") as f:
        f.write(json.dumps(round_entry, sort_keys=True) + "\n")
    return True


def load_gold_calls(con, path=CALLS_LOG):
    """Rebuild gold_calls (one row per symbol per round) from the durable
    log. Returns the number of rows loaded."""
    rows = []
    for r in read_rounds(path):
        source = r["expectation"].get("source_sha256", "")
        for c in r["calls"]:
            rows.append((r["as_of_date"], c["symbol"],
                         r["methodology_version"], c["score"], c["rank"],
                         c["score_percentile"],
                         json.dumps(c["component_percentiles"],
                                    sort_keys=True),
                         c["call"], c["prior_call"],
                         json.dumps(r["expectation"], sort_keys=True),
                         source, r["run_id"], r["created_ts"]))
    con.execute("""
        CREATE OR REPLACE TABLE gold_calls (
            as_of_date DATE, symbol VARCHAR, methodology_version VARCHAR,
            score DOUBLE, rank INTEGER, score_percentile DOUBLE,
            component_percentiles VARCHAR, call VARCHAR, prior_call VARCHAR,
            expectation VARCHAR, expectation_source VARCHAR,
            run_id VARCHAR, created_ts VARCHAR
        )
    """)
    if rows:
        con.executemany(
            "INSERT INTO gold_calls VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    return len(rows)
