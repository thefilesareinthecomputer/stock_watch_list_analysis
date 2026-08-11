"""Candidate-tier fundamental signals: computed and ranked, zero weight.

Per SPEC-SIGNAL-TIERS.md, candidates accumulate a public track record before
they may touch the composite: earnings yield (E/P, handles negative
earnings), gross profitability (Novy-Marx GP/A) and ROE. Nothing here feeds
composite_score; promotion requires walk-forward evidence.

Grain of `gold_candidate_signals`: (symbol, as_of_date), full history, so
the evaluation harness can score candidates at any past date.

Construction notes that carry the correctness:

- ASOF LEFT JOIN, never plain ASOF JOIN: a symbol with no filing before the
  as-of date must appear with nulls, not vanish (plan.md gotcha 0a).
- Market cap uses the RAW close times a split-adjusted share count. The
  share count is measured at the filing's cover date, so any split between
  that date and the as-of date rescales it; without this, a 10-for-1 split
  understates market cap ten-fold for up to a year.
- Percentiles rank non-null rows only. PERCENT_RANK cannot exclude rows
  from its window, and DuckDB sorts nulls last under ASC, so ranking the
  full set would hand the missing-fundamentals rows the top percentiles.
  Missing stays NULL here; consumers apply COALESCE(pct, 0.5) - neutral,
  not worst (parent spec P4).

DuckDB-only by design: local computes, Databricks serves (SPEC.md).
"""

# name -> metric column. All three are higher-is-better, so every window
# orders ASC: PERCENT_RANK assigns 0.0 to the first row, meaning ASC hands
# the largest value 1.0 (see scoring.components direction contract).
CANDIDATE_COMPONENTS = {
    "earnings_yield_pct": "earnings_yield",
    "gross_profitability_pct": "gross_profitability",
    "roe_pct": "roe",
}


def _rank_ctes():
    ctes, joins = [], []
    for name, metric in CANDIDATE_COMPONENTS.items():
        ctes.append(f"""
        rank_{metric} AS (
            SELECT symbol, as_of_date,
                PERCENT_RANK() OVER (
                    PARTITION BY as_of_date ORDER BY {metric} ASC) AS {name}
            FROM metrics WHERE {metric} IS NOT NULL
        )""")
        joins.append(f"LEFT JOIN rank_{metric} USING (symbol, as_of_date)")
    return ",".join(ctes), "\n        ".join(joins)


def build_candidate_signals(con):
    """Build gold_candidate_signals from prices, signals and fundamentals."""
    from common.edgar import ENTITY_SCHEMA, NON_OPERATING_SIC
    con.execute(ENTITY_SCHEMA)  # tolerate a warehouse without the entity pass
    non_operating = ", ".join(f"'{sic}'" for sic in NON_OPERATING_SIC)
    ctes, joins = _rank_ctes()
    con.execute(f"""
        CREATE OR REPLACE TABLE gold_candidate_signals AS
        WITH prices AS (
            SELECT symbol, date, close,
                EXP(SUM(LN(CASE WHEN split_ratio IS NULL OR split_ratio = 0
                                THEN 1.0 ELSE split_ratio END))
                    OVER (PARTITION BY symbol ORDER BY date)) AS cum_split
            FROM bronze_prices
        ),
        base AS (
            SELECT s.symbol, s.as_of_date, p.close, p.cum_split,
                   m.net_income, m.shares_outstanding, m.shares_date,
                   m.filed, m.roe, m.gross_profitability, m.revenues,
                   e.sic
            FROM (SELECT DISTINCT symbol, as_of_date FROM silver_signals) s
            JOIN prices p
              ON p.symbol = s.symbol AND p.date = s.as_of_date
            ASOF LEFT JOIN silver_fundamental_metrics m
              ON m.symbol = s.symbol AND m.filed <= s.as_of_date
            LEFT JOIN bronze_entity e ON e.symbol = s.symbol
        ),
        shares_asof AS (
            SELECT b.*, sp.cum_split AS cum_split_at_shares
            FROM base b
            ASOF LEFT JOIN prices sp
              ON sp.symbol = b.symbol AND sp.date <= b.shares_date
        ),
        metrics AS (
            -- The 400-day staleness guard: a share count older than one
            -- annual cycle plus filing lag is not a basis for market cap.
            -- Multi-class filers (BRK) report per-class counts as
            -- dimensioned facts CompanyFacts omits, leaving an
            -- undimensioned count that can be years stale - paired with
            -- today's price that manufactures an E/P off by orders of
            -- magnitude. NULL scores neutral; garbage scores wrong.
            -- Operating-company guard, two layers. SIC: commodity trusts
            -- (SLV) and investment vehicles file 10-Ks whose "net income"
            -- is asset appreciation - fictional earnings yields that
            -- outrank every real company (SLV ranked #2 before this).
            -- Revenue marker: a belt for entities whose SIC is missing.
            -- Excluded entities score neutral on earnings-based ratios.
            SELECT symbol, as_of_date, filed, gross_profitability,
                CASE WHEN revenues IS NOT NULL
                     AND COALESCE(sic, '') NOT IN ({non_operating})
                     THEN roe END AS roe,
                CASE WHEN shares_outstanding > 0 AND close > 0
                     AND revenues IS NOT NULL
                     AND COALESCE(sic, '') NOT IN ({non_operating})
                     AND shares_date >= as_of_date - INTERVAL 400 DAY
                     THEN net_income /
                          (close * shares_outstanding
                           * cum_split / cum_split_at_shares)
                END AS earnings_yield
            FROM shares_asof
        ),{ctes}
        SELECT metrics.*,
               earnings_yield_pct, gross_profitability_pct, roe_pct
        FROM metrics
        {joins}
    """)
