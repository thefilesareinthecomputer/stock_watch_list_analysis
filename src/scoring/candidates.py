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
- Market cap follows the share-terms rule (SPEC 5.1): the stored close is
  in the scoring session's own terms, and the filed share count is brought
  to that date with the splits logged in corporate_actions between the
  filing's share date and the scoring date. Nothing after the scoring
  date enters, so a later split cannot shift an earlier yield.
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
    from common.corporate_actions import ensure_table
    from common.edgar import ENTITY_SCHEMA, NON_OPERATING_SIC
    con.execute(ENTITY_SCHEMA)  # tolerate a warehouse without the entity pass
    non_operating = ", ".join(f"'{sic}'" for sic in NON_OPERATING_SIC)
    ctes, joins = _rank_ctes()
    ensure_table(con)  # corporate_actions, for a warehouse without the log yet
    con.execute(f"""
        CREATE OR REPLACE TABLE gold_candidate_signals AS
        WITH base AS (
            SELECT s.symbol, s.as_of_date, p.close,
                   m.net_income, m.shares_outstanding, m.shares_date,
                   m.filed, m.roe, m.gross_profitability, m.revenues,
                   e.sic
            FROM (SELECT DISTINCT symbol, as_of_date FROM silver_signals) s
            JOIN bronze_prices p
              ON p.symbol = s.symbol AND p.date = s.as_of_date
            ASOF LEFT JOIN silver_fundamental_metrics m
              ON m.symbol = s.symbol AND m.filed <= s.as_of_date
            LEFT JOIN bronze_entity e ON e.symbol = s.symbol
        ),
        -- Share-terms rule (SPEC 5.1): the close is in the scoring
        -- session's own terms and the filed share count in its
        -- shares_date's terms. The count comes to the scoring date through
        -- the splits logged between the two (inversely if the count
        -- postdates the date). A split after the scoring date touches
        -- neither side, so nothing leaks backwards.
        shares_asof AS (
            SELECT b.*, COALESCE((
                SELECT EXP(SUM(CASE
                    WHEN c.date > b.shares_date AND c.date <= b.as_of_date
                        THEN LN(c.ratio)
                    WHEN c.date > b.as_of_date AND c.date <= b.shares_date
                        THEN -LN(c.ratio) END))
                FROM corporate_actions c WHERE c.symbol = b.symbol
            ), 1.0) AS shares_factor
            FROM base b
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
                          (close * shares_outstanding * shares_factor)
                END AS earnings_yield
            FROM shares_asof
        ),{ctes}
        SELECT metrics.*,
               earnings_yield_pct, gross_profitability_pct, roe_pct
        FROM metrics
        {joins}
    """)
