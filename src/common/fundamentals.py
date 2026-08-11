"""Silver fundamentals: point-in-time metrics derived from EDGAR facts.

Grain of `silver_fundamental_metrics`: one row per (symbol, filed) - a
"knowledge event". Each row answers: as of this filing date, what were the
latest annual fundamentals a person could have known? Scoring joins it with
ASOF LEFT JOIN on filed <= as_of_date (LEFT, because a plain ASOF JOIN
silently drops symbols with no filing yet - see plan.md gotcha 0a).

Point-in-time rules, in order:

1. A fact exists from its `filed` date, never from its period end.
2. For the same concept, the newest period wins; for the same period, the
   latest filing wins (restatements). Ordering by (period_end, filed) means
   an amendment restating an OLD period cannot roll the series backwards.
3. Concepts advance independently: the dei share count is dated at the 10-K
   cover page rather than fiscal year end, so each concept takes its own
   latest-known value at every knowledge event.

Annual (10-K, FY) facts only. Flow concepts must span roughly a fiscal year,
which drops the quarterly and year-to-date entries that share the FY label.

DuckDB-only by design: local computes, Databricks serves (SPEC.md). No
engine-parity requirement applies until this SQL must run on Spark.
"""
from common.edgar import CONCEPT_TAGS

FLOW_CONCEPTS = {"net_income", "gross_profit", "revenues", "cost_of_revenue"}

METRIC_COLUMNS = [
    "net_income", "gross_profit_reported", "revenues", "cost_of_revenue",
    "assets", "equity", "shares_outstanding",
]


def _concept_map_values():
    rows = []
    for concept, (taxonomy, tags) in CONCEPT_TAGS.items():
        for priority, tag in enumerate(tags):
            rows.append(f"('{concept}', '{taxonomy}', '{tag}', {priority})")
    return ",\n              ".join(rows)


def build_fundamental_tables(con):
    """Build silver_fundamentals and silver_fundamental_metrics in DuckDB."""
    con.execute(f"""
        CREATE OR REPLACE TABLE silver_fundamentals AS
        WITH concept_map(concept, taxonomy, tag, priority) AS (
            VALUES
              {_concept_map_values()}
        ),
        facts AS (
            SELECT b.symbol, m.concept, m.priority,
                   b.start_date, b.end_date, b.value, b.filed, b.form
            FROM bronze_fundamentals b
            JOIN concept_map m
              ON b.taxonomy = m.taxonomy AND b.tag = m.tag
            -- 20-F and 40-F are the annual reports of foreign private
            -- issuers; the ones in bronze already tag under us-gaap.
            WHERE (b.form LIKE '10-K%' OR b.form LIKE '20-F%'
                   OR b.form LIKE '40-F%')
              AND b.fiscal_period = 'FY'
              AND (b.start_date IS NULL
                   OR (b.end_date - b.start_date) BETWEEN 300 AND 400)
        )
        -- One tag per (symbol, concept, period, filing): filers name the same
        -- concept differently across eras, so the preference order picks per
        -- period rather than per symbol.
        SELECT symbol, concept, start_date, end_date, value, filed, form
        FROM facts
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY symbol, concept, end_date, filed
            ORDER BY priority) = 1
    """)

    con.execute("""
        CREATE OR REPLACE TABLE silver_fundamental_metrics AS
        WITH events AS (
            SELECT DISTINCT symbol, filed FROM silver_fundamentals
        ),
        known AS (
            SELECT e.symbol, e.filed AS knowledge_date,
                   f.concept, f.value, f.end_date
            FROM events e
            JOIN silver_fundamentals f
              ON f.symbol = e.symbol AND f.filed <= e.filed
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY e.symbol, e.filed, f.concept
                ORDER BY f.end_date DESC, f.filed DESC) = 1
        ),
        pivoted AS (
            SELECT symbol, knowledge_date AS filed,
                MAX(CASE WHEN concept = 'net_income' THEN value END) AS net_income,
                MAX(CASE WHEN concept = 'net_income' THEN end_date END) AS period_end,
                MAX(CASE WHEN concept = 'gross_profit' THEN value END) AS gross_profit_reported,
                MAX(CASE WHEN concept = 'revenues' THEN value END) AS revenues,
                MAX(CASE WHEN concept = 'cost_of_revenue' THEN value END) AS cost_of_revenue,
                MAX(CASE WHEN concept = 'assets' THEN value END) AS assets,
                MAX(CASE WHEN concept = 'equity' THEN value END) AS equity,
                MAX(CASE WHEN concept = 'shares_outstanding' THEN value END) AS shares_outstanding,
                MAX(CASE WHEN concept = 'shares_outstanding' THEN end_date END) AS shares_date
            FROM known
            GROUP BY symbol, knowledge_date
        )
        SELECT *,
            COALESCE(gross_profit_reported, revenues - cost_of_revenue)
                AS gross_profit,
            net_income / NULLIF(equity, 0) AS roe,
            COALESCE(gross_profit_reported, revenues - cost_of_revenue)
                / NULLIF(assets, 0) AS gross_profitability
        FROM pivoted
    """)
