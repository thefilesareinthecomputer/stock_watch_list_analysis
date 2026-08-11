"""Scoring variants expressed as data, never as code edits.

Decision (plan.md, before task 8): config entries, not free SQL fragments.
A variant is a JSON entry - named components, each a constrained scalar
expression over the joined signal columns, a direction, and a weight. Data
because a variant must be recordable in the trial log, diffable, hashable,
and reproducible; a code edit is none of those. Expressions may compute
(12-1 momentum needs arithmetic) but reject statement separators and
comments, so they cannot smuggle in subqueries or side effects.

Scoring mirrors the production composite's discipline: each component is
PERCENT_RANK'd within as_of_date over NON-NULL values only (nulls would
squat on the top ranks - see scoring.candidates), missing components score
neutral 0.5, and the weighted sum is normalized by total weight.

Input columns come from silver_signals LEFT JOIN gold_candidate_signals,
restricted to monthly evaluation dates (backtest.harness rationale).
"""
import hashlib
import json
import os
import re

_FORBIDDEN = re.compile(r";|--|/\*")
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

VARIANTS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "variants.json")

CANDIDATE_COLUMNS = ("earnings_yield", "gross_profitability", "roe",
                     "earnings_yield_pct", "gross_profitability_pct",
                     "roe_pct")


def validate_variant(variant):
    if not _IDENT.match(variant.get("name", "")):
        raise ValueError(f"variant name invalid: {variant.get('name')!r}")
    components = variant.get("components", [])
    if not components:
        raise ValueError(f"{variant['name']}: no components")
    for comp in components:
        if not _IDENT.match(comp.get("name", "")):
            raise ValueError(f"{variant['name']}: component name invalid")
        expr = comp.get("expression", "")
        if not expr or _FORBIDDEN.search(expr):
            raise ValueError(
                f"{variant['name']}.{comp['name']}: forbidden expression")
        if not isinstance(comp.get("ascending"), bool):
            raise ValueError(
                f"{variant['name']}.{comp['name']}: ascending must be bool")
        if not (isinstance(comp.get("weight"), (int, float))
                and comp["weight"] > 0):
            raise ValueError(
                f"{variant['name']}.{comp['name']}: weight must be > 0")
    return variant


def load_variants(path=VARIANTS_PATH):
    with open(path) as f:
        return [validate_variant(v) for v in json.load(f)]


def definition_hash(variant):
    """Stable content hash so a recorded result names its exact methodology."""
    canonical = json.dumps(variant, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def component_sql(comps):
    """The shared ranking discipline: value exprs, per-component non-null
    PERCENT_RANK CTEs, their joins, and the neutral-on-missing weighted sum.
    Used by both variant scoring and the registry's composite so the
    definition exists once."""
    val_exprs = ",\n                   ".join(
        f"({c['expression']}) AS comp_{i}" for i, c in enumerate(comps))
    rank_ctes = ",".join(f"""
        rank_{i} AS (
            SELECT symbol, as_of_date,
                PERCENT_RANK() OVER (
                    PARTITION BY as_of_date
                    ORDER BY comp_{i} {"ASC" if c["ascending"] else "DESC"}
                ) AS pct_{i}
            FROM vals WHERE comp_{i} IS NOT NULL
        )""" for i, c in enumerate(comps))
    rank_joins = "\n        ".join(
        f"LEFT JOIN rank_{i} USING (symbol, as_of_date)"
        for i in range(len(comps)))
    weighted = " + ".join(
        f"COALESCE(pct_{i}, 0.5) * {c['weight']}" for i, c in enumerate(comps))
    total_weight = sum(c["weight"] for c in comps)
    return val_exprs, rank_ctes, rank_joins, weighted, total_weight


def score_table(con, variant, horizon, table_name="variant_scores_tmp",
                benchmark="SPY"):
    """Materialize (symbol, as_of_date, score) for one variant, one horizon.

    Returns table_name; pass it to backtest.harness.evaluation_frame.
    """
    validate_variant(variant)
    if not _IDENT.match(table_name):
        raise ValueError(f"invalid table name: {table_name}")
    comps = variant["components"]
    val_exprs, rank_ctes, rank_joins, weighted, total_weight = \
        component_sql(comps)
    candidate_cols = ", ".join(f"c.{col}" for col in CANDIDATE_COLUMNS)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {table_name} AS
        WITH eval_dates AS (
            SELECT MAX(as_of_date) AS as_of_date
            FROM backtest_forward_returns
            WHERE symbol = ? AND horizon = ?
            GROUP BY DATE_TRUNC('month', as_of_date)
        ),
        inputs AS (
            SELECT s.*, {candidate_cols}
            FROM silver_signals s
            LEFT JOIN gold_candidate_signals c
              USING (symbol, as_of_date)
            JOIN eval_dates d ON s.as_of_date = d.as_of_date
        ),
        vals AS (
            SELECT symbol, as_of_date,
                   {val_exprs}
            FROM inputs
        ),{rank_ctes}
        SELECT v.symbol, v.as_of_date,
               ({weighted}) / {total_weight} AS score
        FROM vals v
        {rank_joins}
    """, [benchmark, horizon])
    return table_name
