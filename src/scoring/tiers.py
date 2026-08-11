"""Signal tier registry: which signals may move a recommendation, as data.

Tier is data, not code (SPEC-SIGNAL-TIERS §1): the registry file carries
every signal's tier, the evidence behind it, and the dated promotion and
demotion events - so a tier change is a recorded event, never a silent
commit. Only `scored` entries reach the composite; `candidate` entries are
computed and evaluated at zero weight; `monitored` entries are stored.

The scored set defines the LOCAL methodology-v2 composite. Production stays
on scoring.components v1 until L5 ships candidate data to Databricks -
earnings_yield does not exist there yet.
"""
import json
import os

from scoring.variants import component_sql, validate_variant

TIERS = ("scored", "candidate", "monitored")
REGISTRY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "signal_tiers.json")


def load_registry(path=REGISTRY_PATH):
    with open(path) as f:
        registry = json.load(f)
    names = [s["name"] for s in registry["signals"]]
    if len(names) != len(set(names)):
        raise ValueError("registry: a signal appears in more than one entry")
    for signal in registry["signals"]:
        if signal["tier"] not in TIERS:
            raise ValueError(f"{signal['name']}: unknown tier {signal['tier']}")
        if signal["tier"] == "scored":
            for key in ("expression", "ascending", "weight"):
                if key not in signal:
                    raise ValueError(f"{signal['name']}: scored needs {key}")
    for event in registry.get("events", []):
        if event["action"] in ("promote", "demote") \
                and event["signal"] not in names:
            raise ValueError(f"event names unknown signal {event['signal']}")
    return registry


def scored_variant(registry):
    """The scored tier as a variant dict for the shared scoring machinery."""
    components = [
        {"name": s["name"], "expression": s["expression"],
         "ascending": s["ascending"], "weight": s["weight"]}
        for s in registry["signals"] if s["tier"] == "scored"
    ]
    return validate_variant({
        "name": f"methodology_{registry['methodology_version'].replace('-', '_')}",
        "components": components,
    })


def candidate_variants(registry):
    """One single-component variant per computed candidate, for evaluation."""
    return [validate_variant({
        "name": f"cand_{s['name']}",
        "components": [{"name": s["name"], "expression": s["expression"],
                        "ascending": s["ascending"], "weight": 1.0}],
    }) for s in registry["signals"]
        if s["tier"] == "candidate" and s.get("computed", True)]


def rank_latest(con, registry, table_name="gold_watchlist_ranked_v2"):
    """Rank the latest as_of_date with the scored composite.

    Reads only scored components by construction - candidates cannot
    contribute, which is what the weight-zero test asserts.
    """
    comps = scored_variant(registry)["components"]
    val_exprs, rank_ctes, rank_joins, weighted, total_weight = \
        component_sql(comps)
    pct_cols = ", ".join(
        f"COALESCE(pct_{i}, 0.5) AS {c['name']}_pct"
        for i, c in enumerate(comps))
    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        WITH inputs AS (
            SELECT s.*, c.earnings_yield, c.gross_profitability, c.roe
            FROM silver_signals s
            LEFT JOIN gold_candidate_signals c USING (symbol, as_of_date)
            WHERE s.as_of_date = (SELECT MAX(as_of_date) FROM silver_signals)
        ),
        vals AS (
            SELECT symbol, as_of_date,
                   {val_exprs}
            FROM inputs
        ),{rank_ctes}
        SELECT v.symbol, v.as_of_date, {pct_cols},
               ({weighted}) / {total_weight} AS composite_score,
               RANK() OVER (ORDER BY ({weighted}) / {total_weight} DESC)
                   AS composite_rank
        FROM vals v
        {rank_joins}
    """)
    return table_name
