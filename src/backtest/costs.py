"""Trading costs: flat basis points per side.

Decision recorded 2026-08-11 (plan.md asked for it before task 6): flat,
not spread-aware. At a 3-12 month holding period the cost that matters is
round-trip commission plus typical large-cap spread, and a flat charge
models it within noise; spread-aware modelling buys precision the horizon
does not need. Revisit only if the universe grows small-cap heavy.

A round trip is charged once per measured window: entry and exit each pay
one side. Zero-cost and costed results therefore differ by exactly
2 * cost_bps / 10_000, which is what the acceptance test asserts.
"""

DEFAULT_COST_BPS = 10.0  # per side; ~free-broker commission + large-cap spread


def round_trip_cost(cost_bps=DEFAULT_COST_BPS):
    """Total drag of one entry plus one exit, as a return fraction."""
    return 2.0 * cost_bps / 10_000.0


def net_return(gross_return, cost_bps=DEFAULT_COST_BPS):
    """Gross forward return minus a full round trip.

    Works elementwise on pandas Series as well as floats.
    """
    return gross_return - round_trip_cost(cost_bps)
