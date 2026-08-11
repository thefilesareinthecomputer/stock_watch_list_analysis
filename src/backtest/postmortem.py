"""Post-mortem: settlements graded into a dated, immutable report.

Measurement is automated; the decision is human-ratified
(SPEC-SIGNAL-TIERS §4). This module turns a settlement list into drift
state and SUGGESTED registry events - it never applies one. Reports are
immutable by convention: a report that already exists is never rewritten,
corrections go in later reports, exactly like the call log itself.

Drift rules (registry `calls.drift`, revisable only by recorded event),
all evaluated at the 21-session rung because those windows are disjoint -
the only rung where consecutive settlements are independent observations
(plan gotcha 0e):
  - realized excess below the haircut mean for `below_mean_rounds`
    consecutive settlements -> review suggestion;
  - below the haircut p10 for `below_p10_rounds` consecutive -> review;
  - a signal's attributed IC insignificant on fold-level t across all
    settlements (once at least `below_mean_rounds` exist) -> demotion
    review for that signal.
Single-interval misses are noise by construction (1-month fold-t ~1.9)
and every report says so.

Reports carry aggregates and signal-level attribution only - never
symbols, because results/ is tracked and the watchlist is private.
"""
import json
import os

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
REPORT_DIR = os.path.join(ROOT, "results", "post_mortem")

DRIFT_RUNG = 21

NOISE_NOTE = ("Single-interval misses are noise by construction "
              "(1-month fold-level t ~1.9); only cumulative drift across "
              "vintages triggers a suggestion.")
SURVIVORSHIP_NOTE = ("Survivorship caveat: expectations were estimated on "
                     "today's survivors, so realized-vs-expected gaps read "
                     "worse than they are; compare trends, not levels.")


def _trailing(flags):
    """Length of the trailing run of True."""
    count = 0
    for flag in reversed(flags):
        if not flag:
            break
        count += 1
    return count


def drift_state(settlements, drift_cfg):
    """Cumulative drift at the 21d rung, and the suggestions it warrants."""
    at_rung = sorted((s for s in settlements if s["horizon"] == DRIFT_RUNG),
                     key=lambda s: s["as_of_date"])
    below_mean = _trailing([s["below_haircut_mean"] for s in at_rung])
    below_p10 = _trailing([s["below_haircut_p10"] for s in at_rung])

    signal_t = {}
    for component in (at_rung[-1]["attribution"] if at_rung else {}):
        ics = pd.Series([s["attribution"].get(component) for s in at_rung],
                        dtype=float).dropna()
        n = len(ics)
        if n < 2 or ics.std(ddof=1) == 0:
            signal_t[component] = {"n": n, "t": None}
        else:
            signal_t[component] = {
                "n": n,
                "t": float(ics.mean() / ics.std(ddof=1) * (n ** 0.5))}

    suggestions = []
    if below_mean >= drift_cfg["below_mean_rounds"]:
        suggestions.append({
            "action": "review",
            "reason": f"realized {DRIFT_RUNG}d excess below the haircut mean "
                      f"for {below_mean} consecutive settlements "
                      f"(threshold {drift_cfg['below_mean_rounds']})"})
    if below_p10 >= drift_cfg["below_p10_rounds"]:
        suggestions.append({
            "action": "review",
            "reason": f"realized {DRIFT_RUNG}d excess below the haircut p10 "
                      f"for {below_p10} consecutive settlements "
                      f"(threshold {drift_cfg['below_p10_rounds']})"})
    for component, stats in signal_t.items():
        if (stats["n"] >= drift_cfg["below_mean_rounds"]
                and stats["t"] is not None
                and abs(stats["t"]) < drift_cfg["fold_t_bar"]):
            suggestions.append({
                "action": "demotion_review", "signal": component,
                "reason": f"attributed IC insignificant: t {stats['t']:.2f} "
                          f"over {stats['n']} settlements "
                          f"(bar {drift_cfg['fold_t_bar']})"})
    return {"trailing_below_mean": below_mean,
            "trailing_below_p10": below_p10,
            "signal_t": signal_t,
            "suggestions": suggestions}


def write_report(report_date, settlements, drift, directory=REPORT_DIR):
    """Write YYYY-MM-DD.{json,md}; a no-op if the date already has one.

    Returns True when written. Immutability mirrors emit_round: the first
    report for a date stands, corrections go in later reports.
    """
    os.makedirs(directory, exist_ok=True)
    json_path = os.path.join(directory, f"{report_date}.json")
    if os.path.exists(json_path):
        return False

    n_vintages = len({s["as_of_date"] for s in settlements})
    payload = {"report_date": report_date, "n_vintages_settled": n_vintages,
               "settlements": settlements, "drift": drift,
               "notes": [NOISE_NOTE, SURVIVORSHIP_NOTE]}
    with open(json_path, "w") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")
    with open(os.path.join(directory, f"{report_date}.md"), "w") as f:
        f.write(_markdown(report_date, settlements, drift, n_vintages))
    return True


def _markdown(report_date, settlements, drift, n_vintages):
    lines = [f"# Post-mortem {report_date}", ""]
    if not settlements:
        lines.append("No vintage has a closed window yet - nothing to grade.")
    else:
        lines += [f"{n_vintages} vintage(s) settled.", "",
                  "| vintage | rung | realized | expected (haircut mean) "
                  "| p10 | below mean | below p10 |",
                  "|---|---|---|---|---|---|---|"]
        for s in sorted(settlements,
                        key=lambda s: (s["as_of_date"], s["horizon"])):
            lines.append(
                f"| {s['as_of_date']} | {s['horizon']} "
                f"| {s['realized_excess_net']:+.4f} "
                f"| {s['expected_mean_haircut']:+.4f} "
                f"| {s['expected_p10_haircut']:+.4f} "
                f"| {'yes' if s['below_haircut_mean'] else 'no'} "
                f"| {'yes' if s['below_haircut_p10'] else 'no'} |")
        lines += ["", "## Attribution (fold-level t per signal, "
                      f"{DRIFT_RUNG}d rung)", ""]
        for component, stats in drift["signal_t"].items():
            t = "n/a" if stats["t"] is None else f"{stats['t']:.2f}"
            lines.append(f"- {component}: t {t} over {stats['n']} settlements")
    lines += ["", "## Drift", "",
              f"- trailing settlements below haircut mean: "
              f"{drift['trailing_below_mean']}",
              f"- trailing settlements below haircut p10: "
              f"{drift['trailing_below_p10']}", ""]
    if drift["suggestions"]:
        lines.append("## SUGGESTED registry events (a human records or "
                     "declines these; nothing is automatic)")
        lines += ["", *(f"- {s['action']}"
                        + (f" [{s['signal']}]" if "signal" in s else "")
                        + f": {s['reason']}" for s in drift["suggestions"])]
    else:
        lines.append(f"Within expectation - nothing to learn yet "
                     f"(n={n_vintages}).")
    lines += ["", f"{NOISE_NOTE}", "", f"{SURVIVORSHIP_NOTE}", ""]
    return "\n".join(lines)
