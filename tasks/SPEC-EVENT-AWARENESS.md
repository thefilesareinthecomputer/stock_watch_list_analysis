# Spec: Event Awareness - Weekly Review, Market-State Monitor, Claims Log

In-flight feature spec. On completion its durable essence folds into root
`SPEC.md` and this file moves to `tasks/completed/`.

Parent: `tasks/SPEC-RECOMMENDATION-ENGINE.md`. Depends on:
`tasks/SPEC-FIRST-ACTIONABLE-ROUND.md` (off-cycle rounds, trade journal),
task 13 universe tables, FRED + EDGAR plumbing. Informed by: advisor session
2026-08-12 and the deep-research pass of the same date (findings:
prediction-market APIs, event indices, news APIs, news-signal literature).

---

## Objective

Give the operator eyes on the whole tape and the world behind it - absolute
market state, world-event stress, and a graded record of event claims -
without turning a validated 3-12-month cross-sectional engine into an
unvalidated market timer or a daily news reactor.

Three capabilities, one weekly surface:

1. **Weekly sell review** - "should I sell anything?" answered from evidence
   the system already stands behind, with act / too-soon / punt as verdicts.
2. **Market-state monitor** - absolute awareness (breadth, credit, drawdown,
   volatility, policy and geopolitical stress), display-tier forever.
3. **Event feeds and claims log** - world events as structured, gradeable
   data: prediction markets, uncertainty indices, 8-K filings, and the
   operator's own logged claims - never a headline firehose.

## Rulings recorded (2026-08-12, user + advisor)

- Scheduled automation with manual run available at any point.
- Prediction-market sources: Kalshi AND Polymarket, read-only.
- Abandonment rule written at build start (interview), before the monitor.
- The monitor is **permanently display-tier**: with ~3 independent regime
  events in the data and one added every few years, it can never reach the
  registry's fold-t > 3.0 promotion bar. Recorded as a registry event;
  no promotion path exists or will be added.
- Thresholds come from literature with recorded citations, never fit to our
  own 2010-2026 history (three regime events is an in-sample trap).
- Headline/news APIs (Finnhub, Alpha Vantage, Marketaux, Tiingo,
  Polygon/Massive) and GDELT: **rejected for now**. Reasons recorded:
  free tiers decayed or paywalled; licensing unclear; GDELT is
  institutional-scale noise; and the evidence caps news-sentiment
  predictability at one quarter (Heston & Sinha 2017) - below our horizon -
  while attention-driven retail trading measurably loses (Barber, Huang,
  Odean & Schwarz 2022, -4.7% 20-day abnormal on attention names).

## Non-goals / explicit refusals

- No monitor-to-rank, monitor-to-call, or monitor-to-size coupling. Ever.
- No new sell criteria. "Act" verdicts surface only evidence the system
  already emits (deteriorating, exit-percentile breach, harvest, data
  integrity). Drawdown-vs-entry is DISPLAYED and can never justify "act" -
  an unvalidated stop-loss overlay is the bleed-money path.
- No RSS/headline ingestion, no scraping, no self-tuning of the claims
  process. Sources and claim categories get graded; nothing gets tuned.
- No daily output. The daily check prints only when something is armed or
  fired. If it prints daily, it has failed.
- No cash-timing automation. Acting on monitor state is a human decision,
  journaled and classified like every other action.

## Design

### Phase 0 - Abandonment rule (gates the monitor; needs the user)

A ~10-minute interview at build start produces
`knowledge/ABANDONMENT-RULE.md` (private): the pre-committed conditions
under which the operator de-risks or stops entirely, written in calm
conditions. A registry event records its existence and sha256, never its
contents. The monitor (Phase M) does not build until this exists - the
2008 failure mode is deciding under duress, and this artifact, not any
dial, is the direct countermeasure.

### Phase W - Weekly review (first build)

`scripts/weekly_review.py`, read-only against the warehouse; writes
`reports/weekly_YYYY-MM-DD.md` (gitignored) and prints it. Sections:

1. **Verdicts per held name**: act / too-soon / punt. "Act" only from:
   `deteriorating` newly true since last review; advisory call-state
   recompute showing a held name below the exit percentile; loss-harvest
   flags (tax, not alpha); or a data-integrity failure on a held name.
   Everything else is "too-soon" (evidence exists but below threshold,
   stated) or "punt" (nothing new).
2. **Context per held name**: rank, call, drawdown-vs-basis where the
   journal knows it (displayed, never verdict-driving).
3. **Monitor state** (Phase M, once built): current dial readings + state.
4. **Event feeds** (Phase E): prediction-market movers, 8-K hits on held
   and called names, claims due for resolution this week.
5. Standing caveats verbatim.

Freshness gate: held names only (the full-universe gate stays with the
monthly build). State for "newly true since last review" lives in a small
warehouse table keyed by review date.

Cadence: scheduled weekly, Monday pre-open, via a durable scheduled task;
`uv run python scripts/weekly_review.py` runs identically by hand anytime.

### Phase M - Market-state monitor

New bronze tables, appended by a fetch script (FRED API key already in
`.env`; GPR by direct file download):

| Input | Source | Series/file | Why this one |
|---|---|---|---|
| Breadth | own prices | % of banked universe above 200d MA | internal, one number |
| Credit | FRED | HY OAS (ICE BofA) | risk price not derivable from our own data |
| Drawdown | own prices | SPY vs trailing high | the direct absolute question |
| Volatility | FRED | VIXCLS | momentum-crash precondition input |
| Policy stress | FRED | USEPUINDXD (daily), GEPUCURRENT | world events: policy |
| Geopolitics | matteoiacoviello.com | GPR monthly + GPRD daily (XLS, CC BY 4.0) | world events: conflict/threats |

**FRED gotcha (recorded 2026-08-12):** ICE BofA series on FRED carry only a
3-year window since April 2026 - snapshot full available history into
bronze on first fetch and append thereafter; never re-window.

**Momentum-crash condition**: flag when market is below its 200d MA with
elevated volatility - the documented precondition for the scored
composite's own failure mode (Daniel & Moskowitz 2016; our negative folds
2016/2018/2020). This ties the monitor to a measured weakness of our
actual signal, not generic doom detection.

States: `normal` / `caution` / `stress`, from registry-held thresholds,
each threshold carrying its literature citation in the registry entry.
Every state change is logged (dated, with dial values) so the monitor
accumulates its own graded track record even though it can never be
promoted. Monitor state is printed in the weekly review and the decision
report, and `journal_agreement` classification extends to it: an action
taken while the monitor is in `stress` is visible as such forever.

### Phase A - Armed alerts (mid-interval escape valve)

Threshold semantics: **level hysteresis** - a dial crossing its threshold
ARMS; still beyond it after 2 consecutive closes FIRES; disarm only at a
materially better level (registry-held gap), not on time decay. Genuine
discontinuities (gaps) are the human's weekly reading, not the machine's.

Firing produces exactly one thing: a printed recommendation to record an
`authorize_off_cycle` registry event and run the existing off-cycle round
machinery. No new decision path, no automated action. The daily check
(`scripts/daily_check.py`, scheduled, ~2 min) prints nothing unless
something is armed or fired.

### Phase E - Event feeds and claims log

- **`claims_log.jsonl`** (gitignored - the action field names real
  decisions). Append-only. Entry: source, claim as stated, operationalized
  form (named variable, threshold, deadline), entry date, prior (market
  probability when adopted from a prediction market), resolution date,
  outcome (`confirmed` / `falsified` / `expired`), and **action_taken** -
  the money field: did reacting to this help or hurt. Unfalsifiable claims
  are refused into `unfalsifiable-discarded` entries, which is itself
  source-quality data. Entry helper: `scripts/claim.py` (add / resolve /
  list-due).
- **Prediction-market watch**: weekly pull, Kalshi public market data +
  Polymarket Gamma API, read-only, no keys: top world-event markets by
  volume and largest week-over-week probability moves, shown in the weekly
  review with one-command adoption into the claims log. Caveat printed
  with every reading: calibration is domain-conditional; political markets
  compress toward 50% (Le 2026, arXiv 2602.19520). Raw pulls cached in
  bronze; nothing redistributed - private warehouse only, ToS
  conservatively assumed personal-use.
- **8-K watch**: EDGAR current-events Atom feed (`getcurrent`, type=8-K,
  existing User-Agent), filtered to held + called names, weekly, shown
  with item codes. Free, official, already-plumbed.

## Cadence summary

| When | What | Output |
|---|---|---|
| Daily (scheduled) | `daily_check.py` - armed-alert state | Silent unless armed/fired |
| Weekly Mon pre-open (scheduled) | `weekly_review.py` - full surface | One private report, always |
| Monthly (existing) | rebalance round + universe refresh | Unchanged |
| Anytime (manual) | any of the above by hand | Identical behavior |

## Boundaries

**Always:** monitor display-tier; thresholds cited, registry-held; alerts
and state changes logged with dates; claims operationalized or refused;
journal actions classified against calls AND monitor state; private
outputs gitignored; caveats verbatim on every surface.

**Ask first:** any threshold change (registry event); adding an input dial;
adding an event source; acting on a fired alert (that IS the
authorization conversation); any coupling between this spec's outputs and
scoring.

**Never:** promote the monitor; auto-trade on any alert; let drawdown
justify an "act" verdict; ingest headlines wholesale; tune claim grading;
restate a logged alert, state change, or claim.

## Success criteria

| # | Criterion | Verified by |
|---|---|---|
| 1 | Weekly review verdicts derive only from sanctioned evidence | Fixture tests per verdict path; drawdown-only fixture yields no "act" |
| 2 | Review runs read-only and identically scheduled or manual | No writes outside reports/ + state table; test |
| 3 | Monitor states reproduce from stored dial data | State function pure over bronze fixtures |
| 4 | Every threshold carries a citation in the registry | Registry schema test |
| 5 | Monitor ineligibility for promotion is a recorded event | Registry event present; tiers code has no monitor path |
| 6 | Armed alert: arms, fires only after 2 confirming closes, level-hysteresis disarm | State-machine fixture tests |
| 7 | Fired alert produces only an off-cycle authorization recommendation | Output test; no state mutation |
| 8 | Daily check silent when nothing armed | Empty-fixture test asserts no output |
| 9 | Claims: unfalsifiable refused; resolution grades recorded; action_taken required on resolve | Parser/fixture tests |
| 10 | HY OAS bronze never loses pre-window history | Append-only test; first-fetch snapshot present |
| 11 | Prediction-market pull degrades gracefully when an API is down | Fetch-failure fixture: report notes the gap, run continues |
| 12 | Journal agreement classifies actions against monitor state | Fixture test |
| 13 | Abandonment rule exists (hash-recorded) before monitor ships | Registry event gate |

## Open questions

1. **Threshold values** - proposed at build time with citations (e.g.,
   200d MA breadth bands, OAS levels, VIX percentile) and ruled on before
   first scheduled run.
2. **Kalshi/Polymarket "world-event" category filter** - exact category
   allowlist decided when the first real pull is inspected.
3. **Weekly schedule time** - Monday pre-open assumed; confirm at deploy.
