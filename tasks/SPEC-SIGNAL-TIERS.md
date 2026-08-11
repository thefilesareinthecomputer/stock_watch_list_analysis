# Spec: Tiered Signals, Validation, and Buy/Sell Calls

In-flight feature spec. Folds into root `SPEC.md` on completion; the husk moves
to `tasks/completed/`.

Parent: `tasks/SPEC-RECOMMENDATION-ENGINE.md`. Depends on
`tasks/SPEC-LOCAL-WAREHOUSE.md` (L1/L2 shipped).

---

## Objective

Produce a **buy or sell call for every symbol at pipeline runtime**, where buy
means "most likely to outperform the benchmark over the forecast window", and
where the confidence in that call is earned by evidence rather than asserted.

Three mechanisms make that possible, and this spec covers all three:

1. **Tiered signals** - every signal is computed and recorded, but only signals
   that have earned it affect a recommendation.
2. **Walk-forward validation** - evidence comes from held-out *time*, not
   held-out symbols.
3. **Honest trial accounting** - the number of variants tried is recorded before
   results are seen, or the winner cannot be distinguished from the luckiest.

## Non-goals

- Position sizing, execution, or order generation.
- Cash/regime timing. The engine picks what, never whether.
- Replacing the composite with an ML model. Not excluded forever; excluded until
  the linear factor version has a measured baseline to beat.

---

## 1. Signal tiers

Every signal lives in exactly one tier. Tier is **data, not code** - a registry
the pipeline reads - so promotion and demotion are recorded events rather than
commits.

| Tier | In the composite? | Computed? | Purpose |
|---|---|---|---|
| `scored` | Yes, weighted | Yes | Has earned its place on walk-forward evidence |
| `candidate` | No, weight zero | Yes, and evaluated every run | Accumulating a track record in public |
| `monitored` | No | Yes, stored only | Cheap to keep; available if a pattern emerges |

**Why tiers rather than deletion.** A signal removed from the code stops
accumulating evidence, so the decision to drop it can never be revisited on
data. A `candidate` costs one column and buys the option to promote it later
with an actual track record behind it.

### Promotion and demotion

Promotion from `candidate` to `scored` requires **all** of:

- Walk-forward information coefficient positive and stable across folds - not
  full-sample IC (see §2).
- `t > 3.0`, not 2.0, on the fold-level results. The higher bar is the
  multiple-testing correction (Harvey, Liu & Zhu 2016).
- Trial count logged prospectively (§3) so the Sharpe can be deflated.
- Correlation with existing `scored` signals below a stated threshold. A signal
  that duplicates one already in the composite adds false confidence, not
  information.

Demotion is automatic: a `scored` signal whose walk-forward IC turns
insignificant returns to `candidate`. Recorded, dated, and reversible.

### Initial assignment

**`scored`** - the four the composite already uses, fixed and direction-tested,
kept only so the engine keeps functioning while better ones are validated. None
of them has earned this tier on evidence; they are incumbents, not winners.

| Signal | Note |
|---|---|
| momentum (30d return) | Placeholder. 1-month returns *reverse*; replace with 12-1 |
| value (earnings yield) | Once P/E becomes E/P, per parent spec P4 |
| risk (RSI) | Mislabelled - a timing oscillator, not a risk measure |
| quality (MFI) | Mislabelled - volume-weighted momentum, not quality |

**`candidate`** - strong prior evidence in the literature, unvalidated *here*:

| Signal | Why it is a candidate |
|---|---|
| 12-1 momentum | The standard momentum definition; 30d is the wrong window |
| Gross profitability | Best-evidenced quality metric (Novy-Marx); computable from EDGAR |
| Trailing realized volatility | The low-volatility anomaly; a real risk measure |
| Beta | Ascending; related to but distinct from realized vol |
| Earnings yield (E/P) | Handles negative earnings, which raw P/E cannot |

**`monitored`** - computed and stored, no current expectation:

RSI, MFI, MACD histogram, Bollinger %B and bandwidth, OBV, ATR, dividend yield
gap, congressional trade activity.

These stay because they are already computed and cost nothing to store. Several
also remain useful in the **alert layer**, which is a different question from
whether they predict returns - "is now a bad moment to act on a view I already
hold" is not "does this rank stocks".

### Combination

Open, to be tested as variants rather than decided by argument:

- **Percentile rank** (current) discards magnitude - 1st and 2nd rank identically
  whether the gap is 0.1% or 40%.
- **Z-score within date** preserves magnitude but needs **winsorizing** first, or
  a single outlier dominates. The existing dashboard shows exactly this failure:
  one P/E near 370 flattening every other bar.
- **Weighting**: equal, or inverse-volatility so a noisy signal cannot dominate
  by accident.

---

## 2. Walk-forward validation

**Hold out time, not symbols.** Stocks co-move, so a held-out *set of stocks*
during a bull market mostly measures the market. Held-out *periods* are where the
information is.

Procedure: score the universe as of date D using only what was knowable at D,
measure realized excess return over the forecast window, roll D forward, repeat.
Report per-fold results, never a single pooled number.

**Full-sample IC is not validation.** Widely-used tooling (alphalens and
descendants) reports full-sample cross-sectional IC, which uses the whole history
to evaluate every date. Treating those tear sheets as evidence is the same error
class as a look-ahead backtest, using a respected library.

Non-negotiable checks, all of which must pass before any result is believed:

- **Known-answer:** the harness run on the benchmark returns ~zero excess. A
  harness that finds edge in SPY is broken.
- **Look-ahead:** shifting features forward one day degrades every metric. If it
  does not, information is leaking backwards.
- **Decay:** report IC by horizon so the forecast window is chosen from measured
  signal half-life rather than assumed to be six months.

### Standing caveats on every result

Survivorship (the universe is today's survivors) and absent fundamentals history
bias *levels* upward. Relative comparison between variants largely survives this
because the bias is common-mode; absolute Sharpe and CAGR do not. Every printed
result carries this, and no go-live threshold is derived from a backtest level.

Note the sharper form of the survivorship problem: its worst effect is not
inflating returns but **manufacturing apparent persistence**, which is precisely
what a validation harness is supposed to detect.

---

## 3. Trial accounting

Every variant evaluated is recorded **before its result is seen**: an
identifier, the definition, the date, and the intent. Including the ones
abandoned halfway.

This cannot be retrofitted. Trials you did not record are trials you cannot
count, and without the count the best of N variants is statistically
indistinguishable from the luckiest of N.

Consequence to accept up front: published predictors decay ~26% out of sample
and ~58% after publication, and the flashiest in-sample results decay hardest
(McLean & Pontiff 2016). A backtest that looks excellent is evidence of
overfitting more often than of edge.

---

## 4. Buy/sell calls

A call is emitted per symbol per run into the append-only snapshot, carrying the
signals and tier configuration that produced it.

**Gated.** No call is emitted before walk-forward validation exists. A buy label
attached to the current ranking is the current ranking with a new name, and that
ranking was inverted until 2026-08-10 and has never been shown to predict
anything.

**Hysteresis, not thresholds.** Enter on a high rank, exit only on a
substantially worse one. Cross-sectional percentiles jiggle daily, so a bare
threshold generates turnover with no information. Turnover matters concretely:
most anomalies survive costs below roughly 50% one-sided monthly turnover and
few survive above it (Novy-Marx & Velikov 2016), and the most-active retail
quintile underperformed the market by ~6.5 points annually (Barber & Odean 2000).

---

## Success criteria

| # | Criterion | Verified by |
|---|---|---|
| 1 | Every signal has a tier; tier is data, not code | Registry read at runtime |
| 2 | A `candidate` never affects a recommendation | Weight-zero test |
| 3 | Promotion requires walk-forward evidence at t > 3.0 | Promotion gate test |
| 4 | Validation holds out time, never symbols | Fold construction test |
| 5 | Harness returns ~zero excess on the benchmark | Known-answer test |
| 6 | Shifting features forward degrades results | Look-ahead test |
| 7 | Trial count is queryable and monotonic | Trial log |
| 8 | Forecast window chosen from measured decay | IC-by-horizon report |
| 9 | No call emitted before 4-7 pass | Sequencing gate |
| 10 | Every result carries its survivorship caveat | Harness output |

## Open questions

1. **Forecast window** - 6 months assumed; decide from measured decay.
2. **Combination method** - percentile vs winsorized z-score; test as variants.
3. **Weighting** - equal vs inverse-volatility.
4. **Correlation threshold** for promotion - how similar is too similar?
5. **Sector neutrality** - rank within sector, or accept the tilt deliberately?
   Value and quality across a full universe become structural sector bets.
6. **`POSITIONS.md` format** - gitignored, holds held positions and priority.
   Shape it into the priority tier once its contents exist.
