<h1 align="center">Stock Analytics Lakehouse</h1>

<p align="center">
  <strong>Equity market analytics and portfolio decision support on Databricks</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11+-blue.svg" alt="Python">
  <img src="https://img.shields.io/badge/Databricks-Free%20Edition-orange.svg" alt="Databricks">
  <img src="https://img.shields.io/badge/Pipeline-19%20tasks-green.svg" alt="Tasks">
  <img src="https://img.shields.io/badge/Layers-Bronze%20%E2%86%92%20Silver%20%E2%86%92%20Gold-brightgreen.svg" alt="Medallion">
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> &middot; <a href="#how-to-read-the-gold-data">Reading the Data</a> &middot; <a href="#tables">Tables</a> &middot; <a href="#sample-queries">Queries</a> &middot; <a href="#visualizations-in-databricks">Visualizations</a> &middot; <a href="#indicator-formulas">Formulas</a>
</p>

---

## How to Read the Gold Data

You have 8 gold tables. Here's what each one is for and how to use it.

### Start Here: `gold.daily_analytics`

One big table. Zero joins. Every column for every ticker on every date. This is your primary exploration surface.

```sql
SELECT * FROM gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.daily_analytics)
ORDER BY composite_score DESC;
```

**What the columns mean:**

| Group | Columns | How to read it |
|-------|---------|---------------|
| **Price** | `open, high, low, close, volume` | OHLCV for each trading day |
| **Trend** | `ma_50, ma_200, ema_50, ema_200` | Moving averages. Price above MA-200 = long-term uptrend. Price below = downtrend. |
| **Momentum** | `rsi, macd, macd_signal_line, macd_histogram` | RSI > 70 = overbought, < 30 = oversold. MACD histogram > 0 = bullish momentum. |
| **Volatility** | `bollinger_upper, bollinger_lower, bollinger_pct_b, bollinger_bandwidth, atr_14d` | BandWidth < 10th percentile = squeeze (breakout incoming). %B > 1 = above upper band. ATR = average daily range. |
| **Volume** | `obv, mfi` | OBV rising = accumulation. MFI > 80 = overbought, < 20 = oversold (volume-weighted RSI). |
| **Returns** | `change_30d_pct, change_90d_pct, change_365d_pct` | Note: these are 21/63/252 *trading* days, not calendar days. |
| **Fundamentals** | `trailing_pe, forward_pe, market_cap, debt_to_equity, return_on_equity, ...` | Current snapshot (same values for all dates — historical PIT is in `fact_fundamental_snapshot`). |
| **Dividends** | `dividend_yield, dividend_yield_gap, dividend_yield_trap` | `yield_trap = true` means current yield is >1.5pp above 5-year average — often a price-drop trap. |
| **Scoring** | `momentum_pct, value_pct, risk_pct, quality_pct, composite_score` | See below. |

### Understanding Composite Score

`composite_score` = equal-weight average of four `PERCENT_RANK` dimensions, each normalized to 0–1:

| Dimension | Ranks by | High score means |
|-----------|----------|-----------------|
| `momentum_pct` | 30-day return (DESC) | Strong recent price momentum |
| `value_pct` | P/E ratio (ASC) | Low P/E = cheaper = better value |
| `risk_pct` | RSI (ASC) | Low RSI = oversold = less near-term risk |
| `quality_pct` | MFI (DESC) | High MFI = accumulation pressure = quality |

**How to use it:**
- `composite_score > 0.75` — strong candidate, multiple dimensions aligned
- `0.25 < composite_score < 0.75` — mixed signals, look at individual dimensions
- `composite_score < 0.25` — weak across the board, or extreme in one dimension dragging it down

A stock can be #1 in momentum but #20 in value — the composite balances them. Always check the individual dimensions before acting.

### Understanding Alerts

`gold.signal_alerts` fires when a threshold is crossed on the latest date:

| Alert Type | What It Means | How to Act |
|-----------|---------------|------------|
| `RSI_OVERSOLD` | RSI dropped below 30 | Potential buying opportunity — but check fundamentals and trend first |
| `RSI_OVERBOUGHT` | RSI rose above 70 | Consider taking profits or tightening stops |
| `MACD_CROSSOVER` | MACD histogram flipped sign (positive→negative or vice versa) | Positive flip = bullish momentum shift. Negative flip = bearish. Confirm with volume. |
| `BOLLINGER_SQUEEZE` | Bollinger BandWidth in the bottom 10th percentile for this symbol over the last 6 months | Volatility contraction — a breakout is likely. Direction unknown until it happens. |
| `MA200_CROSS_ABOVE` | Price crossed above the 200-day moving average | Classic bullish signal — long-term trend turning up |
| `MA200_CROSS_BELOW` | Price crossed below the 200-day moving average | Classic bearish signal — long-term trend turning down |
| `DIVIDEND_YIELD_TRAP` | Current dividend yield is >1.5pp above the 5-year average | The yield is high because the price dropped, not because dividends increased. Verify payout ratio < 75% before buying. |

**Important:** Alerts are point-in-time snapshots, rebuilt each pipeline run. They reflect the *latest* trading day only. For historical alert patterns, query `gold.signal_history` and compute your own thresholds.

### What Is Congress Doing on My Stocks?

`gold.congressional_trades_summary` aggregates Senate stock trade disclosures per symbol. Join it to `gold.daily_analytics`:

```sql
-- Which congresspeople traded stocks on my watchlist recently?
SELECT
    c.symbol, c.office_name, c.chamber, c.trade_type,
    c.trade_count, c.avg_amount_midpoint, c.last_trade_date,
    a.close, a.composite_score, a.rsi
FROM gold.congressional_trades_summary c
JOIN gold.daily_analytics a
    ON c.symbol = a.symbol
    AND a.as_of_date = (SELECT MAX(as_of_date) FROM gold.daily_analytics)
ORDER BY c.last_trade_date DESC;
```

**Columns in `gold.congressional_trades_summary`:**

| Column | Meaning |
|--------|---------|
| `symbol` | Ticker |
| `office_name` | Senator/Representative name |
| `chamber` | Senate (House data not yet available) |
| `trade_type` | `buy`, `sell`, `sell_full`, `exchange` |
| `trade_count` | Number of reported transactions |
| `avg_amount_midpoint` | Midpoint of the reported dollar range (e.g., $8,000 for "$1,001–$15,000") |
| `max_amount_range` | Upper bound of reported range |
| `total_estimated_value` | Sum of midpoints |
| `last_trade_date` | Most recent filing date |

**Limitations:** Senate data only (45-day filing delay). Amounts are ranges, not exact. House trades require a separate API.

### How Long Until I Can Make Informed Decisions?

| Time Since First Run | What's Reliable | What's Not |
|----------------------|-----------------|------------|
| **Day 1** | Prices, fundamentals, congressional trades, macro data | RSI/MACD/Bollinger (need history), composite scores |
| **1 week** | Short-term RSI (14-day), MACD starting to form | MA-200 (needs 200 days), composite scores unreliable |
| **1 month** | RSI, MACD, Bollinger Bands, OBV, MFI all valid | MA-200 still converging, 90-day and 365-day returns not yet meaningful |
| **3 months** | MA-200 converging, composite scores starting to be meaningful | 365-day returns, long-term trend analysis |
| **1 year** | Everything. Full backtest capability, MA-200 reliable, all return windows valid | Factor regression (roadmap) |

The pipeline uses `period="max"` for prices, so yfinance provides all available history on first run. The warmup period (200 trading days ≈ 10 months) is dropped for EMA-200 convergence. After first run, you'll have years of historical data — but EMA-200 and 252-day returns need time to stabilize.

---

## Pipeline DAG

19 tasks, fully parallelized where possible:

```
setup_catalog
 ├── ingest_prices ──────────────────┐
 ├── ingest_fundamentals ───────────┤
 ├── ingest_fred ────────────────────┤  Bronze
 ├── ingest_fama_french ────────────┤  (append-only)
 ├── ingest_congressional ──────────┤
 ├── ingest_splits ─────────────────┘
 │
 │   build_silver_prices ──────────────┐
 │   build_silver_fundamentals ───────┤  Silver
 │   build_silver_signals ─────────────┤  (MERGE dedup)
 │   build_silver_congressional ──────┤
 │   build_silver_splits ──────────────┘
 │
 │   build_gold_dim_date ──────────────────┐
 │   build_gold_dim_security ───────────────┤  Gold dims
 │                                          │
 │   build_gold_fact_market ────────────────┤  Gold facts
 │   build_gold_fact_fundamentals ─────────┤  (star schema)
 │   build_gold_fact_signals ───────────────┘
 │
 └── build_gold_analytics ───────────────── Gold serving tables
 └── build_gold_congressional ───────────── Gold serving tables
```

<details>
<summary><strong>Scheduling &amp; Data Accumulation</strong></summary>

The pipeline runs **daily after US market close** (10 PM ET, Mon-Fri). NYSE closes at 4 PM ET; yfinance data is typically available by 5-6 PM.

| Cadence | Cron | Trade-off |
|---------|------|-----------|
| **Daily (recommended)** | `0 0 22 ? * MON-FRI` | Captures every trading day |
| Weekly | `0 0 22 ? * FRI` | Misses intraweek signal flips |
| Ad-hoc | Manual | Run after market events |

Run manually: `databricks bundle run stock_analytics_pipeline`

**Idempotency — re-runs are safe:**

| Layer | Write Mode | What Happens on Re-run |
|-------|-----------|----------------------|
| Bronze prices | append + `_run_id` | New rows appended. Silver MERGE dedupes on `(symbol, date)`. |
| Bronze fundamentals | SCD2 MERGE | Expired versions get `is_current=false`, new versions inserted. No duplicates. |
| Bronze macro/factors | append | New rows appended. |
| Silver | MERGE on natural keys | New dates inserted, existing rows updated. No duplicates. |
| Gold facts | CREATE OR REPLACE | Rebuilt from Silver each run. |
| Gold serving | CREATE OR REPLACE | Rebuilt from Silver each run. |

After 1 year: ~5,040 rows per ticker-level table. After 3 years: ~15,120 rows.

See [`documentation/scheduling-and-accumulation.md`](documentation/scheduling-and-accumulation.md) for full details.

</details>

---

## Tables

### Bronze — Raw Evidence, Append-Only

| Table | Mode | Content |
|-------|------|---------|
| `bronze.daily_prices` | append | OHLCV + `_run_id`, `_ingest_ts`, `_source_event_ts` |
| `bronze.company_fundamentals` | append (SCD2) | Fundamentals with `effective_from/to`, `is_current`, `attr_hash` |
| `bronze.macro_indicators` | append | FRED: Treasury yields, Fed funds, unemployment, CPI |
| `bronze.factor_returns` | append | Fama-French 5-factor + momentum daily returns |
| `bronze.congressional_trades` | append | Senate stock trade disclosures (symbol, office, amount, date) |
| `bronze.stock_splits` | append | Stock split events from yfinance |

### Silver — Validated, Deduped

| Table | Mode | Key |
|-------|------|-----|
| `silver.daily_prices` | MERGE | `(symbol, date)` |
| `silver.daily_signals` | MERGE | `(symbol, as_of_date)` |
| `silver.fundamentals_current` | MERGE | `(symbol)` — current SCD2 version |
| `silver.congressional_trades` | MERGE | `(symbol, transaction_date, office_name, trade_type)` |
| `silver.stock_splits` | MERGE | `(symbol, split_date)` |

### Gold — Two Ways to Query

#### Star Schema (BI, dashboards, structured analysis)

```
dim_date ──── dim_security ───┬── fact_market_price_daily
                               ├── fact_fundamental_snapshot
                               └── fact_signal_snapshot
```

Three fact tables joined through two conformed dimensions. Each fact is a different business process. Use for drill-across queries, filters, or clear data lineage.

| Table | Mode | Content |
|-------|------|---------|
| `gold.dim_date` | replace | NYSE trading calendar, `is_trading_day`, `is_early_close`, `is_split_day`, prior/next trading day |
| `gold.dim_security` | MERGE (SCD2) | Ticker name, sector, industry, exchange, country |
| `gold.fact_market_price_daily` | replace | OHLCV + 1d/5d/21d/63d/252d returns |
| `gold.fact_fundamental_snapshot` | replace | PE, EPS, margins, growth, debt ratios (point-in-time from SCD2) |
| `gold.fact_signal_snapshot` | replace | All indicators + PERCENT_RANK composite scores |

#### `gold.daily_analytics` — One Big Table (exploration, ad-hoc, ML)

A single wide table joining signals + prices + fundamentals. **Zero joins needed.**

| | Star Schema | `daily_analytics` |
|:-:|---|---|
| **Joins** | 2-3 per query | Zero |
| **Best for** | BI, drill-across, lineage | Exploration, ML, ad-hoc |
| **Flexibility** | Add new fact independently | Add column = rebuild table |

### Gold Serving Tables

| Table | Content | Use For |
|-------|---------|---------|
| `gold.daily_analytics` | **Master wide table — all prices, indicators, fundamentals.** | Ad-hoc queries, ML features, exploration |
| `gold.watchlist_ranked` | Top tickers ranked by composite score | "What should I look at today?" |
| `gold.signal_alerts` | Threshold alerts (RSI, MACD, Bollinger, MA-200, dividend trap) | "What's happening right now?" |
| `gold.signal_history` | Time series of all indicators per symbol | Charting, backtesting |
| `gold.benchmark_compare` | Each ticker vs SPY/QQQ 30d/90d/365d returns | Relative performance |
| `gold.portfolio_candidates` | Bucketed: top_momentum, oversold, value_dividend, low_volatility | Stock screening |
| `gold.congressional_trades_summary` | Congressional trades per symbol/office | "What is Congress trading?" |
| `gold.trade_log` | Manual trade journal (INSERT via SQL) | Tracking your own trades |

---

## Sample Queries

### Daily Analytics (zero-join table)

```sql
-- Top 10 momentum stocks today
SELECT symbol, short_name, close, change_30d_pct, rsi, composite_score
FROM gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.daily_analytics)
ORDER BY change_30d_pct DESC
LIMIT 10;

-- Oversold stocks with strong fundamentals
SELECT symbol, close, rsi, trailing_pe, return_on_equity, sector
FROM gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.daily_analytics)
  AND rsi < 30
  AND trailing_pe < 25
  AND return_on_equity > 0.15
ORDER BY rsi;

-- Sector rotation
SELECT sector, AVG(rsi) AS avg_rsi, AVG(change_30d_pct) AS avg_mom, COUNT(*) AS n
FROM gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.daily_analytics)
  AND sector IS NOT NULL
GROUP BY sector
ORDER BY avg_rsi DESC;

-- Value stocks with dividend safety
SELECT symbol, close, trailing_pe, dividend_yield, dividend_yield_trap, payout_ratio
FROM gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.daily_analytics)
  AND trailing_pe BETWEEN 8 AND 18
  AND dividend_yield > 0.02
  AND NOT dividend_yield_trap
  AND payout_ratio < 0.75
ORDER BY dividend_yield DESC;
```

### Congressional Trades

```sql
-- Which senators bought stocks I'm watching?
SELECT c.symbol, c.office_name, c.trade_type, c.trade_count,
       c.avg_amount_midpoint, c.last_trade_date
FROM gold.congressional_trades_summary c
WHERE c.trade_type = 'buy'
ORDER BY c.last_trade_date DESC;

-- Congressional activity on high-momentum stocks
SELECT c.symbol, c.office_name, c.trade_type, c.trade_count,
       a.change_30d_pct, a.rsi, a.composite_score
FROM gold.congressional_trades_summary c
JOIN gold.daily_analytics a
  ON c.symbol = a.symbol
  AND a.as_of_date = (SELECT MAX(as_of_date) FROM gold.daily_analytics)
WHERE a.composite_score > 0.75
ORDER BY a.composite_score DESC;
```

### Alerts

```sql
-- Today's signal alerts
SELECT alert_type, symbol, alert_value, as_of_date
FROM gold.signal_alerts
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.signal_alerts)
ORDER BY alert_type, symbol;

-- Stocks crossing above 200-day MA (bullish breakout)
SELECT symbol, as_of_date, alert_value AS close_price
FROM gold.signal_alerts
WHERE alert_type = 'MA200_CROSS_ABOVE';

-- Bollinger squeeze candidates
SELECT symbol, bollinger_bandwidth
FROM gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.daily_analytics)
  AND bollinger_bandwidth IS NOT NULL
ORDER BY bollinger_bandwidth ASC
LIMIT 10;
```

### Star Schema (drill-across)

```sql
-- How does momentum correlate with valuation across sectors?
SELECT s.sector, s.industry,
       AVG(sig.rsi) AS avg_rsi,
       AVG(f.`trailingPE`) AS avg_pe,
       AVG(f.`dividendYield`) AS avg_yield
FROM gold.fact_signal_snapshot sig
JOIN gold.dim_security s ON sig.security_key = s.security_key AND s.is_current = true
JOIN gold.dim_date d ON sig.date_key = d.date_key
LEFT JOIN gold.fact_fundamental_snapshot f
  ON sig.security_key = f.security_key AND sig.date_key = f.date_key
WHERE d.date = (
  SELECT MAX(date) FROM gold.dim_date WHERE is_trading_day = TRUE
)
GROUP BY s.sector, s.industry
ORDER BY avg_rsi DESC;
```

More queries in [`documentation/sql-exploration-queries.md`](documentation/sql-exploration-queries.md).

---

## Visualizations in Databricks

### 1. Watchlist Dashboard — "What should I look at today?"

**Table:** `gold.watchlist_ranked`

```sql
SELECT symbol, composite_score, momentum_pct, value_pct, risk_pct, quality_pct,
       rsi, pe_ratio, last_closing_price
FROM gold.watchlist_ranked
ORDER BY composite_score DESC;
```

**Chart:** Bar chart. X = `symbol`, Y = `composite_score`. Color by sector (join to `daily_analytics` for sector).

### 2. Alert Dashboard — "What's happening right now?"

**Table:** `gold.signal_alerts`

```sql
SELECT alert_type, symbol, alert_value, as_of_date
FROM gold.signal_alerts
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.signal_alerts)
ORDER BY alert_type, symbol;
```

**Chart:** Table with conditional formatting. RSI_OVERSOLD rows in green, RSI_OVERBOUGHT in red, BOLLINGER_SQUEEZE in yellow, MA200 crosses in blue.

### 3. Congressional Tracker — "What is Congress trading?"

**Table:** `gold.congressional_trades_summary` + `gold.daily_analytics`

```sql
SELECT c.symbol, c.office_name, c.chamber, c.trade_type,
       c.trade_count, c.avg_amount_midpoint, c.last_trade_date,
       a.close, a.composite_score, a.rsi
FROM gold.congressional_trades_summary c
JOIN gold.daily_analytics a
  ON c.symbol = a.symbol
  AND a.as_of_date = (SELECT MAX(as_of_date) FROM gold.daily_analytics)
ORDER BY c.last_trade_date DESC;
```

**Chart:** Table grouped by `office_name`. Add a scatter plot: X = `composite_score`, Y = `avg_amount_midpoint`, color by `trade_type`.

### 4. Sector Rotation Heatmap

**Table:** `gold.daily_analytics`

```sql
SELECT sector, AVG(rsi) AS avg_rsi, AVG(change_30d_pct) AS avg_momentum,
       AVG(composite_score) AS avg_score
FROM gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.daily_analytics)
  AND sector IS NOT NULL
GROUP BY sector
ORDER BY avg_score DESC;
```

**Chart:** Heatmap or treemap. Size = count of stocks, color = `avg_score`.

### 5. Price + MACD Dual-Axis Chart

**Table:** `gold.signal_history`

```sql
SELECT as_of_date, symbol, last_closing_price, macd, macd_signal_line, macd_histogram
FROM gold.signal_history
WHERE symbol = 'AAPL'
ORDER BY as_of_date DESC
LIMIT 90;
```

**Chart:** Line chart. Left Y-axis = `last_closing_price`, right Y-axis = `macd_histogram` (bar). Add `macd` and `macd_signal_line` as lines on the right axis.

### 6. Dividend Yield Trap Scanner

**Table:** `gold.daily_analytics`

```sql
SELECT symbol, close, dividend_yield, dividend_yield_gap, dividend_yield_trap,
       payout_ratio, trailing_pe
FROM gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.daily_analytics)
  AND dividend_yield > 0.02
ORDER BY dividend_yield_gap DESC;
```

**Chart:** Scatter plot. X = `dividend_yield`, Y = `dividend_yield_gap`, color = `dividend_yield_trap` (red/green). Size = `payout_ratio`.

### 7. Benchmark Comparison

**Table:** `gold.benchmark_compare`

```sql
SELECT symbol, last_closing_price,
       change_30d_pct, spy_change_30d, qqq_change_30d,
       change_90d_pct, spy_change_90d, qqq_change_90d,
       change_365d_pct, spy_change_365d, qqq_change_365d
FROM gold.benchmark_compare
ORDER BY change_365d_pct DESC;
```

**Chart:** Grouped bar chart. Each ticker shows its 30d/90d/365d returns alongside SPY and QQQ.

Step-by-step Databricks dashboard tutorials in [`documentation/databricks-visualization-tutorials.md`](documentation/databricks-visualization-tutorials.md).

---

## Alerting Flow

### Current State

Alerts are **SQL tables**, rebuilt on each pipeline run. There is no push notification mechanism. To check alerts:

```sql
SELECT * FROM gold.signal_alerts
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.signal_alerts);
```

### How Alerts Work

1. Pipeline runs daily after market close
2. `build_gold_analytics` computes alert thresholds against the latest snapshot + signal history
3. `gold.signal_alerts` is replaced with that day's alerts
4. You query the table to see what fired

### Alert Types and Thresholds

| Alert | Threshold | Meaning |
|-------|-----------|---------|
| `RSI_OVERSOLD` | RSI < 30 | Price may be oversold — potential buy |
| `RSI_OVERBOUGHT` | RSI > 70 | Price may be overbought — potential sell |
| `MACD_CROSSOVER` | Histogram flips sign | Momentum direction change |
| `BOLLINGER_SQUEEZE` | BandWidth in bottom 10th percentile (per-symbol, 126-day window) | Volatility contraction — breakout likely |
| `MA200_CROSS_ABOVE` | Price crosses above 200-day MA | Long-term bullish signal |
| `MA200_CROSS_BELOW` | Price crosses below 200-day MA | Long-term bearish signal |
| `DIVIDEND_YIELD_TRAP` | Current yield > 5-yr avg + 1.5pp | High yield from price drop, not dividend growth |

### Limitations

- **No push notifications** — alerts exist only as table rows
- **No alert history** — `signal_alerts` is replaced each run (use `signal_history` for historical thresholds)
- **No deduplication** — same alert fires each day until the condition clears
- **No severity levels** — all alerts are equal priority

### Roadmap

| Priority | Feature |
|----------|---------|
| High | Alert history table (append, don't replace) |
| High | Email/webhook notification on alert fire |
| Medium | Alert severity (critical, warning, info) |
| Medium | Net congressional direction per symbol |
| Low | News/sentiment integration |

---

## Quick Start

### Prerequisites

- Databricks CLI (`brew install databricks`)
- Terraform (`brew install terraform`)
- Python 3.11+
- FRED API key ([free](https://fred.stlouisfed.org/docs/api/api_key.html))

### 1. Configure

```bash
git clone <repo-url> && cd stock_watch_list_analysis
cp .env.example .env
# Edit .env: add your tickers and FRED_API_KEY
```

### 2. Local Development

```bash
python -m venv stocks-venv && source stocks-venv/bin/activate
pip install -r requirements.txt
pytest tests/ -v
```

### 3. Deploy and Run

```bash
DATABRICKS_TF_EXEC_PATH=$(which terraform) \
DATABRICKS_TF_VERSION=$(terraform version -json | jq -r '.terraform_version') \
databricks bundle deploy

databricks bundle run stock_analytics_pipeline
```

### 4. Explore

```sql
SELECT * FROM stock_analytics.gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.daily_analytics)
ORDER BY composite_score DESC;
```

---

## Indicator Formulas

All formulas use industry-standard definitions with citations.

| Indicator | Formula | Citation |
|-----------|---------|----------|
| RSI (14) | Wilder's smoothing `ewm(alpha=1/14)` | Wilder, "New Concepts" (1978) |
| ATR (14) | Same RMA as RSI | Wilder, same |
| MACD | EMA(12) - EMA(26), signal = EMA(9) | Appel (1979) |
| Histogram | MACD - signal | Standard |
| Bollinger | SMA(20) +/- 2 sigma | Bollinger (2001) |
| %B | (price - lower) / (upper - lower) | Bollinger |
| BandWidth | (upper - lower) / middle | Bollinger |
| OBV | Cumulative +/-volume | Granville (1963) |
| MFI | 100 - 100/(1 + money_ratio) | Quong & Soudack (1989) |
| Sharpe | (return - rf) / sigma | Sharpe (1994) |
| Sortino | (return - target) / downside sigma | Sortino & Price (1994) |
| Max Drawdown | max peak-to-trough | Standard |
| Beta | Cov(stock, bench) / Var(bench) | Standard |
| Dividend Yield Gap | TTM yield - 5-year avg yield | Traps: gap > 1.5pp |

---

## Architecture Decisions

- **Bronze stores evidence, not truth** — append-only, never overwrite. Every row carries `_run_id`, `_ingest_ts`, `_source_system`, `_source_event_ts`, `_load_type`.
- **Fundamentals are beliefs, not facts** — SCD2 with `attr_hash` change detection suppresses phantom versions from unchanged yfinance replays.
- **Point-in-time correctness** — `_source_event_ts` (trade date) vs `_ingest_ts` (receipt time). Backtests using revised data show 15-25% higher Sharpe ratios. Never confuse them.
- **Kimball in Gold** — star schema with conformed dimensions. Different business processes = different fact tables. Serving tables coexist for dashboards.
- **`gold.daily_analytics` is the exploration surface** — one big table, zero joins, all columns. Start here for ad-hoc questions.
- **PERCENT_RANK not raw RANK** — normalize each dimension to [0,1] before combining. Raw rank sums favor large-cap stocks.
- **Liquid Clustering** on `(symbol, date)` for new tables — no partitioning, no Z-ORDER.