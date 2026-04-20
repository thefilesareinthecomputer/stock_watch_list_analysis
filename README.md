# Stock Analytics Lakehouse

**Equity market analytics and portfolio decision support on Databricks.**

Bronze → Silver → Gold medallion pipeline with Kimball dimensional modeling,
point-in-time correctness, SCD2 versioning, and 14 production tasks.

<p align="center">
  <img src="static-assets/stock-dashboard.png" alt="Stock Dashboard" width="700">
</p>

<p align="center">
  <img src="static-assets/databricks-sql.png" alt="Databricks SQL Editor" width="700">
</p>

<p align="center">
  <img src="static-assets/deployment.png" alt="Databricks Deployment" width="700">
</p>

---

## What It Does

| Feature | Detail |
|---------|--------|
| **20+ tickers daily OHLCV** | yfinance → Bronze (append-only, PIT metadata) |
| **50+ indicators per ticker** | RSI, MACD, Bollinger %B, OBV, MFI, Sharpe, Sortino, max drawdown, beta |
| **Wilder's smoothing** | Industry-standard RSI/ATR with citations — not Cutler's SMA variant |
| **Kimball star schema** | `dim_date` (NYSE calendar), `dim_security` (SCD2), 3 fact tables |
| **One Big Table for exploration** | `gold.daily_analytics` — every column, zero joins, ad-hoc ready |
| **Point-in-time Bronze** | Every row carries `_run_id`, `_ingest_ts`, `_source_event_ts` — never confuse receipt time with trade time |
| **SCD2 fundamentals** | PE, EPS, analyst targets get revised. Each revision creates a new version, never overwrites |
| **4-dim composite scoring** | Momentum, value, risk, quality — PERCENT_RANK normalized, not raw rank sums |
| **Signal alerts** | RSI crossover, MACD histogram flip, Bollinger squeeze, MA-200 crossover, dividend yield trap |
| **Macro context** | FRED yield curve, Fed funds, unemployment, CPI + Fama-French 5-factor |
| **Data quality checks** | Null %, duplicate keys, impossible prices, date monotonicity, future-dated events |

---

## Pipeline DAG

14 tasks, fully parallelized where possible:

```
setup_catalog
    ├── ingest_prices ──────────────────┐
    ├── ingest_fundamentals ─────────────┤
    ├── ingest_fred ─────────────────────┤   ← Bronze (append-only)
    ├── ingest_fama_french ──────────────┘
    │
    ├── build_gold_dim_date ─────────────┐
    │   build_silver_prices ─────────────┤
    │   build_silver_fundamentals ────────┤   ← Silver (MERGE dedup)
    │   build_silver_signals ─────────────┘
    │
    │   build_gold_dim_security ──────────┐
    │   build_gold_fact_market ───────────┤
    │   build_gold_fact_fundamentals ─────┤   ← Gold (star schema + serving)
    │   build_gold_fact_signals ──────────┘
    │
    └── build_gold_analytics ────────────────── serving tables
```

### Scheduling

The pipeline runs **daily after US market close**. The default schedule is:

```
0 0 22 ? * MON-FRI    (10:00 PM ET, Monday–Friday)
```

**Why after close**: NYSE closes at 4:00 PM ET. yfinance typically has complete daily data by 5–6 PM ET. The 10 PM slot provides a safe buffer for data availability.

**Recommended cadences**:

| Cadence | What | Why |
|---------|------|-----|
| **Daily (weekdays)** | Full pipeline | Captures every trading day's price + indicator changes |
| **Weekly** | Enough for casual monitoring | Misses intraweek signal flips (MACD crossovers, RSI entries) |
| **Ad-hoc** | Run manually after market events | Earnings surprises, Fed decisions, market corrections |

To change the schedule, edit `databricks.yml` → `schedule.quartz_cron_expression`.

To run manually:
```bash
databricks bundle run stock_analytics_pipeline
```

---

## Tables

### Bronze (raw evidence, append-only)

| Table | Mode | Content |
|-------|------|---------|
| `bronze.daily_prices` | append | OHLCV + `_run_id`, `_ingest_ts`, `_source_event_ts` |
| `bronze.company_fundamentals` | append (SCD2) | Fundamentals with `effective_from/to`, `is_current`, `attr_hash` |
| `bronze.macro_indicators` | append | FRED: Treasury yields, Fed funds, unemployment, CPI |
| `bronze.factor_returns` | append | Fama-French 5-factor + momentum daily returns |

### Silver (validated, deduped)

| Table | Mode | Key |
|-------|------|-----|
| `silver.daily_prices` | MERGE | `(symbol, date)` |
| `silver.daily_signals` | MERGE | `(symbol, as_of_date)` |
| `silver.fundamentals_current` | MERGE | `(symbol)` — current SCD2 version |

### Gold — Two Ways to Query

The Gold layer provides **two complementary access patterns** for the same data. Use whichever fits your use case.

#### Pattern 1: Star Schema (structured analysis, BI, dashboards)

```
              dim_security ──┐
dim_date ─────┤               ├── fact_market_price_daily
              │               ├── fact_fundamental_snapshot
              │               └── fact_signal_snapshot
              └─────────────────────────────────┘
```

- **3 fact tables** joined through **2 conformed dimensions**
- Each fact = a different business process (market observation, fundamental snapshot, signal computation)
- Surrogate keys (`security_key`, `date_key`) enable SCD2 tracking
- **Drill-across queries** join facts through shared dimensions

**When to use**: Building dashboards with filters/drill-down, cross-referencing facts, audit/lineage needs, adding new facts without touching others.

#### Pattern 2: `gold.daily_analytics` — One Big Table (exploration, ad-hoc)

```sql
SELECT * FROM gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.daily_analytics)
ORDER BY composite_score DESC
```

**What feeds into it:**

| Source | Columns |
|--------|---------|
| `silver.daily_prices` | open, high, low, close, volume |
| `silver.daily_signals` | All technical indicators, returns, composite scores, trade signals |
| `silver.fundamentals_current` | sector, industry, market_cap, PE, EPS, margins, growth, debt |

**Every column:**

```
IDENTITY:   symbol, as_of_date
PRICE:      open, high, low, close, volume
SIGNALS:    trade_signal, rsi, macd, macd_signal_line, macd_histogram,
            bollinger_upper, bollinger_lower, bollinger_pct_b, bollinger_bandwidth,
            obv, mfi, ma_50, ma_200, ema_50, ema_200, atr_14d
RETURNS:    last_day_change_abs, last_day_change_pct,
            change_30d_pct, change_90d_pct, change_365d_pct, last_closing_price
FUNDAMENTALS: short_name, long_name, sector, industry, country, currency, exchange,
              market_cap, trailing_pe, forward_pe, price_to_book,
              trailing_eps, forward_eps, dividend_rate, dividend_yield, payout_ratio,
              beta, profit_margins, operating_margins, gross_margins,
              revenue_growth, earnings_growth, return_on_equity, return_on_assets,
              debt_to_equity, current_ratio, free_cashflow
DIVIDEND:   dividend_yield, dividend_yield_gap, dividend_yield_trap
```

**When to use**: Ad-hoc SQL exploration, ML feature engineering, "show me everything about AAPL today", zero-join dashboards.

| Aspect | Star Schema | `daily_analytics` (OBT) |
|--------|-------------|--------------------------|
| Joins | 2–3 per query | Zero |
| Best for | BI, drill-across, lineage | Exploration, ML, ad-hoc |
| Flexibility | Add new fact without touching others | Add column = rebuild table |
| Use case | "Dashboard filtered by sector and date" | "Everything about AAPL right now" |

#### Gold Table Reference

**Dimensions:**

| Table | Mode | Content |
|-------|------|---------|
| `gold.dim_date` | replace | NYSE trading calendar, is_trading_day, early close, prior/next trading day |
| `gold.dim_security` | MERGE (SCD2) | Ticker name, sector, industry, exchange, country. `attr_hash` suppresses phantom versions |

**Facts (each has one declared grain):**

| Table | Grain | Content |
|-------|-------|---------|
| `gold.fact_market_price_daily` | 1 row per (security, date) | OHLCV + 1d/5d/21d/63d/252d returns |
| `gold.fact_fundamental_snapshot` | 1 row per (security, date) | PE, EPS, margins, growth, debt ratios |
| `gold.fact_signal_snapshot` | 1 row per (security, date) | All indicators + PERCENT_RANK composite scores |

**Serving (denormalized, dashboard-ready):**

| Table | Content |
|-------|---------|
| `gold.daily_analytics` | **Master wide table — all prices, indicators, fundamentals. Zero joins.** |
| `gold.watchlist_ranked` | Top tickers by momentum, value, risk, quality, composite |
| `gold.signal_alerts` | RSI cross, MACD flip, Bollinger squeeze, MA cross, yield trap |
| `gold.signal_history` | Time series for charting |
| `gold.benchmark_compare` | vs SPY/QQQ cumulative returns |
| `gold.portfolio_candidates` | top_momentum, oversold, value_dividend, low_volatility |
| `gold.trade_log` | Manual trade journal (INSERT via SQL) |

---

## Sample Queries

### Quick Exploration (zero joins)

```sql
-- Top 10 momentum stocks today
SELECT symbol, short_name, close, change_30d_pct, rsi, composite_score
FROM gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.daily_analytics)
ORDER BY change_30d_pct DESC LIMIT 10;

-- Oversold stocks with strong fundamentals
SELECT symbol, close, rsi, trailing_pe, return_on_equity, sector
FROM gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.daily_analytics)
  AND rsi < 30 AND trailing_pe < 25 AND return_on_equity > 0.15
ORDER BY rsi;

-- Sector rotation: average RSI and momentum
SELECT sector, AVG(rsi) AS avg_rsi, AVG(change_30d_pct) AS avg_momentum, COUNT(*) AS stocks
FROM gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.daily_analytics)
GROUP BY sector ORDER BY avg_rsi DESC;

-- Value stocks with dividend safety
SELECT symbol, close, trailing_pe, dividend_yield, payout_ratio
FROM gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.daily_analytics)
  AND trailing_pe BETWEEN 8 AND 18 AND dividend_yield > 0.02
  AND dividend_yield_trap = false AND payout_ratio < 0.75
ORDER BY dividend_yield DESC;
```

### Star Schema (drill-across)

```sql
-- Momentum vs valuation across sectors
SELECT s.sector, s.industry,
       AVG(sig.rsi) AS avg_rsi, AVG(f.trailing_pe) AS avg_pe
FROM gold.fact_signal_snapshot sig
JOIN gold.dim_security s ON sig.security_key = s.security_key
JOIN gold.dim_date d ON sig.date_key = d.date_key
LEFT JOIN gold.fact_fundamental_snapshot f
    ON sig.security_key = f.security_key AND sig.date_key = f.date_key
WHERE d.calendar_date = (SELECT MAX(calendar_date) FROM gold.dim_date WHERE is_trading_day = TRUE)
GROUP BY s.sector, s.industry ORDER BY avg_rsi DESC;
```

### Alerts

```sql
-- Today's signal alerts
SELECT alert_type, symbol, alert_value FROM gold.signal_alerts
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.signal_alerts)
ORDER BY alert_type, symbol;

-- Bollinger squeeze (volatility compression)
SELECT symbol, bollinger_bandwidth FROM gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.daily_analytics)
  AND bollinger_bandwidth IS NOT NULL
ORDER BY bollinger_bandwidth ASC LIMIT 10;

-- Dividend yield traps (TTM yield well above forward yield)
SELECT symbol, alert_value AS yield_gap FROM gold.signal_alerts
WHERE alert_type = 'DIVIDEND_YIELD_TRAP';
```

### Time Series

```sql
-- 30-day RSI trend
SELECT as_of_date, rsi, macd, macd_signal_line, bollinger_pct_b
FROM gold.signal_history WHERE symbol = 'AAPL'
ORDER BY as_of_date DESC LIMIT 30;

-- vs SPY/QQQ
SELECT symbol, change_30d_pct, spy_change_30d, change_90d_pct, spy_change_90d
FROM gold.benchmark_compare ORDER BY change_90d_pct DESC;
```

More queries in [`sql/gold_views.sql`](sql/gold_views.sql).

---

## Visualization Tips

| Chart | Best Table | How |
|-------|-----------|-----|
| Ranked watchlist | `gold.watchlist_ranked` | Bar chart by `composite_score DESC`, color by sector |
| Momentum heatmap | `gold.daily_analytics` | Pivot `sector` × `change_30d_pct`, color scale green→red |
| RSI overbought/oversold | `gold.signal_alerts` | Filter `RSI_OVERSOLD`/`RSI_OVERBOUGHT`, conditional formatting |
| Bollinger squeeze | `gold.daily_analytics` | Filter low `bollinger_bandwidth`, small multiples by sector |
| Sector rotation | `gold.daily_analytics` | Scatter: x=`change_30d_pct`, y=`rsi`, bubble=`market_cap`, color=`sector` |
| Price + MACD overlay | `gold.signal_history` | Dual-axis: price line + MACD histogram bars |
| Dividend yield trap | `gold.signal_alerts` | Filter `DIVIDEND_YIELD_TRAP`, red-flagged table |
| Fundamentals radar | `gold.daily_analytics` | 1 stock, radar chart on normalized PE, yield, margins, ROE, growth |
| Risk-return scatter | `gold.daily_analytics` | x=`beta`, y=`change_365d_pct`, color by sector |

---

## Quick Start

### Prerequisites

- Databricks CLI (`brew install databricks`)
- Terraform (`brew install terraform`)
- Python 3.11+
- FRED API key ([get one free](https://fred.stlouisfed.org/docs/api/api_key.html))

### 1. Configure

```bash
git clone <repo-url> && cd stock_watch_list_analysis
git checkout develop

# Create .env from example
cp .env.example .env
# Edit .env — add your tickers, Databricks host, FRED API key
```

### 2. Local Development

```bash
python -m venv stocks-venv && source stocks-venv/bin/activate
pip install -r requirements.txt
pytest tests/ -v        # 99 passed, 2 skipped
```

### 3. Deploy & Run

```bash
DATABRICKS_TF_EXEC_PATH=$(which terraform) \
DATABRICKS_TF_VERSION=$(terraform version -json | jq -r '.terraform_version') \
databricks bundle deploy

databricks bundle run stock_analytics_pipeline
```

### 4. Explore

Open Databricks SQL Editor and start with:

```sql
SELECT * FROM stock_analytics.gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.daily_analytics)
ORDER BY composite_score DESC;
```

---

## Indicator Formulas

All formulas use industry-standard definitions with citations. No shortcuts.

| Indicator | Formula | Citation |
|-----------|---------|----------|
| RSI (14) | Wilder's smoothing `ewm(alpha=1/14)` | Wilder, "New Concepts" (1978) |
| ATR (14) | Same RMA as RSI | Wilder, same |
| MACD | EMA(12) − EMA(26), signal = EMA(9) | Appel (1979) |
| Histogram | MACD − signal | Standard |
| Bollinger | SMA(20) ± 2σ | Bollinger (2001) |
| %B | (price − lower) / (upper − lower) | Bollinger |
| BandWidth | (upper − lower) / middle | Bollinger |
| OBV | Cumulative ±volume | Granville (1963) |
| MFI | 100 − 100/(1 + money_ratio) | Quong & Soudack (1989) |
| Sharpe | (return − rf) / σ | Sharpe, J. Portfolio Mgmt (1994) |
| Sortino | (return − target) / downside σ | Sortino & Price (1994) |
| Max Drawdown | max peak-to-trough | Standard |
| Beta | Cov(stock, bench) / Var(bench) | Standard |

---

## Key Architecture Decisions

- **Bronze stores evidence, not truth** — append-only, never overwrite. Each row carries `_run_id`, `_ingest_ts`, `_source_system`, `_source_event_ts`, `_load_type`.
- **Fundamentals are beliefs, not facts** — SCD2 with `attr_hash` change detection suppresses phantom versions from unchanged yfinance replays.
- **Point-in-time correctness** — `_source_event_ts` (trade date) vs `_ingest_ts` (receipt time). Backtests using revised data show 15-25% higher Sharpe ratios. Never confuse them.
- **Kimball in Gold** — star schema with conformed dimensions (`dim_date`, `dim_security`). Different business processes = different fact tables. Serving tables coexist for dashboards.
- **`gold.daily_analytics` is the exploration surface** — one big table, zero joins, all columns. Start here for ad-hoc questions.
- **PERCENT_RANK not raw RANK** — normalize each dimension to [0,1] before combining. Raw rank sums favor large-cap stocks.
- **Liquid Clustering** on `(symbol, date)` for new tables — no partitioning, no Z-ORDER.

---

## Data Quality

Every layer has validation:

| Layer | Checks |
|-------|--------|
| Bronze | Non-empty, expected columns, null %, price consistency (high ≥ low, volume ≥ 0), `_source_event_ts ≤ _ingest_ts` |
| Silver | `(symbol, date)` dedup verified, no impossible prices, date monotonicity |
| Gold | Latest date exists, rank columns populated, dashboard tables non-empty |

---

## Non-Goals

Tick data / real-time streaming / options / broker integration / autonomous trading / heavy ML / NLP / hourly intraday (Phase 3).

## Resources

- [Databricks Medallion Architecture](https://docs.databricks.com/aws/en/lakehouse/medallion)
- [Databricks Data Modeling](https://docs.databricks.com/aws/en/transform/data-modeling)
- [Kimball Dimensional Modeling Techniques](https://www.kimballgroup.com/wp-content/uploads/2013/08/2013.09-Kimball-Dimensional-Modeling-Techniques11.pdf)
- [Point-in-Time Data for Investment Decisions](https://starqube.com/point-in-time-data/)
- [Databricks Liquid Clustering](https://databricks.com/blog/announcing-general-availability-liquid-clustering)
- [FRED API](https://fred.stlouisfed.org/docs/api/fred/)
- [Fama-French Data Library](https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html)