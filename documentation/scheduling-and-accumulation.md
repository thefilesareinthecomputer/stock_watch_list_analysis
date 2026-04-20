# Scheduling and Data Accumulation

## How Often to Run

The pipeline is designed to run **once per trading day after US market close**.

| Schedule | Cron | Time (ET) | Notes |
|----------|------|-----------|-------|
| **Daily (recommended)** | `0 0 22 ? * MON-FRI` | 10:00 PM | Captures every trading day. Best for signal tracking. |
| Weekly | `0 0 22 ? * FRI` | 10:00 PM Friday | Misses intraweek signal flips (MACD crossovers, RSI entries). |
| Ad-hoc | Manual | Anytime | Run after market events (earnings, Fed decisions). |

The default in `databricks.yml` is **daily at 10 PM ET weekdays**. This gives yfinance time to post complete data (market closes at 4 PM ET, data typically available by 5-6 PM).

To change the schedule, edit the `schedule` block in `databricks.yml`.

## Does Data Accumulate Over Time?

**Yes, but differently at each layer.** This is the core design of the medallion architecture.

### Bronze — Appends Every Run (Accumulates)

| Table | Behavior | What Accumulates |
|-------|----------|------------------|
| `bronze.daily_prices` | **Append** | Every run adds new rows. Re-running the same date adds duplicate rows (deduped by Silver). |
| `bronze.company_fundamentals` | **SCD2 MERGE** | New versions created when `attr_hash` changes (sector reclassification, name change). Unchanged symbols left alone. |
| `bronze.macro_indicators` | **Append** | Every run adds FRED observations. Duplicates across runs expected. |
| `bronze.factor_returns` | **Append** | Every run adds Fama-French daily factors. Duplicates expected. |

**Key point**: Bronze is your audit trail. Running the pipeline daily builds a growing historical record. Each row carries `_run_id` and `_ingest_ts` so you can trace exactly when data arrived.

### Silver — MERGE Preserves History (Accumulates)

| Table | Behavior | What Accumulates |
|-------|----------|------------------|
| `silver.daily_prices` | **MERGE on (symbol, date)** | New trading dates are inserted. Existing dates get updated values (e.g., if yfinance corrects a price). |
| `silver.daily_signals` | **MERGE on (symbol, as_of_date)** | Same pattern — new dates inserted, existing dates updated. |
| `silver.fundamentals_current` | **MERGE on (symbol)** | Only the current version of each ticker's fundamentals. Updated when Bronze SCD2 changes. |

After 252 trading days (1 year), `silver.daily_prices` will have ~5,040 rows for 20 tickers. After 3 years, ~15,120 rows. This is where your time-series depth lives.

### Gold — Most Tables Rebuilt Fresh (Does NOT Accumulate)

| Table | Behavior | Accumulates? |
|-------|----------|-------------|
| `gold.fact_market_price_daily` | **CREATE OR REPLACE TABLE** | No — fully rebuilt each run from Silver |
| `gold.fact_fundamental_snapshot` | **CREATE OR REPLACE TABLE** | No — rebuilt from Silver fundamentals_current |
| `gold.fact_signal_snapshot` | **CREATE OR REPLACE TABLE** | No — rebuilt from Silver signals |
| `gold.daily_analytics` | **CREATE OR REPLACE TABLE** | No — rebuilt from 3 Silver tables |
| `gold.watchlist_ranked` | **CREATE OR REPLACE TABLE** | No — only today's rankings |
| `gold.signal_alerts` | **CREATE OR REPLACE TABLE** | No — only today's alerts |
| `gold.signal_history` | **CREATE OR REPLACE TABLE** | Yes* — rebuilt from full Silver signals history |
| `gold.benchmark_compare` | **CREATE OR REPLACE TABLE** | No — only today's comparison |
| `gold.portfolio_candidates` | **CREATE OR REPLACE TABLE** | No — only today's candidates |
| `gold.dim_date` | **OVERWRITE** | No — deterministic rebuild |
| `gold.dim_security` | **SCD2 MERGE** | Yes — tracks sector/industry changes over time |
| `gold.trade_log` | **CREATE IF NOT EXISTS** | Manual — you INSERT rows yourself |

**Important**: Even though Gold fact tables are rebuilt each run, `signal_history` and `daily_analytics` include **all historical dates** from Silver because they `SELECT * FROM silver.daily_signals` (which has accumulated via MERGE). So when you query:

```sql
SELECT * FROM gold.signal_history WHERE symbol = 'AAPL' ORDER BY as_of_date DESC LIMIT 30
```

You get 30 days of history — because Silver has 30 days, and Gold rebuilds from all of Silver each run.

## Running Daily: What Actually Happens

After 1 month of daily runs (22 trading days), here's what each table contains:

| Table | Approximate Rows | Content |
|-------|------------------|---------|
| `bronze.daily_prices` | 440 (20 tickers × 22 runs) | Raw evidence with run metadata |
| `bronze.company_fundamentals` | 20-40 | SCD2 versions (1 per symbol, maybe some changed) |
| `bronze.macro_indicators` | ~110 | 5 series × 22 runs |
| `bronze.factor_returns` | ~120 | 6 factors × ~22 days |
| `silver.daily_prices` | 440 | Deduped — one row per (symbol, date) |
| `silver.daily_signals` | 440 | One row per (symbol, as_of_date) |
| `gold.daily_analytics` | 440 | Rebuilt from Silver, but contains all 22 days |
| `gold.signal_history` | 440 | Same — all 22 days |

After 1 year: ~5,040 rows per ticker-level table. After 3 years: ~15,120 rows. The data grows linearly with trading days.

## Why Gold Fact Tables Rebuild (Instead of Append)

The Gold layer is **serving tables**, not evidence. Rebuilding ensures:

1. **Consistency** — If a Silver correction is made (e.g., yfinance price correction), Gold picks it up automatically
2. **Simplicity** — NoMERGE logic needed, no duplicate handling
3. **Performance** — `CREATE OR REPLACE TABLE` is fast for the data volumes here (~5K-50K rows)

If you need historical Gold snapshots (e.g., "what did the composite score look like on March 15?"), the answer is: **re-run the pipeline for that date** using Bronze's point-in-time data, or query `silver.daily_signals` directly which preserves history via MERGE.

## Configuring the Schedule

Edit `databricks.yml`:

```yaml
schedule:
  quartz_cron_expression: "0 0 22 ? * MON-FRI"  # 10 PM ET weekdays
  timezone_id: America/New_York
```

Common alternatives:

| When | Cron Expression |
|------|----------------|
| Daily 10 PM ET | `0 0 22 ? * MON-FRI` |
| Daily 6 AM ET (before market) | `0 0 6 ? * MON-FRI` |
| Weekdays + Saturday | `0 0 22 ? * MON-SAT` |
| Every 4 hours | `0 0 4/4 ? * *` |

After editing, redeploy:

```bash
databricks bundle deploy
```