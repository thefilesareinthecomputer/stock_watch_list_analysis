# SQL Exploration Queries

A collection of SQL queries for exploring the lakehouse data in Databricks SQL Editor. Start with `gold.daily_analytics` for zero-join exploration, then drill into star schema for cross-fact analysis.

---

## Quick Start: Zero-Join Exploration

### Today's Market Snapshot

```sql
-- Everything about every stock today (zero joins)
SELECT symbol, short_name, sector, close, volume,
       rsi, macd_histogram, bollinger_pct_b,
       change_30d_pct, trailing_pe, dividend_yield,
       composite_score
FROM stock_analytics.gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.daily_analytics)
ORDER BY composite_score DESC;
```

### Top Momentum Stocks

```sql
SELECT symbol, short_name, close, change_30d_pct, rsi, macd_histogram
FROM stock_analytics.gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.daily_analytics)
  AND change_30d_pct IS NOT NULL
ORDER BY change_30d_pct DESC
LIMIT 10;
```

### Oversold Stocks (Potential Bounce Candidates)

```sql
SELECT symbol, close, rsi, trailing_pe, return_on_equity, sector
FROM stock_analytics.gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.daily_analytics)
  AND rsi < 30
  AND trailing_pe < 25
  AND return_on_equity > 0.10
ORDER BY rsi ASC;
```

### Value Stocks with Dividend Safety

```sql
SELECT symbol, close, trailing_pe, forward_pe, dividend_yield, payout_ratio
FROM stock_analytics.gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.daily_analytics)
  AND trailing_pe BETWEEN 8 AND 18
  AND dividend_yield > 0.02
  AND dividend_yield_trap = false
  AND payout_ratio < 0.75
ORDER BY dividend_yield DESC;
```

### Sector Rotation Overview

```sql
SELECT sector,
       COUNT(*) AS stock_count,
       AVG(rsi) AS avg_rsi,
       AVG(change_30d_pct) AS avg_momentum,
       AVG(trailing_pe) AS avg_pe,
       AVG(market_cap) AS avg_market_cap
FROM stock_analytics.gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.daily_analytics)
  AND sector IS NOT NULL
GROUP BY sector
ORDER BY avg_momentum DESC;
```

### Volatility Squeeze (Bollinger BandWidth at 6-Month Low)

```sql
SELECT symbol, close, bollinger_bandwidth, bollinger_pct_b, sector
FROM stock_analytics.gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.daily_analytics)
  AND bollinger_bandwidth IS NOT NULL
ORDER BY bollinger_bandwidth ASC
LIMIT 15;
```

### MACD Crossovers Today

```sql
SELECT symbol, macd, macd_signal_line, macd_histogram
FROM stock_analytics.gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.daily_analytics)
  AND macd_histogram IS NOT NULL
  AND SIGN(macd_histogram) != SIGN(
      LAG(macd_histogram) OVER (PARTITION BY symbol ORDER BY as_of_date)
  );
```

### Risk-Return Scatter (for charting)

```sql
SELECT symbol, sector, close, beta, atr_14d, change_365d_pct, market_cap
FROM stock_analytics.gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.daily_analytics)
  AND beta IS NOT NULL
  AND change_365d_pct IS NOT NULL
ORDER BY change_365d_pct DESC;
```

---

## Signal Alerts

### All Active Alerts Today

```sql
SELECT alert_type, symbol, alert_value, as_of_date
FROM stock_analytics.gold.signal_alerts
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.signal_alerts)
ORDER BY alert_type, symbol;
```

### RSI Oversold Stocks

```sql
SELECT a.symbol, a.alert_value AS rsi_value, d.close, d.trailing_pe, d.sector
FROM stock_analytics.gold.signal_alerts a
JOIN stock_analytics.gold.daily_analytics d
  ON a.symbol = d.symbol AND a.as_of_date = d.as_of_date
WHERE a.alert_type = 'RSI_OVERSOLD'
ORDER BY a.alert_value;
```

### Dividend Yield Traps

```sql
SELECT symbol, alert_value AS yield_gap_pct
FROM stock_analytics.gold.signal_alerts
WHERE alert_type = 'DIVIDEND_YIELD_TRAP';
```

---

## Time Series Analysis

### 30-Day RSI Trend for a Stock

```sql
SELECT as_of_date, rsi, macd, macd_signal_line, bollinger_pct_b
FROM stock_analytics.gold.signal_history
WHERE symbol = 'AAPL'
ORDER BY as_of_date DESC
LIMIT 30;
```

### Price + Bollinger Band History

```sql
SELECT as_of_date, close, bollinger_upper, bollinger_lower, bollinger_pct_b
FROM stock_analytics.gold.daily_analytics
WHERE symbol = 'NVDA'
ORDER BY as_of_date DESC
LIMIT 60;
```

### Compare vs SPY/QQQ Benchmark

```sql
SELECT symbol, change_30d_pct, spy_change_30d, change_90d_pct, spy_change_90d,
       change_365d_pct, spy_change_365d
FROM stock_analytics.gold.benchmark_compare
ORDER BY change_365d_pct DESC;
```

### Sector Momentum Over Time (Weekly Average RSI)

```sql
SELECT
    DATE_TRUNC('WEEK', as_of_date) AS week,
    sector,
    AVG(rsi) AS avg_rsi,
    AVG(change_30d_pct) AS avg_momentum
FROM stock_analytics.gold.daily_analytics
WHERE sector IS NOT NULL
  AND as_of_date >= DATE_SUB(CURRENT_DATE(), 90)
GROUP BY DATE_TRUNC('WEEK', as_of_date), sector
ORDER BY week DESC, avg_rsi DESC;
```

---

## Star Schema Queries (Drill-Across)

These use the fact + dimension tables for cross-referencing different business processes.

### Momentum vs Valuation by Sector

```sql
SELECT s.sector, s.industry,
       AVG(sig.rsi) AS avg_rsi,
       AVG(f.trailing_pe) AS avg_pe,
       AVG(f.dividend_yield) AS avg_yield,
       COUNT(*) AS stock_count
FROM stock_analytics.gold.fact_signal_snapshot sig
JOIN stock_analytics.gold.dim_security s ON sig.security_key = s.security_key AND s.is_current = true
JOIN stock_analytics.gold.dim_date d ON sig.date_key = d.date_key
LEFT JOIN stock_analytics.gold.fact_fundamental_snapshot f
    ON sig.security_key = f.security_key AND sig.date_key = f.date_key
WHERE d.calendar_date = (SELECT MAX(calendar_date) FROM stock_analytics.gold.dim_date WHERE is_trading_day = TRUE)
GROUP BY s.sector, s.industry
ORDER BY avg_rsi DESC;
```

### Full Drill-Across: Prices + Fundamentals + Signals

```sql
SELECT
    d.calendar_date, s.symbol, s.sector,
    p.close, p.return_30d,
    f.trailing_pe, f.dividend_yield, f.market_cap,
    sig.rsi, sig.composite_score
FROM stock_analytics.gold.fact_signal_snapshot sig
JOIN stock_analytics.gold.dim_security s ON sig.security_key = s.security_key AND s.is_current = true
JOIN stock_analytics.gold.dim_date d ON sig.date_key = d.date_key
LEFT JOIN stock_analytics.gold.fact_market_price_daily p
    ON sig.security_key = p.security_key AND sig.date_key = p.date_key
LEFT JOIN stock_analytics.gold.fact_fundamental_snapshot f
    ON sig.security_key = f.security_key AND sig.date_key = f.date_key
WHERE d.calendar_date = (SELECT MAX(calendar_date) FROM stock_analytics.gold.dim_date WHERE is_trading_day = TRUE)
ORDER BY sig.composite_score DESC;
```

---

## Portfolio Candidates

### By Category

```sql
SELECT candidate_bucket, symbol, last_closing_price, rsi, pe_ratio, change_30d_pct
FROM stock_analytics.gold.portfolio_candidates
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.portfolio_candidates)
ORDER BY candidate_bucket, symbol;
```

### Top Momentum Stocks

```sql
SELECT symbol, change_30d_pct, rsi, last_closing_price
FROM stock_analytics.gold.portfolio_candidates
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.portfolio_candidates)
  AND candidate_bucket = 'top_momentum'
ORDER BY change_30d_pct DESC;
```

### Oversold Bounce Candidates

```sql
SELECT symbol, rsi, last_closing_price
FROM stock_analytics.gold.portfolio_candidates
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.portfolio_candidates)
  AND candidate_bucket = 'oversold'
ORDER BY rsi ASC;
```

---

## Data Quality Checks

### Bronze Prices: Run Metadata

```sql
SELECT _run_id, COUNT(*) AS rows, MIN(_ingest_ts) AS earliest, MAX(_ingest_ts) AS latest
FROM stock_analytics.bronze.daily_prices
GROUP BY _run_id
ORDER BY earliest DESC;
```

### Silver: Verify Dedup

```sql
SELECT symbol, date, COUNT(*) AS cnt
FROM stock_analytics.silver.daily_prices
GROUP BY symbol, date
HAVING cnt > 1;
-- Should return 0 rows
```

### Gold: Check Latest Date

```sql
SELECT MAX(as_of_date) AS latest_date, COUNT(*) AS row_count
FROM stock_analytics.gold.daily_analytics;
```

### Gold: Coverage by Sector

```sql
SELECT sector, COUNT(*) AS stocks,
       AVG(trailing_pe) AS avg_pe,
       AVG(dividend_yield) AS avg_yield,
       AVG(rsi) AS avg_rsi
FROM stock_analytics.gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.daily_analytics)
  AND sector IS NOT NULL
GROUP BY sector
ORDER BY stocks DESC;
```