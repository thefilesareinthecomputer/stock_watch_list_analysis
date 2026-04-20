# Databricks Visualization Tutorials

Step-by-step guides for building dashboards and visualizations in Databricks SQL/LLM for stock analysis and recommendation.

---

## Before You Start

All queries reference the `stock_analytics` catalog. If your default catalog is different, prefix table names:

```sql
USE CATALOG stock_analytics;
-- or prefix each table: stock_analytics.gold.daily_analytics
```

The best starting table for most visualizations is **`gold.daily_analytics`** — it's a wide denormalized table with zero joins needed.

---

## Tutorial 1: Watchlist Dashboard

The primary dashboard. Shows your entire watchlist ranked by composite score with sector coloring.

### Step 1: Create the Query

```sql
SELECT
    symbol,
    short_name,
    sector,
    close,
    change_30d_pct,
    rsi,
    trailing_pe,
    dividend_yield,
    composite_score
FROM stock_analytics.gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.daily_analytics)
ORDER BY composite_score DESC;
```

### Step 2: Create a Dashboard

1. In Databricks, go to **Dashboards** → **Create Dashboard**
2. Name it "Stock Watchlist"
3. Add a **Data Widget** using the query above

### Step 3: Configure Visualization

- **Visualization type**: Table
- **Conditional formatting**:
  - `rsi`: Red below 30 (oversold), green above 70 (overbought)
  - `change_30d_pct`: Red below 0, green above 0
  - `composite_score`: Green gradient (higher = better)
- **Sort**: By `composite_score DESC`
- **Columns to show**: All columns from the SELECT

### Step 4: Add Sector Filter

Add a dropdown filter on `sector` so you can focus on Technology, Healthcare, etc.

---

## Tutorial 2: Momentum Heatmap

Visualize which sectors and stocks have the most momentum.

### Query

```sql
SELECT sector, symbol, change_30d_pct, rsi, market_cap
FROM stock_analytics.gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.daily_analytics)
  AND sector IS NOT NULL
ORDER BY sector, change_30d_pct DESC;
```

### Visualization

- **Type**: Pivot table or heatmap
- **Rows**: `sector`
- **Columns**: `symbol` (or top 5 per sector)
- **Values**: `change_30d_pct`
- **Color scale**: Red (-20%) → Yellow (0%) → Green (+20%)

---

## Tutorial 3: RSI Overbought/Oversold Tracker

Shows stocks with extreme RSI readings — potential reversal signals.

### Query

```sql
SELECT symbol, close, rsi, change_30d_pct, sector,
       CASE
           WHEN rsi < 30 THEN 'OVERSOLD'
           WHEN rsi > 70 THEN 'OVERBOUGHT'
           ELSE 'NEUTRAL'
       END AS rsi_zone
FROM stock_analytics.gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.daily_analytics)
  AND (rsi < 30 OR rsi > 70)
ORDER BY rsi;
```

### Visualization

- **Type**: Scatter plot
- **X-axis**: `rsi`
- **Y-axis**: `change_30d_pct`
- **Color**: `rsi_zone` (red for oversold, green for overbought)
- **Size**: `market_cap` (larger = bigger company)
- **Tooltip**: `symbol`, `short_name`, `sector`

### Interpretation

- **RSI < 30 (oversold)**: Stock may be due for a bounce. Look for confirmation from fundamentals (low PE, positive earnings growth).
- **RSI > 70 (overbought)**: Stock may be overextended. Look for divergences (price up but RSI falling).

---

## Tutorial 4: Signal Alerts Dashboard

Shows today's actionable signals: crossovers, squeezes, yield traps.

### Query

```sql
SELECT alert_type, symbol, alert_value, as_of_date
FROM stock_analytics.gold.signal_alerts
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.signal_alerts)
ORDER BY alert_type, symbol;
```

### Visualization

- **Type**: Table with conditional formatting
- **Group by**: `alert_type`
- **Color coding**:
  - `RSI_OVERSOLD`: Red background
  - `RSI_OVERBOUGHT`: Dark green background
  - `MACD_CROSSOVER`: Yellow background (bullish crossover) or gray (bearish)
  - `BOLLINGER_SQUEEZE`: Orange background (volatility compression)
  - `MA200_CROSS_ABOVE`: Green background (bullish breakout)
  - `MA200_CROSS_BELOW`: Red background (bearish breakdown)
  - `DIVIDEND_YIELD_TRAP`: Red background (yield likely to be cut)

### How to Act on Each Alert

| Alert | What It Means | Action |
|-------|--------------|--------|
| RSI_OVERSOLD | Stock sold off heavily | Check fundamentals. If PE < 20, ROE > 15%, consider entry |
| RSI_OVERBOUGHT | Stock overextended | Consider taking profits or tightening stops |
| MACD_CROSSOVER | Momentum shifting | If MACD crosses above signal → bullish. Below → bearish |
| BOLLINGER_SQUEEZE | Volatility compression | Breakout imminent. Direction unknown — wait for confirmation |
| MA200_CROSS_ABOVE | Price crossed above 200-day MA | Long-term bullish signal |
| MA200_CROSS_BELOW | Price fell below 200-day MA | Long-term bearish signal |
| DIVIDEND_YIELD_TRAP | TTM yield > forward yield by >1.5pp | Dividend likely to be cut. Avoid or short |

---

## Tutorial 5: Sector Rotation Tracker

Shows which sectors are gaining/losing momentum over time.

### Query

```sql
SELECT
    DATE_TRUNC('WEEK', as_of_date) AS week,
    sector,
    AVG(rsi) AS avg_rsi,
    AVG(change_30d_pct) AS avg_momentum,
    AVG(market_cap) AS avg_market_cap,
    COUNT(*) AS stock_count
FROM stock_analytics.gold.daily_analytics
WHERE sector IS NOT NULL
  AND as_of_date >= DATE_SUB(CURRENT_DATE(), 90)
GROUP BY DATE_TRUNC('WEEK', as_of_date), sector
ORDER BY week DESC, avg_momentum DESC;
```

### Visualization

- **Type**: Line chart
- **X-axis**: `week`
- **Y-axis**: `avg_momentum`
- **Series**: One line per `sector`
- **Highlight**: Which sectors are trending up vs down over the past 3 months

---

## Tutorial 6: Value + Dividend Stock Screener

Find stocks with strong fundamentals and safe dividends.

### Query

```sql
SELECT symbol, short_name, sector, close,
       trailing_pe, forward_pe,
       dividend_yield, payout_ratio,
       return_on_equity, debt_to_equity,
       free_cashflow
FROM stock_analytics.gold.daily_analytics
WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.daily_analytics)
  AND trailing_pe BETWEEN 8 AND 20
  AND dividend_yield > 0.02
  AND dividend_yield_trap = false
  AND payout_ratio < 0.70
  AND return_on_equity > 0.12
  AND debt_to_equity < 2.0
ORDER BY dividend_yield DESC;
```

### Interpretation Filters

| Filter | Why |
|--------|-----|
| PE 8-20 | Not too cheap (value trap) or expensive (growth premium) |
| Yield > 2% | Meaningful dividend |
| Yield trap = false | Not a false yield (TTM > FWD by >1.5pp) |
| Payout < 70% | Dividend is sustainable |
| ROE > 12% | Company generates good returns |
| D/E < 2.0 | Not overleveraged |

---

## Tutorial 7: Benchmark Comparison Chart

Compare your stocks against SPY and QQQ.

### Query

```sql
SELECT symbol, change_30d_pct, spy_change_30d, change_90d_pct, spy_change_90d,
       change_365d_pct, spy_change_365d
FROM stock_analytics.gold.benchmark_compare
ORDER BY change_365d_pct DESC;
```

### Visualization

- **Type**: Bar chart
- **Bars**: `change_30d_pct`, `spy_change_30d` (grouped per symbol)
- **Sort**: By `change_365d_pct DESC`
- **Add reference line** at y=0 (break-even)
- **Interpretation**: Stocks above the SPY line are outperforming the market.

---

## Tutorial 8: Price + MACD Dual-Axis Chart

Overlay MACD on a price chart for timing signals.

### Query

```sql
SELECT as_of_date, last_closing_price, macd, macd_signal_line, macd_histogram
FROM stock_analytics.gold.signal_history
WHERE symbol = 'AAPL'
ORDER BY as_of_date DESC
LIMIT 90;
```

### Visualization

- **Type**: Combo chart (dual-axis)
- **Left Y-axis**: `last_closing_price` (line)
- **Right Y-axis**: `macd` (line), `macd_signal_line` (line), `macd_histogram` (bar)
- **X-axis**: `as_of_date`
- **Key signals**:
  - MACD line crosses above signal line → bullish
  - MACD line crosses below signal line → bearish
  - Histogram growing → momentum strengthening

---

## Tutorial 9: Fundamentals Radar Chart

Compare a single stock's fundamentals against sector averages.

### Query

```sql
-- Target stock
SELECT symbol, sector,
       trailing_pe, dividend_yield, profit_margins, return_on_equity, revenue_growth
FROM stock_analytics.gold.daily_analytics
WHERE symbol = 'AAPL'
  AND as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.daily_analytics);

-- Sector average for comparison
SELECT sector,
       AVG(trailing_pe) AS avg_pe,
       AVG(dividend_yield) AS avg_yield,
       AVG(profit_margins) AS avg_margins,
       AVG(return_on_equity) AS avg_roe,
       AVG(revenue_growth) AS avg_growth
FROM stock_analytics.gold.daily_analytics
WHERE sector = 'Technology'
  AND as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.daily_analytics)
GROUP BY sector;
```

### Visualization

- **Type**: Radar/spider chart (if available in your Databricks visualization)
- **Axes**: PE (inverted — lower is better), Yield, Margins, ROE, Growth
- **Two series**: Stock values vs sector averages
- **Interpretation**: Larger area = stronger fundamentals relative to peers

---

## Tutorial 10: Data Freshness Monitor

Quick health check: is the pipeline running and is data current?

### Query

```sql
SELECT
    'prices' AS source,
    MAX(date) AS latest_date,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT symbol) AS ticker_count
FROM stock_analytics.silver.daily_prices

UNION ALL

SELECT
    'signals' AS source,
    MAX(as_of_date) AS latest_date,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT symbol) AS ticker_count
FROM stock_analytics.silver.daily_signals

UNION ALL

SELECT
    'fundamentals' AS source,
    MAX(as_of_date) AS latest_date,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT symbol) AS ticker_count
FROM stock_analytics.gold.daily_analytics;
```

### Visualization

- **Type**: Table
- **Conditional formatting**: `latest_date` — red if > 2 days old, yellow if 1 day old, green if today
- **Purpose**: At a glance, see if the pipeline ran successfully and data is fresh

---

## Making Stock Recommendations: A Decision Framework

These visualizations support a systematic approach to stock recommendations:

### Step 1: Screen (Quantitative Filters)

Use **Tutorial 6** (Value + Dividend Screener) or **Tutorial 3** (RSI Tracker) to narrow from 20+ tickers to 5-8 candidates.

### Step 2: Confirm (Technical Signals)

Use **Tutorial 4** (Signal Alerts) and **Tutorial 8** (MACD Chart) to confirm timing.

### Step 3: Compare (Relative Performance)

Use **Tutorial 7** (Benchmark Comparison) to see if candidates are beating SPY/QQQ.

### Step 4: Context (Sector Rotation)

Use **Tutorial 5** (Sector Rotation) to understand macro trends. Is the sector strengthening or weakening?

### Step 5: Decide (Risk Assessment)

Use **Tutorial 9** (Fundamentals Radar) to confirm financial health. Check `debt_to_equity`, `payout_ratio`, and `free_cashflow`.

### Decision Matrix

| Signal | Momentum | Value | Quality | Action |
|--------|----------|-------|---------|--------|
| Strong | RSI > 60, MACD > 0 | PE < 15, Yield > 3% | ROE > 15%, D/E < 1 | **Strong Buy** |
| Moderate | RSI 40-60 | PE 15-25 | ROE 10-15% | **Hold / Watch** |
| Weak | RSI < 30 | PE > 25 | D/E > 2 | **Avoid / Short** |