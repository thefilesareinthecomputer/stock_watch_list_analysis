# Stock Analytics Lakehouse

Production-grade Databricks lakehouse for equity screening and investment
decision support — Delta Lake, Unity Catalog, and Declarative Automation Bundles.

<p align="center">
  <img src="static-assets/stock-dashboard.png" alt="Stock Dashboard" width="700">
</p>

Live Databricks dashboard showing composite-ranked watchlist with RSI, MACD,
Bollinger Bands, and fundamental signals across 20 tickers.

<p align="center">
  <img src="static-assets/databricks-sql.png" alt="Databricks SQL Editor" width="700">
</p>

Query the Gold layer directly from the Databricks SQL Editor.

<p align="center">
  <img src="static-assets/deployment.png" alt="Databricks Deployment" width="700">
</p>

Pipeline deployed and running end-to-end on Databricks Free Edition.

---

## Architecture

**Medallion architecture**: Bronze (raw) → Silver (features) → Gold (analytics)

- **No notebooks** — all Python scripts deployed via Databricks Asset Bundles (DABs)
- **Serverless compute** on Databricks Free Edition
- **Unity Catalog** for governance and table access control
- **CI/CD** via GitHub Actions (test → validate → deploy)

```
setup_catalog
    ├── ingest_prices ──────┐
    └── ingest_fundamentals ┤
                    build_silver
                        └── build_gold
```

## Databricks Stack

| Component | Detail |
|-----------|--------|
| **Platform** | Databricks Free Edition |
| **Compute** | Serverless (environment v3) |
| **Catalog** | Unity Catalog (`stock_analytics`) |
| **Deployment** | DABs (`databricks bundle deploy`) |
| **Orchestration** | Databricks Jobs (Mon-Fri 10pm ET) |
| **Storage** | Delta Lake tables |
| **Dashboard** | Built-in Databricks SQL dashboard |

## Tables

| Layer | Table | Write Mode | Description |
|-------|-------|------------|-------------|
| Bronze | `stock_analytics.bronze.daily_prices` | overwrite | OHLCV price history (adjusted) |
| Bronze | `stock_analytics.bronze.company_fundamentals` | overwrite | Curated fundamentals (typed) |
| Silver | `stock_analytics.silver.daily_signals` | idempotent append | Technical indicators + fundamentals |
| Gold | `stock_analytics.gold.watchlist_ranked` | replace | Ranked watchlist with composite scores |
| Gold | `stock_analytics.gold.signal_history` | replace | Signal time series for dashboards |
| Gold | `stock_analytics.gold.trade_log` | manual | Trade journal (INSERT via SQL editor) |

## Quick Start

### Prerequisites

- Databricks CLI (`brew install databricks`)
- Terraform (`brew install terraform`)
- Python 3.11+

### 1. Authenticate

```bash
databricks auth login --host https://YOUR_WORKSPACE.cloud.databricks.com
```

### 2. Configure

Edit `databricks.yml` — set your workspace host.

Edit `src/common/config.py` — set your ticker watchlist.

### 3. Deploy & Run

```bash
# Deploy the bundle
DATABRICKS_TF_EXEC_PATH=$(which terraform) \
DATABRICKS_TF_VERSION=$(terraform version -json | jq -r '.terraform_version') \
databricks bundle deploy

# Run the pipeline
databricks bundle run stock_analytics_pipeline
```

### 4. Query

Open the Databricks SQL Editor and run:

```sql
-- Top 10 stocks by composite rank
SELECT symbol, trade_signal, rsi, pe_ratio, change_30d_pct,
       composite_rank, last_closing_price
FROM stock_analytics.gold.watchlist_ranked
ORDER BY composite_rank ASC
LIMIT 10;

-- Current Buy signals
SELECT symbol, rsi, macd, last_closing_price
FROM stock_analytics.gold.watchlist_ranked
WHERE trade_signal = 'Buy';

-- Signal history for a ticker
SELECT as_of_date, trade_signal, rsi, macd, last_closing_price
FROM stock_analytics.gold.signal_history
WHERE symbol = 'AAPL'
ORDER BY as_of_date DESC;
```

## Local Development

```bash
python -m venv stocks-venv && source stocks-venv/bin/activate
pip install -r requirements.txt
pytest tests/ -v
```

## Trade Logging

```sql
INSERT INTO stock_analytics.gold.trade_log VALUES (
    '2026-04-21', 'AAPL', 'BUY', 10, 195.50, 1955.00,
    'RSI oversold, composite rank #3', 'Buy', 28.4, 24.1, NULL
);
```

Track P&L against current prices:

```sql
SELECT t.symbol, t.price AS entry, w.last_closing_price AS current,
       ROUND(((w.last_closing_price - t.price) / t.price) * 100, 2) AS return_pct
FROM stock_analytics.gold.trade_log t
JOIN stock_analytics.gold.watchlist_ranked w ON t.symbol = w.symbol
WHERE t.action = 'BUY';
```

## Schedule

Pipeline runs Mon-Fri at 10pm ET (after market close).

## Indicators

| Indicator | Column | Description |
|-----------|--------|-------------|
| RSI (14-day) | `rsi` | Relative Strength Index |
| MACD | `macd`, `macd_signal_line` | Moving Average Convergence/Divergence |
| Bollinger Bands | `bollinger_upper`, `bollinger_lower` | 20-day ± 2σ |
| ATR (14-day) | `atr_14d` | Average True Range |
| Moving Averages | `ma_50`, `ma_200` | 50-day and 200-day SMA |
| Price Change | `change_30d_pct`, `change_90d_pct`, `change_365d_pct` | Momentum |
| Fundamentals | `eps`, `pe_ratio`, `dividend_yield` | From yfinance |
| Composite Rank | `composite_rank` | RSI + value + momentum rank sum |