# CLAUDE.md

Databricks medallion lakehouse for equity analytics. **Architecture, tables, query
patterns, indicator formulas, and design decisions live in [`README.md`](README.md)** —
read it before touching the pipeline. This file is operational guidance only.

## Working agreement
- User runs all installs and git commands. Assistant writes to requirements files and
  **stages** git commands for approval — never commits or pushes directly.
- Prefer architectural polish over more indicators / ML / tickers.
- Indicator correctness matters: match industry-standard formulas with citations.

## Build / test / deploy
```bash
python -m venv stocks-venv && source stocks-venv/bin/activate
pip install -r requirements.txt
pytest tests/ -v

set -a; source .env; set +a          # load FRED_API_KEY, ALERT_EMAIL from .env
BUNDLE_VAR_fred_api_key="$FRED_API_KEY" \
BUNDLE_VAR_alert_email="$ALERT_EMAIL" \
DATABRICKS_TF_EXEC_PATH=/opt/homebrew/bin/terraform \
DATABRICKS_TF_VERSION=1.14.8 \
databricks bundle deploy
databricks bundle run stock_analytics_pipeline
```
No private values live in `databricks.yml`; the FRED key and failure-notification email flow from
`.env` at deploy via `BUNDLE_VAR_fred_api_key` / `BUNDLE_VAR_alert_email`. Auth is OAuth (`databricks
auth login`), no PAT. Set `ALERT_EMAIL` in `.env` or failure emails go nowhere.

## Branch & deploy workflow
Solo, one Databricks environment. Work on `develop`; `main` is stable and the deploy trigger.
- Push/PR to `develop` or `main` runs CI (`.github/workflows/deploy.yml`): `test` then `bundle validate`.
- Push to `main` also runs `bundle deploy` (`default` target). Deploy is gated behind green `test`.
- Deploy updates the job definition only; the pipeline runs on its daily cron or `bundle run`.
- **Ship:** merge `develop` -> `main` and push. Never commit straight to `main`.
- CI auth/config live in GitHub repo secrets (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `FRED_API_KEY`,
  `ALERT_EMAIL`), fed to deploy as `BUNDLE_VAR_*`. Local dev stays keyless (OAuth); only CI holds a token.

## Databricks Free Edition gotchas (these break the build)
1. `__file__` undefined — `spark_python_task` runs via `exec()`. Use `try/except` + `os.getcwd()` fallback.
2. `spark.createDataFrame(pandas_df)` fails (PySpark Connect ChunkedArray bug). Build Row-based with string dates + SQL `CAST`.
3. DBFS disabled — no temp writes to `/tmp`. Row-based approach only.
4. `environment_version` must be `"3"` (not `"3.1"`). Valid: 1–5.
5. Serverless only, daily quota limits. No classic compute.
6. Declare deps in `environments.spec.dependencies` in `databricks.yml`.
7. yfinance ≥ 0.2.51 returns multi-level columns even for one ticker — `droplevel("Ticker")`.
8. No `Adj Close` — `auto_adjust=True` is the default; `close` IS the adjusted close.

## Query gotcha: star-schema columns are camelCase
`gold.daily_analytics` uses snake_case aliases; the fact/dim tables keep yfinance camelCase
(backtick-quote them in SQL): `trailingPE`, `dividendYield`, `marketCap`, `returnOnEquity`,
`shortName`. `dim_date` uses `date` (not `calendar_date`); `fact_market_price_daily` uses
`return_21d/63d/252d` (not 30d/90d). `dividend_yield_trap` is BOOLEAN in gold, DOUBLE in silver.

## Ticker config
Watchlist source of truth: `src/common/tickers.txt` (one ticker per line, `#` comments ok).
This file is **gitignored** (private — real strategy list). A fresh clone falls back to the
tracked `tickers.example.txt` starter. `config.py::_load_tickers()` prefers `tickers.txt`,
else the example. Because the real file is gitignored, `databricks.yml` force-includes it via
`sync.include` so the deploy ships the real list, not the example. Benchmarks `SPY`/`QQQ` are
always added on top. To edit the watchlist, change `tickers.txt` (and mirror on other devices).

## Code size — track after major changes to prevent bloat
```bash
find src   -name "*.py" -exec wc -l {} +   # ~3966 across 29 files
find tests -name "*.py" -exec wc -l {} +   # ~861 across 6 files
```
