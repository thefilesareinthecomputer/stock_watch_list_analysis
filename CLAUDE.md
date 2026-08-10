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
uv sync                              # builds .venv from pyproject.toml + uv.lock
uv run pytest tests/ -v              # Spark tests need a JDK on JAVA_HOME
# JDK for local Spark tests (else they skip): brew install openjdk@17
# export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home

set -a; source .env; set +a          # load FRED_API_KEY, ALERT_EMAIL from .env
BUNDLE_VAR_fred_api_key="$FRED_API_KEY" \
BUNDLE_VAR_alert_email="$ALERT_EMAIL" \
databricks bundle deploy
databricks bundle run stock_analytics_pipeline
```
No private values live in `databricks.yml`; the FRED key and failure-notification email flow from
`.env` at deploy via `BUNDLE_VAR_fred_api_key` / `BUNDLE_VAR_alert_email`. Auth is OAuth (`databricks
auth login`), no PAT. Set `ALERT_EMAIL` in `.env` or failure emails go nowhere.
Bundles run on Terraform under the hood, but the CLI downloads its own (`databricks bundle debug
terraform` reports which). No local terraform install is needed; `DATABRICKS_TF_EXEC_PATH` and
`DATABRICKS_TF_VERSION` are for air-gapped use only.

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
else the example. `databricks.yml` force-includes `tickers.txt` via `sync.include`, so a **manual**
deploy from a machine that has the file ships the real list. The **CI deploy** materializes
`tickers.txt` from the `WATCHLIST` repo secret (comma- or newline-separated) before deploying;
keep that secret in sync with your local `tickers.txt`. If the secret is empty, the deploy falls
back to `tickers.example.txt`. Benchmarks `SPY`/`QQQ` are always added on top.
To edit the watchlist, change `WATCHLIST` in `.env`, then run `uv run python scripts/watchlist.py seed`
(and `... check` to confirm every symbol still resolves on yfinance - dead tickers contribute no rows
and never fail the job; retired ones are explained by `src/common/ticker_migrations.json`)
to materialize `tickers.txt` for a manual deploy. Mirror the change to the `WATCHLIST` repo secret.

## Code size — track after major changes to prevent bloat
```bash
find src   -name "*.py" -exec wc -l {} +   # ~3966 across 29 files
find tests -name "*.py" -exec wc -l {} +   # ~861 across 6 files
```
