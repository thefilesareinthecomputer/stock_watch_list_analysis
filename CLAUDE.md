# CLAUDE.md

Databricks medallion lakehouse for equity analytics. **Architecture, tables, query
patterns, deploy steps, and design decisions live in [`README.md`](README.md)**;
first principles and operational constraints (incl. the Free Edition build
breakers) in [`SPEC.md`](SPEC.md). This file is agent guidance only.

## Working agreement
- User runs all installs and git commands. Assistant writes to requirements files
  and **stages** git commands for approval - never commits or pushes directly.
- Prefer architectural polish over more indicators / ML / tickers.
- Indicator correctness matters: match industry-standard formulas with citations.

## Build / test / deploy
```bash
uv sync
uv run pytest tests/ -v              # Spark tests need JDK 17 on JAVA_HOME, else they skip
set -a; source .env; set +a          # FRED_API_KEY, ALERT_EMAIL
BUNDLE_VAR_fred_api_key="$FRED_API_KEY" BUNDLE_VAR_alert_email="$ALERT_EMAIL" \
databricks bundle deploy             # auth: OAuth via `databricks auth login`, no PAT
```
No private values in `databricks.yml`; secrets flow from `.env` at deploy. CI
secrets and the `WATCHLIST` repo secret: README "Quick Start" step 3.

## Local-first development (DuckDB)
Databricks is the production target; iteration happens locally (~2 min against
~18 min plus quota per run there).
```bash
uv run python scripts/backfill.py               # yfinance -> warehouse/market.duckdb
uv run python scripts/backfill_fundamentals.py  # SEC EDGAR -> bronze_fundamentals
uv run python scripts/build_local.py            # silver + gold + forward returns
uv run python scripts/evaluate.py --candidates  # walk-forward verdict on candidates
uv run python scripts/ic_decay.py               # IC by 1-12 month horizon
uv run python scripts/validate_calls.py         # call state machine replay
uv run python scripts/rebalance.py              # settle -> report -> emit call round
uv run python scripts/backfill_universe.py      # broad universe + line of sight (monthly)
uv run pytest tests/test_engine_parity.py -q    # same SQL, both engines
```
EDGAR needs a contact User-Agent: `EDGAR_USER_AGENT` in `.env`, falling back to
`ALERT_EMAIL`; neither set -> the backfill refuses to run.
Indicators are shared code (`common.indicators.build_signal_series`, pure pandas),
so parity there is by construction. SQL that must run on both engines goes through
`tests/test_engine_parity.py` first and stays in the common dialect subset.

## Private, gitignored, does not travel between devices
| Path | What | Regenerable? |
|---|---|---|
| `.env` | Secrets, `WATCHLIST` | No |
| `src/common/tickers.txt` | The real watchlist | Yes - `watchlist.py seed` |
| `warehouse/` | Local DuckDB | Yes - `backfill.py`, ~2 min |
| `calls_log.jsonl` | Buy/sell call record (append-only evidence) | **No** |
| `knowledge/` | Research and KB, incl. `POSITIONS.md` (held positions) | **No** |
| `_relay.md` | Handoff scratch file, bidirectional | No |

Never echo the contents of these into commit messages, tracked files, or anything
reaching the public repo.

## Branch & deploy workflow
Solo, one Databricks environment. Work on `develop`; `main` is stable and the
deploy trigger (CI: test -> validate, push to `main` also deploys). **Ship:**
merge `develop` -> `main` and push. Never commit straight to `main`.

## Ticker config
`src/common/tickers.txt` (gitignored) is the watchlist source of truth; a fresh
clone falls back to `tickers.example.txt`. To edit: change `WATCHLIST` in `.env`,
run `uv run python scripts/watchlist.py seed` (and `... check` - dead tickers fail
silently in the pipeline), mirror the change to the `WATCHLIST` repo secret.
Gotcha: the dotenv `WATCHLIST` shadows `tickers.txt` locally - `_load_tickers()`
reads the env var first.

## Code size - track after major changes to prevent bloat
```bash
find src   -name "*.py" -exec wc -l {} +   # ~6599 across 51 files
find tests -name "*.py" -exec wc -l {} +   # ~5203 across 33 files
```
