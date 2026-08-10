# Completed - 2026-08-09

## 2026-08-09 - watchlist privatization + cross-device repo tidy-up

Branch: `feature/upgrade-stock-pipeline`, later folded into `develop`.

- **Docs de-staled.** The watchlist source of truth had moved from the dotenv
  `USER_STOCK_WATCH_LIST` to `src/common/tickers.txt`, but CLAUDE.md, README and
  `.env.example` still claimed the dotenv. Corrected.
- **Watchlist privatized.** This is a public repo, so the list was split to keep
  logic public and the strategy list private:
  - `src/common/tickers.example.txt` - tracked public starter (frozen copy of the
    324-ticker dev list).
  - `src/common/tickers.txt` - the private real list, gitignored (`.gitignore:25`).
  - `config.py::_load_tickers()` prefers `tickers.txt`, falls back to the example.
  - `databricks.yml` gained `sync.include: [src/common/tickers.txt]` so a manual
    deploy still ships the real list despite the gitignore.
  - `databricks.yml` became tracked once the FRED key was removed.
- **FRED key removed from `databricks.yml`.** It flows from the dotenv at deploy
  via the `fred_api_key` bundle variable / `BUNDLE_VAR_fred_api_key`. Verified the
  old key never leaked: `databricks.yml` was never committed and the key string
  appears in zero commits across all branches. No rotation needed.
- **CI/CD wired.** GitHub repo secrets `DATABRICKS_HOST`, `DATABRICKS_TOKEN`,
  `FRED_API_KEY`, `ALERT_EMAIL`, `WATCHLIST`. Push to `main` runs
  test -> validate -> deploy; push/PR to `develop` runs test -> validate only.

## 2026-08-09 - machine setup, dependency pinning, CI action bump

Shipped to `develop` and `main` as `82e1228`, `5ba25a4`, `52489de`.

### Local machine configured

Pulled the 19-commit overhaul (medallion `src/transforms/`, `tests/`, uv
migration). Installed Databricks CLI v1.11.0, authenticated by OAuth (keyring,
no PAT), built `.venv` from `uv.lock`, seeded the private watchlist. Removed an
orphaned pre-uv Python 3.11.4 virtualenv from the repo root.

### `scripts/watchlist.py` added (seed + check)

Started as `scripts/seed_tickers.py` and was merged the same day with a validator
into one script with `seed` and `check` subcommands, so the seeded list and the
validated list share one parser and cannot drift.

`check` verifies every symbol still resolves on yfinance, which nothing else
surfaces: a bad ticker contributes no rows and the job stays green. It costs
~1.5s per symbol and threading does not help - measured, 25 symbols took 36s
sequential against 40s threaded, so the bottleneck is Yahoo-side rate limiting,
not the loop. Results are therefore cached for 30 days per symbol, since only
typos and delistings make a symbol go bad and neither decays faster than that.
`--full` forces a complete recheck.

#### seed behaviour

Materializes `src/common/tickers.txt` from the `WATCHLIST` value in the dotenv.
Pipeline code reads `WATCHLIST` from the environment directly, but
`databricks.yml` force-syncs the file into the bundle, so a manual deploy ships
whatever that file holds. The script loads the dotenv in-process and imports
`_parse_tickers` from `config.py`, so there is one parser and no drift.

### Job dependencies pinned - and the near-miss that shaped the pins

`databricks.yml` had unbounded ranges (`numpy>=1.24`, `pandas>=2.0`), so the job
installed whatever was newest at run time while local was exact-pinned.

The first attempt pinned `numpy==2.4.4` to match the lockfile. **That would have
broken the job.** Serverless `environment_version: "3"` ships
`pyarrow==15.0.2`, which declares `numpy>=1.16.6,<2`, and nothing in the
dependency list upgrades pyarrow. Arrow is what Spark uses for every pandas
conversion, so the break would have surfaced at the next cron fire, not at deploy.

The old unbounded range had been accidentally protecting against this:
`numpy>=1.24` is already satisfied by the base image's 1.26.4, so pip never
touched numpy. Pinning removed that protection.

Verification method, offline and quota-free: downloaded the real env-3 manifest
(`requirements-env-3.txt`, linked from the environment version 3 release notes),
and resolved candidate pin sets against it. A union solve is useless here because
the manifest itself pins `pandas==1.5.3`; what matters is the pip-upgrade-on-top
sequence and each package's declared constraints. The full base image cannot be
rebuilt locally - pandas 1.5.3 has no cp312 wheel and will not compile.

Resolved pins, all matching the env-3 base where the base constrains them:
`yfinance==1.3.0`, `numpy==1.26.4`, `pandas==3.0.2`, `python-dotenv==1.2.2`,
`exchange_calendars==4.13.2`, `fredapi==0.5.2`, `requests==2.32.2`.

`requests` was previously undeclared and borrowed from the base image despite
`ingest_congressional.py:53` importing it. Pinned at the base version, so it
declares the dependency while installing nothing new.

`pyproject.toml` was aligned to the same numpy and requests versions so tests
exercise what the job runs, and `requires-python` was capped to `>=3.12,<3.13`.
The open-ended `>=3.12` let uv resolve against 3.14, where pandas 3.0.2 demands
`numpy>=2.3.3` - the lock refused to build until the window was closed.

**Verified end to end:** 186 tests pass, `bundle validate` OK, and a full manual
`bundle run` completed with all 19 tasks SUCCESS, including `ingest_prices` over
the whole watchlist and the full silver/gold chain.

### Terraform prerequisite removed from the docs

`databricks bundle debug terraform` reports that bundles do run on Terraform, but
the CLI downloads its own (TF 1.5.5 + Databricks provider 1.124.0), and
`DATABRICKS_TF_EXEC_PATH` / `DATABRICKS_TF_VERSION` are documented for air-gapped
use only. CLAUDE.md had pinned `DATABRICKS_TF_VERSION=1.14.8` against a Homebrew
terraform - a pairing that was never valid, since the CLI wants 1.5.5. Both
variables were dropped from the documented deploy command and a deploy confirmed
it runs clean without them. No local terraform install is needed.

### CI actions moved to Node 24

GitHub deprecated Node 20 and was force-running both actions on Node 24.
`actions/checkout` v4 -> v7, `astral-sh/setup-uv` v6 -> v9.0.0.

Took two commits: the first used `astral-sh/setup-uv@v9`, which does not exist -
setup-uv publishes floating major tags only through `v7`, with v8 and v9 as exact
tags only. CI failed at action resolution; `validate` and `deploy` are gated
behind `test`, so both skipped and nothing reached Databricks. Fixed by pinning
the exact `v9.0.0` tag.

### Public example watchlist cut to 10 tickers

`src/common/tickers.example.txt` went from the frozen 324-ticker dev list to ten
widely held large caps: AAPL, MSFT, NVDA, AMZN, GOOGL, META, TSLA, BRK-B, JPM, V.
Benchmarks SPY/QQQ are appended by `config.BENCHMARK_TICKERS`, so they are not in
the file.

Validated: the file parses to exactly 10 unique uppercase symbols; a fresh-clone
simulation (copied tree, no `tickers.txt`, run from a directory with no dotenv)
loads all 10 through the fallback; all 10 resolve on yfinance with price data.

Two things this surfaced. `BRK-B` is the correct yfinance spelling - the old
324-ticker list carried `BRKB`, which returns no data at all. And `load_dotenv()`
in `config.py` re-reads `WATCHLIST` from the dotenv even when the variable is
unset in the parent environment, so the fallback cannot be exercised from the
repo root at all; testing it requires a working directory with no dotenv above it.

### Watchlist swept: 8 dead symbols, all corporate actions

The first full `check` found 8 of 324 symbols returning no data, reproducibly.
None were typos - every one was a corporate action, and three were duplicates the
list had been carrying under both the old and new name:

| Dead | Became | Note |
|---|---|---|
| `CHK` | `EXE` | Merged with Southwestern Oct 2024, renamed Expand Energy. `EXE` already held |
| `MNMD` | `DFTX` | Rebranded Definium Therapeutics Jan 2026. `DFTX` already held |
| `CYBN` | `HELP` | Rebranded Helus Pharma, NYSE American -> Nasdaq Jan 2026. `HELP` already held |
| `CIVI` | `SM` | Merged into SM Energy Jan 2026, 1.45 SM per CIVI. Added |
| `SQ` | `XYZ` | Block ticker change Jan 2025. Added |
| `ITCI` | none | Acquired by J&J for cash, delisted Apr 2025 |
| `DM` | none | Acquired by Nano Dimension for cash, delisted Apr 2025 |
| `TGTG` | none | Never a valid US listing; Too Good To Go is private |

Result: 324 -> 318, verified all resolving. The dotenv, the `WATCHLIST` repo
secret and `tickers.txt` were all updated from one generated file, so the three
cannot disagree by transcription.

`src/common/ticker_migrations.json` records each retirement with date, reason and
source. `check` reads it, so a dead symbol explains itself instead of needing the
research again, and anything not in the file prints as `UNMAPPED` rather than
being silently tolerated. It also emits a paste-ready corrected `WATCHLIST`.

### Open risks closed by inspection

- **`sync.include` overriding `.gitignore`** - confirmed. `tickers.txt` is present
  in the deployed workspace bundle at
  `.bundle/stock-analytics/default/files/src/common/`.
- **`WATCHLIST` repo secret** - confirmed present (added 2026-08-09).
- **The plaintext FRED key security note** - already resolved earlier the same
  day; the note in `plan.md` was stale and described a file state that no longer
  existed.
