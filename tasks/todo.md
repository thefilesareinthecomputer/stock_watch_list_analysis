# TODO

## On this (desktop) device - DONE
- [x] Edit `.env.example`: added `FRED_API_KEY` (done by user).
- [x] Untracked the private watchlist (`git rm --cached src/common/tickers.txt`), committed and
      pushed as `6e325a2` on `feature/upgrade-stock-pipeline`. Databricks CLI OAuth profile set up.

## Databricks CLI auth - OAuth, per device (no PAT)
- [ ] Run on THIS machine and the laptop (opens a browser, writes ~/.databrickscfg):
      `databricks auth login --host https://dbc-fc3cf8bd-4b59.cloud.databricks.com`
- [ ] Profile is device-local (never in git). Both machines use the same tracked databricks.yml.
- [ ] After login here, assistant runs `databricks bundle validate` to confirm sync.include + FRED var.

## On the laptop (next session) - full setup is in README "Quick Start"
- [ ] Pull `develop`.
- [ ] Install tools: `brew install uv databricks terraform openjdk@17`.
- [ ] `uv sync` (builds .venv; installs pinned Python 3.12 itself - no system Python needed).
- [ ] Recreate the private watchlist: `cp src/common/tickers.example.txt src/common/tickers.txt`
      then adjust. It is gitignored, so it does NOT arrive via git - mirror changes manually.
- [ ] `cp .env.example .env`, set `FRED_API_KEY` and `ALERT_EMAIL`.
- [ ] `databricks auth login --host <workspace>` (OAuth, per device; databricks.yml arrives via git).
- [ ] Run tests: `export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`
      then `uv run pytest tests/ -v` (without JAVA_HOME the Spark tests skip).

## CI/CD model (GitHub Actions, .github/workflows/deploy.yml)
- Branch model: work on `develop`, `main` is stable + the deploy trigger. One Databricks env.
- Triggers: push/PR to `develop` or `main` runs test + validate; push to `main` ALSO deploys
  (`databricks bundle deploy`, `default` target). Deploy updates the job def; the pipeline runs
  on its daily cron (22:00 MON-FRI) or a manual `databricks bundle run`.
- CI auth + config are GitHub REPO SECRETS (not vars, not env-secrets - jobs declare no environment):
  `DATABRICKS_HOST`, `DATABRICKS_TOKEN` (PAT reused from dotenv), `FRED_API_KEY`, `ALERT_EMAIL`.
  Added 2026-08-09. FRED key + email reach the deploy via `BUNDLE_VAR_*` from those secrets.
- Local dev stays keyless (OAuth U2M). Only CI uses the token.
- [x] `ALERT_EMAIL` added to dotenv + example (done by user).

## Known gap - CI deploy uses the EXAMPLE watchlist
- The CI deploy (GitHub Actions) checks out only git contents, and `tickers.txt` is gitignored,
  so the deployed job falls back to `tickers.example.txt`. Harmless while example == real.
- WHEN the real list diverges: materialize `tickers.txt` in the deploy job from a GitHub secret
  (add a `WATCHLIST` secret, write the file before `databricks bundle deploy`). Not yet done.
- A manual deploy from a machine that has the real `tickers.txt` DOES ship it (sync.include).

## Verified end to end (2026-08-09)
- [x] FRED key + alert email BUNDLE_VAR resolution confirmed via validate.
- [x] Full pipeline green on main push: test (uv) -> validate -> deploy. "Deployment complete!"
- [x] Note: deploy updates the job definition; the pipeline itself runs on its daily cron
      (22:00 MON-FRI) or a manual `databricks bundle run stock_analytics_pipeline`.

## Done this session
- FRED key no longer hardcoded in `databricks.yml`. It flows from `.env` at deploy via the
  `fred_api_key` bundle variable / `BUNDLE_VAR_fred_api_key`. Single source of truth is now `.env`.
  Verified the old key never leaked: `databricks.yml` was never committed and the key string is
  in zero commits across all branches. No rotation needed.
