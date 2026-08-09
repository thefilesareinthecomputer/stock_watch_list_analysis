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

## On the laptop (next session)
- [ ] Pull the branch.
- [ ] Recreate the private watchlist: `cp src/common/tickers.example.txt src/common/tickers.txt`,
      then adjust to the real strategy list. It is gitignored, so it will NOT arrive via git -
      each device keeps its own copy and changes must be mirrored manually.
- [ ] `databricks.yml` now arrives via git (newly tracked) - no need to recreate it.
- [ ] Set up the Databricks CLI with OAuth (see below) - per device, not synced via git.

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

## Verify later (next `databricks bundle deploy`)
- [ ] Confirm the real `tickers.txt` (not the example) reached the Databricks job. If not, the
      `sync.include` in `databricks.yml` did not override `.gitignore`. (Only item not yet proven.)
- [x] FRED key + alert email variable resolution CONFIRMED via `databricks bundle validate`:
      BUNDLE_VAR values substitute into the job parameter default and on_failure. Runtime still
      relies on the ingest_fred task reading `{{job.parameters.fred_api_key}}` from argv.

## Done this session
- FRED key no longer hardcoded in `databricks.yml`. It flows from `.env` at deploy via the
  `fred_api_key` bundle variable / `BUNDLE_VAR_fred_api_key`. Single source of truth is now `.env`.
  Verified the old key never leaked: `databricks.yml` was never committed and the key string is
  in zero commits across all branches. No rotation needed.
