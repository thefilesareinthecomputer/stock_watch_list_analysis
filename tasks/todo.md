# TODO

## On this (desktop) device - staged for the user to run
- [ ] Untrack the now-private real watchlist (keeps the local file, stops tracking it):
      `git rm --cached src/common/tickers.txt`
- [x] Edit `.env.example`: added `FRED_API_KEY` (done by user).
- [ ] Stage and commit the tidy-up:
      `git add CLAUDE.md README.md .gitignore .env.example src/common/config.py src/common/tickers.example.txt databricks.yml tasks/`
      `git rm --cached src/common/tickers.txt`
      `git commit -m "chore: privatize watchlist; track databricks.yml (FRED key -> .env); de-stale docs"`
- [ ] Push: `git push` (branch already tracks origin/feature/upgrade-stock-pipeline).

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

## Before next deploy
- [ ] Add `ALERT_EMAIL=c3por2d2atat@gmail.com` to `.env` (and a placeholder in `.env.example`).
      If unset, `on_failure` resolves to an empty string and the deploy is rejected.

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
