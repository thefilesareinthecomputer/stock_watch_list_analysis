# TODO

Session-to-session handoff snapshot. Resolved threads leave; survivors compress
to state + next action + pointer. Full records: `tasks/completed/`.

## State as of 2026-08-09

Desktop is fully configured and verified. `develop` and `main` are aligned at
`52489de`; CI green (test -> validate -> deploy). Job definition on Databricks
came from CI on `main`. Record: `completed/plan-completed-2026-08-09.md`.

## Open

1. **Laptop not yet set up.** Full steps are in README "Quick Start". Two
   corrections that the older instructions got wrong:
   - Do **not** `brew install terraform` - the CLI downloads its own.
   - Recreate the private watchlist with
     `uv run python scripts/watchlist.py seed` (reads `WATCHLIST` from the dotenv),
     not by copying the example. The example is now a 10-ticker starter, not the
     real list.

2. **Watchlist swept and corrected - 318 tickers, all resolving.** Eight dead
   symbols removed and two successors added (`SM`, `XYZ`) on 2026-08-09; the
   causes are recorded in `src/common/ticker_migrations.json`. Dotenv, the
   `WATCHLIST` repo secret and `tickers.txt` all carry the same 318.
   **Recurring:** run `uv run python scripts/watchlist.py check` after any
   watchlist edit, and monthly when the 30-day cache expires. New dead symbols
   print as `UNMAPPED` - research and add them to the migrations file.

3. **`feature/upgrade-stock-pipeline` still exists on origin** at `bd0694f`,
   fully superseded by `develop`. **Decision needed:** delete it or keep it.
   Not deleting a remote branch without a ruling.

4. **`setup-uv` is pinned to exact `v9.0.0`**, so it will not pick up patch
   releases. Bump manually, or switch to a floating `v9` tag once astral
   publishes one.

## Deferred, not blocking

- `requests` is pinned at the base-image version (2.32.2) rather than the newest.
  Raising it means proving it against the env-3 base first; see `plan.md` item 2.
- The env-3 base ships pandas 1.5.3 and the job upgrades to 3.0.2 on every run.
  That predates this session and is unchanged, but it is install time on each run.
