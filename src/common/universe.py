"""Rule-based universe: line of sight over every listed common stock.

Three layers, deliberately different contracts (parent spec P2 + task 13):

- `bronze_prices_broad`: a rolling ~2-year adjusted close+volume window for
  the WHOLE inventory, replaced on every refresh. Screening data, not
  evidence - the evidence contract (raw prices, immutable, own adjustment
  factors) stays with `bronze_prices` for the tracked set.
- `universe_membership`: append-only enter/exit events for the computed
  top-N-by-dollar-volume tier, so the tier is reconstructable at any past
  date. Rule-computed from our own data - never licensed index membership.
- `gold_line_of_sight`: every broad symbol ranked on SQL-computed price
  signals with tier tags. SIZE IS A TAG, NOT A FILTER: small caps stay in,
  and the `emerging` tag flags the ones whose momentum or dollar-volume
  acceleration says they might get big.
"""
from datetime import datetime, timezone

import pandas as pd

BROAD_TIER = "broad_top_n"
BROAD_WINDOW_DAYS = 730          # ~2 trading years: enough for 12-1 momentum
FETCH_BATCH = 100

# Emerging-tag rules: relative rank alone is NOT a signal - the top decile
# of a cross-section always holds ~10% of the universe, bull or crash, so
# each leg also demands ABSOLUTE confirmation (actually rising, volume
# actually building). In a bear market the tag correctly shrinks toward
# zero. Floors keep it tradeable. Visible constants, not buried judgment.
MIN_PRICE = 1.0                  # no sub-dollar pennies
MIN_DOLLAR_VOLUME = 200_000      # median daily traded value, 63 sessions
EMERGING_PERCENTILE = 0.90       # relative bar, both legs
MIN_MOMENTUM = 0.0               # 12-1 return must be positive
MIN_DV_ACCELERATION = 1.0        # recent $vol must exceed the year's


BROAD_SCHEMA = """
    CREATE TABLE IF NOT EXISTS bronze_prices_broad (
        symbol VARCHAR, date DATE, close DOUBLE, volume BIGINT,
        _fetched_at DATE
    )
"""


# A symbol fetched within this many days counts as done when resuming: an
# interrupted run often spans midnight (both 2026-08-11 kills did), and an
# exact same-day check would restart a 6k-symbol fetch from zero. Well
# under the monthly refresh cadence, so a real refresh still refetches all.
RESUME_MAX_AGE_DAYS = 5


def fetch_broad_window(con, symbols, days=BROAD_WINDOW_DAYS, fetch=None,
                       today=None, resume_max_age=RESUME_MAX_AGE_DAYS):
    """Rolling adjusted window for all symbols -> bronze_prices_broad.

    Written INCREMENTALLY, one batch at a time, and resumable: symbols
    fetched within `resume_max_age` days are skipped, so a killed or
    crashed run (sleep, throttling - a ~6k-symbol fetch takes hours and
    was killed twice on 2026-08-11) loses one batch, not the run, even
    across midnight.

    auto_adjust=True is fine HERE and only here: rows are replaced on
    every refresh, so yfinance's history rewriting cannot corrupt anything
    downstream - nothing durable derives from this table.
    """
    import yfinance as yf
    con.execute(BROAD_SCHEMA)
    today = today or pd.Timestamp.today().date()
    start = (pd.Timestamp(today) - pd.Timedelta(days=days)).date()
    cutoff = (pd.Timestamp(today) - pd.Timedelta(days=resume_max_age)).date()
    done = {r[0] for r in con.execute(
        "SELECT DISTINCT symbol FROM bronze_prices_broad "
        "WHERE _fetched_at > ?", [cutoff]).fetchall()}
    todo = [s for s in symbols if s not in done]
    if done:
        print(f"  resuming: {len(done)} symbols fetched within "
              f"{resume_max_age} days", flush=True)

    total = 0
    for i in range(0, len(todo), FETCH_BATCH):
        batch = todo[i:i + FETCH_BATCH]
        if fetch is not None:
            raw = fetch(batch, str(start))
        else:
            raw = yf.download(batch, start=str(start), auto_adjust=True,
                              group_by="ticker", progress=False,
                              threads=True)
        frames = []
        if raw is not None and not raw.empty:
            for symbol in batch:
                try:
                    sub = raw[symbol] if len(batch) > 1 else raw
                except KeyError:
                    continue
                sub = sub.dropna(subset=["Close"])
                if sub.empty:
                    continue
                frames.append(pd.DataFrame({
                    "symbol": symbol,
                    "date": pd.to_datetime(sub.index).date,
                    "close": sub["Close"].values,
                    "volume": sub["Volume"].values}))
        if frames:
            frame = pd.concat(frames, ignore_index=True)
            # Same guard as the evidence backfill: a partial bar for an
            # unfinished session must not reach the screening table either
            # (it feeds deteriorating and the harvest price fallback).
            from common.quality import completed_session_cutoff
            frame = frame[frame["date"] <= completed_session_cutoff()]
            frame["_fetched_at"] = today
            con.register("incoming_broad", frame)
            # Replace-per-symbol keeps the refresh idempotent while stale
            # rows from a prior refresh survive until their symbol's turn.
            con.execute("DELETE FROM bronze_prices_broad WHERE symbol IN "
                        "(SELECT DISTINCT symbol FROM incoming_broad)")
            con.execute("INSERT INTO bronze_prices_broad "
                        "SELECT * FROM incoming_broad")
            con.unregister("incoming_broad")
            total += len(frame)
        if (i // FETCH_BATCH) % 5 == 4:
            print(f"  broad window {min(i + FETCH_BATCH, len(todo))}"
                  f"/{len(todo)} symbols, {total} rows", flush=True)
    return total


def rank_dollar_volume(con, top_n):
    """The current top-N by trailing 63-session median dollar volume.

    Median, not mean: one merger-rumor day should not buy a symbol into
    the tier. Returns an ordered DataFrame (symbol, dollar_volume, rank).
    """
    return con.execute("""
        WITH recent AS (
            SELECT symbol, close * volume AS dollar_volume,
                   ROW_NUMBER() OVER (PARTITION BY symbol
                                      ORDER BY date DESC) AS rn
            FROM bronze_prices_broad
        )
        SELECT symbol, MEDIAN(dollar_volume) AS dollar_volume,
               RANK() OVER (ORDER BY MEDIAN(dollar_volume) DESC) AS rank
        FROM recent WHERE rn <= 63
        GROUP BY symbol
        ORDER BY rank
        LIMIT ?
    """, [top_n]).df()


def current_members(con, tier=BROAD_TIER):
    """Membership reconstructed from the event log (empty if no table)."""
    exists = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'universe_membership'").fetchone()[0]
    if not exists:
        return set()
    rows = con.execute("""
        SELECT symbol FROM universe_membership WHERE tier = ?
        GROUP BY symbol
        HAVING SUM(CASE WHEN action = 'enter' THEN 1 ELSE -1 END) > 0
    """, [tier]).fetchall()
    return {r[0] for r in rows}


def update_membership(con, top, tier=BROAD_TIER):
    """Append enter/exit events so the tier is PIT-reconstructable.

    Symbols are retired by exit events, never deleted (SPEC P3). Returns
    (n_entered, n_exited). Idempotent for an unchanged top set.
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS universe_membership (
            symbol VARCHAR, tier VARCHAR, action VARCHAR,
            event_ts TIMESTAMP, reason VARCHAR, rank_at_event INTEGER
        )
    """)
    now = datetime.now(timezone.utc)
    members = current_members(con, tier)
    ranks = dict(zip(top["symbol"], top["rank"]))
    entering = [s for s in top["symbol"] if s not in members]
    exiting = sorted(members - set(top["symbol"]))
    if entering:
        con.executemany(
            "INSERT INTO universe_membership VALUES (?, ?, 'enter', ?, ?, ?)",
            [(s, tier, now, "dollar-volume rank within top N", int(ranks[s]))
             for s in entering])
    if exiting:
        con.executemany(
            "INSERT INTO universe_membership VALUES (?, ?, 'exit', ?, ?, NULL)",
            [(s, tier, now, "dollar-volume rank fell out of top N")
             for s in exiting])
    return len(entering), len(exiting)


def build_line_of_sight(con, watchlist, held):
    """gold_line_of_sight: every broad symbol, ranked, tagged, none dropped.

    SQL-computed price signals only - the pandas indicator battery would
    turn a 6,000-symbol build into an hour. 12-1 momentum and dollar-volume
    acceleration are the "might get big" signals; `emerging` flags
    below-tier names in the top decile of BOTH, each with absolute
    confirmation (rising, volume building), above the penny/illiquid
    floors - so the list is a shortlist that shrinks in a bear market,
    not a permanent decile. Watchlist/held tags come from the private
    config at build time and exist only in the gitignored warehouse.
    """
    members = current_members(con)
    con.register("incoming_tags", pd.DataFrame({
        "symbol": sorted(set(watchlist) | set(held) | members),
    }).assign(
        is_watchlist=lambda d: d["symbol"].isin(set(watchlist)),
        is_held=lambda d: d["symbol"].isin(set(held)),
        is_top_n=lambda d: d["symbol"].isin(members)))
    con.execute(f"""
        CREATE OR REPLACE TABLE gold_line_of_sight AS
        WITH ordered AS (
            SELECT symbol, date, close, volume, close * volume AS dv,
                   ROW_NUMBER() OVER (PARTITION BY symbol
                                      ORDER BY date DESC) AS rn
            FROM bronze_prices_broad
        ),
        signals AS (
            SELECT symbol,
                   MAX(CASE WHEN rn = 1 THEN date END) AS as_of_date,
                   MAX(CASE WHEN rn = 1 THEN close END) AS close,
                   MAX(CASE WHEN rn = 21 THEN close END)
                       / NULLIF(MAX(CASE WHEN rn = 252 THEN close END), 0)
                       - 1 AS mom_12_1,
                   MAX(CASE WHEN rn = 1 THEN close END)
                       / NULLIF(MAX(CASE WHEN rn = 63 THEN close END), 0)
                       - 1 AS ret_3m,
                   MAX(CASE WHEN rn = 1 THEN close END)
                       / NULLIF(MAX(CASE WHEN rn = 126 THEN close END), 0)
                       - 1 AS ret_6m,
                   MAX(CASE WHEN rn = 1 THEN close END)
                       / NULLIF(MAX(CASE WHEN rn = 252 THEN close END), 0)
                       - 1 AS ret_12m,
                   MEDIAN(CASE WHEN rn <= 63 THEN dv END) AS dollar_volume_63d,
                   MEDIAN(CASE WHEN rn <= 21 THEN dv END)
                       / NULLIF(MEDIAN(CASE WHEN rn <= 252 THEN dv END), 0)
                       AS dv_acceleration
            FROM ordered
            GROUP BY symbol
        ),
        ranked AS (
            SELECT s.*, t.is_watchlist, t.is_held, t.is_top_n,
                   PERCENT_RANK() OVER (ORDER BY mom_12_1 ASC NULLS FIRST)
                       AS mom_pct,
                   PERCENT_RANK() OVER (ORDER BY dv_acceleration ASC
                                        NULLS FIRST) AS dv_accel_pct
            FROM signals s
            LEFT JOIN incoming_tags t USING (symbol)
        )
        SELECT symbol, as_of_date, close, mom_12_1, mom_pct,
               ret_3m, ret_6m, ret_12m,
               -- The sell-side mirror of emerging, and deliberately
               -- ABSOLUTE: lower than 3, 6, and 12 months ago is steady
               -- decline whatever the rest of the market is doing.
               (ret_3m < 0 AND ret_6m < 0 AND ret_12m < 0) AS deteriorating,
               dollar_volume_63d, dv_acceleration, dv_accel_pct,
               COALESCE(is_watchlist, FALSE) AS is_watchlist,
               COALESCE(is_held, FALSE) AS is_held,
               COALESCE(is_top_n, FALSE) AS is_top_n,
               (NOT COALESCE(is_top_n, FALSE)
                AND close >= {MIN_PRICE}
                AND dollar_volume_63d >= {MIN_DOLLAR_VOLUME}
                AND mom_pct >= {EMERGING_PERCENTILE}
                AND mom_12_1 > {MIN_MOMENTUM}
                AND dv_accel_pct >= {EMERGING_PERCENTILE}
                AND dv_acceleration > {MIN_DV_ACCELERATION})
                   AS emerging
        FROM ranked
    """)
    con.unregister("incoming_tags")
    return con.execute(
        "SELECT COUNT(*), COUNT(*) FILTER (emerging) "
        "FROM gold_line_of_sight").fetchone()
