"""Seed and validate the private watchlist.

    uv run python scripts/watchlist.py seed          # WATCHLIST -> tickers.txt
    uv run python scripts/watchlist.py check         # every ticker resolves?
    uv run python scripts/watchlist.py check --full  # ignore the cache

seed writes src/common/tickers.txt from the WATCHLIST value in the dotenv.
Pipeline code reads WATCHLIST from the environment directly, but databricks.yml
force-syncs the file into the bundle, so a manual deploy ships whatever it holds.
CI materializes the same file from the WATCHLIST repo secret instead.

check validates what config actually resolves (WATCHLIST, else tickers.txt, else
the example) plus the benchmarks, so it verifies the list the pipeline will use.
A bad symbol fails silently in the pipeline - it just contributes no rows - so
nothing else surfaces it.

Both commands live here, sharing one parser and one config import, so the seeded
list and the validated list cannot drift apart.

Checking costs ~1.5s per symbol against Yahoo, and threading does not help
(measured: 25 symbols took 36s sequential, 40s threaded). So results are cached:
a symbol confirmed within CACHE_DAYS is skipped. Only typos and delistings make a
symbol go bad, and neither decays on a shorter timescale.
"""
import argparse
import json
import os
import sys
from datetime import date, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from dotenv import load_dotenv  # noqa: E402 - after sys.path setup

load_dotenv(os.path.join(ROOT, ".env"))

from common.config import _parse_tickers  # noqa: E402 - one parser, no drift

TARGET = os.path.join(ROOT, "src", "common", "tickers.txt")
CACHE = os.path.join(ROOT, "src", "common", ".tickers_validated.json")
MIGRATIONS = os.path.join(ROOT, "src", "common", "ticker_migrations.json")
CACHE_DAYS = 30
CHUNK = 50


def seed():
    tickers = _parse_tickers(os.getenv("WATCHLIST", ""))
    if not tickers:
        sys.exit("WATCHLIST is unset or empty in the dotenv - tickers.txt not written.")
    with open(TARGET, "w") as f:
        f.write("\n".join(tickers) + "\n")
    print(f"Wrote {len(tickers)} tickers to {TARGET}")


def _load_cache():
    try:
        with open(CACHE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def check(full):
    import yfinance as yf
    from common.config import TICKERS, BENCHMARK_TICKERS

    symbols = sorted(set(TICKERS) | set(BENCHMARK_TICKERS))
    cache = {} if full else _load_cache()
    fresh = (date.today() - timedelta(days=CACHE_DAYS)).isoformat()
    todo = [s for s in symbols if cache.get(s, "") < fresh]
    print(f"{len(symbols) - len(todo)} cached, {len(todo)} to check", flush=True)

    bad = []
    for i in range(0, len(todo), CHUNK):
        batch = todo[i:i + CHUNK]
        close = yf.download(batch, period="5d", progress=False,
                            auto_adjust=True)["Close"]
        for s in batch:
            if s not in close.columns or close[s].dropna().empty:
                bad.append(s)
            else:
                cache[s] = date.today().isoformat()
        # flush: this runs for minutes, and piped stdout is block-buffered
        print(f"  checked {min(i + CHUNK, len(todo))}/{len(todo)}", flush=True)

    with open(CACHE, "w") as f:
        json.dump(cache, f, indent=2, sort_keys=True)

    if bad:
        _report(bad, symbols)
        sys.exit(1)
    print(f"\nAll {len(symbols)} tickers resolve.")


def _report(bad, symbols):
    """Explain each dead symbol from ticker_migrations.json, and print the
    corrected list ready to paste into WATCHLIST (dotenv and repo secret)."""
    with open(MIGRATIONS) as f:
        known = json.load(f)

    print(f"\n{len(bad)} of {len(symbols)} returned no data:")
    unmapped = []
    for s in bad:
        m = known.get(s)
        if not m:
            unmapped.append(s)
            print(f"  {s:<6} UNMAPPED - research it and add it to "
                  f"ticker_migrations.json")
            continue
        became = m["successor"] or "ceased to exist"
        print(f"  {s:<6} -> {became:<10} ({m['date'] or 'n/a'}) {m['reason']}")

    successors = {known[s]["successor"] for s in bad
                  if known.get(s) and known[s]["successor"]}
    keep = sorted((set(symbols) - set(bad)) | successors)
    added = sorted(successors - set(symbols))
    print(f"\nCorrected WATCHLIST ({len(keep)} tickers; "
          f"dropped {len(bad)}, added {len(added) or 'none'}"
          f"{': ' + ', '.join(added) if added else ''}):")
    print(",".join(keep))
    if unmapped:
        print(f"\nStill unexplained: {', '.join(unmapped)}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("seed", help="write tickers.txt from WATCHLIST")
    c = sub.add_parser("check", help="verify every ticker resolves on yfinance")
    c.add_argument("--full", action="store_true", help="ignore the cache")
    args = p.parse_args()

    seed() if args.cmd == "seed" else check(args.full)
