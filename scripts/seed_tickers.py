"""Materialize src/common/tickers.txt from the WATCHLIST value in .env.

Pipeline code reads WATCHLIST straight from the environment, but databricks.yml
force-syncs tickers.txt into the bundle, so a manual `databricks bundle deploy`
ships whatever that file holds. Run this first on any machine where the
watchlist only lives in .env. CI does the equivalent from the WATCHLIST secret.

    uv run python scripts/seed_tickers.py
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from dotenv import load_dotenv  # noqa: E402 - after sys.path setup
from common.config import _parse_tickers  # noqa: E402 - one parser, no drift

TARGET = os.path.join(ROOT, "src", "common", "tickers.txt")


def main():
    load_dotenv(os.path.join(ROOT, ".env"))
    tickers = _parse_tickers(os.getenv("WATCHLIST", ""))
    if not tickers:
        sys.exit("WATCHLIST is unset or empty in .env - tickers.txt not written.")
    with open(TARGET, "w") as f:
        f.write("\n".join(tickers) + "\n")
    print(f"Wrote {len(tickers)} tickers to {TARGET}")


if __name__ == "__main__":
    main()
