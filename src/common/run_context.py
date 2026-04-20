"""
Point-in-time metadata for pipeline runs.

Every Bronze row carries:
  - _run_id          — UUID per pipeline execution (links rows from same run)
  - _ingest_ts       — when the platform received the data
  - _source_system   — where the data came from (e.g. "yfinance")
  - _source_event_ts — when the market event occurred (trade date, snapshot date)
  - _load_type       — "full" or "incremental"

IMPORTANT: Always ORDER BY _source_event_ts, never _ingest_ts.
The #1 cause of broken point-in-time implementations is confusing
when the data was received (_ingest_ts) with when the event happened
(_source_event_ts). Backtests using revised data show 15-25% higher
Sharpe ratios than initial-release data, so getting this right matters.
"""
import uuid
from datetime import datetime, date, timezone
from typing import Literal


def new_run_id() -> str:
    """Generate a unique run ID for this pipeline execution."""
    return str(uuid.uuid4())


def now_ts() -> str:
    """Current UTC timestamp in ISO format (for _ingest_ts)."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def source_event_ts(event_date: date | str) -> str:
    """
    Convert a trade date or snapshot date to ISO format (for _source_event_ts).

    For daily prices: the trade date (when the market closed).
    For fundamentals: the snapshot date (when the data was asserted).
    """
    if isinstance(event_date, str):
        return f"{event_date}T00:00:00Z"
    return event_date.isoformat() + "T00:00:00Z"


# Constants for metadata columns
SOURCE_SYSTEM = "yfinance"
LOAD_TYPE: Literal["full", "incremental"] = "full"

# Column names — used by ingestion and quality modules
META_COLUMNS = [
    "_run_id",
    "_ingest_ts",
    "_source_system",
    "_source_event_ts",
    "_load_type",
]