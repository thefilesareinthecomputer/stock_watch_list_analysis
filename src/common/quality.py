"""
Data quality validation functions for each medallion layer.

Usage:
    from common.quality import check_bronze_prices, check_silver_prices

Each check function returns a list of warning/error strings.
Empty list = all checks passed.
"""
import pandas as pd
import numpy as np
from typing import List


def check_not_empty(df: pd.DataFrame, name: str) -> List[str]:
    """Check DataFrame is not empty."""
    if df.empty:
        return [f"{name}: DataFrame is empty"]
    return []


def check_expected_columns(df: pd.DataFrame, expected: set, name: str) -> List[str]:
    """Check all expected columns exist."""
    missing = expected - set(df.columns)
    if missing:
        return [f"{name}: Missing columns: {missing}"]
    return []


def check_null_percentage(
    df: pd.DataFrame, columns: list, name: str, threshold: float = 0.05
) -> List[str]:
    """Check null percentage in specified columns is below threshold."""
    issues = []
    for col in columns:
        if col in df.columns:
            null_pct = df[col].isna().sum() / len(df) if len(df) > 0 else 0
            if null_pct > threshold:
                issues.append(
                    f"{name}: Column '{col}' has {null_pct:.1%} nulls (threshold {threshold:.0%})"
                )
    return issues


def check_duplicate_keys(df: pd.DataFrame, keys: list, name: str) -> List[str]:
    """Check for duplicate rows on composite key."""
    dup_count = df.duplicated(subset=keys).sum()
    if dup_count > 0:
        return [f"{name}: {dup_count} duplicate rows on key {keys}"]
    return []


def check_price_consistency(df: pd.DataFrame, name: str) -> List[str]:
    """Check for impossible price relationships (OHLCV)."""
    issues = []
    if all(c in df.columns for c in ("high", "low", "close")):
        high_lt_low = (df["high"] < df["low"]).sum()
        if high_lt_low > 0:
            issues.append(f"{name}: {high_lt_low} rows where high < low")

    if "volume" in df.columns:
        neg_vol = (df["volume"] < 0).sum()
        if neg_vol > 0:
            issues.append(f"{name}: {neg_vol} rows with negative volume")

    if all(c in df.columns for c in ("close", "high", "low")):
        outside_range = (
            (df["close"] > df["high"]) | (df["close"] < df["low"])
        ).sum()
        if outside_range > 0:
            issues.append(f"{name}: {outside_range} rows where close outside high-low range")
    return issues


def check_date_monotonic(df: pd.DataFrame, symbol_col: str, date_col: str, name: str) -> List[str]:
    """Check dates are monotonically increasing per symbol."""
    issues = []
    if symbol_col in df.columns and date_col in df.columns:
        for sym, group in df.groupby(symbol_col):
            dates = group[date_col]
            if dates.is_monotonic_increasing is False:
                issues.append(f"{name}: Dates not monotonic for {sym}")
    return issues


def check_source_event_ts_before_ingest_ts(df: pd.DataFrame, name: str) -> List[str]:
    """Check _source_event_ts <= _ingest_ts (no future-dated events)."""
    issues = []
    if "_source_event_ts" in df.columns and "_ingest_ts" in df.columns:
        future = df[df["_source_event_ts"] > df["_ingest_ts"]]
        if not future.empty:
            issues.append(
                f"{name}: {len(future)} rows where _source_event_ts > _ingest_ts"
            )
    return issues


# ── Layer-specific composite checks ─────────────────────────────────

def check_bronze_prices(df: pd.DataFrame) -> List[str]:
    """Run all Bronze price quality checks."""
    issues = []
    issues += check_not_empty(df, "bronze.daily_prices")
    issues += check_expected_columns(
        df, {"symbol", "date", "open", "high", "low", "close", "volume"},
        "bronze.daily_prices"
    )
    issues += check_null_percentage(
        df, ["close", "symbol", "date"], "bronze.daily_prices"
    )
    issues += check_price_consistency(df, "bronze.daily_prices")
    issues += check_source_event_ts_before_ingest_ts(df, "bronze.daily_prices")
    return issues


def check_silver_prices(df: pd.DataFrame) -> List[str]:
    """Run all Silver price quality checks."""
    issues = []
    issues += check_not_empty(df, "silver.daily_prices")
    issues += check_expected_columns(
        df, {"symbol", "date", "close"}, "silver.daily_prices"
    )
    issues += check_duplicate_keys(df, ["symbol", "date"], "silver.daily_prices")
    issues += check_price_consistency(df, "silver.daily_prices")
    issues += check_date_monotonic(df, "symbol", "date", "silver.daily_prices")
    return issues