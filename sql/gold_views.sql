-- =============================================================
-- DASHBOARD SQL QUERIES FOR DATABRICKS SQL EDITOR
-- =============================================================
-- Copy/paste these into Databricks SQL dashboards.
-- All queries reference the Gold serving layer.

-- ── 1. Signal Alerts (today's threshold crossings) ────────────
SELECT symbol, alert_type, alert_value, as_of_date
FROM stock_analytics.gold.signal_alerts
ORDER BY alert_type, symbol;

-- ── 2. Top 10 by Composite Score (PERCENT_RANK) ──────────────
SELECT symbol, trade_signal, rsi, pe_ratio, change_30d_pct,
       momentum_pct, value_pct, risk_pct, quality_pct,
       composite_score, composite_rank,
       last_closing_price
FROM stock_analytics.gold.watchlist_ranked
ORDER BY composite_score DESC
LIMIT 10;

-- ── 3. Sector Heatmap (RSI by sector from dim_security) ──────
SELECT sec.sector, sig.symbol, sig.rsi, sig.trade_signal,
       sig.last_closing_price, sig.composite_score
FROM stock_analytics.gold.watchlist_ranked sig
JOIN stock_analytics.gold.dim_security sec
    ON sig.symbol = sec.symbol AND sec.is_current = true
ORDER BY sec.sector, sig.rsi ASC;

-- ── 4. Risk-Adjusted Ranking ─────────────────────────────────
-- Uses fact_signal_snapshot for risk metrics
SELECT f.symbol, f.rsi, f.mfi, f.bollinger_bandwidth,
       f.momentum_pct, f.risk_pct, f.quality_pct, f.composite_score
FROM stock_analytics.gold.fact_signal_snapshot f
WHERE f.as_of_date = (SELECT MAX(as_of_date) FROM stock_analytics.gold.fact_signal_snapshot)
ORDER BY f.composite_score DESC;

-- ── 5. Benchmark Comparison ──────────────────────────────────
SELECT symbol, last_closing_price,
       change_30d_pct, spy_change_30d, qqq_change_30d,
       change_90d_pct, spy_change_90d, qqq_change_90d,
       change_365d_pct, spy_change_365d, qqq_change_365d
FROM stock_analytics.gold.benchmark_compare
ORDER BY change_365d_pct DESC;

-- ── 6. Portfolio Candidates ──────────────────────────────────
SELECT candidate_bucket, symbol, last_closing_price,
       rsi, pe_ratio, dividend_yield, change_30d_pct
FROM stock_analytics.gold.portfolio_candidates
ORDER BY candidate_bucket, symbol;

-- ── 7. Signal History for a Specific Ticker ──────────────────
SELECT as_of_date, trade_signal, rsi, macd, macd_histogram,
       last_closing_price, bollinger_pct_b, bollinger_bandwidth,
       obv, mfi, ma_50, ma_200, ema_50, ema_200
FROM stock_analytics.gold.signal_history
WHERE symbol = 'AAPL'
ORDER BY as_of_date DESC;

-- ── 8. All Current Buy Signals ───────────────────────────────
SELECT symbol, rsi, macd, macd_histogram, pe_ratio,
       last_closing_price, composite_score, composite_rank
FROM stock_analytics.gold.watchlist_ranked
WHERE trade_signal = 'Buy'
ORDER BY composite_score DESC;

-- ── 9. Dividend Yield Traps ──────────────────────────────────
SELECT symbol, dividend_yield, dividend_yield_gap,
       pe_ratio, last_closing_price
FROM stock_analytics.gold.watchlist_ranked
WHERE dividend_yield_trap = true
ORDER BY dividend_yield_gap DESC;

-- ── 10. Trade Log with Current Price for P&L ────────────────
SELECT
    t.trade_date, t.symbol, t.action, t.shares,
    t.price AS entry_price, w.last_closing_price AS current_price,
    ROUND((w.last_closing_price - t.price) * t.shares, 2) AS unrealized_pnl,
    ROUND(((w.last_closing_price - t.price) / t.price) * 100, 2) AS return_pct,
    t.rationale, t.signal_at_entry
FROM stock_analytics.gold.trade_log t
LEFT JOIN stock_analytics.gold.watchlist_ranked w ON t.symbol = w.symbol
WHERE t.action = 'BUY'
ORDER BY t.trade_date DESC;

-- ── 11. Data Freshness Check ─────────────────────────────────
SELECT 'signals' AS source,
       MAX(as_of_date) AS latest_date,
       COUNT(*) AS total_rows
FROM stock_analytics.gold.signal_history
UNION ALL
SELECT 'watchlist', MAX(as_of_date), COUNT(*)
FROM stock_analytics.gold.watchlist_ranked
UNION ALL
SELECT 'alerts', MAX(as_of_date), COUNT(*)
FROM stock_analytics.gold.signal_alerts;

-- ── 12. Log a Trade (run manually) ───────────────────────────
-- INSERT INTO stock_analytics.gold.trade_log VALUES (
--     '2026-04-21', 'AAPL', 'BUY', 10, 195.50, 1955.00,
--     'RSI oversold at 28, composite rank #3, below MA200',
--     'Buy', 28.4, 24.1, NULL
-- );