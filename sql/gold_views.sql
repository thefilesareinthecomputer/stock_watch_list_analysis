-- =============================================================
-- REFERENCE QUERIES FOR DASHBOARDS AND AD-HOC ANALYSIS
-- =============================================================

-- Top 10 stocks by composite rank (today)
SELECT symbol, trade_signal, rsi, pe_ratio, change_30d_pct,
       rsi_rank, value_rank, momentum_rank, composite_rank,
       last_closing_price
FROM stock_analytics.gold.watchlist_ranked
ORDER BY composite_rank ASC
LIMIT 10;

-- Signal history for a specific ticker
SELECT as_of_date, trade_signal, rsi, macd, last_closing_price,
       ma_50, ma_200, bollinger_upper, bollinger_lower
FROM stock_analytics.gold.signal_history
WHERE symbol = 'AAPL'
ORDER BY as_of_date DESC;

-- All current Buy signals
SELECT symbol, rsi, macd, pe_ratio, last_closing_price, composite_rank
FROM stock_analytics.gold.watchlist_ranked
WHERE trade_signal = 'Buy'
ORDER BY composite_rank ASC;

-- Trade log with current price for P&L tracking
SELECT
    t.trade_date,
    t.symbol,
    t.action,
    t.shares,
    t.price AS entry_price,
    w.last_closing_price AS current_price,
    ROUND((w.last_closing_price - t.price) * t.shares, 2) AS unrealized_pnl,
    ROUND(((w.last_closing_price - t.price) / t.price) * 100, 2) AS return_pct,
    t.rationale,
    t.signal_at_entry
FROM stock_analytics.gold.trade_log t
LEFT JOIN stock_analytics.gold.watchlist_ranked w
    ON t.symbol = w.symbol
WHERE t.action = 'BUY'
ORDER BY t.trade_date DESC;

-- Example: Log a trade (run manually after making a decision)
-- INSERT INTO stock_analytics.gold.trade_log VALUES (
--     '2026-04-21', 'AAPL', 'BUY', 10, 195.50, 1955.00,
--     'RSI oversold at 28, composite rank #3, below MA200',
--     'Buy', 28.4, 24.1, NULL
-- );
