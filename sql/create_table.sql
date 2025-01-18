-- Create database
CREATE DATABASE stockdb;

-- Connect to database
\c stockdb

-- Create table for stock metrics
CREATE TABLE stock_metrics (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    symbol VARCHAR(50),
    avg_price DECIMAL(18,8),
    total_volume DECIMAL(18,8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_symbol_window ON stock_metrics(symbol, window_start);