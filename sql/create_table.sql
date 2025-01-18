-- Create database
CREATE DATABASE stockdb;

-- Connect to database
\c stockdb

-- Create table for stock metrics
CREATE TABLE stock_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    avg_price DECIMAL(18,8) NOT NULL,
    total_volume DECIMAL(18,8) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_symbol_window ON stock_metrics(symbol, window_start);
CREATE INDEX idx_window ON stock_metrics(window_start, window_end);