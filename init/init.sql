CREATE DATABASE IF NOT EXISTS finance;

CREATE TABLE IF NOT EXISTS finance.transaction (
    transaction_id UUID,
    timestamp UInt32,
    amount Decimal(10, 2),
    currency String,
    sender_id String,
    country FixedString(2),
    payment_method Nullable(String),
    receiver_id String,
    is_cross_border UInt8,
    destination_country FixedString(2)
) ENGINE = MergeTree()
ORDER BY (timestamp, transaction_id);

CREATE TABLE IF NOT EXISTS finance.anomaly (
    id UUID DEFAULT generateUUIDv4(),
    transaction_id UUID,
    reason String,
    timestamp UInt32
) ENGINE = MergeTree()
ORDER BY (timestamp);

CREATE TABLE finance.daily_metrics (
    date Date,
    total_transactions UInt32,
    total_cross_border_transac UInt32,
    total_amount Decimal(20, 2),
    anomaly_count UInt32,
    unique_senders UInt32
) ENGINE = MergeTree()
ORDER BY (date);

CREATE TABLE finance.sender_risk_score (
    sender_id String,
    total_transactions UInt32,
    total_anomalies UInt32,
    risk_score Float32,
    last_updated DateTime
) ENGINE = MergeTree()
ORDER BY (sender_id, last_updated);