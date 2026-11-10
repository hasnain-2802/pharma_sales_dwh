CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.batch_ingestion_log (
    log_id        BIGSERIAL PRIMARY KEY,
    batch_id      TEXT,
    source_name   TEXT,
    file_name     TEXT,
    start_ts      TIMESTAMP,
    end_ts        TIMESTAMP,
    duration_sec  NUMERIC(10,2),
    rows_read     BIGINT,
    rows_loaded   BIGINT,
    rows_rejected BIGINT,
    status        TEXT,
    error_msg     TEXT
);

CREATE TABLE IF NOT EXISTS stg.rejected_records (
    reject_id     BIGSERIAL PRIMARY KEY,
    batch_id      TEXT,
    source_name   TEXT,
    file_name     TEXT,
    reject_reason TEXT,
    row_data      TEXT,
    rejected_at   TIMESTAMP DEFAULT now()
);
