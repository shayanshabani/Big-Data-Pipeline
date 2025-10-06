-- File: 01_init_schema.sql
-- This script will be executed automatically when the ClickHouse container starts for the first time.

-- Create a dedicated database for our data.
-- The 'IF NOT EXISTS' clause prevents errors on subsequent restarts if the database already exists.
CREATE DATABASE IF NOT EXISTS streaming_db;

--------------------------------------------------------------------------------
-- PART 1: KAFKA ENGINE TABLE (The "Pipe" into the Kafka Topic)
--------------------------------------------------------------------------------
-- This table does not store data itself but acts as a live stream from Kafka.
CREATE TABLE streaming_db.kafka_source_stream (
    -- The schema here MUST match the JSON structure from Kafka.
    -- ClickHouse is case-sensitive with JSON keys by default.
    RECORD_DATE DateTime64(3, 'UTC'),
    RAT UInt8,
    total_page_succeed_flag_zero UInt64,
    total_PAGE_DELAY_MSEL Float64,
    sum_of_page_size UInt64,
    total_rows UInt64
) ENGINE = Kafka
SETTINGS
    -- Connect to the internal listener of our Kafka broker
    kafka_broker_list = 'kafka-1:19092',

    -- The name of the topic to consume from
    kafka_topic_list = 'streaming-output',

    -- A unique name for the consumer group. ClickHouse uses this to track offsets.
    kafka_group_name = 'clickhouse_streaming_consumer_group',

    -- This format expects one JSON object per line. It's fast and efficient.
    kafka_format = 'JSONEachRow',

    -- Number of consumer threads on this node.
    kafka_num_consumers = 2,

    -- If a JSON field is missing in a message, insert the default value for the column type instead of failing.
    kafka_skip_broken_messages = 1;


--------------------------------------------------------------------------------
-- PART 2: MERGETREE TABLE (The Permanent, Performant Storage)
--------------------------------------------------------------------------------
-- This is where the data will actually be stored for fast analytical queries.
CREATE TABLE streaming_db.fact_records (
    -- We use the same columns and types from the Kafka table.
    RECORD_DATE DateTime64(3, 'UTC'),
    RAT UInt8,
    total_page_succeed_flag_zero UInt64,
    total_PAGE_DELAY_MSEL Float64,
    sum_of_page_size UInt64,
    total_rows UInt64,

    -- Add a timestamp for when the record was inserted into ClickHouse. Useful for auditing.
    ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
-- PARTITION BY: This is critical for performance. It tells ClickHouse to store data
-- in separate physical folders based on the date. Queries with a date filter will be much faster.
PARTITION BY toDate(RECORD_DATE)
-- ORDER BY: This is the primary key. Data is physically sorted on disk by this key.
-- Choose columns that you will frequently filter or aggregate on.
ORDER BY (RAT, RECORD_DATE);


--------------------------------------------------------------------------------
-- PART 3: MATERIALIZED VIEW (The "Glue" that automates the data flow)
--------------------------------------------------------------------------------
-- This view runs in the background, continuously watching the Kafka table
-- and inserting new messages into our permanent MergeTree table.
CREATE MATERIALIZED VIEW streaming_db.kafka_to_mergetree_mv TO streaming_db.fact_records AS
SELECT
    RECORD_DATE,
    RAT,
    total_page_succeed_flag_zero,
    total_PAGE_DELAY_MSEL,
    sum_of_page_size,
    total_rows
FROM streaming_db.kafka_source_stream;
