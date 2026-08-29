-- 003 · kafka_auto_offset_reset = 'earliest'
--
-- Companion to `docker/clickhouse/init/01-schema.sql`. Run this against a
-- database that already exists; the init script carries the same value for a new
-- one. Edit both.
--
-- WHY
--
-- The topic is on ONE partition. If it is ever grown (to 3, say, because
-- ingestion is lagging), the new partitions have no committed offset for our
-- consumer group, so librdkafka's `auto.offset.reset` decides where they start.
-- Its default is 'latest'.
--
-- On 'latest' the sequence is:
--
--   1. partitions 1 and 2 are created
--   2. the PRODUCER refreshes topic metadata on its own schedule and starts
--      writing to them
--   3. the CONSUMER refreshes and rebalances, is assigned them, and begins at
--      "latest" — i.e. AFTER whatever step 2 already wrote
--
-- Everything written in that gap is skipped permanently. There is no watermark
-- and no replay position to come back for it. Signals are money.
--
-- 'earliest' closes it. It costs nothing before the topic grows: partition 0 has
-- a committed offset, so the setting is never consulted.
--
-- A Kafka engine table cannot be ALTERed — `ALTER TABLE ... MODIFY SETTING` does
-- not work on it. So this is a drop-and-recreate. It is safe:
--
--   * `signal_log` is NOT touched. No archived row is lost.
--   * `kafka_group_name` is unchanged, so partition 0's committed offset is
--     still there and consumption resumes exactly where it stopped.
--   * Anything produced during the gap between the DROP and the CREATE is picked
--     up afterwards from that committed offset — Kafka retains it for 7 days.
--
-- Drop the MV FIRST. It is what holds a consumer attached to the stream.

DROP TABLE IF EXISTS signals.signal_log_mv;
DROP TABLE IF EXISTS signals.kafka_signals;

-- Recreate the stream, adding the setting. Substitute your real broker settings —
-- in AWS these are kafka.yourdomain.com:9094 + sasl_ssl, not the compose values.
CREATE TABLE IF NOT EXISTS signals.kafka_signals
(
  raw String
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list       = 'kafka:29092',
  kafka_topic_list        = 'signals',
  kafka_group_name        = 'clickhouse-signal-log',
  kafka_format            = 'JSONAsString',
  kafka_num_consumers     = 1,
  kafka_auto_offset_reset = 'earliest';

-- Then the MV, unchanged from 01-schema.sql.
CREATE MATERIALIZED VIEW IF NOT EXISTS signals.signal_log_mv TO signals.signal_log AS
SELECT
  JSONExtractString(raw, 'signalId')                              AS signal_id,
  parseDateTime64BestEffort(JSONExtractString(raw, 'receivedAt'))  AS received_at,
  JSONExtractString(raw, 'apiKeyHash')                            AS api_key_hash,
  JSONExtractRaw(raw, 'body')                                     AS payload
FROM signals.kafka_signals;
