-- Flink SQL table definitions for heart-rate pipeline

CREATE TABLE `heart-rate-raw` (
  user_id STRING,
  event_id STRING,
  device_id STRING,
  source_app_id STRING,
  create_time BIGINT,
  update_time BIGINT,
  start_time BIGINT,
  end_time BIGINT,
  time_offset BIGINT,
  `comment` STRING,
  heart_rate FLOAT,
  heart_beat_count INT,
  `min` FLOAT,
  `max` FLOAT,
  client_data_id STRING,
  client_data_ver INT,
  start_time_ts AS TO_TIMESTAMP_LTZ(start_time, 3),
  WATERMARK FOR start_time_ts AS start_time_ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'confluent',
  'value.format' = 'avro-registry'
);

CREATE TABLE `heart-rate-cleaned` (
  user_id STRING,
  event_id STRING,
  device_id STRING,
  source_app_id STRING,
  create_time TIMESTAMP_LTZ(3),
  update_time TIMESTAMP_LTZ(3),
  start_time TIMESTAMP_LTZ(3),
  end_time TIMESTAMP_LTZ(3),
  time_offset BIGINT,
  `comment` STRING,
  heart_rate FLOAT,
  heart_beat_count INT,
  `min` FLOAT,
  `max` FLOAT,
  client_data_id STRING,
  client_data_ver INT,
  WATERMARK FOR start_time AS start_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'confluent',
  'value.format' = 'json-registry'
);

CREATE TABLE `heart-rate-features` (
  user_id STRING,
  window_start TIMESTAMP_LTZ(3),
  window_end TIMESTAMP_LTZ(3),
  sample_count BIGINT,
  avg_hr DOUBLE,
  min_hr DOUBLE,
  max_hr DOUBLE,
  hr_stddev DOUBLE,
  hr_range DOUBLE,
  elevated_count BIGINT
) WITH (
  'connector' = 'confluent',
  'value.format' = 'json-registry'
);

CREATE TABLE `heart-rate-risk-events` (
  user_id STRING,
  event_id STRING,
  event_time TIMESTAMP_LTZ(3),
  heart_rate FLOAT,
  anomaly_type STRING,
  risk_level STRING,
  window_start TIMESTAMP_LTZ(3),
  window_end TIMESTAMP_LTZ(3),
  detected_at TIMESTAMP_LTZ(3)
) WITH (
  'connector' = 'confluent',
  'value.format' = 'json-registry'
);
