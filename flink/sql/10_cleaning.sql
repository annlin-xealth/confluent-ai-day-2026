-- Stage 1: Cleaning + deduplication
-- Filters out invalid values.

INSERT INTO `heart-rate-cleaned`
SELECT
  user_id,
  event_id,
  device_id,
  source_app_id,
  TO_TIMESTAMP_LTZ(create_time, 3) AS create_time,
  TO_TIMESTAMP_LTZ(update_time, 3) AS update_time,
  TO_TIMESTAMP_LTZ(start_time, 3) AS start_time,
  TO_TIMESTAMP_LTZ(end_time, 3) AS end_time,
  time_offset,
  `comment`,
  heart_rate,
  heart_beat_count,
  `min`,
  `max`,
  client_data_id,
  client_data_ver
FROM `heart-rate-raw`
WHERE
  heart_rate BETWEEN 30 AND 220
  AND heart_beat_count > 0
  AND end_time >= start_time
  AND event_id IS NOT NULL;
